# Proposal: Speed up the client eval pipeline (demo-scoped)

Status: In progress (2026-05-16) — see §7 Progress
Owner: Ryan Knight
Scope: `packages/dbxcarta-client` eval harness (`dbxcarta.client.eval`)
Related: `docs/dense_1000-fix.md` (embedding fix, already proven)

This is scoped for a **demo**: make the run finish in minutes instead of
hours with decent parallelization, not to harden a production eval harness.
Production-grade items (parity oracles, fail-fast required env vars,
determinism assertions, equivalence regression tests, driver-memory caps)
are intentionally out of scope — see §5.

## 1. Problem

A dense-1000 client eval run ran ~2.5h and was cancelled before the
`graph_rag` arm finished. The embedding fix (`docs/dense_1000-fix.md`) is
proven, so the remaining problem is pure throughput: the harness does
everything serially.

Two serial loops dominate:

1. **graph_rag retrieval** — `_run_graph_rag_arm` (`arms.py:214`) calls
   `retriever.retrieve()` once per question, fully serial. Each
   `retrieve()` issues ~10-11 sequential Cypher round trips to a remote
   Neo4j (`graph_retriever.py:140`). 505 questions x ~11 ≈ ~5,500
   sequential network calls. This is why the arm never finished.
2. **Grading loop** — per gradable question every grading arm runs the
   generated SQL twice (`execute_sql` then `fetch_rows` in `_grade_correct`,
   `arms.py:47`) plus the reference SQL once. Three arms do this — the two
   LLM arms (`no_context`, `schema_dump`) plus `graph_rag` — on top of the
   reference arm. Order ~5,000 serial warehouse statements, with the
   generated SQL run twice for no reason.

Both loops are I/O-bound and per-question independent.

## 2. Fixes

### Fix A — Parallelize graph_rag retrieval (the main win)

`ThreadPoolExecutor` over the per-question `retriever.retrieve()` calls in
`_run_graph_rag_arm`.

- Safe: the Neo4j driver is thread-safe; `retrieve()` opens its own
  `self._driver.session()` per call (`graph_retriever.py:150`) and reads
  only immutable settings. Many sessions over one driver is the intended
  driver usage.
- Order-preserving: submit all, collect into `{question_id: bundle}`, then
  build `questions_with_prompts` / `traces` by iterating `questions` in
  original order. Output is identical to the serial path.
- Concurrency hardcoded to `8` for the 10K multi-catalog demo (module
  constant, optional `DBXCARTA_CLIENT_RETRIEVAL_CONCURRENCY` env override).
  At 10K tables each `retrieve()` is heavier (see §6), so 8 concurrent
  workers is the safer default than 16 unless the Neo4j tier is confirmed
  large. The neo4j driver defaults `max_connection_pool_size` to 100, so
  no pool change is needed at either value.
- On a worker exception, let it propagate and abort the run (close the
  retriever in the existing `finally`). Acceptable for a demo; no partial
  collection, no future-cancellation choreography.

Expected: retrieval is I/O-bound, so roughly N-fold speedup — graph_rag
goes from "never finishes" to minutes.

### Fix B — Stop the redundant warehouse executions

1. Collapse the generated-SQL double execution: in the grading arms, fetch
   the generated rows once and derive `executed` / `non_empty` from that
   single call instead of `execute_sql` then `fetch_rows` on the same
   statement. This is sound by inspection of `executor.py`: the two
   functions share identical timeout/state/error classification; the only
   difference is `fetch_rows` paginates and `execute_sql` reads the first
   chunk only, which is irrelevant to `executed`/`non_empty`/`error`.
2. Cache reference result sets per question (`{question_id: (cols, rows)}`)
   and reuse across arms instead of re-running the reference SQL once per
   arm. The demo question set is fixed, so this is straightforward.

Effect: warehouse statements drop from ~5,000 to ~1,000, independent of
parallelism.

## 3. Plan

0. Preconditions:
   - **Fix M (required code change, multi-catalog correctness blocker —
     see §6).** `_fetch_columns` builds every retrieved FQN as
     `` `{settings.dbxcarta_catalog}`.`{schema}`.`{table}` ``
     (`graph_retriever.py:142`, `:461`), discarding the real catalog that
     is already encoded in `t.id` / `c.id`. The demo graph spans multiple
     catalogs, so every retrieved table outside `DBXCARTA_CATALOG` gets a
     wrong-catalog FQN and graph_rag generates SQL against the wrong
     catalog. Fix: add `catalog_from_node_id` (parts[0]) to `ids.py` and
     derive each row's catalog from `col_id` in `_fetch_columns` instead
     of the single `settings.dbxcarta_catalog`. Small, but it must land
     before the demo or graph_rag is wrong regardless of speed.
   - Config, no code: set `dbxcarta_client_max_expansion_tables > 0` so FK
     expansion is capped; confirm the Neo4j tier can take the chosen
     concurrency.
1. Fix M — catalog-aware FQN (above). Correctness, not performance, but
   gating the demo.
2. Fix B — collapse double execution + reference cache. Small, low risk.
3. Fix A — parallelize retrieval, hardcoded concurrency, order-preserving
   collection.
4. Run the full eval. Sanity-check: graph_rag completes, row counts look
   right (~505 per arm), spot-check a handful of correctness verdicts and
   a few retrieved contexts — confirm tables from non-`DBXCARTA_CATALOG`
   catalogs now carry their real catalog in the FQN (validates Fix M), and
   contexts are not exploded.

That's the whole demo scope.

## 4. Risk notes

- Order preservation in Fix A's parallel collection is the one thing that
  must be right — verified by iterating `questions` in original order when
  building output, not by relying on completion order.
- Reference-result caching changes verdicts only for non-deterministic
  reference SQL (no `ORDER BY`, `now()`, `rand()`). The demo question set's
  reference SQL is expected deterministic; if a result looks off in the
  spot-check, that's the first thing to look at.
- No change to retrieval Cypher, prompts, grading logic, or schema.

## 5. Explicitly out of scope (production hardening, not needed for a demo)

- Required-no-default env vars with fail-fast startup errors (using
  hardcoded constants instead).
- Phase 0 serial baseline as a formal parity oracle.
- `execute_sql`/`fetch_rows` equivalence regression test, determinism
  assertions over the question set, injected-exception abort test.
- Driver-memory caps / per-result-size fallback on the reference cache.
- Fix C (parallelize the grading loop) — add only if A+B leave the run
  visibly slow.
- Embedding-call parallelism, `ai_query` throttling, cluster cold-start /
  local-waiter mismatch (infra, see `docs/dense_1000-fix.md`).

## 6. 10K multi-catalog notes

The demo graph is ~10K tables across multiple catalogs, not the 1K the
original analysis assumed. The parallelization design is unaffected (thread
safety, order preservation, warehouse dedup are all independent of graph
size), but four things change at this scale and are handled above:

1. **Heavier per-`retrieve()` cost.** The lexical steps
   (`_lexical_table_ids`, `_lexical_column_table_ids`,
   `graph_retriever.py:89-128`) do `toLower(name) CONTAINS tok` — an
   unindexed scan over Table / Column nodes per question token. At ~10x the
   nodes this becomes a dominant per-question cost, not just a round-trip.
   Parallelism still wins (8-16 concurrent slow queries beat 505 serial),
   but each worker now does real work, which is why concurrency is dropped
   to 8 by default (Fix A).
2. **FK-expansion explosion.** With
   `dbxcarta_client_max_expansion_tables <= 0`,
   `_references_table_ids_ranked` (`graph_retriever.py:393-394`) uses the
   *uncapped* FK walk. On a denser 10K-table graph this floods
   `_fetch_columns` / `_fetch_values` and prompt size. Set
   `max_expansion_tables > 0` for the demo (precondition 0); this also
   re-enables the cosine re-rank query.
3. **Neo4j load.** 8 concurrent unindexed-scan queries over a 10K-table /
   ~100K-column graph is a very different load than over 1K. Fine on a
   decently sized instance; verify the tier before the demo, or keep
   concurrency at 8 (lower if the instance is small).
4. **Multi-catalog correctness (traced, confirmed — drives Fix M).**
   Ingest builds node IDs catalog-qualified — `catalog.schema.table.column`
   (`schema_graph.py:63,96,124`), so IDs are globally unique. But the
   retriever discards the catalog twice:
   - `schema_from_node_id` returns only the schema (`ids.py:6-9`) and
     `Schema.name` is the bare schema (`schema_graph.py:62`), so the seed
     filter (`_filter_seed_pairs_to_schemas`) and every Cypher
     `s.name IN $schemas` match on bare schema name. Two catalogs with a
     same-named schema both pass. Latent; only bites the demo if two of
     its catalogs share a schema name — check `DBXCARTA_SCHEMAS` against
     the demo catalogs.
   - **Hard blocker:** `_fetch_columns` rebuilds the FQN with the single
     `settings.dbxcarta_catalog` (`graph_retriever.py:142`, `:461`),
     throwing away the real catalog that is already in `c.id` / `t.id`.
     Multi-catalog confirmed, so this mislabels every out-of-
     `DBXCARTA_CATALOG` table and graph_rag emits wrong-catalog SQL. This
     is **Fix M** (precondition 1) — a required code change, independent of
     and prior to the performance work.

## 7. Progress

| Phase | Status | Notes |
|-------|--------|-------|
| 1 — Fix M (catalog-aware FQN) | Done | `catalog_from_node_id` added to `ids.py`; `_fetch_columns` derives catalog per-row from `col_id`, dead single-catalog param removed (`graph_retriever.py`). Helper unit tests added; full client suite (148) + ids/trace (25) green. |
| 2 — Fix B (collapse double exec + reference cache) | Done (2026-05-16) | Generated SQL runs once via `fetch_rows` (executed/non_empty/error derived from it); `_ReferenceCache` memoizes one reference `fetch_rows` per question_id, reused by reference + all grading arms. `arms.py`/`run.py` updated, `test_embed.py` signature fixed. 151 client tests green. (22 `tests/spark/` failures are pre-existing `layers`-branch WIP, outside this scope.) |
| 3 — Fix A (parallelize retrieval) | Done (2026-05-16) | `ThreadPoolExecutor` over per-question `retriever.retrieve()` in `_run_graph_rag_arm`; collect by `question_id`, build prompts/traces by iterating `questions` in order. Concurrency default 8, env override `DBXCARTA_CLIENT_RETRIEVAL_CONCURRENCY` (validated: default/override/blank/invalid). Worker exception propagates and aborts; retriever closed in existing `finally`. Order preservation + exception propagation verified. 151 client tests green. |
| 4 — Full eval run + sanity check | Blocked on operator | Requires a Databricks job run (cannot execute from here). Operator: confirm graph_rag completes, ~505 rows/arm, spot-check verdicts and that non-`DBXCARTA_CATALOG` tables carry their real catalog in the FQN (validates Fix M), contexts not exploded. |

Config preconditions (operator action, no code):

- [ ] Set `dbxcarta_client_max_expansion_tables > 0` in the demo `.env`
      (default `0` is uncapped; §6.2). This also re-enables the cosine
      re-rank query.
- [ ] Confirm the Neo4j tier absorbs the chosen retrieval concurrency
      (default 8; §6.3). Lower `DBXCARTA_CLIENT_RETRIEVAL_CONCURRENCY` on a
      small instance.
- [ ] Check `DBXCARTA_SCHEMAS` against the demo catalogs for the latent
      same-named-schema collision (§6.4 first bullet). Out of code scope
      here; Fix M only addresses the hard FQN blocker.
