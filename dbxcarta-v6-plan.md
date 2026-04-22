# DBxCarta v6 — Implementation Plan

## Relationship to v5

This plan supersedes `dbxcarta-v5-plan.md` for the **Execution checklist** only. The design, goal, verification criteria, node property layout, and text expression for Table embeddings are unchanged and carry forward by reference.

See `dbxcarta-v5-plan.md:1-110` for:

- Pipeline-at-a-glance diagram (updated below with v6 additions)
- Goal
- How to verify and validate
- Design summary
- Node property layout for embeddings
- Re-embedding and UC-to-Neo4j correlation (Stage 7 here)
- Text expression for Table embeddings

When pointing another agent at a subset of a file, the `Read` tool accepts `offset` (1-based line) and `limit` arguments; human-readable prose uses the `path:start-end` convention (e.g. `dbxcarta-v5-plan.md:1-110`).

## Best practices reference

All design rules that shape this plan's stages are captured with sources in `docs/best-practices.md`. Stage 2 items below cite the sections they derive from. If a stage introduces a new external-system rule, record it there with a source link before (or as part of) shipping the stage.

The doc is organized into three sections:

- **Spark / Databricks** — `ai_query` batch guidance, `failOnError` semantics, batch-optimized embedding models, lazy-eval re-execution trap, metadata caching.
- **Neo4j Spark Connector** — write repartitioning for lock avoidance, `batch.size` tuning, Aura leader-only writes, vector index bootstrap ordering.
- **Project-level principles** — UC-as-sole-source, memo-not-authority caching, fail-fast preflight, counted-not-thrown embedding failures.

## What shipped in v5

### Stage 0: `ai_query` spike (complete)

Authoritative reference: `worklog/spike-ai-query.md`. Key outcome: `ai_query(..., failOnError => false)` returns `struct<result: array<double>, errorMessage: string>`, not a bare array. The transform projects `.result` as the `embedding` column and treats `result IS NULL` as the row-level failure signal. The spike ran against `databricks-bge-large-en` at 1024 dimensions; v6 switches the default endpoint (see Stage 2 below) and re-validates schema parity as part of that stage.

### Stage 1: Pipeline rebuild (complete with known gaps)

Done:

- `scripts/run_dbxcarta.py` is a single submission script with no `DBXCARTA_JOB` dispatch.
- `src/dbxcarta/pipeline.py` owns `run_dbxcarta()` and all orchestration (preflight, Neo4j driver lifecycle, bootstrap, extract, transform wiring, writes, summary).
- `schema_graph.py`, `sample_values.py`, `embeddings.py` are pure transform modules that take and return Spark DataFrames; none open Neo4j, read `Settings`, or submit writes.
- `sample_values._candidates_from_columns_df` reads the cached `columns_df` in memory; the Neo4j Python driver is out of the candidate-discovery path.
- `.env` configuration consolidated: `DBXCARTA_INCLUDE_VALUES`, `DBXCARTA_INCLUDE_EMBEDDINGS_{TABLES,COLUMNS,VALUES,SCHEMAS,DATABASES}`, `DBXCARTA_EMBEDDING_FAILURE_THRESHOLD`, `DBXCARTA_EMBEDDING_ENDPOINT`, `DBXCARTA_EMBEDDING_DIMENSION`. `DBXCARTA_JOB` removed.
- Preflight checks every grant and endpoint permission required by the enabled flags and fails fast on any missing.
- Run-summary Delta uses `CREATE TABLE IF NOT EXISTS` (decision override from v5's `DROP TABLE IF EXISTS`) with schema-merge on write, preserving history across runs.
- Run-summary schema includes `embedding_model`, `embedding_flags`, `embedding_attempts`, `embedding_successes`, `embedding_failure_rate`, `embedding_failure_threshold`.

Audit against v5's "Partial" status lines (re-read of `src/dbxcarta/embeddings.py`, `src/dbxcarta/summary.py`, `src/dbxcarta/pipeline.py` as of 2026-04-21):

- `add_embedding_column` already appends all five properties (`embedding_text`, `embedding_text_hash`, `embedding`, `embedding_model`, `embedded_at`), already accepts `expected_dimension` and `label`, already nulls `embedding` on dimension mismatch, and already drops `embedding_text` in the helper for every label except `Table` (`embeddings.py:33-57`).
- Run-summary already carries `embedding_failure_rate_per_label MAP<STRING,DOUBLE>` in both the Python dataclass (`summary.py:45`) and the Delta struct (`summary.py:113`, `pipeline.py:403`).
- Per-label threshold check already runs before the aggregate check and names the offending label in the error message (`pipeline.py:270-288`).

v5's status lines overstated the gap. What is actually still open for Stage 1b:

- No `tests/embeddings/` suite exists; the contract has no automated verification yet.
- `dbxcarta-phase2.md` / `dbxcarta-phase3.md` are still present and superseded.
- One small refinement surfaced while answering design questions for this plan: retain a row-level `errorMessage` when a vector is nulled for dimension mismatch, so post-mortem can tell endpoint failures apart from shape failures (see Design decisions below).

## Pipeline at a glance (v6)

```
  ┌──────────────────────────────────────────────────────────────────┐
  │                      Unity Catalog                                │
  │  information_schema.{schemata, tables, columns, constraints}      │
  │  data tables (for distinct-value sampling)                        │
  └──────────────────────────────┬───────────────────────────────────┘
                                 │
                                 ▼
  ┌──────────────────────────────────────────────────────────────────┐
  │  PREFLIGHT  (fail fast, no partial mode)                          │
  │   • UC grants for configured catalog/schemas                      │
  │   • Serving-endpoint invoke permission (per enabled flag)         │
  │   • CREATE TABLE IF NOT EXISTS run-summary; CREATE VOLUME IF NEED │
  └──────────────────────────────┬───────────────────────────────────┘
                                 │
                                 ▼
  ┌──────────────────────────────────────────────────────────────────┐
  │  EXTRACT  (Spark SQL reads, one per source view)                  │
  │   columns_df.cache()                                              │
  │   source tables → distinct-count pre-pass for sampling            │
  └──────────────────────────────┬───────────────────────────────────┘
                                 │
                                 ▼
  ┌──────────────────────────────────────────────────────────────────┐
  │  TRANSFORM  (pure Spark)                                          │
  │   1. Build node and relationship DataFrames                       │
  │   2. Enrich nodes with embeddings (ai_query, per-label flag)      │
  │   3. *** Materialize enriched nodes to a Delta staging table ***  │
  │      (prevents re-invocation of ai_query across downstream        │
  │       actions: failure-rate agg, Neo4j write, optional ledger)    │
  └──────────────────────────────┬───────────────────────────────────┘
                                 │
                                 ▼
  ┌──────────────────────────────────────────────────────────────────┐
  │  LOAD  (Neo4j Spark Connector, MERGE, batch.size tuned)           │
  │   0. Bootstrap: uniqueness constraints, vector indexes            │
  │   1. Read node DFs from Delta staging → write to Neo4j            │
  │      (nodes: default coalesce(1); tune later)                     │
  │   2. Write relationship DFs                                       │
  │      (all rels: coalesce(1) to avoid endpoint-node lock contention)│
  │   3. Emit run summary (Delta):                                    │
  │      per-label + aggregate failure rates vs threshold             │
  └──────────────────────────────┬───────────────────────────────────┘
                                 │
                                 ▼
                         ┌───────────────┐
                         │  Neo4j AuraDB │
                         └───────────────┘
```

Diagram diffs from v5:

- TRANSFORM step 3 adds a Delta staging materialization to prevent `ai_query` re-execution across downstream actions.
- LOAD step 1 reads from Delta staging rather than from the in-memory enriched DataFrame.
- LOAD step 2 pins `.coalesce(1)` for relationship writes.
- PREFLIGHT reflects the v5 Stage-1 decision to preserve run-summary history via `CREATE TABLE IF NOT EXISTS`.

## Execution checklist

### Stage 1b: Close Stage-1 gaps

Strictly the items the v5 plan specced but did not finish. No new design.

- [ ] Extend `add_embedding_column(df, text_expr, endpoint, *, label)` to append five properties instead of four: add `embedding_text_hash` as `sha2(embedding_text, 256)` computed in Spark. Add dimension validation: if `size(embedding)` does not equal `DBXCARTA_EMBEDDING_DIMENSION`, null the `embedding` column so the row is counted as a failure. Add the `label` keyword parameter; the function uses it to tag per-label attempt/success counters and (later) to decide whether to retain `embedding_text` on the node. For labels other than `Table`, the caller drops `embedding_text` before the Neo4j write; `embedding_text_hash` stays on every embedded node.
- [ ] Extend the run-summary schema with `embedding_failure_rate_per_label MAP<STRING, DOUBLE>`. Keys use Neo4j label casing (`Table`, `Column`, `Value`, `Schema`, `Database`).
- [ ] Change the threshold check in `pipeline.py` so the run fails when either the aggregate rate or any enabled label's rate exceeds `DBXCARTA_EMBEDDING_FAILURE_THRESHOLD`. Both values are recorded so a post-mortem read shows which tripped.
- [ ] Extend the existing embeddings tests to assert: `embedding_text_hash == sha256(embedding_text)` on `Table`; a row with a wrong-shape vector is counted as a failure; per-label threshold trips independently of aggregate.
- [ ] Archive or delete `dbxcarta-phase2.md` and `dbxcarta-phase3.md`. Superseded by v5 + v6.

### Stage 2: Apply best-practice hardening

Three design changes surfaced by the v5 review. Each is grounded in `docs/best-practices.md`; items below cite the section.

- [ ] **Materialize-once between transform and load** (`docs/best-practices.md` Spark §4). After `add_embedding_column` runs on a node DataFrame, write the result to a Delta staging table under the existing DBxCarta volume path, then read it back. The failure-rate aggregation and the Neo4j write both consume the Delta-read DataFrame. The staging table is truncated at the start of each run. No `.cache()` / `.persist()` is used in place of this; inference spend justifies the I/O round-trip. The staging-table path lives in `.env` as `DBXCARTA_STAGING_PATH` with a sensible default under the configured volume.
- [ ] **Neo4j write partitioning and batch size** (`docs/best-practices.md` Neo4j §1, §2). In `pipeline.py`, every relationship DataFrame is `.coalesce(1)` before the connector write. Node writes default to `.coalesce(1)` for the first green run. Add `DBXCARTA_NEO4J_BATCH_SIZE=20000` to `.env` and thread it into the connector's `batch.size` option on every write. The constant is read once from `Settings` and passed explicitly to each write call so there is one place to change it.
- [ ] **Swap default embedding endpoint to `databricks-gte-large-en`** (`docs/best-practices.md` Spark §3). Change `DBXCARTA_EMBEDDING_ENDPOINT` default from `databricks-bge-large-en` to `databricks-gte-large-en`. `DBXCARTA_EMBEDDING_DIMENSION` stays at `1024`. Re-run `scripts/run_spike_ai_query.py` against the new endpoint and confirm: (a) the return struct schema is unchanged (`struct<result: array<double>, errorMessage: string>`), (b) the dimension is 1024, (c) `failOnError => false` behavior is unchanged. Update `worklog/spike-ai-query.md` with the new endpoint's observation. If any of (a)-(c) differ, fall back to `bge-large-en` and record the reason.
- [ ] Add a smoke test covering the materialize-once invariant: run the pipeline with a tiny fixture, assert the staging Delta table has one row per in-scope node and that its `embedding` column is populated, and assert the Neo4j write and failure-rate agg both read from Delta (instrument via counter or log assertion).

### Stage 3: First green run with Table embeddings only

Validates the end-to-end pipeline with the smallest possible embedding scope turned on. (Was Stage 2 in v5.)

- [ ] Set `.env` flags so only `DBXCARTA_INCLUDE_EMBEDDINGS_TABLES=true`; all other embedding flags off.
- [ ] Constrain the scope so only a handful of tables hit the serving endpoint. Set `DBXCARTA_SCHEMAS` to a single small schema in the target catalog. Per-request throughput tuning is deferred to Stage 7; `DBXCARTA_SCHEMAS` is the scope lever for this phase.
- [ ] Wipe the target Neo4j instance manually (Aura console reset, or `MATCH (n) DETACH DELETE n`) before the first green run. No automated reset path ships with v6.
- [ ] Bootstrap step creates the Neo4j vector index on `Table.embedding` with the dimension read from `DBXCARTA_EMBEDDING_DIMENSION` and cosine similarity. Indexes for other labels are created only when their flags turn on.
- [ ] Run `dbxcarta submit scripts/run_dbxcarta.py` against a fresh Neo4j instance bootstrapped with uniqueness constraints.
- [ ] Inspect the run summary: all stages executed; `Table` failure rate under `0.05`; per-label rate recorded; node and relationship counts match expectations; the staging Delta table is populated with the expected row count.
- [ ] Run the same submit a second time and confirm counts are identical.

### Stage 4: Verification suites

Covers the three concerns of the v6 graph with separate pytest suites that all run after a single submit. (Was Stage 3 in v5.)

- [ ] `tests/schema_graph/` unchanged in its coverage of nodes, relationships, identifier shape, idempotency, and run-summary presence.
- [ ] `tests/sample_values/` unchanged in its coverage of `Value` nodes, `HAS_VALUE` relationships, cardinality filtering, and idempotency.
- [ ] `tests/embeddings/` asserts: every in-scope `Table` node carries all five embedding properties; `embedding_text_hash == sha256(embedding_text)` on `Table`; nodes outside the enabled scope carry none of the five; the vector index exists with the expected configuration; a cosine-similarity probe against a sampled real table returns non-empty semantically related neighbors.
- [ ] All three suites pass after the second green run.

### Stage 5: Expand embedding coverage (later phases)

Rolled out one label at a time after the Table rollout is validated. Each step is a configuration change plus a test-suite expansion, not a code change. (Was Stage 4 in v5.)

- [ ] Turn on `DBXCARTA_INCLUDE_EMBEDDINGS_COLUMNS=true`, add the `Column` vector index to the bootstrap, re-run, extend tests. `Column` nodes store `embedding_text_hash` but not the full `embedding_text`.
- [ ] Turn on `DBXCARTA_INCLUDE_EMBEDDINGS_VALUES=true`, add the `Value` vector index, re-run, extend tests. `Value` nodes are hash-only.
- [ ] Turn on `DBXCARTA_INCLUDE_EMBEDDINGS_SCHEMAS=true` and `DBXCARTA_INCLUDE_EMBEDDINGS_DATABASES=true`, add the corresponding vector indexes, re-run, extend tests. These are hash-only.

### Stage 6: REFERENCES relationship (W8)

(Was Stage 5 in v5.) `W8` is the v4 work-item label for the `(Column)-[:REFERENCES]->(Column)` write (read `R4` over `information_schema.referential_constraints` ⨝ `key_column_usage`). See `dbxcarta-v5-fk.md` for the implementation plan and `worklog/v5-fk-findings.md` for the FK-join investigation.

v6 ships with `REFERENCES` stubbed and `row_counts["fk_references"] = 0`.

- [ ] Validate the `information_schema` FK join against `graph_enriched_lakehouse`: view column names, Delta FK declarations present, coverage percent, skip-with-warning vs fail-run policy.
- [ ] Implement the resolved join as a transform that produces the `REFERENCES` relationship DataFrame.
- [ ] Extend `tests/schema_graph/` to assert `REFERENCES` source and target columns exist and that coverage is recorded in the run summary.

### Stage 7: Operational hardening

Optimizations deliberately deferred. (Was Stage 6 in v5.)

- [ ] Re-embedding policy via a Delta embedding ledger. Introduce a ledger keyed by `(node_label, node_identifier, embedding_model)` with columns `embedding_text_hash`, `embedding array<double>`, `embedded_at`. The transform left-joins candidate DataFrames against the ledger before calling `ai_query`. Hit condition: `(identifier, model, hash)` all match. Miss conditions: missing identifier, hash changed (metadata edited), or model changed (endpoint swapped). The ledger is updated after each successful write. The ledger is a pipeline-owned memo, not authority; UC remains the only source of truth and the ledger can be dropped and rebuilt at any time. Stage 2's materialize-once Delta staging table is not the ledger; the ledger is a separate, durable table keyed by identity across runs.
- [ ] Per-run scope. Design an incremental refresh mode that reads `information_schema.tables.last_altered` and filters transforms to tables modified since the last successful run.
- [ ] Endpoint throughput benchmark. Measure sustained `ai_query` throughput against the target catalog size. Before increasing Neo4j write parallelism beyond `coalesce(1)`, confirm the sink is the bottleneck (Neo4j leader cores), not the source.

## Open items deferred to later phases

- Re-embedding policy via Delta embedding ledger. v6 persists `embedding_text_hash` + `embedding_model` on each node so Stage 7 is additive.
- Per-run scope narrowing via `last_altered`. v6 processes the full catalog every run.
- `REFERENCES` relationship. v6 stubs it at zero coverage; Stage 6 resolves.
- Full embedding coverage across all node labels. v6 enables `Table` only; remaining labels roll out one at a time.
- Endpoint throughput and Neo4j parallelism benchmarking. v6 defers all performance work past `coalesce(1)` + `batch.size=20000`.

## Out of scope

- Databricks Workflows scheduling. v6 remains a one-shot submit through `databricks-job-runner`.
- Multi-catalog ingestion. v6 handles a single configured catalog.
- Any change to the graph contract in `dbxcarta.contract`. Labels, relationship types, identifier generation, and contract version carry forward from v4 unchanged.
- Any change to the Neo4j Spark Connector or DBR version pair. The provisional pin from v4 carries forward.
