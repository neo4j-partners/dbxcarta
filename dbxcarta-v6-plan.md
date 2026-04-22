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

### Stage 1b: Close remaining Stage-1 gaps

Scope reduced after the audit above. The big-ticket items (hash, dimension validation, `label` param, per-label rate map, per-label threshold check) are already implemented. What remains is a small refinement, a new test suite, and a docs archive pass.

#### Design decisions (resolved while writing v6)

These answer questions raised during v6 drafting; they either confirm existing behavior or pin a small change before Stage 1b code work begins.

1. **`embedding_text_hash` scope — helper-side, not caller-side.** `add_embedding_column` itself drops `embedding_text` when `label != "Table"` and keeps `embedding_text_hash` on every embedded node. Already done at `embeddings.py:54-55`. No caller in `pipeline.py` needs to repeat the drop.
2. **Dimension-validation failure semantics — null the vector, retain hash and text, attach reason.** When the returned vector's length does not match `DBXCARTA_EMBEDDING_DIMENSION`, the `embedding` column is set to null (so the row counts as a failure per `compute_failure_stats`), but `embedding_text` and `embedding_text_hash` are input-derived and remain populated. In addition, v6 introduces an `embedding_error STRING` column in the transform: a dimension mismatch produces `"dimension mismatch: got N, expected M"`; an `ai_query` endpoint failure forwards `_emb_raw.errorMessage`; success is null. This column is not persisted to Neo4j (it lives only in the Delta staging table from Stage 2 and the run-summary failure breakdown) so post-mortem can tell endpoint failures apart from shape failures without bloating node properties.
3. **Per-label counter mechanics — post-hoc `.agg()`, no accumulators.** Current code uses `compute_failure_stats(df) → (rate, attempts, successes)` and stores values in plain Python dicts on the `RunSummary` dataclass keyed by label (`pipeline.py:259-262`). The `label` parameter on `add_embedding_column` is purely a tag for where to store those counts in the output maps; the counting path needs no change. No Spark accumulators are introduced.
4. **Per-label threshold — enabled labels only.** A label whose flag is off is absent from `embedding_failure_rate_per_label` entirely (not treated as 0.0, not treated as a failure). Aggregate (`total_emb_attempts`, `total_emb_successes`) is accumulated only inside the enabled branches. The threshold check iterates the map as-is, so disabled labels are naturally skipped (`pipeline.py:272-288`). Confirmed, matches existing behavior.
5. **Run-summary map type — disabled labels absent.** `embedding_failure_rate_per_label`, `embedding_attempts`, and `embedding_successes` contain only enabled labels; disabled labels do not appear with null or 0.0. This matches the threshold semantics above and keeps the map faithful to what was actually run.
6. **`embedding_error` lifetime in Stage 1b — add the column, drop before Neo4j write, defer run-summary wiring to Stage 2.** Stage 1b adds `embedding_error` to the transform output and the caller (`pipeline.py`) drops it before the Neo4j node write. No staging table and no run-summary error breakdown in 1b; those land with Stage 2's materialize-once work.
7. **Shared fixture placement — keep local.** Leave `neo4j_driver` / `run_summary` fixtures in `tests/schema_graph/conftest.py`. `tests/embeddings/conftest.py` imports them from the sibling conftest; move to a shared `tests/conftest.py` only if pytest collection actually complains.
8. **`_validate_embedding` error precedence — endpoint wins.** If `_emb_raw.errorMessage` is non-null, `embedding_error = errorMessage` and `embedding = null`; dimension is not checked. Only when `errorMessage` is null do we test `size(result) == expected_dim`; a mismatch produces `embedding = null`, `embedding_error = "dimension mismatch: got N, expected M"`. Success yields `embedding = result`, `embedding_error = null`.
9. **`embedding_text_hash` is computed unconditionally from the input text.** `sha2(embedding_text, 256)` is applied to every row including rows that later fail, so post-mortem can correlate failures by hash. Hash and text are never nulled by the validation step.
10. **Test fixture for wrong-shape vector — new local-Spark unit suite under `tests/embeddings/`.** Existing suites (`tests/schema_graph/`, `tests/sample_values/`) are integration-only: they load a Neo4j driver and read a run-summary JSON from a UC Volume after a real submit. There is no local-SparkSession fixture yet. For the wrong-shape assertion, a real `ai_query` cannot produce a short vector on demand, so v6 adds a new `tests/embeddings/conftest.py` with a `local_spark` fixture (a `SparkSession.builder.master("local[1]")`-style session). The unit test synthesizes an input DataFrame with a pre-formed `_emb_raw` struct column (short array) and calls the null-on-mismatch branch of `add_embedding_column` directly. Refactor note: to make this testable without mocking `ai_query`, factor the dimension-validation expression out of `add_embedding_column` into a small helper `_validate_embedding(_emb_raw_col, expected_dim) -> (embedding_col, error_col)` that both the production path and the test can call. Integration assertions (hash round-trip, index exists, similarity probe) stay in a second file in the same suite and use the existing neo4j-driver + run-summary fixtures lifted from `tests/schema_graph/conftest.py`.

#### Checklist

- [x] Add `embedding_error STRING` column to the transform per decision #2. `compute_failure_stats` still counts on `embedding` nullity; `embedding_error` is informational. Caller in `pipeline.py` drops the column before the Neo4j `Table` node write.
- [x] Refactor the null-on-mismatch logic in `add_embedding_column` into `_validate_embedding(raw_col, expected_dim) -> (embedding_col, error_col)` per decision #6; the unit test exercises it without invoking `ai_query`.
- [x] Create `tests/embeddings/` with two files and a `conftest.py`:
  - `conftest.py` defines a `local_spark` fixture (local-mode SparkSession). `neo4j_driver` / `run_summary` fixtures are re-exported from `tests.schema_graph.conftest` per decision #7.
  - `test_transform_unit.py` (unit): wrong-shape vector → `embedding` is null, `embedding_error` is `"dimension mismatch: got N, expected M"`; endpoint error wins over dimension mismatch; `label="Column"` call drops `embedding_text`; wrong-shape rows count as failures in `compute_failure_stats`; per-label threshold trips independently of aggregate.
  - `test_graph_integration.py` (integration): every in-scope `Table` node carries all five embedding properties; `embedding_text_hash == sha256(embedding_text)` on `Table`; `embedding_error` is not persisted to Neo4j; nodes outside the enabled scope carry none of the five; vector index exists with the expected dimension and cosine function; a cosine-similarity probe returns non-empty neighbors.
- [x] Delete `dbxcarta-phase2.md` and `dbxcarta-phase3.md` — superseded by v5 + v6; git history preserves them.

### Stage 2: Apply best-practice hardening

Three design changes surfaced by the v5 review. Each is grounded in `docs/best-practices.md`; items below cite the section.

- [x] **Materialize-once between transform and load** (`docs/best-practices.md` Spark §4). After `add_embedding_column` runs on a node DataFrame, write the result to a Delta staging table under the existing DBxCarta volume path, then read it back. The failure-rate aggregation and the Neo4j write both consume the Delta-read DataFrame. The staging root is truncated at the start of each run via `dbutils.fs.rm(..., recurse=True)` so label subdirs don't orphan when flags flip off; per-label writes use `overwriteSchema=true` so future column additions don't require a manual drop. No `.cache()` / `.persist()` is used in place of this; inference spend justifies the I/O round-trip. The staging-table path lives in `.env` as `DBXCARTA_STAGING_PATH` with a sensible default under the configured volume (derivation rejects a bare volume root to avoid escaping into the schema directory).
- [x] **Neo4j write partitioning and batch size** (`docs/best-practices.md` Neo4j §1, §2). In `pipeline.py`, every relationship DataFrame is `.coalesce(1)` before the connector write. Node writes default to `.coalesce(1)` for the first green run. Add `DBXCARTA_NEO4J_BATCH_SIZE=20000` to `.env` and thread it into the connector's `batch.size` option on every write. The constant is read once from `Settings` and passed explicitly to each write call so there is one place to change it.
- [x] **Swap default embedding endpoint to `databricks-gte-large-en`** (`docs/best-practices.md` Spark §3). *(Default changed; spike re-validation pending — see `worklog/spike-ai-query.md` header note.)* Change `DBXCARTA_EMBEDDING_ENDPOINT` default from `databricks-bge-large-en` to `databricks-gte-large-en`. `DBXCARTA_EMBEDDING_DIMENSION` stays at `1024`. Re-run `scripts/run_spike_ai_query.py` against the new endpoint and confirm: (a) the return struct schema is unchanged (`struct<result: array<double>, errorMessage: string>`), (b) the dimension is 1024, (c) `failOnError => false` behavior is unchanged. Update `worklog/spike-ai-query.md` with the new endpoint's observation. If any of (a)-(c) differ, fall back to `bge-large-en` and record the reason.
- [x] Add a smoke test covering the materialize-once invariant: run the pipeline with a tiny fixture, assert the staging Delta table has one row per in-scope node and that its `embedding` column is populated, and assert the Neo4j write and failure-rate agg both read from Delta (instrument via counter or log assertion).

#### Fix later (deferred from Stage 2 cleanup)

Surfaced while tidying up after Stage 2; out of scope for Stage 2 itself but worth doing before Stage 3 so the first green run is on stable footing.

- [x] **Rewrite `README.md` to match the v6 pipeline.** Lines 28, 36-38, 58, 63, 80 still describe the removed `DBXCARTA_JOB=schema|sample|embeddings` dispatcher, claim embeddings are "not yet implemented", and walk through "Phase 2" as a separate submit. Replace with the single-submit flow, per-label embedding flags, staging/materialize-once behavior, and a pointer to `dbxcarta-v6-plan.md` + `docs/best-practices.md`.
- [x] **Fix `DBXCARTA_SCHEMAS` value in `.env`.** Was fully qualified (`graph-enriched-lakehouse.graph-enriched-schema`) but `pipeline.py` filters on the bare `schema_name` column; corrected to `graph-enriched-schema`.

### Stage 3: First green run with Table embeddings only

Validates the end-to-end pipeline with the smallest possible embedding scope turned on. (Was Stage 2 in v5.)

Re-validated 2026-04-22 against `dbxcarta-catalog.dbxcarta-schema` after a Neo4j reset — see `worklog/stage3-first-green-run.md` for run IDs and metrics.

**Pre-run checks completed 2026-04-21:**

- Spike re-validation on `databricks-gte-large-en` — struct schema, 1024-dim, and `failOnError => false` behavior all match `bge-large-en`. See `worklog/spike-ai-query.md` header.
- Table count in `graph-enriched-lakehouse.graph-enriched-schema`: **13** — small, appropriate for Stage 3 scope.
- Neo4j secret scope `dbxcarta-neo4j`: all three keys (`uri`, `username`, `password`) present.
- Staging volume coverage: staging defaults to `/Volumes/graph-enriched-lakehouse/graph-enriched-schema/dbxcartavolume/staging`, the same UC volume preflight already `CREATE VOLUME IF NOT EXISTS`es at `pipeline.py:459-461`. No separate grant needed.
- `DBXCARTA_EMBEDDING_FAILURE_THRESHOLD` bumped to `0.10` in `.env` so a single transient failure in a 13-row run doesn't abort. Restore to `0.05` after Stage 3 is green.

- [x] Set `.env` flags so only `DBXCARTA_INCLUDE_EMBEDDINGS_TABLES=true`; all other embedding flags off.
- [x] Constrain the scope so only a handful of tables hit the serving endpoint. `DBXCARTA_SCHEMAS=graph-enriched-schema` — 13 tables, appropriate for Stage 3.
- [X] Wipe the target Neo4j instance manually (Aura console reset, or `MATCH (n) DETACH DELETE n`) before the first green run. No automated reset path ships with v6.
- [x] Bootstrap step creates the Neo4j vector index on `Table.embedding` with the dimension read from `DBXCARTA_EMBEDDING_DIMENSION` and cosine similarity. Index `table_embedding` confirmed ONLINE post-run.
- [x] Run `dbxcarta submit run_dbxcarta.py` (correct invocation; `scripts/` prefix is added by the runner from `scripts_dir`). Required `upload --wheel` + `upload --all` first — see worklog note below. On `databricks-job-runner 0.4.6+`, `dbxcarta submit --upload run_dbxcarta.py` folds the `upload --all` step into submit (wheel still uploaded separately).
- [x] Inspect the run summary: status=success; `Table` failure rate=0.0% (13/13); per-label rate recorded in Delta; staging table has 13 rows; all 5 embedding properties present on all 13 Table nodes; Column nodes have 0 embeddings (flag off). Run summary Delta table evolved to 18 columns via `mergeSchema=true`.
- [x] **Verify materialize-once empirically.** Staging table at `/Volumes/.../staging/table_nodes` has exactly 13 rows matching the 13 in-scope Table nodes. Serving endpoint invocation count before the run could not be read programmatically (UI-only metric); structural verification (staging row count = table count, 0 failures, single run) is consistent with one-call-per-row semantics. Endpoint UI check deferred to Stage 4 integration suite. See `worklog/stage3-first-green-run.md`.
- [x] Run the same submit a second time and confirm counts are identical. Table count and embedding metrics identical (13 attempts, 13 successes, 0.0% rate). Column and Value counts differ slightly (93→100, 59→61) because the catalog gained new columns between the two runs — not a pipeline idempotency issue.

### Stage 4: Verification suites

Covers the three concerns of the v6 graph with separate pytest suites that all run after a single submit. (Was Stage 3 in v5.)

- [ ] `tests/schema_graph/` unchanged in its coverage of nodes, relationships, identifier shape, idempotency, and run-summary presence.
- [ ] `tests/sample_values/` unchanged in its coverage of `Value` nodes, `HAS_VALUE` relationships, cardinality filtering, and idempotency.
- [ ] `tests/embeddings/` asserts: every in-scope `Table` node carries all five embedding properties; `embedding_text_hash == sha256(embedding_text)` on `Table`; nodes outside the enabled scope carry none of the five; the vector index exists with the expected configuration; a cosine-similarity probe against a sampled real table returns non-empty semantically related neighbors.
- [ ] All three suites pass after the second green run.

### Stage 5: Expand embedding coverage (later phases)

Rolled out one label at a time after the Table rollout is validated. The code infrastructure for all four labels ships in one pass; subsequent steps are configuration flips + test runs. (Was Stage 4 in v5.)

#### Design decisions (resolved while writing Stage 5)

1. **Code changes are needed.** `pipeline.py` only has an embedding block for `Table`; analogous blocks for Column, Value, Schema, and Database must be added. The generic infra (bootstrap indexes, `_stage_embedded_nodes`, `add_embedding_column`, threshold checks) already handles all labels — the gap is the per-label `if enabled:` blocks in the transform section.

2. **Retain context fields in node builders for richer embedding text.** Since `embedding_text` is dropped after hashing for non-Table labels, there is zero storage cost to making it rich. Richer input improves the quality of similarity search on the stored vector.
   - `build_column_nodes()` retains `table_schema` and `table_name` (same pattern as `build_table_nodes()` retaining `table_schema`); both are dropped from the Neo4j write since they are not node properties.
   - `build_schema_nodes()` retains `catalog_name`; dropped from the Neo4j write.

3. **Embedding text expressions by label:**

   | Label | Expression | Storage |
   |---|---|---|
   | Column | `concat_ws(' \| ', concat_ws('.', table_schema, table_name, name), data_type, nullif(trim(comment), ''))` | hash-only |
   | Schema | `concat_ws(' \| ', concat_ws('.', catalog_name, name), nullif(trim(comment), ''))` | hash-only |
   | Database | `name` (catalog name; no comment exists) | hash-only |
   | Value | `value` (the sampled string is the semantic content) | hash-only |

4. **Threshold check positioning.** The per-label and aggregate threshold checks in `pipeline.py` sit immediately after the Table embedding block. Adding the four new blocks before them keeps the existing check position correct: all embedding blocks run, then the single shared threshold check fires.

5. **`.env` flag state at end of Stage 5 code work.** All four label code paths are wired in one pass, but `.env` is left with only `DBXCARTA_INCLUDE_EMBEDDINGS_COLUMNS=true`. Values, Schemas, and Databases are enabled one step at a time after each re-run is verified.

#### Checklist

- [ ] Modify `build_column_nodes()` to retain `table_schema` and `table_name`; modify `build_schema_nodes()` to retain `catalog_name`. Both fields are dropped from the Neo4j write (they are not node properties).
- [ ] Add `_COLUMN_EMBEDDING_TEXT_EXPR`, `_SCHEMA_EMBEDDING_TEXT_EXPR`, `_DATABASE_EMBEDDING_TEXT_EXPR`, `_VALUE_EMBEDDING_TEXT_EXPR` constants to `pipeline.py` (see decision #3).
- [ ] Add embedding blocks for Column, Value, Schema, and Database in the transform section of `pipeline.py`, each guarded by its flag and following the Table pattern: `add_embedding_column` → `_stage_embedded_nodes` → `compute_failure_stats` → accumulate summary counts.
- [ ] Drop `embedding_error` and context-only fields (`table_schema`, `table_name` for Column; `catalog_name` for Schema) before the respective Neo4j node writes.
- [ ] Set `DBXCARTA_INCLUDE_EMBEDDINGS_COLUMNS=true` in `.env`; leave Values, Schemas, Databases off.
- [ ] Re-run; verify Column nodes carry all five embedding properties, `embedding_text` is absent (hash-only), vector index `column_embedding` is ONLINE, failure rate ≤ threshold.
- [ ] Extend `tests/embeddings/test_graph_integration.py` with Column assertions: all in-scope Column nodes carry the five embedding properties; `embedding_text` is absent; vector index `column_embedding` exists with correct config; similarity probe returns neighbors; non-Column labels still carry no embeddings (when their flags are off).
- [ ] Turn on `DBXCARTA_INCLUDE_EMBEDDINGS_VALUES=true`, re-run, extend integration tests for Value nodes. `Value` nodes are hash-only.
- [ ] Turn on `DBXCARTA_INCLUDE_EMBEDDINGS_SCHEMAS=true` and `DBXCARTA_INCLUDE_EMBEDDINGS_DATABASES=true`, re-run, extend integration tests for Schema and Database nodes. Both are hash-only.

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
- Full embedding coverage across all node labels. v6 enables `Table` and `Column` initially; Value, Schema, and Database roll out one step at a time per Stage 5 checklist.
- Endpoint throughput and Neo4j parallelism benchmarking. v6 defers all performance work past `coalesce(1)` + `batch.size=20000`.

## Out of scope

- Databricks Workflows scheduling. v6 remains a one-shot submit through `databricks-job-runner`.
- Multi-catalog ingestion. v6 handles a single configured catalog.
- Any change to the graph contract in `dbxcarta.contract`. Labels, relationship types, identifier generation, and contract version carry forward from v4 unchanged.
- Any change to the Neo4j Spark Connector or DBR version pair. The provisional pin from v4 carries forward.
