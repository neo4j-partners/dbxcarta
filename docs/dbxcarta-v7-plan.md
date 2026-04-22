# DBxCarta v7 — Implementation Plan

## Relationship to v5 and v6

This plan supersedes `dbxcarta-v6-plan.md` for the **Execution checklist** only. Design decisions, goal, verification criteria, node property layout, pipeline diagram, and text expressions for all label embeddings are unchanged and carry forward by reference.

See `dbxcarta-v5-plan.md:1-110` for the canonical goal statement, how-to-verify criteria, design summary, and node property layout. See `dbxcarta-v6-plan.md` for:

- Pipeline-at-a-glance diagram (v6, valid through v7)
- Stage 1b–3 design decisions (all resolved, all shipped)
- Stage 5 design decisions for per-label embedding text expressions
- Stage 5A per-label rollout log

When pointing another agent at a subset of a file, the `Read` tool accepts `offset` (1-based line) and `limit` arguments; human-readable prose uses the `path:start-end` convention.

## Best practices reference

All design rules are captured with sources in `docs/best-practices.md`. Stages below cite the sections they derive from. If a stage introduces a new external-system rule, record it there before shipping the stage.

The doc is organized into three sections:

- **Spark / Databricks** — `ai_query` batch guidance, `failOnError` semantics, batch-optimized embedding models, lazy-eval re-execution trap, metadata caching.
- **Neo4j Spark Connector** — write repartitioning for lock avoidance, `batch.size` tuning, Aura leader-only writes, vector index bootstrap ordering.
- **Project-level principles** — UC-as-sole-source, memo-not-authority caching, fail-fast preflight, counted-not-thrown embedding failures.

## What shipped in v6

### Stage 1b (complete)

`embedding_error STRING` column added to the transform; `_validate_embedding` refactored into a testable helper. `tests/embeddings/` created with `conftest.py` (local-mode SparkSession), `test_transform_unit.py` (unit tests for dimension mismatch, endpoint-error precedence, hash semantics, failure counting, per-label threshold logic), and `test_graph_integration.py` (integration suite covering all five labels). Superseded plan docs `dbxcarta-phase2.md` and `dbxcarta-phase3.md` deleted.

### Stage 2 (complete)

Materialize-once: every embedded node DataFrame is written to a Delta staging table under the configured volume before any downstream action reads it — `ai_query` is invoked exactly once per row per run. `DBXCARTA_STAGING_PATH` and `DBXCARTA_NEO4J_BATCH_SIZE` added to `.env`. Relationship writes use `.coalesce(1)`. Default endpoint swapped to `databricks-gte-large-en` (re-validated against `bge-large-en` — struct schema, 1024-dim, `failOnError => false` unchanged). README rewritten for the v6 single-submit flow. `DBXCARTA_SCHEMAS` corrected to bare schema name.

### Stage 3 (complete)

First green run against `dbxcarta-catalog.dbxcarta-schema` (13 Table nodes, 0.0% embedding failure rate, `table_embedding` vector index ONLINE). Second run confirmed idempotency. Staging table verified at 13 rows matching the 13 in-scope Table nodes. See `worklog/stage3-first-green-run.md`.

### Stage 5 + 5A (complete)

Embedding code blocks for Column, Value, Schema, and Database added to `pipeline.py`. `build_column_nodes()` and `build_schema_nodes()` updated to retain context fields for richer embedding text; those fields are dropped before Neo4j writes. All four label-enabling flags turned on one at a time with a run-and-verify loop per Stage 5A. `tests/embeddings/test_graph_integration.py` extended with per-label assertions (properties, no `embedding_text`, vector index config, cosine probe). All Stage 5A steps verified. `DBXCARTA_EMBEDDING_FAILURE_THRESHOLD` is currently 0.10 in `.env` (bumped for Stage 3; not yet restored — see Stage 4b below).

### Stage 6: REFERENCES relationship (complete)

Implemented and verified 2026-04-22 against the seeded W8 fixture schemas. The three-way `referential_constraints ⨝ key_column_usage ⨝ key_column_usage` join is in `pipeline._extract` and `schema_graph.build_references_rel`. Four run-summary counters record FK coverage: `fk_declared`, `fk_resolved`, `fk_skipped`, `fk_references`. Per unresolved FK, a `logger.warning` is emitted; the run does not abort. `tests/schema_graph/test_fk_references.py` covers the edge-count invariant (universal), the accounting invariant (universal), and exact fixture coverage (scope-gated to `dbxcarta_fk_test` + `dbxcarta_fk_test_b`). Verification run ID `14050798953569`: 3/3 declared FKs resolved, 4 column-pair edges written, all FK tests passed. See `worklog/v5-fk-findings.md` for the full investigation report.

The v6 plan checklist shows Stage 6 as unchecked because the implementation and write-up were completed between plan drafts. It is treated as shipped for v7.

### Stage 4 status (tests exist, not yet run to green)

The three test suites (`tests/schema_graph/`, `tests/sample_values/`, `tests/embeddings/`) were written and exercised piecemeal during earlier stages, but all three suites have not been run together and confirmed green after the full pipeline (all five embedding labels, REFERENCES) stabilized. Four tests in `tests/schema_graph/` were reported as failing in `worklog/v5-fk-findings.md §8.4` with `AttributeError: 'NoneType' object has no attribute 'data_array'` — an SDK `statement_execution.execute_statement` result-shape mismatch. Guards (`if result.result is None: pytest.skip(...)`) were added after those failures were reported; whether the guards now cause skips or the underlying issue still surfaces as an error is unconfirmed. Stage 4b below closes this gap.

## Execution checklist

Stage 4b green is the v7 milestone — the "first green run" equivalent. Stages 7.1, 7.2, and 7.3 each ship independently as their own PR once 4b is done; v7 closes when all four are verified.

### Stage 4b: Confirm verification suites green

One small config fix, one investigation, then a full suite run.

#### Pre-run fix

- [ ] Restore `DBXCARTA_EMBEDDING_FAILURE_THRESHOLD=0.05` in `.env`. It was bumped to 0.10 for Stage 3's first run on a small fixture schema and never restored per the v6 Stage 3 instruction.

#### SDK result-shape investigation

The `data_array` failures were observed before the `if result.result is None: pytest.skip(...)` guards were added to the affected test files. Three of the four files now have those guards; `test_id_normalization.py` also has `pytest.skip()` calls but is missing `import pytest`. The current error behaviour is unknown — the suites may now skip cleanly, fail with `NameError`, or still surface `AttributeError` depending on whether the underlying cause (query timeout or wrong disposition) has been resolved by the guards.

Affected files:
- `tests/schema_graph/test_complex_types.py` (all 5 parametrize variants)
- `tests/schema_graph/test_id_normalization.py::test_sampled_column_ids_exist`
- `tests/schema_graph/test_id_normalization.py::test_python_id_matches_spark_sql_id`
- `tests/schema_graph/test_run_summary.py::test_summary_delta_row_exists`

- [ ] Add `import pytest` to `tests/schema_graph/test_id_normalization.py` (unconditional fix regardless of current symptom).
- [ ] Run `pytest tests/schema_graph/ -v` and observe the current failure mode for the four tests (skip, NameError, AttributeError, or pass). Then apply the smallest fix that makes them green or skipping:
  - If they skip cleanly → acceptable, but prefer pass: if adding `disposition=Disposition.INLINE, format=Format.JSON_ARRAY` is a ≤ 2-line change per file, make them pass rather than leave a permanent skip in the regression suite.
  - If `AttributeError: 'NoneType' object has no attribute 'data_array'` → `result.result` is still None at access; the guard isn't being reached. Check whether `execute_statement` is returning before query completion (`wait_timeout` too short) or using `EXTERNAL_LINKS` disposition. Fix: either increase the wait timeout or add `disposition=Disposition.INLINE, format=Format.JSON_ARRAY` to each `execute_statement` call.
  - If another error → investigate and fix.

#### Suite confirmation

- [ ] Run `pytest tests/schema_graph/ -v` and confirm all tests pass or skip (no failures).
- [ ] Run `pytest tests/sample_values/ -v` and confirm all tests pass or skip.
- [ ] Run `pytest tests/embeddings/ -v` and confirm all tests pass or skip.
- [ ] All three suites pass (or skip with a documented reason) against a run where all five embedding labels are enabled and `fk_references > 0`. Record the run ID here and in a `docs/stage4b-verification.md`.

### Stage 7: Operational hardening

Three additive improvements. Implement in order; each does not block the pipeline's correctness if deferred.

#### 7.1 Re-embedding ledger

**What this does:** Avoids paying inference cost for nodes whose content hasn't changed. Before calling `ai_query` on a batch, the pipeline checks a durable Delta ledger keyed by `(node_label, node_identifier, embedding_model)`. If the stored `embedding_text_hash` matches the current hash and the model matches, the stored vector is reused. Only hashes that have changed — or are new — call the endpoint.

**Design decisions (to resolve before implementation):**

1. **Ledger table location and lifetime** — the ledger is a pipeline-owned Delta table (not a staging table; it persists across runs). Literal default: `/Volumes/dbxcarta-catalog/dbxcarta-schema/dbxcartavolume/ledger`. In `Settings`, derive programmatically as `Path(summary_volume).parent / "ledger"` (same sibling pattern as staging). Add `DBXCARTA_LEDGER_PATH` to `.env.sample` commented out, matching the style of `DBXCARTA_STAGING_PATH`.

2. **Hit condition** — the ledger is one Delta table per label, stored at `<ledger_root>/<label>` (e.g. `…/ledger/table`). Within a per-label table the merge key is `(id, embedding_model)`, eliminating any cross-label id collision. A hit requires: `id` and `embedding_model` match and `embedding_text_hash` in the ledger equals the current hash. Any mismatch is a miss and triggers a new `ai_query` call. Model changes (endpoint swapped) invalidate all cached vectors for that label. Rows where `embedding_error != null` are excluded from the ledger upsert and re-attempted on every subsequent run until they succeed.

3. **Integration point** — `_embed_and_stage` in `pipeline.py` is the right integration point: left-join the input DataFrame against the per-label ledger on `(id, embedding_model)` before calling `add_embedding_column`, carry hits forward, call `add_embedding_column` only on misses, then union. The ledger is updated (upsert) after the staging write so it always reflects what was written to Neo4j. When 7.2 incremental mode is also enabled, a table re-entering scope after a skip hits the ledger first: hash match means text unchanged despite `last_altered` advancing (reuse vector); hash miss means content changed (re-embed).

4. **Ledger is memo, not authority** — UC remains the only source of truth. The ledger can be dropped at any time; the pipeline rebuilds it on the next full run. Do not add ledger-read paths to preflight; a missing ledger is treated as a full miss (same as a fresh run).

5. **`DBXCARTA_LEDGER_ENABLED` flag** — off by default. Enables the ledger without changing the default pipeline behavior. When off, `_embed_and_stage` behaves identically to v6.

6. **`embedded_at` on a ledger hit** — carry the original `embedded_at` from the ledger row (the timestamp when the vector was first computed). A freshly-written timestamp would obscure whether the vector pre-dates a model or endpoint change; the original timestamp preserves that debugging signal.

**Checklist:**

- [x] Add `DBXCARTA_LEDGER_ENABLED` and `DBXCARTA_LEDGER_PATH` to `Settings` and `.env.sample`. Default path derived as sibling to staging root. (`pipeline.py` Settings; `.env.sample` Stage 7.1 section)
- [x] Add `_read_ledger(spark, path, label) -> DataFrame | None` — returns the ledger DataFrame for a label (or None if the ledger table doesn't exist yet). (`pipeline.py:_read_ledger`)
- [x] Modify `_embed_and_stage` to: (1) when `DBXCARTA_LEDGER_ENABLED`, left-join input against the ledger to separate hits from misses; (2) call `add_embedding_column` on misses only; (3) synthesize the same five columns on the hit branch; (4) union hits and newly-embedded misses; (5) write union to staging; (6) upsert newly-embedded rows to the ledger. Hit/miss split extracted into `_split_by_ledger` for testability. (`pipeline.py:_embed_and_stage`, `_split_by_ledger`)
- [x] Add `_upsert_ledger(staged_df, path, label)` — Delta MERGE on `(id, embedding_model)`; error rows excluded. (`pipeline.py:_upsert_ledger`)
- [x] Extend run summary with `embedding_ledger_hits MAP<STRING, BIGINT>`; added to `RunSummary` dataclass, `emit_delta` schema, and `CREATE TABLE IF NOT EXISTS` DDL. (`summary.py`, `pipeline.py:_preflight`)
- [x] Unit tests added: `test_ledger_all_rows_hit_misses_empty` (misses empty when all rows match), `test_ledger_hash_mismatch_is_miss`, `test_ledger_model_mismatch_is_miss`. All 3 pass locally. (`tests/embeddings/test_transform_unit.py`)
- [ ] Enable `DBXCARTA_LEDGER_ENABLED=true` in `.env`, submit, and confirm: (a) first run computes all vectors; (b) second run with no catalog changes hits 100% of vectors from the ledger (0 `ai_query` calls); (c) changing one table comment causes a hash miss for that table only; (d) per-label hit counts recorded in run summary. — **Pending user-driven Databricks submit.**

#### 7.2 Incremental scope via `last_altered`

**What this does:** Instead of processing the entire catalog every run, the pipeline reads `information_schema.tables.last_altered` and restricts the transform scope to tables modified since the last successful run. This shrinks the embedding batch size in steady-state dramatically.

**Design decisions (to resolve before implementation):**

1. **Trigger column** — `tables.last_altered` (available in Unity Catalog's `information_schema.tables`). Cutoff is the `ended_at` timestamp of the most recent successful run, read from the run-summary Delta table.

2. **Scope of incremental mode** — applies to Table, Column, and Value nodes (which are derived from `tables_df` and `columns_df`). Schema and Database nodes are small (typically 1–5 rows) and always re-processed. The incremental filter is applied to `tables_df` before building node and relationship DataFrames; downstream DataFrames (columns, values) inherit the filtered scope.

3. **Fallback to full mode** — if no prior successful run is found in the summary table, or if `DBXCARTA_INCREMENTAL=false`, the pipeline runs the full catalog (current v6 behavior).

4. **Relationship consistency** — because relationships are written with MERGE semantics, processing a subset of tables does not remove relationships from tables that are not in scope; it simply skips re-asserting them. This is consistent with the existing "UC-as-sole-source" principle for reads and the "MERGE-idempotent" principle for writes.

5. **`DBXCARTA_INCREMENTAL` flag** — off by default. On by default is too risky until the scope-narrowing logic is validated on a production-scale catalog.

6. **`row_counts["tables"]` invariant** — `row_counts["tables"]` must remain the full catalog table count (same as v6, derived before the incremental filter is applied). A filtered count goes into the separate `row_counts["tables_in_scope"]` key. This preserves the existing `test_node_counts.py` assertion that compares `row_counts["tables"]` against the Neo4j Table node count for full runs; that assertion only fires when `tables_in_scope == tables` (full mode or empty-diff incremental run), which is still correct.

7. **`last_altered` coverage** — `last_altered` advances on DDL changes (column add/drop, comment edits). It does not advance on pure DML (new distinct values arriving in a categorical column). Value node embeddings can therefore be stale after data-only changes. Known limitation for v7; a full run (`DBXCARTA_INCREMENTAL=false`) is the remediation. Confirm empirically before shipping — see checklist item below.

8. **Deletion detection** — incremental mode never sees dropped tables or columns; Neo4j carries orphan nodes until the next full run. Accepted limitation for v7. Document alongside the `DBXCARTA_INCREMENTAL` flag in the README.

**Checklist:**

- [ ] Verify empirically that a column-comment edit advances `last_altered` in `information_schema.tables`: alter a test table's column comment, then query `SELECT last_altered FROM information_schema.tables WHERE ...`. Document the observed behaviour in a one-line comment in `pipeline.py` near `_last_successful_run_ended_at`.
- [ ] Add `DBXCARTA_INCREMENTAL` to `Settings` and `.env.sample`. Default `false`.
- [ ] Add `_last_successful_run_ended_at(spark, summary_table) -> datetime | None` — queries the run-summary Delta table for the most recent row where `status = 'success'` and returns `ended_at`.
- [ ] In `_extract`, when incremental mode is on and a prior `ended_at` is found, filter `tables_df` (and cascade to `columns_df` via an inner join on `table_schema`, `table_name`) to only tables where `last_altered > ended_at`. Log the count of in-scope vs. total tables.
- [ ] Add `row_counts["incremental_cutoff_ts"]` and `row_counts["tables_in_scope"]` to the run summary so the incremental filter is auditable.
- [ ] Enable `DBXCARTA_INCREMENTAL=true` in `.env` on a test run. Verify: (a) second run after a stable catalog has `tables_in_scope` = 0 and completes in seconds; (b) editing one table's comment causes `tables_in_scope = 1` on the next run; (c) Neo4j counts are unchanged after the near-zero-scope run.
- [ ] Extend `tests/schema_graph/test_idempotency.py` with an assertion that a run where `tables_in_scope = 0` leaves all Neo4j counts unchanged.

#### 7.3 Parallel node writes via `repartition(N, id)`

**What this does:** v6 defaults every node DataFrame to `coalesce(1)` to avoid Neo4j write lock contention. `repartition(N, id)` achieves disjoint-id partitioning instead — two Spark tasks can write concurrently without ever contending on the same node's uniqueness lock — so write parallelism is safe by construction rather than by benchmark. Throughput measurement is deferred to `worklog/benchmarking_plan.md`.

**Design decisions:**

1. **Rule** — node writes use `df.repartition(N, "id")` before the Neo4j write. Relationship writes remain `coalesce(1)` per the Neo4j Spark Connector guidance in `docs/best-practices.md` Neo4j §1 (relationships lock two endpoint nodes, so disjoint-id partitioning does not generalize to them).

2. **Default `N`** — `DBXCARTA_NEO4J_NODE_PARTITIONS=4` in `.env.sample`. Raising `N` is a config change; evidence for a different value comes from `benchmarking_plan.md` when that work is picked up.

3. **Scope** — applies to all five node labels (Database, Schema, Table, Column, Value).

**Checklist:**

- [ ] Add `DBXCARTA_NEO4J_NODE_PARTITIONS` to `Settings` and `.env.sample`. Default `4`.
- [ ] In the load stage, replace each `<node_df>.coalesce(1).write…` with `<node_df>.repartition(settings.neo4j_node_partitions, "id").write…`. Leave relationship writes on `coalesce(1)`.
- [ ] Submit once and confirm idempotency: node counts and relationship counts are unchanged from a v6-equivalent reference run.
- [ ] Update `docs/best-practices.md` Neo4j §1 (already amended with the standing rule; add a run ID once observed).

## Open items deferred to later phases

- **Endpoint throughput benchmark** — moved to `worklog/benchmarking_plan.md`. Picks up the "which side is the bottleneck?" question once there's a reason to ask it (e.g. slow production runs).
- **Multi-catalog ingestion** — v7 remains single-catalog. Multiple catalogs would require parameterizing `DBXCARTA_CATALOG` across runs or introducing a catalog list.
- **Databricks Workflows scheduling** — v7 remains a one-shot submit. A recurring job would need a DAB definition.
- **Enforced FK property on REFERENCES edges** — all observed Databricks FK constraints have `enforced = NO`. If future tables use enforced constraints, a `REFERENCES.enforced BOOLEAN` property is a contract change deferred to v8.
- **`ai_query` concurrency tuning** — a follow-up if the benchmark plan (`worklog/benchmarking_plan.md`) finds the endpoint is the bottleneck. Would involve Databricks serving endpoint concurrency limits and whether `ai_query` supports batching the text array (rather than one row per call).

## Out of scope

- Any change to the graph contract in `dbxcarta.contract`.
- Any change to the Neo4j Spark Connector or DBR version pair.
- Cross-catalog FK references (Unity Catalog DDL does not permit them).
