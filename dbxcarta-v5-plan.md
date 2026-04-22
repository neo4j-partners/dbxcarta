# DBxCarta v5 — Implementation Plan

## Pipeline at a glance

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
  │                                                                   │
  │   • UC grants for configured catalog/schemas                      │
  │   • Serving-endpoint invoke permission (per enabled flag)         │
  │   • DROP+CREATE run-summary Delta; CREATE VOLUME IF NEEDED        │
  └──────────────────────────────┬───────────────────────────────────┘
                                 │
                                 ▼
  ┌──────────────────────────────────────────────────────────────────┐
  │  EXTRACT  (Spark SQL reads, one per source view)                  │
  │                                                                   │
  │   databases_df   schemas_df   tables_df   columns_df              │
  │   (columns_df cached; sample_values reads it in-memory)           │
  │   source tables → distinct-count pre-pass for sampling            │
  └──────────────────────────────┬───────────────────────────────────┘
                                 │
                                 ▼
  ┌──────────────────────────────────────────────────────────────────┐
  │  TRANSFORM  (pure Spark, no side effects)                         │
  │                                                                   │
  │   1. Build node DataFrames                                        │
  │        database_nodes  schema_nodes  table_nodes                  │
  │        column_nodes    value_nodes                                │
  │      — each has  id, name, properties, contract_version           │
  │                                                                   │
  │   2. Build relationship DataFrames                                │
  │        has_schema  has_table  has_column  has_value               │
  │        (references stubbed in v5; see Stage 5)                    │
  │                                                                   │
  │   3. Enrich nodes with embeddings  (per-label flag)               │
  │        text       = <label-specific Spark SQL expression>         │
  │        embedding  = ai_query(<endpoint>, text, failOnError=false) │
  │        appends: embedding_text (Table only),                      │
  │                 embedding_text_hash, embedding,                   │
  │                 embedding_model, embedded_at                      │
  └──────────────────────────────┬───────────────────────────────────┘
                                 │
                                 ▼
  ┌──────────────────────────────────────────────────────────────────┐
  │  LOAD  (Neo4j Spark Connector, MERGE semantics throughout)        │
  │                                                                   │
  │   0. Bootstrap:  uniqueness constraints,  vector indexes          │
  │                  (dim = DBXCARTA_EMBEDDING_DIMENSION, cosine)     │
  │   1. Write all node DataFrames                                    │
  │   2. Write all relationship DataFrames                            │
  │   3. Emit run summary (Delta): per-label embedding attempts /     │
  │      successes, failure_rate vs threshold (fail iff exceeded)     │
  └──────────────────────────────┬───────────────────────────────────┘
                                 │
                                 ▼
                         ┌───────────────┐
                         │  Neo4j AuraDB │
                         └───────────────┘
```

## Goal

Rebuild DBxCarta as a single Spark ETL pipeline that reads Unity Catalog metadata, enriches nodes with vector embeddings during the transform stage, and writes the resulting graph to Neo4j AuraDB. The unified pipeline replaces the three separate phase jobs from v4. Embeddings become a column added by `ai_query()` in the transform stage, not a separate job that reads from Neo4j and writes back. The first shippable version turns embeddings on for `Table` nodes only and defers all other labels to later rollouts controlled by feature flags.

## How to verify and validate

The v5 pipeline is complete when all of the following hold against the `graph_enriched_lakehouse` catalog:

1. A single `dbxcarta submit run_dbxcarta.py` call produces the full graph in one run: all structural nodes and relationships, sampled values for categorical columns, and embeddings on `Table` nodes.
2. Every `Table` node in Neo4j has an `embedding` property of the expected vector dimension and an `embedding_model` property matching the configured endpoint.
3. A vector index on `Table.embedding` exists with the expected dimension and cosine similarity function.
4. A similarity probe against a real table sampled from the target catalog returns semantically related neighbors as its top matches.
5. The run summary records counts for rows read, nodes written, relationships written, embedding attempts, embedding successes, and embedding failures per label and aggregate. Both the aggregate failure rate and every enabled label's failure rate are below the configured threshold.
6. A second submit of the same script produces identical node and relationship counts and does not produce duplicate nodes or relationships.
7. All three pytest suites pass: `tests/schema_graph/`, `tests/sample_values/`, and `tests/embeddings/`.

## Design summary

The pipeline is one Spark job with three stages. The extract stage reads Unity Catalog `information_schema` views and the data tables needed for distinct-value sampling. The transform stage builds node and relationship DataFrames, then enriches each node DataFrame with a text column and an embedding column produced by the Databricks `ai_query()` SQL function. The load stage bootstraps Neo4j with uniqueness constraints and vector indexes, writes all node DataFrames, then writes all relationship DataFrames through the Neo4j Spark Connector with MERGE semantics. Unity Catalog is the only source of truth. Neo4j is written to and never read from during the run. No intermediate Delta staging layer exists.

Sample-value candidate discovery derives from the in-memory `columns_df` cached during the extract stage, not from a Neo4j read. Filtering happens in Spark on `data_type` and the configured schema scope. This keeps the pipeline's "UC is the only source of truth" rule intact and removes the neo4j Python driver from the candidate-discovery path.

Feature flags in `.env` control which node labels receive embeddings on any given run. The first green run has `Table` embeddings turned on and all others turned off. Rolling out additional labels is a configuration change, not a code change.

`ai_query()` runs with `failOnError => false`, so individual rows that fail return null instead of aborting the query. The pipeline counts null embeddings and compares the failure rate against a configurable threshold, **both per-label and aggregate**. A run is marked failed when either the aggregate rate or any enabled label's rate exceeds the threshold. This catches the case where one label silently degrades while another masks it in aggregate (2% on Tables plus 8% on Values aggregates under 5% but Values is broken).

A returned vector whose length does not match the configured `DBXCARTA_EMBEDDING_DIMENSION` is treated as a row-level failure (null embedding, counted against the failure rate), not an abort. If the configured dimension is wrong globally, every row mismatches and the threshold trips cleanly; special-casing abort would duplicate the threshold mechanism.

### Node property layout for embeddings

Every embedded node carries the same five-property contract, regardless of label:

| Property | Type | Purpose |
|---|---|---|
| `embedding` | `array<double>` | the vector; null signals row-level failure |
| `embedding_model` | `string` | the configured endpoint; lets downstream skip when the endpoint changes |
| `embedding_text_hash` | `string` (sha256 hex) | fingerprint of the exact input text; lets downstream detect metadata drift without storing the full text |
| `embedded_at` | `timestamp` | from `current_timestamp()`; debugging and staleness |
| `embedding_text` | `string` | the exact text fed to the model. **Stored on `Table` nodes only in v5.** For `Column` / `Value` / `Schema` / `Database` (later stages) only the hash is stored to keep nodes lean; the full text can always be reconstructed from UC by re-applying the label's text expression. |

Nodes without embeddings carry none of these properties, so the node schema is predictable: either the full set is present or none is.

### Re-embedding and UC-to-Neo4j correlation (Stage 6)

The correlation key between UC and a Neo4j node is already deterministic: every node's identifier is a pure function of UC metadata per the frozen contract (`catalog.schema.table` for `Table`, etc.). No lookup is needed to go from UC to node identity.

What is missing is a way to answer "is this node's embedding current for the configured model and the current metadata text?" without reading Neo4j. Stage 6 introduces a **Delta embedding ledger** written as a side-effect of the transform, keyed by `(node_label, node_identifier, embedding_model)` with columns `embedding_text_hash`, `embedding array<double>`, `embedded_at`. Future runs left-join candidate DataFrames against the ledger before calling `ai_query`:

- identifier missing in ledger → embed
- `embedding_text_hash` differs (metadata changed, e.g. comment edited) → embed
- `embedding_model` differs (endpoint swapped) → embed
- otherwise → reuse the cached vector, skip the call

**The ledger is a pipeline-owned artifact, not authority.** UC stays the only source of truth; Neo4j stays write-only during a run; the ledger is just a memo of "what we've already spent tokens on." It can be dropped and rebuilt from scratch at any time — the next run just re-embeds everything once.

**Why not read Neo4j to answer this?** Because it couples the pipeline to the state of the sink and makes partial-graph recovery a first-class concern. A Delta ledger gives you the same skip behavior without that coupling, and it's queryable from a notebook for debugging ("show me every `Table` I've embedded in the last 30 days").

v5 ships with the five-property contract (including `embedding_text_hash`) on every embedded node so Stage 6 can introduce the ledger additively, without a contract break or a backfill.

The preflight at the top of every run fails fast on any missing grant or endpoint permission required by the enabled flags. There is no graceful-skip or partial-graph mode. A missing permission produces a clear error naming the failing grant and aborts the run before any read or write occurs. This keeps the pipeline's contract simple: a submit either produces the full configured graph or fails with an actionable error.

### Text expression for Table embeddings

The Table text fed to `ai_query()` on the first green run is the schema-qualified table name joined to the comment with a pipe separator, with empty comments handled cleanly so tables without a comment embed on identifier alone:

`concat_ws(' | ', concat_ws('.', schema_name, table_name), nullif(trim(comment), ''))`

This produces text like `sales.orders | Customer order line items with status and fulfillment timestamps`. Schema qualification disambiguates same-named tables across schemas. The pipe separator gives the embedding model a clean boundary between identifier-like text and natural-language description. Enriching the text with child column names or sampled values is a deliberate later iteration, not a first-run decision. Column and Value text expressions are decided when those rollouts land.

## Execution checklist

### Stage 0: ai_query spike (first, blocking)

**Status:** complete. See `worklog/spike-ai-query.md` for the authoritative reference. Key outcome: `ai_query(..., failOnError => false)` returns `struct<result: array<double>, errorMessage: string>`, not a bare array. The transform stage must project `.result` (array<double>, dim=1024) as the `embedding` column and treat `result IS NULL` as the failure signal.

Validates the single biggest assumption in the new design before any rebuild work begins.

- [x] Create `scripts/run_spike_ai_query.py` modeled on `scripts/run_dbxcarta.py`: same `_cluster_bootstrap.inject_params()` entry, submittable via `dbxcarta submit scripts/run_spike_ai_query.py`. The script runs a minimal `ai_query()` call against `databricks-bge-large-en` on three or four representative test strings including one deliberately invalid input, prints the output schema, observed dimension, and null behavior to stdout, and exits.
- [x] Confirm the returned column type is a Spark array of doubles, compatible with the Neo4j Spark Connector's float-array property write path. **Actual:** a struct wrapping `array<double>` — the `.result` field is array<double> and is the value the Neo4j Spark Connector should receive. The transform projects `.result` before the write.
- [x] Confirm the returned vector dimension matches the 1024 expected by bge-large-en.
- [x] Confirm `failOnError => false` returns null for deliberately invalid input instead of aborting the query. **Actual:** the struct is returned for every row; on failure `result` is null and `errorMessage` is populated. The query itself completes without aborting.
- [x] Confirm the cluster's service principal has invoke permission on the serving endpoint (separate from Unity Catalog grants). The same probe call becomes the preflight's endpoint-permission check at job startup.
- [x] Write the spike note at `worklog/spike-ai-query.md`: two paragraphs recording the spike results, the exact `ai_query` invocation used, and the observed output schema. This note becomes the authoritative reference for the transform stage.

### Stage 1: Pipeline rebuild

Consolidates the three existing phase scripts into one and reshapes the modules into transforms rather than jobs.

#### Module layout

Orchestration lives in a new `src/dbxcarta/pipeline.py` module that owns the `run_dbxcarta()` entry point. The pipeline module is the only place that loads `Settings`, runs the preflight, opens the Neo4j driver, bootstraps constraints and vector indexes, calls the extract queries, wires the transforms together, issues the Spark Connector writes, and emits the run summary. The existing `schema_graph.py`, `sample_values.py`, and `embeddings.py` modules shrink to pure transforms that take and return Spark DataFrames. They do not open Neo4j connections, do not read `Settings`, and do not submit writes. This preserves the "no module submits a job" rule in the checklist item below while also making the misnamed `schema_graph.run_schema()` orchestration go away rather than growing a second orchestration entry point inside a transform module. Tests can exercise individual transforms by passing fixture DataFrames, and the pipeline module is the single place to look for anything that touches external systems.

Concretely, after Stage 1 the wheel's shape is:
- `dbxcarta/pipeline.py` — `run_dbxcarta()` and all orchestration (preflight, neo4j driver lifecycle, constraint and vector-index bootstrap, extract, transform wiring, writes, summary).
- `dbxcarta/schema_graph.py` — pure node-and-relationship DataFrame builders (`build_database_node`, `build_schema_nodes`, `build_table_nodes`, `build_column_nodes`, `build_has_schema_rel`, `build_has_table_rel`, `build_has_column_rel`).
- `dbxcarta/sample_values.py` — pure `sample(columns_df, catalog, schema_list, settings) -> (value_node_df, has_value_rel_df, sample_stats)` and the cardinality-pre-pass helpers; no Neo4j read.
- `dbxcarta/embeddings.py` — `add_embedding_column(df, text_expr, endpoint)` plus a small failure-rate helper. No orchestration.
- `dbxcarta/__init__.py` — re-exports `run_dbxcarta` from `pipeline`.
- `scripts/run_dbxcarta.py` — `inject_params(); from dbxcarta import run_dbxcarta; run_dbxcarta()`.

- [x] Rewrite `scripts/run_dbxcarta.py` as the single submission script, removing the `DBXCARTA_JOB` dispatch. It calls one entry point (`run_dbxcarta()`) with no branching.
- [x] Collapse the wheel's three entry points (`run_schema`, `run_sample`, `run_embeddings`) into a single entry point `run_dbxcarta()` that lives in a new `pipeline.py` module. `__init__.py` re-exports it.
- [x] Keep `schema_graph.py`, `sample_values.py`, and `embeddings.py` as transform modules inside the wheel. Each exposes pure functions that take and return Spark DataFrames. No module submits a job, opens Neo4j, reads `Settings`, or issues writes; all three responsibilities live in `pipeline.py`.
- [x] Add `add_embedding_column(df, text_expr, endpoint, *, label)` to `embeddings.py` as the reusable transform helper that appends five columns to any node DataFrame: `embedding_text` (the exact text fed to the model, from the passed expression), `embedding_text_hash` (sha256 hex of the text, computed in Spark via `sha2(embedding_text, 256)`), `embedding` (the vector returned by `ai_query()` with `failOnError => false`, with length validated against `DBXCARTA_EMBEDDING_DIMENSION` and nulled on mismatch), `embedding_model` (the configured endpoint string), and `embedded_at` (from `current_timestamp()`). All five land on the node as properties in the Neo4j write. The caller is responsible for dropping `embedding_text` before the write for any label other than `Table`; `embedding_text_hash` remains on every embedded node. Nodes without embeddings carry none of these properties, so the node schema is predictable: either the full set is present or none is. For the first green run, the `Table` caller passes the text expression pinned in the Design summary above.
- [x] Rewrite `sample_values._read_candidates` to accept the cached `columns_df` from the extract stage and filter it in memory on `data_type IN ('STRING', 'BOOLEAN')` and the configured schema scope. Remove the neo4j Python driver import from this path. The distinct-count pre-pass remains a Spark query against the source tables and is unchanged. **Done as `_candidates_from_columns_df`.**
- [x] Consolidate the `.env` configuration keys. Replace the three phase-specific blocks with one set of keys plus per-label embedding flags. Remove `DBXCARTA_JOB` entirely. The new keys and their defaults when unset: `DBXCARTA_INCLUDE_VALUES=true` (sampling always runs by default, matching v4's implicit behavior), `DBXCARTA_INCLUDE_EMBEDDINGS_TABLES=false`, `DBXCARTA_INCLUDE_EMBEDDINGS_COLUMNS=false`, `DBXCARTA_INCLUDE_EMBEDDINGS_VALUES=false`, `DBXCARTA_INCLUDE_EMBEDDINGS_SCHEMAS=false`, `DBXCARTA_INCLUDE_EMBEDDINGS_DATABASES=false`, `DBXCARTA_EMBEDDING_FAILURE_THRESHOLD=0.05`, `DBXCARTA_EMBEDDING_ENDPOINT=databricks-bge-large-en`, and `DBXCARTA_EMBEDDING_DIMENSION=1024` (the expected vector length; used to size the Neo4j vector index and to assert shape in the transform). The dimension lives in `.env` so that swapping endpoints is a pure configuration change.
- [x] Update the preflight in the unified script to check every grant and endpoint permission required by the enabled flags, and to fail fast with a clear error naming the missing grant. The preflight runs before any read or write. There is no graceful-skip or partial-graph mode.
- [x] In the run-summary preflight, use `CREATE TABLE IF NOT EXISTS` (preserving run history) with the v5 schema and a migration comment; `emit_delta` uses `mergeSchema=true` so existing v4 tables gain the new columns on the next write. The Volume is still created with `CREATE VOLUME IF NOT EXISTS`. **Decision override from plan: user chose `CREATE TABLE IF NOT EXISTS` over `DROP TABLE IF EXISTS`.**
- [x] Extend the run-summary schema with the following embedding fields: `embedding_model STRING`, `embedding_flags MAP<STRING, BOOLEAN>` (per label, configured state), `embedding_attempts MAP<STRING, BIGINT>` (per label), `embedding_successes MAP<STRING, BIGINT>` (per label), `embedding_failure_rate_per_label MAP<STRING, DOUBLE>` (per enabled label), `embedding_failure_rate DOUBLE` (aggregate across enabled labels), and `embedding_failure_threshold DOUBLE` (the threshold this run was configured against). The run fails when either the aggregate rate or any per-label rate exceeds the threshold; both are recorded so post-mortem reads show which one tripped. Failures are derived on read as `attempts - successes` rather than stored separately. Map keys use Neo4j label casing (`Table`, `Column`, `Value`, `Schema`, `Database`) to match Neo4j count queries, not the lowercase identifier-normalization convention.
- [x] Delete `run_dbxcarta_schema.py`, `run_dbxcarta_sample.py`, and `run_dbxcarta_embeddings.py` once the unified script is working end to end. **N/A: these separate scripts never existed; the old dispatcher was the single `run_dbxcarta.py`.**
- [ ] Archive or delete `dbxcarta-phase2.md` and `dbxcarta-phase3.md` once this v5 plan is accepted, since they are superseded.

### Stage 2: First green run with Table embeddings only

Validates the end-to-end pipeline with the smallest possible embedding scope turned on.

- [ ] Set `.env` flags so only `DBXCARTA_INCLUDE_EMBEDDINGS_TABLES=true`; all other embedding flags off.
- [ ] Constrain the first green run's scope so only a handful of tables hit the serving endpoint. Set `DBXCARTA_SCHEMAS` to a single small schema in the target catalog. Per-request throughput tuning is deferred to Stage 6; `DBXCARTA_SCHEMAS` is the scope lever for this phase.
- [ ] Wipe the target Neo4j instance manually (Aura console reset, or a `MATCH (n) DETACH DELETE n` Cypher statement) before the first green run. No automated reset path ships with v5.
- [ ] Add a bootstrap step that creates the Neo4j vector index on `Table.embedding` with the dimension read from `DBXCARTA_EMBEDDING_DIMENSION` and cosine similarity function. Indexes for other labels are created only when their flags turn on.
- [ ] Run `dbxcarta submit scripts/run_dbxcarta.py` against a fresh Neo4j instance bootstrapped with the uniqueness constraints.
- [ ] Inspect the run summary and confirm all three stages executed, the failure rate on `Table` embeddings is under 0.05, and node and relationship counts match expectations.
- [ ] Run the same submit a second time and confirm counts are identical on the second run.

### Stage 3: Verification suites

Covers the three concerns of the v5 graph with separate pytest suites that all run after a single submit.

- [ ] Keep the existing `tests/schema_graph/` suite unchanged in its coverage of nodes, relationships, identifier shape, idempotency, and run-summary presence.
- [ ] Keep the existing `tests/sample_values/` suite unchanged in its coverage of `Value` nodes, `HAS_VALUE` relationships, cardinality filtering, and idempotency.
- [ ] Create a new `tests/embeddings/` suite that asserts every in-scope `Table` node carries all five embedding properties (`embedding`, `embedding_model`, `embedding_text`, `embedding_text_hash`, `embedded_at`) with expected types and a vector dimension matching the configured model, that `embedding_text_hash` equals `sha256(embedding_text)` on `Table` (the one label where both are stored, so the contract can be verified end-to-end), that nodes outside the enabled scope carry none of the five, that the vector index exists with the expected configuration, and that a cosine-similarity probe against a sampled real table returns non-empty semantically related neighbors.
- [ ] Confirm all three suites pass after the second green run.

### Stage 4: Expand embedding coverage (later phases)

Rolled out one label at a time after the Table rollout is validated and stable. Each step is a configuration change plus a test-suite expansion, not a code change.

- [ ] Turn on `DBXCARTA_INCLUDE_EMBEDDINGS_COLUMNS=true`, add the `Column` vector index to the bootstrap, re-run, and extend `tests/embeddings/` to cover `Column` nodes. `Column` nodes store `embedding_text_hash` but not the full `embedding_text` (hash-only storage per the node property layout in the Design summary).
- [ ] Turn on `DBXCARTA_INCLUDE_EMBEDDINGS_VALUES=true`, add the `Value` vector index to the bootstrap, re-run, and extend tests. `Value` nodes are hash-only for the same reason.
- [ ] Turn on `DBXCARTA_INCLUDE_EMBEDDINGS_SCHEMAS=true` and `DBXCARTA_INCLUDE_EMBEDDINGS_DATABASES=true`, add the corresponding vector indexes, re-run, and extend tests. These are hash-only as well; `Table` remains the only label storing the full `embedding_text`.

### Stage 5: REFERENCES relationship (W8) (later phase)

`W8` is the v4 work-item label for the `(Column)-[:REFERENCES]->(Column)` write (paired with read `R4` over `information_schema.referential_constraints` ⨝ `key_column_usage`). See `dbxcarta-v5-fk.md` for the implementation plan and `worklog/v5-fk-findings.md` for the completed FK-join investigation (three-way join confirmed on Databricks UC, 100% coverage on seeded fixtures).

v5 ships with `REFERENCES` stubbed and `row_counts["fk_references"] = 0`. The investigation recorded in v4's Open Items resolves in a follow-up.

- [ ] Validate the `information_schema` foreign-key join against `graph_enriched_lakehouse`: confirm Databricks view column names match the ISO spec, confirm Delta foreign-key declarations appear in the views, measure coverage as a percentage of declared foreign keys resolved to source-and-target column identifier tuples, and decide whether unresolvable foreign keys are skipped with a warning or fail the run.
- [ ] Implement the resolved join as a new transform that produces the `REFERENCES` relationship DataFrame.
- [ ] Extend `tests/schema_graph/` to assert `REFERENCES` source-and-target columns exist and that coverage is recorded in the run summary.

### Stage 6: Operational hardening (later phase)

Optimizations deliberately deferred from v5 to keep the first shippable version simple.

- [ ] Re-embedding policy via a Delta embedding ledger. Introduce a ledger table keyed by `(node_label, node_identifier, embedding_model)` with columns `embedding_text_hash`, `embedding array<double>`, `embedded_at`. The transform left-joins candidate node DataFrames against the ledger before calling `ai_query`: rows whose `(identifier, model, hash)` match reuse the cached vector; rows that are missing, that have a different hash (metadata changed, e.g. a modified column comment), or that were embedded under a different model are re-embedded. The ledger is updated after each successful write so the next run sees the fresh state. **The ledger is a pipeline-owned artifact, not authority.** UC stays the only source of truth; Neo4j stays write-only during a run; the ledger is just a memo of "what we've already spent tokens on." It can be dropped and rebuilt from scratch at any time — the next run just re-embeds everything once. The reason the pipeline does not read Neo4j to answer "what's already embedded?" is that reading the sink couples the pipeline to sink state and makes partial-graph recovery a first-class concern; a Delta ledger gives the same skip behavior without that coupling and is queryable from a notebook for debugging ("show me every `Table` I've embedded in the last 30 days"). Because v5 already persists `embedding_text_hash` and `embedding_model` on every embedded node, Stage 6 is a purely additive feature — no contract break, no backfill.
- [ ] Per-run scope. Design an incremental refresh mode that reads `information_schema.tables.last_altered` and filters transforms to tables modified since the last successful run. Current design reads and re-writes the full catalog on every run.
- [ ] Endpoint throughput benchmark. Measure sustained `ai_query` throughput against the target catalog size and decide whether batch tuning or parallelism adjustments are needed.

## Open items deferred to later phases

- Re-embedding policy via Delta embedding ledger. Current design re-embeds every enabled node on every run; v5 persists `embedding_text_hash` + `embedding_model` on each node so Stage 6 can add the ledger additively.
- Per-run scope narrowing via `last_altered`. Current design processes the full catalog every run.
- `REFERENCES` relationship. v5 stubs it at zero coverage; the FK-join investigation resolves in a follow-up phase.
- Full embedding coverage across all node labels. v5 enables `Table` only; remaining labels roll out one at a time.
- Endpoint throughput benchmarking. v5 defers all performance work to a later phase.

## Out of scope

- Databricks Workflows scheduling. v5 remains a one-shot submit through `databricks-job-runner`.
- Multi-catalog ingestion. v5 handles a single configured catalog.
- Any change to the graph contract in `dbxcarta.contract`. Labels, relationship types, identifier generation, and contract version carry forward from v4 unchanged.
- Any change to the Neo4j Spark Connector or DBR version pair. The provisional pin from v4 carries forward.
