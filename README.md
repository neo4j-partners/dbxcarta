# dbxcarta

*Inspired by [neocarta](https://github.com/neo4j-field/neocarta).*

dbxcarta builds a metadata knowledge graph in Neo4j from Unity Catalog, designed as the semantic layer for **GraphRAG** workflows. Unity Catalog metadata, including table names, column descriptions, comments, and sampled values, is the unstructured content: each piece is embedded with a Databricks foundation model and stored as a vector property on its graph node. A downstream client, such as a Text2SQL agent, MCP tool, or schema-aware RAG pipeline, embeds a user question, runs a similarity search against the graph to find the most relevant schema nodes, then follows graph relationships to pull surrounding context: columns, values, and foreign-key references, all in a single retrieval step before the LLM call.

The graph enforces a stable, typed schema so the retrieval result is always structured: nodes carry dotted IDs (`catalog.schema.table.column`), typed labels, and explicit relationships, so the client always knows what it got back.

The output graph follows a standard shape so it can be consumed by downstream agents and MCP tooling that expect this schema:

- **Nodes**: `Database`, `Schema`, `Table`, `Column`, `Value`
- **Relationships**:
  - `(:Database)-[:HAS_SCHEMA]->(:Schema)`
  - `(:Schema)-[:HAS_TABLE]->(:Table)`
  - `(:Table)-[:HAS_COLUMN]->(:Column)`
  - `(:Column)-[:HAS_VALUE]->(:Value)`
  - `(:Column)-[:REFERENCES]->(:Column)` *(stubbed in v6; see Stage 6 in `dbxcarta-v6-plan.md`)*

Each node carries a stable dotted `id` such as `catalog.schema.table.column`, plus a `description` and, where applicable, an `embedding` vector for semantic similarity search.

## Architecture

### Build time: pipeline writes the graph

Unity Catalog metadata flows through a single Spark job that extracts, embeds, and loads every enabled node label into Neo4j. Embeddings are generated inside Spark via `ai_query` and materialized to a Delta staging table before the Neo4j write, so the embedding call happens exactly once per run.

```
┌──────────────────────────────────────────── BUILD TIME ────────────────────────────────────────────┐
│                                                                                                    │
│  Unity Catalog          Spark Pipeline                Delta Staging        Neo4j (Aura)            │
│  ─────────────          ──────────────                ─────────────        ────────────            │
│  information_schema ──► Preflight                                                                  │
│  (tables, columns,      Extract (SQL)  ──► Transform ──► ai_query() ──► materialize ──► MERGE     │
│   schemas, values)      builds DFs          embeds         (failOnError   Delta write    nodes +   │
│                                             per label       =false)                     vector idx │
│                                                                                                    │
└────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

### Query time: client retrieves schema context

A client performs two steps: a vector similarity search to find the most relevant nodes, then a graph traversal to expand that seed set into a full schema subgraph. The combination delivers both semantic relevance and structural completeness: the LLM receives the right tables *and* their columns, values, and relationships.

```
┌──────────────────────────────────────────── QUERY TIME ────────────────────────────────────────────┐
│                                                                                                    │
│  User question                                                                                     │
│       │                                                                                            │
│       ▼                                                                                            │
│  Client  (Text2SQL agent / MCP tool / schema-aware RAG pipeline)                                   │
│       │                                                                                            │
│       ├─ ① embed question ──► similarity search ──► top-k nodes (Table, Column, Value)            │
│       │                        (cosine, vector idx)                                                │
│       │                                                                                            │
│       └─ ② graph traversal ──► HAS_COLUMN, HAS_VALUE, REFERENCES ──► full schema subgraph         │
│                                                                                                    │
│       combined context ──► LLM ──► SQL / answer                                                   │
│                                                                                                    │
└────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

## Design

Everything runs inside Databricks — no external orchestrators, no local execution, no service accounts.

- **Single submission**: one script (`scripts/run_dbxcarta.py`) drives the whole pipeline in one Databricks Job. Phases are no longer dispatched via `DBXCARTA_JOB`; scope is controlled by per-label embedding flags in `.env`.
- **Spark**: extraction and transformation use PySpark DataFrames, so the pipeline scales to large catalogs without single-process bottlenecks.
- **Model Serving**: embeddings are generated in Spark via `ai_query` against a Databricks-hosted foundation model endpoint (`databricks-gte-large-en` by default), with `failOnError => false` so row-level failures are counted rather than thrown.
- **Materialize-once**: enriched node DataFrames are written to a Delta staging table between transform and load, so the failure-rate aggregation and the Neo4j write both consume the staged rows without re-invoking `ai_query`.
- **Neo4j Spark Connector**: bulk, partitioned writes from DataFrames. Relationship writes are `coalesce(1)` to avoid endpoint-node lock contention on Aura; `batch.size` is tuned via `DBXCARTA_NEO4J_BATCH_SIZE`.
- **Preflight**: grants and serving-endpoint permissions required by the enabled flags are checked before any extract runs; missing permissions fail the run fast.
- **Secrets**: Neo4j credentials live in a Databricks secret scope and are injected at job time, not read from a local file.
- **Metadata source**: Unity Catalog `information_schema` only — no pluggable multi-source connector layer.
- **Run observability**: every run emits a `RunSummary` to stdout, a timestamped JSON file in a UC Volume, and a row appended to a Delta table (`CREATE TABLE IF NOT EXISTS`, schema-merge on write) so history is queryable via SQL. The summary records per-label embedding attempts, successes, and failure rates alongside the threshold and the endpoint used.
- **`databricks-job-runner`**: CLI wrapper around the Databricks SDK that handles upload, submit, and cleanup.

See `dbxcarta-v6-plan.md` for the full staged implementation plan and `docs/best-practices.md` for the design rules (Spark / Databricks, Neo4j Spark Connector, project-level principles) that shape the pipeline.

## Quickstart

One-time setup:

```bash
uv sync
cp .env.sample .env          # fill in values
./setup_secrets.sh --profile <your-profile>
```

First green run (Table embeddings only; see Stage 3 in `dbxcarta-v6-plan.md`):

1. In `.env`, set `DBXCARTA_INCLUDE_EMBEDDINGS_TABLES=true` and leave all other `DBXCARTA_INCLUDE_EMBEDDINGS_*` flags off. Constrain `DBXCARTA_SCHEMAS` to a single small schema.
2. Bump `DBXCARTA_EMBEDDING_FAILURE_THRESHOLD=0.10` for small-fixture runs so a single transient endpoint failure doesn't abort. Restore to `0.05` once the run is green.
3. Wipe the target Neo4j instance (Aura console reset, or `MATCH (n) DETACH DELETE n`) so the bootstrap creates the vector index from scratch.
4. Build and upload the wheel, then submit (the `--upload` flag uploads every `scripts/*.py` before the run):

```bash
uv run dbxcarta upload --wheel
uv run dbxcarta submit --upload run_dbxcarta.py
```

Equivalent without `--upload` (useful when you know scripts haven't changed since the last upload):

```bash
uv run dbxcarta upload --wheel
uv run dbxcarta upload --all
uv run dbxcarta submit run_dbxcarta.py
```

The `submit` argument is a script name relative to `scripts/` — do not include the `scripts/` prefix. `submit` does not rebuild the wheel; re-run `upload --wheel` when `src/dbxcarta/` changes. `--upload` covers scripts only; the wheel must be uploaded separately.

5. Verify: `status=success`, per-label embedding failure rate `0.0%`, staging Delta table row count equals the in-scope node count, and all five embedding properties (`embedding`, `embedding_text`, `embedding_text_hash`, `embedding_model`, `embedded_at`) present on every in-scope `Table` node.
6. Submit again and confirm counts are idempotent. MERGE semantics guarantee no duplicate nodes.

Verification suites:

```bash
uv run pytest tests/schema_graph
uv run pytest tests/sample_values
uv run pytest tests/embeddings
```

Tail logs from any run:

```bash
uv run dbxcarta logs <run_id>
```

## Configuration

All pipeline behavior is controlled by `.env`. See `.env.sample` for the full set. The key knobs:

- **Scope**: `DBXCARTA_CATALOG` (required), `DBXCARTA_SCHEMAS` (comma-separated bare schema names; empty = all).
- **Sample values**: `DBXCARTA_INCLUDE_VALUES`, `DBXCARTA_SAMPLE_LIMIT`, `DBXCARTA_SAMPLE_CARDINALITY_THRESHOLD`.
- **Embeddings — per label**: `DBXCARTA_INCLUDE_EMBEDDINGS_{TABLES,COLUMNS,VALUES,SCHEMAS,DATABASES}`. Each is independent; turn them on one at a time.
- **Embeddings — endpoint**: `DBXCARTA_EMBEDDING_ENDPOINT` (default `databricks-gte-large-en`), `DBXCARTA_EMBEDDING_DIMENSION` (default `1024`), `DBXCARTA_EMBEDDING_FAILURE_THRESHOLD` (default `0.05`). The threshold is checked per label *and* in aggregate; either trip fails the run.
- **Staging and Neo4j tuning**: `DBXCARTA_STAGING_PATH` (Delta staging root for materialize-once; defaults under the configured volume), `DBXCARTA_NEO4J_BATCH_SIZE` (default `20000`).
- **Run summary**: `DBXCARTA_SUMMARY_VOLUME`, `DBXCARTA_SUMMARY_TABLE`.

For the first green run, enable `DBXCARTA_INCLUDE_EMBEDDINGS_TABLES=true` only and constrain `DBXCARTA_SCHEMAS` to a single small schema. Expand coverage one label at a time after verifying failure rates and the vector index.

## Client quick start

The client is a batch evaluation job that measures whether GraphRAG retrieval from the Neo4j semantic layer produces more accurate SQL than a raw schema dump or no context at all. It runs three retrieval arms (no-context, schema-dump, GraphRAG hybrid) against a curated questions file and records execution rates per arm in a Delta run-summary table.

Prerequisites: the server pipeline must have run at least once with `DBXCARTA_INCLUDE_EMBEDDINGS_TABLES=true` so the `table_embedding` vector index is ONLINE.

**1. Set client variables in `.env`:**

```bash
DBXCARTA_CHAT_ENDPOINT=<model-serving-endpoint-name>   # required; no default
# Optional — defaults shown
DBXCARTA_EMBED_ENDPOINT=databricks-gte-large-en
DBXCARTA_CLIENT_ARMS=no_context,schema_dump,graph_rag
DBXCARTA_CLIENT_TOP_K=5
DBXCARTA_CLIENT_TIMEOUT_SEC=30
DBXCARTA_CLIENT_SERVERLESS=false                       # set true to run on serverless instead of the cluster
```

**2. Upload and submit:**

```bash
uv run dbxcarta upload --wheel
uv run dbxcarta submit --upload run_dbxcarta_client.py
```

**3. Verify:** the run summary printed to the job log shows per-arm `executed` counts and `non_empty` rates. A JSON artifact lands in `DBXCARTA_SUMMARY_VOLUME` and a row is appended to `DBXCARTA_SUMMARY_TABLE`.

Run the client test suite locally:

```bash
uv run pytest tests/client
```

**Questions file.** `examples/client/questions/questions.json` contains 20 curated questions spanning single-table counts, filter-with-literal, multi-table joins, and FK-walk queries. All 20 carry `reference_sql` for ground-truth grading. Edit this file to add questions for your catalog before the first run; the distribution of question types is what determines whether the harness can distinguish the retrieval arms.

**Arms.** Run a single arm to iterate without paying the cost of the others:

```bash
DBXCARTA_CLIENT_ARMS=graph_rag uv run dbxcarta submit --upload run_dbxcarta_client.py
```

## Automated end-to-end test

`scripts/run_autotest.py` is a self-contained harness that provisions a known fixture schema in Unity Catalog, runs the full pipeline against it, and asserts the resulting `RunSummary` JSON meets expected thresholds. It produces a dated JSON result file in the configured volume and exits non-zero on any failure.

**Prerequisites** — the following must be set in `.env` (or the environment):

| Variable | Purpose |
|----------|---------|
| `DATABRICKS_PROFILE` | Databricks CLI profile for auth |
| `DATABRICKS_CLUSTER_ID` or `DATABRICKS_COMPUTE_MODE=serverless` | Compute for the pipeline job |
| `DATABRICKS_WAREHOUSE_ID` | SQL warehouse used for schema setup |
| `DBXCARTA_CATALOG` | Must be `dbxcarta-catalog` |
| `DBXCARTA_SUMMARY_VOLUME` | UC Volume path where `RunSummary` JSON and autotest results are written |

**Run:**

```bash
uv run python scripts/run_autotest.py
```

**Phases:**

| Phase | What it does |
|-------|-------------|
| 0 — Preflight | Verifies workspace connectivity and that the SQL warehouse is reachable |
| 1 — Unit test gate | Runs the fast offline pytest suite; aborts if any test fails |
| 2 — Schema setup | Tears down and recreates the fixture schemas (`dbxcarta_test_{sales,inventory,hr,events}`) in `dbxcarta-catalog` using `tests/fixtures/setup_test_catalog.sql` |
| 3 — Ingest run | Builds and uploads the wheel, uploads scripts, submits `run_dbxcarta.py`, waits for `SUCCESS`, and downloads the `RunSummary` JSON |
| 4 — Assertions | Validates the `RunSummary`: `status=success`, `error=null`, `schemas >= 4`, `tables >= 19`, `fk_declared >= 16`, `fk_edges >= 16`, `neo4j_counts` non-empty |
| 5 — Output JSON | Writes `autotest_results_<ts>.json` to `DBXCARTA_SUMMARY_VOLUME/autotest/` and locally to `outputs/` (git-ignored) |

The fixture covers all the structural edge cases:

- **Cross-schema FKs**: sales → hr, sales → inventory
- **Self-referential FK**: `employees.manager_id`
- **Composite PK / associative table**: `product_suppliers`
- **Complex column types**: `STRUCT`, `ARRAY`, `MAP`, `VARIANT`, `BINARY`
- **External schema** (`dbxcarta_test_external`): requires a UC Volume path; excluded from ingest but included in teardown

**Notes:**
- The harness locates the `RunSummary` via a before/after volume diff — `DATABRICKS_JOB_RUN_ID` is not set for one-time `runs.submit()` jobs, so the file is always written as `dbxcarta_local_<ts>.json`.
- Schema setup and teardown are idempotent — re-running always starts from a clean state.
- Unit tests run with `--ignore=tests/schema_graph --ignore=tests/sample_values --ignore=tests/integration` to exclude slow live-catalog suites.
- Results are also written locally to `outputs/autotest_results_<ts>.json` (git-ignored) for quick inspection without going back to the volume.

## Upload and submit

**`upload`**
- `--wheel` — builds the package, bumps the patch version, and uploads the wheel to `DATABRICKS_VOLUME_PATH/wheels/`. Re-run whenever `src/dbxcarta/` changes.
- `--all` — copies every `scripts/*.py` to the workspace. Re-run whenever `scripts/` changes.

**`submit <script>`**

The script name is relative to `scripts/`. Scripts named `run_dbxcarta*` auto-attach the latest uploaded wheel. All non-Databricks `.env` variables are forwarded to the job.

- `--upload` — uploads `scripts/*.py` before submitting, replacing a separate `upload --all` step.
- `--no-wait` — returns immediately with the run ID.
- `--compute {cluster,serverless}` — overrides `DATABRICKS_COMPUTE_MODE` for this run.
