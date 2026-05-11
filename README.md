# dbxcarta

*Inspired by [neocarta](https://github.com/neo4j-field/neocarta).*

dbxcarta builds a metadata knowledge graph in Neo4j from Unity Catalog. The graph serves as a semantic layer for **GraphRAG** workflows: a Text2SQL agent, MCP tool, or schema-aware RAG pipeline queries the graph at runtime to retrieve the schema context it needs before generating SQL.

The build pipeline extracts Unity Catalog metadata, including table names, column descriptions, comments, and sampled values, embeds each piece using a Databricks foundation model, and writes the result to Neo4j as typed nodes with vector properties. At query time, a client embeds a user question, runs a similarity search to find the most relevant schema nodes, then follows graph relationships to expand that seed set into a full schema subgraph: columns, values, and foreign-key references, all in one retrieval step before the LLM call.

The graph follows a stable, typed schema:

- **Nodes**: `Database`, `Schema`, `Table`, `Column`, `Value`
- **Relationships**:
  - `(:Database)-[:HAS_SCHEMA]->(:Schema)`
  - `(:Schema)-[:HAS_TABLE]->(:Table)`
  - `(:Table)-[:HAS_COLUMN]->(:Column)`
  - `(:Column)-[:HAS_VALUE]->(:Value)`
  - `(:Column)-[:REFERENCES]->(:Column)` from declared and inferred foreign keys

Each node carries a stable dotted `id` such as `catalog.schema.table.column`, a `description`, and where applicable an `embedding` vector for semantic similarity search.

## Core vs examples

Core dbxcarta does not create Lakehouse tables. It builds a semantic layer over
an existing Unity Catalog scope configured by `DBXCARTA_CATALOG` and
`DBXCARTA_SCHEMAS`.

Companion examples show how to point dbxcarta at a known upstream project:

- `examples/finance-genie/` pairs dbxcarta with
  `/Users/ryanknight/projects/databricks/graph-on-databricks/finance-genie`.
  Finance Genie creates the finance Lakehouse tables and Gold graph-enriched
  features; dbxcarta creates the Neo4j semantic layer over those tables.

Print the Finance Genie dbxcarta overlay:

```bash
uv run dbxcarta preset finance-genie --print-env
```

Check whether the expected Finance Genie tables are present:

```bash
uv run dbxcarta preset finance-genie --check-ready
```

## Quickstart

This path creates the demo Unity Catalog schemas, builds the Neo4j semantic
layer, then runs the demo client against that graph.

### 1. Configure the project

```bash
uv sync
cp .env.sample .env
```

Open `.env` and replace the placeholders. Keep the demo defaults already
organized in `.env.sample`: the four `dbxcarta_test_*` schemas, table and
column embeddings enabled, values enabled, `DBXCARTA_CLIENT_ARMS=graph_rag`,
and `DBXCARTA_CLIENT_QUESTIONS` pointing at the UC Volume copy of
`demo_questions.json`.

Use an existing UC catalog, schema, and volume, or create them if your
principal has permission:

```bash
uv run dbxcarta schema create <catalog>.<schema>
uv run dbxcarta volume create <catalog>.<schema>.<volume>
```

Create the Neo4j secrets in Databricks. These keys are read from the secret
scope at job runtime.

```bash
./setup_secrets.sh --profile <your-profile>
```

If the Neo4j database already has dbxcarta data, clear it before the first demo
run so constraints and vector indexes are created cleanly:

```cypher
MATCH (n) DETACH DELETE n;
```

### 2. Create the demo source schemas

`scripts/run_demo.py` uses `DBXCARTA_CATALOG`, `DATABRICKS_WAREHOUSE_ID`, and
`DATABRICKS_VOLUME_PATH` from `.env`. It creates and populates the demo source
schemas in Unity Catalog.

```bash
uv run python scripts/run_demo.py
```

### 3. Build the semantic layer

Upload the package wheel, upload the demo questions file to the configured UC
Volume, then submit the ingest job. The `submit --upload` flag uploads
`scripts/*.py`; it does not rebuild the wheel.

```bash
uv run dbxcarta upload --wheel
uv run dbxcarta upload --data tests/fixtures
uv run dbxcarta submit --upload run_dbxcarta.py
```

The ingest run should finish with `status=success`. It writes the graph to
Neo4j, writes JSON run output under `DBXCARTA_SUMMARY_VOLUME`, and appends a
row to `DBXCARTA_SUMMARY_TABLE`.

### 4. Run the demo client

The demo client embeds each question, retrieves context from the Neo4j semantic
layer, asks the configured chat endpoint for SQL, executes the SQL on the
warehouse, and writes a client run summary.

```bash
uv run dbxcarta submit --upload run_dbxcarta_client.py
```

Check the job output for per-arm `executed` and `non_empty` rates:

```bash
uv run dbxcarta logs <run_id>
```

### 5. Verify and clean up

Run local tests:

```bash
uv run pytest
```

Run live integration tests after a successful ingest:

```bash
uv run pytest tests/integration -m live
```

Re-run structural verification against the most recent successful run summary:

```bash
uv run dbxcarta verify
```

Remove the demo schemas when you are done:

```bash
uv run python scripts/run_demo.py --teardown
```

## Architecture

### Graph schema

```text
+----------------+     HAS_SCHEMA      +----------------+
| Database       | -------------------> | Schema         |
| id             |                      | id             |
| name           |                      | name           |
+----------------+                      +----------------+
                                                  |
                                                  | HAS_TABLE
                                                  v
                                         +----------------+
                                         | Table          |
                                         | id             |
                                         | name           |
                                         | table_type     |
                                         | embedding      |
                                         +----------------+
                                                  |
                                                  | HAS_COLUMN
                                                  v
                                         +----------------+
                                         | Column         |
                                         | id             |
                                         | name           |
                                         | data_type      |
                                         | embedding      |
                                         +----------------+
                                            |          |
                                            |          | REFERENCES
                                            |          v
                                            |   +----------------+
                                            |   | Column         |
                                            |   | confidence     |
                                            |   | source         |
                                            |   | criteria       |
                                            |   +----------------+
                                            |
                                            | HAS_VALUE
                                            v
                                         +----------------+
                                         | Value          |
                                         | id             |
                                         | value          |
                                         | count          |
                                         | embedding      |
                                         +----------------+
```

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

See `docs/best-practices.md` for the design rules (Spark / Databricks, Neo4j Spark Connector, project-level principles) that shape the pipeline.

## Configuration

All pipeline and client behavior is controlled by `.env`. Copy
`.env.sample`, fill in the placeholders, and use the comments in that file as
the configuration reference. The sample is organized by Databricks auth,
workspace locations, compute, secrets, Unity Catalog scope, run artifacts,
embeddings, Neo4j write tuning, and client settings.

## Demo Client Details

The client is a batch evaluation job for the Neo4j semantic layer. In the
quickstart it runs the `graph_rag` arm against `tests/fixtures/demo_questions.json`
after that file is uploaded to `DATABRICKS_VOLUME_PATH`.

The question set exercises cross-schema joins (`sales` to `inventory` and
`sales` to `hr`), self-referential FKs, composite FK paths, and intra-schema
event analytics. For your own catalog, upload a replacement questions JSON file
to the UC Volume and point `DBXCARTA_CLIENT_QUESTIONS` at it.

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
- Unit tests run with `--ignore=tests/integration` to exclude slow live-catalog suites.
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
