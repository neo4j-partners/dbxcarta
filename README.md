# dbxcarta

A Databricks-native pipeline for generating a metadata knowledge graph in Neo4j from Unity Catalog. The graph is intended as a semantic layer for query generation and routing workflows, such as Text2SQL agents and retrieval over schema metadata.

The output graph follows a standard shape so it can be consumed by downstream agents and MCP tooling that expect this schema:

- **Nodes**: `Database`, `Schema`, `Table`, `Column`, `Value`
- **Relationships**:
  - `(:Database)-[:HAS_SCHEMA]->(:Schema)`
  - `(:Schema)-[:HAS_TABLE]->(:Table)`
  - `(:Table)-[:HAS_COLUMN]->(:Column)`
  - `(:Column)-[:HAS_VALUE]->(:Value)`
  - `(:Column)-[:REFERENCES]->(:Column)`

Each node carries a stable dotted `id` such as `catalog.schema.table.column`, plus a `description` and, where applicable, an `embedding` vector for semantic similarity search.

## Design

Everything runs inside Databricks — no external orchestrators, no local execution, no service accounts.

- **Jobs**: each phase runs as a Databricks Job, not a local script. Runs are reproducible, logged, and observable in the workspace.
- **Spark**: extraction and transformation use PySpark DataFrames, so the pipeline scales to large catalogs without single-process bottlenecks.
- **Model Serving**: embeddings are generated via Databricks-hosted foundation models, keeping inference inside the workspace and governed by Unity Catalog permissions.
- **Neo4j Spark Connector**: bulk, partitioned writes from DataFrames — no per-row `MERGE` statements from the Python driver.
- **Secrets**: Neo4j credentials live in a Databricks secret scope and are injected at job time, not read from a local file.
- **Metadata source**: Unity Catalog `information_schema` only — no pluggable multi-source connector layer.
- **Run observability**: every run emits a `RunSummary` to stdout, a timestamped JSON file in a UC Volume, and a row appended to a Delta table so history is queryable via SQL.
- **`databricks-job-runner`**: CLI wrapper around the Databricks SDK that handles upload, submit, and cleanup. A single dispatcher script runs all phases; the active phase is controlled by `DBXCARTA_JOB=schema|sample|embeddings` in `.env`.

## Architecture

Spark jobs read from Unity Catalog `information_schema` and a SQL warehouse, then write to Neo4j through the Neo4j Spark Connector. Credentials come from a Databricks secret scope at job runtime; every run appends a summary to a UC Volume and a Delta table.

The pipeline has three phases:

- **Schema Graph** (`DBXCARTA_JOB=schema`): builds the structural graph — `Database`, `Schema`, `Table`, and `Column` nodes with `HAS_SCHEMA`, `HAS_TABLE`, `HAS_COLUMN`, and `REFERENCES` edges — from `information_schema`.
- **Sample Values** (`DBXCARTA_JOB=sample`): adds `Value` nodes and `HAS_VALUE` edges by sampling distinct values from STRING and BOOLEAN columns. High-cardinality columns are skipped; threshold and sample limit are configurable.
- **Embeddings** (`DBXCARTA_JOB=embeddings`): *not yet implemented* — will generate vector embeddings for node descriptions via Databricks Model Serving and write them back onto the `embedding` property.

```
Unity Catalog ──► Spark Job ──► Transform ──► Neo4j Spark Connector ──► Neo4j Graph
(information_schema                (id contract,                              ▲
 + SQL warehouse                    DataFrames)                               │
 for sample values)                                                           │
                                                                     Embeddings Job
                                                              (foundation model APIs)
```

## Quickstart

```bash
uv sync
cp .env.sample .env          # fill in values
./setup_secrets.sh --profile <your-profile>

uv run dbxcarta upload --wheel
uv run dbxcarta upload --all
uv run dbxcarta submit run_dbxcarta.py   # DBXCARTA_JOB=schema in .env

uv run pytest tests/schema_graph
```

To run Phase 2, set `DBXCARTA_JOB=sample` in `.env` and resubmit:

```bash
uv run dbxcarta submit run_dbxcarta.py
uv run pytest tests/sample_values
```

Tail logs from any run:

```bash
uv run dbxcarta logs <run_id>
```

## Upload and submit

`upload --wheel` builds the `dbxcarta` package, bumps the patch version in `pyproject.toml`, and uploads the wheel to `DATABRICKS_VOLUME_PATH/wheels/`. `upload --all` copies every `*.py` in `scripts/` to `DATABRICKS_WORKSPACE_DIR/scripts/` in the workspace. Re-run `upload --wheel` when `src/dbxcarta/` changes; re-run `upload --all` when `scripts/` changes.

`submit` does not upload — it runs whatever is already in the workspace. Because the script name starts with `run_dbxcarta`, the runner auto-attaches the latest uploaded wheel so the cluster can `import dbxcarta`. All non-Databricks variables from `.env` (including `DBXCARTA_JOB`) are forwarded to the job as arguments, so the dispatcher picks the right phase without any cluster-side config file.

Flags on `submit`:
- `--no-wait` — return immediately with the run ID instead of blocking until completion
- `--compute {cluster,serverless}` — override `DATABRICKS_COMPUTE_MODE` for a single submission
