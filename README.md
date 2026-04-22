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

## Databrick Native Design Decisions

This project is designed to be run on Databricks and leverage the Databricks ecosystem. The core design choices:

1. **Run as Databricks Jobs**: each phase is a job entry point orchestrated and scheduled through the Databricks Jobs scheduler, not a local Python script. Runs are reproducible, logged, and observable in the workspace.
2. **Use Spark for ETL**: extraction and transformation use PySpark DataFrames on Databricks compute rather than row-by-row Python, so the pipeline scales to large Unity Catalogs without a single-process bottleneck.
3. **Use Databricks Model Serving endpoints**: embeddings are generated against Databricks-hosted foundation models served through Model Serving, keeping inference inside the workspace and governed by Unity Catalog permissions instead of calling an external embeddings API.
4. **Leverage Databricks compute infrastructure**: job clusters or serverless compute, SQL warehouses for sample-value queries, Unity Catalog as the single metadata source, UC Volumes for run-summary artifacts, and a Delta run-summary table for history.
5. **Use `databricks-job-runner` for job lifecycle**: project scripts, wheels, and jobs are uploaded, submitted, monitored, and cleaned up through the `databricks-job-runner` CLI rather than bespoke SDK code. It wraps the Databricks Python SDK into a reusable `Runner` that exposes `upload`, `submit`, `validate`, `logs`, and `clean` subcommands, plus Unity Catalog helpers for catalogs, schemas, and volumes. The project's `cli.py` simply configures a `Runner`, and `scripts/` contains a separate entry-point script for each phase (`run_dbxcarta_schema.py`, `run_dbxcarta_sample.py`, `run_dbxcarta_embeddings.py`).

Additional differences worth calling out:

- **Write path**: the Neo4j Spark Connector performs bulk, partitioned writes from DataFrames, rather than the neo4j Python driver issuing per-row `MERGE` statements.
- **Secrets**: Neo4j credentials live in a Databricks secret scope and are read at job time via `dbutils.secrets`, not from a local `.env` file.
- **Authentication**: Databricks-native using a workspace profile, OAuth, and `dbutils`, with no separate service-account or local gcloud setup.
- **Metadata source**: directly targets `<catalog>.information_schema` plus UC system tables, rather than a pluggable multi-source connector layer.
- **Run observability**: every run emits a typed `RunSummary` to stdout, a timestamped JSON file in a UC Volume, and an append to a Delta table so history is queryable via SQL.

## Proposed Architecture

Extraction, transformation, and loading execute as Spark jobs on Databricks compute, reading from Unity Catalog `information_schema` and a SQL warehouse for sample values, and writing to Neo4j through the Neo4j Spark Connector. Credentials are resolved at job runtime from a Databricks secret scope (default `dbxcarta-neo4j`); run metadata is emitted to stdout, a UC Volume JSON file, and a Delta run-summary table.

The pipeline is organized into three phases, each a separate submitted script:

- **Phase 1, Schema Graph** (`run_dbxcarta_schema.py`): reads `schemata`, `tables`, and `columns` from `<catalog>.information_schema`, builds node and relationship DataFrames using a shared id contract in `dbxcarta.contract`, and writes `Database`, `Schema`, `Table`, `Column` nodes plus `HAS_SCHEMA`, `HAS_TABLE`, `HAS_COLUMN`, and `REFERENCES` foreign-key edges via the Neo4j Spark Connector.
- **Phase 2, Sample Values** (`run_dbxcarta_sample.py`): samples distinct column values from a Databricks SQL warehouse and writes `Value` nodes and `HAS_VALUE` edges, giving the graph representative data for each column.
- **Phase 3, Embeddings** (`run_dbxcarta_embeddings.py`): generates vector embeddings for node descriptions via Databricks Model Serving and writes them back onto the `embedding` property, enabling semantic similarity retrieval.

```
Unity Catalog ──► Spark Job ──► Transform ──► Neo4j Spark Connector ──► Neo4j Graph
(information_schema                (id contract,                              ▲
 + SQL warehouse                    DataFrames)                               │
 for sample values)                                                           │
                                                                     Embeddings Job
                                                              (foundation model APIs)
```

## Quickstart

### 1. Install

```bash
uv sync
```

### 2. Configure

Create `.env` in the repo root (not committed) with both the job-time settings and the Neo4j credentials:

```
DATABRICKS_PROFILE=azure-rk-knight
DATABRICKS_SECRET_SCOPE=dbxcarta-neo4j
DBXCARTA_CATALOG=<your_catalog>
DBXCARTA_SCHEMAS=<optional,comma,separated>
DBXCARTA_SUMMARY_VOLUME=/Volumes/<catalog>/<schema>/<volume>
DBXCARTA_SUMMARY_TABLE=<catalog>.<schema>.dbxcarta_run_summary
DATABRICKS_WAREHOUSE_ID=<warehouse_id>

NEO4J_URI=neo4j+s://<host>:7687
NEO4J_USERNAME=neo4j
NEO4J_PASSWORD=<password>
```

Push the Neo4j credentials to the Databricks secret scope:

```bash
./setup_secrets.sh --profile azure-rk-knight
```

### 3. Upload and run the schema graph job

Build and upload the `dbxcarta` wheel, upload the phase scripts, then submit:

```bash
uv run dbxcarta upload --wheel
uv run dbxcarta upload --all
uv run dbxcarta submit run_dbxcarta_schema.py
```

- `upload --wheel` builds `dbxcarta` with `uv build` and uploads the wheel to the UC Volume. Because the scripts follow the `run_dbxcarta_*` naming convention, `submit` auto-attaches the latest uploaded wheel to the job so the cluster can `import dbxcarta`. Re-run `upload --wheel` whenever code under `src/dbxcarta/` changes.
- `upload --all` copies every `*.py` in `scripts/` to the configured workspace directory. Re-run it whenever `scripts/` changes.
- `submit` runs the last-uploaded script — it does not upload on its own.

To run the other phases:

```bash
uv run dbxcarta submit run_dbxcarta_sample.py
uv run dbxcarta submit run_dbxcarta_embeddings.py
```

Flags on `submit`:
- `--no-wait` — return immediately with the run ID instead of blocking until completion
- `--compute {cluster,serverless}` — override `DATABRICKS_COMPUTE_MODE` from `.env`

Tail logs for a submitted run:

```bash
uv run dbxcarta logs <run_id>
```

### 4. Verify

After a successful run, execute the pytest suite against the live Neo4j graph and the latest run-summary JSON:

```bash
uv run pytest tests/schema_graph
```
