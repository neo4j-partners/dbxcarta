# dbxcarta

A Databricks-native pipeline for generating a metadata knowledge graph in Neo4j from Unity Catalog. The graph is intended as a semantic layer for query generation and routing workflows, such as Text2SQL agents and retrieval over schema metadata.

The output graph follows a standard shape so it can be consumed by downstream agents and MCP tooling that expect this schema:

- **Nodes**: `Database`, `Schema`, `Table`, `Column`, `Value`
- **Relationships**:
  - `(:Database)-[:HAS_SCHEMA]->(:Schema)`
  - `(:Schema)-[:HAS_TABLE]->(:Table)`
  - `(:Table)-[:HAS_COLUMN]->(:Column)`
  - `(:Column)-[:HAS_VALUE]->(:Value)`
  - `(:Column)-[:REFERENCES]->(:Column)` *(stubbed in v6; see Stage 6 in `dbxcarta-v6-plan.md`)*

Each node carries a stable dotted `id` such as `catalog.schema.table.column`, plus a `description` and, where applicable, an `embedding` vector for semantic similarity search.

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

## Architecture

```
Unity Catalog ──► Preflight ──► Extract ──► Transform ──► Delta staging ──► Neo4j Spark Connector ──► Neo4j Graph
(information_schema   (grants,    (Spark SQL)  (build DFs,    (materialize        (MERGE, coalesce(1)
 + source tables       endpoint                 ai_query       once — prevents     on rels, batch.size)
 for sample values)    permissions)             embeddings)    re-invocation)
```

One submission builds the schema graph, adds `Value` nodes and `HAS_VALUE` edges, and attaches embeddings for every enabled label in a single pass. Turning a feature on or off is a `.env` flag, not a separate job.

See `dbxcarta-v6-plan.md` for the full staged implementation plan and `docs/best-practices.md` for the design rules (Spark / Databricks, Neo4j Spark Connector, project-level principles) that shape the pipeline.

## Quickstart

One-time setup:

```bash
uv sync
cp .env.sample .env          # fill in values
./setup_secrets.sh --profile <your-profile>
```

First green run (Table embeddings only — see `dbxcarta-v6-plan.md` Stage 3):

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
6. Submit again and confirm counts are idempotent (MERGE semantics — no duplicate nodes).

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

## Upload and submit

`upload --wheel` builds the `dbxcarta` package, bumps the patch version in `pyproject.toml`, and uploads the wheel to `DATABRICKS_VOLUME_PATH/wheels/`. `upload --all` copies every `*.py` in `scripts/` to `DATABRICKS_WORKSPACE_DIR/scripts/` in the workspace. Re-run `upload --wheel` when `src/dbxcarta/` changes; re-run `upload --all` when `scripts/` changes.

`submit` runs whatever is already in the workspace, with one shortcut: `submit --upload` (added in `databricks-job-runner 0.4.6`) uploads every `scripts/*.py` right before the run, so you can fold the script upload into the submit step. It does not cover the wheel — always re-run `upload --wheel` after editing `src/dbxcarta/`. The `script` positional is a name relative to `scripts/`; the runner prepends `scripts/` and resolves it against `DATABRICKS_WORKSPACE_DIR`. Because the script name starts with `run_dbxcarta`, the runner auto-attaches the latest uploaded wheel so the cluster can `import dbxcarta`. All non-Databricks variables from `.env` are forwarded to the job as arguments.

Flags on `submit`:
- `--upload` — upload every `scripts/*.py` before submitting (requires `databricks-job-runner >= 0.4.6`)
- `--no-wait` — return immediately with the run ID instead of blocking until completion
- `--compute {cluster,serverless}` — override `DATABRICKS_COMPUTE_MODE` for a single submission
