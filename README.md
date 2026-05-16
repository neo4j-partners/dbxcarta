# dbxcarta

*Inspired by [neocarta](https://github.com/neo4j-field/neocarta).*

dbxcarta builds a metadata knowledge graph in Neo4j from Unity Catalog. The graph serves as a semantic layer for GraphRAG workflows: Text2SQL agents, MCP tools, and schema-aware RAG pipelines query the graph at runtime to retrieve the schema context they need before generating SQL.

**What the build pipeline does:**

- Extracts Unity Catalog metadata: table names, column descriptions, comments, and sampled values
- Embeds each piece using a Databricks foundation model
- Writes the result to Neo4j as typed nodes with vector properties

**What happens at query time:**

- A client embeds a user question
- Runs a similarity search to find the most relevant schema nodes
- Follows graph relationships to expand that seed set into a full schema subgraph: columns, values, and foreign-key references in one retrieval step before the LLM call

## Graph schema

The graph follows a stable, typed schema:

**Nodes:** `Database`, `Schema`, `Table`, `Column`, `Value`

**Relationships:**
- `(:Database)-[:HAS_SCHEMA]->(:Schema)`
- `(:Schema)-[:HAS_TABLE]->(:Table)`
- `(:Table)-[:HAS_COLUMN]->(:Column)`
- `(:Column)-[:HAS_VALUE]->(:Value)`
- `(:Column)-[:REFERENCES]->(:Column)` — from declared and inferred foreign keys

Each node carries:
- A stable dotted `id` such as `catalog.schema.table.column`
- A `description`
- An `embedding` vector for semantic similarity search (where applicable)

## Use dbxcarta as a library

dbxcarta is split into separate Spark and client packages. There is no
top-level `dbxcarta` import surface; consumers import the layer they need.

| Capability | Distribution | Import path | Console script |
|------------|--------------|-------------|----------------|
| Databricks Spark ingest, graph contract, IDs, validators, verification, preset runner | `dbxcarta-spark` | `dbxcarta.spark` | `dbxcarta`, `dbxcarta-ingest` |
| Retrieval runtime and Text2SQL eval harness | `dbxcarta-client` | `dbxcarta.client` | `dbxcarta-client` |

**Layer responsibilities:**

- **Spark** owns the concrete Unity Catalog ingest implementation, the graph contract, verification, Databricks validators, preset capability protocols, and the operational CLI.
- **Client** owns retrieval primitives and the Text2SQL eval harness.

Code running with Databricks Spark access can construct `SparkIngestSettings` and call the ingest implementation directly:

```python
from dbxcarta.spark import SparkIngestSettings, run_dbxcarta

settings = SparkIngestSettings(
    dbxcarta_catalog="analytics",
    dbxcarta_schemas="finance,customer_success",
    dbxcarta_summary_volume="/Volumes/analytics/ops/dbxcarta/summaries",
    dbxcarta_summary_table="analytics.ops.dbxcarta_runs",
    dbxcarta_include_embeddings_tables=True,
    dbxcarta_include_embeddings_columns=True,
)

run_dbxcarta(settings=settings)
```

The no-argument form, `run_dbxcarta()`, is the Databricks wheel entrypoint. It loads `SparkIngestSettings` from environment variables and runs the same pipeline.

## Examples and presets

Companion examples show how to package reusable configuration and demo data for
known upstream projects. Each example is its own Python package that depends on
the relevant dbxcarta distributions as normal pip dependencies and exposes a
module-level `preset` object. The Spark package provides the preset protocol and
loader; examples own their concrete preset implementations.

**Available examples:**

- `examples/integration/finance-genie/` pairs dbxcarta with Finance Genie. Finance Genie creates the finance Lakehouse tables and Gold graph-enriched features; dbxcarta creates the Neo4j semantic layer over those tables.
- `examples/integration/schemapile/` materializes a reproducible SchemaPile slice as Delta tables, writes the generated UC schema list to `.env.generated`, generates a SQL-validated question set, and exposes `dbxcarta_schemapile_example:preset`.
- `examples/integration/dense-schema/` generates a synthetic 500- or 1000-table single schema for stress testing schema-context retrieval. It shares the same preset pattern and exposes `dbxcarta_dense_schema_example:preset`.
- `examples/demos/` is reserved for walkthroughs that are not migration-gate consumers.

### Preset workflow

A **preset** is a reusable configuration adapter published by an external package. It bundles three things together:

1. **Environment overlay:** the set of environment variables needed to run dbxcarta against a specific upstream project or schema
2. **Readiness check** (optional): logic to verify the expected tables and resources exist before running ingest
3. **Question upload** (optional): logic to push a demo question set to the configured UC Volume

**Why use a preset?**

Without a preset, every developer running dbxcarta against the same upstream project (such as Finance Genie) must manually collect and maintain the correct environment variables. A preset packages that knowledge once and makes it repeatable. Install the example package, then reference the preset by import path instead of managing env vars by hand.

**How a preset is identified:**

A preset is referenced by its import path, for example `your_pkg.module:preset`. The left side is a Python module path; the right side is the name of the `preset` object within that module. The CLI resolves this at runtime by importing the module and reading the object.

**The preset interface:**

| Method | Required | CLI flag | Purpose |
|--------|----------|----------|---------|
| `env()` | Yes | `--print-env` | Returns the environment variable overlay for this preset |
| `readiness(ws, warehouse_id)` | No | `--check-ready` | Checks that expected tables and resources are present in the workspace |
| `upload_questions(ws)` | No | `--upload-questions` | Uploads the preset's demo question set to the configured UC Volume |

**Step-by-step workflow:**

1. Install the example package alongside dbxcarta:
   ```bash
   uv pip install -e examples/integration/finance-genie/
   ```

2. Print the environment overlay the preset provides:
   ```bash
   uv run dbxcarta preset dbxcarta_finance_genie_example:preset --print-env
   ```

3. Verify the expected source tables exist in the workspace before running ingest:
   ```bash
   uv run dbxcarta preset dbxcarta_finance_genie_example:preset --check-ready --strict-optional
   ```

4. Upload the preset's demo question set to the configured UC Volume:
   ```bash
   uv run dbxcarta preset dbxcarta_finance_genie_example:preset --upload-questions
   ```

**Generated examples** (schemapile, dense-schema) follow the same pattern, but the materializer owns the source schemas. The preset reads `DBXCARTA_SCHEMAS` at command runtime, so you must set that variable before running any preset command:

```bash
uv pip install -e examples/integration/schemapile/
# Source or copy examples/integration/schemapile/.env.generated first so DBXCARTA_SCHEMAS is set.
uv run dbxcarta preset dbxcarta_schemapile_example:preset --print-env

uv pip install -e examples/integration/dense-schema/
# Set DBXCARTA_SCHEMAS to the generated dense schema, e.g. dense_500.
uv run dbxcarta preset dbxcarta_dense_schema_example:preset --print-env
```

Keep the generated overlay in the environment (or copy its `DBXCARTA_SCHEMAS=...` line into `.env`) before running `--print-env`, `--check-ready`, `--upload-questions`, ingest, or client jobs.

See the example READMEs for the full setup flows:

- [`examples/integration/finance-genie/README.md`](examples/integration/finance-genie/README.md)
- [`examples/integration/schemapile/README.md`](examples/integration/schemapile/README.md)

## Migration notes

This repository uses a clean boundary cutover. Old top-level imports are deleted instead of re-exported.

| Old path | New path |
|----------|----------|
| `from dbxcarta import run_dbxcarta` | `from dbxcarta.spark import run_dbxcarta` |
| `from dbxcarta import Settings` | `from dbxcarta.spark import SparkIngestSettings` |
| `from dbxcarta import run_client` | `from dbxcarta.client.eval import run_client` |
| `dbxcarta.contract` | `dbxcarta.spark.contract` |
| `dbxcarta.databricks` | `dbxcarta.spark.databricks` for operational helpers; `dbxcarta.client.databricks` for client workflow helpers |
| `dbxcarta.verify` | `dbxcarta.spark.verify` |
| `dbxcarta.ingest.*` | `dbxcarta.spark.ingest.*` |
| `dbxcarta.ingest.pipeline` | `dbxcarta.spark.run` |
| `dbxcarta.client.client` | `dbxcarta.client.eval.run` |
| `dbxcarta.entrypoints.ingest` | `dbxcarta.spark.entrypoint` |
| `dbxcarta.entrypoints.client` | `dbxcarta.client.eval.entrypoint` |
| `dbxcarta.presets` module file | `dbxcarta.spark.presets` and `dbxcarta.spark.loader` |

The `dbxcarta` and `dbxcarta-ingest` commands are registered by
`dbxcarta-spark`; `dbxcarta-client` is registered by `dbxcarta-client`.

## Public API and version contract

External projects depend on the distribution that matches the capability they use. The public surfaces are:

- **Spark:** `SparkIngestSettings`, `run_dbxcarta`, graph contract enums and constants, Databricks identifier/path validators, preset loading, `verify_run`, and the `dbxcarta` / `dbxcarta-ingest` wheel entrypoints
- **Client:** retrieval primitives, SQL parsing and read-only guards, result comparison, `ClientSettings`, and the `dbxcarta.client.eval` harness

A preset is a small configuration adapter published by an external package and
referenced by an import-path spec like `your_pkg.module:preset`. Optional
readiness and question-upload hooks live in `dbxcarta.spark.presets` for CLI
and demo integrations; they do not require the Spark package to depend on the
client runtime.

Removing or renaming a public name above is a breaking change. Adding a new name is additive. Implementation modules below a layer remain internal unless documented here.

## Quickstart

This path creates the demo Unity Catalog schemas, builds the Neo4j semantic layer, then runs the demo client against that graph.

### 1. Configure the project

```bash
uv sync
cp .env.sample .env
```

Open `.env` and replace the placeholders. Keep the demo defaults already organized in `.env.sample`:

- The four `dbxcarta_test_*` schemas
- Table and column embeddings enabled
- Values enabled
- `DBXCARTA_CLIENT_ARMS=graph_rag`
- `DBXCARTA_CLIENT_QUESTIONS` pointing at the UC Volume copy of `demo_questions.json`

Use an existing UC catalog, schema, and volume, or create them if your principal has permission:

```bash
uv run dbxcarta schema create <catalog>.<schema>
uv run dbxcarta volume create <catalog>.<schema>.<volume>
```

Create the Neo4j secrets in Databricks. These keys are read from the secret scope at job runtime.

```bash
./setup_secrets.sh --profile <your-profile>
```

If the Neo4j database already has dbxcarta data, clear it before the first demo run so constraints and vector indexes are created cleanly:

```cypher
MATCH (n) DETACH DELETE n;
```

### 2. Create the demo source schemas

`scripts/run_demo.py` uses `DBXCARTA_CATALOG`, `DATABRICKS_WAREHOUSE_ID`, and `DATABRICKS_VOLUME_PATH` from `.env`. It creates and populates the demo source schemas in Unity Catalog.

```bash
uv run python scripts/run_demo.py
```

### 3. Build the semantic layer

Upload the package wheel, upload the demo questions file to the configured UC Volume, then submit the installed wheel's ingest entrypoint.

```bash
uv run dbxcarta upload --wheel
uv run dbxcarta upload --data tests/fixtures
uv run dbxcarta submit-entrypoint ingest
```

The ingest run should finish with `status=success`. It:

- Writes the graph to Neo4j
- Writes JSON run output under `DBXCARTA_SUMMARY_VOLUME`
- Appends a row to `DBXCARTA_SUMMARY_TABLE`

### 4. Run the demo client

The demo client:

- Embeds each question
- Retrieves context from the Neo4j semantic layer
- Asks the configured chat endpoint for SQL
- Executes the SQL on the warehouse
- Writes a client run summary

```bash
uv run dbxcarta submit-entrypoint client
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

![dbxcarta Graph Schema](docs/assets/graph-schema.png)

Editable source: [`docs/assets/graph-schema.excalidraw`](docs/assets/graph-schema.excalidraw).

### Build time: pipeline writes the graph

Unity Catalog metadata flows through a single Spark job that extracts, embeds, and loads every enabled node label into Neo4j. Embeddings are generated inside Spark via `ai_query` and materialized to a Delta staging table before the Neo4j write, so the embedding call happens exactly once per run.

```
┌──────────────────────────────────────────── BUILD TIME ────────────────────────────────────────────┐
│                                                                                                    │
│  ┌──────────────────┐   ┌──────────────────┐   ┌──────────────────┐   ┌──────────────────┐      │
│  │ Unity Catalog    │──►│ Preflight        │──►│ Extract          │──►│ Transform        │      │
│  │ information_schema│   │ permission check │   │ SQL to DataFrames│   │ typed graph rows │      │
│  └──────────────────┘   └──────────────────┘   └──────────────────┘   └──────────────────┘      │
│                                                                            │                       │
│                                                                            ▼                       │
│  ┌──────────────────┐   ┌──────────────────┐   ┌──────────────────┐                                │
│  │ Neo4j (Aura)     │◄──│ Delta Staging    │◄──│ Embed            │                                │
│  │ MERGE + indexes  │   │ materialized rows│   │ ai_query/label   │                                │
│  └──────────────────┘   └──────────────────┘   └──────────────────┘                                │
│                                                                                                    │
└────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

**Pipeline stages:**

- **Unity Catalog:** reads source metadata from `information_schema`, including tables, columns, schemas, and sampled values
- **Preflight:** checks grants, endpoint access, and required configuration before the Spark job does any expensive work
- **Extract:** queries Unity Catalog metadata into Spark DataFrames for each enabled node and relationship type
- **Transform:** shapes raw metadata into stable graph rows with typed labels, dotted IDs, descriptions, and relationship keys
- **Embed:** calls `ai_query` inside Spark for each enabled label; row-level failures are captured instead of aborting the whole run
- **Delta Staging:** materializes enriched rows once so validation, summaries, and Neo4j writes reuse the same embedding results
- **Neo4j:** writes nodes and relationships with `MERGE`, then creates or updates the vector indexes used at query time

### Query time: client retrieves schema context

A client performs two steps: a vector similarity search to find the most relevant nodes, then a graph traversal to expand that seed set into a full schema subgraph. The combination delivers both semantic relevance and structural completeness: the LLM receives the right tables and their columns, values, and relationships.

```
┌──────────────────────────────────────────── QUERY TIME ────────────────────────────────────────────┐
│                                                                                                    │
│  ┌──────────────────┐   ┌──────────────────┐   ┌──────────────────┐   ┌──────────────────┐      │
│  │ User Question    │──►│ Client           │──►│ Embed Question   │──►│ Vector Search    │      │
│  │ natural language │   │ Text2SQL/MCP/RAG │   │ query vector     │   │ top-k graph nodes│      │
│  └──────────────────┘   └──────────────────┘   └──────────────────┘   └──────────────────┘      │
│                                                                            │                       │
│                                                                            ▼                       │
│  ┌──────────────────┐   ┌──────────────────┐   ┌──────────────────┐                                │
│  │ SQL / Answer     │◄──│ LLM              │◄──│ Graph Traversal  │                                │
│  │ generated result │   │ combined context │   │ schema subgraph  │                                │
│  └──────────────────┘   └──────────────────┘   └──────────────────┘                                │
│                                                                                                    │
└────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

**Query stages:**

- **User Question:** starts as a natural-language request from a Text2SQL, MCP, or schema-aware RAG workflow
- **Client:** coordinates retrieval by embedding the question, querying Neo4j, assembling context, and calling the LLM
- **Embed Question:** converts the user question into the same vector space used by table, column, and value embeddings
- **Vector Search:** finds the most semantically relevant `Table`, `Column`, and `Value` nodes using cosine similarity and Neo4j vector indexes
- **Graph Traversal:** expands the top-k seed nodes through `HAS_COLUMN`, `HAS_VALUE`, and `REFERENCES` relationships
- **LLM:** receives the combined schema context and produces the final SQL or answer
- **SQL / Answer:** returns the generated query or response to the calling workflow

## Design

Everything runs inside Databricks: no external orchestrators, no local execution, no service accounts.

- **Single submission:** one installed wheel entrypoint (`dbxcarta-ingest`, submitted with `dbxcarta submit-entrypoint ingest`) drives the whole pipeline in one Databricks Job. Scope is controlled by per-label embedding flags in `.env`.
- **Spark:** extraction and transformation use PySpark DataFrames, so the pipeline scales to large catalogs without single-process bottlenecks.
- **Model Serving:** embeddings are generated in Spark via `ai_query` against a Databricks-hosted foundation model endpoint (`databricks-gte-large-en` by default), with `failOnError => false` so row-level failures are counted rather than thrown.
- **Materialize-once:** enriched node DataFrames are written to a Delta staging table between transform and load, so the failure-rate aggregation and the Neo4j write both consume the staged rows without re-invoking `ai_query`.
- **Neo4j Spark Connector:** bulk, partitioned writes from DataFrames. Relationship writes default to `coalesce(1)` to avoid endpoint-node lock contention on Aura, configurable via `DBXCARTA_REL_WRITE_PARTITIONS` (raise only with production evidence); `batch.size` is tuned via `DBXCARTA_NEO4J_BATCH_SIZE`.
- **Preflight:** grants and serving-endpoint permissions required by the enabled flags are checked before any extract runs; missing permissions fail the run fast.
- **Secrets:** Neo4j credentials live in a Databricks secret scope and are injected at job time, not read from a local file.
- **Metadata source:** Unity Catalog `information_schema` only; no pluggable multi-source connector layer.
- **Run observability:** every run emits a `RunSummary` to stdout, a timestamped JSON file in a UC Volume, and a row appended to a Delta table so history is queryable via SQL. The summary records per-label embedding attempts, successes, and failure rates alongside the threshold and the endpoint used.
- **`databricks-job-runner`:** CLI wrapper around the Databricks SDK that handles upload, submit, and cleanup.

See `docs/reference/best-practices.md` for the design rules (Spark / Databricks, Neo4j Spark Connector, project-level principles) that shape the pipeline.

## Configuration

All pipeline and client behavior is controlled by `.env`. Copy `.env.sample`, fill in the placeholders, and use the comments in that file as the configuration reference. The sample is organized by:

- Databricks auth
- Workspace locations
- Compute
- Secrets
- Unity Catalog scope
- Run artifacts
- Embeddings
- Neo4j write tuning
- Client settings

## Demo Client Details

The client is a batch evaluation job for the Neo4j semantic layer. In the quickstart it runs the `graph_rag` arm against `tests/fixtures/demo_questions.json` after that file is uploaded to `DATABRICKS_VOLUME_PATH`.

The question set exercises:

- Cross-schema joins (`sales` to `inventory` and `sales` to `hr`)
- Self-referential FKs
- Composite FK paths
- Intra-schema event analytics

For your own catalog, upload a replacement questions JSON file to the UC Volume and point `DBXCARTA_CLIENT_QUESTIONS` at it.

## Automated end-to-end test

`scripts/run_autotest.py` is a self-contained harness that:

- Provisions a known fixture schema in Unity Catalog
- Runs the full pipeline against it
- Asserts the resulting `RunSummary` JSON meets expected thresholds
- Produces a dated JSON result file in the configured volume
- Exits non-zero on any failure

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
| 3 — Ingest run | Builds and uploads the wheel, submits `dbxcarta-ingest`, waits for `SUCCESS`, and downloads the `RunSummary` JSON |
| 4 — Assertions | Validates the `RunSummary`: `status=success`, `error=null`, `schemas >= 4`, `tables >= 19`, `fk_declared >= 16`, `fk_edges >= 16`, `neo4j_counts` non-empty |
| 5 — Output JSON | Writes `autotest_results_<ts>.json` to `DBXCARTA_SUMMARY_VOLUME/autotest/` and locally to `outputs/` (git-ignored) |

The fixture covers all the structural edge cases:

- **Cross-schema FKs:** sales to hr, sales to inventory
- **Self-referential FK:** `employees.manager_id`
- **Composite PK / associative table:** `product_suppliers`
- **Complex column types:** `STRUCT`, `ARRAY`, `MAP`, `VARIANT`, `BINARY`
- **External schema** (`dbxcarta_test_external`): requires a UC Volume path; excluded from ingest but included in teardown

**Notes:**

- The harness locates the `RunSummary` via a before/after volume diff. `DATABRICKS_JOB_RUN_ID` is not set for one-time `runs.submit()` jobs, so the file is always written as `dbxcarta_local_<ts>.json`.
- Schema setup and teardown are idempotent; re-running always starts from a clean state.
- Unit tests run with `--ignore=tests/integration` to exclude slow live-catalog suites.
- Results are also written locally to `outputs/autotest_results_<ts>.json` (git-ignored) for quick inspection without going back to the volume.

## Upload and submit

### Supply-chain checks

Run the local supply-chain checks before submitting a changed package to Databricks:

```bash
uv lock --check
uv sync --frozen --group test --all-packages
uv run pytest
rm -rf dist
uv build --all-packages
uv run python scripts/security/artifact_audit.py inspect dist
uv run python scripts/security/artifact_audit.py provenance dist \
  --output dist/supply-chain-provenance.json
```

The provenance file records:

- The source commit
- Lockfile hash
- Package version
- SHA256 hashes for built artifacts

Treat it as audit evidence for the reviewed artifacts.

**`upload`**
- `--wheel` — builds the relevant dbxcarta wheel, bumps the patch version, and uploads the wheel to `DATABRICKS_VOLUME_PATH/wheels/`. Re-run whenever `packages/dbxcarta-*/src/` changes.
- `--all` — copies every `scripts/*.py` to the workspace. Re-run whenever `scripts/` changes.

**`submit <script>`**

The script name is relative to `scripts/`. Scripts named `run_dbxcarta*` auto-attach the latest uploaded wheel. All non-Databricks `.env` variables are forwarded to the job.

Supply-chain note: `upload --wheel` currently combines version bumping, building, and UC Volume upload. For reviewed releases, prefer a CI-built wheel and provenance manifest as the artifact of record. A future hardening step should make Databricks submission select a wheel by explicit version or hash instead of by latest local wheel mtime.

- `--upload` — uploads `scripts/*.py` before submitting, replacing a separate `upload --all` step.
- `--no-wait` — returns immediately with the run ID.
- `--compute {cluster,serverless}` — overrides `DATABRICKS_COMPUTE_MODE` for this run.
