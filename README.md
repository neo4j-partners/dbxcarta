# dbxcarta: Neo4j Semantic Layer for Databricks Unity Catalog

*Inspired by [neocarta](https://github.com/neo4j-field/neocarta).*

## Quick start

New here? Pick an example and follow its README end to end:

- **[Finance Genie](examples/finance-genie/README.md#quick-start)**: two-catalog medallion demo with curated finance tables enriched into gold.
- **[SchemaPile](examples/schemapile/README.md#quick-start)**: a reproducible [SchemaPile](https://github.com/amsterdata/schemapile) slice materialized as Delta tables in a dedicated catalog.
- **[Dense schema](examples/dense-schema/README.md#quick-start)**: a synthetic 500 or 1000 table schema that stress-tests schema-context retrieval.

To map your own catalog instead of a demo, follow [Quickstart: demo catalog](#quickstart-demo-catalog).

## Overview

Unity Catalog publishes tables, columns, and declared constraints but not
meaning: which tables sit near a question, which relationships exist beyond
declared foreign keys, which medallion layer holds the trustworthy version of an
entity. dbxcarta builds a semantic layer over Unity Catalog in Neo4j that turns
that flat metadata into a graph a question can traverse:

- **Similarity** is an embedding distance.
- **A relationship** is a confidence-scored edge.
- **Every table** carries its medallion layer.

Text2SQL agents, MCP tools, and schema-aware RAG pipelines query the graph at
runtime to retrieve the slice they need before generating SQL.

```
┌──────────────────────┐   ┌───────────────┐   ┌──────────────────────┐   ┌──────────────────────┐   ┌──────────────────────┐   ┌───────────────┐
│ dbxcarta-materialize │──►│ Unity Catalog │──►│ neocarta connector   │──►│ Neo4j semantic layer │──►│ dbxcarta-client      │──►│ SQL / answer  │
│ blueprint to tables  │   │ flat metadata │   │ ingest job (wheel):  │   │ typed nodes, vectors,│   │ query: embed,        │   │ generated     │
│ (seeds example data) │   │               │   │ extract, inline      │   │ confidence-scored FKs│   │ vector + graph fetch │   │ result        │
│                      │   │               │   │ embed, infer FKs     │   │                      │   │                      │   │               │
└──────────────────────┘   └───────────────┘   └──────────────────────┘   └──────────────────────┘   └──────────────────────┘   └───────────────┘
```

- **Leftmost stage (examples only):** `dbxcarta-materialize` seeds the bundled examples' demo tables into Unity Catalog from a committed blueprint. Against your own catalog the tables already exist, so the flow starts at Unity Catalog and materialize is not used.
- **Ingest pipeline:** the [neocarta](https://github.com/neo4j-field/neocarta) connector wheel builds the semantic layer; dbxcarta no longer carries its own copy. dbxcarta is the operator tooling and evaluation around it: `dbxcarta-submit` (the `dbxcarta` command) stages the neocarta wheel and deploys it as a Databricks job, while `dbxcarta-client`, `dbxcarta-materialize`, and the bundled `examples/` evaluate and demonstrate the result. To map your own catalog you pull the neocarta wheel and run it with `dbxcarta`.
- **Further reading:** [`docs/reference/architecture.md`](docs/reference/architecture.md) is the canonical architecture reference for the semantic-layer thesis, the three storage planes, and the validation model. [`docs/README.md`](docs/README.md) is the full documentation map and reading order. [`docs/explanation/why-semantic.md`](docs/explanation/why-semantic.md) covers why a graph layer earns its place over Unity Catalog alone.

## Packages

dbxcarta is four packages over a shared, Spark-free core, plus the external
neocarta connector wheel it pulls for ingest:

- **neocarta (external):** the ingest pipeline. Reads Unity Catalog and writes the semantic layer.
- **`dbxcarta-core`:** the foundation every other package builds on.
- **`dbxcarta-submit`:** the operator-local CLI (the `dbxcarta` command) that stages the neocarta wheel, builds and uploads the local wheels, and submits the jobs.
- **`dbxcarta-client`, `dbxcarta-materialize`, `examples/`:** the evaluation and demo tier that proves and showcases the layer.

The siblings each depend on core but never on one another, and there is no
top-level `dbxcarta` import surface, so library consumers import only the layer
they need.

```
┌───────────────────────────────────────────────────────────────────────────┐
│ examples/   finance-genie · schemapile · dense-schema                       │
│ overlay env + questions.json                   depend on → client, core     │
└───────────────────────────────────────────────────────────────────────────┘
                                     │
        ┌────────────────────────────┼────────────────────────────┐
        ▼                            ▼                            ▼
┌──────────────────────┐  ┌──────────────────────┐  ┌──────────────────────┐
│ neocarta (external)  │  │ dbxcarta-client      │  │ dbxcarta-materialize │
│ UC ingest to Neo4j   │  │ retrieval + eval     │  │ build demo tables    │
│ +databricks-spark    │  │ +requests            │  │ +pyspark (shell)     │
└──────────────────────┘  └──────────────────────┘  └──────────────────────┘
                                     │            (client, materialize)
                                     ▼
┌───────────────────────────────────────────────────────────────────────────┐
│ dbxcarta-core      Spark-free shared foundation · databricks-sdk only       │
│ identifiers · catalogs · workspace · executor · readiness · env ·           │
│ materialize (pure SQL builders) · questions · config · volume_io            │
└───────────────────────────────────────────────────────────────────────────┘

   dbxcarta-submit   operator CLI (the `dbxcarta` command) · depends on core +
                     databricks-job-runner. Stages the neocarta ingest wheel,
                     builds/uploads the client and materialize wheels, and
                     submits the jobs. Runs locally; never on the cluster.
```

**The ingest pipeline**: builds the semantic layer; pulled from neocarta.

| Distribution | Capability | Import path | Console script |
|--------------|------------|-------------|----------------|
| `neocarta` (external) | Reads Unity Catalog metadata, embeds it inline, infers foreign keys, and writes the semantic-layer graph into Neo4j. Pulled as a wheel with the `databricks-spark` extra; dbxcarta stages and runs it. | `neocarta` | `neocarta-databricks-ingest` |

**Foundation & operator tooling**: pull the neocarta wheel and run it on Databricks.

| Distribution | Capability | Import path | Console script |
|--------------|------------|-------------|----------------|
| `dbxcarta-core` | The shared foundation every other package builds on: common helpers for naming, config, secrets, and running SQL. Depends only on the Databricks SDK. | `dbxcarta.core` | — |
| `dbxcarta-submit` | The command you run on your own machine to stage the neocarta wheel, build and upload the local wheels, and launch the Databricks jobs. | `dbxcarta.submit` | `dbxcarta` |

**Evaluation & demo**: prove and showcase the layer; not needed to build it for your own catalog.

| Distribution | Capability | Import path | Console script |
|--------------|------------|-------------|----------------|
| `dbxcarta-client` | Queries the finished graph to retrieve schema context and scores how well the semantic layer helps Text2SQL. | `dbxcarta.client` | `dbxcarta-client`, `dbxcarta-embed-probe` |
| `dbxcarta-materialize` | A Databricks job that creates the example demo tables from a saved blueprint. | `dbxcarta.materialize` | `dbxcarta-materialize` |

**Which package do I need?** To map your own catalog, install `dbxcarta-core`,
pull the `neocarta` wheel, and run them with the `dbxcarta` command (provided by
`dbxcarta-submit`). The rest are for evaluation and demos. New here? Jump to the
[Quickstart](#quickstart-demo-catalog).

**Layer responsibilities:**

*The ingest pipeline (external)*

- **neocarta** reads Unity Catalog and builds the Neo4j semantic layer: extract, inline-embed, infer foreign keys, and write the graph. It is pulled as a wheel with the `databricks-spark` extra and run as the `neocarta-databricks-ingest` job; dbxcarta does not carry its source.

*Foundation & operator tooling*

- **Core** is the shared bottom layer every dbxcarta package builds on. It provides the common building blocks for naming and paths, reading config and secrets, running SQL, and loading settings. It depends only on the Databricks SDK, never Spark, Neo4j, or the job runner. The boundaries are enforced by `tests/boundary/test_import_boundaries.py`.
- **Submit** is the `dbxcarta` command you run on your own machine to stage the neocarta ingest wheel, build and upload the client and materialize wheels, and submit Databricks jobs. It also provides the `ready` operator helper. It is the only layer that touches the job runner, and it never runs on the cluster.

*Evaluation & demo*

- **Client** queries the finished graph to retrieve schema context and runs the Text2SQL evaluation that proves the layer earns its place.
- **Materialize** is a Databricks job that creates the example demo tables from a saved blueprint.

The component-level breakdown of each layer is in [`docs/reference/architecture.md`](docs/reference/architecture.md#packages-and-layer-responsibilities).

## The ingest pipeline (neocarta connector)

The Unity Catalog ingest is the [neocarta](https://github.com/neo4j-field/neocarta)
connector wheel. It owns the concrete ingest implementation and the graph
contract; dbxcarta stages the wheel and submits it as the ingest job.

**What the build pipeline does:**

- Extracts Unity Catalog metadata across one or more catalogs: table names, column descriptions, comments, and sampled values
- Tags every table with its medallion layer from the `catalog:layer` entries in `DBXCARTA_CATALOGS` / `NEOCARTA_DATABRICKS_CATALOGS`; dbxcarta reads only the silver and gold layers, folding them into one graph
- Embeds each piece inline, in-cluster, with a native `ai_query()` call against a Databricks serving endpoint, so a single job produces a fully embedded graph
- Discovers foreign keys from declared constraints plus metadata and semantic inference, each edge scored by confidence
- Writes the result to Neo4j as typed nodes with vector properties

Because dbxcarta runs neocarta's inline embedding mode, the ingest job emits a
fully embedded graph and the only operator follow-up is `materialize`. The
decoupled external embedding path (re-embedding without re-running the job) is
deferred to a later phase; see the alignment plan's Phase 7.

### Graph schema

The pipeline writes a stable typed contract the client traverses: nodes
`Database`, `Schema`, `Table`, `Column`, and `Value`, connected by `HAS_SCHEMA`,
`HAS_TABLE`, `HAS_COLUMN`, `HAS_VALUE`, and confidence-scored `REFERENCES` edges
between columns. Each node carries a dotted catalog-qualified `id`, a
`description`, and an `embedding` where applicable; `Table` nodes also carry a
`layer` tier. neocarta owns the authoritative contract; the shape the client
reads is described in
[`docs/reference/architecture.md`](docs/reference/architecture.md).

![dbxcarta Graph Schema](docs/assets/graph-schema.png)

### Build time: the ingest job writes the graph

Unity Catalog metadata flows through a single Spark job run from the neocarta
wheel: preflight checks grants and config, extract unions `information_schema`
into DataFrames, transform shapes typed graph rows, embed calls `ai_query` per
label inline, then the Neo4j write lands nodes and relationships and refreshes the
vector indexes. The pipeline internals live in the
[neocarta](https://github.com/neo4j-field/neocarta) connector.

### Configuring the ingest job

The neocarta wheel reads its own `NEOCARTA_DATABRICKS_*` environment contract. The
example overlays set it directly:

```bash
# In the selected dbxcarta-overlay.env — the neocarta wheel reads NEOCARTA_DATABRICKS_*
NEOCARTA_DATABRICKS_CATALOG=analytics_silver
NEOCARTA_DATABRICKS_CATALOGS=analytics_silver:silver,analytics_gold:gold
NEOCARTA_DATABRICKS_SCHEMAS=finance,customer_success
NEOCARTA_DATABRICKS_INCLUDE_EMBEDDINGS_TABLES=true
NEOCARTA_DATABRICKS_INCLUDE_EMBEDDINGS_COLUMNS=true
NEOCARTA_DATABRICKS_EMBEDDING_ENDPOINT=databricks-gte-large-en
NEOCARTA_DATABRICKS_EMBEDDING_DIMENSION=1024
NEOCARTA_DATABRICKS_EMBEDDING_STAGING_VOLUME=/Volumes/analytics/ops/dbxcarta/staging
NEOCARTA_DATABRICKS_SUMMARY_VOLUME=/Volumes/analytics/ops/dbxcarta/summaries
NEOCARTA_DATABRICKS_SECRET_SCOPE=dbxcarta-neo4j-analytics
```

- **`NEOCARTA_DATABRICKS_CATALOG`:** the single anchor catalog used for preflight and provisioning.
- **`NEOCARTA_DATABRICKS_CATALOGS`:** every catalog folded into one graph, the default model since a build normally spans several. Each entry is `catalog` or `catalog:layer`, where the optional `:layer` suffix sets the `Table.layer` property. A single-catalog build remains supported: leave `CATALOGS` blank and it falls back to the anchor.
- **`EMBEDDING_STAGING_VOLUME`:** required whenever any include-embeddings flag is on, because dbxcarta runs inline embeddings.
- **`SUMMARY_VOLUME`:** where each detached run writes its `summary_<run_id>.json` report.

The full overlay contract is documented per example in
`examples/<name>/dbxcarta-overlay.env`.

The operator forwards these `KEY=VALUE` pairs to the cluster as job parameters,
and the neocarta wheel's settings load them on startup. Operator-only and
client-only keys keep their `DBXCARTA_*` / `DATABRICKS_*` names and are harmlessly
ignored by neocarta.

### Design principles

Everything runs inside Databricks: no external orchestrators, no local execution,
no service accounts. The build is a single Spark submission and writes Neo4j
behind a fail-closed boundary. That architectural rationale is in
[`docs/reference/architecture.md`](docs/reference/architecture.md#building-the-layer).
The operational rules, Spark and Neo4j connector tuning, preflight grant checks,
secret handling, metadata-source scope, and run observability, are in
[`docs/reference/best-practices.md`](docs/reference/best-practices.md).

## dbxcarta-client: validation harness

The client package owns retrieval primitives and the Text2SQL eval harness. It
queries the Neo4j semantic layer the neocarta ingest job built.

### Query time: client retrieves schema context

A client embeds a user question, runs a vector similarity search to find the most
relevant `Table`, `Column`, and `Value` nodes, then traverses `HAS_COLUMN`,
`HAS_VALUE`, and `REFERENCES` edges to expand that seed into a full schema
subgraph before the LLM call. The combination delivers semantic relevance and
structural completeness in one retrieval step. The query-time and validation
walkthrough is in
[`docs/reference/architecture.md`](docs/reference/architecture.md#how-we-validate).

### Demo client details

The client is a batch evaluation job that proves the semantic layer earns its
place. It runs each question through up to four arms selected by
`DBXCARTA_CLIENT_ARMS`, scored the same way (parsed, executed, non-empty, matched
against a reference result):

- **`reference`** executes the ground-truth SQL to establish the correct result.
- **`no_context`** gives the model only the catalog name, the floor.
- **`schema_dump`** pastes a bounded schema pulled from the graph, capped to a token-matched budget so it is a real control rather than a paste-everything strawman.
- **`graph_rag`** is the semantic layer in use: vector-seed plus confidence-ranked `REFERENCES` expansion. It justifies the build pipeline only if it beats `schema_dump` at the matched budget and both clear `no_context`.

The arms cache generation in the ops catalog so unchanged re-runs skip inference.
The cache layout and refresh controls, and the validation rationale, are in
[`docs/reference/architecture.md`](docs/reference/architecture.md#how-we-validate).

In the quickstart the client runs the `graph_rag` arm against
`tests/fixtures/questions.json`, a local file the client reads directly. The
question set exercises cross-schema joins, self-referential FKs, composite FK
paths, and intra-schema event analytics. For your own catalog, point
`DBXCARTA_CLIENT_QUESTIONS` at a replacement local questions JSON file.

## Examples

### How an example is configured

An example is a folder under `examples/` that carries everything dbxcarta needs
to run against a specific upstream project. The configuration is two files, no
Python object:

1. **`dbxcarta-overlay.env`:** the committed, secret-free set of environment variables needed to run dbxcarta against that project or schema. Select it with `--env-file` (or `DBXCARTA_ENV_FILE`) and it layers over the base `.env`.
2. **`questions.json`:** the bundled demo question set, kept beside the overlay at the example root.

The operator CLI and the local client read both directly. `dbxcarta ready`
resolves the catalog list from the loaded overlay and confirms each ingested
catalog holds a data schema; its logic lives in `dbxcarta.submit.readiness`. The
client runs locally and opens `questions.json` straight off local disk at the
path `DBXCARTA_CLIENT_QUESTIONS` names (by default the file beside the overlay).
There is no upload step; nothing stages the questions to a Volume anymore.

| Command | Purpose |
|---------|---------|
| `dbxcarta ready` | Checks that each ingested catalog holds a data schema |
| `dbxcarta-client` | Runs the Text2SQL evaluation locally against the bundled `questions.json` |

### Available examples

Companion examples show how to package reusable configuration and demo data for
known upstream projects. Each example folder carries an overlay and a questions
file; the dense-schema and schemapile folders also ship a small Python package
for their standalone data-generation tooling.

#### Finance Genie

[**Follow the Finance Genie Quick Start →**](examples/finance-genie/README.md#quick-start)

- **Layout:** two-catalog medallion. Finance Genie writes curated business tables to silver and graph-enriched features to gold, named `graph-enriched-finance-silver` and `-gold`.
- **Graph:** the ingest folds both catalogs into one Neo4j semantic layer via `NEOCARTA_DATABRICKS_CATALOGS`, tagging each table's tier from the `:layer` suffix on each entry.
- **Ops catalog:** run summaries and the generation cache land in a separate `dbxcarta-catalog`.

#### SchemaPile

[**Follow the SchemaPile Quick Start →**](examples/schemapile/README.md#quick-start)

- **Source:** a reproducible slice of [SchemaPile](https://github.com/amsterdata/schemapile) materialized as Delta tables in a dedicated catalog.
- **Schema list:** `schemapile_lakehouse` is a dedicated, data-only catalog, so a blank `NEOCARTA_DATABRICKS_SCHEMAS` auto-discovers the materialized schemas.
- **Questions:** a SQL-validated question set is generated.
- **Overlay:** `examples/schemapile/dbxcarta-overlay.env`.

#### Dense schema

[**Follow the Dense Schema Quick Start →**](examples/dense-schema/README.md#quick-start)

- **Purpose:** stress-test schema-context retrieval against dense schema context.
- **Schema:** a synthetic single schema of 500 or 1000 tables.
- **Catalog:** reuses the SchemaPile lakehouse catalog.
- **Overlay:** `examples/dense-schema/dbxcarta-overlay.env`.

### Example workflow

Every example follows the same steps. The per-example config lives in the
committed `dbxcarta-overlay.env`, so select that overlay once and every command
picks it up.

1. Select the example's overlay (the single source of per-example config):
   ```bash
   export DBXCARTA_ENV_FILE=examples/finance-genie/dbxcarta-overlay.env
   ```

2. Check readiness, which confirms each ingested catalog holds a data schema:
   ```bash
   uv run dbxcarta ready
   ```

3. After the ingest job has built the graph, run the client evaluation locally
   (it reads the bundled `questions.json` directly and needs no cluster):
   ```bash
   uv run dbxcarta-client
   ```

**Generated examples** (schemapile, dense-schema) follow the identical steps;
materialize the source data first so readiness sees a data schema. Swap the
overlay path:

```bash
export DBXCARTA_ENV_FILE=examples/schemapile/dbxcarta-overlay.env
uv run dbxcarta ready

export DBXCARTA_ENV_FILE=examples/dense-schema/dbxcarta-overlay.env
uv run dbxcarta ready
```

See each example's README for its full setup flow.

## Quickstart: demo catalog

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
- `DBXCARTA_CLIENT_QUESTIONS` pointing at the local `tests/fixtures/questions.json`

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

This quickstart uses the built-in demo catalog, created locally through the SQL warehouse by `run_demo.py`. The bundled examples (finance-genie, schemapile, dense-schema) instead seed their tables with the serverless `dbxcarta materialize` job; see each example's README.

```bash
uv run python scripts/run_demo.py
```

### 3. Build the semantic layer

Stage the neocarta ingest wheel and the local wheels, then submit the neocarta
connector's ingest entrypoint.

```bash
uv run dbxcarta publish-wheels
uv run dbxcarta submit-entrypoint ingest
```

The ingest run should finish with `status=success`. It:

- Writes the fully embedded graph to Neo4j
- Writes a `summary_<run_id>.json` run report under `NEOCARTA_DATABRICKS_SUMMARY_VOLUME`

### 4. Run the demo client

The demo client runs locally (no cluster). For each question it:

- Embeds the question
- Retrieves context from the Neo4j semantic layer
- Asks the configured chat endpoint for SQL
- Executes the SQL on the warehouse
- Prints a truncated per-arm summary

```bash
uv run dbxcarta-client
```

The run prints per-arm `executed` and `non_empty` rates straight to your
terminal.

### 5. Verify and clean up

Run local tests:

```bash
uv run pytest
```

Run live integration tests after a successful ingest:

```bash
uv run pytest tests/integration -m live
```

Remove the demo schemas when you are done:

```bash
uv run python scripts/run_demo.py --teardown
```

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
| `DBXCARTA_CATALOG` | The autotest provisions its fixture schemas and ops artifacts in one catalog and requires `dbxcarta-catalog`. This is the autotest harness's own single-catalog convention, not the general model: a normal build separates data catalogs from the ops catalog (see [`docs/reference/architecture.md`](docs/reference/architecture.md)). |
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
| 3 — Ingest run | Stages the neocarta wheel and submits `dbxcarta submit-entrypoint ingest`, waits for `SUCCESS`, and downloads the `RunSummary` JSON |
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

## dbxcarta-submit: operator tooling

These operator commands are provided by the `dbxcarta-submit` package, a thin CLI wrapper around `databricks-job-runner` that handles upload, submit, and cleanup. It depends on `dbxcarta-core`, runs on the operator's machine, and is never installed on the cluster. No other package depends on the job runner.

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

**`bootstrap`**

Creates the catalog, schema, and volume named by the selected overlay's `DATABRICKS_VOLUME_PATH` (`CREATE ... IF NOT EXISTS`), running locally against a SQL warehouse. Idempotent, so it is the first step of every `e2e-<example>-ingest` make target and a no-op on later runs; needs `DATABRICKS_WAREHOUSE_ID` and operator privilege. `--dry-run` prints the three names (and refuses a protected catalog) without touching the workspace.

**`teardown`**

Drops the catalog or schema named by the overlay's `DBXCARTA_TEARDOWN_TARGET` (`schema:<catalog>.<schema>` or `catalog:<catalog>`, dropped with `CASCADE`). Destructive and never automatic: it requires `--yes-i-mean-it`, and without it — or under `--dry-run` — only prints what it would drop. A shared protected-name blocklist guards both commands, so neither can name a system catalog. Run by hand via `make e2e-<example>-teardown`.

**`publish-wheels`**

Stages the prebuilt neocarta ingest wheel from its local build folder, builds the local client and materialize wheels and bumps their patch version, uploads all of them to `DATABRICKS_VOLUME_PATH/wheels/`, and ships the cluster bootstrap script. Re-run whenever `packages/dbxcarta-*/src/` changes or the neocarta wheel is rebuilt.

**`upload`** (generic, passed through to `databricks-job-runner`)
- `--all` — copies every `scripts/*.py` to the workspace. Re-run whenever `scripts/` changes.
- `--data DIR` — uploads a data directory to the UC volume.

**`submit <script>`**

The script name is relative to `scripts/`. All non-Databricks `.env` variables are forwarded to the job. Wheel entrypoints (the neocarta ingest connector and the client) are not submitted this way: use `submit-entrypoint {ingest|client}`, which attaches the staged wheel and runs its entrypoint.

Supply-chain note: `publish-wheels` currently combines version bumping, building, and UC Volume upload. For reviewed releases, prefer a CI-built wheel and provenance manifest as the artifact of record. A future hardening step should make Databricks submission select a wheel by explicit version or hash instead of by latest local wheel mtime.

- `--upload` — uploads `scripts/*.py` before submitting, replacing a separate `upload --all` step.
- `--no-wait` — returns immediately with the run ID.
- `--compute {cluster,serverless}` — overrides `DATABRICKS_COMPUTE_MODE` for this run.

### Clean up a project

Each integration writes its run artifacts to an ops plane: a run summary table, the per-arm client staging/retrieval tables, and a volume holding `runs/`, `staging/`, and the embedding-reuse `ledger/`. To reset one integration, point `scripts/clean-dbxcarta.py` at that integration's overlay env file. The ops location is read from `DBXCARTA_SUMMARY_TABLE` and `DATABRICKS_VOLUME_PATH` in the overlay, so the same script cleans any project:

```bash
uv run scripts/clean-dbxcarta.py \
  --profile aws-partner-rk \
  --env-file examples/finance-genie/dbxcarta-overlay.env
```

It first reads the workspace and prints every table and volume path it will delete, then waits for a `y/n` answer before touching anything. It drops the ops tables (`dbxcarta_run_summary`, `client_retrieval`, every `client_staging_*`) and empties the ops volume contents, including the embedding ledger. The schema and the volume object itself are kept. Pass `-y`/`--yes` to skip the prompt in automation, or `--warehouse-id` to override warehouse selection.

## Package responsibilities

The per-package public surfaces and how they fit together (core, submit, client,
materialize, and the external neocarta ingest wheel) are in
[`docs/reference/architecture.md`](docs/reference/architecture.md#packages-and-layer-responsibilities).

## Known operational gotchas

Operational lessons from running the pipeline, including why dropping a data
catalog on a Default-Storage account is not round-trippable through the tooling,
are in
[`docs/reference/operational-lessons.md`](docs/reference/operational-lessons.md).
