# Adding a New Data Source as an External Client

This is a step-by-step guide to standing up a new data source from **outside** the
dbxcarta repo: a standalone consumer subproject that depends on dbxcarta as a
published, versioned library, points the pipeline at your own Unity Catalog
tables, and runs ingest and evaluation through your own Databricks Asset Bundle.
It is written for someone who owns a separate repository and wants dbxcarta to
build a semantic layer over data they already have.

The worked example is the `finance-genie/dbxcarta` subproject in
`graph-on-databricks`:
<https://github.com/neo4j-partners/graph-on-databricks/tree/main/finance-genie>.
Read its `dbxcarta/README.md` alongside this guide. Everything below generalizes
that subproject to a new consumer.

This is the published-library path. It is different from adding an in-repo
`examples/<name>/` integration: a consumer imports no dbxcarta source, does not
use the operator-only `dbxcarta-submit` CLI, and does not have a sibling dbxcarta
checkout. If you are adding an integration inside the dbxcarta repo itself, that
is a different workflow and not what this guide covers.

## Quick start: copy and adapt

The fastest way to a new consumer is to copy the worked example and edit three
config files. The Python code is config-driven, so no catalog name is baked into
it, and `databricks.yml` holds no dbxcarta config (it reads the overlay at run
time), so there is no bundle to edit for a new data source.

```bash
# From your repo, copy the reference subproject:
cp -r <path-to>/graph-on-databricks/finance-genie/dbxcarta <consumer>/dbxcarta
cd <consumer>/dbxcarta
```

Then edit, in order of importance:

1. **`dbxcarta-overlay.env`** (the single source of dbxcarta config): set the
   `DBXCARTA_*` operator and client keys (catalog, `DATABRICKS_VOLUME_PATH`,
   `DBXCARTA_SUMMARY_VOLUME`, `DBXCARTA_CLIENT_QUESTIONS`, `DATABRICKS_SECRET_SCOPE`)
   and the `NEOCARTA_DATABRICKS_*` ingest contract (catalog, schema list,
   embedding flags) to your catalog and scope. The full key list is in Part 3
   below. Keep it secret-free.
2. **`.env`** (copy from `.env.sample`): your `DATABRICKS_PROFILE`,
   `DATABRICKS_WAREHOUSE_ID`, `DATABRICKS_CLUSTER_ID`, and the `NEO4J_*` values
   for local tooling. This file also carries its own copy of the catalog values
   for the local demo, so match them to the overlay.
3. **`questions.json`**: your question set, with `reference_sql` retargeted to
   your catalog on the gradable questions.

Then provision the secret scope (`./setup_secrets.sh --profile <profile>`) and
run the flow. Parts 3 through 5 below cover each of these in full.

Optional, cosmetic: rename the Python package `finance_genie_dbxcarta` to
`<consumer>_dbxcarta` (and update `pyproject.toml`, `scripts/run_jobs.py`'s
job-name constants, and the bundle and job names in `databricks.yml`). Skip it
and everything still runs; your bundle and jobs just keep the `finance_genie`
names. The rename is the only reason to touch `databricks.yml` at all.

## What you are building

A consumer subproject has four moving parts:

- **A consumer subproject**: a small folder (for example `<consumer>/dbxcarta/`)
  that ships a `dbxcarta-overlay.env` and a `questions.json`. It may also be a
  Python package, whose `pyproject.toml` pins dbxcarta by version with no
  `[tool.uv.sources]`, when it carries a read-only local demo CLI or other
  standalone tooling.
- **An overlay env file**: `dbxcarta-overlay.env`, a committed, secret-free file
  that names the catalog, schema, volume, and feature flags for this data source.
  It is the single source of truth for the integration's dbxcarta config.
- **A consumer-owned Databricks Asset Bundle**: a `databricks.yml` with one
  `python_wheel_task` job that runs the neocarta connector's
  `neocarta-databricks-ingest` entry point on a cluster. (The client is no longer
  a cluster job: it runs locally as `dbxcarta-client`.) This replaces
  `dbxcarta-submit`, which is operator-only, unpublished, and rebuilds wheels from
  the dbxcarta source tree a consumer does not have.
- **The data itself in Unity Catalog**: tables an upstream process already owns
  and populates. A consumer points at existing data. There is no blueprint and
  no materialize step.

The dbxcarta pipeline reads Unity Catalog metadata, builds a Neo4j semantic layer
with embeddings and inferred foreign keys, then runs a Text2SQL evaluation
against it. Your job when adding a data source is to point that pipeline at your
tables, give it a question set, run the ingest job from your bundle, then run the
client locally.

## How dbxcarta reaches your project

A consumer pins these by version:

- `dbxcarta-core`: the foundation. The readiness check, the env loader, and the
  job entry-point plumbing.
- `dbxcarta-client[graph]`: the Text2SQL evaluation harness, the `dbxcarta-client`
  entry point, and the Neo4j driver via the `[graph]` extra.
- `neocarta[databricks-spark]`: the ingest pipeline. It reads Unity Catalog,
  embeds inline, infers foreign keys, and writes the Neo4j semantic layer, and
  registers the `neocarta-databricks-ingest` console entry point. It lives in the
  separate [neocarta](https://github.com/neo4j-field/neocarta) project; dbxcarta
  no longer carries the pipeline.

While PyPI publishing is unavailable, these wheels (the two dbxcarta packages and
the neocarta connector wheel) are **vendored** into the consumer repo and resolved
locally with a relative find-links path. The consumer's `pyproject.toml` still
pins normal versions with no source overrides, so it is identical to the eventual
published case. Only where uv looks for the wheels changes. See
`dbxcarta/docs/reference/simulate-publish.md` for the full mechanism, and
"Vendored wheels" below for the consumer-side steps.

## Concepts in one minute

- **Operational commands**: `dbxcarta ready` confirms your catalog holds data,
  and the local `dbxcarta-client` runs the Text2SQL evaluation, reading your
  `questions.json` straight off local disk. Both run locally and read the
  selected overlay plus the adjacent `questions.json` directly. There is no
  per-integration Python object to publish, and no step that stages questions to
  a volume. The ingest job never calls them; config lives in the overlay.
- **Overlay**: `dbxcarta-overlay.env`. Committed, secret-free, per-integration.
  Selected with `DBXCARTA_ENV_FILE` or `--env-file`. It holds the operator/client
  `DBXCARTA_*` values and the `NEOCARTA_DATABRICKS_*` ingest contract: catalog,
  schemas, volume, embedding flags, client arms, and the per-integration secret
  scope (a scope name, not a secret), set as both `DATABRICKS_SECRET_SCOPE` and
  `NEOCARTA_DATABRICKS_SECRET_SCOPE`.
- **Standalone `.env`**: the consumer's own infra and secrets for local tooling
  (the local demo, readiness checks). It holds the Databricks profile, the
  warehouse, the cluster ID, and, for local use, the `NEO4J_*` connection
  values. It is gitignored. Copy it from the committed `.env.sample`.
- **Consumer DAB**: `databricks.yml`. Two jobs run the published entry points on
  a preprovisioned classic cluster. It carries no dbxcarta config: `run_jobs.py`
  forwards the overlay's `KEY=VALUE` pairs to each job at run time, so the overlay
  is the single source and the two cannot diverge. The entry points parse those
  pairs into the environment, then fetch `NEO4J_*` from the secret scope.
- **Vendored wheels**: `dbxcarta-dist/`, a committed directory of the dbxcarta
  wheels and the neocarta connector wheel plus a committed `uv.toml` find-links.
  It is the simulated index until those projects are on PyPI, reached both by
  local `uv sync` and by the cluster ingest job.
- **Secret scope**: a Databricks secret scope holding `NEO4J_URI`,
  `NEO4J_USERNAME`, and `NEO4J_PASSWORD`. The neocarta ingest entry point reads
  these from the scope itself on the cluster, given the
  `NEOCARTA_DATABRICKS_SECRET_SCOPE` parameter.

## Prerequisites

- A Databricks profile that can read your catalog, deploy bundles, and read
  secrets.
- A Databricks SQL warehouse ID for catalog metadata reads and evaluation
  queries.
- A preprovisioned classic single-user cluster
  (`data_security_mode=SINGLE_USER`). Ingest needs it because the Neo4j Spark
  connector is a task-level Maven library and is not supported on serverless.
- The upstream tables already present in Unity Catalog.
- A Neo4j instance, and its connection values, for the secret scope.

## Part 1: Scaffold the consumer subproject

The fastest start is to copy the `finance-genie/dbxcarta` layout and rename it.
Its files are the reference for every step below.

```
<consumer>/dbxcarta/
├── pyproject.toml              # pinned dbxcarta deps, no source overrides
├── databricks.yml              # ingest job (consumer-owned DAB, config-free)
├── dbxcarta-overlay.env        # committed, secret-free dbxcarta config (single source)
├── .env.sample                 # standalone local-demo config (copy to .env)
├── setup_secrets.sh            # provision the Neo4j secret scope from the overlay + .env
├── questions.json              # eval fixture for your tables
├── uv.toml                     # committed find-links to ./dbxcarta-dist
├── dbxcarta-dist/              # vendored dbxcarta wheels (committed)
├── scripts/refresh_dbxcarta_dist.sh  # maintainer: refresh dbxcarta-dist
├── scripts/run_jobs.py         # deploy + run ingest then client
├── src/<consumer>_dbxcarta/
│   ├── __init__.py             # package marker
│   └── local_demo.py           # read-only local CLI
└── tests/                      # non-live tests
```

Then change the following:

- **`pyproject.toml`**: pin dbxcarta-core, dbxcarta-client, and the neocarta
  connector wheel by version, with no `[tool.uv.sources]`. This is the whole
  point of the published-library approach.

  ```toml
  dependencies = [
      "dbxcarta-core==1.1.0",
      "dbxcarta-client[graph]==1.1.0",
      "neocarta[databricks-spark]==<neocarta version>",
      "databricks-sdk>=0.40",
      "python-dotenv",
  ]
  ```

- **No Python wiring** is needed for the operational commands. `dbxcarta ready`
  and the local `dbxcarta-client` read the selected overlay and the adjacent
  `questions.json` directly, so the readiness check comes from `dbxcarta-core`
  with nothing to implement per integration. The `src/` package exists only when
  you ship standalone tooling like the local demo.

- **`questions.json`**: your demo question set. Each item is an object with a
  non-empty `question_id` and `question`. Add `reference_sql` for any question
  you want graded for correctness, targeting your actual catalog and schema:

  ```json
  [
    {
      "question_id": "fg_q01",
      "question": "How many accounts are there?",
      "notes": "single base table count",
      "reference_sql": "SELECT COUNT(*) FROM `graph-enriched-lakehouse`.`graph-enriched-schema`.accounts"
    }
  ]
  ```

- **`local_demo.py`**: the read-only CLI. It answers one question with graph
  context locally, with no Databricks job, and allows only `SELECT`, `WITH`, and
  `EXPLAIN`. Keep its consumer-root anchor pointed at your `questions.json`.

### Vendored wheels

While dbxcarta and neocarta are not on PyPI, commit a copy of their wheels into
`dbxcarta-dist/` and point uv at them with a relative find-links path. A
maintainer builds the dbxcarta wheels in the dbxcarta checkout and the neocarta
connector wheel in the neocarta checkout, then refreshes the vendored copy:

```bash
# In the dbxcarta checkout (maintainer only):
uv build --package dbxcarta-core
uv build --package dbxcarta-client

# In the neocarta checkout (maintainer only):
uv build --wheel   # the neocarta connector wheel, databricks-spark extra

# In the consumer subproject:
./scripts/refresh_dbxcarta_dist.sh   # copies the built wheels into ./dbxcarta-dist
```

Commit `dbxcarta-dist/`, `uv.toml`, and `uv.lock`. The find-links path is
relative, so the lock is portable and a new developer needs only `uv sync`. The
`uv.toml` is a single line:

```toml
find-links = ["./dbxcarta-dist"]
```

When dbxcarta is published to PyPI, delete `uv.toml` and `dbxcarta-dist/`, and the
same pins resolve from PyPI unchanged.

## Part 2: Confirm the data in Unity Catalog

A consumer points at existing data. An upstream process owns and populates the
tables, so there is nothing to generate. Confirm the catalog and schema exist,
and note their exact names. You will list them in the overlay in Part 3, and
readiness in Part 4 will confirm the catalog holds a data schema.

The same catalog can hold the dbxcarta ops artifacts (the per-run
`summary_<run_id>.json` reports on the ops volume, the uploaded question set, and
the client generation cache), so there is usually no separate ops catalog to
create. neocarta writes the run summary as a JSON report to the summary volume;
the client's generation-cache tables are created automatically on the first eval
run.

Generating your own data from a blueprint is an in-repo example concern and is
not part of the published consumer path.

## Part 3: Configure the overlay and the standalone `.env`

### The overlay

Edit `dbxcarta-overlay.env`. It is committed and must stay secret-free, so never
put `NEO4J_*` or tokens in it. The bundle forwards these key/value pairs to the
cluster as job parameters.

The overlay carries two sets of keys. The neocarta ingest wheel reads only its
own `NEOCARTA_DATABRICKS_*` contract; the operator helpers and the client read the
`DBXCARTA_*` / `DATABRICKS_*` keys. Set both sets so the same scope and catalog
reach each side. Set at least the following:

Operator and client keys:

- `DATABRICKS_SECRET_SCOPE`: the per-integration secret scope name, for example
  `dbxcarta-neo4j-<consumer>`. This is a scope name, not a secret.
- `DBXCARTA_CATALOG`: the catalog readiness and provisioning anchor on.
- `DATABRICKS_VOLUME_PATH`: the ops volume root, in exact
  `/Volumes/<catalog>/<schema>/<volume>` form.
- `DBXCARTA_CLIENT_QUESTIONS`: the local path the client reads `questions.json`
  from (defaults to the file beside the overlay).
- `DBXCARTA_CLIENT_ARMS`: the evaluation arms to run, for example
  `no_context,schema_dump,graph_rag`.

neocarta ingest contract (the ingest job reads these):

- `NEOCARTA_DATABRICKS_SECRET_SCOPE`: the same scope name as above; neocarta reads
  `NEO4J_*` from it on the cluster.
- `NEOCARTA_DATABRICKS_CATALOG` and `NEOCARTA_DATABRICKS_CATALOGS`: the anchor
  catalog and the `catalog:layer` list folded into one graph.
- `NEOCARTA_DATABRICKS_SCHEMAS`: comma-separated bare schema names, or blank to
  auto-discover every schema in the catalog.
- The embedding flags and endpoint:
  `NEOCARTA_DATABRICKS_INCLUDE_EMBEDDINGS_TABLES=true` (and the column/schema/
  database flags), `NEOCARTA_DATABRICKS_EMBEDDING_ENDPOINT`, and
  `NEOCARTA_DATABRICKS_EMBEDDING_DIMENSION`. For a cheaper first run, enable only
  the tables flag.
- `NEOCARTA_DATABRICKS_EMBEDDING_STAGING_VOLUME`: required whenever any
  include-embeddings flag is on (inline mode stages here).
- `NEOCARTA_DATABRICKS_SUMMARY_VOLUME`: where each detached run writes its
  `summary_<run_id>.json` report.

A selected overlay that does not resolve is a hard error, never a silent
fallback. `DATABRICKS_SECRET_SCOPE` has no code default, so a run with no overlay
fails loudly at config load.

### The standalone `.env`

Copy the committed template and fill in your infra and local secrets:

```bash
cp .env.sample .env
```

This file holds the Databricks profile, the warehouse ID, the cluster ID, and,
for local tooling, the `NEO4J_*` values. It is gitignored and never layered into
the overlay. Local readiness checks and the local demo read it; the cluster
jobs do not, since they read secrets from the scope.

## What each stage needs

The pipeline has one cluster job (ingest) plus local steps (readiness, then the
client evaluation). They do not share prerequisites, so it helps to see what each
one actually requires before running anything. The `dbxcarta ready` check and the
`dbxcarta-client` evaluation are local steps, not stages the ingest job enforces.

| Stage | Where it runs | What it needs | What it produces |
|-------|---------------|---------------|------------------|
| Setup | local | `uv sync` (vendored wheels), the secret scope provisioned, `.env` filled in | a working consumer |
| Readiness (optional) | local CLI | the warehouse, and `DBXCARTA_CATALOG` from the overlay | confirmation the catalog holds a data schema |
| Ingest job | cluster | catalog and schema, the ops volume, the warehouse, the secret scope, the embedding endpoint and flags, the classic cluster with the Neo4j Maven connector | the fully embedded Neo4j semantic layer, and a `summary_<run_id>.json` report on the volume |
| Eval (client) | local | the semantic layer the ingest built, the chat endpoint, the embedding endpoint, and the local `questions.json` | per-arm Text2SQL metrics printed to your terminal |
| Local demo | local | `.env`, and for `ask` the built semantic layer; it reads the bundled `questions.json` from the package, not the volume | one answered question locally |

Two points the table makes explicit:

- **Readiness is an optional preflight, not an ingest gate.** The ingest job never
  calls `dbxcarta ready`. Skip readiness and ingest still runs; it only tells you
  in advance whether the catalog has data.
- **The client reads `questions.json` locally, and ingest ignores it.** The client
  runs on your machine and opens the bundled `questions.json` directly, so there
  is no upload step and nothing stages questions to a volume. Ingest never reads
  `questions.json`.

## Part 4: Provision and run

Run these from the consumer subproject. Every step is idempotent.

1. **Sync.** dbxcarta resolves from the vendored `./dbxcarta-dist`; third-party
   deps resolve from PyPI.

   ```bash
   uv sync
   ```

2. **Provision the Neo4j secret scope.** Put `NEO4J_URI`, `NEO4J_USERNAME`, and
   `NEO4J_PASSWORD` into the scope named in the overlay. The consumer repo's
   `setup_secrets.sh` (modeled on dbxcarta's) reads the scope name from the
   overlay and the values from the standalone `.env`.

   ```bash
   ./setup_secrets.sh --profile <your-profile>
   ```

3. **Check readiness (optional).** Confirms the catalog holds at least one data
   schema beyond the auto-created `information_schema` and `default`. This is a
   preflight for your own confidence; the ingest job does not require it.

   ```bash
   uv run dbxcarta ready --env-file dbxcarta-overlay.env
   ```

4. **Deploy and run ingest, then run the client locally.** The vendored wheels
   ship to the cluster as `whl:` libraries, so there is no separate wheel-publish
   step. `run_jobs.py` reads the overlay, deploys, and runs the ingest job,
   forwarding the overlay plus the warehouse as run-time parameters. The client is
   no longer a cluster job: once ingest finishes, run it locally with
   `dbxcarta-client`, which reads the overlay and the local `questions.json`
   itself and needs no cluster.

   ```bash
   # Deploy and run the ingest job on the cluster
   uv run scripts/run_jobs.py \
     --cluster-id <cluster-id> --warehouse-id <warehouse-id>

   # After ingest finishes, run the client evaluation locally
   DBXCARTA_ENV_FILE=dbxcarta-overlay.env uv run dbxcarta-client
   ```

   Add `--target prod` or `--no-deploy` to reuse the last deployment. Running
   `databricks bundle` by hand works too, but because `databricks.yml` carries no
   config you must forward the overlay pairs and the warehouse after `--` on the
   ingest run; a bare run fails loud rather than using stale values.

The ingest job runs the neocarta connector's `neocarta-databricks-ingest` entry
point: it reads Unity Catalog metadata, embeds inline, and writes the fully
embedded Neo4j semantic graph. The client then runs locally as `dbxcarta-client`:
it benchmarks `questions.json` across the configured arms with no cluster.

## Part 5: Test and verify

- **Run the local demo.** It needs the standalone `.env` and a built semantic
  layer for the `ask` subcommand, but `questions` and a read-only `sql` query
  work against the catalog alone.

  ```bash
  uv run python -m <consumer>_dbxcarta.local_demo questions
  uv run python -m <consumer>_dbxcarta.local_demo preflight
  uv run python -m <consumer>_dbxcarta.local_demo ask --question-id fg_q01 --show-context
  ```

- **Read the client scores.** The local client prints per-arm metrics (attempted,
  parsed, executed, non_empty, exec_rate, correct_rate) to your terminal. The
  result you are checking for is `graph_rag` matching or beating `schema_dump` on
  `correct_rate`. The `no_context` arm is the zero-context baseline floor. The
  three arms are a progression, not three attempts at one task.

- **Run the non-live tests.** They run today against the vendored wheels, with no
  live Databricks or Neo4j.

  ```bash
  uv run pytest
  ```

The local demo and the non-live tests run with only `uv sync`. The ingest job
additionally requires the upstream catalog populated, the secret scope
provisioned, and a preprovisioned cluster and warehouse; the local client run
needs the built semantic layer and the chat and embedding endpoints. They cannot
complete end to end until those are in place.

## Who does what

The flow spans three places. A new developer resolves and runs entirely from the
consumer subproject; only refreshing the vendored wheels reaches into dbxcarta.

| Step | Runs in | What it does |
|------|---------|--------------|
| (maintainer) Refresh dbxcarta wheels | **dbxcarta** + consumer | Rebuild dbxcarta wheels, then `./scripts/refresh_dbxcarta_dist.sh` and commit `dbxcarta-dist/`. New developers skip this. |
| Populate the catalog | **upstream pipeline** | Create the data the semantic layer is built over. |
| Provision the secret scope | **consumer** (`setup_secrets.sh`) | Put `NEO4J_*` in the per-integration scope. |
| Resolve, configure, run, demo | **consumer subproject** | `uv sync`, the overlay, readiness, upload questions, the bundle jobs, the local demo. |

## Checklist for a new data source

- [ ] Copied the `finance-genie/dbxcarta` layout, renamed the package and `src/`
      folder.
- [ ] `pyproject.toml` pins `dbxcarta-core`, `dbxcarta-client[graph]`, and
      `neocarta[databricks-spark]` by version, with no `[tool.uv.sources]`.
- [ ] `questions.json` written beside the overlay, with `reference_sql` targeting
      your catalog on the gradable questions.
- [ ] Vendored wheels committed in `dbxcarta-dist/`, with `uv.toml` find-links and
      a committed `uv.lock`.
- [ ] `dbxcarta-overlay.env` set with the operator/client `DBXCARTA_*` keys and the
      `NEOCARTA_DATABRICKS_*` ingest contract (catalog, schemas, embedding flags +
      endpoint, staging and summary volumes, secret scope). No secrets in it.
- [ ] `.env` copied from `.env.sample` and filled in for local tooling.
- [ ] Secret scope provisioned, readiness passes.
- [ ] `databricks.yml` deployed; ingest job run, then `dbxcarta-client` run locally.
- [ ] Client scores read from the terminal; local demo and non-live tests pass.

## Where to read more

- The reference consumer:
  <https://github.com/neo4j-partners/graph-on-databricks/tree/main/finance-genie>,
  and its `dbxcarta/README.md`.
- `docs/reference/simulate-publish.md`: how a consumer resolves dbxcarta from
  vendored wheels until PyPI, and how to flip to PyPI.
- `docs/proposals/published.md`: the published-library consumer design.
- `docs/reference/architecture.md`: the package responsibilities and how the
  pieces fit, including the client cache mechanics.
- `docs/reference/pipeline.md`: where the ingest pipeline now lives (the neocarta
  connector) and what dbxcarta still owns.
- `docs/reference/best-practices.md`: the design rules the pipeline must follow.
- `docs/proposals/env-layering.md`: the full env layering model.
