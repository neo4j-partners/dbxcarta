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

## What you are building

A consumer subproject has four moving parts:

- **A consumer Python package**: a small standalone package (for example
  `<consumer>/dbxcarta/`) whose `pyproject.toml` pins dbxcarta by version with no
  `[tool.uv.sources]`. It exposes a `preset` object, ships a `questions.json`,
  and carries a read-only local demo CLI.
- **An overlay env file**: `dbxcarta-overlay.env`, a committed, secret-free file
  that names the catalog, schema, volume, and feature flags for this data source.
  It is the single source of truth for the integration's dbxcarta config.
- **A consumer-owned Databricks Asset Bundle**: a `databricks.yml` with two
  `python_wheel_task` jobs that run the published `dbxcarta-ingest` and
  `dbxcarta-client` entry points on a cluster. This replaces `dbxcarta-submit`,
  which is operator-only, unpublished, and rebuilds wheels from the dbxcarta
  source tree a consumer does not have.
- **The data itself in Unity Catalog**: tables an upstream process already owns
  and populates. A consumer points at existing data. There is no blueprint and
  no materialize step.

The dbxcarta pipeline reads Unity Catalog metadata, builds a Neo4j semantic layer
with embeddings and inferred foreign keys, then runs a Text2SQL evaluation
against it. Your job when adding a data source is to point that pipeline at your
tables, give it a question set, and run the two jobs from your bundle.

## How dbxcarta reaches your project

dbxcarta publishes three packages a consumer pins by version:

- `dbxcarta-core`: the foundation. `StandardPreset`, the env loader, and the job
  entry-point plumbing.
- `dbxcarta-spark`: `SparkIngestSettings`, `run_dbxcarta`, and the `dbxcarta` and
  `dbxcarta-ingest` console entry points.
- `dbxcarta-client[graph]`: the Text2SQL evaluation harness, the `dbxcarta-client`
  entry point, and the Neo4j driver via the `[graph]` extra.

While PyPI publishing is unavailable, these wheels are **vendored** into the
consumer repo and resolved locally with a relative find-links path. The
consumer's `pyproject.toml` still pins normal versions with no source overrides,
so it is identical to the eventual published case. Only where uv looks for the
wheels changes. See `dbxcarta/docs/reference/simulate-publish.md` for the full
mechanism, and "Vendored wheels" below for the consumer-side steps.

## Concepts in one minute

- **Preset**: the `StandardPreset` object your package exposes. `StandardPreset`
  itself is implemented in `dbxcarta-core`, so your `preset.py` only supplies the
  `questions.json` path; the behavior is inherited from the library. It provides
  two hooks: `readiness()` to confirm your catalog holds data, and
  `upload_questions()` to push your `questions.json` to the ops volume. Both run
  locally through the `dbxcarta preset` CLI only. The cluster jobs never call the
  preset, so it carries no config; config lives in the overlay.
- **Overlay**: `dbxcarta-overlay.env`. Committed, secret-free, per-integration.
  Selected with `DBXCARTA_ENV_FILE` or `--env-file`. It holds dbxcarta-scoped
  values only: catalog, schema, volume, summary table, embedding flags, client
  arms, and the per-integration `DATABRICKS_SECRET_SCOPE` (a scope name, not a
  secret).
- **Standalone `.env`**: the consumer's own infra and secrets for local tooling
  (the local demo, readiness checks). It holds the Databricks profile, the
  warehouse, the cluster ID, and, for local use, the `NEO4J_*` connection
  values. It is gitignored. Copy it from the committed `.env.sample`.
- **Consumer DAB**: `databricks.yml`. Two jobs run the published entry points on
  a preprovisioned classic cluster. It carries no dbxcarta config: `run_jobs.py`
  forwards the overlay's `KEY=VALUE` pairs to each job at run time, so the overlay
  is the single source and the two cannot diverge. The entry points parse those
  pairs into the environment, then fetch `NEO4J_*` from the secret scope.
- **Vendored wheels**: `dbxcarta-dist/`, a committed directory of dbxcarta wheels
  plus a committed `uv.toml` find-links. It is the simulated index until dbxcarta
  is on PyPI, reached both by local `uv sync` and by the cluster jobs.
- **Secret scope**: a Databricks secret scope holding `NEO4J_URI`,
  `NEO4J_USERNAME`, and `NEO4J_PASSWORD`. The ingest entry point reads these from
  the scope itself on the cluster, given the `DATABRICKS_SECRET_SCOPE` parameter.

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
├── databricks.yml              # ingest + client jobs (consumer-owned DAB, config-free)
├── dbxcarta-overlay.env        # committed, secret-free dbxcarta config (single source)
├── .env.sample                 # standalone local-demo config (copy to .env)
├── setup_secrets.sh            # provision the Neo4j secret scope from the overlay + .env
├── questions.json              # eval fixture for your tables
├── uv.toml                     # committed find-links to ./dbxcarta-dist
├── dbxcarta-dist/              # vendored dbxcarta wheels (committed)
├── scripts/refresh_dbxcarta_dist.sh  # maintainer: refresh dbxcarta-dist
├── scripts/run_jobs.py         # deploy + run ingest then client
├── src/<consumer>_dbxcarta/
│   ├── __init__.py             # re-exports `preset`
│   ├── preset.py               # StandardPreset(questions_file=...)
│   └── local_demo.py           # read-only local CLI
└── tests/                      # non-live tests
```

Then change the following:

- **`pyproject.toml`**: pin the three dbxcarta packages by version, with no
  `[tool.uv.sources]`. This is the whole point of the published-library
  approach.

  ```toml
  dependencies = [
      "dbxcarta-core==1.1.0",
      "dbxcarta-spark==1.1.0",
      "dbxcarta-client[graph]==1.1.0",
      "databricks-sdk>=0.40",
      "python-dotenv",
  ]
  ```

- **`preset.py`** stays a one-liner. It constructs the shared `StandardPreset`
  with the bundled question file:

  ```python
  from pathlib import Path
  from dbxcarta.core.presets import StandardPreset

  preset = StandardPreset(questions_file=Path(__file__).resolve().parents[2] / "questions.json")

  __all__ = ["preset"]
  ```

  `StandardPreset` is defined in `dbxcarta-core`, so `readiness()` and
  `upload_questions()` come with the library. Your `preset.py` only points it at
  your `questions.json`. There is nothing to implement here, and nothing about
  the preset is required by the ingest job.

- **`__init__.py`** re-exports `preset`.
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

While dbxcarta is not on PyPI, commit a copy of its wheels into `dbxcarta-dist/`
and point uv at them with a relative find-links path. A maintainer builds the
wheels in the dbxcarta checkout and refreshes the vendored copy:

```bash
# In the dbxcarta checkout (maintainer only):
uv build --package dbxcarta-core
uv build --package dbxcarta-spark
uv build --package dbxcarta-client

# In the consumer subproject:
./scripts/refresh_dbxcarta_dist.sh   # copies dbxcarta/dist/* into ./dbxcarta-dist
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

The same catalog can hold the dbxcarta ops artifacts (the run-summary table, the
uploaded question set, and the generation cache on the ops volume), so there is
usually no separate ops catalog to create. The run-summary table is created
automatically on the first ingest.

Generating your own data from a blueprint is an in-repo example concern and is
not part of the published consumer path.

## Part 3: Configure the overlay and the standalone `.env`

### The overlay

Edit `dbxcarta-overlay.env`. It is committed and must stay secret-free, so never
put `NEO4J_*` or tokens in it. The bundle forwards these key/value pairs to the
cluster as job parameters.

Set at least the following:

- `DATABRICKS_SECRET_SCOPE`: the per-integration secret scope name, for example
  `dbxcarta-neo4j-<consumer>`. This is a scope name, not a secret.
- `DBXCARTA_CATALOG`: the catalog the semantic layer is built over.
- `DBXCARTA_SCHEMAS`: comma-separated bare schema names, or blank to
  auto-discover every schema in the catalog.
- `DATABRICKS_VOLUME_PATH`: the ops volume root, in exact
  `/Volumes/<catalog>/<schema>/<volume>` form.
- `DBXCARTA_SUMMARY_VOLUME` and `DBXCARTA_SUMMARY_TABLE`: the run-summary volume
  path and the fully qualified `catalog.schema.table` for run history.
- `DBXCARTA_CLIENT_QUESTIONS`: the volume path your `questions.json` is uploaded
  to.
- `DBXCARTA_CLIENT_ARMS`: the evaluation arms to run, for example
  `no_context,schema_dump,graph_rag`.
- The embedding flags and endpoint, for example
  `DBXCARTA_INCLUDE_EMBEDDINGS_TABLES=true`, `DBXCARTA_EMBEDDING_ENDPOINT`, and
  `DBXCARTA_EMBEDDING_DIMENSION`. For a cheaper first run, enable only the tables
  flag.

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

The pipeline has two cluster jobs (ingest, then eval) plus local helpers. They do
not share prerequisites, so it helps to see what each one actually requires before
running anything. The preset hooks (`readiness`, `upload_questions`) are local CLI
steps, not stages the jobs enforce.

| Stage | Where it runs | What it needs | What it produces |
|-------|---------------|---------------|------------------|
| Setup | local | `uv sync` (vendored wheels), the secret scope provisioned, `.env` filled in | a working consumer |
| Readiness (optional) | local CLI | the warehouse, and `DBXCARTA_CATALOG` from the overlay | confirmation the catalog holds a data schema |
| Ingest job | cluster | catalog and schema, the ops volume, the warehouse, the secret scope, the embedding endpoint and flags, the classic cluster with the Neo4j Maven connector | the Neo4j semantic layer, embeddings, and the run-summary table |
| Upload questions | local CLI | `questions.json` and `DBXCARTA_CLIENT_QUESTIONS` from the overlay | `questions.json` on the ops volume |
| Eval (client) job | cluster | the semantic layer the ingest built, the chat endpoint, the embedding endpoint, and `questions.json` already on the volume | per-arm Text2SQL metrics |
| Local demo | local | `.env`, and for `ask` the built semantic layer; it reads the bundled `questions.json` from the package, not the volume | one answered question locally |

Two points the table makes explicit:

- **Readiness is an optional preflight, not an ingest gate.** The ingest job never
  calls the preset. Skip readiness and ingest still runs; it only tells you in
  advance whether the catalog has data.
- **Uploading questions is a prerequisite of the eval job, not of ingest.** The
  client job reads `questions.json` from the volume, so upload it before that job.
  Ingest ignores `questions.json` entirely.

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
   uv run dbxcarta preset <consumer>_dbxcarta:preset --check-ready \
     --env-file dbxcarta-overlay.env
   ```

4. **Upload the question set.** Pushes `questions.json` to
   `DBXCARTA_CLIENT_QUESTIONS` on the volume. This is a prerequisite of the eval
   job, which reads questions from that path. It is not needed for ingest, so you
   can do it any time before the client job.

   ```bash
   uv run dbxcarta preset <consumer>_dbxcarta:preset --upload-questions \
     --env-file dbxcarta-overlay.env
   ```

5. **Deploy and run the bundle.** The vendored wheels ship to the cluster as
   `whl:` libraries, so there is no separate wheel-publish step. `run_jobs.py` is
   the supported path: it reads the overlay, deploys, then runs ingest, then the
   client after ingest finishes (each `bundle run` blocks), forwarding the overlay
   plus the warehouse to each job as run-time parameters.

   ```bash
   uv run scripts/run_jobs.py \
     --cluster-id <cluster-id> --warehouse-id <warehouse-id>
   ```

   Add `--target prod`, `--no-deploy` to reuse the last deployment, or
   `--no-client` to stop after ingest. Running `databricks bundle` by hand works
   too, but because `databricks.yml` carries no config you must forward the
   overlay pairs and the warehouse after `--` on each run; a bare run fails loud
   rather than using stale values.

The ingest job runs the `dbxcarta-ingest` entry point: it reads Unity Catalog
metadata, writes embeddings and the Neo4j semantic graph, and creates the
run-summary table. The client job runs `dbxcarta-client`: it benchmarks
`questions.json` across the configured arms.

## Part 5: Test and verify

- **Run the local demo.** It needs the standalone `.env` and a built semantic
  layer for the `ask` subcommand, but `questions` and a read-only `sql` query
  work against the catalog alone.

  ```bash
  uv run python -m <consumer>_dbxcarta.local_demo questions
  uv run python -m <consumer>_dbxcarta.local_demo preflight
  uv run python -m <consumer>_dbxcarta.local_demo ask --question-id fg_q01 --show-context
  ```

- **Read the client scores.** The client job reports per-arm metrics (attempted,
  parsed, executed, non_empty, exec_rate, correct_rate) in its job output. The
  result you are checking for is `graph_rag` matching or beating `schema_dump` on
  `correct_rate`. The `no_context` arm is the zero-context baseline floor. The
  three arms are a progression, not three attempts at one task.

- **Run the non-live tests.** They run today against the vendored wheels, with no
  live Databricks or Neo4j.

  ```bash
  uv run pytest
  ```

The local demo and the non-live tests run with only `uv sync`. The ingest and
client jobs additionally require the upstream catalog populated, the secret scope
provisioned, and a preprovisioned cluster and warehouse. They cannot complete end
to end until those are in place.

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
- [ ] `pyproject.toml` pins `dbxcarta-core`, `dbxcarta-spark`, and
      `dbxcarta-client[graph]` by version, with no `[tool.uv.sources]`.
- [ ] `preset.py` constructs `StandardPreset` with the bundled `questions.json`.
- [ ] `questions.json` written, with `reference_sql` targeting your catalog on
      the gradable questions.
- [ ] Vendored wheels committed in `dbxcarta-dist/`, with `uv.toml` find-links and
      a committed `uv.lock`.
- [ ] `dbxcarta-overlay.env` set with catalog, schema, volume, summary table,
      secret scope, client questions, and arms. No secrets in it.
- [ ] `.env` copied from `.env.sample` and filled in for local tooling.
- [ ] Secret scope provisioned, readiness passes, questions uploaded.
- [ ] `databricks.yml` deployed; ingest job run, then the client job.
- [ ] Client scores read from the job output; local demo and non-live tests pass.

## Where to read more

- The reference consumer:
  <https://github.com/neo4j-partners/graph-on-databricks/tree/main/finance-genie>,
  and its `dbxcarta/README.md`.
- `docs/reference/simulate-publish.md`: how a consumer resolves dbxcarta from
  vendored wheels until PyPI, and how to flip to PyPI.
- `docs/proposals/published.md`: the published-library consumer design.
- `docs/reference/public-api.md`: the stable public surfaces and the version
  contract a consumer pins against.
- `docs/reference/pipeline.md`: what the ingest pipeline does, stage by stage.
- `docs/reference/best-practices.md`: the design rules the pipeline must follow.
- `docs/proposals/env-layering.md`: the full env layering model.
</content>
</invoke>
