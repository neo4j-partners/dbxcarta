# Adding a New Data Source

This is a step-by-step guide to standing up a new integration in dbxcarta: a new
preset, a new set of catalogs to ingest, and the checks that confirm it works.
It is written for someone who has run an existing example once and wants to add
their own.

If you have not run an example yet, read one of the example READMEs first
(`examples/finance-genie/README.md` is the most complete), then come back here.

## What you are building

A dbxcarta integration has three moving parts:

- **An example package**: a small standalone Python package under `examples/<name>/`
  that owns the configuration for one data source. It exposes a `preset` object
  and ships a demo question set.
- **An overlay env file**: `examples/<name>/dbxcarta-overlay.env`, a committed,
  secret-free file that names the catalogs, schemas, volume, and feature flags
  for this integration. It is the single source of truth for the integration's
  dbxcarta config.
- **The data itself in Unity Catalog**: either tables that already exist in your
  workspace, or tables you generate from a committed blueprint.

The dbxcarta pipeline reads Unity Catalog metadata, builds a Neo4j semantic
layer with embeddings and inferred foreign keys, then runs a Text2SQL
evaluation against it. Your job when adding a data source is to point that
pipeline at new tables and give it a question set to evaluate.

## Two kinds of data source

Pick the path that matches where your data comes from. Everything else in this
guide is shared.

- **Point at existing data** (the `finance-genie` pattern). Some upstream
  project already owns and populates the Unity Catalog tables. You only tell
  dbxcarta which catalogs and schemas to read. There is no blueprint and no
  materialize step.
- **Generate your own data** (the `dense-schema` and `schemapile` pattern). You
  commit a **blueprint** JSON that describes schemas and tables, then run a
  shared `materialize` job that creates those Delta tables in your data catalog.
  Use this when you have no upstream source and want a synthetic or sliced
  dataset.

## Concepts in one minute

- **Preset**: the `StandardPreset` object your package exposes. It provides two
  optional hooks the CLI uses: `readiness()` to confirm your catalogs hold data,
  and `upload_questions()` to push your `questions.json` to the ops volume. The
  preset carries no config; config lives in the overlay.
- **Base `.env`**: the repo-root file with shared Databricks infra and the Neo4j
  secrets. You never edit it per integration.
- **Overlay**: `examples/<name>/dbxcarta-overlay.env`. Committed, secret-free,
  per-integration. Selected with `DBXCARTA_ENV_FILE` or `--env-file`. Precedence
  is process env over overlay over base.
- **Ops plane**: a catalog, schema, and volume that hold run summaries, the
  question set, and caches. Created by `bootstrap`. Separate from your data.
- **Blueprint**: a committed JSON of schemas and tables, used only on the
  generate-your-own-data path, materialized into Delta tables by the shared
  `materialize` job.

## Prerequisites

- A Databricks profile that can create catalogs, schemas, and volumes.
- A Databricks SQL warehouse ID.
- Neo4j connection details for the secret scope.
- The repo synced: `uv sync` from the repo root.

## Part 1: Create the example package

Copy an existing example and rename it. `dense-schema` is the best starting
point if you will generate data; `finance-genie` is best if you point at
existing data.

```bash
# Copy an existing example as your starting layout
cp -r examples/finance-genie examples/my-source
```

Then change the following:

- **Package name** in `examples/my-source/pyproject.toml` and the `src/` folder
  name (for example `src/dbxcarta_my_source_example/`).
- **`preset.py`** stays a one-liner. It constructs the shared `StandardPreset`
  with the bundled question file:

  ```python
  from pathlib import Path
  from dbxcarta.core.presets import StandardPreset

  preset = StandardPreset(questions_file=Path(__file__).resolve().parents[2] / "questions.json")

  __all__ = ["preset"]
  ```

- **`__init__.py`** re-exports `preset`.
- **`questions.json`**: your demo question set. Each item is an object with a
  non-empty `question_id` and `question`. Add `reference_sql` for any question
  you want graded for correctness:

  ```json
  [
    {
      "question_id": "ms_q01",
      "question": "How many customers are there?",
      "notes": "single base table count",
      "reference_sql": "SELECT COUNT(*) AS customer_count FROM ..."
    }
  ]
  ```

Install the package so the CLI can resolve your preset by import path:

```bash
# Install the new example package in editable mode
uv pip install -e examples/my-source/
```

The preset is now resolvable at `dbxcarta_my_source_example:preset`.

## Part 2: Get the data into Unity Catalog

### Path A: Point at existing data

If an upstream process already created your tables, there is nothing to
generate. Confirm the catalogs and schemas exist, and note their exact names.
You will list them in the overlay in Part 3, and readiness in Part 4 will
confirm each catalog holds a data schema.

### Path B: Generate your own data from a blueprint

1. Write a blueprint JSON under `examples/my-source/blueprint/` describing the
   schemas and tables you want. Commit it. It is the source of truth for the
   dataset. Existing examples ship one file, for example
   `blueprint/candidates_500.json`.
2. Bootstrap and materialize are covered in Part 4. The `materialize` command
   stages the committed blueprint to the ops volume and submits the shared
   `dbxcarta-materialize` job, which creates the schemas and Delta tables in the
   data catalog that `bootstrap` created.

Blueprint generation and question generation are example-local concerns. They
are not part of the `dbxcarta-spark` or `dbxcarta-client` public APIs.
`materialize` is the one shared product step every generate-your-own example
uses.

## Part 3: Configure the overlay

Edit `examples/my-source/dbxcarta-overlay.env`. It is committed and must stay
secret-free, so never put `NEO4J_*` or tokens in it. The submit path forwards
these key/value pairs to the cluster as job parameters.

Set at least the following:

- `DATABRICKS_SECRET_SCOPE` — the per-integration secret scope name, for example
  `dbxcarta-neo4j-my-source`. This is a scope name, not a secret.
- `DBXCARTA_CATALOG` — the single anchor catalog used for preflight, verify, and
  ops.
- `DBXCARTA_CATALOGS` — comma-separated catalogs to ingest. Each entry is
  `catalog` or `catalog:layer`. Example:
  `my-source-silver:silver,my-source-gold:gold`.
- `DBXCARTA_SCHEMAS` — comma-separated bare schema names, or blank to
  auto-discover every schema in the catalogs.
- `DATABRICKS_VOLUME_PATH` — the ops volume root, in exact
  `/Volumes/<catalog>/<schema>/<volume>` form.
- `DBXCARTA_SUMMARY_TABLE` — fully qualified `catalog.schema.table` for run
  history.
- `DBXCARTA_CLIENT_QUESTIONS` — the volume path your `questions.json` is uploaded
  to.
- `DBXCARTA_CLIENT_ARMS` — the evaluation arms to run, for example
  `no_context,schema_dump,graph_rag`.
- `DBXCARTA_TEARDOWN_TARGET` — what `teardown` is allowed to drop, for example
  `catalog:my-source-data,schema:dbxcarta-catalog.my_source_ops`.
- The embedding flags you want, for example
  `DBXCARTA_INCLUDE_EMBEDDINGS_TABLES=true`. For a cheaper first run, enable only
  the tables flag.

Select the overlay for every dbxcarta command in this session:

```bash
# Point every dbxcarta command at your overlay for this shell
export DBXCARTA_ENV_FILE=examples/my-source/dbxcarta-overlay.env
```

A selected overlay that does not resolve is a hard error, never a silent
fallback to base-only. `DATABRICKS_SECRET_SCOPE` has no code default, so a run
with no overlay fails loudly at config load.

## Part 4: Provision and ingest

Run these from the repo root. Every step is idempotent, so re-running is safe.

1. **Bootstrap the ops plane.** Creates the ops catalog, schema, and volume from
   `DATABRICKS_VOLUME_PATH`. On the generate-your-own path it also creates the
   data catalog from `DBXCARTA_CATALOG`.

   ```bash
   uv run dbxcarta-submit bootstrap
   ```

2. **Materialize the data** (generate-your-own path only). Stages the committed
   blueprint and runs the serverless materialize job.

   ```bash
   uv run dbxcarta-submit materialize
   ```

3. **Provision the Neo4j secret scope.** `setup_secrets.sh` reads the scope name
   from the overlay and the `NEO4J_*` values from the integration's standalone
   `.env`.

   ```bash
   ./setup_secrets.sh --profile <your-profile>
   ```

4. **Check readiness.** Confirms each ingested catalog holds at least one data
   schema beyond the auto-created `information_schema` and `default`.

   ```bash
   uv run dbxcarta preset dbxcarta_my_source_example:preset --check-ready
   ```

5. **Upload the question set.** Pushes `questions.json` to
   `DBXCARTA_CLIENT_QUESTIONS`.

   ```bash
   uv run dbxcarta preset dbxcarta_my_source_example:preset --upload-questions
   ```

6. **Build and ship the wheels.** Rebuilds the per-package wheels from current
   source and ships the bootstrap script.

   ```bash
   uv run dbxcarta-submit publish-wheels
   ```

7. **Submit the ingest job.** Builds the Neo4j semantic layer. Ingest requires a
   cluster, since the Neo4j connector is not supported on serverless.

   ```bash
   uv run dbxcarta-submit submit-entrypoint ingest
   ```

## Part 5: Test and verify

- **Verify the semantic layer.** Checks node counts, catalog against graph, and
  embeddings. Defaults to the most recent successful run.

  ```bash
  uv run dbxcarta verify
  ```

- **Run the client evaluation.** Submits the Text2SQL evaluation across the arms
  in `DBXCARTA_CLIENT_ARMS`.

  ```bash
  uv run dbxcarta-submit submit-entrypoint client
  ```

- **Read the scores.** The job reports `SUCCESS`, but the per-arm scores are in
  the job stdout. Print them with the run ID the submit step echoes:

  ```bash
  uv run dbxcarta-submit logs <run-id>
  ```

  The result you are checking for is `graph_rag` matching or beating
  `schema_dump` on `correct_rate`. The `no_context` arm is the zero-context
  baseline floor.

- **Run the project test suites.**

  ```bash
  make test        # fast unit and light integration suite
  make test-it     # live integration tests; needs a loaded catalog
  make test-slow   # slow Spark FK discovery regression guard
  ```

Once setup is in place, the loop you repeat on every code change is just two
make targets. Run ingest first, let it finish, then run the client:

```bash
make e2e-my-source-ingest   # add these targets to the Makefile alongside the others
make e2e-my-source-client
```

To add those targets, copy the `e2e-dense-schema-*` block in the `Makefile` and
change the example name and overlay path.

## Dry runs and cleanup

- **Dry-run bootstrap or materialize** to print the plan without changing
  anything:

  ```bash
  uv run dbxcarta-submit bootstrap --dry-run
  uv run dbxcarta-submit materialize --dry-run
  ```

- **Tear down** drops only `DBXCARTA_TEARDOWN_TARGET` and requires an explicit
  confirmation flag, so it never drops silently:

  ```bash
  uv run dbxcarta-submit teardown --yes-i-mean-it
  ```

## Checklist for a new data source

- [ ] Copied an example, renamed the package and `src/` folder.
- [ ] `preset.py` constructs `StandardPreset` with the bundled `questions.json`.
- [ ] `questions.json` written, with `reference_sql` on the gradable questions.
- [ ] Package installed with `uv pip install -e examples/my-source/`.
- [ ] Data is in Unity Catalog, either upstream-owned or materialized from a
      committed blueprint.
- [ ] `dbxcarta-overlay.env` set with catalogs, schemas, volume, summary table,
      secret scope, client questions, and arms. No secrets in it.
- [ ] `DBXCARTA_ENV_FILE` exported to the overlay.
- [ ] Ops plane bootstrapped, secrets provisioned, readiness passes, questions
      uploaded.
- [ ] Wheels published, ingest submitted, `verify` passes.
- [ ] Client evaluation run and scores read from the job logs.
- [ ] `make e2e-my-source-*` targets added to the Makefile.

## Where to read more

- `examples/finance-genie/README.md`: the most complete point-at-existing-data
  walkthrough.
- `examples/dense-schema/README.md`: the generate-your-own-data and blueprint
  walkthrough.
- `docs/reference/pipeline.md`: what the ingest pipeline does, stage by stage.
- `docs/reference/best-practices.md`: the design rules the pipeline must follow.
- `docs/proposals/env-layering.md`: the full env layering model.
