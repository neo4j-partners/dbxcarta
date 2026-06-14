# dbxcarta Finance Genie Example

This directory is a standalone Python package, `dbxcarta-finance-genie-example`,
that shows how an outside application can use dbxcarta without copying runner
scripts. Its primary job is to own Finance Genie configuration. That
configuration is the committed `dbxcarta-overlay.env` plus the bundled
`questions.json`; the dbxcarta CLI reads both directly.

## Quick Start

The full flow from a clean checkout to a scored client run, all from the repo
root. The first block is one-time setup; the two make targets are the loop you
repeat on every change. Each numbered section under [Setup Flow](#setup-flow)
explains a step in more detail.

```bash
# Install dbxcarta into the workspace virtualenv
uv sync
# Install this example package in editable mode
uv pip install -e examples/finance-genie/

# Select the Finance Genie overlay for every dbxcarta command
export DBXCARTA_ENV_FILE=examples/finance-genie/dbxcarta-overlay.env

# Confirm each ingested catalog holds a data schema; section 2 creates them
uv run dbxcarta ready

# Provision the ops plane: catalog, finance_genie_ops schema, dbxcarta-ops volume
uv run dbxcarta bootstrap

# Provision the Neo4j secret scope
./setup_secrets.sh --profile aws-partner-rk
```

The demo question set ships as `questions.json` beside the overlay and the
client reads it locally, so there is no question-upload step.

With setup in place, run the two make targets from the repo root. The `-ingest`
target rebuilds the wheels from current source and builds the semantic layer, so
run it first and let it finish, then run `-client`:

```bash
# Rebuild the wheels from source, submit ingest, build the semantic layer
make e2e-finance-genie-ingest
# Run the client evaluation locally once ingest finishes
make e2e-finance-genie-client
```

The upstream Finance Genie data is owned by the upstream project and created
once per workspace; readiness confirms each ingested catalog holds a data
schema. See [section 2](#2-prepare-finance-genie). The other setup steps are
idempotent, so re-running them is safe. After setup, every code change replays
through the two make targets alone.

### Find the results

The `-client` make target submits a Databricks job and reports `SUCCESS` when the
job finishes, but the actual evaluation scores are in the job's stdout, not in
the make output. Print them with the run ID the submit step echoes:

```bash
# Print the client job's stdout, where the evaluation scores are
uv run dbxcarta logs <run-id>
```

The summary is one line per arm:

```
[dbxcarta_client] run_id=... job=dbxcarta_client status=success catalog=...
  no_context:  attempted=12 parsed=12 executed=2  non_empty=2  exec_rate=16.7%  non_empty_rate=16.7%  correct_rate=100.0%
  schema_dump: attempted=12 parsed=12 executed=12 non_empty=12 exec_rate=100.0% non_empty_rate=100.0% correct_rate=91.7%
  graph_rag:   attempted=12 parsed=12 executed=12 non_empty=12 exec_rate=100.0% non_empty_rate=100.0% correct_rate=100.0%
```

Reading a line:

- `attempted` — questions sent to the arm.
- `parsed` — responses that yielded valid SQL.
- `executed` — generated SQL that ran on the warehouse without error.
- `non_empty` — executed queries that returned at least one row.
- `exec_rate` / `non_empty_rate` — `executed` / `non_empty` over `attempted`.
- `correct_rate` — fraction of gradable questions whose result set matched the
  reference SQL (only questions that carry a reference are gradable).

The three arms are a progression, not three attempts at the same task. `no_context`
is a deliberate zero-context baseline: it gives the model only the question and the
catalog and schema names, no tables or columns, so a low `exec_rate` is expected and
is the floor you compare against. `schema_dump` adds a token-capped schema dump, and
`graph_rag` adds context retrieved from the knowledge graph. `graph_rag` matching or
beating `schema_dump` on `correct_rate` is the result the run is checking for.

The harness does not enforce a pass/fail threshold on these rates; it reports them
for you to read. The same numbers, plus per-question detail, are persisted two ways:

- A JSON file per run under `DBXCARTA_SUMMARY_VOLUME`
  (`/Volumes/dbxcarta-catalog/finance_genie_ops/dbxcarta-ops/dbxcarta/runs`),
  named `<job>_<run-id>_<timestamp>.json`, including every question's generated
  SQL and result.
- A run-history Delta table `DBXCARTA_SUMMARY_TABLE`
  (`dbxcarta-catalog.finance_genie_ops.dbxcarta_run_summary`), one row per run,
  for comparing arms across runs in SQL.

## What lives here, and why

```
examples/finance-genie/
├── pyproject.toml                                         # standalone package
├── README.md                                              # this file
├── dbxcarta-overlay.env                                   # per-example dbxcarta config (single source of truth)
├── .env.sample                                            # standalone local-demo config reference
├── questions.json                                         # demo question fixture
├── src/dbxcarta_finance_genie_example/
│   ├── __init__.py                                        # package marker
│   └── local_demo.py                                      # optional read-only local CLI
└── ../../../tests/examples/finance-genie/
    ├── test_overlay.py
    └── test_local_demo.py
```

The package declares dbxcarta as a normal pip dependency in `pyproject.toml`.
Inside this repo, `uv` resolves it to the editable parent through
`[tool.uv.sources]`; from outside, you would pin it like
`dependencies = ["dbxcarta-core", "dbxcarta-client[graph]"]` and pip would fetch
those distributions from your wheel index. The ingest connector itself
(`neocarta[databricks-spark]`) is not a local dependency: the submit step stages
its wheel onto the cluster. There is nothing privileged about the example's
relationship to the dbxcarta workspace.

The Finance Genie per-example config lives in the committed
`examples/finance-genie/dbxcarta-overlay.env` (the single source of truth). The
ingest pipeline itself is the neocarta connector, submitted from that overlay:
`dbxcarta submit-entrypoint ingest` stages the neocarta wheel and runs its
`neocarta-databricks-ingest` entrypoint on the cluster, reading the
`NEOCARTA_DATABRICKS_*` contract keys the overlay forwards as job parameters.
There is no dbxcarta Python ingest object to import.

The CLI reads the overlay and the bundled `questions.json` directly. There is no
per-example Python object to publish. The two operational commands are:

- `dbxcarta ready` prints a `ReadinessReport` describing whether each ingested
  catalog holds a data schema in Unity Catalog. The catalog list comes from the
  loaded overlay.
- `dbxcarta-client` runs the Text2SQL evaluation locally, reading `questions.json`
  straight off local disk. It opens the file beside the selected overlay, at the
  path named by `DBXCARTA_CLIENT_QUESTIONS`. There is no upload step.

## Template guidance for a new application package

To build your own application package, copy this layout and change:

1. The package name in `pyproject.toml` and the src folder name.
2. The catalog list (and any `:layer` suffixes) and dbxcarta features in
   `dbxcarta-overlay.env`, the single source of truth for per-example config.
3. The `questions.json` fixture, if you want a demo question set.

No Python wiring is needed for the CLI: `dbxcarta ready` and `dbxcarta-client`
read the selected overlay and the adjacent `questions.json` directly, so a new
application package can be just an overlay and a questions file. The `src/` package is only for standalone tooling like the local demo.

## Responsibility Boundary

Finance Genie (upstream project) owns:

- Synthetic finance data generation.
- Unity Catalog base tables: `accounts`, `merchants`, `transactions`,
  `account_links`, `account_labels`.
- Neo4j GDS enrichment for the finance demo.
- Unity Catalog Gold tables: `gold_accounts`,
  `gold_account_similarity_pairs`, `gold_fraud_ring_communities`.
- Genie Spaces and demo validation.

dbxcarta (via this example's overlay) owns:

- Unity Catalog metadata extraction from `information_schema`.
- Table, column, value, schema, and database embeddings.
- Sample-value nodes and inferred `REFERENCES` edges.
- Neo4j semantic-layer writes and vector indexes.
- GraphRAG schema retrieval and Text2SQL evaluation.

The ops run-summary table (`DBXCARTA_SUMMARY_TABLE`) is created automatically
on the first run that writes to it (materialize and the client evaluation),
from the writer's own schema. It holds run history only, not source data, so it
is disposable: drop it to reset, and the next run recreates it with the current
schema.

## Quick iterate loop (testing dbxcarta changes)

Once the one-time prerequisites are in place (steps 1–8 below: example package
installed, ops plane bootstrapped, secrets refreshed, questions uploaded,
upstream UC tables present), the pipeline runs in two make targets from the
repo root —
ingest first, then the client evaluation once ingest finishes:

```bash
# Rebuild the wheels from source, submit ingest, build the semantic layer
make e2e-finance-genie-ingest
# Run the client evaluation locally once ingest finishes
make e2e-finance-genie-client
```

The `-ingest` target rebuilds the wheels from your current source, then
submits `ingest`; the `-client` target submits `client`, so it reflects
local edits to the dbxcarta packages on every run. The targets set
`DBXCARTA_ENV_FILE` to this directory's
`dbxcarta-overlay.env` inline on each command, so they pick up the right
dbxcarta config no matter what shell you run them from. They do **not** use
this directory's standalone `./.env` (that file is only for the local
demo in section 11). `make help` lists the targets for every example.

The sections below are the full first-time setup and the individual
commands the target wraps.

## Setup Flow

Run these commands from the dbxcarta repo unless a step says otherwise.

### 1. Install dbxcarta and the example package

```bash
# Install dbxcarta into the workspace virtualenv
uv sync
# Install this example package in editable mode
uv pip install -e examples/finance-genie/
```

### 2. Prepare Finance Genie

From `finance-genie/enrichment-pipeline` (the upstream project), run the
Finance Genie setup path that generates data, creates Unity Catalog tables,
runs GDS, and writes Gold tables.

The validated UC scope for this workspace is the
`graph-enriched-finance-silver` and `graph-enriched-finance-gold` catalogs,
each holding the `graph-enriched-schema` schema.

The upstream project writes its curated tables to the silver catalog:

- `accounts`, `merchants`, `transactions`, `account_links`, `account_labels`

and its graph-enriched tables to the gold catalog:

- `gold_accounts`, `gold_account_similarity_pairs`,
  `gold_fraud_ring_communities`

dbxcarta readiness does not check these tables individually; it confirms each
catalog holds a data schema, and the upstream project owns table-level
validation.

### 3. Configure dbxcarta

dbxcarta loads config in two layers. The repo-root `.env` is the shared
**base**: Databricks infra and the Neo4j secrets, never edited per
integration. This directory's committed, secret-free
`dbxcarta-overlay.env` is the Finance Genie **overlay**: only the
dbxcarta-scoped values (medallion catalogs, schemas, volume, summary,
sample/embedding flags, client arms). Select it by exporting
`DBXCARTA_ENV_FILE` once. No root `.env` edit, ever:

```bash
# Select the Finance Genie overlay for every dbxcarta command
export DBXCARTA_ENV_FILE=examples/finance-genie/dbxcarta-overlay.env
```

Every `dbxcarta` command below then picks it up. Precedence is
process env over overlay over base. With `DBXCARTA_ENV_FILE` unset,
only the base `.env` loads, exactly as before.

This overlay is the single source of truth for the example's dbxcarta config;
edit it directly to change catalogs, schemas, flags, or the secret scope.

This file (`dbxcarta-overlay.env`) is the dbxcarta CLI overlay only. It
is distinct from `./.env` / `./.env.sample`, which are the self-contained
config for the standalone local demo (section 11) and never layer.

### 4. Check readiness

Readiness confirms each ingested catalog (silver and gold) holds at least one
data schema beyond the auto-created `information_schema` and `default`:

```bash
# Confirm each ingested catalog holds at least one data schema
uv run dbxcarta ready
```

It does not check individual tables; the upstream project owns and validates
those. A catalog holding only `information_schema` and an empty `default`
reports not ready.

### 5. Bootstrap the ops plane

Create the ops catalog, the `finance_genie_ops` schema, and the `dbxcarta-ops`
volume that hold run summaries, the generation cache, and the uploaded question
set. `bootstrap` reads the overlay's `DATABRICKS_VOLUME_PATH`, is idempotent, and
does not create the upstream medallion data catalogs:

```bash
# Create the ops catalog, finance_genie_ops schema, and dbxcarta-ops volume
uv run dbxcarta bootstrap
```

`bootstrap` checks whether the catalog already exists before issuing
`CREATE CATALOG` and skips the create when it does. On accounts with Default
Storage and no metastore storage root, `CREATE CATALOG` is rejected without a
`MANAGED LOCATION` even with `IF NOT EXISTS`, so a pre-created catalog (for
example one created in the workspace UI) must not be re-created. The schema and
volume creates remain `IF NOT EXISTS`.

To remove only the ops schema later, without touching the shared ops catalog,
run `teardown` (it drops the overlay's `DBXCARTA_TEARDOWN_TARGET`,
`schema:dbxcarta-catalog.finance_genie_ops`):

```bash
# Drop only the finance_genie_ops schema, leaving the shared ops catalog intact
uv run dbxcarta teardown --yes-i-mean-it
```

### 6. Refresh Neo4j secrets

dbxcarta jobs read Neo4j credentials from the Databricks secret scope:

```bash
# Provision the Databricks secret scope with Neo4j credentials
./setup_secrets.sh --profile aws-partner-rk
```

### 7. Questions: read locally, no upload

The client runs locally and reads the bundled `questions.json` straight off local
disk at the path named by `DBXCARTA_CLIENT_QUESTIONS` (by default the file beside
the overlay), so there is no upload step. Just confirm the path resolves to your
questions file.

### 8. Build and upload dbxcarta artifacts

```bash
# Rebuild the per-package wheels and ship the bootstrap script
uv run dbxcarta publish-wheels
```

`publish-wheels` rebuilds the per-package wheels from current source and
already ships the bootstrap script (it calls `upload_all` internally), so
no separate `upload --all` step is needed.

> Every `dbxcarta` command in steps 7–10 reads the overlay from the
> `DBXCARTA_ENV_FILE` you exported in step 3. If you skipped that,
> export it now:
> `export DBXCARTA_ENV_FILE=examples/finance-genie/dbxcarta-overlay.env`.

### 9. Build the semantic layer

Submit the installed wheel's ingest entrypoint:

```bash
# Submit the ingest job to build the semantic layer
uv run dbxcarta submit-entrypoint ingest
```

The ingest job writes `summary_<run_id>.json` to `DBXCARTA_SUMMARY_VOLUME`;
read it (or the job's stdout via `uv run dbxcarta logs <run-id>`) to confirm
the run built the semantic layer.

### 10. Run the client evaluation

The client runs locally, with no cluster. It reads the bundled `questions.json`
directly, calls the serving endpoints and Neo4j, and prints a truncated per-arm
summary.

```bash
# Run the client evaluation locally
uv run dbxcarta-client
```

### 11. Run the local CLI demo

After the semantic layer is built, use the local read-only CLI in this
package to demonstrate the flow without submitting another Databricks job.

The local demo loads its own `.env` from this directory and never inherits
the parent dbxcarta repo's `.env`. Copy the sample and fill in your
workspace, warehouse, chat endpoint, and Neo4j credentials:

```bash
# Copy the local demo env template, which never inherits the parent repo .env
cp examples/finance-genie/.env.sample examples/finance-genie/.env
# then edit examples/finance-genie/.env
```

Then run any of the demo subcommands from anywhere (they resolve `.env`
relative to the package, not the current working directory):

```bash
# Check connectivity and config before running the demo
uv run --directory examples/finance-genie python -m dbxcarta_finance_genie_example.local_demo preflight
# List the demo question set
uv run --directory examples/finance-genie python -m dbxcarta_finance_genie_example.local_demo questions
# Answer one question and show the retrieved graph context
uv run --directory examples/finance-genie python -m dbxcarta_finance_genie_example.local_demo ask --question-id fg_q01 --show-context
# Run an ad-hoc read-only SQL query against a base table
uv run --directory examples/finance-genie python -m dbxcarta_finance_genie_example.local_demo sql "SELECT COUNT(*) FROM \`graph-enriched-lakehouse\`.\`graph-enriched-schema\`.accounts"
```

The local demo allows only `SELECT`, `WITH`, and `EXPLAIN` statements.

## Overlay Defaults

The overlay targets the full `graph-enriched-schema` and enables every
embedding label (tables, columns, values, schemas, databases) for graph-RAG
retrieval. dbxcarta recovers join paths from Finance Genie column names and
comments via metadata FK inference. The Finance Genie tables are not created
with declared foreign-key constraints. The overlay disables criteria
injection because Finance Genie inferred relationships do not carry literal
join-predicate strings.

For a cheaper first validation run, override the embedding flags in the overlay
and start with `NEOCARTA_DATABRICKS_INCLUDE_EMBEDDINGS_TABLES=true` only.
