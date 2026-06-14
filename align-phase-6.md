# Phase 6 testing plan: full local end-to-end run

This is a plain-English checklist for proving the whole dbxcarta cutover works
against a real cluster and a real Neo4j instance. The code is done. What is left
is a live run.

The flow has two halves. First the operator sets up the cluster and the
workspace so the ingest job can run. Then the operator runs the job, runs the
client, and checks the results. Everything is driven from the repo root with one
example overlay selected at a time.

The three examples are `finance-genie`, `schemapile`, and `dense-schema`. Pick
one to start. The steps are identical for each; only the overlay path changes.

---

## Section 1: Operator setup (cluster and workspace)

Do these once before the first run. Most are idempotent, so re-running them is
safe.

### Build and stage the connector

- **Build the neocarta wheel.** Build it from the neocarta project at
  `/Users/ryanknight/projects/neo4j-field/neocarta` so there is a wheel file in
  its local build folder. dbxcarta points at that folder and uploads the wheel
  for you. It does not build neocarta itself.
- **Confirm the wheel source setting** in the submit tooling points at
  neocarta's local build folder. This is the one setting that later flips to a
  package index. For Phase 6 it stays on the local folder.

### Confirm the cluster runtime

- **Leave the Neo4j Spark Connector Maven coordinate as it is.** It reads
  `..._for_spark_3` in `submit/cli.py`, and that `for_spark_3` JAR has been
  tested against Spark 4 and works. The existing coordinate and cluster setup are
  validated, so no bump is needed. The submit tooling's preflight check passes on
  the current coordinate.
- **Target a Spark 4 Databricks Runtime.** neocarta moved from pyspark 3.5 to
  4.1, so the cluster must run a Spark 4 DBR. The validated `for_spark_3` Neo4j
  Spark Connector JAR runs against that Spark 4 runtime.
- **Confirm neo4j 6 on the driver side.** The pinned ingest closure already pins
  `neo4j==6.1.0`. Make sure nothing on the cluster shadows it with an older
  driver.

### Provision the workspace plane

- **Select the example overlay** for every command, either by exporting
  `DBXCARTA_ENV_FILE=examples/<name>/dbxcarta-overlay.env` or by passing
  `--env-file` on each command. Nothing runs base-only; a missing overlay is a
  hard error.
- **Create the catalog, schema, and volume** with `dbxcarta bootstrap`. This is
  idempotent and creates the ops plane the overlay names.
- **Provision the Neo4j secret scope** with the example's `setup_secrets.sh`. It
  reads the scope name from the overlay and the `NEO4J_*` secrets from the
  example's standalone `.env`, then writes them into the Databricks secret scope
  the ingest job reads.
- **Confirm the upstream data exists.** Run `dbxcarta ready`. It checks that each
  catalog named in the overlay holds a data schema. The upstream tables for each
  example are created once per workspace and are owned by the upstream project,
  not by this run.
- **Confirm the embedding endpoint is reachable.** Inline embeddings call the
  `databricks-gte-large-en` serving endpoint at 1024 dimensions. The chat
  endpoint the client uses must also be live.

### What gets uploaded to Databricks

- The prebuilt **neocarta connector wheel**, staged onto the Unity Catalog
  Volume by `publish-wheels`.
- The **materialize wheel** and the **cluster bootstrap script**, also staged by
  `publish-wheels`.
- The **Neo4j secrets**, written into the secret scope by `setup_secrets.sh`.
- The **catalog, schema, and volume**, created by `bootstrap`.
- Nothing else. The client and the questions file stay local and never upload.

---

## Section 2: Running the tests and what to check

### Step 1: Rebuild wheels and submit ingest

- Run `make e2e-<name>-ingest`. This rebuilds the wheels from current source,
  runs `bootstrap` again as a no-op, stages the wheels with `publish-wheels`, and
  submits the ingest job with `submit-entrypoint ingest`.
- **Check:** the submit passes its preflight check, including the Maven library
  check, and the job starts. The existing `for_spark_3` connector coordinate is
  validated against Spark 4, so preflight should pass as-is.
- **Check:** the ingest job reaches `SUCCESS`. Print its logs with
  `dbxcarta logs <run-id>` using the run ID the submit step echoes.

### Step 2: Confirm the graph and the run summary

- **Check the graph in Neo4j.** The ingest job should produce Database, Schema,
  Table, and Column nodes for the catalogs in the overlay, with the relationships
  between them.
- **Check the embeddings are inline.** Because dbxcarta runs inline mode, the
  nodes should already carry 1024-dimension vectors. There should be no separate
  embed step. A fully embedded graph from one job is the whole point of this
  mode.
- **Check the run summary file.** The overlay sets a summary Volume, so a
  detached run leaves a `summary_<run_id>.json` report on the Volume. Confirm it
  is there and describes the run.

### Step 3: Run the materialize follow-up

- Run `dbxcarta materialize` with the same overlay selected. Inline mode already
  embedded the graph, so materialize is the only operator follow-up.
- **Check:** materialize reaches `SUCCESS` and writes its Delta tables.

### Step 4: Run the client locally

- Run `make e2e-<name>-client`. This runs `dbxcarta-client` on the local machine
  with no cluster. It loads the overlay and the base `.env`, reads the local
  `questions.json`, calls the serving endpoints and Neo4j directly, and prints a
  truncated report.
- **Check:** the client runs to completion with no cluster and no Delta writes.
- **Check the scores.** The report prints one line per arm
  (`no_context`, `schema_dump`, `graph_rag`) with attempted, parsed, executed,
  non-empty, and correct rates. The `graph_rag` arm exercises the embedded graph,
  so its scores confirm the inline embeddings are usable end to end.

### Step 5: Run the full test suite and linters

- Run `make test` for the default unit suite.
- Run `uv run ruff check .` and `uv run mypy` over the four packages.
- **Check:** all pass with the Spark package gone.

### Step 6: One project-wide cleanliness sweep

- Grep the whole repo, across code, workflows, and docs, for:
  - `dbxcarta-spark` and `dbxcarta.spark` (the old package and import path).
  - `verify` and `upload-questions` (the retired commands).
  - `dbxcarta-submit` as a command invocation (the old command name).
- **Check:** the only remaining hits are intentional provenance comments and the
  known deferred CI workflows, which Phase 4 leaves naming the old package on
  purpose until neocarta publishes.

### Step 7: Tear down when done

- Run `make e2e-<name>-teardown` to drop the example's teardown target. It
  requires `--yes-i-mean-it` or it only prints what it would drop. It leaves the
  shared catalog and the upstream data intact.

---

## Phase 6 is done when

- The ingest job runs end to end off the local neocarta wheel against a Spark 4
  cluster and a real Neo4j instance.
- The graph comes out fully embedded inline, with a run summary on the Volume.
- Materialize succeeds, and the client runs locally and prints sensible scores.
- All tests and linters pass.
- The cleanliness sweep finds no live trace of the old package or the retired
  commands.

## Known blockers to clear first

- A Spark 4 DBR with the validated `for_spark_3` Neo4j Connector JAR attached.
  The Maven coordinate stays as-is; it is already tested against Spark 4.
- A reachable `databricks-gte-large-en` embedding endpoint and a live chat
  endpoint for the client.
