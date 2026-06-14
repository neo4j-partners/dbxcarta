# Phase 6 testing plan: full local end-to-end run

This is a plain-English checklist for proving the whole dbxcarta cutover works
against a real cluster and a real Neo4j instance. The code is done. What is left
is a live run, and the agent drives that run end to end.

## Key agent instruction: keep a work log in this file

The agent executing this plan owns the whole flow. There is no human operator
stepping through commands. The agent runs every command, inspects every result,
and decides whether each check passed.

As the agent works, it MUST keep a detailed work log directly in this document,
under the **Work log** section at the bottom. For every step the agent runs, it
records:

- the exact command or tool call it ran,
- the run ID, job ID, or other handle that came back,
- the result it observed, including the numbers from each check (node counts,
  embedding dimensions, arm scores, pass or fail),
- any deviation from the plan and why.

When the run finishes, the agent writes a short summary at the end of the work
log: which examples ran, what passed, what failed, and anything a human needs to
follow up on. The work log is the durable record of the run. Treat writing it as
part of each step, not an afterthought.

The agent reports results, but a human makes the final accept-or-reject call on
the judgment checks (graph shape, sensible scores). The agent records the numbers
and flags anything anomalous rather than silently declaring success.

## The flow

The flow has two halves. First the agent sets up the cluster and the workspace
so the ingest job can run. Then the agent runs the job, runs the client, and
checks the results. Everything is driven from the repo root with one example
overlay selected at a time.

This run targets `examples/finance-genie`. Wherever a step shows `<name>`, read
it as `finance-genie` and the overlay path as
`examples/finance-genie/dbxcarta-overlay.env`. The same steps later repeat for
`schemapile` and `dense-schema` by swapping the overlay path; this plan and its
work log cover the finance-genie run.

## Human prerequisites

Two things must be in place before the agent starts. The agent cannot do these
itself and should confirm them up front, failing loudly if either is missing:

- **Databricks is logged in.** The CLI profile named in the root `.env` resolves
  to a live workspace session. The agent assumes this is already true.
- **Each example's standalone `.env` holds real `NEO4J_*` secrets.**
  `setup_secrets.sh` reads `NEO4J_URI`, `NEO4J_USERNAME`, and `NEO4J_PASSWORD`
  from `examples/<name>/.env`, not from the root `.env` and not from the overlay.
  If those values are placeholders, the script skips the example. A human fills
  these in once.

Everything past this point the agent runs.

---

## Section 1: Setup (cluster and workspace)

Do these once before the first run. Most are idempotent, so re-running them is
safe.

### Confirm the staged connector

- **The neocarta wheel is already built.** It lives at
  `/Users/ryanknight/projects/neo4j-field/neocarta/dist/neocarta-0.6.0-py3-none-any.whl`.
  The agent does not build it. It confirms the file is present and notes the
  exact filename in the work log. If the wheel predates the latest neocarta
  dependency bump (pyspark 4.1.2, neo4j 6.2.0), the agent flags that a rebuild is
  needed so the cluster install picks up the new pins.
- **Confirm the wheel source setting** points at that `dist/` folder.
  `publish-wheels` reads `NEOCARTA_WHEEL_SOURCE` from the root `.env` and copies
  the newest matching wheel from there to the Volume. The agent checks that
  `NEOCARTA_WHEEL_SOURCE` resolves to neocarta's `dist/` directory. This is the
  one setting that later flips to a package index. For Phase 6 it stays on the
  local folder.
- **Confirm the ingest entrypoint.** The wheel registers the console script
  `neocarta-databricks-ingest`, backed by `neocarta.connectors.databricks.run:run_ingest`.
  The submit tooling launches this on the cluster.

### Confirm the cluster runtime

- **The Neo4j Spark Connector Maven coordinate reads**
  `org.neo4j:neo4j-connector-apache-spark_2.13:5.4.3_for_spark_3` at
  `packages/dbxcarta-submit/src/dbxcarta/submit/cli.py:136`. This is the latest
  stable connector, still the `for_spark_3` JAR, and it runs against Spark 4. The
  agent confirms the cluster has the `5.4.3_for_spark_3` JAR attached as a Maven
  library, because the submit preflight only checks the connector is present, not
  its version. If the cluster still carries the old `5.3.10` JAR, the agent flags
  that it must be re-attached at 5.4.3.
- **Target a Spark 4 Databricks Runtime.** neocarta runs on a Spark 4 DBR, so
  the cluster must run a Spark 4 runtime. The validated `for_spark_3` Neo4j Spark
  Connector JAR runs against that Spark 4 runtime.
- **Confirm neo4j 6 on the driver side.** The wheel pins `neo4j>=6.2.0`. The
  agent confirms nothing on the cluster shadows it with an older driver.

### Provision the workspace plane

- **Select the example overlay** for every command, either by exporting
  `DBXCARTA_ENV_FILE=examples/<name>/dbxcarta-overlay.env` or by passing
  `--env-file` on each command. Nothing runs base-only; a missing overlay is a
  hard error. The dbxcarta CLI reads the root base `.env` layered under this
  overlay.
- **Create the catalog, schema, and volume** with `dbxcarta bootstrap`. This is
  idempotent and creates the ops plane the overlay names.
- **Provision the Neo4j secret scope** by running the example's
  `setup_secrets.sh`. It reads the scope name from the overlay and the `NEO4J_*`
  secrets from the example's standalone `examples/<name>/.env`, then writes them
  into the Databricks secret scope the ingest job reads.
- **Confirm the upstream data exists.** Run `dbxcarta ready`. It checks that each
  catalog named in the overlay holds a data schema. The upstream tables for each
  example are created once per workspace and are owned by the upstream project,
  not by this run.
- **Confirm the embedding endpoint is reachable** with
  `uv run dbxcarta-embed-probe "ping"`. It sends one input to the
  `databricks-gte-large-en` serving endpoint and prints `result: OK` with the
  vector dimension; the agent confirms `dim: 1024`. Run it with `--count 150` to
  also probe the batch path the inline embed uses. The chat endpoint the client
  uses must also be live; the client run in Step 3 exercises it.

### What gets uploaded to Databricks

- The prebuilt **neocarta connector wheel**, staged onto the Unity Catalog
  Volume by `publish-wheels`.
- The **cluster bootstrap script**, also staged by `publish-wheels`.
- The **Neo4j secrets**, written into the secret scope by `setup_secrets.sh`.
- The **catalog, schema, and volume**, created by `bootstrap`.
- Nothing else. The client and the questions file stay local and never upload.

---

## Section 2: Running the tests and what to check

The agent runs each step, records the command and result in the work log, and
checks the listed conditions before moving on.

### Step 1: Rebuild wheels and submit ingest

- Run `make e2e-finance-genie-ingest`. This rebuilds the wheels from current
  source, runs `bootstrap` again as a no-op, stages the wheels with
  `publish-wheels`, and submits the ingest job with `submit-entrypoint ingest`.
  The submit runs **attached**: it waits for the run to reach a terminal state
  before returning. Do not pass `--no-wait`. Nothing here runs async; the
  client step does not start until this run is terminal.
- **Check:** the submit passes its preflight check, including the Maven library
  check at `5.4.3_for_spark_3`, and the job starts.
- **Check:** the ingest job reaches `SUCCESS`. Print its logs with
  `dbxcarta logs <run-id>` (a passthrough to the job-runner `logs` verb; with no
  run ID it fetches the latest run) using the run ID the submit step echoes, and
  record that run ID in the work log.

### Step 2: Confirm the graph and the run summary

- **Check the graph in Neo4j.** There is no Neo4j MCP server wired in, so the
  agent queries Neo4j directly with a short Python snippet using the `neo4j`
  driver and the `NEO4J_*` creds from `examples/finance-genie/.env` — the same
  instance the ingest job wrote to, since `setup_secrets.sh` provisions the
  secret scope from that file. Do not read these from the root `.env`; it can
  name a different instance. The ingest job should
  produce Database, Schema, Table, and Column nodes for the catalogs in the
  overlay, with the relationships between them. Record the node counts per label.
- **Check the embeddings are inline.** Because dbxcarta runs inline mode, the
  nodes should already carry 1024-dimension vectors. There should be no separate
  embed step. The agent confirms a sample node carries a vector of the expected
  dimension. A fully embedded graph from one job is the whole point of this mode.
- **Check the run summary file.** The overlay sets a summary Volume, so a
  detached run leaves a `summary_<run_id>.json` report on the Volume. The agent
  confirms it is there and reads it to confirm it describes the run.

### Step 3: Run the client locally

- Run `make e2e-finance-genie-client`. This runs `dbxcarta-client` on the local machine
  with no cluster. It loads the overlay and the base `.env`, takes the
  `NEO4J_*` secrets from the overlay's sibling `examples/finance-genie/.env`,
  reads the local `questions.json`, calls the serving endpoints and Neo4j
  directly, and prints a truncated report.
- **Check:** the client runs to completion with no cluster and no Delta writes.
- **Check the scores.** The report prints one line per arm
  (`no_context`, `schema_dump`, `graph_rag`) with attempted, parsed, executed,
  non-empty, and correct rates. The `graph_rag` arm exercises the embedded graph,
  so its scores confirm the inline embeddings are usable end to end. Record every
  arm's numbers in the work log and flag anything that looks off.

### Step 4: Run the full test suite and linters

- Run `make test` for the default unit suite.
- Run `uv run ruff check .` and `uv run mypy` over the four packages.
- **Check:** all pass with the Spark package gone. Record the pass or fail.

### Step 5: One project-wide cleanliness sweep

- Grep the whole repo, across code, workflows, and docs, for:
  - `dbxcarta-spark` and `dbxcarta.spark`, the old package and import path.
  - `verify` and `upload-questions`, the retired commands.
  - `dbxcarta-submit` as a command invocation, the old command name.
- **Check:** the only remaining hits are intentional provenance comments and the
  known deferred CI workflows, which Phase 4 leaves naming the old package on
  purpose until neocarta publishes. Record the surviving hits and why each is
  expected.

### Step 6: Tear down when done

- Run `make e2e-finance-genie-teardown` to drop the example's teardown target. It
  requires `--yes-i-mean-it` or it only prints what it would drop. It leaves the
  shared catalog and the upstream data intact. Record what was dropped.

---

## Phase 6 is done when

- The ingest job runs end to end off the local neocarta wheel against a Spark 4
  cluster and a real Neo4j instance.
- The graph comes out fully embedded inline, with a run summary on the Volume.
- The client runs locally and prints sensible scores.
- All tests and linters pass.
- The cleanliness sweep finds no live trace of the old package or the retired
  commands.
- The work log below is complete, with a result recorded for every step and a
  closing summary.

## Known blockers to clear first

- A Spark 4 DBR with the `5.4.3_for_spark_3` Neo4j Connector JAR attached. The
  coordinate at `cli.py:136` is the latest stable connector and runs against
  Spark 4. If the cluster still has the old `5.3.10` JAR, re-attach it at 5.4.3.
- A neocarta wheel rebuilt against the current pins (pyspark 4.1.2, neo4j 6.2.0)
  staged in neocarta's `dist/`.
- A reachable `databricks-gte-large-en` embedding endpoint and a live chat
  endpoint for the client.

---

## Work log

The agent fills this in as it runs. One entry per step, per example. Record the
command, the handle that came back, the result and numbers, and the pass or fail.
Write the closing summary here when the run finishes.

### finance-genie

_to be filled in by the agent_

### schemapile

_deferred — not part of this run._

### dense-schema

_deferred — not part of this run._

### Summary

_to be filled in by the agent at the end of the run_
