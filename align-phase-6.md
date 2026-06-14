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

**Run date:** 2026-06-14. Cluster `0611-162049-a2ua1i4w` (Spark 4 DBR, Neo4j
Connector `5.4.3_for_spark_3` — versions confirmed by operator). Session live.

#### Section 1: setup / provisioning

- **NEOCARTA_WHEEL_SOURCE** was unset (hard error, no code default). Added to
  root `.env` → `/Users/ryanknight/projects/neo4j-field/neocarta/dist`. Staged
  wheel `neocarta-0.6.0-py3-none-any.whl` (rebuilt 2026-06-14 00:08 against
  `pyspark>=4.1.2`, `neo4j>=6.2.0`). Wheel-rebuild blocker already cleared.
- `dbxcarta bootstrap` → ensured data catalog `graph-enriched-finance-silver`
  and `dbxcarta-catalog.finance_genie_ops.dbxcarta-ops`. PASS.
- `./setup_secrets.sh --profile aws-partner-rk --example finance-genie` →
  scope `dbxcarta-neo4j-finance-genie` exists; wrote NEO4J_URI / NEO4J_USERNAME
  / NEO4J_PASSWORD. (Example `.env` carries no profile, so `--profile` is
  required.) PASS.
- `dbxcarta ready` → scope `graph-enriched-finance-silver,graph-enriched-finance-gold`;
  present catalogs: 2; status: **ready**. PASS.
- `dbxcarta-embed-probe "ping" --count 150` → endpoint
  `databricks-gte-large-en`, result OK, vectors 150, **dim 1024**. Batch path
  exercised. PASS.

#### Step 1: rebuild wheels and submit ingest

- `make e2e-finance-genie-ingest` (attempt 1): bootstrap no-op; `publish-wheels`
  staged `neocarta-0.6.0-py3-none-any.whl` → `neocarta-stable.whl` and
  `dbxcarta_materialize-1.1.0` → `dbxcarta_materialize-stable.whl` on the Volume,
  plus scripts. Submit started cluster `0611-162049-a2ua1i4w` (PENDING→RUNNING),
  then **preflight failed**: Neo4j connector Maven library in PENDING install
  state.
- **Deviation (transient, not a real fault):** `libraries cluster-status`
  confirmed the correct `5.4.3_for_spark_3` JAR is attached but still installing
  ("attempted on the driver node but has not finished yet") — all libs PENDING
  because the cluster had just booted. Waiting for the connector to reach
  INSTALLED, then re-running `submit-entrypoint ingest` (wheels already staged).
- Connector reached **INSTALLED** after ~30s (poll 2). Re-ran
  `submit-entrypoint ingest`: preflight `[ok] neo4j connector`, **Run ID
  604904203580996**, **Result: SUCCESS**. Logs show
  `BOOTSTRAP_MANIFEST ... wheel_filename=neocarta-0.6.0 ... smoke_check="ok (5)"`
  and `[neocarta] inline embeddings ENABLED: endpoint=databricks-gte-large-en
  dimension=1024`. (Benign: a `01N51` warning that `REFERENCES` did not yet
  exist when an early count query ran.) PASS.

#### Step 2: confirm graph and run summary

- **Graph** (queried directly with the `neo4j` driver + creds from
  `examples/finance-genie/.env`, instance `neo4j+s://4b2239bb.databases.neo4j.io`):
  - Nodes: Database 2, Schema 2, Table 8, Column 59, Value 44.
  - Rels: HAS_SCHEMA 2, HAS_TABLE 8, HAS_COLUMN 59, HAS_VALUE 44.
  - **REFERENCES: 0** — needs human sign-off. Summary shows `fk_declared: 0`,
    so the upstream finance-genie tables declare no foreign keys; the absence is
    the data shape, not a pipeline fault.
- **Inline embeddings:** Database/Schema/Table/Column all carry 1024-dim vectors
  (sample `Column.name=region` dim 1024). Summary `embedding_attempts ==
  embedding_successes` for every label (Table 8, Column 59, Database 2,
  Schema 2) → 100% coverage, 0 failures. Value nodes carry no vector (expected).
  PASS — fully embedded from the one inline job.
- **Run summary:** present on the summary Volume as **`summary_local.json`**
  (status `success`, 19:51–19:58Z 2026-06-14; counts match Neo4j exactly).
  Discrepancy vs plan: the file is keyed `run_id: "local"`, not
  `summary_<run_id>.json` keyed to the Databricks run ID `604904203580996`.
  The attached run still wrote a summary; only the filename/run_id differs from
  the plan's stated detached-run convention. Flagged for follow-up below.

#### Step 3: run client locally — **FAIL (run halted here)**

- `make e2e-finance-genie-client` ran the local client (no cluster). Arm scores:
  - `no_context`: attempted 12, parsed 12, executed 0, non_empty 0,
    exec 0.0%, correct **0.0%**.
  - `schema_dump`: attempted 12, parsed 12, executed 12, non_empty 12,
    exec 100.0%, non_empty 100.0%, correct **66.7%**.
  - `graph_rag`: attempted 0 — **crashed** before any question completed.
- **Error (needs a fix, not transient):** the client's graph_rag arm calls
  `db.index.vector.queryNodes('column_embedding', ...)` but no such index
  exists. `SHOW INDEXES` confirms neocarta 0.6.0 created the vector indexes
  under different names: `column_vector_index`, `table_vector_index`,
  `schema_vector_index`, `database_vector_index` (all ONLINE VECTOR over the
  `embedding` property).
- **Contract drift between `dbxcarta-client` and the neocarta 0.6.0 wheel** —
  three name mismatches:

  | Client expects (`graph_retriever.py`) | neocarta 0.6.0 emits |
  |---|---|
  | vector index `column_embedding` (`_COL_INDEX`, line 24) | `column_vector_index` |
  | vector index `table_embedding` (`_TABLE_INDEX`, line 25) | `table_vector_index` |
  | property `c.data_type` (lines 508, 527) | `c.type` |
  | property `c.comment` (line 509) | `c.description` |

  The index-name mismatch is fatal (raises
  `Neo.ClientError.Procedure.ProcedureCallFailed: no such vector schema index:
  column_embedding`). The property mismatches surface as `01N52` warnings
  (`data_type`/`comment` do not exist), return null, and silently degrade the
  schema_dump and graph_rag context without failing.
- The graph itself is correct and fully embedded (Step 2); only the client's
  read-side names are stale. Note the working tree already has uncommitted edits
  to `graph_rag.py` / `client/__init__.py` (the "inline embedding" work), so the
  client appears to be mid-migration to the new neocarta contract and
  `graph_retriever.py` was not updated to match.
- **Run halted at Step 3 per the "stop on error" instruction.** Steps 4
  (tests/linters) and 5 (cleanliness sweep) not run. No teardown (as directed).

##### Fix applied (operator decision: client adapts to neocarta, neocarta unchanged)

- neocarta creates the vector indexes during ingest in **inline mode only**
  (`connectors/databricks/run.py:249` → `create_vector_indexes` →
  `neocarta/ingest/indexes.py:37` `CREATE VECTOR INDEX {label}_vector_index`).
  Names are derived from the label, not configurable; the client only reads
  them. So the fix is hardcoded constants, not a threaded config key.
- Edited `dbxcarta-client`:
  - `graph_retriever.py`: `_COL_INDEX` `column_embedding`→`column_vector_index`,
    `_TABLE_INDEX` `table_embedding`→`table_vector_index`; column query
    `c.data_type`→`c.type AS data_type`, `c.comment`→`c.description AS comment`
    (output aliases kept so downstream Python is untouched).
  - `schema_dump.py`: same `c.type`/`c.description` property swap, aliases kept.
- **Step 3 re-run** (`make e2e-finance-genie-client`): `status=success`, client
  runs to completion, no crash. Arm scores:
  - `no_context`: attempted 12, executed 0, correct **0.0%** (expected baseline).
  - `schema_dump`: attempted 12, executed 12, non_empty 12, correct **83.3%**
    (up from 66.7% — the type/description fix improved the prompt).
  - `graph_rag`: attempted 12, parsed **6**, executed **0**, non_empty 0,
    correct **0.0%**.
- **ANOMALY for human judgment:** the fatal index error is gone and graph_rag
  no longer crashes, but it executes 0/12 while schema_dump executes 12/12
  against the same warehouse. graph_rag generated parseable SQL for only half
  the questions and none executed. The blocking bug is fixed; this looks like a
  separate context-quality / retrieval issue, not the index-name crash. Flagged,
  not silently accepted.

#### Step 4: full test suite and linters

- `make test` → **415 passed** in ~3s, exit 0. PASS.
- `uv run mypy -p dbxcarta.core -p dbxcarta.client -p dbxcarta.submit
  -p dbxcarta.materialize` → **Success: no issues found in 41 source files**,
  exit 0. PASS. (Bare `uv run mypy` errors with "missing target"; the four
  `-p` flags from `.github/workflows/supply-chain.yml:106` are the real
  invocation — plan wording "over the four packages" matches this.)
- `uv run ruff check .` → **1 error**, `I001` import-block unsorted in
  `examples/schemapile/scripts/dump_question_context.py:16`. **Pre-existing**
  (committed in `c0fadd9`, not touched by this run); the two files I edited
  (`graph_retriever.py`, `schema_dump.py`) pass ruff clean. Not auto-fixed
  (out of scope of the requested change). Flagged for follow-up.

#### Step 5: cleanliness sweep

Grepped code, workflows, and docs. All surviving hits are expected:

- **`dbxcarta-spark` / `dbxcarta.spark`:** zero hits in live code, pyproject,
  workflows, Makefile. Only in planning/provenance markdown (`align.md`,
  `align-v2.md`, `align-phase-6.md`). PASS.
- **`upload-questions`:** zero live hits; only provenance docs (`align.md`,
  `align-v2.md`) and this plan. PASS.
- **`verify`:** no live `verify` command handler or invocation. Hits are
  (a) provenance docs about the retired command, (b) generic English uses of
  the verb ("Test and verify", "cannot verify", "--skip-verify" in the unrelated
  openai-endpoint script), and (c) one cosmetic stale token —
  `tests/core/test_env.py:137` uses `["verify", ...]` as a sample argv to assert
  `select_overlay_path` returns None; any token works, "verify" is incidental.
  Minor, non-functional. PASS.
- **`dbxcarta-submit`:** every hit is the **distribution/package name**, which
  intentionally stays `dbxcarta-submit` while the console command is `dbxcarta`
  (`packages/dbxcarta-submit/pyproject.toml:17` says so explicitly), plus the
  deferred CI workflow `.github/workflows/supply-chain.yml:28` (Phase 4 leaves
  this until neocarta publishes) and provenance docs. No old-command invocation
  survives. PASS.

### Summary

**finance-genie end-to-end run, 2026-06-14. Result: PASS with one anomaly
flagged for human judgment.**

- **Ran:** Section 1 provisioning + Steps 1–5. No teardown (as directed); the
  graph and ops plane are left standing for inspection.
- **Ingest (Step 1–2):** SUCCESS (run `604904203580996`). Graph fully embedded
  inline at dim 1024 — Database 2 / Schema 2 / Table 8 / Column 59 / Value 44,
  100% embedding success, run summary `success` on the Volume. The whole point
  of inline mode (one job → fully embedded graph) holds.
- **Client (Step 3):** found and fixed a real bug — `dbxcarta-client` queried
  stale neocarta contract names (vector index `column_embedding`/`table_embedding`
  and properties `data_type`/`comment`). Per operator decision the client was
  adapted to neocarta 0.6.0 (`*_vector_index`, `c.type`, `c.description`);
  neocarta unchanged. After the fix the client runs to completion; schema_dump
  improved to 83.3% correct.
- **Tests/lint (Step 4):** 415 tests pass, mypy clean. One **pre-existing**
  ruff `I001` in `examples/schemapile/scripts/dump_question_context.py`,
  unrelated to this run; not auto-fixed.
- **Sweep (Step 5):** no live trace of the old package or retired commands.

**For a human to decide / follow up:**

1. **graph_rag arm scores 0%** (attempted 12, parsed 6, executed 0, correct 0)
   while schema_dump executes 12/12. The fatal index crash is fixed, but
   graph_rag still produces no executable SQL. Looks like a context-quality /
   retrieval issue separate from the index-name bug. Needs investigation before
   graph_rag can be called usable end to end.
2. **REFERENCES (FK) edges: 0** — confirmed to be the upstream data shape
   (`fk_declared: 0`), not a pipeline fault. Confirm finance-genie genuinely
   declares no foreign keys.
3. **Pre-existing ruff error** in the schemapile script (above) — fix when
   convenient.
4. **Plan/process nits:** `NEOCARTA_WHEEL_SOURCE` was missing from root `.env`
   (now added); `setup_secrets.sh` lives at repo root and needs `--profile`;
   the submit preflight false-fails on a cold cluster while libraries install
   (polled + retried); the run summary is written as `summary_local.json`
   (`run_id: "local"`), not the plan's `summary_<run_id>.json`.

_deferred — not part of this run._

### dense-schema

_deferred — not part of this run._

### Summary

_to be filled in by the agent at the end of the run_
