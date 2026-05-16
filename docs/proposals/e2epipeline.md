# Finance Genie End-to-End Pipeline — Work Audit Log

**Date started:** 2026-05-14
**Operator:** Claude Code (session)
**Branch:** `layers`
**Target:** `examples/integration/finance-genie`
**Workspace:** `azure-rk-knight` (https://adb-1098933906466604.4.azuredatabricks.net)
**UC scope:** `graph-enriched-lakehouse`.`graph-enriched-schema`

## Context

A large refactoring landed. This run exercises the full Finance Genie pipeline
end to end to validate the refactor against a live workspace, recording every
test, deployment, pipeline, and job run with summary results, fixes applied,
and recommendations.

Decisions confirmed with operator:
- Root `.env` is currently a schemapile/dense_500 config. Approach:
  **back up to `.env.schemapile.bak`, swap to finance-genie overlay, restore
  at end.**
- **Full live run** authorized: real Databricks job submission, Neo4j secret
  push, client evaluation job.

## Pre-flight findings

- All 5 base tables (`accounts`, `merchants`, `transactions`,
  `account_links`, `account_labels`) and all 3 Gold tables (`gold_accounts`,
  `gold_account_similarity_pairs`, `gold_fraud_ring_communities`) already
  exist in `graph-enriched-lakehouse.graph-enriched-schema`. Upstream Finance
  Genie data-prep (README step 2) is therefore already satisfied; it will be
  verified, not regenerated.

## Checklist

- [x] **Step 0 — Test suite.** Run repo test suite (non-live) to sanity-check
  the refactor before any live submission. **PASS** (356 passed; mypy
  invocation undefined — documented for follow-up).
- [x] **Step 1 — Install.** `uv sync`; `uv pip install -e
  examples/integration/finance-genie/`. **PASS**.
- [x] **Step 2 — Prepare Finance Genie.** Verify UC base + Gold tables present
  and row counts sane (no upstream regeneration). **PASS**.
- [x] **Step 3 — Configure dbxcarta.** Back up root `.env`; print preset env;
  write finance-genie overlay to root `.env`. **PASS**.
- [x] **Step 4 — Check readiness.** `dbxcarta preset … --check-ready
  --strict-optional`. **PASS** (`status: ready`, 11.1s).
- [x] **Step 5 — Refresh Neo4j secrets.** `./setup_secrets.sh --profile
  azure-rk-knight`. **PASS** (3 secrets written, 2.2s).
- [x] **Step 6 — Upload question set.** `dbxcarta preset …
  --upload-questions`. **PASS** (12 questions, 4471 bytes uploaded).
- [x] **Step 7 — Build and upload artifacts.** `dbxcarta upload --wheel`;
  `dbxcarta upload --all`. **PASS** (wheel + 3 scripts uploaded).
- [ ] **Step 8 — Build semantic layer.** `dbxcarta submit-entrypoint ingest`
  (classic cluster); then `dbxcarta verify`. **FAILED — BLOCKED**
  (LIBRARY_INSTALLATION_ERROR; awaiting operator direction, no fix applied).
- [ ] **Step 9 — Run client evaluation.** `dbxcarta submit-entrypoint
  client`.
- [ ] **Step 10 — Local CLI demo.** `local_demo preflight | questions | ask |
  sql`.
- [ ] **Step 11 — Restore `.env`.** Restore schemapile config from backup.

## Run log

### Step 0 — Test suite — PASS (with mypy caveat)

**Command:** `uv sync --group test && uv run --group test pytest -q`

**Result:** `356 passed, 1 skipped, 3 deselected in 9.15s`. The skip + 3
deselected are the `live`-marked tests, excluded by the default
`pytest.ini_options` config. Suite is fast and green — no refactor
regressions surfaced at the unit/integration level.

**Open issue — mypy invocation undefined (NEEDS INVESTIGATION):**

The repo configures strict mypy in `pyproject.toml` (per-module overrides for
~14 spark modules) but defines no invocation: no CI workflow, no `scripts/`,
no pre-commit config, no Makefile, no docs command. Every attempted invocation
fails with:

```
src/dbxcarta/spark/run.py: error: Source file found twice under different
module names: "spark.run" and "dbxcarta.spark.run"
Found 1 error in 1 file (errors prevented further checking)
```

Root cause: the uv workspace has two packages (`dbxcarta-spark`,
`dbxcarta-client`) whose sources share the implicit `dbxcarta.*` namespace
across two separate `src/` roots. mypy resolves the same file under both
`spark.run` and `dbxcarta.spark.run` and aborts before type-checking.
Attempts that failed: passing both src dirs at root; per-package `mypy src`;
`--explicit-package-bases --namespace-packages` on the package dirs.

**What needs investigation (deferred, not blocking pipeline):**
- Establish the canonical mypy command. Likely `MYPYPATH` set to both
  `src` roots plus `mypy --namespace-packages -p dbxcarta.spark -p
  dbxcarta.client`, or adding `mypy_path`/`explicit_package_bases` to the
  `[tool.mypy]` table so the namespace packages resolve unambiguously.
- Add the resolved command to a `scripts/` entry or CI so the strict config
  is actually enforced. Currently the strict overrides are dead config.

Per operator decision, documented and moving on; mypy is not a pipeline gate.

### Step 1 — Install — PASS

**Commands:** `uv sync`; `uv pip install -e
examples/integration/finance-genie/`.

**Result:** Workspace synced. `dbxcarta-finance-genie-example==0.1.0`
installed editable. Import verified: `preset` resolves to
`FinanceGeniePreset(catalog='graph-enriched-lakehouse',
schema='graph-enriched-schema', volume='graph-enriched-volume')` with the
expected 5 base + 3 Gold table tuples. Refactor did not break the preset
import path or `Preset` protocol surface.

### Step 2 — Verify Finance Genie UC data — PASS

**Query:** row counts across all 8 expected tables in
`graph-enriched-lakehouse`.`graph-enriched-schema`.

| table | rows |
| --- | --- |
| accounts | 25,000 |
| merchants | 7,500 |
| transactions | 250,000 |
| account_links | 300,000 |
| account_labels | 25,000 |
| gold_accounts | 239 |
| gold_account_similarity_pairs | 2,000 |
| gold_fraud_ring_communities | 2 |

All base and Gold tables present and populated. Upstream Finance Genie
data-prep is satisfied; no regeneration performed.

### Step 3 — Configure dbxcarta — PASS

- Backed up root `.env` → `.env.schemapile.bak`. Confirmed gitignored via
  `git check-ignore` (matched by `.env.*` rule).
- `dbxcarta preset dbxcarta_finance_genie_example:preset --print-env`
  produced the 20-key Finance Genie overlay; CLI preset path works post-
  refactor.
- Wrote new root `.env` = preset overlay + workspace-specific block
  (profile, cluster `1029-205109-yca7gn2n`, warehouse `a2946a63e3a3643d`,
  secret scope `dbxcarta-neo4j`, chat endpoint
  `databricks-claude-sonnet-4-6`, Neo4j creds, `DBXCARTA_VERIFY_GATE=true`)
  carried over from the prior `.env`. Also carried client tuning knobs
  `DBXCARTA_SCHEMA_DUMP_MAX_CHARS=50000` and
  `DBXCARTA_CLIENT_MAX_EXPANSION_TABLES=20` which the preset does not emit.

**Note/recommendation:** the preset `env()` intentionally emits only the
schema-scoped overlay, not workspace connection or client tuning knobs. The
README step 3 ("copy those values into dbxcarta/.env") understates this — a
naive copy of `--print-env` output drops `DATABRICKS_PROFILE`,
`DATABRICKS_WAREHOUSE_ID`, secret scope, compute, chat endpoint, and Neo4j
creds. Recommend the README explicitly state that the overlay is merged on
top of the workspace block, not a full `.env`.

### Step 4 — Check readiness — PASS

**Command:** `uv run dbxcarta preset dbxcarta_finance_genie_example:preset
--check-ready --strict-optional`

**Result (exit 0, 11.1s wall):**
```
scope: graph-enriched-lakehouse.graph-enriched-schema
present expected tables: 8
required tables: ready
optional tables: ready
status: ready
```

**Investigated false alarm — "stuck checking readiness":** First attempt was
interrupted by operator who observed no output. Diagnosed:
- Warehouse `a2946a63e3a3643d` was warm (MCP queries in Step 2 returned in
  seconds) — not a cold-start.
- `WorkspaceClient(profile='azure-rk-knight')` auth probe: 2.1s, valid
  `databricks-cli` OAuth token — not an auth hang.
- Re-ran under `timeout 90`: completed cleanly in 11.1s.

Root cause: the `--check-ready` path emits **zero output** until the full
`ReadinessReport` is built (~11s of subprocess spin-up + client build + one
`information_schema.tables` query). Silent startup is indistinguishable from
a hang. Not a refactor regression; no code defect.

**Recommendation:** add a one-line stderr progress marker (e.g.
`checking readiness for <scope>…`) before the workspace client / SQL call in
`_handle_preset` so the CLI is not silent during the ~10s startup. Low effort,
removes a recurring "is it stuck?" footgun.

### Step 5 — Refresh Neo4j secrets — PASS

**Command:** `./setup_secrets.sh --profile azure-rk-knight` (2.2s, exit 0)

Scope `dbxcarta-neo4j` already existed; wrote `NEO4J_URI`,
`NEO4J_USERNAME`, `NEO4J_PASSWORD`. Script read creds from the freshly
written finance-genie `.env`. No issues.

### Step 6 — Upload question set — PASS

**Command:** `uv run dbxcarta preset dbxcarta_finance_genie_example:preset
--upload-questions` (exit 0, 2.5s)

Fixture `questions.json` has 12 questions (keys: `question_id`,
`question`, `notes`, `reference_sql`). Verified landed at
`/Volumes/graph-enriched-lakehouse/graph-enriched-schema/graph-enriched-volume/dbxcarta/questions.json`
(4471 bytes) via `files.list_directory_contents`.

**Recommendation (recurring):** `--upload-questions` also produces zero
stdout on success. Same silent-CLI pattern as `--check-ready`. A single
confirmation line (`uploaded 12 questions -> <dest>`) would make success
observable and is worth bundling with the Step 4 recommendation.

### Step 7 — Build and upload artifacts — PASS

**`dbxcarta upload --wheel`** (exit 0): built `dbxcarta-spark` wheel,
uploaded to
`/Volumes/graph-enriched-lakehouse/graph-enriched-schema/graph-enriched-volume/wheels/dbxcarta_spark-1.0.0-py3-none-any.whl`
(53,380 bytes on volume; local `dist/` copy 52.1K). Verified present.

**`dbxcarta upload --all`** (exit 0): uploaded `run_autotest.py`,
`run_demo.py`, `run_spike_ai_query.py` →
`/Users/ryan.knight@neo4j.com/dbxcarta/scripts`.

**Observation 1 — version-string mismatch:** build log printed
`Building dbxcarta-spark wheel (v1.0.1)...` but the produced artifact is
`dbxcarta_spark-1.0.0` (pyproject is `1.0.0`). The "v1.0.1" label in the
runner's build message does not match the actual wheel version. Cosmetic but
misleading in logs — recommend the runner derive the printed version from the
built wheel filename / package metadata rather than a separate counter.

**Observation 2 — stale pre-rename wheels on volume:** the volume `wheels/`
dir still holds `dbxcarta-0.2.31` … `dbxcarta-0.2.35` from before the
package was renamed `dbxcarta` → `dbxcarta-spark`. `submit-entrypoint`
resolves by package name (`find_latest_wheel(dist, "dbxcarta-spark")`) so
these are not picked up, but they are dead clutter. Recommend a one-time
cleanup of the old `dbxcarta-0.2.x` wheels.

**Process note:** earlier "upload got stuck" alarm was caused by running the
command piped through `tail`, which buffers all stdout until EOF — no
progress visible for the whole run. Re-ran backgrounded with streaming
output: `--wheel` and `--all` each completed in seconds, exit 0. No defect.

### Step 8 — Build semantic layer — FAILED (BLOCKED, no fix applied)

**Command:** `uv run dbxcarta submit-entrypoint ingest --compute cluster`

**Submission (worked):**
- Entrypoint `dbxcarta-ingest`, package `dbxcarta-spark`, wheel
  `/Volumes/.../wheels/dbxcarta_spark-1.0.0-py3-none-any.whl`, 29 env params
  from `.env`.
- Job id `326482183294529`, run id `89954757287477`, task run
  `113497362624256`.
- Run page:
  `https://adb-1098933906466604.4.azuredatabricks.net/?o=1098933906466604#job/326482183294529/run/89954757287477`

**Failure:** `INTERNAL_ERROR` / `result_state=FAILED`, termination code
`LIBRARY_INSTALLATION_ERROR` (`CLIENT_ERROR`). Run lasted ~22s (setup 1s,
execution 0s, cleanup 20s) on classic cluster `1029-205109-yca7gn2n`
("Small Spark 4.0", now auto-terminated). Message:

> Library installation attempted on the driver node of cluster
> 1029-205109-yca7gn2n and failed. Unable to find or download the required
> package or its dependencies. … (Databricks truncates server-side at
> "Suggested Action: Verify package name …", skipping 916 bytes — the full
> pip stderr is only in the terminated cluster's library-install logs.)

**Diagnosis (facts):**
- The wheel file itself is present and readable on the volume (verified in
  Step 7). Failure is dependency resolution, not a missing wheel.
- `dbxcarta-spark` declares 7 deps, all public PyPI: `databricks-job-runner`
  (0.4.8, `registry = pypi.org/simple` per `uv.lock`), `databricks-sdk`,
  `neo4j`, `pydantic`, `pydantic-settings`, `pyspark`, `python-dotenv`. No
  internal/private cross-dependency, so not a private-package 404.
- Cluster is Spark 4.0 (DBR ships Python 3.12) → `requires-python>=3.12` is
  satisfied; not a Python-version mismatch.
- Pre-refactor `dbxcarta-0.2.31…0.2.35` wheels still sit in the volume
  `wheels/` dir, implying the pipeline ran successfully on this cluster
  before the package was renamed `dbxcarta` → `dbxcarta-spark` (0.2.x →
  1.0.0).

**Candidate root causes (cannot fully disambiguate without the cluster's
library-install logs):**
1. **No public PyPI egress on this classic cluster.** The wheel installs
   from `/Volumes` (local) but its 7 transitive deps must be pulled from
   pypi.org; a network-locked Azure cluster blocks that. Most consistent
   with "wheel found, deps cannot be downloaded".
2. **Refactor changed the dependency surface vs. cluster pre-provisioning.**
   The pre-refactor single `dbxcarta` package may have had its deps
   pre-installed on the cluster (cluster libraries / init script) under the
   old name; the rename + new dep list means stale pre-provisioned libs no
   longer satisfy `dbxcarta-spark`, and a fresh download is blocked/failing.
3. Wheel METADATA artifact from the `uv_build` backend (least likely; build
   + upload succeeded, install reached dep-resolution).

**Clarification — compute selection was correct:** the run used the
pre-existing cluster from `.env` (`DATABRICKS_COMPUTE_MODE=cluster`,
`DATABRICKS_CLUSTER_ID=1029-205109-yca7gn2n`). CLI output confirms
`Checking cluster 1029-205109-yca7gn2n... Cluster: RUNNING` — it did **not**
create, resize, or configure a cluster, and did not fall back to serverless.
The failure is **not** in cluster setup. It is in the per-run step where the
submitted task attaches the wheel as a cluster **library**
(`task.libraries = [{whl: .../dbxcarta_spark-1.0.0...}]`), which makes the
driver pip-install the wheel **and resolve/download its 7 transitive deps**
at job start.

**Open question raised by operator (not yet actioned, no fix applied):**
the target cluster is "already set up". Should the ingest entrypoint rely on
that already-provisioned cluster environment instead of attaching the wheel
as a library that forces a fresh pip dependency resolution on every run?
i.e. the failure mode is the library-install/dependency-download step, not
compute provisioning — investigate whether the pre-refactor flow avoided
per-run dep download (deps pre-baked on the cluster / different attach
mechanism) and whether the refactor changed how the wheel is attached.
To be discussed with operator before any change.

**Missing dependencies — what pip must download when the wheel is attached:**

Databricks truncated the pip stderr server-side, so the *single* package it
named is not recoverable from the run logs. What *is* knowable: attaching
`dbxcarta_spark-1.0.0-py3-none-any.whl` as a cluster library forces the
driver's pip to resolve and download `dbxcarta-spark`'s **entire transitive
closure**. From `uv tree --package dbxcarta-spark` (46 packages resolved):

- **Direct (7):** `databricks-job-runner` 0.4.8, `databricks-sdk` 0.103.0,
  `neo4j` 6.1.0, `pydantic` 2.13.3, `pydantic-settings` 2.14.0, `pyspark`
  4.1.1, `python-dotenv` 1.2.2.
- **Transitive (notable):** `google-auth` 2.49.2, `cryptography` 46.0.7,
  `cffi` 2.0.0, `pycparser` 3.0, `pyasn1[-modules]`, `protobuf` 6.33.6,
  `requests` 2.33.1 (+`certifi`, `charset-normalizer`, `idna`, `urllib3`),
  `pydantic-core` 2.46.3, `typing-extensions` 4.15.0, `typing-inspection`
  0.4.2, `annotated-types` 0.7.0, `neo4j`→`pytz` 2026.1, `pyspark`→`py4j`
  0.10.9.9.
- (`pytest` and its deps are an optional `test` extra, not pulled by the
  job.)

Every one of these must be downloadable by the cluster driver at job start
for the library install to succeed; the failure is that pip could not get
**at least one** of them.

**Most likely specific culprit (refactor-relevant):** `pyspark` 4.1.1 is a
declared runtime dependency (`pyspark>=3.5`). Attaching the wheel makes pip
try to download PySpark (hundreds of MB) onto a cluster whose DBR
("Small Spark 4.0") **already provides PySpark**. On a no-egress / locked
cluster this fails outright; even with egress it is a large, fragile,
redundant download. A Spark-side wheel should normally treat `pyspark` as a
provided/peer dependency (not a hard install dep) precisely so it is not
re-downloaded onto a Databricks runtime. Whether the refactor newly pinned
`pyspark` into `dbxcarta-spark`'s `dependencies` (vs. the pre-refactor
package leaving it out / as an extra) is the first thing to check. **Logged
for discussion — no change made.**

**Status:** Real blocker. Per operator standing instruction ("discuss how to
fix a bug before proceeding"), **no fix attempted**. Pipeline halted at
Step 8; Steps 9–10 depend on a built semantic layer and cannot proceed.
`.env` restore (Step 11) still pending and intentionally not yet done.

_(entries appended as each step executes)_
