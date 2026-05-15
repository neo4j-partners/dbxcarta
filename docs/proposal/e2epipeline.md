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
- [ ] **Step 4 — Check readiness.** `dbxcarta preset … --check-ready
  --strict-optional`.
- [ ] **Step 5 — Refresh Neo4j secrets.** `./setup_secrets.sh --profile
  azure-rk-knight`.
- [ ] **Step 6 — Upload question set.** `dbxcarta preset …
  --upload-questions`.
- [ ] **Step 7 — Build and upload artifacts.** `dbxcarta upload --wheel`;
  `dbxcarta upload --all`.
- [ ] **Step 8 — Build semantic layer.** `dbxcarta submit-entrypoint ingest`
  (classic cluster); then `dbxcarta verify`.
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

_(entries appended as each step executes)_
