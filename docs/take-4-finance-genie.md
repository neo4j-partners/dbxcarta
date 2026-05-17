# Take 4: Finance Genie Medallion End-to-End

Postmortem and runbook for the medallion catalog reorganization and the
full dbxcarta ingest. Significant project changes mean the whole pipeline
must be rerun cleanly. This document records what changed, what was run,
what broke, what to do better, and a step-by-step checklist to run and
test everything.

## Context

Finance Genie moved from a single Unity Catalog to a medallion layout of
three catalogs sharing one schema named `graph-enriched-schema`:

| Catalog | Layer | Tables |
| --- | --- | --- |
| `graph-enriched-finance-bronze` | bronze | `account_features`, `account_graph_features`, `account_similarity_pairs`, `training_dataset` |
| `graph-enriched-finance-silver` | silver | `accounts`, `merchants`, `transactions`, `account_links`, `account_labels` |
| `graph-enriched-finance-gold` | gold | `gold_accounts`, `gold_account_similarity_pairs`, `gold_fraud_ring_communities` |

Operational artifacts are redirected out of the dataset catalogs into
`dbxcarta-catalog.finance_genie_ops`, volume `dbxcarta-ops`.

## What changed in the project

### dbxcarta-spark ingest engine

- `settings.py`: added `dbxcarta_catalogs` and `dbxcarta_layer_map`
  fields plus `resolved_catalogs()` and `layer_map()`. Empty defaults
  preserve historical single-catalog behavior for other presets.
- `contract.py`: `CONTRACT_VERSION` bumped to `1.1` for the additive
  Table `layer` property. Readers treat a missing layer as null.
- `schema_graph.py`: one Database node per catalog; `build_table_nodes`
  adds a `layer` column from the catalog-to-layer map; `build_has_schema_rel`
  drops the single-catalog assumption.
- `extract.py`, `preflight.py`, `fk/declared.py`, `fk/discovery.py`:
  information_schema reads union across all resolved catalogs.

Cross-catalog FK support was explicitly descoped. FK discovery still
skips cross-catalog pairs.

### Finance Genie preset and questions

- `finance_genie.py`: three-catalog preset with bronze/silver/gold
  constants, `catalogs`, `layer_map`, and `volume_path` properties, ops
  redirect, and a readiness check that queries silver for base tables
  and gold for optional tables.
- `questions.json`: all 12 questions repointed to the silver and gold
  catalog names.

### Enrichment pipeline parameterization

The upstream repo at
`/Users/ryanknight/projects/databricks/graph-on-databricks/finance-genie/enrichment-pipeline`
was parameterized for split catalogs with legacy single-catalog
fallback. Files changed: `.env`, `sql/schema.sql`, `sql/gold_schema.sql`,
`upload_and_create_tables.sh`, `jobs/02_neo4j_ingest.py`,
`jobs/03_pull_gold_tables.py`, `jobs/04_validate_gold_tables.py`.
Precedence is `SILVER_CATALOG` or `GOLD_CATALOG`, falling back to the
legacy `CATALOG`. No bronze handling was added because that pipeline
produces no bronze tables. Bronze tables come from GDS notebooks.

### databricks-job-runner fix, version 0.6.1

`publish_wheel_stable` ran a bare `uv build --wheel` at the project
root. In a uv workspace that builds the umbrella root wheel, not the
entrypoint package, so `find_latest_wheel` could not locate
`dbxcarta_spark-*.whl` and the publish failed. Fixed at source in
`/Users/ryanknight/projects/databricks/databricks-job-runner` by passing
`--package <wheel_package>` to the build. Released as 0.6.1 and pinned
in `packages/dbxcarta-spark/pyproject.toml` and `uv.lock`.

## What was run

1. Switched workspace profile to `azure-rk-knight`.
2. Refreshed `.env` from the preset overlay to the medallion values.
3. `dbxcarta preset ... --check-ready --strict-optional`: ready, 5
   silver base plus 3 gold tables present.
4. `setup_secrets.sh --profile azure-rk-knight`: Neo4j scope present.
5. Created the missing managed volume
   `dbxcarta-catalog.finance_genie_ops.dbxcarta-ops`.
6. `dbxcarta preset ... --upload-questions`: questions uploaded.
7. `dbxcarta upload --wheel`: succeeded after the 0.6.1 runner fix.
8. `dbxcarta submit-entrypoint ingest`: ran on Databricks, extracted
   all 12 tables, built embeddings, inferred semantic FKs, wrote to
   Neo4j, then failed the verify gate.

## Issues faced

### 1. Wheel publish broken by the uv workspace layout

The runner built the wrong wheel at the workspace root. Fixed in
runner 0.6.1 with `--package`. The dbxcarta repo root is a uv workspace
umbrella, so any runner build path that does not select the member
package will fail the same way.

### 2. Ops volume did not exist

The ops catalog and schema existed but the managed volume
`dbxcarta-ops` did not. The question upload failed until the volume
was created. The preset names the volume but nothing provisions it.

### 3. Verify gate failed the job although the load succeeded

The ingest loaded all medallion data into Neo4j. The job failed only
because `_verify` raised under `DBXCARTA_VERIFY_GATE=true`. The 9
violations came from two known conditions, not data corruption:

- `_verify` scopes its Neo4j counts off the single `DBXCARTA_CATALOG`,
  which is silver. Silver has 5 tables and 1 schema, so verify
  compared 5 tables against the 12 reported, 1 schema against 3, 24
  columns against 95, and 0 REFERENCES against 13. This is the
  documented decision D4: verify stays single-catalog and warn-only,
  with widening deferred. The shipped `.env` set
  `DBXCARTA_VERIFY_GATE=true`, which contradicted that decision and
  turned expected mismatches into a hard failure.
- The shared Neo4j AuraDB carried stale nodes from prior schemapile
  and dense_500 runs: 1020 Table and 10517 Column nodes at a contract
  version other than `1.1`, plus 2 stale Database and Schema nodes.
  Those polluted the totals and the contract-version check.

### 4. Remote pinned closure stayed at runner 0.6

The submitted job pinned `databricks-job-runner==0.6` in its closure
even after the local upgrade to 0.6.1. The local `--package` fix is a
build-time path, so this run was unaffected, but the remote closure
should repin to 0.6.1 on the next publish.

### 5. Client-side waiter timeout looked like a failure

`submit-entrypoint` timed out client-side after 20 minutes while the
Databricks job kept running for roughly 35 minutes. The timeout is not
a job failure. Run status must be confirmed from the Databricks job
run, not the client exit.

### 6. Take-4 rerun failed at the first Neo4j node write

With `DBXCARTA_VERIFY_GATE=false`, runner 0.6.1, the Neo4j DB wiped,
and a freshly published wheel, the rerun got past verify but failed
in 166s at the first node write, `write_node(... NodeLabel.DATABASE)`
in `run.py:335`, with:

```
[DELTA_SCHEMA_CHANGE_SINCE_ANALYSIS] The schema of your Delta table
has changed in an incompatible way since your DataFrame or DeltaTable
object was created. Latest schema is missing field(s): name,
embedding, contract_version, embedding_text_hash, embedding_model,
embedded_at, embedding_error, id
```

Extraction itself succeeded: `schemas: 3`, `tables: 12`,
`columns: 95`, semantic FK accepted 11, value nodes 51. The failure
is in the embed-stage to Neo4j-write handoff, not in reading the
medallion catalogs.

Mechanism. The embed+stage step writes each label's embedded nodes
to `<staging_root>/<label>_nodes` as Delta with
`mode("overwrite").option("overwriteSchema", "true")`, then reads the
table back and writes that DataFrame to Neo4j (`staging.py:115`,
`stage_embedded_nodes`). The staging root resolves to the sibling
`staging` directory under the summary volume,
`/Volumes/dbxcarta-catalog/finance_genie_ops/dbxcarta-ops/dbxcarta/staging`.
That directory already exists from prior schemapile and dense_500
runs. `DELTA_SCHEMA_CHANGE_SINCE_ANALYSIS` means the read-back
DataFrame's plan was analyzed against one schema and the Delta
table's latest schema differed at write execution: a stale or
incompatible staging table at that path, where `truncate_staging_root`
is meant to clear the whole root once per run.

Contributing factor to evaluate. The built wheel was produced from
the current working tree, which carries a large uncommitted
changeset, not part of the Take-4 medallion work: `run.py`,
`verify/__init__.py`, `verify/graph.py`, `verify/references.py`,
`verify/values.py`, and client files. The ingest wheel must be built
from a known-good tree before drawing conclusions about staging.

Status: BLOCKED pending a decision. Candidate next steps, to be
chosen with the user, not applied unilaterally:

1. Clear the stale `…/dbxcarta/staging` Delta directory the same way
   Neo4j was wiped, then rerun. Lowest-risk if staging is meant to be
   per-run transient.
2. Build the ingest wheel from a clean tree (stash or revert the
   unrelated uncommitted spark/client changes) so the run reflects
   the Take-4 code only, then rerun.
3. Both: clean tree and cleared staging.

## What to do better next time

- Provision the ops volume as part of preset setup or a preflight
  step, rather than failing on first upload.
- Decide the verify policy before the run. For a multi-catalog ingest
  while verify is single-catalog-anchored, set
  `DBXCARTA_VERIFY_GATE=false` so verify is warn-only, matching
  decision D4. Alternatively, widen `_verify` to be catalog-set aware.
- Use a dedicated or wiped Neo4j database for a clean finance-only
  load so contract-version and count checks are meaningful.
- Bump the remote pinned closure to the published runner version
  before submitting jobs.
- Treat the client waiter timeout as expected for long ingests. Poll
  the Databricks run to terminal state instead of trusting the client
  exit code.
- Capture the run summary and verify report as artifacts for every
  run so postmortems do not depend on scrollback.

## Rerun status (2026-05-16)

Decisions locked for this rerun:

- `DBXCARTA_VERIFY_GATE=false`. Verify stays single-catalog-anchored
  per decision D4, so it runs warn-only and does not fail the job.
- Neo4j target wiped by the user before this rerun, so contract-version
  and count checks are meaningful for a clean finance-only load.

Progress is tracked inline in the checklist below. Each item is marked
done with a short result note as it completes.

## Checklist: run and test everything

### A. Code and unit tests

- [x] `uv sync`. Resolved 46 packages.
- [x] `uv pip install -e examples/integration/finance-genie/`.
      Installed `dbxcarta-finance-genie-example==0.1.0`.
- [x] `uv run --with pytest pytest -q` in the dbxcarta repo: 451
      passed, 1 skipped, 6 deselected.
- [x] Runner repo: `uv run --with pytest pytest -q` green, 46 passed.
- [x] Confirmed `databricks-job-runner` pinned `==0.6.1` in
      `packages/dbxcarta-spark/pyproject.toml` and `uv.lock` resolves
      `databricks-job-runner v0.6.1`.

### B. Workspace and configuration

- [x] Switched to profile `azure-rk-knight`
      (`adb-1098933906466604.4.azuredatabricks.net`).
- [x] `dbxcarta preset ... --print-env` matches `.env`.
- [x] `DBXCARTA_VERIFY_GATE=false` set in `.env`.
- [x] `DBXCARTA_CATALOGS` and `DBXCARTA_LAYER_MAP` list all three
      catalogs with bronze/silver/gold layers.

### C. Source data and infrastructure

- [x] Silver and gold populated; bronze populated by the GDS
      notebooks. All present (see next item).
- [x] All 12 tables present in `graph-enriched-schema`: 4 bronze
      (`account_features`, `account_graph_features`,
      `account_similarity_pairs`, `training_dataset`), 5 silver
      (`accounts`, `merchants`, `transactions`, `account_links`,
      `account_labels`), 3 gold (`gold_accounts`,
      `gold_account_similarity_pairs`, `gold_fraud_ring_communities`).
- [x] Ops volume `dbxcarta-catalog.finance_genie_ops.dbxcarta-ops`
      exists.
- [x] Neo4j target wiped by the user before this rerun.
- [x] `setup_secrets.sh` is not present in any local repo, but the
      `dbxcarta-neo4j` secret scope exists with `NEO4J_URI`,
      `NEO4J_USERNAME`, `NEO4J_PASSWORD`, unchanged from the prior
      successful write.

### D. Readiness and artifacts

- [x] `--check-ready --strict-optional`: status ready, 8 expected
      tables present, required and optional both ready.
- [x] `--upload-questions`: `questions.json` present in the ops
      volume `dbxcarta/` path.
- [x] `dbxcarta upload --wheel`: built and published
      `dbxcarta_spark-stable.whl` and `dbxcarta_client-stable.whl`
      via the runner 0.6.1 `--package` build, scripts uploaded.

### E. Ingest

- [x] `dbxcarta submit-entrypoint ingest` submitted. Run
      `209429536888785` (job `562911796534570`).
- [x] Polled to terminal state: `INTERNAL_ERROR` / `FAILED` after
      166s. Task run `1007639542952392`.
- [x] Read `get_output`. Extraction succeeded (`schemas: 3`,
      `tables: 12`, `columns: 95`, semantic FK accepted 11, value
      nodes 51). `DBXCARTA_VERIFY_GATE=false` and
      `databricks-job-runner==0.6.1` confirmed in run params.
- [ ] BLOCKED. The run failed at the first Neo4j node write
      (Database, `run.py:335`), not at verify. See Issue 6 below;
      awaiting a decision before the next rerun.

### F. Verify

- [ ] `dbxcarta verify` against the latest success summary.
- [ ] If verify is single-catalog-anchored, expect scope mismatches
      for schema, table, column, and references counts. Treat them as
      warn-only artifacts of decision D4, not data loss.
- [ ] Confirm this run's nodes carry `contract_version` `1.1`. Any
      non-`1.1` nodes are stale prior-run data, not this run.

### G. Client evaluation

- [ ] `dbxcarta submit-entrypoint client`.
- [ ] Poll the run to terminal state and read its output.
- [ ] Review per-arm accuracy across `no_context`, `schema_dump`, and
      `graph_rag`.

### H. Local demo, optional

- [ ] Copy `examples/integration/finance-genie/.env.sample` to `.env`
      in that directory and fill workspace, warehouse, chat endpoint,
      and Neo4j values.
- [ ] `python -m dbxcarta_finance_genie_example.local_demo preflight`.
- [ ] `python -m dbxcarta_finance_genie_example.local_demo questions`.
- [ ] `python -m dbxcarta_finance_genie_example.local_demo ask
      --question-id fg_q01 --show-context`.
