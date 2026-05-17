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

Resolution applied (user-directed):

- The stale `…/dbxcarta/staging` Delta directory was confirmed to
  hold prior-run `column_nodes, database_nodes, schema_nodes,
  table_nodes, value_nodes` and was recursively removed.
- The unrelated uncommitted spark/client changeset was committed by
  the user (HEAD `6201b89`), so the ingest wheel now builds from a
  clean tree with the latest code.
- `.env` was reconciled back to the finance-genie medallion overlay
  (it had reverted to the schemapile/dense_500 overlay) with
  `DBXCARTA_VERIFY_GATE=false`.
- Wheels rebuilt and republished, scripts uploaded, readiness ready,
  ingest resubmitted.
- First resubmit aborted client-side at the maven preflight: the
  `.env` compute block had also reverted to the schemapile cluster
  `1029-205109-yca7gn2n` ("Small Spark 4.0"), which lacks the Neo4j
  connector. `DATABRICKS_CLUSTER_ID` repointed to the dedicated
  `0515-141455-wb8qxgo2` ("Small Spark 4.0 - DBXCarta"), the cluster
  the earlier successful-extraction run used. Resubmitted.

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

Outcome so far: sections A through F are complete and green. The
ingest run `852936506066184` loaded all 3 catalogs / 12 tables / 95
columns into a wiped Neo4j and verify passed with zero violations.
Section G (client evaluation) is running.

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

- [x] `dbxcarta submit-entrypoint ingest`. After Issue 6 was
      resolved (cleared staging, clean committed tree, finance-genie
      `.env`, DBXCarta cluster), the clean run is
      `852936506066184` (job `969189156517451`), task run
      `848429190360666`.
- [x] Polled to terminal state: `TERMINATED` / `SUCCESS`, ~248s.
- [x] Read `get_output`: run summary `status=success`.
- [x] Run summary confirms `schemas: 3`, `tables: 12`,
      `columns: 95`, value nodes 51, semantic FK 11 + metadata FK 2.
      Neo4j counts: Database 3, Schema 3, Table 12, Column 95,
      Value 51, HAS_SCHEMA 3, HAS_TABLE 12, HAS_COLUMN 95,
      HAS_VALUE 51, REFERENCES 13.

### F. Verify

- [x] Inline verify in the ingest job: `ok=True violations=0`.
- [x] Standalone `dbxcarta verify`: `OK (0 violations)`.
- [x] The D4 single-catalog-anchored caveat no longer applies: the
      committed verify code aggregates count invariants across all
      resolved catalogs (`run.py` now passes
      `catalogs=settings.resolved_catalogs()`), so the multi-catalog
      run verifies clean with zero scope mismatches.
- [x] Neo4j was wiped before the run and verify (which includes the
      contract-version invariant) passed with zero violations, so all
      nodes are this run's at `contract_version` `1.1`.

### G. Client evaluation

- [x] `dbxcarta submit-entrypoint client` submitted (serverless).
- [x] Polled to terminal state and read output: run
      `321469996458442` (task `21901960124707`),
      `TERMINATED` / `SUCCESS`.
- [x] Reviewed per-arm accuracy across `no_context`, `schema_dump`,
      and `graph_rag`. All 12 questions attempted and parsed on every
      arm. Execution and correctness:

      | Arm | attempted | parsed | executed | exec rate | correct rate |
      | --- | --- | --- | --- | --- | --- |
      | `no_context` | 12 | 12 | 2 | 16.7% | 100.0% (2/2) |
      | `schema_dump` | 12 | 12 | 12 | 100.0% | 83.3% (10/12) |
      | `graph_rag` | 12 | 12 | 0 | 0.0% | 0.0% (0/12) |

- [x] `graph_rag` 0/12 root-caused, fixed, and re-verified
      end-to-end. The fix run `938043512057347` brings `graph_rag`
      to 12/12 executed and 100% correct. See G.1, G.1.1, and
      G.1.2.

### G.1 Finding: graph_rag executed 0 of 12

`graph_rag` parsed all 12 generated SQL statements but executed
none, so its correctness is 0%. This is a total-failure anomaly,
not a quality gap: `schema_dump` over the same graph data executed
all 12 and was 83.3% correct, and `no_context` executed cleanly on
the 2 it attempted. A 12/12 parse with 0/12 execute points at the
graph_rag context or its emitted SQL (for example wrong or
backtick/medallion-qualified table identifiers from the graph
context) rather than at the model or the harness.

Artifacts pulled from
`…/dbxcarta/runs/dbxcarta_client_local_20260517T013831Z.json`
make the root cause concrete. Two stacked defects:

1. **graph_rag retrieved zero context for all 12 questions.**
   Every graph_rag `ArmResult` has `context_ids: []`. The arm
   injected no graph context at all, so the model answered each
   question blind. This is the primary defect: the graph retriever
   returns nothing against the medallion graph.
2. **Wrong catalog identifier as the downstream symptom.** With no
   context naming the real catalog, the model guessed an
   underscored name. Every graph_rag query failed with
   `[TABLE_OR_VIEW_NOT_FOUND]` on
   `` `graph_enriched_finance_silver`.`graph-enriched-schema`.<t> ``.
   The real catalog is `graph-enriched-finance-silver` with
   hyphens. The working arms prove the target is fine:
   `no_context` and `schema_dump` both emit the correct
   `` `graph-enriched-finance-silver`.`graph-enriched-schema`.<t> ``
   and execute. `schema_dump` gets the hyphenated name because the
   dumped schema text contains it literally; graph_rag never saw
   it because its context was empty.

### G.1.1 Root cause (pinned)

Direct Neo4j inspection of this run's graph confirms the mechanism.
Ingest normalizes identifiers when it builds node `id`s but
preserves the true Unity Catalog names in the `.name` property:

| Node | `.id` (normalized) | `.name` (true) |
| --- | --- | --- |
| Database | `graph_enriched_finance_silver` | `graph-enriched-finance-silver` |
| Schema | `graph_enriched_finance_silver.graph_enriched_schema` | `graph-enriched-schema` |
| Table | `…​.graph_enriched_schema.transactions` | `transactions` |

This normalization is by design. `contract.generate_id`
lowercases each part and replaces spaces and hyphens with
underscores, and `verify/catalog.py` has an id-normalization
invariant, so the id is deliberately lossy and `.name` is the
authoritative identifier. All 95 columns and 12 tables carry
embeddings and all five vector indexes are `ONLINE`, so the
embedding store is healthy.

The graph_rag retriever has two defects, both from reading the
normalized `id` where it must read the true `.name`:

- **Defect A (breaks every emitted query).**
  `graph_retriever._fetch_columns` builds the table FQN as
  `` `{catalog}`.`{schema}`.`{table}` `` where `catalog` comes from
  `catalog_from_node_id(col_id)`, the normalized id component
  `graph_enriched_finance_silver`. Schema and table come from the
  true `.name`. The result is the exact failing identifier seen in
  every artifact:
  `` `graph_enriched_finance_silver`.`graph-enriched-schema`.`accounts` ``.
  The catalog segment is the underscored, nonexistent name; the
  model copies the context verbatim, so every query fails
  `[TABLE_OR_VIEW_NOT_FOUND]`. `no_context` and `schema_dump` are
  unaffected because they take the catalog from `.env`, not from
  the graph.
- **Defect B (kills vector-seed expansion).**
  `_filter_seed_pairs_to_schemas`, `_select_schemas`, and
  `_normalized_schema_scores` derive a seed's schema with
  `schema_from_node_id`, which returns the normalized
  `graph_enriched_schema`, then compare it against the configured
  schema name `graph-enriched-schema` (hyphens, from
  `DBXCARTA_SCHEMAS`). The comparison never matches, so every
  vector seed is dropped and seed-driven expansion (parent tables,
  FK walk, join criteria) is dead. The lexical fallback still works
  because it filters on the true `s.name`, which is why a
  wrongly-qualified query was still produced rather than a fully
  empty prompt.

Net: any deployment whose catalog or schema name contains a hyphen
(or space or uppercase) breaks graph_rag, because every such name
normalizes to a different id than its true `.name`. The
finance-genie medallion catalogs are the first hyphenated names to
exercise this path; prior schemapile/dense runs used already-normal
names so id and `.name` coincided and the bug was latent.

### G.1.2 Fix (implemented)

The fix is in the client retriever, not ingest. Ingest
normalization is intentional and verify-guarded; `.name` is the
contract's authoritative identifier and the retriever must use it.

- **Defect A.** Stop reconstructing the catalog from the node id.
  Resolve the true catalog from the graph: extend the
  `_fetch_columns` Cypher to walk
  `(db:Database)-[:HAS_SCHEMA]->(s:Schema)-[:HAS_TABLE]->(t:Table)`
  and return `db.name` as the catalog, building the FQN from
  `db.name`, `s.name`, `t.name`. The other column-/value-fetch
  paths that surface table identifiers get the same treatment.
- **Defect B.** Make the schema-scope comparison normalization-safe.
  Either normalize the configured schema names with the same rule
  as `contract.generate_id` before comparing to id-derived schema,
  or resolve each seed's true schema `.name` from the graph and
  compare against the configured (true) names. The first option is
  smaller and keeps the comparison in id-space; the second removes
  id-derived names from scoping entirely and is more robust.
- **Regression guard.** Add a fixture with a hyphenated catalog and
  schema so id-vs-name divergence is exercised. A unit test asserts
  the emitted FQN uses `.name` and that hyphenated configured
  schemas still select seeds.

Decision (locked): Defect B uses option 1, normalize the configured
schema names to id-space with the `contract.generate_id` rule and
compare in normalized space. Defect A resolves the true catalog
from `Database.name` via the graph walk.

Implemented in `packages/dbxcarta-client/src/dbxcarta/client/graph_retriever.py`:

- Added `_normalize_id_part`, mirroring `contract.generate_id`
  (lowercase, spaces and hyphens to underscores).
- Defect A: `_fetch_columns` Cypher now walks
  `(db:Database)-[:HAS_SCHEMA]->(s:Schema)-[:HAS_TABLE]->(t:Table)`
  and returns `db.name AS catalog_name`. The FQN is built from
  `db.name`, `s.name`, `t.name`; the `catalog_from_node_id` import
  and its use were removed.
- Defect B: `_filter_seed_pairs_to_schemas` normalizes the
  configured schema names before comparing to the id-derived
  schema. `retrieve()` maps the normalized `_select_schemas` output
  back to the configured true names (`selected_true`) so the
  `.name`-filtered fetch and lexical queries and the
  `ContextBundle.selected_schemas` carry the real hyphenated names.
- Regression guards in `tests/client/test_retriever.py`: a
  hyphenated-name `_fetch_columns` test asserting the FQN uses
  `.name` not the normalized id, and a `_filter_seed_pairs_to_schemas`
  test asserting a hyphenated configured schema selects
  normalized-id seeds. The pre-existing
  `test_fetch_columns_uses_catalog_from_column_id` was updated to the
  corrected `catalog_name` contract.

Validation:

- Full suite green: 453 passed, 1 skipped, 6 deselected.
- Read-only live check against the run's Neo4j: with the configured
  hyphenated schema, `_fetch_columns` emits
  `` `graph-enriched-finance-silver`.`graph-enriched-schema`.<t> ``
  (the executable form the working arms use), and
  `_filter_seed_pairs_to_schemas` keeps 4/4 real normalized seed
  ids (previously 0/4).

End-to-end proof (run `938043512057347`, SUCCESS): after
republishing the client wheel with the fix and rerunning the
client arm against the same Neo4j, `graph_rag` recovered fully.

| Arm | executed | exec rate | correct rate |
| --- | --- | --- | --- |
| `no_context` | 2/12 | 16.7% | 100.0% (2/2) |
| `schema_dump` | 12/12 | 100.0% | 83.3% (10/12) |
| `graph_rag` (pre-fix, run `321469996458442`) | 0/12 | 0.0% | 0.0% (0/12) |
| `graph_rag` (post-fix, run `938043512057347`) | 12/12 | 100.0% | 100.0% (12/12) |

Artifacts in
`…/dbxcarta/runs/dbxcarta_client_local_20260517T020424Z.json`
confirm both defects are closed: every graph_rag result now has a
non-empty context (10 context ids per question, was 0) and emits
the executable hyphenated FQN, for example
`` SELECT COUNT(*) FROM `graph-enriched-finance-silver`.`graph-enriched-schema`.`accounts` ``.
graph_rag is now the top arm at 100% correct, ahead of
`schema_dump` at 83.3%.

### H. Local demo, optional

- [ ] Copy `examples/integration/finance-genie/.env.sample` to `.env`
      in that directory and fill workspace, warehouse, chat endpoint,
      and Neo4j values.
- [ ] `python -m dbxcarta_finance_genie_example.local_demo preflight`.
- [ ] `python -m dbxcarta_finance_genie_example.local_demo questions`.
- [ ] `python -m dbxcarta_finance_genie_example.local_demo ask
      --question-id fg_q01 --show-context`.
