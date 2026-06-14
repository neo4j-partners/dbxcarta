# Aligning dbxcarta with neocarta: remove the Spark pipeline, pull the connector wheel

> **Hard cutover. No migration, no legacy compatibility, no downstream consumers.** There is no transition window and no support for the old and new layouts at once. When something moves or is deleted, every caller is repointed or removed in the same change. Nothing outside this repo depends on the removed package, so nothing is preserved for compatibility. Each phase fully replaces what it touches; after it lands, the old shape is gone, not deprecated.

## Goal

The Spark ingest pipeline moved to neocarta. It now lives there as the `neocarta` connector wheel with a `databricks-spark` extra. This project, dbxcarta, should stop carrying its own copy of that pipeline and instead pull the neocarta wheel and run it.

This plan removes the `dbxcarta-spark` package from dbxcarta, repoints the operator tooling at the neocarta wheel, turns the example client into plain local Python, and updates every workspace file, CI workflow, and doc that still mentions the old package.

We do all of this so it can be tested fully on a local machine before neocarta publishes its wheel to a package index. The tooling pulls the neocarta wheel from a local build folder for now. A later step flips that source to a real package index, and nothing else has to change.

## The end state in one line

After this work, the only Spark in dbxcarta is the job-submit tooling that launches cluster jobs and the `materialize` job. Everything else, including the example client, is plain local Python talking to Neo4j and the SQL warehouse.

## Key design decisions

- **One operator command.** The surviving command is named `dbxcarta`. The old pipeline command owned that name; when the pipeline package is deleted, the operator tool takes the name over. There is one command, not two.
- **Local wheel now, index later.** The wheel source is a single setting. It points at neocarta's local build folder today and at a package index plus a version later. The swap is one setting and nothing else.
- **Repoint before delete.** The ingest path is pointed at the neocarta wheel and proven to run before the old package is removed, so the pipeline is never broken in the middle.
- **Spark only where it is required.** The job-submit tooling and `materialize` stay on Spark because they have to be. The client moves off Spark to plain local Python, since its only Spark use was reading and writing a couple of tables.
- **Embeddings are generated inline, in the ingest job.** dbxcarta runs neocarta's inline embedding mode: the ingest job embeds nodes in-cluster with a native `ai_query()` call against a Databricks serving endpoint, so a single job produces a fully embedded graph and the only operator follow-up is `materialize`. This is why the pinned ingest closure carries no external embedding provider, no openai and no litellm. External embedding mode, where a separate `neocarta databricks embed` step adds vectors after ingest, is deliberately deferred to Phase 7.

## What stays and what goes (in plain words)

- **Goes away:** the Spark ingest pipeline that reads Unity Catalog tables and writes the semantic graph into Neo4j. That is the `dbxcarta-spark` package. neocarta owns this now.
- **Goes away with it:** the `dbxcarta verify` command. It checks the output of a finished pipeline run, so it belongs with the pipeline. neocarta dropped this check during its migration, so dbxcarta drops it too.
- **Stays on Spark:** the operator job-submit tooling and the `materialize` job. `materialize` runs `spark.sql` to build Delta tables, so it genuinely needs a cluster.
- **Stays, but moves off Spark:** the example client. Its real work is Neo4j retrieval, prompt building, model calls, and scoring, all plain Python. It only used a cluster session to read the question table and write the summary, which it can do straight against the SQL warehouse instead. So it becomes a local Python command.
- **Moves, does not die:** the `ready` and `upload-questions` commands. They are operator helpers, so they move out of the Spark package and into the operator tool. The readiness logic already lives in the shared core, so only the two thin command handlers move.

## The command-line tools, so the change is clear

- **`dbxcarta`** is the single operator command after this change. Today this name belongs to the old pipeline command inside the Spark package, which offers `verify`, `ready`, and `upload-questions`. The pipeline command is deleted. `verify` is retired. `ready` and `upload-questions` move to the operator tool, and the operator tool is renamed to `dbxcarta`.
- **The cluster ingest entry point** is what the job runs on the cluster. Today it points at the Spark package. After this change it points at the neocarta connector's ingest entry point inside the neocarta wheel.
- **`dbxcarta-client`** stays a command, but runs locally instead of on a cluster.

---

## Phase 1: Point the ingest job at a local neocarta wheel

Make the operator tool stage and run the neocarta wheel from a local folder, while the old Spark package is still present.

- Build the neocarta wheel on this machine from the neocarta project at `/Users/ryanknight/projects/neo4j-field/neocarta`, so there is a wheel file in neocarta's build folder to point at.
- Add a setting in the operator tool for where the neocarta wheel comes from. For now this is the local build folder path. Later it becomes a package index plus a version number, and only this one setting changes.
- Change the publish step so that, for the ingest job, it copies the prebuilt neocarta wheel onto the Unity Catalog Volume instead of building the old Spark wheel.
- Before committing to that publish-step change, confirm where the build-from-source logic actually lives. The fetch-instead-of-build swap targets the `databricks_job_runner` `publish_wheel_stable` / `find_latest_wheel` seam, which today builds from `project_dir/dist`. That logic sits in the installed `databricks_job_runner` package, not in this repo. Verify the repoint can be driven entirely from the `dbxcarta-submit` caller (`_handle_publish_wheels`); if the seam is not exposed there, the `databricks_job_runner` package itself needs a change, which widens this phase. Resolve this early so it does not surface mid-implementation.
- Stop copying the shared core code into the ingest wheel. The neocarta wheel already carries everything it needs as normal modules.
- Point the cluster ingest entry point at the neocarta connector's ingest entry point, `neocarta.connectors.databricks.run:run_ingest` in the root `neocarta` wheel, installed with its `databricks-spark` extra (`pyspark`).
- Rename the ingest-relevant overlay keys to neocarta's env-var contract so the wheel reads them directly with no translation step. neocarta's settings use the `NEOCARTA_DATABRICKS_` prefix and ignore unknown env vars, so the operator-only and client-only keys stay on their `DBXCARTA_*`/`DATABRICKS_*` names and are harmlessly forwarded.
  - **Carry across, renamed:** catalog, catalogs, schemas, the four embedding-include flags (tables, columns, schemas, databases), and the secret scope. Add `NEOCARTA_DATABRICKS_SECRET_SCOPE` alongside the existing `DATABRICKS_SECRET_SCOPE` that operator tooling still reads.
  - **Add for inline mode (new keys, not in the old overlay):** because dbxcarta runs inline embeddings, the overlay must also set the inline tuning settings neocarta requires: `NEOCARTA_DATABRICKS_EMBEDDING_ENDPOINT`, `NEOCARTA_DATABRICKS_EMBEDDING_DIMENSION`, `NEOCARTA_DATABRICKS_EMBEDDING_BATCH_TABLES`, and `NEOCARTA_DATABRICKS_EMBEDDING_FAILURE_MAX`. Critically, set `NEOCARTA_DATABRICKS_EMBEDDING_STAGING_VOLUME` to a `/Volumes/<cat>/<schema>/<vol>/<subdir>` subpath: neocarta validates this as required whenever any inline flag is on (it replaces the old summary-volume-derived staging path, which neocarta dropped), so an inline run with it unset fails at settings load.
  - **Replaces the old summary-volume key:** the single `summary-volume` overlay key no longer maps to one neocarta field. neocarta split it into the required `EMBEDDING_STAGING_VOLUME` above and an optional `NEOCARTA_DATABRICKS_SUMMARY_VOLUME`. Set the optional `SUMMARY_VOLUME` to a Volume subpath so each detached cluster ingest run writes a durable `summary_<run_id>.json` report; without it the run summary lives only in memory on the cluster and is lost when the job ends. (Recommended on, since dbxcarta is the operator tooling that needs the run outcome of a detached job.)
  - **Drop entirely:** the embeddings-values flag (neocarta removed Value embedding) and the summary-table name (neocarta has no field for it).
- Set the fixed list of pinned dependencies for the ingest job to neocarta's tested closure for this path: the `neocarta` wheel, `pydantic`, `pydantic-settings`, `pandas`, `neo4j`, `databricks-sdk`, and `python-dotenv`. None of the BigQuery, Dataplex, litellm, openai, sqlglot, torch, or mlflow dependencies are on the inline ingest path, so they are not pinned.
- Leave `pyspark` out of the pinned closure on purpose. The classic cluster's Databricks Runtime supplies `pyspark`, and pip-installing a second copy over the `--no-deps` bootstrap risks version skew with the runtime's Spark JVM and jars. neocarta pins `pyspark` in its `databricks-spark` extra only for local wheel-install reproducibility; that pin is for off-cluster testing, not for the cluster job. The requirement that follows is on the runtime, not the closure: the job must target a Spark 4 DBR (to match neocarta's `pyspark` 3.5 to 4.1 jump) with a Spark 4 build of the Neo4j Spark Connector JAR attached, and `neo4j` 6 on the driver side (neocarta's `neo4j` 5 to 6 jump).
- Keep the existing cluster check for the Neo4j Spark Connector. It is a Java library attached to the cluster, not a Python dependency.

**Phase 1 is done when:** the operator tool stages the local neocarta wheel onto a Volume and submits an ingest job that runs against a test catalog and Neo4j, end to end. The old Spark package has not been touched yet.

**Status: Complete (code) — live end-to-end submit deferred to Phase 6 (needs a Spark 4 cluster + Neo4j).**

The operator tool now stages the prebuilt neocarta wheel and submits the ingest job against it; the old Spark package is untouched, as the phase requires. What was done:

- The ingest entry points at the neocarta connector (wheel package `neocarta`, console script `neocarta-databricks-ingest`). `publish-wheels` copies the prebuilt neocarta wheel from a local build folder onto the Volume instead of building the old Spark wheel; core is only copied into the client/materialize wheels.
- All three overlays carry neocarta's full `NEOCARTA_DATABRICKS_*` inline-embedding contract and keep only the `DBXCARTA_*`/`DATABRICKS_*` keys that surviving operator/client/runner code still reads. The retired spark-ingest keys are gone.
- The pinned ingest closure is neocarta's tested runtime closure for this path. It omits `pyspark`, `databricks-sdk`, `pandas`, and `numpy` because the Databricks Runtime provides them; pinning a copy under `--no-deps` would shadow the runtime build (and risk a numpy C-ABI mismatch). The matching requirement falls on the runtime: a Spark 4 DBR shipping compatible pandas/numpy.

Two carry-forward items for later phases:
- **Maven coordinate** is still `..._for_spark_3` (the existing check was kept per the plan). It must be bumped to the Spark 4 connector JAR before the live submit passes preflight — do this at Phase 6.
- **`test_overlay` repoint** (a Phase 2 item) is blocked until neocarta is on a package index; for now the test validates the overlay's `NEOCARTA_DATABRICKS_*` contract structurally instead of importing neocarta's settings.

Validation: full default suite green (611 passed, 1 skipped), mypy and ruff clean.

## Phase 2: Move the operator commands, rename, and delete the Spark package

Rescue the operator-facing commands and the command name, then remove the package and every workspace wiring that points at it, in one cutover.

- Move the `ready` and `upload-questions` command handlers from the Spark package into the operator tool. The readiness logic they call already lives in the shared core, so only the thin handlers move.
- Retire the `verify` command. Do not move it. It checks pipeline output, which is now neocarta's job.
- Rename the operator tool's command from `dbxcarta-submit` to `dbxcarta`, so the surviving command takes the name the old pipeline command used.
- Delete the whole `dbxcarta-spark` package folder.
- Remove it from the project's list of workspace members, the dependency list, and the workspace source entry that pointed at it.
- Regenerate the project lock file so the package and its build artifacts are gone from it.
- Fix every caller that imports from the old Spark package, not just one. There are four outside the package:
  - `scripts/run_autotest.py` imports `dbxcarta.spark.contract.generate_id`. Repoint it at neocarta's contract equivalent, or inline the small id-generation logic if neocarta does not expose it on a stable path.
  - `tests/examples/finance-genie/test_overlay.py` imported `dbxcarta.spark.settings.SparkIngestSettings`. **Done by substitution in Phase 1, full repoint deferred (acceptable).** The literal repoint at neocarta's `SparkIngestSettings` is blocked: neocarta is not on a package index, and adding its local path as a test dependency would break CI. Phase 1 already removed the `dbxcarta.spark` import and the test now validates the overlay's `NEOCARTA_DATABRICKS_*` contract structurally, which satisfies the intent of this item. The remaining work — importing neocarta's settings to prove the keys parse — rides along with the index publish in "What is deliberately left for later"; this is an accepted drift, not a gap.
  - `tests/integration/conftest.py` imports `dbxcarta.spark.ingest.summary_io` (`load_summary_from_volume`). This checks finished pipeline output, which is now neocarta's job, so drop it along with the retired `verify` check.
  - `tests/boundary/test_import_boundaries.py` asserts on `import dbxcarta.spark`. Update or remove that expectation, since the namespace no longer exists here.

**Phase 2 is done when:** `dbxcarta ready` and `dbxcarta upload-questions` work, the project builds and installs with no reference to the old package, and the old pipeline command is gone.

**Status: Complete.**

What was done:
- `ready` and `upload-questions` moved from the deleted Spark CLI into the operator tool (`dbxcarta.submit.cli`), wired into its dispatch and help. `verify` was retired, not moved (it checked pipeline output, now neocarta's job). Its `_build_neo4j_driver` helper went with it.
- The operator command was renamed from `dbxcarta-submit` to `dbxcarta` (the name the deleted pipeline command owned). The distribution stays named `dbxcarta-submit`; only the console script changed, plus every invocation (Makefile, scripts, help/prog strings, doc-comments). `dbxcarta --help` resolves and lists all seven commands; the old `dbxcarta-ingest` console script is gone.
- The whole `packages/dbxcarta-spark` folder was deleted, along with its workspace member/source/dependency entries and the regenerated lock (`uv lock` removed `dbxcarta-spark`). The shared runner's default `wheel_package` was repointed off the deleted package to a surviving wheel (`dbxcarta-client`).
- The four external callers were fixed: `run_autotest.py` inlines the tiny id rule (the deleted `generate_id`) and calls the renamed command; `tests/integration/conftest.py` dropped the `run_summary` fixture (the pipeline-output loader); `tests/boundary/test_import_boundaries.py` dropped every `dbxcarta.spark`/`dbxcarta-spark` expectation; `test_overlay.py` was already decoupled in Phase 1.

Deviation (in scope, completing the deletion):
- The plan enumerated "four callers outside the package," but the package's own test suite, `tests/spark/` (34 files), imports `dbxcarta.spark` throughout and had to be deleted with the package — leaving it would have made the default suite red. Phase 4 still removes the CI step that ran "the Spark package's tests," confirming these are that suite. Coverage worth keeping was ported: the `_resolve_questions_file` tests moved into `tests/submit/test_cli.py`, and the live `test_semantic_search.py` was decoupled from the removed summary fixture (its real assertions are about the embedded-graph contract neocarta still produces) rather than deleted.

Deferred to later phases (per plan): the `.github/` workflows still naming `dbxcarta-spark` (Phase 4) and the `docs/` references (Phase 5). The remaining `dbxcarta.spark`/`dbxcarta-spark` text in source is intentional provenance comments, not live imports.

Validation: full default suite green (433 passed, 3 deselected — the ~178 `tests/spark/` tests went with the package), `uv sync`/`uv lock` clean, mypy clean on `dbxcarta.submit`, ruff clean, `dbxcarta` command resolves and the old commands are gone.

## Phase 3: Turn the client into local Python and shrink the submit tooling

Take the client off the cluster, then remove the cluster machinery that only the client and the old ingest wheel needed.

- Change the client so it reads the question table and writes the summary straight against the SQL warehouse, the same way the local demo already does, instead of asking a cluster session to do it.
- Remove the cluster session call from the client run path, so the client is plain local Python connecting to Neo4j and the warehouse.
- Remove the client from the operator tool's set of cluster entry points, so there is no "submit the client to a cluster" path.
- Remove the client's pinned dependency list and the client branch of the core-copying step. The only package the tool still builds and copies core into is `materialize`.
- Confirm the examples run the client locally and still pass their checks.
- With the client gone, the generic submit-entrypoint dispatch now has one caller left, ingest. Collapse it into a dedicated "submit ingest" path. Remove the indirection that chose between entry points by name, since there is nothing left to choose between.
- Shrink the entry-point lookup tables to the single ingest entry, and delete the now-unused client and old-ingest entries.

**Phase 3 is done when:** the client runs locally end to end with no cluster, the submit tooling has a single dedicated ingest-submit path with no name-based dispatch, and it only knows about staging the neocarta ingest wheel and building the `materialize` job.

## Phase 4: Update the automation (CI)

Fix every automated workflow that still builds, versions, tests, or type-checks the old package.

- In the main test workflow, remove the test entries that run the Spark package's tests and the entries that add it to example test runs.
- In the same workflow, remove the type-check step that targets the Spark package.
- In the publishing workflow, remove the step that builds the Spark wheel.
- In the release workflow, remove the step that bumps the Spark package's version.

**Phase 4 is done when:** every workflow runs clean with no step that touches the removed package.

## Phase 5: Update the documents

Rewrite the docs so they describe the new shape: the pipeline lives in neocarta, dbxcarta pulls the connector wheel, and the client is local.

- Update the project readme so it describes dbxcarta as the operator tooling that pulls and runs the neocarta connector, not as the home of the Spark pipeline.
- Update the architecture doc so the diagram and text show the pipeline coming from the neocarta wheel and the client running locally.
- State in the readme and architecture doc that dbxcarta runs neocarta's inline embedding mode, so the ingest job emits a fully embedded graph and the only operator follow-up is `materialize`. Point at Phase 7 for the deferred external embedding path.
- Delete the public interface doc. It only maps the old pipeline module names, which no longer exist here. There is nothing left to map.
- Update the release doc to remove the steps that build and version the old Spark wheel.
- Update the simulate-publish doc to stage the neocarta wheel instead of building the old Spark wheel.
- Update the add-a-data-source tutorial to point at the neocarta connector instead of building the old Spark wheel.
- Update the schema doc so it points at the neocarta connector for the graph contract.
- Update the test fixtures readme so it no longer says the self-check lives in the old Spark package.

**Phase 5 is done when:** no doc tells a reader to build, install, or run the removed package, and the two-step flow is described: run the ingest job from the neocarta wheel, then run the operator follow-up.

## Phase 6: Full local end-to-end test and final sweep

Prove the whole thing works on a local machine, with neocarta still unpublished.

- Run the complete operator flow locally: set up the catalog and volume, stage the local neocarta wheel, and submit the ingest job against a test catalog and Neo4j.
- Run the client locally against the resulting graph and confirm it works with no cluster.
- Confirm the ingest job produces the expected graph.
- Run the full project test suite and the linters, and confirm they pass with the Spark package gone.
- Do one project-wide search for the old package name, the old import path, and any retired command, across code, workflows, and docs, and confirm it is clean. This is the one sweep for the whole change.

**Phase 6 is done when:** the ingest job runs end to end off the local neocarta wheel, the client runs locally, all tests and linters pass, and the old package leaves no trace.

## Phase 7: External embedding mode (later)

Add the second, decoupled embedding path so the graph can also be embedded or re-embedded after ingest, without re-running the Spark job. This is deferred because inline mode already produces a fully embedded graph; this phase is only needed when embeddings must be decoupled from ingest, or must use the same external embedding provider the rest of neocarta uses.

- Install `neocarta[cli]` on the operator machine. This is a separate install from the cluster's `neocarta[databricks-spark]` wheel and pulls in the CLI dependencies (click, rich, pydantic-settings) plus the enrichment embedding path.
- Configure an embedding provider for the enrichment layer, for example OpenAI `text-embedding-3-small`, which the inline pinned closure deliberately omits. This provider config and its secret live with the operator tooling, not in the committed overlay.
- Run `neocarta databricks embed` as the post-ingest enrichment step. It reads Database/Schema/Table/Column nodes from Neo4j, calls the embedding model, and writes vectors back, all in-process with no cluster. Flags: `--embedding-model`, `--embedding-dimensions`, `--dry-run`, `--json`.
- Document the consistency rule that matters once two modes exist: inline and external use different models at different dimensions (inline `databricks-gte-large-en` at 1024, external OpenAI `text-embedding-3-small`), and a graph cannot mix modes without rebuilding the vector index, since the index is fixed at one dimension. Pick one mode per graph, or point inline at the OpenAI external-model endpoint (registered with `scripts/setup-openai-endpoint.py`, copied into this repo) so both modes produce the same vectors.

**Phase 7 is done when:** an operator can run `neocarta databricks embed` from `neocarta[cli]` against an ingested graph and add vectors with no cluster, and the docs describe both the inline path (dbxcarta's default) and the external path, plus the rule against mixing them on one graph.

---

## What is deliberately left for later

- **Publishing the neocarta wheel to a package index.** This plan stages the wheel from a local folder. When neocarta publishes, the only change here is the one wheel-source setting from Phase 1: swap the local folder for the index plus a version number.
- **The version bump for the release.** The local testing uses whatever version the neocarta build currently carries. The agreed release version is set at publish time, not here.
