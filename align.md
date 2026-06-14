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
- **Stays, but moves off Spark:** the example client. Its real work is Neo4j retrieval, prompt building, model calls, and scoring. A first reading of this plan assumed the client touched Spark only to read the question table and write the summary, so "move it to the warehouse" looked like the whole job. A closer look found Spark in five places, and the central one is the model-call path: SQL generation runs through the Spark `ai_query()` function over a staging Delta table, and three result tables (`run_summary`, `client_retrieval`, `client_questions`) are written as Delta. The simplification (decided in design discussion, see Phase 3) is to take the client fully off Spark and off remote storage: model calls become plain local web calls to the serving endpoint, exactly the pattern `client/embed.py` already uses for embeddings; questions are read from a local JSON file; the run prints a truncated result to the screen; and the Delta tables and the JSON summary file are dropped because nothing reads them. The client becomes a plain local Python command with no cluster and no warehouse writes.
- **Goes away, not moved:** the `upload-questions` command. Phase 2 moved it into the operator tool alongside `ready`, on the assumption a remote job still needed questions staged to a Volume. The Phase 3 simplification removes that need: the client reads questions from a local JSON file, so nothing reads remotely staged questions anymore. The command and its core helper (`upload_questions`, `_validate_questions_file`) are therefore deleted outright. `ready` stays: it is a genuine operator helper that checks warehouse catalog state, and its logic already lives in the shared core.

## The command-line tools, so the change is clear

- **`dbxcarta`** is the single operator command after this change. Today this name belongs to the old pipeline command inside the Spark package, which offers `verify`, `ready`, and `upload-questions`. The pipeline command is deleted. `verify` is retired. `ready` moves to the operator tool, and the operator tool is renamed to `dbxcarta`. `upload-questions` is retired in Phase 3 once the client reads questions from a local file (see the "goes away" note above); Phase 2 moved it before that simplification was settled, so it lived in the operator tool only briefly.
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

- Move the `ready` and `upload-questions` command handlers from the Spark package into the operator tool. The readiness logic they call already lives in the shared core, so only the thin handlers move. (Note added after design review: moving `upload-questions` turned out to be unnecessary. The Phase 3 simplification has the client read questions from a local file, so nothing reads remotely staged questions and `upload-questions` is deleted in Phase 3. It lived in the operator tool only between Phase 2 and Phase 3. `ready` is a genuine operator helper and stays.)
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

**Phase 2 is done when:** `dbxcarta ready` works (and `dbxcarta upload-questions` worked at the time of Phase 2, before Phase 3 retired it), the project builds and installs with no reference to the old package, and the old pipeline command is gone.

**Status: Complete.**

What was done:
- `ready` and `upload-questions` moved from the deleted Spark CLI into the operator tool (`dbxcarta.submit.cli`), wired into its dispatch and help. `verify` was retired, not moved (it checked pipeline output, now neocarta's job). Its `_build_neo4j_driver` helper went with it.
- The operator command was renamed from `dbxcarta-submit` to `dbxcarta` (the name the deleted pipeline command owned). The distribution stays named `dbxcarta-submit`; only the console script changed, plus every invocation (Makefile, scripts, help/prog strings, doc-comments). `dbxcarta --help` resolves and lists all seven commands; the old `dbxcarta-ingest` console script is gone.
- The whole `packages/dbxcarta-spark` folder was deleted, along with its workspace member/source/dependency entries and the regenerated lock (`uv lock` removed `dbxcarta-spark`). The shared runner's default `wheel_package` was repointed off the deleted package to a surviving wheel (`dbxcarta-client`).
- The four external callers were fixed: `run_autotest.py` inlines the tiny id rule (the deleted `generate_id`) and calls the renamed command; `tests/integration/conftest.py` dropped the `run_summary` fixture (the pipeline-output loader); `tests/boundary/test_import_boundaries.py` dropped every `dbxcarta.spark`/`dbxcarta-spark` expectation; `test_overlay.py` was already decoupled in Phase 1.

Deviation (in scope, completing the deletion):
- The plan enumerated "four callers outside the package," but the package's own test suite, `tests/spark/` (34 files), imports `dbxcarta.spark` throughout and had to be deleted with the package — leaving it would have made the default suite red. Phase 4 still removes the CI step that ran "the Spark package's tests," confirming these are that suite. Coverage worth keeping was ported: the `_resolve_questions_file` tests moved into `tests/submit/test_cli.py` (these were later removed in Phase 3 along with the `upload-questions` command), and the live `test_semantic_search.py` was decoupled from the removed summary fixture (its real assertions are about the embedded-graph contract neocarta still produces) rather than deleted.

Superseded in Phase 3: the `upload-questions` command and its core helpers (`upload_questions`, `_validate_questions_file`) were deleted, along with their tests in `tests/submit/test_cli.py` and `tests/core/test_readiness.py`. Phase 2 moved them on the assumption a remote job still needed staged questions; the Phase 3 client reads questions from a local file, so that assumption no longer holds. `ready` is unaffected.

Deferred to later phases (per plan): the `.github/` workflows still naming `dbxcarta-spark` (Phase 4) and the `docs/` references (Phase 5). The remaining `dbxcarta.spark`/`dbxcarta-spark` text in source is intentional provenance comments, not live imports.

Validation: full default suite green (433 passed, 3 deselected — the ~178 `tests/spark/` tests went with the package), `uv sync`/`uv lock` clean, mypy clean on `dbxcarta.submit`, ruff clean, `dbxcarta` command resolves and the old commands are gone.

## Phase 3: Turn the client into local Python and shrink the submit tooling

Take the client off the cluster, then remove the cluster machinery that only the client and the old ingest wheel needed.

**Scope correction from the original framing.** This phase was first written as if the client touched Spark only to read the question table and write the summary, so the plan was to repoint those two operations at the SQL warehouse. A read of the code found Spark in five places, and the central one is the model-call path. The simplifications below were decided in design discussion and are the authoritative Phase 3 plan. They go further than the original "move it to the warehouse" idea: the client moves off Spark **and** off remote reads and writes entirely, becoming a self-contained local command. The reasoning for each choice is recorded inline so a future reader knows it was deliberate, not an oversight.

Client, taken fully local:

- **Model calls become plain local web calls.** SQL generation runs through the Spark `ai_query()` function over a staging Delta table in `client/generation.py`. Replace it with a direct call to the chat serving endpoint, the same `ws.api_client.do("POST", "/serving-endpoints/{endpoint}/invocations", ...)` pattern `client/embed.py` already uses for embeddings. *Why:* a serving endpoint is reachable from any machine with the operator's normal Databricks credentials, with no cluster. Embeddings already prove the pattern works locally, so chat generation joins it instead of going through Spark or a warehouse round trip.
- **Read questions from a local JSON file only.** Drop the read-from-table branch in `client/questions.py:load_questions` and delete the now-dead `manage_questions` function in `client/eval/run.py` (it has no caller) and its export from `client/eval/__init__.py`. *Why:* the client runs locally, so the questions file is a local path. Reading questions from a remote Delta table existed only for the cluster client, which is going away.
- **Cache model responses in a small local file.** The current cache is a per-arm staging Delta table keyed by an input hash. Replace it with a local file keyed by the same hash, so an identical rerun still skips inference. *Why:* keeps the "do not pay for the model twice" behavior without Spark or any remote table.
- **Print a truncated result; drop all persisted client outputs.** Remove the three Delta-table writes (`run_summary` via `summary.py:emit_delta`, `client_retrieval` via `trace.py:emit_retrieval_traces`, `client_questions` via `manage_questions`) and the JSON summary file. Have the run print a truncated summary to the screen. *Why:* a code-wide check found nothing reads any of these client outputs. `scripts/run_autotest.py` reads the **ingest** job's summary JSON, which the neocarta ingest job still writes; it never reads the client's summary, so dropping the client outputs leaves it untouched.
- **Remove the Spark session and the `spark` parameter threaded through the client.** `run.py` no longer calls `SparkSession.builder.getOrCreate()`, and `spark` is dropped from the `arms.py`, `generation.py`, `summary.py`, and `trace.py` seams. *Why:* with generation, caching, question reads, and outputs all off Spark, the session has nothing left to do, and the client is then plain local Python talking to Neo4j and the serving endpoints.
- Confirm the examples run the client locally and still pass their checks.

Retire the operator command the local client makes redundant:

- **Delete the `upload-questions` command and its core helpers.** Remove the `upload-questions` handler and `_resolve_questions_file` from `submit/cli.py`, and `upload_questions` plus `_validate_questions_file` from `core/readiness.py`, with their tests. *Why:* it staged questions to a Volume only so a remote job could read them; the local client reads a local file, so nothing reads remotely staged questions. `ready` stays. **(Done.)**

Shrink the submit tooling now that the client is no longer a cluster entry point:

- Remove the client from the operator tool's set of cluster entry points, so there is no "submit the client to a cluster" path.
- Remove the client's pinned dependency list and the client branch of the core-copying step. The only package the tool still builds and copies core into is `materialize`.
- With the client gone, the generic submit-entrypoint dispatch now has one caller left, ingest. Collapse it into a dedicated "submit ingest" path. Remove the indirection that chose between entry points by name, since there is nothing left to choose between.
- Shrink the entry-point lookup tables to the single ingest entry, and delete the now-unused client and old-ingest entries.

**Phase 3 is done when:** the client runs locally end to end with no cluster and no warehouse or Delta writes, printing a truncated result; the `upload-questions` command is gone; and the submit tooling has a single dedicated ingest-submit path with no name-based dispatch, knowing only about staging the neocarta ingest wheel and building the `materialize` job.

**Status: Complete.** (One piece this phase's framing never named — *how a local operator actually runs the client* once it is off the cluster — turned out to be a distinct gap. It is carved out as its own **Phase 3.5** below rather than buried in this phase's cleanup notes.)

- **`upload-questions` retirement — done.** Command, core helpers, and tests removed.
- **Client off Spark — done.** SQL generation now calls the chat serving endpoint directly per question through `client/local_generation.py` (the `embed.py` web-call pattern), with responses cached in a local `<cache_dir>/<arm>.json` file keyed by the same input hash (`DBXCARTA_CLIENT_CACHE_DIR`, default `.dbxcarta_cache`). `client/questions.py` reads a local JSON file only (the Delta-table branch and `is_table_ref` are gone). The three Delta writes (`run_summary`, `client_retrieval`, `client_questions`) and the JSON summary file are dropped: `summary.py` keeps only the truncated `emit_stdout`, `trace.py` no longer has `emit_retrieval_traces`, and `manage_questions` is deleted. `run.py` no longer creates a `SparkSession`, and the `spark` parameter is gone from the `arms.py`, `generation.py`, `summary.py`, and `trace.py` seams. The client package now contains no `pyspark`/`SparkSession`/`ai_query` live code (only provenance comments remain). Validation: full default suite green (418 passed, 3 deselected), mypy clean (42 source files), ruff clean.
- **Submit-tooling shrink — done.** Dropped the client cluster entry point: removed `_CLIENT_PINNED_CLOSURE` and the `client` keys from every entry-point lookup table (`_ENTRYPOINT_WHEEL_PACKAGE`, `_ENTRYPOINT_CONSOLE_SCRIPT`, `_ENTRYPOINT_PINNED_CLOSURE`, `_ENTRYPOINT_SMOKE_IMPORTS`, `_ENTRYPOINT_JVM_PROBE_CLASS`), removed `dbxcarta-client` from `_CORE_BUNDLE_PACKAGES` (only `materialize` is built and core-bundled now), and collapsed the `submit-entrypoint` command to a dedicated ingest path (`choices=("ingest",)`, keeping the documented `submit-entrypoint ingest` invocation working). The shared runner's default `wheel_package` now points at `dbxcarta-materialize`. **Note on the plan wording:** the plan said "single ingest entry," but `dbxcarta materialize` is also a live submit entry point that routes through the same `_submit_bootstrap_entrypoint` and lookup tables, so ingest **and** materialize entries are retained; only the client was removed. Tests updated (`test_cli.py`, `test_cli_closure.py`); the old client-submit test became a materialize-submit test. Validation: 418 passed, mypy clean, ruff clean.
- **Example overlay question paths — done.** The three `examples/<name>/dbxcarta-overlay.env` now point `DBXCARTA_CLIENT_QUESTIONS` at the committed repo-relative `examples/<name>/questions.json`. The golden net (`test_ops_config_golden.py`) and the finance-genie overlay test were updated: `DBXCARTA_CLIENT_QUESTIONS` is no longer a value derived from the ops Volume, so it is no longer asserted against the `derive_ops_config` Volume-path derivation (that derivation is still covered directly in `test_ops_config.py`).
- **Post-refactor cleanup of the local client — done.** An in-depth review found residue from the removed Delta sinks; cleaned up:
  - **`RetrievalTrace` trimmed** to only the fields the graph_rag arm consumes (`target_schema`, `col_seed_ids`/`tbl_seed_ids`, `schema_scores`, `chosen_schemas`, `final_col_ids`, and the three metric outputs). The vestigial per-question generation/execution fields and their dead writes in `arms.py` (left over from `emit_retrieval_traces`) are gone.
  - **Dead client summary settings removed.** `dbxcarta_summary_volume`/`dbxcarta_summary_table`, their two validators, and the ops-sink branch of `_resolve_defaults` are gone (nothing read them after `emit_delta`/`emit_json` were removed). `_resolve_defaults` now only defaults the embed endpoint. Materialize keeps its own separate summary settings, and the overlays keep `DBXCARTA_SUMMARY_VOLUME` (consumed by materialize).
  - **Stale `client_questions` Volume-path fallback dropped.** Settings no longer derive a `/Volumes/...` questions path; a blank value now fails preflight loudly. (Core `OpsConfig.client_questions` derivation is left intact — it is still covered directly in `test_ops_config.py` and is a candidate for a later core-side trim.)
  - **Stale comments fixed** in the client `pyproject.toml` (no longer core-bundled), the `dbxcarta_client_refresh` setting (local cache file, not staging table), and `graph_retriever.py` (no longer references the deleted `dbxcarta.spark.contract`).
  - **Local client run (finding E).** Taking the client local left it with no working run command: nothing wired the example overlay into its local entrypoint, and the `Makefile` still called the removed `submit-entrypoint client`. This is a distinct piece of work — a missing local-run command — so it is broken out into its own **Phase 3.5** below.
  - Validation after cleanup: 418 passed, mypy clean (42 files), ruff clean.

## Phase 3.5: Give the local client a working run command

Phase 3 took the client off the cluster but never named how a local operator actually runs it. On the cluster the runner forwarded the overlay's `KEY=VALUE` config as job parameters; off the cluster nothing did. The local entrypoint only parsed forwarded params and read the base `.env`, so it never saw the example overlay where the catalog, volume, secret scope, and the local `DBXCARTA_CLIENT_QUESTIONS` path live. And the `Makefile` `e2e_client` target still called `dbxcarta submit-entrypoint client`, which Phase 3 had removed (`submit-entrypoint` now accepts only `ingest`). The net effect was that there was no working command to run the client locally — a real gap the original plan missed.

- Make `dbxcarta-client` load its config the same way the operator `dbxcarta` CLI does: resolve the selected overlay (`DBXCARTA_ENV_FILE`) and the base `.env` through `resolve_env_files`/`load_env_files` (overlay-then-base, `override=False`, so a real exported env still wins) before constructing `ClientSettings`. The local client reads its Neo4j credentials and Databricks auth from the base `.env` and its per-example catalog/volume/questions from the overlay; the overlay stays secret-free.
- Fix the `Makefile` `e2e_client` target to run `DBXCARTA_ENV_FILE=<overlay> uv run dbxcarta-client` instead of the removed `submit-entrypoint client` path.
- Repoint the docs that still tell a reader to run `dbxcarta submit-entrypoint client` (README, the three example READMEs, `scripts/run_demo.py`, `tests/fixtures/README.md`) to the local `DBXCARTA_ENV_FILE=… uv run dbxcarta-client` invocation. (Carried out as part of the Phase 5 docs sweep.)

**Phase 3.5 is done when:** `DBXCARTA_ENV_FILE=examples/<name>/dbxcarta-overlay.env uv run dbxcarta-client` runs the client locally end to end — overlay resolved, local questions file read, serving endpoints and Neo4j reached with no cluster — and no doc or make target references the removed `submit-entrypoint client`.

**Status: Complete.**

- **Entrypoint overlay loading — done.** `client/eval/entrypoint.py:main` passes `sys.argv[1:]` to `resolve_env_files`, so the overlay is selected by either the `--env-file` CLI flag or the `DBXCARTA_ENV_FILE` env var (full parity with the operator CLI, `--env-file` winning), then loads overlay + base `.env` via `load_env_files`, strips the consumed flag from argv, and runs `inject_params` and `run_client`. A bad or missing selected overlay raises `EnvFileError`, which `main` catches to print `error: …` on stderr and exit 2 (matching the operator handlers) rather than surfacing an uncaught traceback.
- **Makefile — done.** `e2e_client` runs `DBXCARTA_ENV_FILE=<overlay> uv run dbxcarta-client`.
- **Docs — done.** The `submit-entrypoint client` references were repointed to the local `uv run dbxcarta-client` command across the README, the example READMEs, `scripts/run_demo.py`, and `tests/fixtures/README.md` (see the Phase 5 completion notes below).
- **Regression test — done.** `tests/client/test_entrypoint.py` asserts `main` loads the overlay from both the `--env-file` flag and the `DBXCARTA_ENV_FILE` env var, strips the consumed flag, and turns a missing selected overlay into a clean exit 2 with no `run_client` call.

## Phase 4: Update the automation (CI)

Fix every automated workflow that still builds, versions, tests, or type-checks the old package.

- In the main test workflow, remove the test entries that run the Spark package's tests and the entries that add it to example test runs.
- In the same workflow, remove the type-check step that targets the Spark package.
- In the publishing workflow, remove the step that builds the Spark wheel.
- In the release workflow, remove the step that bumps the Spark package's version.

**Phase 4 is done when:** every workflow runs clean with no step that touches the removed package.

**Status: Complete.** Every `.github/` workflow is clean of the removed package. The literal `dbxcarta-spark` build/version/test/type-check entries were already gone (`publish.yaml` builds only `dbxcarta-core` + `dbxcarta-client`; `release.yaml` bumps only the published + surviving-example packages; the `supply-chain.yml` layer matrix and mypy targets carry no spark layer). This pass removed the remaining residue: the dead "Set up Java for PySpark tests" step in the `layer-tests` job (no matrix layer runs PySpark — only the `validate` job's full `pytest` run does, via `dbxcarta-materialize`, and it keeps its Java step), and the stale `finance-genie` mention in the `publish.yaml` example-package comment (finance-genie ships no Python and is no longer a workspace member).

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
- Remove every `upload-questions` reference from the docs and the example readmes (Phase 3 deleted the command). Affected files include `README.md`, `docs/tutorials/add-a-data-source.md`, `docs/reference/public-api.md`, `docs/reference/architecture.md`, `docs/proposals/published.md`, the project `CLAUDE.md` "Environment configuration" section, and each `examples/<name>/README.md` and `examples/<name>/src/.../__init__.py` docstring. Replace the readme/CLAUDE.md description of how questions reach the client with the local-file model (the client reads a local `questions.json`), and drop the `dbxcarta upload-questions` command rows and steps.

**Phase 5 is done when:** no doc tells a reader to build, install, or run the removed package, no doc references the retired `upload-questions` command, and the two-step flow is described: run the ingest job from the neocarta wheel, then run the operator follow-up.

**Status: Complete for the ingest/pipeline cutover; client-local doc portions deferred to the user's separate Phase 3 work.**

Guiding principle (agreed with the user): prioritize accuracy and a clean cutover over the literal plan. Where the plan was wrong or incomplete, the docs were made correct rather than following the plan verbatim. The cutover rule applied throughout: **dbxcarta stops documenting code it no longer owns**; pipeline/contract/internal-types content became pointers to neocarta to prevent drift.

What was done:

- **README.md** — reframed dbxcarta as the operator tooling that pulls and runs the neocarta connector; overview/packages diagrams and tables show the `neocarta connector ingest job` and the external neocarta wheel; the `dbxcarta-spark` section became `## The ingest pipeline (neocarta connector)`; the `SparkIngestSettings`/`run_dbxcarta` library example became a `NEOCARTA_DATABRICKS_*` overlay example; `dbxcarta-submit <cmd>` → `dbxcarta <cmd>`; the `dbxcarta verify` block removed; summary-table claim replaced with `summary_<run_id>.json`; the `run_dbxcarta*` auto-attach naming replaced with `submit-entrypoint`; ingest-attributed catalog/schema keys repointed to `NEOCARTA_DATABRICKS_*`.
- **docs/reference/architecture.md** — three-planes diagram and text show the `neocarta connector ingest job`; four-packages-plus-external-neocarta framing; the relocated client-cache "How we validate" subsection (moved here from the deleted public-api.md); ingest-attributed `DBXCARTA_CATALOGS`/extract/layer-tag references repointed to `NEOCARTA_DATABRICKS_CATALOGS` while keeping `DBXCARTA_CATALOG` as the operator anchor and `DBXCARTA_SUMMARY_TABLE` as the live ops-catalog deriver.
- **docs/reference/public-api.md** — deleted (mapped only the old pipeline module names); the still-referenced client-cache section was relocated into architecture.md first; inbound links repointed.
- **docs/reference/pipeline.md, docs/schema/SCHEMA.md, docs/reference/fk-inference-internal-types.md** — rewritten as pointers to neocarta (the connector owns the pipeline, the authoritative graph contract, and the FK-inference typed layer). SCHEMA.md notes neocarta's contract is authoritative.
- **docs/reference/best-practices.md** — cleaned up for the cutover: intro reframed to name neocarta as the ingest-pipeline owner; the Spark/Databricks and Neo4j-connector rules and the ingest-side project principles attributed to the neocarta connector with dead `dbxcarta.spark` module paths removed and env vars corrected to `NEOCARTA_DATABRICKS_*`; the dbxcarta-owned rules (operator submit verification, config boundary, client node-ID identity, materialize run-summary, submit bootstrap smoke check) kept with their verified-real paths. Rule statements, cited sources, and the §5/§7 numbering the project `CLAUDE.md` cross-references were preserved.
- **docs/reference/release.md, docs/reference/simulate-publish.md, docs/tutorials/add-a-data-source.md, docs/security/supply-chain.md, docs/reference/operational-lessons.md, docs/README.md, tests/fixtures/README.md** — published distributions reduced to core + client; build/stage the prebuilt neocarta wheel; consumer pins `dbxcarta-core` + `dbxcarta-client[graph]` with the neocarta wheel staged on the cluster; `dbxcarta-submit <cmd>` → `dbxcarta <cmd>`; `verify` removed; the add-a-data-source overlay key list split into operator/client `DBXCARTA_*` keys and the `NEOCARTA_DATABRICKS_*` ingest contract.
- **examples/{finance-genie,dense-schema,schemapile}/README.md** — `dbxcarta-submit <cmd>` → `dbxcarta <cmd>`; `dbxcarta verify` removed; dependency lines corrected to `dbxcarta-core` + `dbxcarta-client[graph]` (with the neocarta connector noted as a cluster-staged wheel, not a local dep); the `SparkIngestSettings`/`run_dbxcarta` library snippet replaced with the neocarta entrypoint description; ingest schema/embedding keys repointed to `NEOCARTA_DATABRICKS_*`; the run-summary table no longer attributed to ingest (materialize/client write it; ingest writes `summary_<run_id>.json`).

Deviations from the plan (accuracy-driven, in scope):
- The plan named only public-api.md for deletion. pipeline.md, SCHEMA.md, and fk-inference-internal-types.md also documented now-deleted code, so they were converted to neocarta pointers for a clean cutover.
- best-practices.md was a larger cleanup than the plan implied; it is the bulk of the ingest-pipeline knowledge and was reattributed/corrected rather than left pointing at deleted modules.

Completed in the client-local follow-up (once Phase 3.5 made the local-run command real):
- **`upload-questions` doc references — done.** Removed from `README.md`, `docs/tutorials/add-a-data-source.md`, the three example READMEs and their `src/.../__init__.py` docstrings, `docs/proposals/published.md`, and `docs/reference/architecture.md` (the core `upload_questions` mention). Each was rewritten to the local-`questions.json` model: the client runs locally and reads the bundled `questions.json` straight off local disk, with no upload step and nothing staged to a volume.
- **`submit-entrypoint client` doc references — done.** Repointed to the local `uv run dbxcarta-client` (with `DBXCARTA_ENV_FILE`) in `README.md`, the three example READMEs, `scripts/run_demo.py`, and `tests/fixtures/README.md`.
- **Client-running-locally descriptions — done.** The add-a-data-source tutorial no longer models the client as a cluster eval job (its bundle is now one ingest `python_wheel_task`, the client runs locally; the stage table, steps, and checklist follow). Example READMEs describe the client running locally and printing a truncated summary.
- **Project `CLAUDE.md` — done.** The "Critical design rules" section now states neocarta owns the Spark ingest pipeline and dbxcarta is the operator tooling + local client + materialize; the UDF rule is reframed as governing the neocarta pipeline (best-practices §7) with the §5/§7 cross-references preserved. The env-config section drops the `upload-questions` mention for the local-`questions.json` model.

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
