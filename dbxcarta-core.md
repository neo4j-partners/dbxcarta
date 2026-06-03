# Proposal: a shared `dbxcarta-core` layer

> **Invariant: this is a code-only refactor.** Every helper moves to a new home with its body unchanged. Code changes location, not behavior. The single intended behavior change is the one catalog-rule fix that unblocks the `finance-genie` client run, and it leaves every already-working configuration identical.

## Key goals (plain English)

- This is a code-only refactor, and all functionality must be maintained. Every example builds the same data, asks the same questions, runs the same arms, and produces the same scores it does today. No CLI, SQL, output, or configuration behavior changes.
- The existing test suite is the proof. The same tests that pass today must pass after the move, with only their import paths updated. Green tests are the evidence that nothing broke.
- Every move is a verbatim relocation. The helpers, the warehouse runner, and the preset are already standalone modules, so they change address, not logic. This is what keeps the change low-risk despite touching several packages.
- Stop copying the same small helpers into two places. Write each one once, in a shared layer both sides use.
- Fix one rule once. The check that decides which catalogs a run touches should live in a single place, so the ingest side and the query side can never disagree about it. This single change is what fixes the `finance-genie` client failure, and it leaves every already-valid configuration untouched.
- Keep the light stuff light. The shared layer pulls in only the Databricks SDK, never Spark, so no package takes on a heavy new dependency.
- Make each example package declare exactly what it uses, so a clean standalone install behaves the same as today's workspace install.
- This is a hard cutover. The old module paths (`spark/databricks.py`, `client/databricks.py`, `client/executor.py`, `spark/presets.py`, `spark/env.py`) are deleted outright. No re-export shims, no deprecation window, no backwards compatibility for the old import paths. Every importer in the repo moves to `dbxcarta.core` in the same change, and anything outside the repo that imported the old paths must move with it. The repo is the full consumer set, so the cutover is atomic and complete.

## Current challenges and problems (plain English)

- The two packages themselves are already well separated. `dbxcarta-spark` does ingest and needs Spark. `dbxcarta-client` does query and evaluation and does not need Spark. Neither one depends on the other. That part is healthy.
- Every example declares the Spark package as a dependency and imports two things from it: the preset object and one name-quoting helper. Both of those are about to move to core, so the Spark dependency is heavier than the example actually needs. Each example would be lighter declaring only the client and the shared core.
- The same tiny helpers exist twice. The function that builds a Databricks connection and the function that safely quotes a table name are copy-pasted into a `databricks.py` file in each package. The client keeps its own copy, and the Spark copy is the one `dbxcarta-submit` reuses. Two copies means two things to keep in sync.
- Those two copies have already drifted, and it caused a real failure. The rule that turns the catalog list into the catalogs a run touches lives only on the Spark side. The client re-implements the same parsing by hand. When the catalog list gained a `catalog:layer` form, the Spark side learned to strip the `:layer` suffix and the client side did not, so the `finance-genie` client run failed at config load on a catalog name it should have accepted.
- The "preset" object that every example declares lives inside the Spark package. That one import is the reason all three examples reach into Spark at all. There is nothing Spark-specific about a preset, so its home forces every example to depend on a package it otherwise would not.
- The warehouse runner that executes table-creation and query statements through the SQL warehouse does not use Spark at all, but it lives next to client query code. Anyone wanting it, including the data-authoring scripts, has to depend on the whole client package.

## Proposed changes (plain English)

- Create a new small shared package, `dbxcarta-core`, that sits underneath the existing packages. `dbxcarta-spark`, `dbxcarta-client`, and `dbxcarta-submit` all depend on it. It stays lightweight and pulls in only the Databricks SDK, never Spark.
- Move the duplicated helpers into `dbxcarta-core` so they exist once: building a Databricks connection, reading a workspace secret, validating a name, and safely quoting a table or volume name. Both copies are deleted and every caller uses the shared one.
- Move the catalog-list rule into its own module in `dbxcarta-core`, and have both the Spark side and the client side call it. One rule, one place, so the ingest side and the query side can never disagree again. This is the change that fixes the `finance-genie` failure.
- Move the warehouse runner into `dbxcarta-core`, since it never needed Spark. The client query path, the data-authoring scripts, and the local demo all use it without taking on the full client package.
- Move the "preset" object and its readiness report into `dbxcarta-core`. Its only remaining ties to the Spark package are the name helper and the catalog rule, both of which are moving to core anyway. After this, no example has a reason to touch the Spark package.
- Move the overlay and env-file loader into `dbxcarta-core`. It lives in the Spark package today but is plain Python config loading with nothing Spark-specific, and `dbxcarta-submit` reaches into Spark only for this and the connection helper. Once both move to core, the job-submission tool depends on core alone and stops pulling in Spark, which is the same "keep the light stuff light" goal applied to submit.
- After these moves every example package imports only the client package and the shared core. None imports Spark. Each example drops its `dbxcarta-spark` dependency and declares `dbxcarta-core` instead, so the declared dependencies match what the code actually imports. The heavy Spark ingest stays exactly where it runs today, as a submitted cluster job, never an import inside an example package.
  - `finance-genie` is the purely query-side example. It has no data-authoring scripts, so it uses the client and core and nothing else.
  - `dense-schema` and `schemapile` also build their demo tables and generate questions. They do that through the shared core for the warehouse runner and the name helpers, plus the client for evaluation. They never import Spark either, today or after the move. Their behavior is unchanged.
- Nothing about the behavior of any example changes. This is an internal move of where code lives, plus the one catalog-rule fix that unblocks the `finance-genie` client run while leaving every working configuration identical.

## What does not change

- Every example builds the same tables, generates the same questions, runs the same eval arms, and produces the same scores it does today.
- The `dbxcarta` and `dbxcarta-submit` CLIs keep the same commands, the same flags, and the same output.
- No overlay, `.env`, catalog list, schema, volume, or any other configuration value changes.
- No function signature, SQL statement, or return shape changes. Functions change files, not bodies.
- The one behavior difference is the fix itself: the client accepts the `catalog:layer` list form it should always have accepted, which unblocks `finance-genie`. Single-catalog setups like `dense-schema` and `schemapile` parse exactly as before.

## The concrete changes

- Add a new package `dbxcarta-core` with the `dbxcarta.core` namespace, depending only on `databricks-sdk`, and register it in the root uv workspace `members` and `sources`.
- Add six modules to `dbxcarta-core`:
  - `core/identifiers.py`: the shared name and path helpers (`validate_identifier`, `split_qualified_name`, `quote_identifier`, `quote_qualified_name`, `validate_uc_volume_subpath`, `parse_volume_path`, `uc_volume_parts`, `uc_volume_parent`, `validate_serving_endpoint_name`, `check_not_protected`, `UC_PROTECTED_NAMES`).
  - `core/catalogs.py`: `resolve_catalogs`, moved verbatim out of `spark/settings.py`. It is the catalog-list policy rule, not a name helper, so it gets its own single-responsibility module and imports `validate_identifier` / `check_not_protected` from `core/identifiers.py` rather than living among them. This is the one shared parser both `SparkSettings` and `ClientSettings` call.
  - `core/workspace.py`: `build_workspace_client`, `read_workspace_secret`.
  - `core/executor.py`: the warehouse runner (`execute_sql`, `execute_ddl`, `fetch_rows`, `split_sql_statements`, `preflight_warehouse`).
  - `core/presets.py`: `ReadinessReport`, the `Preset` / `ReadinessCheckable` / `QuestionsUploadable` protocols, and `StandardPreset`.
  - `core/env.py`: the overlay and env-file loader (`EnvFileError`, `resolve_env_files`, `select_overlay_path`, `load_env_files`, `inject_params`), moved verbatim out of `spark/env.py`. It imports only `os`, `sys`, and `pathlib`, so it carries nothing Spark-specific. Moving it is what lets `dbxcarta-submit` depend on core alone and drop its Spark dependency.
- Delete the now-duplicate or relocated files: `spark/databricks.py`, `client/databricks.py`, `client/executor.py`, `spark/presets.py`, and `spark/env.py`.
- Repoint every importer to `dbxcarta.core`:
  - spark: `settings.py`, `cli.py`, `loader.py`, `entrypoint.py`, `ingest/summary_io.py`, `ingest/preflight.py`, `ingest/transform/embeddings.py`, `ingest/transform/staging.py`.
  - client: `trace.py`, `embed_probe.py`, `generation.py`, `summary.py`, `settings.py`, `eval/arms.py`, `eval/run.py`.
  - submit: `uc_admin.py`, `cli.py` (its only Spark imports are `spark.databricks` and `spark.env`, both moving to core).
  - examples: the three `preset.py` files, `dense-schema` and `schemapile` `materialize.py` and `question_generator.py`, and `finance-genie` `local_demo.py`.
  - scripts: `scripts/run_demo.py` (imports `client.executor` and `client.databricks`), `scripts/run_autotest.py` (imports `client.databricks` and `client.executor`), and `scripts/run_spike_ai_query.py` (imports `validate_serving_endpoint_name` from `spark.databricks`). These are in-repo dev tooling, so they move with everything else; under the hard cutover they break at import if missed.
- Wire `ClientSettings._validate_catalogs` and `resolved_catalogs` to the shared `resolve_catalogs`, so the client strips the `:layer` suffix the same way the pipeline does. This is the line that fixes the failed run.
- Add the `dbxcarta-core` dependency to `dbxcarta-spark`, `dbxcarta-client`, and `dbxcarta-submit`. For `dbxcarta-submit`, also drop the `dbxcarta-spark` dependency: once `databricks.py` and `env.py` move to core, submit imports nothing else from Spark, so it depends only on `dbxcarta-core` and `databricks-job-runner` and no longer pulls in `pyspark` or `neo4j`. Add `dbxcarta-core` to all three example packages, which already declare `dbxcarta-client[graph]`. Once the preset and the name helper move to core, the accidental Spark import disappears, so no example declares or imports Spark.
- Get `dbxcarta.core` onto the cluster by bundling it into each application wheel at build time. The pyproject dependency above is enough for local and CI resolution, but the cluster install has no slot for a separate core wheel, so a different mechanism is needed there. The reason, confirmed in the pinned `databricks-job-runner==0.6.2`:
  - The bootstrap (`bootstrap.py`) does exactly two installs. First the single project wheel from `BootstrapConfig.wheel_volume_path` with `--force-reinstall --no-deps`: ingest gets the `dbxcarta-spark` wheel, client gets the `dbxcarta-client` wheel, one wheel by name. Then the `pinned_closure` with `--no-deps`, whose entries are published PyPI `name==version` strings installed from the index.
  - `BootstrapConfig` has one wheel field, not a list, so there is no way to install a second local wheel. And `dbxcarta-core` is not on any index, so it cannot ride the closure either. A standalone `dbxcarta-core` wheel therefore has nowhere to be installed. Changing the runner to install multiple wheels is out of scope, since the project pins and does not own it.
  - The fix that respects the single-wheel `--no-deps` model: build the `dbxcarta-spark` and `dbxcarta-client` wheels so each physically includes the `dbxcarta/core` modules alongside its own. The source stays in `packages/dbxcarta-core` as the single source of truth; only the wheel build copies it in. A normal `--no-deps` install of either application wheel then carries core with it, with no runner change, no new index, and no closure entry.
  - Mechanics to settle before committing: the app wheels use the `uv_build` backend with a single `module-name`, so including a second `dbxcarta.*` package in each wheel needs either a force-include from the build backend or a copy-of-core-before-build step in `publish-wheels`. Confirm which during Phase 1 of the plan.
  - Add core's top-level import to `_ENTRYPOINT_SMOKE_IMPORTS` for both entrypoints, so a wheel built without the core modules fails the post-install smoke check loudly rather than at first `import dbxcarta.core`.
  - core pulls in only `databricks-sdk`, which is already DBR-provided (`_DBR_PROVIDED_PACKAGES`), so this adds no new entry to the pinned closures.
- Update the tests. A test relocates to `tests/core` only if it tests core code exclusively; a test that also exercises Spark or example wiring stays where it is and only has its import paths repointed.
  - Relocate to `tests/core`: `tests/spark/test_databricks_secret.py`, `tests/spark/test_databricks_uc.py`, `tests/client/test_executor.py`, `tests/spark/test_presets.py`, and `tests/spark/test_env.py`. Each of these tests a helper that now lives in core.
  - Repoint in place: the three `tests/examples/<name>/test_preset.py` files stay under `tests/examples/`. They also exercise `spark.loader` (`load_preset`) and `SparkIngestSettings` overlay parsing, both of which stay in Spark, so only their `StandardPreset` and protocol imports move to `dbxcarta.core`.
  - `tests/submit/test_cli.py` and `tests/integration/conftest.py` patch helpers by string path (`dbxcarta.spark.databricks...`, `dbxcarta.spark.env...`, `dbxcarta.client.databricks...`); repoint those strings to `dbxcarta.core...` or the patch silently stops taking effect.
  - Update `tests/boundary/test_import_boundaries.py` to add `dbxcarta-core` as the new bottom layer:
    - add `"core": ("dbxcarta.spark", "dbxcarta.client")` to `forbidden_by_layer` in `test_source_imports_preserve_layer_boundaries`, so core can never import up into Spark or client (`_layer_source_files` already resolves the `packages/dbxcarta-core/src/dbxcarta/core` path).
    - add a runtime-load check that importing `dbxcarta.core` pulls in neither `dbxcarta.spark`, `dbxcarta.client`, `pyspark`, nor `neo4j`.
    - add `dbxcarta-core` to the `test_distribution_does_not_require_job_runner` parametrize list, since only `dbxcarta-submit` may declare the job runner; optionally add `"core"` to the `test_layer_root_does_not_load_job_runner` parametrize.
  - The catalog-rule and `:layer` tests live in `tests/spark/settings/test_settings_validators.py` and keep passing as is, since `SparkIngestSettings.resolved_catalogs()` still delegates to the shared `resolve_catalogs`. Add a client-side test that `ClientSettings` accepts a `catalog:layer` list, the case that failed in production.
- Rebuild the wheels, re-run the `finance-genie` ingest and client jobs, and confirm the client run reaches the per-arm scores.
