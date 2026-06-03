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
- This is a hard cutover. The old module paths (`spark/databricks.py`, `client/databricks.py`, `client/executor.py`, `spark/presets.py`) are deleted outright. No re-export shims, no deprecation window, no backwards compatibility for the old import paths. Every importer in the repo moves to `dbxcarta.core` in the same change, and anything outside the repo that imported the old paths must move with it. The repo is the full consumer set, so the cutover is atomic and complete.

## Current challenges and problems (plain English)

- The two packages themselves are already well separated. `dbxcarta-spark` does ingest and needs Spark. `dbxcarta-client` does query and evaluation and does not need Spark. Neither one depends on the other. That part is healthy.
- Every example already declares only the client package as its dependency, yet each one imports two things from the Spark package: the preset object and one name-quoting helper. So all three carry an accidental, undeclared Spark dependency that works only because the workspace happens to install Spark alongside them. A clean standalone install of any example would break.
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
- After these moves every example package imports only the client package and the shared core. None imports Spark. Because each example already declares the client as its only dependency, this just makes the code match the declaration. The heavy Spark ingest stays exactly where it runs today, as a submitted cluster job, never an import inside an example package.
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
- Add five modules to `dbxcarta-core`:
  - `core/identifiers.py`: the shared name and path helpers (`validate_identifier`, `split_qualified_name`, `quote_identifier`, `quote_qualified_name`, `validate_uc_volume_subpath`, `parse_volume_path`, `uc_volume_parts`, `uc_volume_parent`, `validate_serving_endpoint_name`, `check_not_protected`, `UC_PROTECTED_NAMES`).
  - `core/catalogs.py`: `resolve_catalogs`, moved verbatim out of `spark/settings.py`. It is the catalog-list policy rule, not a name helper, so it gets its own single-responsibility module and imports `validate_identifier` / `check_not_protected` from `core/identifiers.py` rather than living among them. This is the one shared parser both `SparkSettings` and `ClientSettings` call.
  - `core/workspace.py`: `build_workspace_client`, `read_workspace_secret`.
  - `core/executor.py`: the warehouse runner (`execute_sql`, `execute_ddl`, `fetch_rows`, `split_sql_statements`, `preflight_warehouse`).
  - `core/presets.py`: `ReadinessReport`, the `Preset` / `ReadinessCheckable` / `QuestionsUploadable` protocols, and `StandardPreset`.
- Delete the now-duplicate or relocated files: `spark/databricks.py`, `client/databricks.py`, `client/executor.py`, and `spark/presets.py`.
- Repoint every importer to `dbxcarta.core`:
  - spark: `settings.py`, `cli.py`, `loader.py`, `ingest/summary_io.py`, `ingest/preflight.py`, `ingest/transform/embeddings.py`, `ingest/transform/staging.py`.
  - client: `trace.py`, `embed_probe.py`, `generation.py`, `summary.py`, `settings.py`, `eval/arms.py`, `eval/run.py`.
  - submit: `uc_admin.py`, `cli.py`.
  - examples: the three `preset.py` files, `dense-schema` and `schemapile` `materialize.py` and `question_generator.py`, and `finance-genie` `local_demo.py`.
  - scripts: `scripts/run_demo.py` (imports `client.executor` and `client.databricks`), `scripts/run_autotest.py` (imports `client.databricks` and `client.executor`), and `scripts/run_spike_ai_query.py` (imports `validate_serving_endpoint_name` from `spark.databricks`). These are in-repo dev tooling, so they move with everything else; under the hard cutover they break at import if missed.
- Wire `ClientSettings._validate_catalogs` and `resolved_catalogs` to the shared `resolve_catalogs`, so the client strips the `:layer` suffix the same way the pipeline does. This is the line that fixes the failed run.
- Add the `dbxcarta-core` dependency to `dbxcarta-spark`, `dbxcarta-client`, and `dbxcarta-submit`. Add `dbxcarta-core` to all three example packages, which already declare `dbxcarta-client[graph]`. Once the preset and the name helper move to core, the accidental Spark import disappears, so no example declares or imports Spark.
- Make the cluster runner install the core wheel. The pyproject dependency above is necessary for local and CI resolution but is **not** sufficient on the cluster: the runner installs curated, fully pinned closures with `--no-deps` (`submit/cli.py`, `_INGEST_PINNED_CLOSURE` / `_CLIENT_PINNED_CLOSURE`), so pip performs no resolution and will never pull `dbxcarta-core` transitively. The wheel itself is also installed by name, not resolved. So the change is explicit, in `submit/cli.py`:
  - `publish-wheels` builds and ships the `dbxcarta-core` wheel alongside the spark and client wheels.
  - the bootstrap installs `dbxcarta-core` **before** `dbxcarta-spark` and `dbxcarta-client` on both the ingest and client entrypoints, since each imports from it.
  - add `dbxcarta` (or the relevant top-level import name) to `_ENTRYPOINT_SMOKE_IMPORTS` for both entrypoints so a missing core wheel fails the post-install smoke check loudly rather than at first `import dbxcarta.core`.
  - core pulls in only `databricks-sdk`, which is already DBR-provided (`_DBR_PROVIDED_PACKAGES`), so core adds no new entry to the pinned closures.
- Move the affected tests to `tests/core` and update every import and monkeypatch path to the new locations:
  - `tests/spark/test_databricks_secret.py`, `tests/spark/test_databricks_uc.py`, `tests/client/test_executor.py`, `tests/spark/test_presets.py`, and the three `tests/examples/<name>/test_preset.py` files.
  - `tests/submit/test_cli.py` and `tests/integration/conftest.py` patch helpers by string path (`dbxcarta.spark.databricks...`, `dbxcarta.client.databricks...`); repoint those strings to `dbxcarta.core...` or the patch silently stops taking effect.
  - rewrite the catalog-rule and `:layer` tests to cover the one shared `resolve_catalogs`, and add a client-side test that `ClientSettings` accepts a `catalog:layer` list, the case that failed in production.
- Rebuild the wheels, re-run the `finance-genie` ingest and client jobs, and confirm the client run reaches the per-arm scores.
