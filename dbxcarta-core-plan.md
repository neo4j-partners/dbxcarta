# Implementation plan: the shared `dbxcarta-core` layer

This is the phased, step-by-step plan for the refactor described in `dbxcarta-core.md`. It is a code-only refactor. Every helper moves to a new home with its body unchanged, except for the single catalog-rule fix that unblocks the `finance-genie` client run. The phases are ordered so the tree stays buildable as you go, and the last phase proves nothing broke.

## Phase 1: stand up the core package — Complete

Create the empty shared package and register it so the workspace knows about it, before any code moves in. Nothing depends on it yet, so this phase cannot break anything.

- [x] Create the `dbxcarta-core` package with the `dbxcarta.core` namespace.
- [x] Declare its only dependency as the Databricks SDK.
- [x] Register it in the root workspace as a member and a source.
- [x] Confirm the workspace still resolves and installs cleanly. (`uv sync`: built + installed `dbxcarta-core==1.1.0`, 90 packages resolved.)

## Phase 2: move the shared code into core — Complete

Relocate the six modules into core, one file at a time, with bodies unchanged. After each move the old file is gone and the new one exists, but nothing imports the new home yet, so expect import errors until Phase 3. Do Phases 2 and 3 together as one working session.

- [x] Move the name and path helpers into `core/identifiers.py`.
- [x] Move the catalog-list rule into `core/catalogs.py` as its own module.
- [x] Move the connection and secret helpers into `core/workspace.py`.
- [x] Move the warehouse runner into `core/executor.py`.
- [x] Move the preset object, readiness report, and protocols into `core/presets.py`.
- [x] Move the overlay and env-file loader into `core/env.py`.
- [x] Delete the old files: the two `databricks.py` copies, the client executor, the spark preset module, and the spark env module. (git tracked all as renames.)

## Phase 3: repoint every importer — Complete

Update every file that imported the old paths to import from `dbxcarta.core` instead. This is a hard cutover with no compatibility shims, so the whole repo moves in one pass. When this phase is done the tree imports cleanly again.

- [x] Repoint the Spark package importers.
- [x] Repoint the client package importers.
- [x] Repoint the submit package importers.
- [x] Repoint the example package importers.
- [x] Repoint the in-repo dev scripts.
- [x] Confirm there are no remaining references to any old import path anywhere in the repo. (grep clean; full import smoke green.)

## Phase 4: apply the one behavior fix — Complete

Wire the client's catalog handling to the shared rule so it strips the `:layer` suffix exactly the way the pipeline does. This is the single intended behavior change and the line that fixes the failed run.

- [x] Point the client's catalog validation and resolution at the shared catalog rule.
- [x] Confirm a single-catalog setup still resolves exactly as before. (`['finance']`.)
- [x] Confirm a `catalog:layer` list is now accepted on the client side. (`bronze:bronze,silver:silver,gold:gold` → `['bronze','silver','gold']`.)

## Phase 5: fix the dependency declarations — Complete

Make every package declare what it now uses, so a clean standalone install matches the workspace install.

- [x] Add the core dependency to the Spark, client, and submit packages.
- [x] Add the core dependency to all three example packages (and swap each example's `dbxcarta-spark` workspace source for `dbxcarta-core`).
- [x] Drop the Spark dependency from the submit package, since it now needs only core and the job runner.
- [x] Confirm no example declares or imports Spark. (Submit's only remaining `dbxcarta-spark` references are the wheel-package name strings for the ingest job it ships — not an import or a dependency.) `uv lock` + `uv sync` clean.

## Phase 6: get core onto the cluster — Complete (cluster validation deferred to Phase 8)

The local dependency declaration is enough for resolution, but the cluster bootstrap installs only a single application wheel by name plus a published-package closure, with no slot for a separate core wheel and no way to pull one from an index. So core rides along by being bundled into each application wheel at build time. The source stays single in the core package; only the wheel build copies it in.

- [x] Decide how the build includes core in each wheel: **both levers, since uv_build needs both.** The spark and client build backends set `module-name = "dbxcarta"` with `namespace = true` (a stable change — a normal build with no core copied in yields exactly today's contents), and `dbxcarta-submit publish-wheels` copies the core source into each entrypoint package's `src/dbxcarta/core` for the duration of the build (`_core_bundled_into`), removing it after. A force-include alone is not supported by uv_build 0.11; the copy step alone would not be packaged unless the backend also selects the whole `dbxcarta` namespace.
- [x] Build the Spark and client wheels so each physically carries the core modules alongside its own. (Verified locally: both wheels contain all six `dbxcarta/core/*.py` + `py.typed`; a normal build carries none.)
- [x] Add a post-install smoke import for core so a wheel built without it fails loudly rather than at first use. (`dbxcarta.core` added to both entrypoints' `_ENTRYPOINT_SMOKE_IMPORTS`; the bootstrap `importlib.import_module`s it.)
- [x] Confirm core adds no new entry to the pinned closures, since its one dependency (`databricks-sdk`) is already in `_DBR_PROVIDED_PACKAGES`. (No closure change.)

Notes: `.gitignore` guards the transient `packages/dbxcarta-{spark,client}/src/dbxcarta/core/` copies against an interrupted publish run. The end-to-end cluster install + import is the one thing that cannot be proven locally; it is covered by the Phase 8 live job re-runs.

## Phase 7: move and update the tests — Complete

Relocate only the tests that test core code exclusively. Tests that also exercise Spark or example wiring stay where they are and get import repoints. Add the one new test for the fix.

- [x] Relocate the core-only tests into `tests/core` (5 files; imports repointed to core).
- [x] Repoint, in place, the example preset tests and any test that also touches Spark (3 example preset tests keep `spark.loader`/`SparkIngestSettings`, move only `StandardPreset` + protocols to core).
- [x] Repoint the string-path patches in the submit and integration tests, including the env path, so the patches keep taking effect (`dbxcarta.core.env.load_env_files`, `dbxcarta.core.workspace.build_workspace_client`).
- [x] Add core as the bottom layer in the import-boundary tests (forbidden-by-layer entry, runtime-load check, job-runner parametrize, distribution parametrize).
- [x] Add a client-side test that accepts a `catalog:layer` list, the case that failed in production (`tests/client/test_settings.py`).

Also added `core/py.typed` and `-p dbxcarta.core` to the CI mypy command so core is type-checked. Full suite: **508 passed, 1 skipped**; boundary suite 11 passed; ruff clean; mypy clean across all four packages.

## Phase 8: review and prove it — Complete

The existing test suite is the proof that the refactor changed location, not behavior. The live runs then confirmed the fix works end to end on real infrastructure, and surfaced two bugs the local proof could not reach (recorded below): the test suite cannot exercise the cluster install/import path or a clean-slate ops-table recreate.

- [x] Run the full test suite and confirm it is green. (**508 passed, 1 skipped, 6 deselected** at the time; **525 passed** now, including the two new regression guards below.)
- [x] Run the import-boundary tests and confirm core stays the bottom layer with no upward imports. (11 passed, incl. the new core checks.) Also: ruff clean, mypy clean across all four packages.
- [x] Rebuild all wheels. (`uv build --all-packages` builds all seven, incl. the new `dbxcarta-core` wheel; the bundled publish build was verified to carry `dbxcarta/core` in the spark and client wheels.)
- [x] Re-run the `finance-genie` ingest job and confirm it succeeds. (Run `77902504554814`: `Result: SUCCESS`; the run summary writes cleanly.)
- [x] Re-run the `finance-genie` client job and confirm it reaches the per-arm scores. (Run `861343423268463`: `no_context` correct 100% / exec 16.7%, `schema_dump` correct 91.7% / exec 100%, `graph_rag` correct 100% / exec 100%, matching the README reference.)
- [ ] Spot-check that the other examples still build the same tables, ask the same questions, and report the same scores. (Not yet re-run on live infra; `dense-schema` and `schemapile` share the same code paths the `finance-genie` run exercised.)

### Bugs surfaced by the live runs (fixed)

- **Smoke check named a wheel module.** Phase 6 added `dbxcarta.core` to `_ENTRYPOINT_SMOKE_IMPORTS`, but the pinned `databricks-job-runner==0.6.2` bootstrap runs the smoke check before it prepends the per-run wheel target to `sys.path`, so the import failed every cluster run with `No module named 'dbxcarta'`. The wheel bundling itself was correct. Fix: drop `dbxcarta.core` from both smoke lists (they may name only shared-environment packages) and move the "wheel carries core" guarantee to a build-time assertion, `_assert_wheel_bundles_core`, in `publish-wheels`. Guarded by a new test that the smoke lists hold no `dbxcarta.*` entry. See best-practices §10 (Project-level).
- **Run-summary type drift (pre-existing on `main`).** The preflight `CREATE TABLE` declared `embedding_failure_threshold DOUBLE` while the writer wrote `LongType`; `dbxcarta_embedding_failure_max` is an `int` count, so `BIGINT`/`LongType` is correct. The conflict was masked while the table existed and only failed once the table was recreated for a clean run. Fix: preflight DDL `DOUBLE → BIGINT`. The writer schema and preflight columns are now each exposed in one place and pinned together by `tests/spark/test_summary_schema_agreement.py`. See best-practices §9 (Project-level).
- **`bootstrap` could not run on a Default-Storage account.** `CREATE CATALOG IF NOT EXISTS` is rejected without a `MANAGED LOCATION` on accounts with Default Storage and no metastore storage root, even when the catalog already exists. Fix: guard the create behind a `catalog_exists` check (`SHOW CATALOGS`) so bootstrap skips it when the catalog is already present, keeping the command idempotent.

## Review checklist before calling it done

- [x] Every move was a verbatim relocation, with the single exception of the client catalog fix. (Bodies unchanged; only import lines repointed.)
- [x] No old import path survives anywhere in the repo. (grep sweep clean.)
- [x] Submit no longer depends on Spark, and no example depends on Spark. (Submit's only `dbxcarta-spark` references are wheel-name strings for the ingest job it ships.)
- [x] The full suite and the boundary tests are green.
- [x] The `finance-genie` client run, which failed before, now completes. (Live-verified: ingest run `77902504554814` and client run `861343423268463` both SUCCESS, per-arm scores match the README; the client accepts and strips the `catalog:layer` list as intended.)
- [x] No CLI command, flag, output, SQL statement, function signature, or config value changed. (Only intended change: the client catalog-list behavior fix.)

## Resolved — `python-dotenv` for a standalone `dbxcarta-submit` install

`core/env.py` lazily imports `python-dotenv` in `load_env_files`. Before the refactor, `env.py` lived in `dbxcarta-spark`, which declares `python-dotenv`, so `dbxcarta-submit` got it transitively. After the refactor submit depends only on `dbxcarta-core` + the job runner, neither of which pulled `python-dotenv`, so a clean standalone `pip install dbxcarta-submit` would have failed when `bootstrap`/`teardown` call `load_env_files`. **Resolved:** `dbxcarta-core` now declares `python-dotenv` (the package that actually uses it; tiny and pure-Python, so core stays lightweight). Every consumer gets it transitively. Boundary tests confirm core still requires no job runner.
