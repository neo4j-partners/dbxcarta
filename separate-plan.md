# Plan: Split Job Submission Into `dbxcarta-submit`

This is the execution plan for the proposal in `separate.md`. Read that first for the reasoning. This file is the source of truth for status.

## Goal

Move all Databricks job-submission machinery out of the core `dbxcarta-spark` package into a new operator-local package, `dbxcarta-submit`, so the core and client packages no longer carry `databricks-job-runner` in their dependency closure. Ingestion, verify, preset, and the on-cluster entrypoint behave exactly as before. Operators submit and upload through a new `dbxcarta-submit` command.

## Assumptions

- All submission machinery currently lives in `packages/dbxcarta-spark/src/dbxcarta/spark/cli.py`, and every `databricks_job_runner` import is confined to that file. This is the seam the split follows.
- The on-cluster ingest entrypoint is a separate console script and module; it is not part of the submission code and does not move.
- The env-overlay helpers live in `dbxcarta.spark.env` and stay in core. `dbxcarta-submit` imports them from core.
- The repo uses a uv workspace with both packages as members. The new package joins the same workspace.
- `dbxcarta-client` already excludes the job runner and needs no source change.

## Decisions (locked)

- **New package and command are both `dbxcarta-submit`.** Reads clearly as the submit/operator tool and avoids confusion with the upstream `databricks-job-runner` it wraps. Dropped: `dbxcarta-job-runner` (collides with the upstream name), `dbxcarta-ops`.
- **Core keeps the `dbxcarta` command for `verify` and `preset`.** These are domain commands with no submission concern. Operators relearn one command name for submit/upload/logs; everyone else is unaffected. Dropped: moving verify/preset to a new command.
- **`dbxcarta-submit` is operator-local only, not published as a wheel.** It runs on the submitting machine and is never installed on cluster, so there is nothing to distribute.
- **Dependency direction is one way.** `dbxcarta-submit` depends on the core and the job runner. Core and client never depend on `dbxcarta-submit` or the job runner.

## Deliberately not doing

- Not changing `databricks-job-runner`. It stays generic; no dbxcarta values or domain logic move into it.
- Not changing the on-cluster ingest entrypoint, bootstrap path, or the Spark pipeline.
- Not changing the client package source or its console scripts.
- Not adding a fourth shared package. Shared env handling stays in core and is imported by `dbxcarta-submit`.
- Not publishing `dbxcarta-submit` as a distributed artifact.
- Not redesigning the CLI argument parsing. The split preserves the existing dispatch style.

## Risks

- **Import-time coupling.** The core CLI builds a job runner at module import today. If any core path still imports that code after the split, the job runner stays in the closure. Mitigated by the Phase 5 closure test.
- **Bootstrap run-name wiring.** The `cli_command` value feeds runner metadata such as run names. It must move with the submission code and update to the new command, or submitted runs are misnamed.
- **Unknown-subcommand behavior change.** The core CLI loses its `runner.main()` fallback. Core must explicitly reject non-`verify`/`preset` input instead of silently passing it through.
- **Lockfile and workspace drift.** Adding a member without regenerating the lock, or leaving the job runner in core's pins, would mask the separation.

## Phase checklist

### Phase 1 — Skeleton package (Complete, reviewed)
Outcome: `dbxcarta-submit` exists, builds, and installs, with no logic yet.
- [x] Add `packages/dbxcarta-submit` with its own project file.
- [x] Declare dependencies: the job runner and the core package.
- [x] Add it as a workspace member and regenerate the lockfile.
- [x] Confirm it builds and installs.

Validation: `uv lock` added `dbxcarta-submit v1.0.0`; `uv build --package dbxcarta-submit` produced the wheel and sdist; `uv sync` plus `import dbxcarta.submit` and `import dbxcarta.spark` both succeed. Built wheels are gitignored under `dist/`.
Notes: layout mirrors the existing packages exactly. Implicit namespace package under `src/dbxcarta/submit`, `uv_build` backend with `module-name = "dbxcarta.submit"`, `py.typed` marker, module docstring stating the package owns all job-runner contact. No console script yet; that is Phase 4. Core still carries the job runner; trimming is Phase 3.

### Phase 2 — Move the submission code (Complete, reviewed)
Outcome: all submission logic lives in `dbxcarta-submit` and runs under the new command.
- [x] Move the entrypoint table, pinned closures, smoke imports, and the Neo4j connector probe.
- [x] Move the runner wiring: the shared runner and the ingest runner with its connector preflight.
- [x] Move the submit-entrypoint command and the wheel upload command.
- [x] Point the new command at the generic job-runner pass-through for submit, validate, logs, and clean.
- [x] Import the env-overlay helpers from core rather than duplicating them.
- [x] Update the `cli_command` metadata to the new `dbxcarta-submit` command.
- [x] Resolve the `upload` dispatch: kept the `--wheel` guard (behavior-preserving). Still flagged for the user; not dropped.

Validation: `uv build` and `uv sync` succeed; `dbxcarta-submit --help` lists the generic runner commands; `import dbxcarta.submit.cli` constructs the Runner; mypy clean under the repo's strict config; existing core CLI/closure/boundary tests pass (13 passed).
Notes:
- Core `cli.py` is intentionally untouched in this phase. The submission code now exists in both places; Phase 3 removes it from core. This keeps Phase 2 in a valid, testable state.
- Resequenced: the `dbxcarta-submit` console script is registered now rather than in Phase 4, because the phase outcome requires the command to run. Phase 4 still verifies both commands and updates docs.
- The TYPE_CHECKING block from core (`WorkspaceClient`, `Driver`, `SparkIngestSettings`) was not carried over; the submission module does not use those types.
- `_DBR_PROVIDED_PACKAGES` is referenced by `tests/spark/test_cli_closure.py`, so it is part of the closure surface and moves with the submission code in Phase 5.

### Phase 3 — Trim the core (Complete, reviewed)
Outcome: core builds with no reference to the job runner anywhere in its closure.
- [x] Reduce the core `dbxcarta` command to `verify` and `preset` only.
- [x] Replace the old runner fallback with a usage message and non-zero exit for any other subcommand.
- [x] Remove the job runner from the core package's dependencies.
- [x] Confirm the core builds and installs with no job runner present.

Validation: the rebuilt `dbxcarta-spark` wheel's `Requires-Dist` no longer lists `databricks-job-runner`; `uv tree` shows the job runner absent from both the core and client resolved closures; mypy clean across `dbxcarta.spark`, `dbxcarta.client`, and `dbxcarta.submit`; `dbxcarta` with no args and with an unknown command prints usage and exits 2; `dbxcarta verify --help` and `dbxcarta preset` still dispatch; the import-boundary tests pass.
Notes:
- Also removed two pre-existing dead imports surfaced by the trim: `from pathlib import Path` (never used) and `TypedDict` (only used by the moved `_RunnerKwargs`).
- The overlay banner is kept for verify/preset but no longer sets `runner.env_file`, since the runner is gone.
- Expected, deferred breakage: `tests/presets/test_cli.py` and `tests/spark/test_cli_closure.py` are entirely submission tests that import moved symbols from `dbxcarta.spark.cli`. They fail until Phase 5 repoints them at `dbxcarta.submit.cli`.

### Phase 4 — Wire commands and update docs (Complete, reviewed)
Outcome: each command resolves to the right package and the guidance reflects the split.
- [x] Register the console scripts so `dbxcarta` lands in core and `dbxcarta-submit` lands in the new package (verified both resolve; `dbxcarta-ingest` on-cluster script untouched).
- [x] Update the env-overlay documentation and any references that assume one package.
- [x] Update the project guidance files that describe how to run ingestion and how to submit jobs.

Validation: ruff (`uvx ruff check .`) and mypy (all three packages) clean; `dbxcarta` shows the verify/preset usage; `dbxcarta-submit --help` lists the submission and generic-runner commands; a repo-wide grep confirms no remaining `dbxcarta <submission-cmd>` references outside the deliberately-deferred proposals.
Files changed (command rename `dbxcarta` -> `dbxcarta-submit` for upload/submit-entrypoint/generic-runner commands only; `verify`/`preset` left on `dbxcarta`):
- Executable: `scripts/run_autotest.py` (the real break the review caught), `scripts/run_demo.py` help text, `scripts/run_spike_ai_query.py` docstring, `packages/dbxcarta-client/.../eval/run.py` help string.
- Docs: `README.md`, `examples/{dense-schema,finance-genie,schemapile}/README.md`, `tests/fixtures/README.md`, `docs/reference/best-practices.md`, `docs/security/supply-chain.md`.
- CI: `.github/workflows/supply-chain.yml` mypy step gains `-p dbxcarta.submit`.
Scoping decisions:
- `docs/proposals/*` (`fix-zombines-v4.md`, `tuning.md`, `env-layering.md`) left unchanged: they are historical design records, and rewriting their internal narratives would misrepresent what was true when written.
- Project `CLAUDE.md` needed no command edits: it has no `submit-entrypoint`/`upload` references, and its generic `uv run dbxcarta <cmd> --env-file` env example still holds for verify/preset.
- The CI test matrix still has no `submit` layer and `uv run pytest` will be red until Phase 5 repoints the two moved submission test files. Adding a `submit` matrix layer is Phase 5 work, since it depends on where those tests land.

### Phase 5 — Move and update tests (Complete, reviewed)
Outcome: tests live with the code they cover and the separation is guarded.
- [x] Move `tests/spark/test_cli_closure.py` and the submission half of `tests/presets/test_cli.py` into `tests/submit/`, repointing their imports from `dbxcarta.spark.cli` to `dbxcarta.submit.cli`.
- [x] Keep the verify/preset core test in core: `test_build_neo4j_driver_raises_on_missing_secret` exercises `dbxcarta.spark.cli._build_neo4j_driver` (the verify path), so it stays on `dbxcarta.spark.cli` and moved to `tests/spark/test_cli.py`. No dedicated `preset`-command test exists today; that is a pre-existing coverage gap, not introduced here.
- [x] Add a dependency-closure test in `tests/boundary/test_import_boundaries.py`: `test_layer_root_does_not_load_job_runner` asserts importing `dbxcarta.spark`/`dbxcarta.client` never loads `databricks_job_runner`, and `test_distribution_does_not_require_job_runner` asserts it is absent from each one's `Requires-Dist` (via `importlib.metadata.requires`).
- [x] Update the CI mypy invocation to add `-p dbxcarta.submit` (already landed in Phase 4, line 115 of `supply-chain.yml`; confirmed).

Validation: `uv run pytest tests/submit tests/spark/test_cli.py tests/boundary` -> 17 passed; full `uv run pytest` no longer errors at collection (the remaining local failures are a `sparkDriver` `BindException` from the `local_spark` fixture on this machine, an environment limit unrelated to the split — those tests run in CI with Java/network); mypy clean across all three packages; ruff clean.
Notes:
- `tests/presets/test_cli.py` was not a clean wholesale move. The file was misnamed (its docstring read "dbxcarta CLI guardrails", not presets) and mixed five submission tests with one verify-path test. It was split rather than moved, and the now-empty `tests/presets/` directory was removed.
- CI test matrix: the `presets` layer (which ran the now-deleted `tests/presets`) is replaced by a `submit` layer (`package: dbxcarta-submit`, `tests: tests/submit`). Syncing `dbxcarta-submit` pulls the core and the job runner the submission tests need.
- `tests/submit/test_cli.py` and `tests/spark/test_cli.py` share a basename; `--import-mode=importlib` (already configured) collects both without an `__init__.py`, matching the existing convention for `tests/spark` and `tests/boundary`.
- The submission `_RunnerStub.cli_command` was corrected to `"uv run dbxcarta-submit"` to match the renamed command.

### Phase 6 — Verify end to end (Pending)
Outcome: every path behaves as before, with the job runner absent from core and client.
- [ ] A client install brings in the core with no job runner present.
- [ ] `verify` and `preset` work from the core `dbxcarta` command.
- [ ] `dbxcarta-submit` submits both the ingest and client entrypoints, and upload publishes both wheels.
- [ ] An on-cluster ingestion run still succeeds.

## Completion criteria

- The job runner appears only in the `dbxcarta-submit` closure, enforced by an automated test.
- `dbxcarta verify` and `dbxcarta preset` behave identically to before.
- `dbxcarta-submit` performs every submit and upload action the old `dbxcarta` command did.
- On-cluster ingestion is unchanged.
- Docs and project guidance describe the two commands correctly.
