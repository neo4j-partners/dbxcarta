# Separating Job Submission Into Its Own Package

## Key goals

- Keep the core ingestion package free of any job submission dependency, so it never pulls in `databricks-job-runner`.
- Let client consumers install the core without dragging in submission machinery they will never use.
- Move job submission into a thin, separate package, `dbxcarta-submit`, that depends on `databricks-job-runner` and on the core package.
- Leave `databricks-job-runner` generic. None of dbxcarta's project specific values or domain logic should move up into the shared library.
- Keep the same behavior for everyone who runs ingestion, verification, or presets today. The `dbxcarta` command keeps `verify` and `preset` unchanged, and the on cluster ingestion entrypoint is untouched. Operator submission commands move to a new `dbxcarta-submit` command, so the only change anyone sees is the command name used to submit and upload.

## High level design

Today there is one package that does three different jobs at once: it runs the Spark ingestion, it runs dbxcarta domain commands like verify and preset, and it submits jobs to Databricks. Only the submission job needs `databricks-job-runner`, and only the submission job is unwanted by client consumers. The plan splits the submission job out.

The end state is three packages with a clear, one way dependency flow.

- **Core ingestion package** (the existing `dbxcarta-spark`): the Spark pipeline, the on cluster entrypoint, the shared config and helpers, and the domain commands. It depends on Spark, Neo4j, and pydantic. It does not depend on the job runner.
- **Client package** (the existing `dbxcarta-client`): consumed by clients that read the graph but never submit jobs. It already excludes the job runner from its dependency closure, so it stays as it is.
- **New submission package** (`dbxcarta-submit`): the only place that talks to `databricks-job-runner`. It depends on the job runner and on the core package. It owns the operator facing commands that build wheels, upload them, and submit jobs, under a new `dbxcarta-submit` command. It is operator-local tooling, run on the machine that submits jobs, so it is never installed on cluster and is not published as a distributed wheel.

Today every piece of submission machinery already lives in a single file, `packages/dbxcarta-spark/src/dbxcarta/spark/cli.py`, and every `databricks_job_runner` import is confined to that file. The split is therefore a clean lift of the submission half of that one file into the new package, leaving `verify` and `preset` behind.

The dependency direction is the whole point.

- The submission package depends on the core package, never the other way around.
- The core package and the client package never depend on the submission package and never depend on the job runner.
- Clients install only what they need. Operators who submit jobs install the submission package, which brings the core along with it.

## What stays in the core package

- The Spark ingestion pipeline and the on cluster entrypoint that the bootstrap invokes.
- The shared configuration and helpers: settings, environment overlay loading, data contracts, and the Databricks client helpers.
- The preset loader and preset definitions.
- The two domain commands that have nothing to do with submission: the verify command, which checks the resulting graph, and the preset command, which runs ingestion locally or checks readiness.

## What moves to the new submission package

- The table of named entrypoints and their submission settings: the pinned dependency closures, the console script names, the Neo4j connector probe, and the smoke import lists.
- The runner wiring, including the shared runner and the ingest runner with its connector preflight.
- The submit entrypoint command that builds the bootstrap config and submits the job.
- The wheel upload command.
- The pass through to the generic job runner commands such as submit, validate, logs, and clean.

## What is shared and how

- The submission package needs a small amount of environment handling that already lives in the core, such as selecting the active environment overlay. Because the submission package already depends on the core, it imports those helpers from the core. No fourth shared package is needed.
- The submission package knows the names of both wheels it can submit, the core ingestion wheel and the client wheel, because it is responsible for submitting both.

## Decisions (confirmed)

- The new submission package is named `dbxcarta-submit`, and its command line entry point is `dbxcarta-submit`.
- The core keeps the `dbxcarta` command name for the domain actions, verify and preset. The submission package gets its own `dbxcarta-submit` command for the operator actions.
- Verify and preset stay in the core. They are domain commands and do not belong with submission.
- `dbxcarta-submit` is operator-local only. It is not published as a wheel.

## Open behavior questions to settle during implementation

- **Unknown subcommands on `dbxcarta`.** Today `cli.py` ends by dispatching any unrecognized subcommand to `databricks_job_runner.Runner.main()`. Once the core drops the job runner, the core `dbxcarta` command no longer has that fallback. It must print usage and exit non-zero for anything other than `verify` and `preset`. The new `dbxcarta-submit` command owns the generic pass through to the runner.
- **The `cli_command` string.** `_RUNNER_KWARGS` sets `cli_command` to `"uv run dbxcarta"`. This feeds runner wiring such as run names. It moves with the submission code and should become `"uv run dbxcarta-submit"`.
- **The `upload` dispatch guard.** ~~Today `upload` is dispatched only when `--wheel` appears in argv. Confirm whether that guard is still intended or an artifact to drop when the command moves.~~ Resolved: the `--wheel` sentinel was an unparsed argv string that overloaded the runner's own `upload` command. The dbxcarta wheel-publish is now a first-class `dbxcarta-submit publish-wheels` command with its own help; the generic `upload` (file/`--all`/`--data`) passes through to the runner unchanged.

## Phased implementation approach

### Phase 1: Create the new submission package

- Add the `dbxcarta-submit` package alongside the existing packages with its own project file.
- Register it as a member of the root workspace and regenerate the lockfile.
- Declare its dependencies: the job runner and the core package.
- Leave it empty of logic for now. The goal is a buildable, installable skeleton.

### Phase 2: Move the submission code

- Move the entrypoint table, the runner wiring, the submit entrypoint command, and the wheel upload command into the new package.
- Point the new package's command at the generic job runner pass through for the remaining operator commands.
- Have the new package import the small environment helpers it needs from the core.

### Phase 3: Trim the core package

- Reduce the core `dbxcarta` command to the domain actions only, verify and preset.
- Replace the old `runner.main()` fallback with a usage message and a non-zero exit for any other subcommand.
- Remove the job runner from the core package's dependencies.
- Confirm the core builds and installs with no reference to the job runner anywhere in its dependency graph.

### Phase 4: Wire the commands and update configuration

- Register the console scripts for both packages so each command lands in the right package.
- Update the environment overlay documentation and any references that assume a single package.
- Update the project guidance files that describe how to run ingestion and how to submit jobs.

### Phase 5: Move and update the tests

- Move the submission tests, the entrypoint table, upload, and submit-entrypoint tests, into the `dbxcarta-submit` package.
- Keep the verify and preset tests in the core.
- Add a dependency-closure test that fails if `databricks-job-runner` ever re-enters the core or client dependency graph. This is what keeps the separation from regressing.

### Phase 6: Verify end to end

- Confirm a client install brings in the core with no job runner present.
- Confirm the verify and preset commands still work from the core `dbxcarta` command.
- Confirm `dbxcarta-submit` can submit both the ingest entrypoint and the client entrypoint, and that upload still publishes both wheels.
- Confirm the on cluster ingestion run still succeeds, since the core entrypoint and bootstrap path are unchanged.
