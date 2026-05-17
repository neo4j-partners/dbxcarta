# Proposal: End-to-End Verification of the Dense-Schema Pipeline

## Summary of the Goal

Run the entire `examples/dense-schema/` pipeline from a clean start
all the way through to a finished client evaluation, and confirm every stage
runs and passes. Success means one thing in plain English: we go from source
code to a synthetic 500-table schema in Unity Catalog, ingest it, run the
dbxcarta client evaluation against it, and finish with no failed stage and no
unhandled error. If any stage fails, the failure is captured in the work log
with the exact command and error so it can be fixed.

## Section 1: Problem Statement

The dense-schema example is a first-class CI sample consumer that exercises
the full dbxcarta path: environment layering, synthetic schema generation,
Unity Catalog materialization, question generation, ingest, and client
evaluation. Today we do not have a single, repeatable, recorded run that
proves the whole chain works together end to end on a live workspace. Stages
are usually run piecemeal, so a regression in one stage (for example a CLI
subcommand that the README references but the CLI no longer exposes) can go
unnoticed until much later. The impact is low confidence that a fresh
checkout can produce a working evaluation, and slow diagnosis when it cannot.

## Section 2: Proposed Solution

Execute the documented dense-schema flow in order, on the configured live
Databricks workspace, treating each documented command as a checkpoint. The
run is driven by a background execution agent so the long-running Databricks
stages (materialize, ingest, client) can proceed while progress is monitored
and the work log is kept current. Each stage is marked pass or fail with the
command used and the relevant output. Where the README and the current CLI
disagree (the README references `dbxcarta preset` and
`dbxcarta submit-entrypoint`, which the current CLI help does not list), the
run surfaces the discrepancy explicitly rather than guessing, and records the
actual working command.

Expected outcome: a completed work log in this file showing every phase with a
status, validation evidence, and either a clean pass or a precisely described
failure point.

## Section 3: Requirements

1. The repo dependencies and the dense-schema example install cleanly.
2. The synthetic schema generates locally without error.
3. Questions generate (or the committed `questions.json` is used) and validate
   as a non-empty array.
4. The schema materializes into Unity Catalog on the configured profile.
5. The dbxcarta preset resolves and prints its environment.
6. Questions upload to the configured volume path.
7. The ingest stage completes successfully against the materialized schema.
8. The client evaluation stage completes successfully and produces a run
   summary.
9. Every stage's status, command, and evidence is recorded in the work log
   below.

## Assumptions

- The configured Databricks profile (`azure-rk-knight`) is authenticated and
  the warehouse / compute referenced by the env config is available.
- The root `.env` plus any dense-schema env config provide the required
  catalog, volume, and endpoint values.
- A live workspace and Neo4j instance are reachable for the ingest and client
  stages.
- The committed `questions.json` is acceptable if question generation is slow
  or unavailable; regenerating is optional, not required for a pass.

## Risks

- **CLI vs README drift**: the README's `dbxcarta preset` /
  `dbxcarta submit-entrypoint` commands may not match the current CLI. The run
  records the real command and flags this as a documentation bug rather than
  failing silently.
- **Long-running stages**: materialize, ingest, and client may each take many
  minutes. The background agent handles these; the monitor checks progress
  rather than blocking.
- **Cost / shared environment**: materialize and ingest create and write
  Unity Catalog objects on a shared workspace. This is expected for this
  example but is noted so reruns reuse the same schema rather than multiplying
  objects.
- **Partial failure**: a later stage may fail after earlier stages pass. The
  work log preserves the last good stage so the run can resume from there.

## Phase Checklist

### Phase 1 — Environment and install
Outcome: dependencies and the dense-schema example are installed and the CLI
is invokable.
- [x] `uv sync` completes
- [x] `uv pip install -e examples/dense-schema/` completes
- [x] `uv run dbxcarta --help` runs
- [x] dense-schema env config is identified (root `.env` / `.env.sample`
      values present for catalog, volume, warehouse)
Completion criteria: all commands exit 0 and required env keys are resolved.

### Phase 2 — Generate synthetic schema
Outcome: a synthetic schema is generated locally.
- [x] `uv run dbxcarta-dense-generate --tables 500` completes
- [x] Generation output / artifacts confirmed present
Completion criteria: command exits 0, generated schema artifact exists.

### Phase 3 — Questions
Outcome: a non-empty questions file is available for upload.
- [x] Confirm committed `questions.json` is a non-empty array, OR
- [ ] `uv run dbxcarta-dense-generate-questions` completes (not needed; committed files valid)
Completion criteria: a valid non-empty questions JSON exists.

### Phase 4 — Materialize into Unity Catalog
Outcome: the synthetic schema exists in Unity Catalog on the configured
profile.
- [x] `uv run dbxcarta-dense-materialize` completes
- [x] Materialized schema verified present in the target catalog
Completion criteria: command exits 0, schema and tables visible in the
catalog.

### Phase 5 — Preset resolve and questions upload
Outcome: the preset resolves its environment and questions are uploaded.
- [x] Preset env prints (`--print-env`, exact working command recorded)
- [x] Questions upload to the configured volume path
Completion criteria: env prints without error, upload confirmed at the volume
path.

### Phase 6 — Ingest — BLOCKED
Outcome: ingest completes against the materialized schema.
- [x] Ingest entrypoint submitted (exactly once, run 972346919275274)
- [ ] Ingest run run to completion — NOT MET (TERMINATED/CANCELED, USER_CANCELED, externally cancelled at ~39 min)
- [ ] Ingest run reports success — NOT MET
Completion criteria: ingest stage finishes with no failure and produces its
expected outputs. BLOCKED: run externally cancelled before completion.

### Phase 7 — Client evaluation
Outcome: the client evaluation runs end to end and produces a run summary.
- [ ] Client entrypoint submitted and run to completion
- [ ] Run summary produced (table / volume summary as configured)
Completion criteria: client stage finishes with no failure and a run summary
is recorded.

### Phase 8 — Final verification — FAIL
Outcome: the whole chain is confirmed green.
- [ ] All phases marked Complete in the work log — NOT MET (Phase 6 Blocked)
- [x] Final pass/fail verdict written with evidence — verdict: **FAIL**
Completion criteria: every prior phase is Complete, or the exact failing
phase and error are documented. Phase 6 (Ingest) is Blocked; the exact
command and full error are documented in the Work Log. Final verdict: FAIL.

## Completion Criteria (Overall)

The pipeline verification is a PASS when Phases 1 through 7 are each marked
Complete with validation evidence and Phase 8 records a green verdict. It is a
FAIL (with a clear, actionable record) if any phase cannot complete; the work
log must then name the failing phase, the exact command, and the error.

## Work Log

Keep this log current. One entry per status change. Newest entries at the
bottom. Each entry: timestamp, phase, status (Pending / In progress /
Complete / Blocked), command run, and result or error.

| Time | Phase | Status | Command | Result / Notes |
|------|-------|--------|---------|----------------|
| (init) | All | Pending | — | Plan created; execution agent not yet started. |
| 2026-05-16 (start) | 1 | In progress | inspect env config | Note: README references `examples/dense-schema/dbxcarta-overlay.env` per CLAUDE.md layering, but no overlay file exists. dense-schema dir has its own `.env` (DENSE_TABLE_COUNT=1000, DENSE_SCHEMA_NAME=dense_1000, catalog=schemapile_lakehouse). Proceeding with dense-schema/.env config. |
| 2026-05-16 22:02 | 1 | Complete | `uv sync` ; `uv pip install -e examples/dense-schema/` ; `uv run dbxcarta --help` | All exit 0. dense-schema installed (v0.1.0). Env config resolved from `examples/dense-schema/.env` (catalog=schemapile_lakehouse, volume schemapile_volume, warehouse a2946a63e3a3643d, DENSE_TABLE_COUNT=1000/dense_1000). |
| 2026-05-16 22:02 | 1 | DOC BUG | `uv run dbxcarta --help` vs README | CLI/README drift (partial): top-level `dbxcarta --help` lists only {upload,download,submit,validate,logs,clean,catalog,schema,volume} and does NOT advertise `preset` or `submit-entrypoint`. However both subcommands ARE implemented via sys.argv interception in `dbxcarta.spark.cli:main` and work exactly as the README documents (`uv run dbxcarta preset --help` and `submit-entrypoint --help` resolve). Documentation bug: hidden subcommands missing from `--help`; functionally the README commands are correct. |
| 2026-05-16 22:03 | 2 | Complete | `cd examples/dense-schema && uv run dbxcarta-dense-generate --tables 500` | Exit 0. Output: `[dense] wrote 500 tables (904 FK edges) to .cache/candidates_500.json`. Artifact `.cache/candidates_500.json` (2.3M) present, valid JSON dict with keys [format_version, source_slice, selection_params, schemas]. |
| 2026-05-16 22:04 | 3 | Complete | validate questions JSON via orjson | `questions.json` = non-empty list, 59 items. `questions_1000.json` = non-empty list, 505 items (this is the .env-selected DENSE_QUESTIONS_FILE used by the live pipeline). Both have expected keys [question_id, question, reference_sql, schema, source_id, shape]. Question generation not required (committed files valid per plan assumption). |
| 2026-05-16 22:03 | 2 | CONFIG NOTE | inspect config.py / .env | `examples/dense-schema/.env` is committed for the 1000-table config: DENSE_TABLE_COUNT=1000, DENSE_SCHEMA_NAME=dense_1000, DBXCARTA_SCHEMAS=dense_1000, DENSE_QUESTIONS_FILE=questions_1000.json. `materialize.py`/`config.py` derive candidate_cache from DENSE_TABLE_COUNT, so the live pipeline (Phases 4-7) uses `.cache/candidates_1000.json` and schema `dense_1000` (pre-existing 4.6M artifact reused per the no-duplicate-objects rule). The README's `--tables 500` generation step still ran clean and satisfies Phase 2; the 500 artifact is not what the .env-driven materialize consumes. |
| 2026-05-16 22:05 | 4 | In progress | `cd examples/dense-schema && uv run dbxcarta-dense-materialize` | Auth verified (profile azure-rk-knight, user ryan.knight@neo4j.com). Candidate cache `.cache/candidates_1000.json` (4.6M) present. Running materialize of 1000-table dense_1000 schema into catalog schemapile_lakehouse via warehouse a2946a63e3a3643d (background). |
| 2026-05-16 22:11 | 4 | Complete | `cd examples/dense-schema && uv run dbxcarta-dense-materialize` | Exit 0. Output: `[dense] materialized tables=1000 rows=9990 skipped=0 type_fallbacks=0`. Catalog verified via warehouse a2946a63e3a3643d: `schemapile_lakehouse.information_schema.schemata` shows schema `dense_1000` present (1 row); `information_schema.tables` WHERE table_schema='dense_1000' = 1000 tables; spot-check `schemapile_lakehouse.dense_1000.hr_employees` = 10 rows. Schema and tables visible in target Unity Catalog. |
| 2026-05-16 22:12 | 5 | DOC BUG | `uv run dbxcarta preset dbxcarta_dense_schema_example:preset --print-env` (exact README command, from repo root) | Command resolves and exits 0, but `DBXCARTA_SCHEMAS=` is EMPTY. Root cause: `dbxcarta preset` only loads the base root `.env` plus an explicitly-selected overlay; no `examples/dense-schema/dbxcarta-overlay.env` exists (only finance-genie and schemapile have overlays). The dense config (DBXCARTA_SCHEMAS=dense_1000, DBXCARTA_CLIENT_QUESTIONS, DENSE_QUESTIONS_FILE) lives ONLY in `examples/dense-schema/.env`, which is the example's standalone config, not a CLI overlay. README/CLI drift: the documented `dbxcarta preset ... --print-env` / `--upload-questions` commands run but produce an empty schema list (and upload would fail to find the source) unless the dense `.env` is supplied as the env-file overlay. |
| 2026-05-16 22:12 | 5 | Complete | `uv run dbxcarta preset dbxcarta_dense_schema_example:preset --print-env --env-file examples/dense-schema/.env` (from repo root) | Exit 0. Preset env printed correctly: DBXCARTA_CATALOG=schemapile_lakehouse, DBXCARTA_SCHEMAS=dense_1000, DBXCARTA_CLIENT_QUESTIONS=/Volumes/schemapile_lakehouse/_meta/schemapile_volume/dbxcarta/dense_questions.json, plus full embedding/sample/arms overlay. This is the real working `--print-env` command (README command + `--env-file examples/dense-schema/.env`). |
| 2026-05-16 22:12 | 5 | Complete | `cd examples/dense-schema && uv run dbxcarta preset dbxcarta_dense_schema_example:preset --upload-questions --env-file .env` | Exit 0 (`dbxcarta: active env overlay: .env`). Source `questions_1000.json` (299.5K, from DENSE_QUESTIONS_FILE; relative path requires running from the dense-schema dir). Upload verified via volume listing: `/Volumes/schemapile_lakehouse/_meta/schemapile_volume/dbxcarta/dense_questions.json` present, file_size=306726 bytes, last_modified=1778991121000 (2026-05-16 ~22:12 UTC, fresh). This is the real working `--upload-questions` command. |
| 2026-05-16 22:16 | 6 | DOC BUG | `uv run dbxcarta submit-entrypoint --help` vs README | CLI/README drift (same family as Phase 5): README's `uv run dbxcarta submit-entrypoint ingest` does NOT advertise/accept env selection in `--help` (only `{ingest,client}`, `--compute`, `--no-wait`). However `submit-entrypoint` resolves env via `select_overlay_path()` which scans `sys.argv` for `--env-file` before argparse, and there is no `examples/dense-schema/dbxcarta-overlay.env`. Working invocation requires appending `--env-file examples/dense-schema/.env` so DBXCARTA_SCHEMAS=dense_1000 and the dense config reach the submitted job. Also: ingest uses the Neo4j Spark Connector which is blocked on serverless; dense `.env` provides DATABRICKS_CLUSTER_ID=0515-141455-wb8qxgo2 (classic) so `--compute cluster` is implied/used. |
| 2026-05-16 22:16 | 6 | DOC BUG | `uv run dbxcarta submit-entrypoint ingest --env-file examples/dense-schema/.env --compute cluster` (from repo root) | FAILED to parse: `dbxcarta submit-entrypoint: error: unrecognized arguments: --env-file examples/dense-schema/.env`. Root cause: unlike `preset`/`verify` (which call `_load_env`→`resolve_env_files` to STRIP `--env-file` from argv), `submit-entrypoint` only calls the pure `select_overlay_path()` which reads but does NOT strip `--env-file`, so its argparse rejects the flag. CLI/README drift: the README's bare `uv run dbxcarta submit-entrypoint ingest` cannot take `--env-file`; the supported overlay-selection mechanism for submit-entrypoint is the `DBXCARTA_ENV_FILE` environment variable (consumed by `_select_overlay` for the banner and by the runner's `from_env_file` for the submitted job). |
| 2026-05-16 22:17 | 6 | In progress | `DBXCARTA_ENV_FILE=examples/dense-schema/.env uv run dbxcarta submit-entrypoint ingest --compute cluster` (from repo root) | Real working invocation. Submitting ingest wheel entrypoint to classic cluster 0515-141455-wb8qxgo2; writes dense_1000 schema graph into Neo4j. Human approved Neo4j write. Running in background (canonical run, PID 41686), monitoring. |
| 2026-05-16 22:21 | 6 | Note | launch hygiene | A second relaunch was started before confirming the first was alive; detected two concurrent ingest submissions. Killed the duplicate (TaskStop bloetvw4d + kill of its process tree). Single canonical ingest run (started 22:17) preserved and continues. No pipeline source modified. |
| 2026-05-16 22:30 | 6 | Reset | human reset Neo4j; both prior ingest runs CANCELED | Human reset the Neo4j database (graph clean) and re-approved the Neo4j write. Verified via `manage_job_runs list active_only=true`: prior concurrent runs 224671539465633 and 573408380447221 both TERMINATED/CANCELED by user. No ingest active. Starting fresh single submission. |
| 2026-05-16 22:33 | 6 | In progress | `DBXCARTA_ENV_FILE=examples/dense-schema/.env uv run dbxcarta submit-entrypoint ingest --compute cluster --no-wait` (from repo root) | Submitted EXACTLY ONCE. Preflight: cluster 0515-141455-wb8qxgo2 RUNNING, neo4j connector [ok]. Run ID 972346919275274 (job 608393747307679). Verified via `manage_job_runs list active_only=true`: EXACTLY ONE run "dbxcarta: ingest" in RUNNING state (972346919275274); all other ingest runs TERMINATED. Monitoring single run to completion by polling run_id state. No resubmission. |
| 2026-05-16 23:12 | 6 | Blocked | (same single submission: `DBXCARTA_ENV_FILE=examples/dense-schema/.env uv run dbxcarta submit-entrypoint ingest --compute cluster --no-wait`) — terminal state of run 972346919275274 | BLOCKED. The single canonical ingest run 972346919275274 (job 608393747307679) ran RUNNING from 22:33 to 23:12 (~39 min, cluster 0515-141455-wb8qxgo2 healthy/RUNNING the entire time, no error state, log surface showed bootstrap smoke_check ok) and then TERMINATED with result_state=**CANCELED**. Full terminal detail (`databricks jobs get-run 972346919275274`): `life_cycle_state=TERMINATED, result_state=CANCELED, state_message="Run cancelled by user", user_cancelled_or_timedout=True, termination_details={"code":"USER_CANCELED","message":"Run cancelled by user","type":"SUCCESS"}`. start_time=1778992384087 end_time=1778994742291. The cancellation was EXTERNAL — the verification agent issued NO cancel/kill against this run and made exactly ONE ingest submission (no duplicates; pre-submit `manage_job_runs list active_only=true` confirmed zero active ingest runs, post-submit confirmed exactly one RUNNING). Three independent watchers (monitor ban70bcra, extended monitor bw1fqd3qm, background waiter bjdvvikdo) all independently reported the identical USER_CANCELED terminal. Phase 6 success criterion (single ingest run reaches TERMINATED/SUCCESS) NOT met. Per verification rules: marking Blocked, STOPPING, not resubmitting ingest, not proceeding to Phase 7/8. Neo4j graph-count verification not performed (precondition — a successful ingest — was not met). No pipeline source modified. |
| 2026-05-16 23:12 | 7 | Not started | — | Phase 7 (client evaluation) NOT started. Precondition is a successful Phase 6 ingest with a populated Neo4j graph; Phase 6 is Blocked, so per the stop rule Phase 7 was not attempted. |
| 2026-05-16 23:12 | 8 | FAIL | final verdict | **OVERALL VERDICT: FAIL.** Phases 1–5 Complete with evidence (recorded above). Phase 6 (Ingest) BLOCKED: the single, exactly-once ingest submission (run_id 972346919275274) was externally cancelled (USER_CANCELED, "Run cancelled by user", user_cancelled_or_timedout=True) at ~39 min RUNNING, before producing any ingest output; it never reached SUCCESS. No second submission was ever made; no duplicate runs; the agent issued no cancel. Phase 7 not started (Phase-6 precondition unmet). Per Completion Criteria, any phase that cannot complete makes the run a FAIL with an actionable record. Actionable next step for a human: re-run the same single invocation `DBXCARTA_ENV_FILE=examples/dense-schema/.env uv run dbxcarta submit-entrypoint ingest --compute cluster` and ensure the run is NOT cancelled externally; if it self-cancels, inspect cluster 0515-141455-wb8qxgo2 and the Neo4j Spark Connector path. No pipeline source modified (verification only). |
