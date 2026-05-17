# fix-spark-v3 Test Audit Log

Append-only. Newest entries at the bottom. Never edit or delete prior entries.

Entry format:

## <UTC timestamp> — <stage> — <short title>
- Command: `<exact command>`
- Exit: <code>
- Artifact: <path to captured stdout/stderr, or "n/a">
- Result: <one line: pass / fail / observation>
- Notes: <hypothesis, follow-up, or "none">


## 2026-05-16T23:11:10Z — prereq — environment setup
- Command: `uv sync && uv pip install -e examples/integration/{finance-genie,dense-schema}/`
- Exit: 0
- Artifact: n/a
- Result: pass — both example packages installed editable; uv sync clean
- Notes: DBXCARTA_REL_WRITE_PARTITIONS=2 added to repo-root .env (Phase 3 parallel write under test). .env preconfigured for finance-genie (Stage A).

## 2026-05-16T23:11:50Z — stageA — finance-genie readiness
- Command: `uv run dbxcarta preset dbxcarta_finance_genie_example:preset --check-ready --strict-optional`
- Exit: 0
- Artifact: docs/proposals/test-runs/fg-readiness.txt
- Result: pass — status: ready; 8 expected tables present; required + optional ready
- Notes: scope = graph-enriched-finance-silver,graph-enriched-finance-gold.graph-enriched-schema

## 2026-05-16T23:12:53Z — preA — full suite green
- Command: `uv run pytest -q` (background agent a63d5bc4)
- Exit: 0
- Artifact: docs/proposals/test-runs/pytest-preA.txt
- Result: pass — 444 passed, 1 skipped, 6 deselected in 31.93s (above ~412 baseline; FK subset still running)
- Notes: none

## 2026-05-16T23:12:53Z — stageA — questions + artifacts uploaded
- Command: `dbxcarta preset ... --upload-questions; dbxcarta upload --wheel; dbxcarta upload --all`
- Exit: 0 / 0 / 0
- Artifact: docs/proposals/test-runs/fg-upload-questions.txt, fg-upload-wheel.txt, fg-upload-all.txt
- Result: pass — questions.json uploaded; wheel + bootstrap + all artifacts uploaded clean
- Notes: done pre-gate (non-FK, harmless); submit-entrypoint still gated on FK subset green

## 2026-05-16T23:13:41Z — preA — FK subset green (GATE PASSED)
- Command: `uv run pytest tests/spark/fk_* tests/spark/test_rel_partition.py tests/spark/settings -q`
- Exit: 0
- Artifact: docs/proposals/test-runs/pytest-preA-fk.txt
- Result: pass — 114 passed, 3 deselected in 28.40s. preA gate fully green; Stage A submission unblocked.
- Notes: full suite 444 passed + FK subset 114 passed. Proceeding to Stage A ingest.

## 2026-05-16T23:19:31Z — stageA — INGEST FAILED (verify gate)
- Command: `uv run dbxcarta submit-entrypoint ingest` (DBXCARTA_REL_WRITE_PARTITIONS=2)
- Exit: 1 (Databricks RunLifeCycleState.INTERNAL_ERROR)
- Artifact: docs/proposals/test-runs/fg-ingest.txt, fg-ingest-logs.txt
- Result: FAIL — job ran to the load+verify step then raised RuntimeError: verify gate failed, 5 violation(s).
- FK compute (fix-spark-v3 core) SUCCEEDED: no OOM, no driver-collect error.
    summary: schemas=3 tables=12 columns=95 fk_declared=0 fk_skipped=0
    fk_inferred_metadata_accepted=2 fk_inferred_semantic_accepted=11 REFERENCES(summary)=13
- Verify violations (Neo4j vs run summary):
    Schema 1 vs 3 | Table 5 vs 12 | Column 24 vs 95 | Value 25 vs 51 | REFERENCES 0 vs 13
- Hypotheses (NOT yet acted on, pending user decision):
    H1 (verify-scope/config): .env is a 3-catalog medallion overlay
       (DBXCARTA_CATALOGS=bronze,silver,gold); summary counts all 3 catalogs
       while verify is scoped to DBXCARTA_CATALOG=...-silver only -> node
       undercount is a scope artifact, not write loss. Does NOT fully explain
       REFERENCES=0.
    H2 (Phase 3 parallel write): DBXCARTA_REL_WRITE_PARTITIONS=2 -> repartition(2)
       concurrent Neo4j writes; fix-spark-v3 Phase 3/Risks explicitly warn
       parallel rel writes raise Neo4j lock contention. REFERENCES=0 + partial
       HAS_* node-via-MERGE counts are consistent with dropped/contended writes.
- Decision: STOP Stage A per plan (hard failure). Surface to user before any change/re-run.

## 2026-05-17T00:41:54Z — stageA — isolate parallel-write (user answered: 'Isolate parallel-write')
- Command: edit .env DBXCARTA_REL_WRITE_PARTITIONS 2 -> 1; then re-submit ingest (next entry)
- Exit: n/a (config change)
- Artifact: n/a
- Result: user selected option 'Isolate parallel-write' via AskUserQuestion; .env set to n=1
- Notes: verify-scope confound (3-catalog medallion vs single-catalog verify) still present; key signal is REFERENCES count under n=1

## 2026-05-17T00:47:11Z — stageA — transient: neo4j connector preflight pending
- Command: `uv run dbxcarta submit-entrypoint ingest` (REL_WRITE_PARTITIONS=1)
- Exit: 2 (preflight failed before job body)
- Artifact: docs/proposals/test-runs/fg-ingest-p1.txt
- Result: TRANSIENT INFRA — cluster just started; maven org.neo4j:neo4j-connector-apache-spark_2.13:5.3.10 still 'pending'. Not a fix-spark-v3 or partitions issue (n=2 run earlier installed it and ran full Neo4j writes).
- Notes: retry after the maven library finishes installing on the cluster.

## 2026-05-17T00:52:36Z — stageA — ISOLATION RESULT: H2 refuted, H1 confirmed
- Command: `uv run dbxcarta submit-entrypoint ingest` (REL_WRITE_PARTITIONS=1) + `dbxcarta logs`
- Exit: 1 (verify gate raised; same as n=2)
- Artifact: docs/proposals/test-runs/fg-ingest-p1.txt, fg-ingest-p1-logs.txt
- Result: n=1 violations are BYTE-IDENTICAL to n=2: Schema 1v3, Table 5v12, Column 24v95, Value 25v51, REFERENCES 0v13. Deterministic.
- Conclusion:
    H2 (Phase 3 parallel write / lock contention) = REFUTED. Partition count does not affect the outcome at all.
    H1 (verify single-catalog scope vs 3-catalog medallion .env) = CONFIRMED as the cause of the gate failure.
    fix-spark-v3 FK path is HEALTHY: '[REDACTED]/REFERENCES: 13' confirms 13 edges written;
    fk_inferred_metadata_accepted=2, fk_inferred_semantic_accepted=11, no OOM, no driver collect, fk_skipped=0.
- The verify gate failure is a config/scope artifact of the medallion .env, NOT a fix-spark-v3 defect and NOT a parallel-write defect.
- Decision: report definitive isolation result to user; next approach (single-catalog re-run vs verify warn-only) is a user call.

## 2026-05-17T00:59:32Z — root-cause — user approved part-1 verify multi-catalog fix
- Command: n/a (decision)
- Exit: n/a
- Artifact: n/a
- Result: user selected 'Implement part-1 fix now' via AskUserQuestion
- Scope: make verify count invariants (graph node counts, references edge count, values count)
    scope to ALL settings.resolved_catalogs() prefixes instead of only the primary dbxcarta_catalog.
    Add unit tests. Re-run Stage A after. Part-2 (information_schema per-catalog sampling) stays deferred.
- Note: this is a code change OUTSIDE the fix-spark-v3 proposal; user explicitly authorized.

## 2026-05-17T01:04:43Z — root-cause — part-1 verify multi-catalog fix IMPLEMENTED
- Command: edit verify/{__init__,graph,references,values}.py + run.py + cli.py; rewrite tests/spark/test_verify_scope.py
- Exit: 0
- Artifact: n/a (code)
- Result: pass — added scoped_catalogs(summary, catalogs); count invariants now scope to
    all settings.resolved_catalogs() (Database: n.id IN $catalog_ids; Schema/Table/Column/Value/REFERENCES:
    any(p IN $prefixes WHERE id STARTS WITH p)). Single-catalog callers/tests fall back unchanged.
    catalog= (information_schema part-2) left deferred per existing KNOWN LIMITATION.
- Tests: tests/spark/test_verify_scope.py 12 passed (5 new multi-catalog cases incl negative).
    Full suite: 451 passed, 1 skipped, 6 deselected (was 444; +7 new verify-scope tests; no regression).
- Notes: .env DBXCARTA_REL_WRITE_PARTITIONS restored to 2 (original user request; H2 refuted so safe).

## 2026-05-17T01:10:49Z — stageA — fixed run #1: 5 violations -> 1 (Database)
- Command: `uv run dbxcarta submit-entrypoint ingest` (n=2, part-1 verify fix)
- Exit: 1 (verify gate still True at job runtime for this run)
- Artifact: docs/proposals/test-runs/fg-ingest-fixed.txt, fg-ingest-fixed-logs.txt
- Result: MAJOR PROGRESS — Schema/Table/Column/Value/REFERENCES(13) all now verify correctly.
    Sole remaining: graph.node_count_mismatch.Database (Neo4j 3 vs summary 1).
- Root: ExtractCounts has no 'databases' field -> row_counts never carries it ->
    verify default counts.get('databases',1)=1, but build_database_nodes writes 1 per resolved catalog (3).
- Fix (part-1, expected-side): verify Database expected = len(resolved catalog_ids) (fallback to legacy default).
    tests/spark/test_verify_scope.py 12 passed; full suite 451 passed, 1 skipped (no regression).

## 2026-05-17T01:15:57Z — stageA — PASS (fixed run #2)
- Command: `dbxcarta submit-entrypoint ingest` (n=2) ; `dbxcarta verify`
- Exit: 0 / 0
- Artifact: fg-ingest-fixed2.txt, fg-ingest-fixed2-logs.txt, fg-verify.txt
- Result: PASS. status=success; in-job verify ok=True violations=0; CLI verify OK (0 violations).
- Counters: schemas=3 tables=12 columns=95 value_nodes=51 | fk_declared=0 fk_skipped=0
    fk_inferred_metadata_accepted=2 fk_inferred_semantic_accepted=11 REFERENCES=13
- Stage A criteria: success+no OOM+no collect=PASS; verify 0 violations=PASS;
    inferred REFERENCES present/plausible (13=2 meta+11 sem)=PASS; fk_skipped=0=PASS.
- fix-spark-v3 Phases 1/2/3/4 validated on real 3-catalog medallion workload with parallel rel writes (n=2).
  Run URL: job/36136777882525/run/766218515658718

## 2026-05-17T01:16:49Z — preB+stageB-prep — gate launched, dense schema generated
- Command: background agent a3d3cc3f (preB suite); `uv run dbxcarta-dense-generate --tables 500`
- Exit: gen 0 (preB pending)
- Artifact: docs/proposals/test-runs/dense-generate.txt, pytest-preB*.txt
- Result: pass (gen) — 500 tables, 904 FK edges -> .cache/candidates_500.json. preB running in background.
- Notes: single-schema 500-table workload exercises fix-spark-v3 single-schema skew risk + link_key join + scale.

## 2026-05-17T01:30:00Z — preB — gate GREEN (Stage B unblocked)
- Command: background agent a3d3cc3f862cdd134 (full suite + FK subset)
- Exit: FULL_EXIT=0 / FK_EXIT=0
- Artifact: docs/proposals/test-runs/pytest-preB*.txt
- Result: PASS — full suite 451 passed, 1 skipped, 6 deselected (matches post-fix baseline,
    +7 verify-scope tests vs original 444; no regression). FK subset 126 passed, 3 deselected.
- Notes: preB gate satisfied -> proceeding to Stage B (dense-schema) per plan.
