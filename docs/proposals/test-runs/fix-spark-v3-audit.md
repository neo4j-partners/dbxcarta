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
