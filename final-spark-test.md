# Test Plan: Validate fix-spark-v3 FK Rewrite

## Goal

Validate the rewritten Spark-native FK path (`docs/proposals/fix-spark-v3.md`,
Phases 0 through 4, all marked Complete) end to end against two real
Databricks workloads, in order:

1. `examples/integration/finance-genie/` — small correctness smoke. Five base
   tables, no declared foreign keys, semantic inference enabled. Exercises the
   semantic FK path, the asymmetric value-overlap divisor, cache hygiene, and
   the configurable relationship write.
2. `examples/integration/dense-schema/` — single-schema scale test. Hundreds
   of tables in one schema. Exercises the single-schema skew risk called out
   in fix-spark-v3 (the `link_key` join must keep output bounded by real name
   matches, not schema size), bounded driver memory, and the
   `dbxcarta_fk_max_columns` guardrail.

Correctness is judged against fix-spark-v3 semantics, not byte-for-byte parity
with the old Python implementation.

## Ground rules

- The dbxcarta unit/integration test suite is the gate. Databricks job
  submissions cost money and time, so the suite must be green before any
  `submit-entrypoint` run. The suite runs in a **separate background agent**
  (see "Background test agent").
- Keep a single append-only audit log for the entire effort. Never overwrite
  it. Every command, its exit status, and a one-line result go in the log.
- Capture raw output to files. The audit log references those files by path;
  it does not inline large logs.
- Stop on first hard failure in a stage. Record the failure, the captured
  artifact path, and a hypothesis in the audit log before deciding whether to
  continue.

## Audit log: exact instructions

Create the log once, at the start, and only ever append:

```bash
mkdir -p docs/proposals/test-runs
LOG=docs/proposals/test-runs/fix-spark-v3-audit.md
test -f "$LOG" || cat > "$LOG" <<'EOF'
# fix-spark-v3 Test Audit Log

Append-only. Newest entries at the bottom. Never edit or delete prior entries.

Entry format:

## <UTC timestamp> — <stage> — <short title>
- Command: `<exact command>`
- Exit: <code>
- Artifact: <path to captured stdout/stderr, or "n/a">
- Result: <one line: pass / fail / observation>
- Notes: <hypothesis, follow-up, or "none">

EOF
```

Append one entry per meaningful action with a real UTC timestamp:

```bash
{
  echo ""
  echo "## $(date -u +%Y-%m-%dT%H:%M:%SZ) — <stage> — <title>"
  echo "- Command: \`<cmd>\`"
  echo "- Exit: <code>"
  echo "- Artifact: <path>"
  echo "- Result: <one line>"
  echo "- Notes: <hypothesis or none>"
} >> "$LOG"
```

Rules for the log:

- One entry per command that changes state or produces a verdict (setup,
  pytest, each `submit-entrypoint`, each `verify`, each summary query).
- Record the exit code verbatim. Never round a failure up to a pass.
- If a step is skipped, write an entry saying it was skipped and why.
- Captured output goes under `docs/proposals/test-runs/` with a descriptive
  name (for example `fg-ingest-stdout.txt`, `dense-verify.json`). The entry's
  `Artifact:` line points at it.

## Result and error capture

For every Databricks submission and verify, tee raw output to a file and
record the Databricks run id:

```bash
ART=docs/proposals/test-runs
uv run dbxcarta submit-entrypoint ingest 2>&1 | tee "$ART/<stage>-ingest.txt"
uv run dbxcarta logs 2>&1 | tee "$ART/<stage>-ingest-logs.txt"
uv run dbxcarta verify 2>&1 | tee "$ART/<stage>-verify.txt"
```

Pull the structured run summary so FK counts are captured as data, not prose.
The summary is written to the table named by `DBXCARTA_SUMMARY_TABLE`. Query
the latest successful row and save the FK-relevant fields:

- `status` (must be `success`)
- `row_counts` map, specifically the `fk_metadata*`, `fk_semantic*`, and
  `fk_skipped*` keys (these are the Phase 1/2/4 coarse counters and the
  guardrail skip record)
- `references` / declared vs inferred edge counts

Save that row as JSON to `$ART/<stage>-summary.json` and reference it in the
audit log. If `status` is not `success`, capture the `error` field and treat
the stage as failed.

## Prerequisites (run once, before Stage A)

```bash
uv sync
uv pip install -e examples/integration/finance-genie/
uv pip install -e examples/integration/dense-schema/
```

Confirm the Databricks profile and Neo4j secrets are in place:

```bash
uv run dbxcarta preset dbxcarta_finance_genie_example:preset --print-env
./setup_secrets.sh --profile azure-rk-knight
```

Log the prerequisite block as one audit entry (pass/fail on `uv sync` and the
two installs).

## Background test agent (full suite, monitorable)

Before Stage A and again before Stage B, run the **entire** test suite in a
dedicated background agent so it does not block the foreground Databricks work
and can be checked on at any time.

Launch it with the Agent tool, `subagent_type: general-purpose`,
`run_in_background: true`, `description: "Run full dbxcarta test suite"`, with
this prompt:

> Run the full dbxcarta test suite from the repo root:
> `uv run pytest -q 2>&1 | tee docs/proposals/test-runs/pytest-<STAGE>.txt`.
> Then run the Spark FK subset explicitly so its result is unambiguous:
> `uv run pytest tests/spark/fk_metadata tests/spark/fk_semantic
> tests/spark/fk_declared tests/spark/fk_discovery tests/spark/fk_guard
> tests/spark/fk_common tests/spark/test_rel_partition.py
> tests/spark/settings -q 2>&1 | tee
> docs/proposals/test-runs/pytest-<STAGE>-fk.txt`. Do not fix anything. Report
> back: total passed/failed/skipped for each command, the exit codes, the
> names of any failing tests with their assertion messages, and the two
> artifact paths. If anything fails, include the first failing traceback
> verbatim.

Replace `<STAGE>` with `preA` (before Stage A) and `preB` (before Stage B).

Monitor it without blocking: the agent runs detached and notifies on
completion. To check status mid-run, read the tee target
(`docs/proposals/test-runs/pytest-preA.txt`) directly, or send the agent a
follow-up via SendMessage asking for current progress. Do not start any
`submit-entrypoint` run until the agent reports the suite green. Record the
agent's final report as one audit entry, with both artifact paths and the
pass/fail/skip totals.

The expected baseline from fix-spark-v3 is "412 passed, 1 skipped, 3
deselected" (post Phase 1/2) and "168 passed" for `tests/spark` (post Phase
3/4). A drop below those, or any new failure in the FK subset, blocks the
Databricks stages.

## Stage A: finance-genie (correctness smoke)

Goal: prove the rewritten semantic and metadata FK path produces sensible
inferred `REFERENCES` edges on a real catalog with no declared FKs, and that
cache hygiene and the configurable write did not regress.

Steps (log every step):

1. Gate: background test agent reports `preA` green. If not, stop.
2. Configure dbxcarta `.env` from the preset:
   `uv run dbxcarta preset dbxcarta_finance_genie_example:preset --print-env`
   and copy values into `dbxcarta/.env` (semantic inference on, all embedding
   labels on, as the preset defaults).
3. Readiness:
   `uv run dbxcarta preset dbxcarta_finance_genie_example:preset --check-ready --strict-optional`.
   If base tables are missing, stop and record the gap.
4. Upload questions:
   `uv run dbxcarta preset dbxcarta_finance_genie_example:preset --upload-questions`.
5. Build artifacts: `uv run dbxcarta upload --wheel` then
   `uv run dbxcarta upload --all`.
6. Ingest: `uv run dbxcarta submit-entrypoint ingest`, captured per "Result
   and error capture". This is the run that exercises the new FK path.
7. Verify: `uv run dbxcarta verify`, captured.
8. Pull the run summary JSON and record the `fk_metadata*` / `fk_semantic*` /
   `fk_skipped*` counters.

Stage A pass criteria (record each as a line in the findings):

- Ingest `status == success`; no driver-side OOM or `collect`-related error in
  the captured logs (Phase 1/2: no catalog-scale driver collect).
- `verify` returns zero violations, or only violations unrelated to FK edges
  (note any FK-related violation as a failure).
- Inferred `REFERENCES` edges exist and are plausible for Finance Genie
  (account / merchant / transaction linkage recovered from names, comments,
  embeddings). Zero inferred edges on this schema is a failure of the semantic
  path, not a pass.
- `fk_skipped` is false / absent (guardrail disabled by default, catalog is
  tiny).
- No generic `id`→`id` inferred edge (Phase 2 follow-up hardening suppresses
  these).

## Stage B: dense-schema (single-schema scale + guardrail)

Goal: prove the `link_key` join keeps the single-schema cartesian from
collapsing onto one executor (the explicit Risk in fix-spark-v3), that driver
memory stays bounded at hundreds of tables, and that the
`dbxcarta_fk_max_columns` guardrail skips cleanly when tripped.

Steps (log every step):

1. Gate: launch the background test agent for `preB` and wait for green
   (re-run because Stage A may have produced local changes; a clean baseline
   before the scale run is required).
2. Generate the synthetic schema at the default scale:
   `uv run dbxcarta-dense-generate --tables 500`.
3. Configure `.env` from
   `uv run dbxcarta preset dbxcarta_dense_schema_example:preset --print-env`,
   then materialize:
   `uv run dbxcarta-dense-materialize`.
4. Upload questions:
   `uv run dbxcarta preset dbxcarta_dense_schema_example:preset --upload-questions`.
5. Rebuild and upload artifacts (`upload --wheel`, `upload --all`).
6. **Run B1 — guardrail disabled (scale path under load):** ensure
   `dbxcarta_fk_max_columns` is unset/0 in `.env`. Submit ingest, capture,
   verify, pull summary. This is the run that must not OOM the driver and must
   not collapse to one executor.
7. **Run B2 — guardrail tripped:** set `dbxcarta_fk_max_columns` in `.env` to
   a value below this catalog's column count (read the column count from B1's
   summary `extract` counters). Submit ingest, capture, verify, pull summary.
   FK discovery must skip entirely while extract and load still complete.

Stage B pass criteria:

- B1 ingest `status == success`. No driver OOM. No
  catalog-scale-collect error.
- B1 captured Spark logs show the FK join did not degenerate to a single
  partition on one executor for the single schema (inspect stage/partition
  metrics in the run logs; the link_key join output should be bounded by name
  matches, not by `n²` of the schema). Record the observed behavior.
- B1 inferred edge count is non-trivial but bounded (not the full Cartesian).
  Record the count and the rough table/column totals from the summary.
- B2 summary shows `fk_skipped == true` with the limit and reason recorded;
  extract and load counters are still populated (load did not skip). This is
  the Phase 4 skip-not-cap contract.
- B2 `verify` does not report missing-FK as a hard failure beyond what the
  intentional skip implies.

Optional stretch (only if B1 passes and time allows): regenerate at
`--tables 1000` and re-run B1 to push closer to the fix-spark-v3 10k-table
intent. Log it as a separate stage; not a gate.

## Findings summary

At the end, append one final audit-log section titled
`## Findings Summary — fix-spark-v3 validation`. It must contain, in plain
prose backed by the captured artifacts:

- Background suite result for `preA` and `preB` (totals + verdict + artifact
  paths).
- Stage A: per-criterion pass/fail, the FK counter values from the summary
  JSON, and the verify verdict.
- Stage B: B1 and B2 per-criterion pass/fail, observed partition/skew
  behavior, edge counts vs table/column totals, and the guardrail skip record.
- Overall verdict: does the fix-spark-v3 rewrite hold up on real workloads at
  this scale, yes or no, with the specific evidence for any "no".
- Any follow-up items, each linked to the artifact that motivates it.

State failures plainly with their captured output. Do not describe a stage as
passing if any pass criterion was unmet or unverified.

## Sequencing

1. Prerequisites (once).
2. Background test agent `preA` → must be green.
3. Stage A (finance-genie) end to end. Stop on hard failure.
4. Background test agent `preB` → must be green.
5. Stage B (dense-schema) B1 then B2. Stop on hard failure in B1.
6. Optional 1000-table stretch.
7. Findings Summary appended to the audit log.
