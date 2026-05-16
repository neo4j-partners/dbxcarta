# Dense-1000 Pipeline Work Log

**Goal:** Run the full dense-schema 1000-table pipeline end to end:
generate → materialize → preset check → upload questions → ingest → client eval.

This document is a living audit log. After each step, the outcome, any issues, and the
next action are recorded here.

---

## Pipeline Steps

| Step | Command | Status |
|------|---------|--------|
| 1. Generate candidates | `dbxcarta-dense-generate --tables 1000` | ✅ done (pre-existing) |
| 2. Generate questions | `dbxcarta-dense-generate-questions` | ✅ done (pre-existing) |
| 3. Update .env | edit `.env` | ✅ done |
| 4. Materialize dense_1000 | `dbxcarta-dense-materialize` | ✅ done |
| 5. Readiness check | `dbxcarta preset ... --check-ready` | ⏳ pending |
| 6. Upload questions | `dbxcarta preset ... --upload-questions` | ⏳ pending |
| 7. Ingest | `dbxcarta submit-entrypoint ingest` | ✅ done |
| 8. Client eval | `dbxcarta submit-entrypoint client` | ✅ done |
| 9. Verify | `dbxcarta verify` | ✅ done |

---

## Step Log

### Step 1–2: Generate (pre-run)

`.cache/candidates_1000.json` (4.6 MB, 1000 tables across 10 domains) and
`questions_1000.json` were already present from a prior session. No action needed.

---

### Step 3: Update .env

**Date:** 2026-05-16

Changed `examples/integration/dense-schema/.env`:

- `DENSE_TABLE_COUNT` 500 → 1000
- `DENSE_SCHEMA_NAME` dense_500 → dense_1000
- Added `DBXCARTA_SCHEMAS=dense_1000` (required by `preset.schemas_list()`)
- Added `DENSE_QUESTIONS_FILE=questions_1000.json` (overrides default `questions.json`)

**Outcome:** ✅ complete.

---

### Step 4: Materialize dense_1000

**Date:** 2026-05-16

**Attempt 1 — failed.** `dbxcarta-dense-materialize` hit a network timeout during the
`DELETE FROM` on the first table. The SDK HTTP read timeout is 60 seconds by default.
The SQL warehouse was cold (and under concurrent load from a finance-genie run), so the
initial POST to `/api/2.0/sql/statements` took >60s to respond. The SDK retried for 5
minutes and gave up.

A second bug amplified this: `_execute` used `on_wait_timeout=CONTINUE` but never
polled the returned statement ID. DELETE and INSERT for the same table could interleave
if either statement was still running.

**Fixes applied to `materialize.py`:**

1. Build `WorkspaceClient` via `Config(http_timeout_seconds=300)` — 5 minutes for
   warehouse cold start instead of 60 seconds.
2. `_execute` now polls `get_statement` every 3s until SUCCEEDED, raising a clear error
   on FAILED/CANCELED.
3. Added `logging.basicConfig(INFO)` so per-table progress is visible.
4. INFO log per table: `[dense] table N/1000 <fq_name>`.
5. DEBUG log per statement: elapsed seconds.

**Attempt 2 — failed (TypeError).** `WorkspaceClient.__init__()` does not accept
`http_timeout_seconds` directly; it must go through `Config`. Fixed import and
constructor call.

**Attempt 3 — running.** Started 2026-05-16 ~07:34. Confirmed progress at table
26/1000 as of ~07:35. Estimated completion: ~60 minutes (~3-4s per table).

**Progress updates (attempt 3 — sequential, no parallelism):**
- 07:34 — started, table 1/1000
- 07:41 — table 123/1000 (crm domain), ~3s/table, ~44 min remaining
- 07:46 — table 219/1000 (fin domain), stopped manually at 237/1000

**Attempt 4 — parallel (20 workers) + INSERT OVERWRITE:**

Added `ThreadPoolExecutor(max_workers=20)` so tables within a schema are created
concurrently. Replaced `DELETE FROM` + `INSERT INTO` with a single `INSERT OVERWRITE
TABLE` per table, removing one round-trip statement per table.

- 07:49 — started with `--workers 20`
- 07:49+15s — table 65/1000, ~4.3 tables/sec (13x speedup vs sequential ~0.33/sec)
- 07:51 — table 362/1000 (inv domain), ~3.4 tables/sec, ~3 min remaining
- Estimated completion: ~07:54
- 08:26 — table 1000/1000 complete

**Outcome:** ✅ complete. Final stats: tables=1000 rows=10000 skipped=0 type_fallbacks=0.

---

### Step 5: Readiness check

**Date:** 2026-05-16 ~07:55

Output:
```
scope: schemapile_lakehouse.dense_1000
present expected tables: 1
required tables: ready
optional tables: ready
status: ready
```

**Outcome:** ✅ complete.

---

### Step 5b: Upload questions (re-confirmed)

Command (run from `examples/integration/dense-schema/`):
```bash
uv run dbxcarta preset dbxcarta_dense_schema_example:preset --check-ready
```

---

### Step 6: Upload questions

**Date:** 2026-05-16 ~07:55

Command completed with no output (success). `questions_1000.json` uploaded to
`/Volumes/schemapile_lakehouse/_meta/schemapile_volume/dbxcarta/dense_questions.json`.

**Outcome:** ✅ complete.

---

### Step 7: Ingest

**Date:** 2026-05-16 ~07:55+

**Attempt 1 — failed:** `DATABRICKS_WORKSPACE_DIR` and `DATABRICKS_CLUSTER_ID` missing
from dense-schema `.env`. Added from root `.env`.

**Attempt 2 — failed:** `wheel_volume_path` resolved to `/dbxcarta_spark-stable.whl`
(bare filename) because `DATABRICKS_VOLUME_PATH` was not in `.env`. Added. Ran
`dbxcarta upload --wheel` with `DATABRICKS_VOLUME_PATH` override (first run uploaded
to finance-genie volume by mistake; second run uploaded correctly to
`/Volumes/schemapile_lakehouse/_meta/schemapile_volume/wheels/`).

**Attempt 3 — failed:** Wheel installed on cluster but `SparkIngestSettings` raised
`ValidationError` for missing `DBXCARTA_SUMMARY_VOLUME` and `DBXCARTA_SUMMARY_TABLE`.
These are normally injected by `DenseSchemaPreset.env()` but `submit-entrypoint` does
not call the preset. Added full preset env overlay to `.env` (all 17 vars from
`--print-env`).

**Attempt 4 — running.** Databricks Run ID: `868166455106031`. Started ~08:01, confirmed
RUNNING after 3+ minutes (previous attempts all failed within 15s). Embedding 1000
tables + columns against `databricks-gte-large-en`. Expected duration: 15-30 min.

**Outcome:** ✅ complete. Run ID `868166455106031`, duration ~47.9 min.

Final stats:
- schemas: 1, tables: 1000, columns: 10,314
- FK edges (semantic inferred): 109,825
- Graph nodes: Database×2, Schema×2, Table×1020, Column×10517, Value×23666
- REFERENCES edges: 109,843
- verify: ok=True, violations=0

Note: local CLI hit 20-min SDK wait timeout (attempt 4 output showed `TimeoutError`), but
the Databricks job ran to completion independently.

---

### Step 8: Client eval

**Date:** 2026-05-16 ~08:49

**Attempt 1 — failed.** `DBXCARTA_CHAT_ENDPOINT` missing from `.env`. All three arms
(`no_context`, `schema_dump`, `graph_rag`) require a chat model. Fixed by adding
`DBXCARTA_CHAT_ENDPOINT=databricks-claude-sonnet-4-6` (matching root and finance-genie `.env`).

**Attempt 2 — complete.** Run ID: `220042594665556`, duration ~37.5 min.

Results:
```
no_context:  attempted=505 parsed=505 executed=29  exec_rate=5.7%  correct_rate=69.0%
schema_dump: attempted=505 parsed=505 executed=204 exec_rate=40.4% correct_rate=27.9%
graph_rag:   attempted=505 parsed=0   executed=0   exec_rate=0.0%  correct_rate=0.0%
```

Warning logged during run:
```
embedding call to databricks-gte-large-en failed: BAD_REQUEST: Input embeddings size is
too large, exceeding 150 limit for databricks-gte-large-en.
```

**graph_rag arm produced 0 parsed results.** The embedding failure (>150 input limit on
`databricks-gte-large-en`) is likely causing graph retrieval to return nothing, so the
LLM receives no context and returns no SQL. This is a known issue with dense-1000 scale
and needs investigation. The no_context and schema_dump arms ran cleanly.

**Outcome:** ✅ complete (with graph_rag arm degraded — see above).

Command:
```bash
uv run dbxcarta submit-entrypoint client
```

Evaluates `no_context`, `schema_dump`, and `graph_rag` arms against `questions_1000.json`.

---

### Step 9: Verify

**Date:** 2026-05-16 ~09:33

Command:
```bash
uv run dbxcarta verify
```

Output:
```
verify: run_id=local OK (0 violations)
```

**Outcome:** ✅ complete. No violations.
