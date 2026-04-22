# DBxCarta Phase 3 — Review of Phase 2 and Remediation Plan

Phase 2 shipped end-to-end (`run_id 818560288946148` succeeded, test suite green). This document reviews the design and implementation, catalogues issues that surfaced on close reading, and proposes a Phase 3 scope to resolve them. No code changes are made here — each item below is a proposal for discussion before implementation.

---

## Review — what Phase 2 got right

- **Per-table batching.** One `approx_count_distinct` aggregate per table (Step 3) and one `LATERAL VIEW STACK` per table (Step 4) is the correct shape. The alternative — one query per column — would have issued thousands of Spark jobs on a realistic catalog. The implementation in `_cardinality_filter` and `_sample_values` follows this faithfully.
- **Candidate selection anchored on `Column`.** Using the `Column` node as the selective match (`WHERE col.data_type IN ['STRING', 'BOOLEAN']`) before walking to Table/Schema/Database is the right Cypher shape, even though the supporting index is not yet in place (see issue H3 below).
- **Python-side ranking.** Avoiding `ORDER BY` in the STACK query sidesteps a shuffle across the unpivoted result. The driver-side heap is safe because cardinality is bounded by the threshold.
- **Explicit Spark schemas on `createDataFrame`.** Value and HAS_VALUE frames use explicit `StructType` definitions, which prevents `LongType` from silently narrowing to `IntegerType` or degenerating to `NullType` on empty inputs. This is the subtle bug that would have broken Neo4j round-trips.
- **Cardinality distribution emitted.** The `cardinality_{min,p25,p50,p75,p95,max}` block in `row_counts` is the right telemetry shape for tuning the threshold without guessing. Keep it.
- **Preflight fails fast on missing `Column` nodes.** This is the correct way to prevent a silently empty Phase 2 run when Phase 1 has not executed.
- **Per-schema probe, not catalog-level hard fail.** Realistic shared catalogs always have a few unreadable schemas. Dropping them and recording the count is the right tradeoff.

---

## Issues

Severity definitions:

- **Critical** — incorrect data or broken invariant; fix before the next live run.
- **High** — correctness-adjacent or operational hazard; fix in Phase 3.
- **Medium** — quality / robustness; fix in Phase 3 if scope allows.
- **Low** — polish; defer unless trivial.

### C1 — `value_nodes` counter records pre-dedup length

`src/dbxcarta/sample_values.py:141` sets `summary.row_counts["value_nodes"] = len(rows)`, where `rows` is the accumulated list of `(col_id, col_name, val, cnt)` tuples *before* the per-`value_id` dedup in the node-building loop. The Phase 2 plan Step 7 explicitly specifies `"value_nodes" — total Value nodes written (post-dedup)`, and `test_value_count_matches_summary` compares `value_nodes` against the Neo4j `:Value` count.

Today the test passes because `generate_value_id` prefixes the digest with `col_id`, so identical values under different columns do not collide, and within a single column the SQL `GROUP BY col_name, val` already deduplicates. The dedup gate in the implementation (`seen_value_ids`) is a no-op in practice. The counter matches the node count by coincidence, not by construction.

The bug becomes visible the moment the dedup gate ever fires — a retry, a future chunking change, or any code path that appends the same `(col_id, val)` twice will cause `len(rows) > len(value_node_rows)`, and the test will start failing for a reason unrelated to the change. Record `len(value_node_rows)` (and `len(has_value_rows)`) directly after the build loop.

### H1 — Stale `Value` nodes and `HAS_VALUE` edges are never deleted

The writer uses `mode("Overwrite")` on the Neo4j Spark connector, which the connector maps to `MERGE` on `node.keys=id` / relationship source+target keys. MERGE is additive: nothing is deleted. When the top-K for a column drifts between runs (new values enter the head of the frequency distribution, old values fall off), the old `Value` nodes remain in the graph, still connected to their `Column` by `HAS_VALUE`. Over time a column accumulates a stale long tail.

The idempotency test (`test_idempotency.py`) passes because two consecutive runs against a static table produce the same top-K. It does not cover drift. This will manifest first as a slow bloat of `:Value` node count in `_query_neo4j_counts`, then as degraded relevance in Text2SQL (stale literals surface in WHERE-clause suggestions).

Two viable fixes:

1. **Scoped delete before write.** Before writing the new `HAS_VALUE` edges for this run, issue one Cypher per run: `MATCH (c:Column)-[r:HAS_VALUE]->(v:Value) WHERE c.id IN $resampled_col_ids DETACH DELETE v`. `resampled_col_ids` is the list of columns that survived the cardinality filter (i.e. the columns this run will replace). Columns not resampled (all above threshold, or dropped schemas) are left alone.
2. **Full wipe per run.** `MATCH (v:Value) DETACH DELETE v` before the write. Simpler, but wipes columns that the current run is not touching. Only acceptable if every Phase 2 run covers the full candidate set.

Option 1 is the correct shape. It composes with the per-schema probe: a schema skipped in this run retains its previous Values, which is what you want.

### H2 — Neo4j driver is instantiated four times per run

`_read_candidates`, `_preflight` (for the Column-count check), `_bootstrap_constraints`, and `_query_neo4j_counts` each open their own `GraphDatabase.driver(...)`. Each instantiation is a TCP connect + bolt handshake + basic auth. On Aura this is on the order of 200–500 ms each; not catastrophic, but needlessly four times the auth surface and four places for a transient connection error to abort the job.

Fix: instantiate one `driver` in `_run` after the `Neo4jConfig` is built, pass it (not the config) to helpers that need a session. Close in a `try/finally`. This also gives a single place to tune pool settings if the write volume ever grows.

### H3 — Candidate Cypher relies on a non-existent index

The plan (Phase 2, Step 2) states that the `Column`-first anchor "lets the planner use the `Column.data_type` index when one exists." Phase 1's `_bootstrap_constraints` creates unique id constraints on `Database/Schema/Table/Column`, but no index on `Column.data_type`. On a catalog with tens of thousands of `Column` nodes the current query does a full `:Column` scan to apply the `data_type IN [...]` filter.

Fix: add `CREATE INDEX column_data_type IF NOT EXISTS FOR (n:Column) ON (n.data_type)` to Phase 1's constraint bootstrap (or to the Phase 2 bootstrap if ownership is preferred there). Cheap, idempotent, pays back every Phase 2 run.

### M1 — Per-schema probe drops the schema if the *first* table fails

`_filter_readable_schemas` uses `cands[0]` as the probe. If that one table is the anomaly — quarantined, being rewritten, temporarily restricted — the entire schema is dropped even though every other table is readable. The observed cost is silent coverage loss, visible only as a lower `candidate_columns` number and a `skipped_schemas` increment.

Fix: probe up to K tables per schema (K=3 is enough) before declaring the schema unreadable. Or move the probe from schema-granularity to table-granularity — one `SELECT 1 FROM <table> LIMIT 1` per table, recorded as `skipped_tables` instead of `skipped_schemas`. Table-granularity is more work per run but is the accurate shape.

### M2 — Whole-table drop on any chunk failure

Both `_cardinality_filter` and `_sample_values` wrap the *entire* per-table loop in `try/except`. A single failing chunk on a 200-column table discards the other three chunks' results. On wide tables this is the wrong granularity.

Fix: move the `try/except` inside the chunk loop. Log `table + chunk index + exception`; on failure, skip that chunk and continue.

### M3 — `HAS_VALUE` edge count missing from `row_counts`

The run summary records `value_nodes` but not the HAS_VALUE edge count. `test_relationship_integrity.py` covers edge presence from the Neo4j side, but an operational dashboard reading only the Delta summary row cannot distinguish "10 columns each with 5 values" (50 edges) from "50 columns each with 1 value" (also 50 edges, but very different catalog state).

Fix: emit `row_counts["has_value_edges"] = len(has_value_rows)` alongside `value_nodes`.

### M4 — Full table scan on every sample query

`_sample_query` scans the full table for every top-K computation. For large fact tables (billions of rows) this dominates the job wall-clock, even though the top values are typically stable across any reasonable sample. The plan treats this as acceptable, which it is for Phase 2, but Phase 3 should consider:

- `TABLESAMPLE (N PERCENT)` on the inner FROM, or
- `FROM (SELECT * FROM t TABLESAMPLE (1000 ROWS)) s` when row count is bounded.

This is an explicit tradeoff: sampling trades completeness of the frequency estimate for wall-clock. Validate against a live run before committing.

### M5 — Empty strings are not filtered

`WHERE val IS NOT NULL` filters NULLs but not `''`. Empty-string values flow into `:Value` nodes. For Text2SQL substitution, `WHERE status = ''` is almost never a useful suggestion; it pollutes the dropdown and competes with real literals.

Fix: tighten the predicate to `WHERE val IS NOT NULL AND val <> ''`. Record the decision alongside the existing NULL rationale in the Design Decisions block.

### M6 — Cardinality observations include table failures

`_record_cardinality_stats` computes percentiles over the `all_cards` list. A table that raised in the cardinality pre-pass contributes nothing to the list, so the percentile distribution is biased toward tables that read cleanly. This is probably fine — percentile tuning is a soft signal — but worth noting in the summary. Consider a `row_counts["cardinality_failed_tables"]` counter so the distribution has a disclaimer.

### L1 — Column names with single quotes break the STACK query

`_sample_query` builds `'{c}', CAST(\`{c}\` AS STRING)` where `{c}` is the raw column name. If a column name contains a single quote, the literal breaks. Spark identifiers can legally contain quotes when backtick-quoted, though this is vanishingly rare. Fix with `c.replace("'", "''")` in the literal position. Low priority; document the constraint in the meantime.

### L2 — `summary.finish(error=str(exc))` loses the traceback

Only the exception message is captured. On a Databricks Job, the driver log has the traceback, but the summary Delta row (the primary audit trail) does not. Use `traceback.format_exc()` or a truncated variant. Small, durable improvement.

### L3 — `dbxcarta_sample_limit` is not validated positive

A misconfigured `DBXCARTA_SAMPLE_LIMIT=0` would succeed with zero Value nodes and no warning. Add a `field_validator` asserting `> 0`.

---

## Proposed Phase 3 scope

The following order minimises risk: each step is independently shippable and does not require the next step to land.

### Step 1 — Fix `value_nodes` counter (C1)

One-line fix in `sample_values.py`. Move the assignment below the node-build loop:

```python
summary.row_counts["value_nodes"] = len(value_node_rows)
summary.row_counts["has_value_edges"] = len(has_value_rows)
```

No test change required; `test_value_count_matches_summary` keeps passing (and now passes for the right reason). Also addresses M3.

### Step 2 — Add `Column.data_type` index (H3)

Extend Phase 1's `_bootstrap_constraints` with:

```cypher
CREATE INDEX column_data_type IF NOT EXISTS FOR (n:Column) ON (n.data_type)
```

Idempotent. Runs once per Phase 1 execution. Measurable win on the next Phase 2 candidate read against any catalog with more than a few thousand columns.

### Step 3 — Share one Neo4j driver per run (H2)

Refactor `_run` to open one `GraphDatabase.driver(...)` after building `Neo4jConfig`, pass the driver to `_read_candidates`, `_preflight`, `_bootstrap_constraints`, `_query_neo4j_counts`. Close in a `try/finally`. Touch points are local; the `Neo4jConfig` dataclass stays as the secret carrier.

### Step 4 — Scoped delete of stale Value nodes (H1)

Before writing the HAS_VALUE frame, execute one Cypher:

```cypher
MATCH (c:Column)-[:HAS_VALUE]->(v:Value)
WHERE c.id IN $col_ids
DETACH DELETE v
```

**`col_ids` = every `column_id` in the post-schema-probe `candidates`** — every column whose table this run actually read, *before* the cardinality filter. This covers three drift cases that all need stale Values dropped:

1. **Top-K shifted** — column still under threshold; new write replaces, but the delete is what removes the values that fell off the head.
2. **Column went all-NULL/all-empty** — column still under threshold, but the sample query returns nothing; only the delete keeps the graph in sync.
3. **Column rose above the cardinality threshold** — column was under threshold last run (so it has Values), now above (so it is filtered out of `sampled_candidates`). Scoping on `sampled_candidates` would leave its prior Values orphaned forever. This is the case the narrower scope misses.

Columns from schemas dropped by the permission probe are not in `candidates` and are therefore left alone, consistent with the "best-effort coverage" contract.

Concretely:

```python
resampled_col_ids = [col_id for cand in candidates for col_id in cand.column_ids]
with driver.session() as s:
    s.run(
        "MATCH (c:Column)-[:HAS_VALUE]->(v:Value) "
        "WHERE c.id IN $ids DETACH DELETE v",
        ids=resampled_col_ids,
    )
```

Do this *after* the cardinality filter but *before* the node/edge write, so a mid-run crash does not leave the graph empty — it leaves it in the previous run's state.

**Drift test.** Seeding plan: use an ephemeral Delta table in the target catalog (`<catalog>.dbxcarta_test.drift_<uuid>`) rather than a fixture-managed resource. Pattern:

1. `CREATE TABLE ... AS VALUES` with a known set of STRING values.
2. Submit Phase 2 (reuse `Runner.submit()`, same as `test_idempotency`).
3. `INSERT OVERWRITE` the table with a disjoint value set.
4. Submit Phase 2 again.
5. Assert `:Value` nodes attached to this column's `col_id` equal the new set's cardinality, not the union.
6. `DROP TABLE` in a `finally` block.

Mark `@pytest.mark.slow`. The marker is already used by `test_idempotency.py` but is **not registered** in `pyproject.toml` — pytest will warn. Register it while we're here:

```toml
[tool.pytest.ini_options]
markers = ["slow: tests that submit a Databricks job and wait for it"]
```

### Step 5 — Per-chunk `try/except` in sampling and cardinality (M2)

Mechanical refactor of the inner loops in `_cardinality_filter` and `_sample_values`. Exception becomes `table + chunk_index + columns`. The table-level `try/except` stays as a belt-and-suspenders catch for errors that escape chunk scope (e.g. the `createDataFrame` step for that table).

### Step 6 — Probe up to K=3 tables before dropping a schema (M1)

Replace `probe = cands[0]` with a loop over up to three candidates. Drop the schema only when *every* probe raises. `skipped_schemas` keeps its current meaning (count of schemas where all K probes failed).

Rationale for K=3 over full per-table probing: K=3 catches the "one quarantined table in an otherwise-readable schema" failure mode — which is what motivated this issue — at constant probe cost per schema. Per-table probing is strictly more accurate but multiplies probe count by table fanout. Revisit if live telemetry shows schemas where table-level permission varies widely.

### Step 7 — Tighten NULL/empty filter (M5)

Two-character SQL change. Update the Design Decisions note in the plan. Low-risk.

### Step 8 — Traceback and validator polish (L2, L3)

`traceback.format_exc()` at the `except Exception as exc` boundary in `run_sample`. Field validator on `dbxcarta_sample_limit`. Small.

### Step 9 — Instrument wall-clock, defer `TABLESAMPLE` (M4)

Do not introduce `TABLESAMPLE` without a baseline. Phase 3 adds instrumentation only:

- `row_counts["sample_wall_clock_ms"]` — total wall-clock across all `_sample_values` chunks.
- `row_counts["cardinality_wall_clock_ms"]` — same for `_cardinality_filter`.

Measured with a monotonic `time.perf_counter_ns()` bracket around each phase. The split matters: if the cardinality pre-pass is already the dominant cost, `TABLESAMPLE` on the sample query is treating the wrong problem. Decide after one or two live runs against `graph-enriched-lakehouse` whether sampling pays for itself.

### Step 10 — Escape single quotes in STACK column literals (L1)

Two-character fix while the file is open:

```python
stack_expr = ", ".join(
    f"'{c.replace(chr(39), chr(39)*2)}', CAST(`{c}` AS STRING)"
    for c in column_names
)
```

(Using `chr(39)` avoids an f-string quoting knot.) No test change — this is defensive for a column shape the current catalog happens not to contain. Kept in the same PR rather than deferred because the cost is trivial and the failure mode is silent.

---

## Explicit non-goals for Phase 3

- **Numeric/date value sampling.** The STRING/BOOLEAN predicate is load-bearing for the Text2SQL substitution contract. Expanding to numerics requires a separate design conversation around range-vs-literal semantics.
- **Value embeddings.** Lives downstream in the MCP server, not in the Phase 2 job.
- **FK extraction** (Phase 1's pending W8). Track separately.

---

## Resolved decisions

| # | Question | Decision | Rationale |
|---|----------|----------|-----------|
| 1 | H1 delete scope | **All post-schema-probe candidates** (every column read this run, pre-cardinality-filter) | Three drift cases must drop stale Values: top-K shifted, column went all-NULL/all-empty, *and* column rose above the cardinality threshold this run. The third case is filtered out before `sampled_candidates`, so scoping there would leave its prior Values orphaned forever. |
| 2 | H3 index location | **Phase 1** | Phase 1 owns the `:Column` label definition; the `data_type` index belongs with it. Phase 2 remains a pure consumer of the schema graph. |
| 3 | M1 probe granularity | **K=3 per schema** | Catches the motivating failure mode (one quarantined table) at constant probe cost. Revisit only if telemetry shows heterogeneous table-level permissions. |
| 4 | M4 strategy | **Defer `TABLESAMPLE`, instrument only** | Add `sample_wall_clock_ms` + `cardinality_wall_clock_ms` to `row_counts`. Decide after baseline measurement. |
| 5 | Implementation staging | **Two PRs** (see below) | Correctness/operational fixes land first, quality items second. Each PR is independently live-submittable. |
| 6 | L1 single-quote fix | **Include the two-line fix** | Trivial cost, silent failure mode. No reason to leave it as a documented landmine. |
| 7 | Step 4 drift test seeding | **Ephemeral `CREATE TABLE AS VALUES` in a test schema**, dropped in `finally` | Self-contained, no fixture plumbing. `@pytest.mark.slow` to be registered in `pyproject.toml` (currently unregistered — pytest warns). |

## Staging

**PR 1 — Correctness & operations (C1, H1, H2, H3).** Fixes the counter bug, stops stale-Value accumulation, consolidates the Neo4j driver, adds the `Column.data_type` index. The `has_value_edges` counter (M3) rides along since it is a one-liner adjacent to C1. Ships with the drift test. This PR is the one that meaningfully changes graph behavior; worth landing and live-running before the rest.

**PR 2 — Quality & robustness (M1, M2, M5, M6, L1, L2, L3, and wall-clock instrumentation from Step 9).** Granular error handling, probe K=3, empty-string filter, column-name escaping, traceback capture, sample-limit validator, wall-clock timers. Each is independently small; bundled for a single test + submit cycle.

## Closeout checklist

After PR 1 lands and is live-run verified:

- `row_counts["value_nodes"]` equals Neo4j `:Value` count (test already asserts this — now passes for the right reason).
- Second live run with changed source data: `:Value` total does not grow by the delta; columns that were resampled show only the new top-K.
- `_read_candidates` Cypher `EXPLAIN` shows an index seek, not a label scan (confirms H3).
- Log line counts show exactly one Neo4j driver instantiation per run.

After PR 2 lands:

- `skipped_schemas` distribution in `row_counts` shifts toward zero on catalogs where one quarantined table per schema was previously tripping the probe.
- `sample_wall_clock_ms` + `cardinality_wall_clock_ms` appear in the summary; use the split to decide M4.
- `@pytest.mark.slow` no longer produces a `PytestUnknownMarkWarning`.
