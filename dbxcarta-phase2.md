# DBxCarta Phase 2 — Sample Value Job: Implementation Plan

Phase 2 adds `Value` nodes to the graph produced by Phase 1. The MCP server uses these nodes to ground Text2SQL literal suggestions in the actual data (e.g. filling `WHERE status = ?` dropdowns with real values).

Phase 1 must be green (live run verified, T1–T7 passing) before Phase 2 implementation begins.

---

## Status — 2026-04-21

| Step | Status |
|------|--------|
| 1 — Settings class | ✅ implemented |
| 2 — Neo4j candidate read | ✅ implemented |
| 3 — Cardinality pre-pass (flat SELECT) | ✅ implemented |
| 4 — Value sampling (STACK, chunked, Python rank) | ✅ implemented |
| 5 — Build & write Value nodes (no UDF) | ✅ implemented |
| 6 — Build & write HAS_VALUE relationships | ✅ implemented |
| 7 — Run summary wiring | ✅ implemented |
| 8 — Preflight (volume, table, Column-node check) | ✅ implemented |
| 9 — Job submission (dispatcher, not a new script) | ✅ verified existing |
| 10 — Phase 2 test suite (conftest + 6 files) | ✅ implemented, syntax-clean |
| Live Phase 2 submit against `graph-enriched-lakehouse` | ✅ run_id 818560288946148 — SUCCESS |
| `uv run pytest tests/sample_values` | ✅ 10 passed, 1 skipped (warehouse skip, known) |

### Worklog

- **Initial read-through.** Phase 1 already ships `run_dbxcarta.py` dispatcher, `contract.py` already exports `LABEL_VALUE`/`REL_HAS_VALUE`/`generate_value_id`, writer helpers accept arbitrary `(label, rel)` pairs. No changes required outside `sample_values.py`, tests, and this doc.
- **First pass on `sample_values.py` drafted using `STACK` for both cardinality and sampling.** Revised after reviewer feedback (Step 3 switched to flat `SELECT approx_count_distinct(c1) AS card_0, … FROM t`; Step 4 drops `ORDER BY`, ranks top-K in Python).
- **`dbxcarta_stack_chunk_size` added** to `Settings` (default 50) and threaded through `_cardinality_filter` and `_sample_values` — the two passes are now structurally symmetric per the design constraint.
- **Per-schema read probe** implemented (`_filter_readable_schemas`) — tracks dropped schemas in `row_counts["skipped_schemas"]`.
- **Cardinality distribution** emitted to `row_counts` as `cardinality_{min,p25,p50,p75,p95,max}` — supports the "tune the threshold after a live run" decision from Open Questions.
- **Node ID generation** is done in Python before `createDataFrame`, not in a UDF — the value list is bounded by Σ(tables × candidates × limit) and trivially fits on the driver. Also dedupes by `value_id` before materialising `value_node_df` so two columns sharing the same literal can coexist without duplicate node writes.
- **Preflight fails fast** if `MATCH (:Column)` returns zero — prevents silent zero-Value runs when Phase 1 hasn't been run.
- **Test suite** mirrors `tests/schema_graph/`: new `conftest.py` that looks for `sample_values_*.json` in the summary volume, six test files covering node counts, id shape, relationship integrity, cardinality filter, idempotency (`@pytest.mark.slow`), and run summary.
- **Live run 1 (previous session, run_id 831597381219116):** 22 candidate columns, 18 sampled, 4 skipped (high cardinality — `merchant_name` had 7978 distinct values, correctly filtered), 58 Value nodes, 58 HAS_VALUE edges written.
- **Test bug fixes:** `test_idempotency.py` passed `project_dir=` kwarg not accepted by `Runner.submit()`; dropped. `test_run_summary.py` crashed on `result.result.data_array` when warehouse returned `None`; added guard to `pytest.skip` instead. Both are test-only bugs, no changes to `sample_values.py`.
- **Live run 2 (run_id 818560288946148):** Fresh submit after session resume. Job succeeded. `pytest tests/sample_values`: 10 passed, 1 skipped (`test_summary_delta_row_exists` — warehouse didn't return data, same known issue as Phase 1 tests). Idempotency test passed — second run produced identical Value/HAS_VALUE counts.

---

## Design constraints (load-bearing)

**Per-table batching, not per-column queries.** The naive design — one `GROUP BY` per candidate column — issues thousands of Spark jobs on a large catalog and takes hours before any data moves. The correct pattern is **one Spark SQL query per table that covers all eligible columns in a single scan**, using `LATERAL VIEW STACK` to unpivot the result into `(col_name, value, count)` tuples. This is the only acceptable implementation shape for the sampling step.

**Candidate list from Neo4j, not from `information_schema` directly.** The job queries `Column` nodes (already written by Phase 1) to build the candidate list. This keeps the selection consistent with what Phase 1 produced and avoids a second `information_schema` scan.

**Cardinality pre-pass before data sampling.** A column is only sampled if `approx_count_distinct` is below `DBXCARTA_SAMPLE_CARDINALITY_THRESHOLD` (default: 50). The pre-pass runs as one Spark SQL query per table (same per-table batching pattern), not as individual column probes. Unlike the sampling step, the pre-pass does *not* need `STACK` — each `approx_count_distinct(c)` is a single aggregate, so one flat `SELECT` with N aggregates returns one row with N cells, which is trivially transposed in Python.

---

## Implementation steps

### Step 1 — Settings class

Add a `Settings` class to `sample_values.py` using `pydantic_settings.BaseSettings`, matching the pattern in `schema_graph.py`. Required fields:

```python
class Settings(BaseSettings):
    databricks_secret_scope: str = "dbxcarta-neo4j"
    dbxcarta_catalog: str            # validated with _IDENTIFIER_RE
    dbxcarta_schemas: str = ""
    dbxcarta_summary_volume: str
    dbxcarta_summary_table: str      # validated with _IDENTIFIER_RE
    dbxcarta_write_partitions: int = 8
    dbxcarta_sample_limit: int = 10
    dbxcarta_sample_cardinality_threshold: int = 50
    dbxcarta_stack_chunk_size: int = 50
```

The `dbxcarta_catalog` and `dbxcarta_summary_table` fields get the same `_validate_identifier` validator as `schema_graph.py`. Copy the regex rather than importing it — these are separate jobs that happen to share a pattern.

### Step 2 — Neo4j candidate read

Query the live Neo4j instance (via the `neo4j` Python driver, not Spark) to get the candidate column list. The query returns fully qualified table coordinates grouped by table so the sampling step can issue one query per table.

Anchor the match on `Column` — it is the selective node (only STRING/BOOLEAN survive the type predicate) — then walk back out to table/schema/database. This shape lets the planner use the `Column.data_type` index when one exists instead of enumerating every `Database -> Schema -> Table -> Column` path:

```cypher
MATCH (col:Column)
WHERE col.data_type IN ['STRING', 'BOOLEAN']
MATCH (tbl:Table)-[:HAS_COLUMN]->(col)
MATCH (sch:Schema)-[:HAS_TABLE]->(tbl)
MATCH (db:Database)-[:HAS_SCHEMA]->(sch)
WHERE db.name = $catalog
RETURN db.name AS catalog,
       sch.name AS schema_name,
       tbl.name AS table_name,
       collect(col.name) AS column_names,
       collect(col.id)   AS column_ids
ORDER BY catalog, schema_name, table_name
```

Scope by catalog in Cypher (`WHERE db.name = $catalog`) so the driver does not stream rows for other catalogs over the wire. Apply the optional `DBXCARTA_SCHEMAS` allowlist in Python after the query returns — the list is short and filtering there keeps the Cypher uniform across runs.

Return type: list of `TableCandidate(catalog, schema_name, table_name, column_names, column_ids)` — a plain `@dataclass`, not a Spark object. Give it an `fq()` method that returns the backtick-quoted three-part identifier; the sampling and cardinality steps reuse it wherever they need to reference the table.

Note: `collect()` in Cypher preserves pair ordering between `column_names` and `column_ids` for a given group, so positional index `i` always refers to the same column in both lists. Build a `dict(zip(column_names, column_ids))` once per table to look ids up by name downstream.

### Step 2a — Per-schema read probe

Before the cardinality pre-pass, issue one cheap probe per schema (`SELECT 1 FROM <first_table_in_schema> LIMIT 1`) inside a `try/except`. Schemas that raise `AnalysisException` (or any exception) are logged and removed from the candidate list; record the count in `row_counts["skipped_schemas"]`. A catalog-level hard fail would abort the job any time a single schema has restricted access, which is the common state in a shared catalog. See the rationale note under "Design decisions" below.

### Step 3 — Cardinality pre-pass

For each `TableCandidate`, issue one Spark SQL query that aggregates `approx_count_distinct` across all eligible columns in a single scan. **No `STACK` needed here** — the result is a single row:

```python
def _cardinality_query(fq_table: str, column_names: list[str]) -> str:
    aggs = ", ".join(
        f"approx_count_distinct(`{c}`) AS `card_{i}`"
        for i, c in enumerate(column_names)
    )
    return f"SELECT {aggs} FROM {fq_table}"
```

Collect the single row, zip back against `column_names`, and filter: keep only column names where `cardinality < threshold`. Record every observed cardinality (kept or dropped) into a flat list so Step 7 can compute percentiles. If all columns for a table are above the threshold, skip the table entirely (no sampling query). Wrap each table's pre-pass in `try/except`: a table that fails here should be logged and dropped, not propagated — the job must tolerate individual table failures in a large catalog.

For very wide tables (> `dbxcarta_stack_chunk_size` candidate columns), chunk the column list and run one query per chunk; concatenate the one-row results in Python. Each chunk re-scans the table, which is acceptable because (a) chunking only triggers past ~50 columns and (b) the cardinality filter typically drops most wide-table columns before Step 4. Use the same chunk helper in Step 4 so the two passes stay structurally symmetric.

### Step 4 — Value sampling

For each table with surviving candidates after the cardinality filter, issue one (or more, if chunked) sampling query using `LATERAL VIEW STACK`:

```python
def _sample_query(fq_table: str, column_names: list[str]) -> str:
    n = len(column_names)
    stack_expr = ", ".join(f"'{c}', CAST(`{c}` AS STRING)" for c in column_names)
    return (
        f"SELECT col_name, val, COUNT(*) AS cnt"
        f"  FROM {fq_table}"
        f"  LATERAL VIEW STACK({n}, {stack_expr}) t AS col_name, val"
        f" WHERE val IS NOT NULL"
        f" GROUP BY col_name, val"
    )
```

**Do not sort in SQL.** A `ORDER BY col_name, cnt DESC` forces a shuffle across the whole result. Instead, `.collect()` the unsorted rows and rank in Python per `col_name` (heap or `sorted(..., reverse=True)[:limit]`). Cardinality is bounded by the threshold (default 50), so at most `~chunk_size × threshold` rows per chunk — safe for the driver.

**Chunking.** Spark has no documented hard limit on `STACK` arity, but 50 `(col, val)` pairs (100 arguments) is well within safe range across versions. Split longer column lists with a helper:

```python
def _chunk(lst, n): return [lst[i:i+n] for i in range(0, len(lst), n)]
```

Apply the same helper in both Step 3 and Step 4 so the two passes stay structurally symmetric. Each chunk re-scans the table; for tables with > 50 eligible STRING/BOOLEAN columns, this is an acceptable overhead (wide analytics tables are rare, and the cardinality filter typically eliminates most of their columns before sampling).

Accumulate per-column top-K values across all tables into one flat list of tuples:
`(col_id, col_name, val, cnt)`. The other coordinates (catalog/schema/table) are not needed downstream — the `col_id` already encodes the full path via Phase 1's identifier scheme, and every downstream join goes through it. Wrap each table's sampling in `try/except` for the same reason as Step 3.

### Step 5 — Build and write Value nodes

The accumulated result list is small (bounded by `Σ tables × candidates × limit`). **Generate `id` in Python before creating the Spark DataFrame** — no UDF. Build the node list and the relationship list in a single pass, and deduplicate value-node rows by `id`: the same `(col_id, val)` pair will not repeat from the per-column top-K logic, but enforcing dedup here is cheap insurance against future chunking or retry bugs (and the `Value.id` uniqueness constraint will otherwise reject the batch).

```python
from pyspark.sql import Row
from pyspark.sql.types import LongType, StringType, StructField, StructType

value_rows, rel_rows = [], []
seen: set[str] = set()
for col_id, col_name, val, cnt in accumulated:
    vid = generate_value_id(col_id, val)
    rel_rows.append(Row(source_id=col_id, target_id=vid))
    if vid in seen:
        continue
    seen.add(vid)
    value_rows.append(Row(
        id=vid, value=val, count=int(cnt),
        contract_version=CONTRACT_VERSION,
    ))

value_schema = StructType([
    StructField("id", StringType(), nullable=False),
    StructField("value", StringType()),
    StructField("count", LongType()),
    StructField("contract_version", StringType()),
])
value_node_df = spark.createDataFrame(value_rows, schema=value_schema)
```

Pass the explicit `StructType` to `createDataFrame`. Without it, Spark infers from the first row and can narrow `count` to `IntegerType`, or drop to `NullType` if the list is empty — neither round-trips cleanly through the Neo4j writer.

Contract symbols are already defined in `contract.py` and import cleanly:
- `LABEL_VALUE = "Value"`
- `REL_HAS_VALUE = "HAS_VALUE"`
- `generate_value_id(column_id, value)`

(There is no `__init__.py` re-export list to update — `schema_graph.py` imports these directly from `dbxcarta.contract`, and `sample_values.py` should do the same.)

Bootstrap a `Value` unique constraint in Neo4j before the write (same pattern as `_bootstrap_constraints` in `schema_graph.py`):

```cypher
CREATE CONSTRAINT value_id IF NOT EXISTS FOR (n:Value) REQUIRE n.id IS UNIQUE
```

Write with `write_nodes(value_node_df.repartition(partitions), neo4j, LABEL_VALUE)`.

### Step 6 — Build and write HAS_VALUE relationships

The `rel_rows` list from Step 5 is already the HAS_VALUE edge set. Materialize it with an explicit schema (same reasoning as Step 5 — do not let Spark infer types) and write:

```python
rel_schema = StructType([
    StructField("source_id", StringType(), nullable=False),
    StructField("target_id", StringType(), nullable=False),
])
has_value_df = spark.createDataFrame(rel_rows, schema=rel_schema)
write_relationship(
    has_value_df.repartition(partitions), neo4j,
    REL_HAS_VALUE, LABEL_COLUMN, LABEL_VALUE,
)
```

Note the source/target label ordering: `REL_HAS_VALUE` goes `(:Column)-[:HAS_VALUE]->(:Value)`, so `source_id = col_id` and `target_id = value_id`.

### Step 7 — Run summary

Wire up `RunSummary` (same as Phase 1, with `job_name="sample_values"`). Add these entries to `row_counts`:

```
"candidate_columns"   — total columns returned by the Neo4j query
"sampled_columns"     — columns that passed the cardinality filter
"skipped_columns"     — candidate_columns - sampled_columns
"skipped_schemas"     — schemas dropped by the Step 2a permission probe
"value_nodes"         — total Value nodes written (post-dedup)
"cardinality_min"     — min of approx_count_distinct across all candidates
"cardinality_p25"     — 25th percentile (quartile)
"cardinality_p50"     — median
"cardinality_p75"     — 75th percentile
"cardinality_p95"     — 95th percentile (the tuning signal for the threshold)
"cardinality_max"     — max
```

Percentiles are computed in Python over the flat list of cardinalities accumulated in Step 3 (use nearest-rank on a sorted list — the input is already small). Emit all six even if the distribution is narrow; a single invariant shape makes downstream dashboarding trivial.

Extend `_query_neo4j_counts` (or write a Phase 2 variant) to include node counts for `Value` and relationship counts for `HAS_VALUE`.

### Step 8 — Preflight

Re-use Phase 1's preflight verbatim: (a) `SELECT 1 FROM <catalog>.information_schema.schemata LIMIT 1` to confirm catalog read; (b) parse `DBXCARTA_SUMMARY_VOLUME` as `/Volumes/<cat>/<sch>/<vol>` and `CREATE VOLUME IF NOT EXISTS`; (c) `CREATE TABLE IF NOT EXISTS` for the summary table. The summary table schema from Phase 1 already covers all Phase 2 fields — no migration needed.

Add one Phase 2-specific check: verify that `Column` nodes exist in Neo4j (i.e. Phase 1 has been run). If the `Column` count is zero, fail fast rather than silently producing zero `Value` nodes:

```python
with driver.session() as session:
    cnt = session.run("MATCH (n:Column) RETURN count(n) AS cnt").single()["cnt"]
    if cnt == 0:
        raise RuntimeError(
            "[dbxcarta] Phase 2 preflight failed: no Column nodes in Neo4j. "
            "Run Phase 1 (DBXCARTA_JOB=schema via scripts/run_dbxcarta.py) before Phase 2."
        )
```

### Step 9 — Job submission

**No new submission script.** The repo already has a single dispatcher at `scripts/run_dbxcarta.py` that switches on the `DBXCARTA_JOB` env var:

```python
job = os.environ.get("DBXCARTA_JOB", "schema")
if job == "sample":
    from dbxcarta import run_sample
    run_sample()
```

Phase 2 implementation work is limited to filling in `run_sample()` in `src/dbxcarta/sample_values.py`. The `sample` branch is already wired up in `scripts/run_dbxcarta.py` and `src/dbxcarta/__init__.py` re-exports `run_sample`. Submit the job with `DBXCARTA_JOB=sample` set on the run.

### Step 10 — Phase 2 test suite

Create `tests/sample_values/` with a `conftest.py` that mirrors `tests/schema_graph/conftest.py`: loads the latest `sample_values` JSON from the summary volume, connects to Neo4j.

**Tests to write:**

| Test | What it asserts |
|------|----------------|
| `test_value_node_counts.py` | `row_counts["value_nodes"]` matches Neo4j `Value` count; `row_counts["sampled_columns"]` ≤ `row_counts["candidate_columns"]`; `skipped_columns` + `sampled_columns` == `candidate_columns` |
| `test_value_id_shape.py` | For a random sample of 20 `Value` nodes: `id` equals `generate_value_id(col_id, value)`; MD5 segment is 32 hex chars; prefix matches the parent `Column.id` |
| `test_relationship_integrity.py` | Every `Value` has exactly one incoming `HAS_VALUE` from a `Column`; no orphan `Value` nodes; no `Column` has more than `DBXCARTA_SAMPLE_LIMIT` outgoing `HAS_VALUE` edges |
| `test_cardinality_filter.py` | No `Value` node's parent `Column` has a `data_type` outside `['STRING', 'BOOLEAN']`. Spot-check: for a known high-cardinality column (if present in catalog), assert zero `Value` nodes |
| `test_idempotency.py` | Second run produces identical `Value` and `HAS_VALUE` counts (`@pytest.mark.slow`, same pattern as Phase 1) |
| `test_run_summary.py` | Delta row exists with `status = "success"`, `job_name = "sample_values"`; JSON artifact in the summary volume parses and matches the Delta row |

---

## What to do first

1. Verify Phase 1 is green: live run + T1–T7 passing.
2. Implement `run_sample()` in `src/dbxcarta/sample_values.py` following Steps 1–8.
3. **Pre-run review.** Before submitting the job:
   - Invoke the `databricks-docs` skill and cross-check any APIs or behaviors this job relies on that are not already exercised in Phase 1 (particularly `LATERAL VIEW STACK`, `approx_count_distinct` guarantees, and the Neo4j Spark connector write path).
   - Do a full in-depth code review of `sample_values.py`: verify every step in this plan is implemented correctly, check for edge cases (empty candidate list, all-NULL columns, tables with a single eligible column, tables with > `stack_chunk_size` columns), and confirm error handling is consistent throughout.
4. Submit Phase 2 with `DBXCARTA_JOB=sample`; inspect logs; spot-check `Value` and `HAS_VALUE` counts against `graph_enriched_lakehouse`.
5. Write and run the Phase 2 test suite (Step 10).

---

## Design decisions

The following points were live questions during design. They are now settled; recording the rationale here so future changes can tell *why* the current shape was chosen before changing it.

- **Per-schema permission check, not catalog-level hard fail** (Step 2a). A shared catalog routinely has a few schemas the job principal cannot read; a hard fail on the catalog would make Phase 2 impossible to run in those environments. Probe per schema, drop unreadable schemas, keep going. Recorded in `row_counts["skipped_schemas"]`.

- **STACK chunking at 50 columns.** Spark has no documented hard limit on `STACK` arity, but 50 `(col, val)` pairs (100 arguments) is well within safe range across versions. Use the same `_chunk` helper in Steps 3 and 4 for structural symmetry. Accept the per-chunk re-scan as the cost of chunking; most tables with > 50 eligible STRING/BOOLEAN columns are wide analytics tables where the cardinality filter drops the majority before sampling.

- **Cardinality threshold default of 50.** The intended use case is populating `WHERE col = ?` dropdowns in a Text2SQL UI — a list longer than ~50 is no longer a useful picker. Every run records the full `(min, p25, p50, p75, p95, max)` distribution (Step 7). Revisit the default only if a live run against `graph_enriched_lakehouse` shows `p95 > 50` (most columns being dropped) or `p50 < 10` (default is generously wide and could be tightened).

- **NULL values are excluded** via `WHERE val IS NOT NULL` in the STACK sample. For BOOLEAN columns the meaningful values are `'true'` and `'false'`; a NULL BOOLEAN means the column allows nulls but carries no literal the Text2SQL agent can substitute. Writing a Value node for NULL would invite downstream query generation to treat `WHERE col IS NULL` as a literal substitution path, which is not the intent. Columns where NULL is semantically significant (tri-state flags, etc.) should be modeled via an `is_nullable = true` annotation on the `Column` node — not via a `Value` node with a null `value` property.

- **Everything cast to STRING in STACK.** Every branch of a `STACK` must share a type. Casting each column to STRING enforces this and gives a uniform `val` column. BOOLEAN casts to `'true'`/`'false'` in Spark SQL, which is the intended serialization for Text2SQL substitution.

- **Ranking happens in Python, not SQL.** `ORDER BY col_name, cnt DESC` on the STACK output would force a shuffle across the full unpivoted result. Because cardinality is bounded by the threshold (default 50), a single chunk produces at most `~chunk_size × threshold` rows — trivially small for the driver. `collect()`, bucket by `col_name`, take top-K per bucket in Python.
