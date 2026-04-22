# fixmefast — Pipeline Design Review

Issues are ordered by severity: correctness first, scalability second, maintainability third.

Best-practice citations refer to `docs/best-practices.md` by section heading.

---

## 1. Stale Value nodes survive when `INCLUDE_VALUES` is toggled off (correctness)

**Where:** `pipeline.py:458`

`_purge_stale_values` is only called inside `if settings.dbxcarta_include_values`. If run N wrote Value nodes and run N+1 sets `DBXCARTA_INCLUDE_VALUES=false`, the purge never runs. Value nodes from run N remain in the graph forever, attached to columns that the current run does not claim to own. Nothing in the post-run query (`_query_neo4j_counts`) or the summary will reveal this; the graph silently drifts.

This violates **Project-level principle §1** ("Unity Catalog is the only source of truth"): if UC has no qualifying values for a column, the graph must not either.

**Fix:** Always call `_purge_stale_values` for columns that are in scope, regardless of whether values are being re-sampled this run. The guard `if settings.dbxcarta_include_values` should only gate the re-write, not the purge.

**Approach (decided):** Derive `candidate_col_ids` directly from the already-cached `columns_df` (filter to STRING/BOOLEAN data types, compute IDs via `generate_id` from `contract.py`). This avoids re-running schema probing when values are disabled and adds no latency because `columns_df` is already cached from the extract phase. The pre-probe scope is slightly broader than the post-probe scope used when sampling is enabled, but that is correct and safe — stale Values on columns whose schemas became unreadable should also be purged. Move `_purge_stale_values` to always run (before the `if INCLUDE_VALUES` block), passing these IDs. Add a public `get_candidate_col_ids(columns_df)` helper to `sample_values.py` to keep the ID-derivation logic in one place.

---

## 2. Preflight accepts a volume path that will fail `_resolve_staging_path` (correctness)

**Where:** `pipeline.py:565` vs `pipeline.py:511`

The two path parsers use different strategies and different minimum-depth checks:

- `_preflight` does `volume_path.lstrip("/").split("/")` and checks `len(parts) < 4` — accepts `/Volumes/cat/schema/vol` (bare volume root).
- `_resolve_staging_path` does `summary_root.split("/")` (no lstrip) and checks `len(parts) < 6` — rejects that same path.

A user who sets `DBXCARTA_SUMMARY_VOLUME=/Volumes/cat/schema/vol` will pass preflight, then hit a `RuntimeError` from `_resolve_staging_path` at the top of `_run()` before any useful work happens.

This violates **Project-level principle §3** ("Fail fast on missing permissions; never degrade silently"): the preflight should surface the misconfiguration, not `_resolve_staging_path`.

**Fix:** Consolidate the path validation into a single shared function called by both `_preflight` and `_resolve_staging_path`, enforcing the stricter depth requirement in both places.

---

## 3. Preflight cannot detect a dimension mismatch on the embedding endpoint (correctness)

**Where:** `pipeline.py:613`

```python
spark.sql(
    f"SELECT ai_query('{endpoint}', 'preflight', failOnError => false)"
).collect()
```

Per **Spark/Databricks §2** ("Use `failOnError => false` for production batches"), the query completes successfully even if the endpoint returns an error struct — which means the preflight passes even for a completely non-functional endpoint. More importantly, it does not inspect the returned vector's length, so a dimension mismatch between `DBXCARTA_EMBEDDING_DIMENSION` and what the model actually returns goes undetected until the full dataset has been processed and materialized to Delta.

**Fix:** Parse the preflight response. Check that `result` is non-null and that `len(result) == settings.dbxcarta_embedding_dimension`. Abort immediately if either condition fails. This turns an expensive late failure into a cheap early one, consistent with **Project-level principle §3**.

---

## 4. `coalesce(1)` on every write is a hard throughput ceiling (scalability)

**Where:** `pipeline.py:431, 442, 448, 455, 464, 466, 470, 473, 476, 481`

Every node and relationship write is forced through a single Spark partition. The justification for relationship writes is correct — **Neo4j §1** ("Repartition to 1 before writing relationships") explains that concurrent partition writes cause lock contention when endpoint nodes are shared. But that reasoning does not extend to node writes, which carry no inter-row dependency. Forcing all 100k+ Column nodes through one partition negates any horizontal Spark scaling and will dominate wall-clock time on large catalogs. **Neo4j §3** ("Aura writes are leader-only and scale vertically") acknowledges that Spark-side parallelism has a ceiling, but that ceiling is above one partition.

**Fix:** Keep `coalesce(1)` for relationship writes. Remove it from node writes and let the Neo4j Spark Connector batch at the configured `DBXCARTA_NEO4J_BATCH_SIZE` (per **Neo4j §2**). Also remove the stale Stage 7 comment at `pipeline.py:434–436` that framed this as deferred work — the decision is made now.

**Decision (confirmed):** Remove `coalesce(1)` from all node writes and delete the deferral comment.

---

## 5. Value node sample is collected to the Spark driver (scalability)

**Where:** `sample_values.py:104, 129–149`

`_sample_values` returns a Python `list[tuple]`, which is then converted to `Row` objects and re-materialized as a Spark DataFrame via `spark.createDataFrame(value_node_rows)`. The underlying queries already collect per-table through the driver, but gathering all of them into one Python list before recreating the DataFrame means peak driver memory is proportional to the total sample size across the entire catalog, not a single table.

This pattern also bypasses the **Spark/Databricks §4** ("Materialize any DataFrame with `ai_query` before using it twice") principle in spirit: the workaround for lazy re-evaluation is materialization in Spark, not collection through the driver.

**Fix:** Accumulate per-table DataFrames and union them in Spark. Each `spark.sql(query)` call in `_sample_values` already returns a DataFrame; those can be unioned without a driver collect step. Because the raw sample DataFrame contains (col_name, val, cnt) rows across all tables, `generate_value_id` must be applied as a Spark expression (UDF or equivalent) and `dropDuplicates("id")` applied on the value-node DataFrame before returning. The dedup requirement is documented in **Spark/Databricks §5** (new section to be added to `docs/best-practices.md`).

---

## 6. Five nearly-identical embedding blocks (maintainability)

**Where:** `pipeline.py:268–404`

The Table, Column, Schema, Database, and Value embedding blocks each repeat the same pattern: `add_embedding_column` → `_stage_embedded_nodes` → `compute_failure_stats` → update summary dicts → update totals → log. Only the DataFrame, text expression, and label differ. If the pattern changes (e.g., adding `embedding_model_version` to the summary per a future best-practice update), five places need updating.

**Fix:** Extract a helper:

```python
def _embed_and_stage(df, text_expr, label, settings, staging_path, summary):
    enriched = emb.add_embedding_column(df, text_expr, settings.dbxcarta_embedding_endpoint,
                                         settings.dbxcarta_embedding_dimension, label=label)
    staged = _stage_embedded_nodes(enriched, staging_path, label)
    rate, attempts, successes = emb.compute_failure_stats(staged)
    summary.embedding_attempts[label] = attempts
    summary.embedding_successes[label] = successes
    summary.embedding_failure_rate_per_label[label] = rate
    return staged, attempts, successes
```

Then drive all five labels from a loop. The Values label is slightly special (guarded by `value_node_df is not None`) but can still be handled in the same loop with a None check.

---

## 7. `_run()` is ~350 lines of inline orchestration (maintainability)

**Where:** `pipeline.py:137–493`

The function handles extract, five embedding transforms, sample values, threshold checks, six node writes, five relationship writes, and cleanup in a single body. Finding any one piece requires scrolling past everything else, and the summary update pattern is scattered throughout. Because phases are not isolated in separate functions, no phase can be unit-tested without running the whole function.

**Fix:** Decompose into private functions: `_extract(spark, settings, schema_list)`, `_transform_embeddings(...)`, `_transform_sample_values(...)`, `_check_thresholds(...)`, `_load(...)`. `_run()` becomes a sequence of calls with no logic of its own.

**Decision (confirmed):** Proceed with full decomposition.

---

## 8. Three separate Neo4j driver connections where one would do (maintainability)

**Where:** `pipeline.py:627 (_bootstrap_constraints)`, `pipeline.py:671 (_purge_stale_values)`, `pipeline.py:715 (_query_neo4j_counts)`

Each function opens its own `GraphDatabase.driver(...)` context. These three calls are sequential — no concurrent access justifies separate drivers. Each open/close adds latency and a TLS handshake against Aura.

**Fix:** Open one driver in `_run()`, pass it to all three functions, close it once at the end. This also makes the three functions easier to unit-test with an injected mock driver.

---

## 9. `_query_neo4j_counts` counts all nodes in the graph, not just dbxcarta nodes (minor correctness)

**Where:** `pipeline.py:716`

```python
"MATCH (n) RETURN labels(n)[0] AS label, count(*) AS cnt"
```

This counts every node in the Neo4j instance under its first label. Any nodes from other tools or prior experiments inflate the run summary's `neo4j_counts`. If the Aura instance is shared, the summary becomes misleading without any warning.

**Fix:** Count each label explicitly:
```python
for label in (LABEL_DATABASE, LABEL_SCHEMA, LABEL_TABLE, LABEL_COLUMN, LABEL_VALUE):
    result = session.run(f"MATCH (n:{label}) RETURN count(n) AS cnt")
    counts[label] = result.single()["cnt"]
```

---

## 10. `embedding_text` written to Neo4j for Table nodes wastes graph storage (minor)

**Where:** `embeddings.py:83`, `pipeline.py:447`

Non-Table labels drop `embedding_text` before the Neo4j write (`embeddings.py:83`). Table nodes keep it. The hash (`embedding_text_hash`) already provides drift detection per **Project-level principle §2** ("pipeline-owned artifacts memo, they do not authorize"); the full text is redundant for retrieval and adds storage pressure on Aura.

**Fix:** Drop `embedding_text` from Table nodes before the Neo4j write (consistent with all other labels). No new flag. Update the assertion in `tests/embeddings/test_graph_integration.py:45` from `has_text == total` to `has_text == 0`, matching the pattern used for Column, Value, Schema, and Database.

**Decision (confirmed):** Drop `embedding_text`, no flag, update existing test assertion.

---

## Summary table

| # | Issue | File | Best-practice ref | Severity |
|---|-------|------|-------------------|----------|
| 1 | Stale Values survive `INCLUDE_VALUES=false` | pipeline.py:458 | Project §1 | Bug |
| 2 | Preflight accepts path that `_resolve_staging_path` rejects | pipeline.py:565/511 | Project §3 | Bug |
| 3 | Preflight cannot detect embedding dimension mismatch | pipeline.py:613 | Spark §2, Project §3 | Bug |
| 4 | `coalesce(1)` on node writes caps throughput | pipeline.py:431+ | Neo4j §1, §2, §3 | Scalability |
| 5 | Value sample collected to driver memory | sample_values.py:104 | Spark §4 | Scalability |
| 6 | Five duplicate embedding blocks | pipeline.py:268–404 | — | Maintainability |
| 7 | `_run()` is ~350 lines inline | pipeline.py:137 | — | Maintainability |
| 8 | Three separate Neo4j driver connections | pipeline.py:627/671/715 | — | Maintainability |
| 9 | Node count query includes non-dbxcarta nodes | pipeline.py:716 | — | Minor |
| 10 | `embedding_text` written to Neo4j for Tables | pipeline.py:447 | Project §2 | Minor |

---

## Decisions log

| # | Decision |
|---|----------|
| 1 | Derive `candidate_col_ids` from cached `columns_df` (STRING/BOOLEAN filter, no schema probe). Add `get_candidate_col_ids` to `sample_values.py`. Always call purge before the `INCLUDE_VALUES` guard. |
| 4 | Remove `coalesce(1)` from all node writes now. Delete the Stage 7 deferral comment. |
| 5 | Switch `_sample_values` to union DataFrames in Spark. Add `dropDuplicates("id")` for value-node dedup. Document in `docs/best-practices.md` §Spark 5. |
| 7 | Full `_run()` decomposition confirmed. |
| 10 | Drop `embedding_text` from Table nodes. No new flag. Update `test_graph_integration.py:45` assertion. |

---

## Testing Plan

Testing scope: the one-line assertion change for issue 10 is the only test file modification required. Existing integration tests plus a pipeline run cover all other fixes. No new test files.

### Issue 10 — `embedding_text` on Table nodes

Update `tests/embeddings/test_graph_integration.py:45`:

```python
# before
assert result["has_text"] == result["total"]
# after
assert result["has_text"] == 0
```

Also remove `has_text` from the five-property count in the test name and query if desired, to match the four-property pattern used by Column, Value, Schema, and Database tests. No new test file needed.
