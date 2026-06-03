# Fix: foreign keys never became graph edges

Status: code fix complete and unit-tested for `dense-schema` and `schemapile`, and verified end to end live on dense. Re-materialize declared 1000 PK and 1704 FK constraints; the re-ingest carried all 1704 to `fk_edges` and Neo4j `REFERENCES` (was 0); and a 5-question client smoke check confirmed `graph_rag` now separates from the other arms. Remaining: schemapile and finance-genie verification, then commit. See "Live verification (results)" below.

## What was wrong

The dense-schema ingest built a semantic layer with the right nodes but zero relationships between tables. The first ingest run summary showed:

- `tables: 1000`, `columns: 10314`, `value_nodes: 23474`, Neo4j `Table 1000 / Column 10314 / Value 23474`.
- `fk_declared: 0`, `fk_resolved: 0`, `fk_inferred_metadata_accepted: 0`, `fk_edges: 0`, Neo4j `REFERENCES: 0`.

The generator had produced 1704 foreign-key relationships and a primary key per table, so the relationships existed in the source data. They never reached the graph. The `graph_rag` evaluation arm had no edges to traverse, which made its comparison against `no_context` and `schema_dump` meaningless.

### Root cause

The relationship metadata was written to a place nothing downstream reads.

1. `materialize.py` stored each table's primary and foreign keys only as Delta `TBLPROPERTIES` (`dense.primary_keys`, `dense.foreign_keys`, and `schemapile.foreign_keys`). It created no real Unity Catalog `PRIMARY KEY` or `FOREIGN KEY` constraints.
2. The ingest discovers foreign keys two ways, and neither reads those table properties:
   - **Declared path** (`packages/dbxcarta-spark/.../fk/declared.py`): reads real constraints from `information_schema.referential_constraints` joined through `key_column_usage`. There were no declared constraints, so it found nothing.
   - **Metadata inference path** (`packages/dbxcarta-spark/.../fk/inference.py`): infers an edge by matching a foreign-key column name to a primary-key-like target column, gated by a PK gate. The PK gate accepts a column when it is a declared PK, a declared UNIQUE leftmost column, a column literally named `id`, or a `<table>_id` column. The match step requires the source column name, minus an `_id` style suffix, to equal the target table name (for example `customer_id` matches table `customers`).
3. The dense fixture names foreign-key columns by role, not by target table: `parent_id`, `created_by_id`. Stripping `_id` yields `parent` and `created_by`, which do not match the target table names `hr_employees` or `sys_users`. So the heuristic could not reconstruct the edges either.

Result: declared 0, inferred 0, total 0.

## What was fixed

Both `materialize.py` files now declare real Unity Catalog constraints from the primary-key and foreign-key metadata they already hold. Unity Catalog constraints are informational and `NOT ENFORCED`, which is what the declared path reads from `information_schema`.

The change is a two-pass structure per schema:

- **Pass 1**, per table, after the table is created and rows are inserted: set each primary-key column `NOT NULL` (Unity Catalog requires this for a PK) and `ADD CONSTRAINT pk_<table> PRIMARY KEY (...)`.
- **Pass 2**, after every table and its primary key exist: `ADD CONSTRAINT fk_<table>__<cols> FOREIGN KEY (...) REFERENCES <parent> (...)`. The second pass is required because a foreign key cannot reference a parent whose primary key does not yet exist, and tables are created concurrently.

Both materializers also:

- Apply the same name sanitization used for tables and columns to foreign-key targets and referred columns, and skip any constraint whose columns were dropped in sanitization or whose target table was not materialized.
- Quote every identifier with `quote_identifier`, so hyphenated names like `dense-schema-example` and `dense-1000` are handled.
- Build deterministic constraint names, truncated with a stable hash suffix when they would exceed the Unity Catalog length limit.
- Support composite and self-referential keys.
- Treat each constraint ALTER as tolerant: a single failure logs a warning and continues, so one bad table cannot abort the run.
- Report `pk`/`fk` counters in the summary line.

Files changed:

- `examples/dense-schema/src/dbxcarta_dense_schema_example/materialize.py`
- `examples/schemapile/src/dbxcarta_schemapile_example/materialize.py`
- `tests/examples/dense-schema/test_materialize.py` (+2 tests)
- `tests/examples/schemapile/test_materialize.py` (+5 tests)

Once the declared constraints exist, the ingest's declared path emits one edge per column pair at confidence 1.0, which reproduces the exact 1704 dense edges with no heuristic guessing.

## Lessons learned

- Producer and consumer must agree on the channel, not just the content. The materializer wrote correct relationship data, but to `TBLPROPERTIES`, which the ingest never reads. Verify the contract between the two sides, not that each side runs.
- Name-based foreign-key inference only recovers edges when foreign-key columns are named after their target table. Role-named columns such as `parent_id` and `created_by_id` defeat it. When the relationships are known, declare them as real constraints so the declared path picks them up exactly.
- A `make` target's exit code is not the job's result. `dbxcarta-submit submit-entrypoint` waits with a 20-minute timeout, then raises and the target fails, while the cluster job keeps running. The `make ... | tee | tail` pipeline then reports `tail`'s exit code, which masks the failure. The authoritative signal is the job run state from `databricks jobs get-run`, polled to a terminal `life_cycle_state` and `result_state`.
- Related fix already in place: on this Default-Storage account, `CREATE CATALOG IF NOT EXISTS` fails without a `MANAGED LOCATION` even for an existing catalog, so bootstrap and both materializers now guard catalog creation with a `catalog_exists` check.

## Next steps

1. ~~Re-materialize dense.~~ Done: `pks=1000 fks=1704`, 0 warnings.
2. ~~Re-run ingest and confirm the relationships now flow through.~~ Done: `fk_edges=1704`, Neo4j `REFERENCES=1704`.
3. ~~Run the client and confirm `graph_rag` separates from `no_context` and `schema_dump`.~~ Done on a 5-question subset: `graph_rag` exec_rate 80% vs `schema_dump` 60% vs `no_context` 0%.
4. Verify schemapile the same way when its fixture is materialized (regenerate/materialize fixture, re-ingest, confirm `fk_edges`/`REFERENCES`).
5. Confirm finance-genie is unaffected. It runs against tables that should already carry declared constraints.
6. Commit the materializer changes (and the client `dbxcarta_client_max_questions` knob).
7. Decide whether to keep `DBXCARTA_CLIENT_MAX_QUESTIONS=5` in the dense overlay (currently set for the smoke check) or reset it to 0 / remove it for the full evaluation.
8. Optional: address the `criteria`-property warning on declared `REFERENCES` edges (see above).

## Test plan

### Unit (done)

- Dense and schemapile materialize tests assert the PK DDL (`SET NOT NULL` plus `ADD CONSTRAINT ... PRIMARY KEY`) and FK DDL (`ADD CONSTRAINT ... FOREIGN KEY ... REFERENCES`) are emitted for a small in-memory schema payload.
- Tests assert the foreign-key ALTERs come after every primary-key ALTER, proving the second pass.
- Schemapile tests also cover self-referential foreign keys, skipping a foreign key whose target was not materialized, and constraint-name hashing past the length limit.
- Full suite: 522 passed, 1 skipped. Ruff clean on both examples.

### Live verification (results)

Run on 2026-06-03 against the `dense-schema-example.dense-1000` fixture (1000 tables).

1. **Constraints land.** Confirmed via `dense-schema-example`.`information_schema`:
   - `table_constraints`: 1000 `PRIMARY KEY` + 1704 `FOREIGN KEY` rows in `dense-1000`.
   - `referential_constraints`: 1704 rows (was 0).
   - Re-materialize summary: `tables=1000 rows=10000 skipped=0 type_fallbacks=0 pks=1000 fks=1704`, zero warnings. The run is additive: `CREATE TABLE IF NOT EXISTS` no-ops, rows use `INSERT OVERWRITE`, the ALTERs attach the constraints.
2. **Ingest reads them.** Ingest job `529374279411864` finished `TERMINATED / SUCCESS` (45.9 min). Run summary in `dbxcarta-catalog.dense-ops.dbxcarta_run_summary`:
   - `fk_declared=1704`, `fk_resolved=1704`, `fk_inferred_metadata_accepted=0`, `fk_skipped=0`, `fk_edges=1704`.
   - `neo4j_counts.REFERENCES=1704`, matching `fk_edges` exactly (was 0). The declared path carried every edge with no heuristic inference and no skips.
   - `verify_ok=true`, `verify_violation_count=0`; embeddings at 0.0 failure rate.
3. **Graph is correct.** All three counts agree at 1704 (declared = resolved = edges = `REFERENCES`), so every declared constraint became exactly one graph edge.
4. **Evaluation is meaningful.** Client run `686251068611666` finished `TERMINATED / SUCCESS` (2.3 min) on a 5-question subset (`DBXCARTA_CLIENT_MAX_QUESTIONS=5`) across `no_context, schema_dump, graph_rag`:
   - `no_context`: exec_rate 0.0%, non_empty 0.0%.
   - `schema_dump`: exec_rate 60.0%, non_empty 60.0%.
   - `graph_rag`: exec_rate 80.0%, non_empty 80.0%; retrieval `schema_in_context=100%`.
   - `graph_rag` separates from the other two arms and pulls the relationship structure into context, which was the point of the fix. Full-set scoring (correctness grading over the whole question set) is deferred; this subset was a smoke check to confirm the edges flow through to retrieval.

Two non-blocking observations from the client logs:

- The graph retriever queries `r.criteria` on `REFERENCES` edges, but the declared-FK path does not set a `criteria` property, so Neo4j emits a benign "property key does not exist" warning and the value comes back null. Retrieval still traverses the edge (`schema_in_context=100%`), so this is cosmetic for declared FKs, but `DBXCARTA_INJECT_CRITERIA` is effectively a no-op on them. Candidate follow-up: have the declared path stamp a `criteria` (or stop querying it when absent).
- `correct_rate` is 0.0% across all arms on this subset. Expected for a 5-question smoke check focused on retrieval/execution rather than answer correctness; revisit with the full question set and reference SQL.

### Acceptance criteria

- `fk_edges` and Neo4j `REFERENCES` are non-zero and consistent with the declared constraint count.
- The known FK pairs resolve as `REFERENCES` edges in the graph.
- The `graph_rag` arm produces a score distinct from `no_context` and `schema_dump`.

### Operational reminders for the live run

- Stage the cheap check first. Confirm `fk_edges` and `REFERENCES` after ingest before launching the long client run.
- Drive completion off the job run state, not the `make` exit code. Poll `databricks jobs get-run <id>` until `life_cycle_state` is terminal, then read `result_state`.
