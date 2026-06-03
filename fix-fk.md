# Fix: foreign keys never became graph edges

Status: code fix complete and unit-tested for `dense-schema` and `schemapile`. Live verification pending (re-materialize, re-ingest, re-client).

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

1. Re-materialize dense. The run is additive: `CREATE TABLE IF NOT EXISTS` no-ops the existing 1000 tables, and the new ALTERs attach the PK and FK constraints.
2. Re-run ingest and confirm the relationships now flow through.
3. Run the client and confirm `graph_rag` separates from `no_context` and `schema_dump`.
4. Verify schemapile the same way when its fixture is materialized.
5. Confirm finance-genie is unaffected. It runs against tables that should already carry declared constraints.
6. Commit the materializer changes.

## Test plan

### Unit (done)

- Dense and schemapile materialize tests assert the PK DDL (`SET NOT NULL` plus `ADD CONSTRAINT ... PRIMARY KEY`) and FK DDL (`ADD CONSTRAINT ... FOREIGN KEY ... REFERENCES`) are emitted for a small in-memory schema payload.
- Tests assert the foreign-key ALTERs come after every primary-key ALTER, proving the second pass.
- Schemapile tests also cover self-referential foreign keys, skipping a foreign key whose target was not materialized, and constraint-name hashing past the length limit.
- Full suite: 522 passed, 1 skipped. Ruff clean on both examples.

### Live verification (pending)

1. **Constraints land.** After re-materialize, query the data catalog:
   - `information_schema.table_constraints` for `PRIMARY KEY` and `FOREIGN KEY` rows in `dense-1000`.
   - `information_schema.referential_constraints` row count near 1704.
2. **Ingest reads them.** After re-ingest, check the run summary in `dbxcarta-catalog.dense-ops.dbxcarta_run_summary`:
   - `fk_declared` near 1704, `fk_resolved` near `fk_declared`, `fk_skipped` near 0, `fk_edges` near 1704.
   - `neo4j_counts.REFERENCES` greater than 0 and matching `fk_edges`.
3. **Graph is correct.** Spot-check a couple of known pairs in Neo4j, for example `hr_employees.created_by_id -> sys_users.id` and the self-reference `hr_employees.parent_id -> hr_employees.id`, by following `REFERENCES` from the column nodes.
4. **Evaluation is meaningful.** Run the client across `no_context, schema_dump, graph_rag` and compare per-arm scores. The `graph_rag` arm should now differ from the other two, since it finally has relationship structure to retrieve over.

### Acceptance criteria

- `fk_edges` and Neo4j `REFERENCES` are non-zero and consistent with the declared constraint count.
- The known FK pairs resolve as `REFERENCES` edges in the graph.
- The `graph_rag` arm produces a score distinct from `no_context` and `schema_dump`.

### Operational reminders for the live run

- Stage the cheap check first. Confirm `fk_edges` and `REFERENCES` after ingest before launching the long client run.
- Drive completion off the job run state, not the `make` exit code. Poll `databricks jobs get-run <id>` until `life_cycle_state` is terminal, then read `result_state`.
