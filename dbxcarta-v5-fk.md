# DBxCarta v5 — Foreign Key Resolution (W8) Investigation and Implementation

## Goal

Validate that Unity Catalog's `information_schema` views expose foreign-key constraint metadata in a form that DBxCarta can join into source-and-target column pairs, measure how much of the declared foreign-key coverage in the target catalog is actually resolvable, and ship the `REFERENCES` relationship as a first-class part of the DBxCarta graph. v5 ships with `REFERENCES` stubbed at zero coverage; this plan is the follow-up that replaces the stub with a tested implementation.

## Deliverable

A merged change that contains the following:

1. A validated Spark SQL query against `graph_enriched_lakehouse.information_schema` that returns one row per resolved foreign-key column pair with source and target catalog, schema, table, column, and ordinal position.
2. A coverage measurement recorded in the repository documentation, expressed as the percentage of foreign-key constraints declared on the target catalog that resolve end to end to a source-and-target column tuple.
3. A decision recorded in the repository documentation on how to handle foreign keys the join cannot resolve, either skipped with a logged warning or treated as a run failure.
4. An implementation of the `REFERENCES` transform in the unified pipeline that reads through this query, produces the relationship DataFrame, and writes it to Neo4j via the existing Spark Connector write path.
5. An extension to the `tests/schema_graph/` pytest suite that asserts every `REFERENCES` relationship has a source and target `Column` node that exists in the graph, and that the coverage percentage recorded in the run summary matches the coverage measured during investigation.

## Background

v4 identifies foreign-key resolution as a critical open item. The canonical ISO SQL standard join shape is `referential_constraints` joined to two aliased reads of `key_column_usage` (one for the source side, one for the target side), matched on ordinal position to preserve composite-key ordering. The bridge from a source constraint to its target constraint runs through the `unique_constraint_catalog`, `unique_constraint_schema`, and `unique_constraint_name` columns of `referential_constraints`, which point at the unique or primary-key constraint that the foreign key references.

Three things are not yet known against Databricks Unity Catalog specifically. First, whether the column names on the Databricks views match the ISO spec exactly or diverge. Second, whether Delta tables that declare `FOREIGN KEY` in their DDL actually surface in these views and with what `enforced` semantics, given that Databricks foreign-key constraints are informational rather than enforced at write time. Third, what percentage of the declared foreign keys on the target catalog are resolvable end to end.

## Investigation checklist

### Stage 1: Discovery of the view schemas

- [ ] Query `information_schema.referential_constraints` in the target catalog and record the full column list, row count, and a sample of representative rows. Capture whether the table is empty, sparsely populated, or fully populated.
- [ ] Query `information_schema.key_column_usage` in the target catalog and record the full column list, row count, and a sample of representative rows.
- [ ] Query `information_schema.table_constraints` in the target catalog and record the full column list and row count to confirm primary-key and unique-constraint metadata is also exposed.
- [ ] Compare the Databricks view column names against the ISO SQL standard names and record any differences that would affect the join shape.

### Stage 2: Join validation

- [ ] Construct the three-way join from `referential_constraints` to `key_column_usage` (source side) to `key_column_usage` (target side), bridging through the unique-constraint fields, and matching on ordinal position.
- [ ] Execute the join against the target catalog and confirm that at least one representative foreign-key declaration resolves to the expected source-and-target column tuple.
- [ ] Test the join against a table with a composite foreign key, if one exists in the target catalog, to confirm ordinal position preserves composite-key ordering correctly. If no composite foreign keys exist in the target catalog, record that fact and note that composite-key correctness is untested against real data.
- [ ] Test the join against a table with a foreign key that crosses schemas, if one exists, to confirm the bridge fields carry cross-schema references correctly.

### Stage 3: Coverage measurement

- [ ] Count the total number of foreign-key constraints declared in `referential_constraints` for the target catalog.
- [ ] Count the number of those constraints that resolve end to end through the join to a non-null source and target column tuple.
- [ ] Record the resulting coverage percentage in the repository documentation alongside a short description of what the unresolved constraints have in common, if anything.
- [ ] If coverage is substantially below one hundred percent, classify the unresolved cases by cause: missing target primary key, missing `key_column_usage` rows, cross-catalog references, or other patterns.

### Stage 4: Unresolvable foreign key strategy

- [ ] Review the unresolved cases identified in Stage 3 and decide whether each class should be skipped with a logged warning or treated as a run failure.
- [ ] Record the decision and the reasoning in the repository documentation.
- [ ] Confirm the unified pipeline's failure-rate threshold mechanism can be reused for foreign-key resolution failures, or whether foreign keys need their own threshold configuration.

### Stage 5: Implementation

- [ ] Replace the R4 stub in the unified pipeline with the validated join query, filtered to the configured schemas when `DBXCARTA_SCHEMAS` is non-empty.
- [ ] Produce a Spark DataFrame with `source_column_id` and `target_column_id` columns using the same identifier expression used elsewhere in the pipeline, so the `REFERENCES` endpoints match existing `Column` nodes exactly.
- [ ] Write the `REFERENCES` relationship to Neo4j using the existing Spark Connector write path with `save.strategy = "keys"` and `Match` mode on both endpoints.
- [ ] Extend the run summary to record foreign-key constraints declared, resolved, and skipped, and the resulting coverage percentage for this run.

### Stage 6: Verification

- [ ] Extend `tests/schema_graph/` to include a test that asserts the coverage percentage recorded in the run summary matches the coverage measured during investigation within an acceptable tolerance for catalog drift.
- [ ] Extend `tests/schema_graph/` to include a test that for every `REFERENCES` relationship, both endpoint `Column` nodes exist in Neo4j at the exact identifiers the relationship points at.
- [ ] Confirm that a catalog with zero declared foreign-key constraints produces a run with coverage recorded as zero and no `REFERENCES` relationships written, without failing the run.
- [ ] Confirm that a second submit after the first produces identical `REFERENCES` relationship counts and no duplicates.

## Open items

- Whether composite-key foreign keys exist in the target catalog at investigation time. If none exist, composite-key handling ships validated against the join logic only, not against real data. A later catalog that does contain composite keys triggers a re-verification.
- Whether foreign keys that cross catalogs appear in Unity Catalog at all. The investigation records what the views expose; cross-catalog references are out of scope for this plan if the views do not expose them.
- Whether the `enforced` column distinguishes meaningfully between Delta informational foreign keys and enforced constraints from other table types. If it does, the decision to include or exclude informational foreign keys from the graph is a separate product question to raise with downstream consumers.

## Out of scope

- Any change to the graph contract. `REFERENCES` relationship type, endpoint labels, and identifier shapes are already defined in v4 and carry forward unchanged.
- Any change to the unified pipeline's overall shape. This plan adds one transform and one write to the existing extract-transform-load structure established in the v5 plan.
- Inferring foreign keys that are not declared in `information_schema`. DBxCarta ships only what Unity Catalog knows about. Inference from naming conventions or data profiling is a separate future proposal.
