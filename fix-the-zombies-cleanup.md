# Fix The Zombies Cleanup Review

This reviews the implementation of `docs/proposals/fix-zombines-v3.md` through Phase 3, including the dead-code removal that was pulled forward from Phase 4.

The short version: the batching work is mostly pointed in the right direction, but the semantic FK lookup is not safe yet. The current code can still miss the real match, which was the main thing Phase 3 was supposed to fix.

## 1. The schema filter is in the wrong place

Where:

- `packages/dbxcarta-spark/src/dbxcarta/spark/ingest/fk/inference.py:511`
- `packages/dbxcarta-spark/src/dbxcarta/spark/ingest/fk/inference.py:514`

What is wrong:

The code asks Neo4j for the top `k` closest `KeyColumn` nodes first. Then it filters those results to the same catalog and schema.

ELI5:

Imagine asking for the 10 closest people in the whole city, then throwing away anyone not in your school. If all 10 people are from other schools, you get nobody, even if the best person in your school was number 11.

That is the old bug in a smaller shape. The proposal said to filter by catalog and schema inside the vector search. The code does the filter after the vector search.

Recommended fix:

Move the catalog and schema filter inside the `SEARCH` clause:

```cypher
MATCH (s:Column)
WHERE s.embedding IS NOT NULL
CALL (s) {
  MATCH (t:KeyColumn)
    SEARCH t IN (
      VECTOR INDEX keycolumn_embedding
      FOR s.embedding
      WHERE t.catalog = s.catalog
        AND t.schema = s.schema
      LIMIT $k
    ) SCORE AS score
  WHERE t.id <> s.id
  RETURN t.id AS target_id, score
}
RETURN s.id AS source_id, target_id, score
```

Also add a test where wrong-schema key columns are closer than the right-schema key column. The right-schema match must still be returned.

## 2. The `KeyColumn` vector index is missing filter properties

Where:

- `packages/dbxcarta-spark/src/dbxcarta/spark/ingest/load/neo4j_io.py:101`

What is wrong:

The index is created only on `n.embedding`. Neo4j vector search needs filter properties to be added to the vector index when the index is created. The proposal says catalog and schema are filterable properties, but the code does not create them that way.

ELI5:

The database has an address book sorted by "looks similar." We also want to ask "only people in this school." But the address book was not built with school names in it, so it cannot filter by school while searching.

Recommended fix:

Create the `KeyColumn` vector index with `catalog` and `schema` as additional filter properties. For Neo4j versions that support Cypher 25 vector filtering, the shape should be:

```cypher
CREATE VECTOR INDEX keycolumn_embedding IF NOT EXISTS
FOR (n:KeyColumn) ON n.embedding
WITH [n.catalog, n.schema]
OPTIONS {
  indexConfig: {
    `vector.dimensions`: $dim,
    `vector.similarity_function`: 'cosine'
  }
}
```

If the deployed Neo4j version does not support this syntax, do not pretend the Phase 3 plan is implemented. Use a supported fallback and over-fetch by a large amount, or keep semantic FK disabled until the compatible index/query path is ready.

## 3. The code uses a Neo4j feature that may not exist in the target workspace

Where:

- `packages/dbxcarta-spark/src/dbxcarta/spark/ingest/fk/inference.py:507`
- `packages/dbxcarta-spark/src/dbxcarta/spark/ingest/fk/discovery.py:142`

What is wrong:

The production path now depends on Neo4j `SEARCH` vector syntax. The proposal says this syntax still needs to be verified in Phase 6. But the code already calls it when semantic FK is on.

ELI5:

The car now needs a special key. We have not checked if we actually have that key. If we try to drive, the car may not start.

Recommended fix:

Add a startup compatibility check before semantic FK runs:

- Confirm the Neo4j version supports `SEARCH`.
- Confirm `keycolumn_embedding` exists and is `ONLINE`.
- Confirm the index has `embedding`, `catalog`, and `schema`.
- Fail early with a clear message if not.

Do this before the expensive Spark work reaches semantic FK discovery.

## 4. Stale `:KeyColumn` labels are never removed

Where:

- `packages/dbxcarta-spark/src/dbxcarta/spark/run.py:434`
- `packages/dbxcarta-spark/src/dbxcarta/spark/ingest/load/neo4j_io.py:164`

What is wrong:

The code adds `:KeyColumn` to columns that are key-like in this run. It does not remove `:KeyColumn` from columns that used to be key-like but are not key-like anymore.

The Spark filter later rejects non-key targets, but stale `:KeyColumn` nodes can still crowd out good targets in the vector top-k search before Spark sees the rows.

ELI5:

Yesterday a box had a "good key" sticker. Today it is not a good key anymore, but nobody removes the sticker. The search still looks at it first and may miss the real key.

Recommended fix:

Before the chunk loop re-adds current `:KeyColumn` labels, clear old labels in this run's scope:

```cypher
MATCH (c:Column:KeyColumn)
WHERE c.catalog IN $catalogs
  AND (size($schemas) = 0 OR c.schema IN $schemas)
REMOVE c:KeyColumn
```

Then let the existing per-chunk write add `:KeyColumn` back only to the current key-like columns.

## 5. The live semantic test is too weak

Where:

- `tests/spark/fk_semantic/test_semantic_nn_integration.py:25`

What is wrong:

The integration test assumes a live Neo4j graph and index already exist. It only checks that the result has the expected columns. It does not prove that:

- same-schema filtering happens inside the vector search,
- `LIMIT $k` is bound correctly,
- stale `:KeyColumn` labels do not crowd out real targets,
- the right foreign key is returned.

ELI5:

The test checks that the vending machine has a slot. It does not put in money, press the button, or check that the snack comes out.

Recommended fix:

Make the integration test build its own tiny fixture:

- Create two schemas.
- Create one source column.
- Create one wrong-schema key column with a closer vector.
- Create one right-schema key column with a slightly less close vector.
- Build the index.
- Run `semantic_nn_pairs`.
- Assert the right-schema target is returned.
- Clean up the fixture.

This catches the exact bug Phase 3 was meant to fix.

## 6. The value cleanup warning comes after the delete

Where:

- `packages/dbxcarta-spark/src/dbxcarta/spark/run.py:330`
- `packages/dbxcarta-spark/src/dbxcarta/spark/run.py:338`

What is wrong:

If value sampling finds candidate columns but produces zero Value nodes, the code deletes stale Values first and warns after. That can wipe useful old Values when sampling failed silently.

The proposal accepts this tradeoff, but it is still risky.

ELI5:

The smoke alarm rings after the room was already cleared out. The warning is true, but it is too late to stop the damage.

Recommended fix:

Change the order and make this safer:

- If `candidate_columns > 0` and `value_nodes == 0`, do not delete by default.
- Record the warning.
- Either fail the run, or require an explicit setting like `DBXCARTA_ALLOW_EMPTY_VALUE_CLEANUP=true` to delete anyway.

This keeps the run from silently deleting all old Values because one sampling step broke.

## 7. `read_query` turns every parameter into a string

Where:

- `packages/dbxcarta-spark/src/dbxcarta/spark/ingest/load/writer.py:78`
- `packages/dbxcarta-spark/src/dbxcarta/spark/ingest/fk/inference.py:550`

What is wrong:

`dbxcarta_semantic_k` is an integer, but `read_query` passes it to the connector as `str(value)`. Neo4j `LIMIT $k` needs an integer. The connector may coerce this, but the code does not prove it.

ELI5:

The recipe asks for the number 10. We hand it the word "10" and hope the cook understands.

Recommended fix:

Verify the connector's parameter typing in the live integration test. If the connector sends all parameters as strings, change the Cypher to cast safely or change the connector option path to preserve integer type if supported.

## 8. The helper name is now misleading

Where:

- `packages/dbxcarta-spark/src/dbxcarta/spark/run.py:229`
- `packages/dbxcarta-spark/src/dbxcarta/spark/run.py:445`

What is wrong:

`_embed_and_write_table_column_chunks` now writes Table, Column, and Value nodes. The proposal said to rename it. The code did not.

ELI5:

The label on the drawer says "shirts and pants," but the drawer also has socks. That makes the next person look in the wrong place.

Recommended fix:

Rename it to `_embed_and_write_node_chunks`.

## Recommended Fix Order

1. Fix the `KeyColumn` index and `SEARCH` query together.
2. Add a real Neo4j integration test that proves same-schema recall.
3. Add a semantic FK startup compatibility check.
4. Clear stale `:KeyColumn` labels before re-applying them.
5. Make zero-value cleanup safe before delete.
6. Verify connector parameter typing for `k`.
7. Rename the helper.

## Sources Checked

- Neo4j `SEARCH` clause docs: https://neo4j.com/docs/cypher-manual/current/clauses/search/
- Neo4j vector index docs: https://neo4j.com/docs/cypher-manual/current/indexes/semantic-indexes/vector-indexes/
