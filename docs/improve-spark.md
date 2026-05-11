# Improving Spark Scalability

This note captures where the DBxCarta ingest pipeline already follows Spark
best practices and where it could become more Spark-native.

The main principle is simple: keep large data in Spark DataFrames until the
final write step. Collecting small control data is fine. Collecting large
catalog metadata or pairwise inference inputs can limit scale.

## Current State

The extract and graph-shaping stages are mostly Spark-native.

`src/dbxcarta/ingest/extract.py` reads Unity Catalog metadata through Spark SQL:

- `information_schema.schemata`
- `information_schema.tables`
- `information_schema.columns`

Those results stay as Spark DataFrames. They are cached, counted, and passed to
the graph builders.

`src/dbxcarta/ingest/schema_graph.py` maps those DataFrames to the graph schema:

- `Database`
- `Schema`
- `Table`
- `Column`
- `HAS_SCHEMA`
- `HAS_TABLE`
- `HAS_COLUMN`

The Neo4j load also writes Spark DataFrames through the Neo4j Spark Connector.

That part is the right shape for large catalogs.

## Where Driver Collection Happens

Some later stages collect metadata to the Python driver before the Neo4j write.

These are the main spots:

- FK discovery collects all extracted columns into `ColumnMeta` objects.
- PK and UNIQUE constraints are collected into a `PKIndex`.
- Declared FK pairs are collected into `FKEdge` objects.
- Metadata FK inference runs pair logic in Python.
- Semantic FK inference collects column embeddings and compares candidate pairs
  in Python.
- Value-overlap corroboration collects sampled Value data into a Python dict.
- Sample-value planning groups string and boolean columns by table, then
  collects table candidates to the driver.

For thousands of metadata rows, this is usually manageable. For tens or
hundreds of thousands of columns, it can become the main scaling bottleneck.

## Why It Matters

Spark can spread DataFrame work across a cluster.

Python driver loops cannot.

When driver-side logic receives all columns or all candidate pairs, the driver
must hold that data in memory and do the work in one process. That weakens the
main benefit of using Spark.

The highest-risk area is foreign-key inference because candidate pairs can grow
much faster than the number of columns.

```text
columns grow linearly

1,000 columns
2,000 columns
5,000 columns

candidate pairs can grow much faster

1,000 x 1,000
2,000 x 2,000
5,000 x 5,000
```

The code already uses gates to reduce the work, but the safer long-term design
is to push more of those gates into Spark joins and filters.

## Priority 1: Keep Declared FK Edges in Spark

Declared FKs are already read as Spark DataFrames.

Today, the code collects FK rows and turns them into Python `FKEdge` objects.
That can be improved by building the final `REFERENCES` DataFrame directly in
Spark.

Target shape:

```text
referential_constraints
        |
        v
key_column_usage join
        |
        v
Spark DataFrame with:
  source_id
  target_id
  confidence
  source
  criteria
        |
        v
Neo4j Spark Connector
```

Why this is good:

- It removes an avoidable `collect()`.
- It keeps declared FK edge creation distributed.
- It keeps the `REFERENCES` schema the same.

## Priority 2: Move Metadata FK Inference to Spark Joins

Metadata inference currently uses Python objects and pair loops.

A Spark version could work like this:

```text
columns_df as src
        |
        v
columns_df as tgt
        |
        v
join/filter on:
  type compatibility
  name match
  target PK-like evidence
  prior pair suppression
        |
        v
score with Spark columns
        |
        v
rank or attenuate tie-breaks
        |
        v
REFERENCES DataFrame
```

This would preserve the same rules while moving the heavy candidate filtering
into Spark.

Likely Spark columns:

- normalized source column name
- normalized target column name
- source stem
- target table name
- type family
- PK evidence
- comment-token overlap flag
- confidence score

Why this is good:

- Candidate generation becomes distributed.
- Large catalogs no longer require all candidate logic in one Python process.
- Counters can be produced with Spark aggregations.

## Priority 3: Make Semantic FK Inference More Selective

Semantic FK inference is the riskiest path for scale because it can compare
many vector pairs.

Better options:

1. Compare only source-like columns to PK-like target columns.
2. Use Spark filters before collecting anything.
3. Use vector search or approximate nearest-neighbor search instead of Python
   pairwise cosine.
4. Keep semantic inference disabled by default for broad catalog scans.

Target shape:

```text
embedded columns
        |
        v
candidate gate:
  compatible types
  target is PK-like
  pair not already covered
        |
        v
vector similarity search or distributed scoring
        |
        v
REFERENCES DataFrame
```

Why this is good:

- It avoids all-pairs vector comparison in Python.
- It lets semantic inference scale independently from total catalog size.
- It keeps embeddings useful without turning them into the bottleneck.

## Priority 4: Keep Sample Planning More Distributed

The value sampler already keeps sampled value rows in Spark. That is good.

The driver-side part is planning. It groups string and boolean columns by table,
then collects table candidates to Python.

This is acceptable for many catalogs, but very large catalogs could benefit
from a more distributed planner.

Possible improvements:

- Keep candidate columns in a Spark DataFrame longer.
- Write a temporary planning table for candidate columns.
- Process candidates by schema or table batches.
- Add a maximum candidate-column guardrail.

## Priority 5: Add Scale Guardrails

Before driver-side collection, the pipeline can check catalog size and make a
clear decision.

Useful guardrails:

- Maximum columns allowed for Python metadata inference.
- Maximum string and boolean candidate columns for value sampling.
- Maximum semantic candidate pairs.
- Warning when `DBXCARTA_SCHEMAS` is blank on a large catalog.
- Clear error message that suggests narrowing `DBXCARTA_SCHEMAS`.

Example behavior:

```text
if column_count > DBXCARTA_DRIVER_INFERENCE_MAX_COLUMNS:
    fail early with a clear message
```

This protects users from accidentally running a broad catalog scan that creates
too much driver work.

## What Should Stay As Is

Some collections are small and reasonable:

- Preflight checks that return one row.
- Run summary rows.
- Neo4j count summaries.
- Small config or control values.

The goal is not to remove every `collect()`. The goal is to avoid collecting
large metadata sets or large candidate-pair sets before the final write.

## Recommended Implementation Order

1. Build declared `REFERENCES` edges directly as a Spark DataFrame.
2. Add guardrails before the existing driver-side FK inference paths.
3. Move metadata FK inference to Spark joins and aggregations.
4. Make semantic inference use a smaller candidate set or vector search.
5. Rework sample-value planning only if real catalog sizes show it is a
   bottleneck.

This order reduces risk. It improves the clearest bottlenecks first while
keeping the existing graph contract stable.
