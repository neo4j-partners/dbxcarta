# DBxCarta Spark Pipeline

This page explains the DBxCarta Spark pipeline in simple terms.

Think of Unity Catalog as a big library catalog. It knows which databases,
schemas, tables, and columns exist. DBxCarta asks Unity Catalog for that
metadata, turns it into a graph shape, and writes the graph to Neo4j.

The graph helps a Text2SQL app find the right tables and columns before it
asks an LLM to write SQL.

## The Big Picture

The pipeline is one Spark job.

Spark reads Unity Catalog metadata, builds DataFrames, enriches them, then
uses the Neo4j Spark Connector to write nodes and relationships.

```text
+------------------+     +------------------+     +------------------+
| Unity Catalog    |     | Spark pipeline   |     | Neo4j            |
| metadata         | --> | extract          | --> | graph database   |
| schemas          |     | transform        |     | nodes and edges  |
| tables           |     | load             |     | vector indexes   |
| columns          |     | summarize        |     |                  |
+------------------+     +------------------+     +------------------+
```

## What Spark Does

Spark is the worker team.

It can handle many rows at once, so DBxCarta can keep catalog metadata in
Spark instead of pulling the whole catalog into one small Python process.

Spark uses DataFrames. A DataFrame is like a spreadsheet with named columns.
Each stage takes one or more DataFrames and returns new DataFrames.

```text
+-------------------+
| DataFrame         |
+-------------------+
| id                |
| name              |
| comment           |
| contract_version  |
+-------------------+
```

## Step 1: Preflight

Before doing real work, the pipeline checks that the important things are
ready.

It checks:

- The configured Unity Catalog catalog can be read.
- The summary UC Volume can be created or used.
- The summary Delta table can be created or used.
- The embedding endpoint works when embeddings are enabled.

Simple idea:

```text
+------------+     +-----------------+
| Settings   | --> | Preflight checks |
+------------+     +-----------------+
                         |
                         v
                   continue or fail
```

If preflight fails, the job stops early. That protects the graph by catching
setup problems before writes begin.

## Step 2: Extract Unity Catalog Metadata

The extract step reads Unity Catalog `information_schema`.

It reads three main views:

- `information_schema.schemata`
- `information_schema.tables`
- `information_schema.columns`

Those rows become Spark DataFrames.

```text
+----------------------------+
| Unity Catalog              |
| information_schema         |
+----------------------------+
        |
        v
+----------------------------+
| schemata_df                |
| tables_df                  |
| columns_df                 |
+----------------------------+
```

The pipeline filters out `information_schema` itself. If `DBXCARTA_SCHEMAS`
is set, it keeps only those schemas.

## Step 3: Build Graph-Shaped DataFrames

Unity Catalog rows are useful, but Neo4j needs graph pieces.

DBxCarta builds node DataFrames:

- `Database`
- `Schema`
- `Table`
- `Column`

It also builds relationship DataFrames:

- `HAS_SCHEMA`
- `HAS_TABLE`
- `HAS_COLUMN`

```text
+-------------+       HAS_SCHEMA       +----------+
| Database    | ---------------------> | Schema   |
+-------------+                        +----------+
                                             |
                                             | HAS_TABLE
                                             v
                                        +----------+
                                        | Table    |
                                        +----------+
                                             |
                                             | HAS_COLUMN
                                             v
                                        +----------+
                                        | Column   |
                                        +----------+
```

Each node gets a stable id.

Example ids:

```text
catalog
catalog.schema
catalog.schema.table
catalog.schema.table.column
```

Stable ids let Neo4j merge the same node again on the next run.

## Step 4: Add Embeddings

Embeddings are number lists that help with semantic search.

Simple idea:

```text
"customer orders table"
        |
        v
+--------------------+
| embedding endpoint |
+--------------------+
        |
        v
[0.12, 0.03, ...]
```

When table, column, schema, database, or value embeddings are enabled,
Spark calls Databricks `ai_query` to create vectors.

DBxCarta writes the embedded rows to Delta staging first.

```text
+------------------+     +--------------------+     +------------------+
| node DataFrame   | --> | ai_query endpoint  | --> | Delta staging    |
+------------------+     +--------------------+     +------------------+
```

This is important. Spark can run the same work more than once if a DataFrame
is reused. Staging makes the endpoint call happen once, then later steps read
the staged result.

## Step 5: Sample Values

Some columns are easier to understand when we know a few real values.

For string and boolean columns, DBxCarta can sample values.

Example:

```text
+----------------------+      +----------------+
| Column               | ---> | Value          |
| orders.status        |      | "shipped"      |
+----------------------+      +----------------+
        |
        | HAS_VALUE
        v
   "shipped"
```

The pipeline keeps a limit, so it copies only a small sample from each table.

These Value nodes can also get embeddings when value embeddings are enabled.

## Step 6: Find Foreign-Key Relationships

DBxCarta writes `REFERENCES` relationships between columns.

It gets them from three sources:

- Declared foreign keys from Unity Catalog constraints.
- Metadata inference from names, types, primary-key clues, and comments.
- Semantic inference from column embeddings when enabled.

### Declared Foreign Keys

Declared foreign keys come from Unity Catalog constraint metadata.

Simple idea:

```text
Unity Catalog says:
  orders.customer_id points to customers.id

DBxCarta writes:
  orders.customer_id REFERENCES customers.id
```

These edges get `confidence = 1.0` because the catalog declared them.

### Metadata Inference

Metadata inference uses clear rules.

It looks at column pairs and asks simple questions:

- Do the names match, like `id` and `id`?
- Does a source name point at a table, like `customer_id` and `customers.id`?
- Do the data types work together, like `BIGINT` and `INT`?
- Does the target look like a primary key?
- Do the comments share helpful words?

Then DBxCarta uses a fixed score table.

Example:

```text
source column: orders.customer_id
target column: customers.id

name clue:      customer_id points at customers
type clue:      BIGINT works with BIGINT
target clue:    customers.id looks like a primary key
comment clue:   optional bonus when comments overlap

result:
  create REFERENCES edge if the score is high enough
```

This is like a checklist. Stronger clues produce a higher confidence score.

### Semantic Inference

Semantic inference uses the embeddings from Step 4.

An embedding is a list of numbers that represents the meaning of some text.
For a column, the text includes the table name, column name, data type, and
comment.

Example embedding text:

```text
orders.customer_id | BIGINT | customer identifier
```

The embedding endpoint turns that text into a vector.

```text
orders.customer_id text
        |
        v
[0.10, 0.82, 0.04, ...]
```

Semantic inference compares two column vectors with cosine similarity.

Simple idea:

```text
orders.customer_id vector      customers.id vector
        |                              |
        +-------------+----------------+
                      |
                      v
              cosine similarity
                      |
                      v
              score from 0.0 to 1.0
```

A higher cosine score means the two column descriptions point in a similar
direction.

DBxCarta still uses safety gates:

- The target must look like a primary key.
- The source and target data types must work together.
- The pair must be new, so it avoids duplicating a declared or metadata edge.
- The score must be at or above the semantic threshold.

Sampled values can add extra confidence.

Example:

```text
orders.customer_id values:
  101, 102, 103

customers.id values:
  101, 102, 103, 104

value clue:
  many source values appear in the target column
```

When enough source values appear in the target values, DBxCarta adds a small
confidence bonus.

Simple example:

```text
+--------------------------+       REFERENCES       +----------------------+
| orders.customer_id       | ---------------------> | customers.id         |
+--------------------------+                        +----------------------+
```

Each `REFERENCES` edge can carry:

- `confidence`
- `source`
- `criteria`

This tells the reader whether the edge came from a declared constraint or an
inference step.

## Step 7: Write to Neo4j

After extract and transform, the pipeline writes everything to Neo4j.

It writes nodes first.

Then it writes relationships.

```text
+------------------+
| write nodes      |
| Database         |
| Schema           |
| Table            |
| Column           |
| Value            |
+------------------+
        |
        v
+------------------+
| write edges      |
| HAS_SCHEMA       |
| HAS_TABLE        |
| HAS_COLUMN       |
| HAS_VALUE        |
| REFERENCES       |
+------------------+
        |
        v
+------------------+
| Neo4j graph      |
+------------------+
```

Neo4j uses constraints on node ids. That means a rerun updates existing nodes
instead of making duplicates.

## Step 8: Count, Verify, and Summarize

At the end, DBxCarta asks Neo4j how many nodes and relationships exist.

It also runs verification checks.

Then it writes a run summary to three places:

- Driver stdout.
- A JSON file in a UC Volume.
- A row in a Delta summary table.

```text
+------------------+     +------------------+
| completed run    | --> | RunSummary       |
+------------------+     +------------------+
                              |
             +----------------+----------------+
             |                |                |
             v                v                v
        stdout          JSON file        Delta table
```

The summary records useful facts:

- How many schemas, tables, and columns were read.
- How many values were sampled.
- How many foreign keys were found.
- How many embeddings were attempted and succeeded.
- Whether verification passed.

## The Pipeline as One Simple Story

Here is the whole flow in one diagram.

```text
+-------------------+
| Settings          |
+-------------------+
        |
        v
+-------------------+
| Preflight         |
+-------------------+
        |
        v
+-------------------+
| Extract UC        |
| metadata          |
+-------------------+
        |
        v
+-------------------+
| Build graph       |
| DataFrames        |
+-------------------+
        |
        v
+-------------------+
| Add embeddings    |
| and sample values |
+-------------------+
        |
        v
+-------------------+
| Find references   |
+-------------------+
        |
        v
+-------------------+
| Write Neo4j graph |
+-------------------+
        |
        v
+-------------------+
| Verify and        |
| write summary     |
+-------------------+
```

## Tiny Mental Model

DBxCarta is like a mapper.

Unity Catalog says, "Here are the tables and columns."

Spark says, "I will organize them into clean piles."

The transform steps say, "I will add helpful labels, sample values, vectors,
and relationships."

Neo4j says, "I will store it as a connected map."

The client later uses that map to find the right schema context for a user
question.
