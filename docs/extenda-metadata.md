# Extending dbxcarta for Unity Catalog External Metadata

## Can dbxcarta Support External Metadata Objects?

Yes. `dbxcarta` can be extended to ingest Unity Catalog External Metadata
objects, but that would be a new extractor path. It is not supported by the
current pipeline.

Today, `dbxcarta` reads Unity Catalog metadata through `information_schema`:

- `information_schema.schemata`
- `information_schema.tables`
- `information_schema.columns`
- declared constraints and key-column usage for foreign key discovery

That means it discovers UC tables, columns, schemas, constraints, comments, and
sample values. It does not currently call the External Metadata API:

```text
GET /api/2.0/lineage-tracking/external-metadata
```

## Why This Matters

The Neo4j and Unity Catalog federation validation has two metadata paths:

- Materialized metadata tables: Neo4j labels and relationship types are written
  as Delta tables in Unity Catalog.
- External Metadata API: Neo4j labels and relationship types are registered as
  metadata-only external entities in Unity Catalog.

`dbxcarta` can already ingest the first path because those objects appear in
`information_schema.tables` and `information_schema.columns`.

`dbxcarta` cannot currently ingest the second path because External Metadata
objects are not exposed through the `information_schema` views that the pipeline
reads today.

## Proposed Extension

Add a second extractor alongside the existing `information_schema` extractor:

```text
Unity Catalog External Metadata API -> dbxcarta graph DataFrames -> Neo4j semantic graph
```

The extractor would:

1. Call `GET /api/2.0/lineage-tracking/external-metadata`.
2. Filter to supported external systems and entity types.
3. Convert each external metadata object into dbxcarta graph nodes.
4. Reuse the existing embedding, staging, write, summary, and verification
   patterns where possible.

Example configuration:

```text
DBXCARTA_INCLUDE_EXTERNAL_METADATA=true
DBXCARTA_EXTERNAL_METADATA_SYSTEM_TYPES=OTHER
DBXCARTA_EXTERNAL_METADATA_ENTITY_TYPES=NodeLabel,RelationshipType
```

## Mapping Options

### Option 1: Reuse Existing Table and Column Nodes

Map External Metadata objects into the existing graph contract:

- `entity_type=NodeLabel` -> `Table`
- `entity_type=RelationshipType` -> `Table`
- `columns[]` -> `Column`
- `properties.neo4j.*` -> node and column properties

This is the smallest implementation. It lets the existing GraphRAG retriever
work with fewer changes because it already understands `Table`, `Column`,
`HAS_COLUMN`, and embeddings on those nodes.

The downside is semantic accuracy. External Metadata objects are not necessarily
queryable SQL tables. If the LLM sees them as normal tables, it may generate SQL
against objects that are catalog metadata only.

### Option 2: Add External Entity Nodes

Extend the graph contract with explicit external metadata labels:

- `ExternalSystem`
- `ExternalEntity`
- `ExternalField`

And relationships:

- `(:ExternalSystem)-[:HAS_EXTERNAL_ENTITY]->(:ExternalEntity)`
- `(:ExternalEntity)-[:HAS_EXTERNAL_FIELD]->(:ExternalField)`

For Neo4j:

- `NodeLabel` becomes an `ExternalEntity`
- `RelationshipType` becomes an `ExternalEntity`
- `columns[]` becomes `ExternalField`
- Neo4j-specific properties remain attached as metadata

This is more accurate and avoids pretending metadata-only objects are SQL
tables. It also gives downstream agents a clear signal that these objects are
governance and discovery artifacts, not directly queryable relations.

The tradeoff is that the retriever and prompt assembly code must learn how to
include `ExternalEntity` and `ExternalField` context.

## Recommended Direction

Use explicit external metadata nodes.

`dbxcarta` is a semantic graph layer, not only a table catalog mirror. Preserving
the distinction between queryable UC tables and metadata-only external graph
entities keeps the semantic layer honest.

The recommended graph model is:

```text
(:Database {source: "unity_catalog"})
  -[:HAS_SCHEMA]->(:Schema)
  -[:HAS_TABLE]->(:Table)
  -[:HAS_COLUMN]->(:Column)

(:ExternalSystem {name: "neo4j"})
  -[:HAS_EXTERNAL_ENTITY]->(:ExternalEntity {entity_type: "NodeLabel"})
  -[:HAS_EXTERNAL_ENTITY]->(:ExternalEntity {entity_type: "RelationshipType"})
  -[:HAS_EXTERNAL_FIELD]->(:ExternalField)
```

External entities can still be embedded and retrieved semantically, but the
retrieval bundle can mark them as external context instead of SQL context.

## What This Would Prove

Extending `dbxcarta` this way would prove that:

- Unity Catalog External Metadata can become part of the same GraphRAG semantic
  layer as UC tables and columns.
- Neo4j labels and relationship types can be discovered from UC's metadata
  plane without re-querying Neo4j directly.
- The semantic graph can represent both queryable lakehouse assets and
  metadata-only external assets.
- Agents can retrieve graph schema context alongside SQL schema context while
  preserving which objects are directly queryable.

It would not prove SQL query federation by itself. Query federation remains a
separate validation path through JDBC, `remote_query()`, or materialized Delta
tables.

## Implementation Sketch

Add a module such as:

```text
src/dbxcarta/external_metadata.py
```

Responsibilities:

- Fetch External Metadata objects from the workspace API.
- Normalize API rows into Spark DataFrames.
- Build `ExternalSystem`, `ExternalEntity`, and `ExternalField` node DataFrames.
- Build `HAS_EXTERNAL_ENTITY` and `HAS_EXTERNAL_FIELD` relationship DataFrames.
- Preserve `external_metadata_id`, `system_type`, `entity_type`, `url`,
  `description`, and source-specific `properties`.

Then update:

- `contract.py` with new node labels and relationship types.
- `pipeline.py` to optionally call the external metadata extractor.
- `neo4j_io.py` to create constraints and write the new labels.
- `summary.py` to report external metadata counts.
- `client/graph_retriever.py` to include external entities as non-SQL context.
- Verification checks to assert no orphan external fields or entities.

The first useful milestone should ingest External Metadata without changing SQL
generation. The retriever can expose external entities in a separate
`external_context` section so prompt consumers decide how to use them.
