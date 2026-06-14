# dbxcarta Graph Schema

The Neo4j graph contract is **owned by [neocarta](https://github.com/neo4j-field/neocarta)**,
the connector that writes the graph. dbxcarta no longer carries the pipeline, so
it no longer maintains its own copy of the contract; doing so would only let the
two drift. neocarta is authoritative for the exact node labels, properties,
relationship types, indexes, identifier rules, and the current contract version.

In neocarta the contract is defined in code and kept in sync with neocarta's own
schema documentation:

- Labels, relationship types, edge-source values, and the contract version:
  `neocarta.connectors.databricks.contract`
- Identifier generation (`compose_id`, `generate_value_id`):
  `neocarta.connectors.utils.generate_id`, with the equivalent Spark SQL
  expressions in `neocarta.connectors.databricks.ingest.contract_expr` so ids are
  byte-identical on the driver and on executors
- The written-out schema reference: `schema.md` in the neocarta project

## The shape, in one paragraph

`dbxcarta-client` reads this contract at query time. A `Database` node per
catalog fans out through `HAS_SCHEMA`, `HAS_TABLE`, and `HAS_COLUMN` to `Column`
nodes; sampled values hang off columns via `HAS_VALUE`; and confidence-scored
`REFERENCES` edges connect columns across declared and inferred foreign keys.
`Table`, `Column`, and the higher container nodes carry an `embedding` where the
corresponding include-embeddings flag is on, backed by vector indexes, and
`Table` nodes carry a medallion `layer` property. For the precise per-node
property list, nullability, embedding dimensions, and the current
`contract_version`, consult neocarta as above.

## Why dbxcarta still cares

The client's retrieval logic (`dbxcarta-client`) is the reader side of this
contract. When neocarta changes the contract, the client's vector-index names,
property reads, and traversal paths must stay in step. The consuming side of the
contract is described in
[`../reference/architecture.md`](../reference/architecture.md#how-we-validate).
