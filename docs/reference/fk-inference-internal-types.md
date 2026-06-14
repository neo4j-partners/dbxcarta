# FK-Inference Internal Types (now in neocarta)

The typed dataclass layer between Spark DataFrames and the FK discovery
pipeline, and the boundary-conversion points it documented, moved to
[neocarta](https://github.com/neo4j-field/neocarta) with the rest of the ingest
pipeline. dbxcarta no longer carries those modules (the FK dataclass layer and
the run-summary counter types), so it no longer documents their internal types.

In neocarta the FK discovery and its typed layer live under
`neocarta/connectors/databricks/`, with the contract enums in
`neocarta.connectors.databricks.contract`. See neocarta's own documentation for
the current type map, invariants, and boundary-conversion rules.

What stays in dbxcarta:

- **Why FK discovery works the way it does** (the conceptual trade-offs) is in
  [`../explanation/fk-discovery.md`](../explanation/fk-discovery.md) and
  [`design-decisions.md`](design-decisions.md). These describe the approach the
  neocarta pipeline implements, not dbxcarta-owned code.
- **The design rules the pipeline must follow** (native Spark over Python UDFs,
  no driver-side catalog collection) are in
  [`best-practices.md`](best-practices.md).
