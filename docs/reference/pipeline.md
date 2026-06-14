# The ingest pipeline (now in neocarta)

The Spark ingest pipeline that reads Unity Catalog metadata and writes the Neo4j
semantic layer no longer lives in dbxcarta. It moved to
[neocarta](https://github.com/neo4j-field/neocarta), where it ships as the
`neocarta` connector wheel (the `databricks-spark` extra). dbxcarta stages that
wheel and submits it as the ingest job; it does not carry the pipeline source.

The stage-by-stage walkthrough (extract, transform, embed, FK discovery, the
Neo4j write) and the graph contract are documented in neocarta, under
`neocarta/connectors/databricks/`. The contract enums and identifier rules live
in `neocarta.connectors.databricks.contract` and
`neocarta.connectors.utils.generate_id`.

What stays in dbxcarta:

- **How the job is run and tuned.** The operator flow that stages the wheel and
  submits the job, the inline-embedding configuration, and the project design
  rules that constrain the run are in
  [`architecture.md`](architecture.md#building-the-layer) and
  [`best-practices.md`](best-practices.md).
- **How the graph is consumed.** The client retrieval and Text2SQL evaluation
  that read the graph at query time are in
  [`architecture.md`](architecture.md#how-we-validate).

For the inline vs. external embedding modes and the deferred external path, see
Phase 7 of the alignment plan.
