# Documentation

The root [`README.md`](../README.md) covers what dbxcarta is, what it produces,
and how to run the demo. This page maps the rest of the docs and suggests an
order to read them in.

## Start here

A reading path from why the project exists to how it is built:

1. [Root README](../README.md): what dbxcarta is, what it produces, and how to run the demo.
2. [explanation/why-semantic.md](explanation/why-semantic.md): why build a graph layer over Unity Catalog at all.
3. [reference/architecture.md](reference/architecture.md): the three storage planes, the package tiers, the graph contract the client reads, and how the layer is validated.
4. [reference/best-practices.md](reference/best-practices.md): the design rules that constrain the pipeline.

## Tutorials

Step-by-step guides for common tasks.

- [tutorials/add-a-data-source.md](tutorials/add-a-data-source.md): add a new integration end to end, covering the overlay, getting data into Unity Catalog, ingest, and how to test and verify it.

## Reference

Stable technical references for the current system.

- [reference/architecture.md](reference/architecture.md): system design covering the storage planes, package tiers, build path, the graph contract the client reads, the validation model, and the client cache mechanics.
- [reference/best-practices.md](reference/best-practices.md): the authoritative pipeline design rules with cited sources, spanning Spark, the Neo4j connector, and project principles.
- [reference/operational-lessons.md](reference/operational-lessons.md): lessons learned from running the pipeline.

## Explanation

Conceptual background and the rationale for the project.

- [explanation/why-semantic.md](explanation/why-semantic.md): the case for a Neo4j semantic layer over querying `information_schema` directly or curating a Genie space, with prior art.
- [explanation/fk-discovery.md](explanation/fk-discovery.md): FK discovery as the core differentiator, and when to use it versus a curated Genie space.

## Security

- [security/supply-chain.md](security/supply-chain.md): supply-chain controls including lockfiles, immutable installs, version quarantine, and release-workflow checks.

## Proposals

In-progress design work. These describe intended or partially landed changes, not the settled current system. The `proposals/` directory is gitignored local working notes, so these files may not be present in a fresh checkout.

- [proposals/fix-embeddings.md](proposals/fix-embeddings.md): diagnosis and proposed fix for weak `graph_rag` retrieval on comment-less, multi-schema data.

## Semantics

Forward-looking positioning and proposals not yet implemented.

- [semantics/semantic-comparison.md](semantics/semantic-comparison.md): where dbxcarta sits in the enterprise ontology landscape.
- [semantics/strong-owl.md](semantics/strong-owl.md): a proposal for an OWL/SHACL policy layer at ingest and query time.

## Other

- [schemapile/README.md](schemapile/README.md): notes on the generated SchemaPile context artifacts.
- `assets/`: diagrams and other visual assets used by the docs.
