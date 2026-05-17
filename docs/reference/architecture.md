# DBxCarta Architecture

Unity Catalog publishes tables, columns, and declared constraints. It does not
publish meaning. It cannot say which tables sit semantically near a question,
which relationships exist beyond the foreign keys someone remembered to declare,
or which medallion layer holds the trustworthy version of an entity. An agent
asked to write SQL over several hundred tables spread across layered catalogs
has names and structure but no navigable model of what any of it means.

DBxCarta builds that model in Neo4j: a semantic layer over Unity Catalog. The
semantic layer turns flat catalog metadata into a graph a question can traverse,
where similarity is an embedding distance, a relationship is a confidence-scored
edge rather than only a declared key, and the medallion layer of every table is
a queryable property. Text-to-SQL is the workload that exercises it, and the
evaluation harness is how the project proves the layer retrieves the right
slice instead of asserting that it does.

The semantic layer is only useful if it faithfully reflects the catalog it
describes. That fidelity drives the rest of the design: the data being mapped,
the semantic layer derived from it, and the bookkeeping that records what each
run did are kept in three separate storage planes so the layer never indexes
the tool's own exhaust.

## Three storage planes

```
                          UNITY CATALOG (the subject)
   ┌───────────────────────────────────────────────────────────────────┐
   │  graph-enriched-finance-bronze   :bronze                           │
   │  graph-enriched-finance-silver   :silver   (DBXCARTA_CATALOG anchor)│
   │  graph-enriched-finance-gold     :gold                             │
   │     information_schema  +  sampled values        read-only         │
   └───────────────────────────────────────────────────────────────────┘
            │ extract metadata across DBXCARTA_CATALOGS
            │ layer = DBXCARTA_LAYER_MAP[catalog]
            ▼
   ┌──────────────────────────┐        ┌──────────────────────────────┐
   │   dbxcarta-spark         │        │  NEO4J  (the semantic layer)  │
   │   build pipeline         │ write  │  Database─HAS_SCHEMA→Schema    │
   │   preflight → extract →  ├───────►│  ─HAS_TABLE→Table─HAS_COLUMN→  │
   │   graph DFs → embed →    │ fail-  │  Column ; Table.layer (v1.1)   │
   │   sample → FK discover   │ closed │  REFERENCES{confidence}        │
   └────────────┬─────────────┘        │  vector indexes on embeddings │
                │                       └───────────────┬──────────────┘
                │ run summary                  retrieve  │
                ▼                                         ▼
   ┌───────────────────────────────┐        ┌───────────────────────────┐
   │  OPS CATALOG  dbxcarta-catalog │        │   dbxcarta-client          │
   │  (operational metadata only)   │◄───────┤   validation harness       │
   │  dbxcarta_run_summary          │ summary│   arms: reference,         │
   │  client_staging_<arm>  (cache) │  +     │   no_context, schema_dump, │
   │  DBXCARTA_SUMMARY_VOLUME       │  cache │   graph_rag                │
   └───────────────────────────────┘        └───────────────────────────┘
```

The two flows that matter are the downward build path on the left, where Unity
Catalog metadata becomes a semantic graph, and the retrieve path on the right,
where the client queries that graph to answer questions and writes only
operational results back to the ops catalog. The data plane is never written by
DBxCarta. The semantic layer is never polluted by the tool's own records.

### The data plane: Unity Catalog, read-only

The catalogs under evaluation are the source of truth and DBxCarta only reads
them. For a medallion layout each layer is its own catalog. The finance-genie
example uses `graph-enriched-finance-bronze`, `-silver`, and `-gold`.
`DBXCARTA_CATALOG` names a single anchor catalog used for preflight, verify, and
ops provisioning; `DBXCARTA_CATALOGS` lists every catalog folded into one
semantic layer; `DBXCARTA_LAYER_MAP` carries `catalog:layer` pairs so the layer
records which medallion tier each table came from. The build reads
`information_schema` and a bounded sample of values and writes nothing here, so
the meaning the layer encodes stays anchored to what the data team published.

### The semantic layer: Neo4j

The graph is the product. It treats the catalog as meaning rather than as a
list. A `Database` node per catalog fans out through `HAS_SCHEMA`, `HAS_TABLE`,
and `HAS_COLUMN` to `Column` nodes, so structure is traversable instead of
implied by naming. Three enrichments turn that structure into a semantic layer.
Embeddings on table and column nodes, backed by vector indexes, make "near in
meaning" a query a question can run. `REFERENCES` edges carry a confidence score
and are discovered from declared constraints, metadata heuristics, and semantic
inference, so a join path the catalog never declared is still navigable and
ranked by how much to trust it. The `layer` property under graph contract v1.1
records each table's medallion tier, so a retriever can prefer the curated gold
table over a raw bronze one when both could answer a question.

This plane is the one the client reads at query time, and it is regenerable by
design. Because Unity Catalog is the source of truth, the semantic layer can be
rebuilt from it at any time, which is why the build write boundary is
fail-closed rather than incremental: a partial graph is a wrong graph, and a
wrong semantic layer is worse than a missing one.

### The ops plane: dbxcarta-catalog

Every artifact DBxCarta produces about its own runs lands in a separate
operational catalog, never in a data catalog and never in the graph. Its
location is derived from the catalog and schema of `DBXCARTA_SUMMARY_TABLE`; for
finance-genie that resolves to `dbxcarta-catalog`. It holds the run summary
table `dbxcarta_run_summary`, the per-arm generation cache tables
`client_staging_<arm>`, and the summary volume named by
`DBXCARTA_SUMMARY_VOLUME`. The isolation protects the semantic layer's fidelity.
Pipeline-owned artifacts record what happened without becoming part of the
subject. Writing a run summary or a generation cache into a data catalog would
inject DBxCarta's own bookkeeping into the `information_schema` that the next
build reads, and the semantic layer would start encoding meaning about its own
exhaust. The ops catalog is the boundary that keeps the layer a model of the
catalog and nothing else.

## Building the layer

The build path is server-side and runs in Spark. `dbxcarta-spark` reads Unity
Catalog metadata across the resolved catalogs, builds graph-shaped DataFrames,
adds embeddings through `ai_query`, samples values, discovers foreign keys
through declared constraints and metadata and semantic inference, and writes the
semantic layer to Neo4j behind a fail-closed boundary, then records its run
summary in the ops catalog. The pipeline internals are documented in
[`pipeline.md`](pipeline.md); the rules that constrain it, including why rule
logic is native Spark and never a Python UDF, are in
[`best-practices.md`](best-practices.md). One current limitation: post-write
verify keys off the single anchor catalog, so in a multi-catalog build the other
catalogs are written but not independently verified.

### How we validate

A semantic layer is only worth building if a question is answered better by
traversing it than by ignoring it, so `dbxcarta-client` runs a held-out
question set through four arms and scores them the same way: parsed, executed,
non-empty, and matched against a reference result. `graph_rag` is the semantic
layer in use, seeding from vector similarity on the question and expanding along
confidence-ranked `REFERENCES` edges to retrieve a focused subgraph. The other
three are controls. `reference` executes the ground-truth SQL to establish what
a correct answer returns. `no_context` gives the model only the catalog name and
sets the floor. `schema_dump` pastes a bounded schema, capped near two thousand
tokens and pulled from the graph across every resolved catalog, so the control
sees bronze, silver, and gold rather than only the anchor. `graph_rag` justifies
the build pipeline only if it beats `schema_dump` at this matched token budget
and both clear `no_context`; the cap is what turns `schema_dump` from a "paste
everything" strawman into a real competitor for the same budget.

The harness caches generation so iterating on retrieval and prompts does not pay
model latency every run. Each arm generates SQL for all questions in one batched
`ai_query` pass, materialized to a `client_staging_<arm>` Delta table in the ops
catalog that doubles as the cache. A write stamps an `_input_hash` over the
ordered endpoint, arm, and question prompts; an unchanged re-run reads the prior
responses and skips inference, while any change to a prompt, the retrieved
context, the question set, or the endpoint name forces fresh generation.
`DBXCARTA_CLIENT_REFRESH=true` covers the one case the hash cannot see, a model
swap behind a stable endpoint name. The tables use stable names and overwrite
mode, so logical size stays at one run's worth of rows per arm; Delta retains
tombstoned files until a `VACUUM`, and at evaluation scale that is a deliberate
non-decision rather than a missing retention policy.

## What this architecture costs

A semantic layer that lives outside Unity Catalog is only as current as the last
build, and nothing the client does works until that build has run. The
fail-closed write boundary makes the layer a clean rebuild rather than a live
mirror, which is correct when the layer's value is fidelity to a published
catalog and wrong for a system that needs the index to track schema changes in
real time. The single-anchor verify limitation means a multi-catalog build
trusts that the non-anchor catalogs wrote correctly.

The trajectory the architecture is built for is the layer getting richer.
Declared keys give the first edges; metadata and semantic inference add the ones
no one declared, each carrying a confidence the retriever can weigh. Embeddings
make tables findable by meaning rather than by name. The layer property lets the
retriever prefer the curated answer over the raw one. Each build can encode more
of the catalog's meaning than the last, and the validation harness is what keeps
that enrichment honest: a semantic layer is only worth building if a question
can be answered better by traversing it than by ignoring it.
