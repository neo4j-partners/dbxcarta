# The Case for a Semantic Graph Layer Over Unity Catalog

A Unity Catalog with 1,000 tables contains several hundred thousand column definitions. An LLM asked to generate SQL against that catalog needs to know which twenty of those columns are relevant to the question — not all of them, because the context window cannot hold all of them, and not a random twenty, because the wrong schema produces wrong SQL.

That retrieval problem is what dbxcarta solves. Before examining what the graph layer provides, two simpler approaches deserve honest treatment, because the case for the graph layer only holds where those simpler approaches fail.

## Querying information_schema Directly

Unity Catalog's `information_schema` views expose tables, columns, data types, comments, and declared constraints. Everything dbxcarta ingests from UC originates there. The question is whether the information_schema layer can answer "which columns are relevant to this question?" without an intermediate graph.

It cannot, for two reasons. First, `information_schema` has no vector index. Identifying semantically relevant columns requires embedding both the question and the schema objects, then computing similarity. That computation happens outside of SQL, and the schema objects need to be embedded and indexed somewhere before query time. Second, the only alternative to selective retrieval is a full schema dump — every table name, column name, and data type packed into the system prompt. At 50 tables, this is uncomfortable. At 1,000 tables, it is not possible.

`information_schema` is the authoritative source of truth for UC metadata. It is not a retrieval system.

## What Genie Provides

Genie Spaces do perform semantic indexing of Unity Catalog schemas. A Genie Space embeds table and column metadata, uses those embeddings to identify relevant schema context for a natural language question, and generates SQL from the assembled context. The retrieval mechanism Genie uses internally is substantively similar to what dbxcarta builds explicitly.

Genie is the right answer when the requirement is a turnkey natural language SQL interface. For that use case, building dbxcarta is reinventing a managed service.

Genie becomes the wrong answer in two specific situations. The first is scale: Genie Spaces have a per-space table limit in the range of 100 to 200 tables. A 1,000-table catalog cannot fit in a single Space. Sharding across multiple Spaces requires routing logic to decide which Space to query for a given question, and that routing layer faces the same schema-relevance problem at a higher level of abstraction.

The second situation is composability. Genie is an end-to-end product: it takes a question and returns SQL or results. It does not expose the schema context it assembled internally. An agent system that needs the relevant schema nodes as structured input to its own prompt cannot extract that from Genie. The `ContextBundle` that `GraphRetriever` returns — a typed list of columns, sample values, and join predicates — is a retrieval component that can be injected into any prompt, logged, evaluated, or post-processed. Genie does not offer that interface.

## What the Graph Layer Provides

The Neo4j graph holds the full UC metadata as a typed knowledge graph: Database, Schema, Table, Column, and Value nodes connected by structural relationships. The properties that matter at query time are the embeddings on Table and Column nodes and the `REFERENCES` edges between Column nodes.

### Selective Retrieval at Scale

At query time, `GraphRetriever` embeds the user's question with the same model used to embed the schema, then probes two vector indexes simultaneously against all columns and tables in the graph. A 1,000-table catalog with 10,000 columns is one index. The probe returns the ten most similar column nodes. A single Cypher traversal then expands those seed nodes to their parent tables, finds adjacent tables via `REFERENCES` edges, and fetches all columns and sample values for the expanded set.

The result is a schema subgraph that is genuinely relevant to the question, assembled in two round trips, regardless of how many tables the catalog contains.

### Inferred Foreign Keys

Unity Catalog's `information_schema.table_constraints` contains declared constraints only. Most lakehouses have significant join structure that is never declared: column names follow naming conventions, types align, values overlap, but no `FOREIGN KEY` constraint exists. dbxcarta infers those relationships through two independent phases.

The metadata phase scores candidate pairs on name match (exact versus suffix), type compatibility, and target primary-key evidence. Each accepted pair becomes a `REFERENCES` edge with a confidence between 0.78 and 0.95 and `source="inferred_metadata"`. The semantic phase computes cosine similarity over column embeddings for pairs not covered by the metadata phase, with a value-overlap bonus when source values appear in the target column's samples. Each accepted pair becomes a `REFERENCES` edge with `source="semantic"`.

The edge properties matter. A retrieval system can filter on confidence threshold. An evaluation can compare the declared, metadata-inferred, and semantically-inferred edges as separate populations. A debugging session can inspect the literal join predicate stored in `criteria`. None of that is available from a black-box FK inference system.

### The Retrieval Payoff

A question like "which customers placed orders in the last 30 days?" gets embedded and probed against the column index. The seed set might return `orders.customer_id`, `orders.order_date`, and `customers.id` as the most similar columns. The traversal follows `REFERENCES` from `orders.customer_id` to `customers.id`, pulls all columns for both tables, and includes sample values for `order_date` to help the LLM construct the date filter correctly. The LLM receives a precise schema fragment rather than a 1,000-table dump or a guess.

## Graph Analytics for Table Selection

Vector similarity identifies columns that read like the question. It does not identify columns that belong together. For text-to-SQL against a thousand-table catalog, both signals matter. A probe for "orders placed last month" returns high-similarity hits on `order_date`, `created_at`, and `timestamp` columns spread across orders tables, audit logs, session tables, and telemetry streams. The model receives a seed set that spans unrelated subject areas, and the generated SQL either joins across domain boundaries that should not be joined or omits the bridge table that completes the intended query.

Once the inferred FK graph exists, community detection, centrality measures, and connectivity analysis add a structural prior that semantic similarity cannot supply on its own. The graph already sits in Neo4j; running GDS algorithms over it is additive work on top of an artifact the pipeline produces anyway.

### Community Detection for Domain-Bounded Retrieval

Louvain or Leiden on the inferred FK graph partitions the catalog into densely connected clusters that typically align with subject areas: sales, inventory, auth, billing. At query time, the top-K column probe produces a seed set, and the community containing the highest-confidence seed bounds the subsequent FK traversal. The LLM receives columns from one domain rather than a union across three, and cross-domain joins that the underlying business never intended stop being proposed.

This matters most for text-to-SQL at scale. A Genie Space is a manually scoped set of tables defined for a specific question domain. Community detection produces equivalent scoping from the graph itself, cold, and it produces as many partitions as the catalog has natural clusters. The right twenty tables for a question about returns are the ones in the same community as `returns.return_id`, not the ones whose column names happen to embed close to the question.

### Centrality as a Retrieval Prior

PageRank and degree centrality surface the tables that sit at the center of the join structure: customers, products, users, accounts. These are disproportionately join targets for questions across many subject areas. Boosting hub tables in the retrieval seed set is a one-parameter adjustment that pushes the model toward anchoring queries on canonical entity tables rather than on whichever fact or log table happened to score highest on lexical similarity.

### Structural Health Signals

Weakly connected components and articulation points do not participate in retrieval directly. They characterize the catalog. A catalog whose inferred FK graph resolves into one large component with a power-law degree distribution looks like a real schema. One that fragments into many small components suggests either a data architecture with genuine silos or an FK inference pipeline that is missing real relationships. Articulation points identify bridge tables whose removal disconnects the graph, which is a useful artifact for change-impact analysis independent of text-to-SQL.

### Near-Duplicate Detection

Node embeddings (FastRP, node2vec) on the FK graph produce structural vectors that capture a table's role in the schema. A table that sits adjacent to customers and orders with a characteristic degree pattern plays a similar role to another such table, even if their names and columns differ. For catalogs assembled from multiple ingestion paths, parallel dev and prod copies, or migrations in flight, this surfaces consolidation candidates that neither name-based nor content-based matching would catch.

### Evaluation

The client evaluation harness already measures SQL execution rates across three retrieval arms against a curated questions file with `reference_sql`. Graph-analytic retrieval modes drop into that harness as additional arms: community-bounded top-K, PageRank-boosted seed, and the existing global top-K as baseline. Differences in execution rate and reference-SQL match rate across arms answer the question that cannot be answered by inspecting the graph alone: does the structural signal actually improve table selection, or does vector similarity over well-written column descriptions already capture what the graph encodes?

A catalog at target scale is a precondition for this evaluation. On a small fixture with twenty tables, communities are too sparse to form, centrality distributions are too flat, and the arms converge. The signal is meaningful only when the catalog is large enough that flat top-K starts making the table-selection mistakes that graph structure can correct.

## Where This Does Not Apply

For catalogs under roughly 50 tables, the schema dump approach works. The full schema fits in a prompt, and the operational cost of maintaining a Neo4j graph, keeping embeddings current, and running an ingestion pipeline on a schedule is not justified by the retrieval improvement.

For use cases where Genie's end-to-end SQL generation is sufficient, dbxcarta is unnecessary. The graph layer is a component for systems that need to control what schema context reaches the LLM and how it gets there.

The ingestion pipeline also introduces a synchronization gap: the Neo4j graph reflects the state of Unity Catalog at the time of the last pipeline run. Schema changes in UC are invisible to the retriever until the next run. For catalogs that change schema structure frequently, the pipeline schedule must be short enough that stale graph state does not produce retrieval errors.

## Summary

dbxcarta addresses a retrieval problem that exists at the scale where a full schema dump is impractical and where Genie's per-space table limit becomes a constraint. The graph layer provides selective schema retrieval via vector search, inferred foreign key relationships with confidence scores and provenance, and a structured retrieval interface that downstream agent systems can compose with rather than consume wholesale. For catalogs of 1,000 or more tables, these properties are not available from Unity Catalog's metadata views or from Genie alone.

## Prior Art

The combination of a knowledge graph schema representation with vector search for text-to-SQL retrieval is an established pattern. Several published systems cover this ground.

CSR-RAG (Singh et al., Nokia Bell Labs, January 2026) is the closest structural analog. It separates retrieval into three parallel components: Contextual RAG (embedding-based similarity over prior questions), Structural RAG (a knowledge graph of schema objects), and Relational RAG (hypergraph ranking over FK join paths). It reports over 80% recall on a 246-table enterprise database. The critical difference is that CSR-RAG operates only from declared foreign keys and applies no graph analytics algorithms — no community detection, no centrality measures, no node embeddings on the FK graph itself.

The kennethleungty/Text-to-SQL-with-KG-Neo4j-GraphRAG project (February 2025) also represents a SQL schema as a Neo4j knowledge graph and uses GraphRAG for retrieval, but works on a small financial dataset and does not use the Neo4j Graph Data Science library or FK inference.

Pinterest's Analytics Agent (March 2026) operates at a larger scale — 100,000 tables — but takes a fundamentally different approach. It encodes historical analyst queries as intent embeddings and extracts structural patterns (join keys, filters, aggregations) from query execution history. It uses governance metadata and query co-occurrence frequency as ranking signals. There is no FK inference and no graph algorithm layer; the structural signal comes from observed query behavior rather than from the schema graph itself.

Amazon's RASL system uses retrieval-augmented schema linking with vector indexes over schema objects. FalkorDB and several academic papers use knowledge graphs for multi-hop SQL join resolution. None apply GDS algorithms to the schema graph.

A specific search for community detection (Louvain, Leiden) and centrality measures (PageRank, degree) applied to database schemas for text-to-SQL retrieval returned no prior work. The two claims that appear unoccupied in the literature are: (1) using community detection on the inferred FK graph to partition the catalog into subject-area clusters and bound FK traversal to the community of the highest-confidence seed column; and (2) using PageRank or degree centrality as a retrieval prior to promote canonical hub tables — customers, products, accounts — in the seed set. The FK inference pipeline itself, specifically the two-phase approach combining metadata scoring with semantic cosine similarity and value overlap, is more systematic than documented prior work, though FK inference as a category has prior art in database reverse engineering research.

The novelty claim for dbxcarta is therefore scoped: the knowledge-graph-plus-vector-search foundation has prior art, most directly in CSR-RAG. The differentiating contribution is the GDS layer — community detection and centrality analysis on the inferred FK graph as retrieval signals that vector similarity alone cannot supply.

## References

- Singh, R., Boškov, N., Drabeck, L., Gudal, A., & Khan, M. A. (2026). *CSR-RAG: An Efficient Retrieval System for Text-to-SQL on the Enterprise Scale*. arXiv:2601.06564. https://arxiv.org/abs/2601.06564
- Leung, K. (2025). *Text-to-SQL with GraphRAG on Knowledge Graph Semantic Representation of SQL Databases*. GitHub. https://github.com/kennethleungty/Text-to-SQL-with-KG-Neo4j-GraphRAG
- Li, K., & Yang, B. (2026). *Unified Context-Intent Embeddings for Scalable Text-to-SQL*. Pinterest Engineering Blog. https://medium.com/pinterest-engineering/unified-context-intent-embeddings-for-scalable-text-to-sql-793635e60aac
- Amazon Science. (2024). *RASL: Retrieval-Augmented Schema Linking for Massive Database Text-to-SQL*. https://assets.amazon.science/1b/95/8f62e89647348f4c4836f6c3040d/rasl-retrieval-augmented-schema-linking-for-massive-database-text-to-sql.pdf
- FalkorDB. (2024). *Text-to-SQL with Knowledge Graphs: Multi-Hop Queries*. https://www.falkordb.com/blog/text-to-sql-knowledge-graphs/
- Sequeda, J., Allemang, D., & Jacob, B. (2024). *A Benchmark to Understand the Role of Knowledge Graphs on Large Language Model's Accuracy for Question Answering on Enterprise SQL Databases*. Proceedings of the 7th Joint Workshop on Graph Data Management Experiences & Systems (GRADES) and Network Data Analytics (NDA). https://doi.org/10.1145/3661304.3661901
