# Carta Comparison: dbxcarta vs neocarta

## Overview

Both projects solve the same problem: large data catalogs are too big for an LLM to reason about directly, so they turn database metadata into a Neo4j knowledge graph that an AI agent can search semantically to find the right tables before generating SQL. They differ in platform, in how data gets into the graph, in how relationships between tables are discovered, and in how the result is packaged and consumed.

**dbxcarta** is built for Databricks. It runs a single Spark job that reads Unity Catalog metadata, builds the graph in bulk, and writes it to Neo4j all in one server-side submission. It is opinionated about discovering hidden relationships: it actively infers foreign keys that the catalog never declared, using both naming or type heuristics and embedding similarity, and it scores every relationship with a confidence value. It ships as two Python packages, one for the ingest pipeline and one for query-time retrieval, plus an evaluation harness that proves the graph beats a plain schema dump.

**neocarta** is a general-purpose, source-agnostic ETL library. It connects to BigQuery, Dataplex, CSV files, and query logs, pulls the metadata into memory, transforms it into validated models, and loads it into Neo4j in a classic Extract-Transform-Load flow. It relies on relationships that already exist in the source (declared foreign keys, business-glossary tags, real query-log join patterns) rather than guessing new ones. It is published as a single pip-installable library with optional add-ons for a CLI, an MCP server, and a Rust speed-up, and it includes a ready-to-run text-to-SQL agent.

## What dbxcarta provides

- **Databricks-native ingest:** Runs entirely inside Databricks as one Spark job. There is no local extraction step and no external orchestration; the whole build runs server-side and submits once.
- **Bulk Spark pipeline to Neo4j:** Spark builds graph-shaped tables of nodes and relationships, then the Neo4j Spark Connector bulk-loads them with merge semantics. The flow is build, embed, infer, load, verify.
- **Active foreign-key discovery with confidence scores:** Three independent sources produce relationship edges. Declared catalog constraints score 1.0, metadata inference from column naming and type patterns scores in the high band, and semantic inference from embedding similarity scores in the mid band with a bonus when sampled values overlap. Every edge records its confidence, its source, and the literal join predicate when one can be derived.
- **Semantic matching of columns:** When naming and type rules do not connect two columns, it compares their embeddings to suggest joins the catalog never declared.
- **Medallion-layer awareness:** Tables are tagged bronze, silver, or gold so retrieval can prefer curated tables over raw ones, and multiple catalogs fold into one unified graph.
- **Two published Python packages:** `dbxcarta-spark` owns the ingest pipeline and operational CLI; `dbxcarta-client` owns the query-time retrieval, schema-context assembly, and the evaluation harness.
- **GraphRAG retrieval at query time:** A question is embedded, vector search finds the most similar tables and columns, the graph is traversed across confidence-filtered relationship edges, and a focused subgraph of tables, columns, sample values, and join predicates is assembled for the LLM.
- **Built-in evaluation harness:** Runs four arms (ground-truth reference, no-context floor, token-matched schema dump, and graph-RAG) so the graph only ships if it beats a plain schema dump at the same token budget.
- **Three isolated storage planes:** Unity Catalog is read-only source data, Neo4j is the regenerable semantic layer, and a separate operations catalog holds run summaries and caches so pipeline exhaust never contaminates the indexed schema.

## What neocarta provides

- **Source-agnostic ETL library:** A common Extract-Transform-Load pattern works across BigQuery, Google Dataplex, CSV files, and query-log files. Each connector chains an extractor, a transformer, and a loader.
- **Load-then-transform-in-memory flow:** Metadata is pulled from the source, validated into typed models in memory, then bulk-written to Neo4j with the Python driver. An optional Rust accelerator speeds up large bulk loads.
- **Relationships from existing sources, not guesses:** Foreign keys come from declared constraints in the source schema. It does not auto-match columns by name similarity alone.
- **Business-glossary bridging:** Columns and tables can be tagged with shared business terms from glossaries, so tables that use different naming conventions still connect through a common concept. This is its semantic path to relating data across systems.
- **Query-log usage patterns:** Parsing real SQL from query logs reveals which tables and columns are actually joined in production, surfacing relationships through usage.
- **Single pip-installable library with optional extras:** Install the base library, then add a Click CLI, an MCP server, or the Rust performance extension as needed.
- **MCP server with auto-selected retrieval:** At startup it probes the Neo4j database and registers the best available retrieval tool, preferring business-term hybrid, then hybrid, then vector-only or full-text-only, so agents get the strongest search without manual setup.
- **Multiple retrieval modes:** Vector similarity search, Lucene full-text search, and a hybrid mode that merges both, plus simple catalog listing tools.
- **Ready-made text-to-SQL agent:** A LangGraph agent chains neocarta's schema retrieval with a BigQuery executor so a user can ask a natural-language question and get query results back end to end.

## Side-by-side comparison

| Dimension | dbxcarta | neocarta |
|---|---|---|
| Platform | Databricks / Unity Catalog | Source-agnostic (BigQuery, Dataplex, CSV, query logs) |
| Ingest style | Single server-side Spark job, bulk load | Classic Extract-Transform-Load, in-memory transform then bulk load |
| Where work runs | Entirely inside Databricks, one submission | Local or notebook process calling source APIs |
| FK discovery | Active inference: declared + naming/type rules + embedding similarity | Existing only: declared constraints + business terms + query-log usage |
| Semantic column matching | Yes, embedding similarity suggests undeclared joins | Indirect, through shared business-term tags |
| Confidence scoring on edges | Yes, every edge scored and source-tagged | No confidence scoring |
| Layer awareness | Yes, bronze/silver/gold tagging | Not a core feature |
| Packaging | Two packages: ingest (`dbxcarta-spark`) and client (`dbxcarta-client`) | One library plus optional CLI, MCP, and Rust extras |
| Consumer interface | Python retriever API + evaluation harness | CLI, MCP server, and a bundled LangGraph agent |
| End-to-end text-to-SQL | Retrieval + eval framework, no bundled SQL generator | Yes, ready-to-run agent generates and executes SQL |
| Retrieval modes | Vector search then graph traversal | Vector, full-text, and hybrid, auto-selected |
| Built-in evaluation | Yes, four-arm harness vs schema dump | Full schema dump tool for evaluation only |

## Diagrams

### Ingest data flow

**dbxcarta** runs one Spark job that does everything server-side, including active FK inference:

```
Unity Catalog (read-only)
        |
        v
  [ Spark job, single submission ]
        |
   build graph-shaped DataFrames (nodes + relationships)
        |
   embed node text  -->  sample column values
        |
   infer foreign keys
     . declared constraints      (score 1.0)
     . naming / type rules       (high band)
     . embedding similarity      (mid band, +value overlap)
        |
   bulk write via Neo4j Spark Connector (MERGE)
        |
   verify counts + emit run summary
        v
     Neo4j graph (with vector indexes)
```

**neocarta** runs a per-source Extract-Transform-Load chain that uses relationships already present in the source:

```
BigQuery / Dataplex / CSV / Query logs
        |
   [ EXTRACT ] read metadata via APIs / schema introspection
        |
   [ TRANSFORM ] validate into typed models in memory
        |
   [ LOAD ] bulk write nodes + relationships (Neo4j driver, optional Rust speed-up)
        |        . declared FKs from source schema
        |        . business-term tags from glossaries
        |        . join patterns from query logs
        |
   [ ENRICH ] embed descriptions
        |
   [ INDEX ] vector + full-text indexes
        v
     Neo4j graph
```

### GraphRAG and consumer flow

**dbxcarta** exposes a Python retriever and an evaluation harness. The client embeds the question, searches, traverses the graph, and hands a focused subgraph to the LLM:

```
User question
     |
   embed question
     |
   vector search Neo4j  ->  top similar tables + columns
     |
   traverse REFERENCES edges (confidence-filtered)
     |
   assemble context: tables, columns, sample values, join predicates
     |
     v
   LLM generates SQL
     |
   (evaluation harness scores graph-RAG vs schema dump at same token budget)
```

**neocarta** exposes an MCP server and a bundled agent. The agent retrieves schema through MCP, generates SQL, and executes it against the source:

```
User question (natural language)
     |
   LangGraph agent
     |
   neocarta MCP: vector / full-text / hybrid search
     |
   return relevant tables + columns + foreign keys + business terms
     |
   LLM generates SQL
     |
   BigQuery MCP executes the query
     |
     v
   formatted results back to user
```

## Interoperability

The two projects are close enough to be cousins: they write the same core graph shape. Both use the node labels `Database`, `Schema`, `Table`, `Column`, and `Value`, the same relationships `HAS_SCHEMA`, `HAS_TABLE`, `HAS_COLUMN`, `HAS_VALUE`, and `REFERENCES`, the same `embedding` property name for vectors, and cosine-similarity vector indexes. That shared backbone makes a tempting idea: let neocarta act as the query-time client for a graph that dbxcarta builds, since neocarta already ships an MCP server, hybrid retrieval, and a bundled text-to-SQL agent that dbxcarta's client layer does not include.

It does not work out of the box. Three concrete mismatches block it today.

| What | dbxcarta writes | neocarta's retriever expects | Result |
|---|---|---|---|
| Vector index names | `table_embedding`, `column_embedding` | `table_vector_index`, `column_vector_index` | "index does not exist" |
| Column properties | `data_type`, `is_nullable`, `is_key_like` | `type`, `nullable`, `is_primary_key`, `is_foreign_key` | queries run but return nulls |
| Full-text indexes | none (vector only) | `table_full_text_index`, `column_full_text_index` | hybrid and full-text tools silently skipped |

neocarta degrades gracefully. It probes the database at startup and registers only the tools whose indexes exist, so it would not crash against a dbxcarta graph; it would simply find nothing until the names line up. Closing the gap needs one of two changes:

- **Adapt neocarta:** rewrite the index names and column property names in its retrieval Cypher (a handful of files) for vector-search compatibility, then add a post-write step that creates the full-text indexes hybrid search needs.
- **Adapt dbxcarta:** emit neocarta's index names and property aliases at graph-write time.

The extra metadata dbxcarta places on edges (`confidence`, `source`, join `criteria`) and tables (`layer`) coexists harmlessly, since neocarta ignores any property it does not query. The one feature with no bridge is neocarta's `BusinessTerm`, `Glossary`, and `Query` enrichment: dbxcarta produces none of those nodes or their relationships, so neocarta's business-term bridging and query-log tools would have nothing to traverse.
