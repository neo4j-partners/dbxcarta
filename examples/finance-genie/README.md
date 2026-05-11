# Finance Genie Companion Example

This example shows dbxcarta as the semantic-layer companion to
`/Users/ryanknight/projects/databricks/graph-enriched-lakehouse/finance-genie`.

Finance Genie creates and enriches the Lakehouse tables. dbxcarta reads the
resulting Unity Catalog metadata, embeds schema objects and sample values, and
writes a Neo4j semantic layer for GraphRAG and Text2SQL evaluation.

## Responsibility Boundary

Finance Genie owns:

- Synthetic finance data generation.
- Unity Catalog base tables: `accounts`, `merchants`, `transactions`,
  `account_links`, `account_labels`.
- Neo4j GDS enrichment for the finance demo.
- Unity Catalog Gold tables: `gold_accounts`,
  `gold_account_similarity_pairs`, `gold_fraud_ring_communities`.
- Genie Spaces and demo validation.

dbxcarta owns:

- Unity Catalog metadata extraction from `information_schema`.
- Table, column, value, schema, and database embeddings.
- Sample-value nodes and inferred `REFERENCES` edges.
- Neo4j semantic-layer writes and vector indexes.
- GraphRAG schema retrieval and Text2SQL evaluation.

## Setup Flow

1. Prepare Finance Genie.

   From `finance-genie/enrichment-pipeline`, run the Finance Genie setup path
   that generates data, creates Unity Catalog tables, runs GDS, and writes Gold
   tables. The expected UC scope is:

   `graph-enriched-lakehouse.graph-enriched-schema`

2. Configure dbxcarta.

   From the dbxcarta repo, print the recommended overlay:

   ```bash
   uv run dbxcarta preset finance-genie --print-env
   ```

   Copy the relevant values into `dbxcarta/.env`, or use
   `.env.finance-genie.sample` in this directory as the reference.

3. Check that Finance Genie tables exist.

   ```bash
   uv run dbxcarta preset finance-genie --check-ready
   ```

   Add `--strict-gold` when you want the check to fail unless the Gold tables
   have already been created.

4. Upload the finance question set.

   The preset expects the client question file at:

   `/Volumes/graph-enriched-lakehouse/graph-enriched-schema/graph-enriched-volume/dbxcarta/questions.json`

   Upload `examples/finance-genie/questions.json` there before running the
   client evaluation.

5. Build the semantic layer.

   Run the normal dbxcarta upload and submit flow. dbxcarta will read the
   Finance Genie Unity Catalog schema and write semantic metadata to Neo4j.

6. Run the client evaluation.

   After the server pipeline succeeds and vector indexes are online, run
   `run_dbxcarta_client.py` with the finance question set.

## Preset Defaults

The preset includes both base and Gold tables by targeting the full
`graph-enriched-schema`. It enables table, column, value, schema, and database
embeddings, and turns on semantic FK inference so dbxcarta can recover join
paths from Finance Genie column names, comments, and embeddings even though the
Finance Genie tables are not created with declared foreign-key constraints.

If you only want a cheaper first validation run, override the embedding flags
in `.env` and start with `DBXCARTA_INCLUDE_EMBEDDINGS_TABLES=true` only.
