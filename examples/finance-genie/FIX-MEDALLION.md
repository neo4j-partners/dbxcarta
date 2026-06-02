# Finance Genie medallion fix

How the finance-genie example was made to run on the `aws-partner-rk`
workspace under its committed medallion architecture, what was broken, and how
to redo or avoid the same work next time.

## TL;DR

The committed preset (`finance_genie.py`) and overlay (`dbxcarta-overlay.env`)
declare a medallion layout across four Unity Catalog catalogs. None of those
catalogs existed on `aws-partner-rk`, the finance Gold tables had never been
built, and the Neo4j Aura instance held an unrelated graph. The fix kept the
committed design untouched and instead provisioned the workspace to match the
contract: built the missing Gold tables upstream, then created the medallion
catalogs and cloned the tables into the right layers.

## The contract the example expects

`finance_genie.py` is the single source of truth. Its `env()` declares:

| Key | Value |
| --- | --- |
| `DBXCARTA_CATALOG` | `graph-enriched-finance-silver` |
| `DBXCARTA_CATALOGS` | `graph-enriched-finance-bronze,graph-enriched-finance-silver,graph-enriched-finance-gold` |
| `DBXCARTA_LAYER_MAP` | `…-bronze:bronze,…-silver:silver,…-gold:gold` |
| `DBXCARTA_SCHEMAS` | `graph-enriched-schema` |
| ops volume / summary | under `dbxcarta-catalog.finance_genie_ops` |

The readiness check expects the 5 base tables in the **silver** catalog and the
3 Gold tables in the **gold** catalog, both in schema `graph-enriched-schema`.
Bronze holds intermediate GDS feature tables and is not a readiness gate.

## What was wrong

1. **Stale warehouse in the upstream `.env`.** `finance-genie/.env` line 73
   still pointed `GRAPH_FRAUD_ANALYST_WAREHOUSE_ID` at the dead azure warehouse
   `a2946a63e3a3643d`. The live `aws-partner-rk` warehouse is
   `c37d0438c79ad6c5`.

2. **Gold tables never built.** The finance data existed only in a single
   legacy catalog, `graph-on-databricks.graph-enriched-schema`, and held just
   the 5 base tables. The 3 Gold tables (`gold_accounts`,
   `gold_account_similarity_pairs`, `gold_fraud_ring_communities`) were absent.

3. **Neo4j Aura held a different graph.** The instance at
   `4b2239bb.databases.neo4j.io`, shared by both the upstream `.env` and
   dbxcarta's base `.env`, contained an aviation/maintenance graph (`Aircraft`,
   `Flight`, `Sensor`, and so on) with zero finance nodes. The GDS step needs an
   enriched finance graph, so the Gold tables could not be produced from it.

4. **Medallion catalogs did not exist.** The four catalogs the overlay names
   (`graph-enriched-finance-bronze`, `-silver`, `-gold`, and the ops catalog
   `dbxcarta-catalog`) were not present in the metastore. The data lived in one
   single catalog instead, so the committed medallion config had nothing to
   bind to.

## How it was fixed

### 1. Correct the stale warehouse

Edited the real file behind the `enrichment-pipeline/.env` symlink,
`finance-genie/.env`:

```
GRAPH_FRAUD_ANALYST_WAREHOUSE_ID=c37d0438c79ad6c5
```

### 2. Build the Gold tables upstream

The upstream finance-genie pipeline owns data and Gold tables. Confirmed the
aircraft graph in Aura was disposable, then ran the stages that produce the
Gold tables. From `finance-genie/enrichment-pipeline`:

```bash
uv run python -m cli upload --all          # ship job scripts + sql/gold_schema.sql
uv run python -m cli submit 02_neo4j_ingest.py     # wipes the graph, loads finance graph
uv run setup/run_gds.py                            # PageRank / Louvain / Betweenness / NodeSimilarity
uv run python -m cli submit 03_pull_gold_tables.py # write the 3 Gold Delta tables
uv run python -m cli submit 04_validate_gold_tables.py  # 6-check correctness gate
```

Result: `graph-on-databricks.graph-enriched-schema` gained the 3 Gold tables
(`gold_accounts` 25,000 rows, `gold_account_similarity_pairs` 206,277 rows,
`gold_fraud_ring_communities` 12 rows). The validation gate passed.

> **Warning.** `jobs/02_neo4j_ingest.py` runs
> `MATCH (n) CALL { WITH n DETACH DELETE n } IN TRANSACTIONS OF 10000 ROWS`.
> That deletes every node in the target Neo4j database, not only Account and
> Merchant nodes. Confirm the instance is safe to wipe before running it. The
> Aura instance is shared with dbxcarta, so check what is in it first.

### 3. Provision the medallion catalogs to match the contract

This kept the committed preset and overlay unchanged. The workspace was shaped
to fit them. All four catalogs needed an explicit `MANAGED LOCATION`, explained
in the gotchas below.

```sql
-- Catalogs (one statement each; the SQL tool is single-statement)
CREATE CATALOG IF NOT EXISTS `graph-enriched-finance-silver`
  MANAGED LOCATION 's3://databricks-storage-7474647869599596/unity-catalog/7474647869599596/graph-enriched-finance-silver';
CREATE CATALOG IF NOT EXISTS `graph-enriched-finance-gold`
  MANAGED LOCATION 's3://databricks-storage-7474647869599596/unity-catalog/7474647869599596/graph-enriched-finance-gold';
CREATE CATALOG IF NOT EXISTS `graph-enriched-finance-bronze`
  MANAGED LOCATION 's3://databricks-storage-7474647869599596/unity-catalog/7474647869599596/graph-enriched-finance-bronze';
CREATE CATALOG IF NOT EXISTS `dbxcarta-catalog`
  MANAGED LOCATION 's3://databricks-storage-7474647869599596/unity-catalog/7474647869599596/dbxcarta-catalog';

-- Schemas
CREATE SCHEMA IF NOT EXISTS `graph-enriched-finance-silver`.`graph-enriched-schema`;
CREATE SCHEMA IF NOT EXISTS `graph-enriched-finance-gold`.`graph-enriched-schema`;
CREATE SCHEMA IF NOT EXISTS `graph-enriched-finance-bronze`.`graph-enriched-schema`;
CREATE SCHEMA IF NOT EXISTS `dbxcarta-catalog`.`finance_genie_ops`;

-- Ops volume
CREATE VOLUME IF NOT EXISTS `dbxcarta-catalog`.`finance_genie_ops`.`dbxcarta-ops`;

-- Base tables -> silver (one DEEP CLONE per table)
CREATE TABLE IF NOT EXISTS `graph-enriched-finance-silver`.`graph-enriched-schema`.accounts
  DEEP CLONE `graph-on-databricks`.`graph-enriched-schema`.accounts;
-- repeat for: merchants, transactions, account_links, account_labels

-- Gold tables -> gold (one DEEP CLONE per table)
CREATE TABLE IF NOT EXISTS `graph-enriched-finance-gold`.`graph-enriched-schema`.gold_accounts
  DEEP CLONE `graph-on-databricks`.`graph-enriched-schema`.gold_accounts;
-- repeat for: gold_account_similarity_pairs, gold_fraud_ring_communities
```

Bronze stays empty. Its feature tables are not produced by the current upstream
pipeline and are not a readiness gate.

### 4. Verify

```bash
DBXCARTA_ENV_FILE=examples/finance-genie/dbxcarta-overlay.env \
  uv run dbxcarta preset dbxcarta_finance_genie_example:preset --check-ready --strict-optional
```

Expected output: `status: ready`, 8 expected tables present, required and
optional both ready.

## Gotchas that cost time

- **Default Storage blocks plain `CREATE CATALOG`.** This metastore has
  Databricks Default Storage enabled with no metastore storage root, so a bare
  `CREATE CATALOG name` fails with `Metastore storage root URL does not exist`.
  Supply `MANAGED LOCATION` under a registered external location. Find one with
  `SHOW EXTERNAL LOCATIONS`. Here it is `partner_us_east_2` at
  `s3://databricks-storage-7474647869599596/unity-catalog/7474647869599596`, and
  the existing catalogs already sit under it.

- **`dbxcarta-submit` subcommands reject `--env-file`.** `publish-wheels` and
  `submit-entrypoint` parse only their own arguments, so `--env-file` errors
  with `unrecognized arguments`. Select the overlay with the
  `DBXCARTA_ENV_FILE` environment variable instead. The `dbxcarta preset` and
  `dbxcarta verify` commands do accept `--env-file`.

- **The shared Neo4j Aura instance.** Both projects point at `4b2239bb`. Any
  ingest that wipes the graph affects both. Check graph contents before
  running `02_neo4j_ingest.py`.

- **`DEEP CLONE` preserves column comments.** Verified on the cloned silver
  tables. dbxcarta reads column comments for FK inference and embeddings, so a
  plain `CREATE TABLE AS SELECT` would have dropped signal the example needs.
  Use `DEEP CLONE`, not CTAS.

- **The clones are a point-in-time copy.** If the upstream regenerates the
  source data in `graph-on-databricks`, the medallion catalogs go stale.
  Re-run the `DEEP CLONE` statements to refresh.

- **Upstream validator path bug.** `validation/validate_neo4j_graph.py` looks
  for `ground_truth.json` under `enrichment-pipeline/data/`, but the committed
  data lives in `finance-genie/data/`. The graph state was checked with a
  direct Cypher query instead.

## Doing this faster next time

1. **Script the provisioning.** The catalog, schema, volume, and clone
   statements above are idempotent (`IF NOT EXISTS`, `DEEP CLONE` into a fresh
   name). Put them in one SQL file or a small Python helper keyed off the preset
   values so the medallion can be rebuilt in one command. Driving it from
   `preset.catalogs`, `preset.gold_catalog`, and `preset.base_tables` keeps the
   script in sync with the contract.

2. **Decide once: provision or generate.** Two clean options exist. Either
   provision the medallion by cloning from the single source catalog, as done
   here, or change the upstream pipeline to write each layer to its own catalog.
   Cloning leaves the upstream untouched and is the lighter change. Generating
   removes the point-in-time staleness but is a larger upstream rework.

3. **Confirm the warehouse and catalog names against the live workspace
   first.** Use the Databricks MCP tools or the CLI to list catalogs, warehouse
   IDs, serving endpoints, and external locations before editing any `.env`.
   The stale warehouse and the missing catalogs were both caught this way.

4. **Run readiness before any submit.** `--check-ready --strict-optional` is a
   cheap gate that catches missing catalogs, missing tables, and broken auth
   before a billable job runs.

5. **Keep the ops catalog separate from the ingested schema.** dbxcarta ingests
   every table in `DBXCARTA_SCHEMAS`. The run-summary table and staging volume
   live in `dbxcarta-catalog.finance_genie_ops` so they are never mistaken for
   source tables.

## Reference values for this workspace

| Item | Value |
| --- | --- |
| Profile | `aws-partner-rk` |
| Workspace | `dbc-cc887abc-9779.cloud.databricks.com` |
| Warehouse | `c37d0438c79ad6c5` |
| Cluster | `0602-030236-rg24mqlw` |
| External location | `partner_us_east_2` at `s3://databricks-storage-7474647869599596/unity-catalog/7474647869599596` |
| Source catalog | `graph-on-databricks.graph-enriched-schema` |
| Neo4j Aura | `4b2239bb.databases.neo4j.io` (shared) |
| Chat endpoint | `databricks-claude-sonnet-4-6` |
| Embedding endpoint | `databricks-gte-large-en` |
