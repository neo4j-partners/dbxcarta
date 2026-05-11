# Finance Genie Validation Checklist

## Goal

Validate dbxcarta as the semantic-layer companion for the live Finance Genie
Lakehouse tables in Databricks.

Finance Genie owns the Lakehouse tables and graph-enriched Gold features.
dbxcarta owns the Neo4j semantic layer built from Unity Catalog metadata.

## Safety Notes

- [Complete] Confirm dbxcarta does not run a whole-database Neo4j reset in the normal ingest path.
- [Complete] Do not run `MATCH (n) DETACH DELETE n` or any Neo4j cleanup against the finance database.
- [Complete] Confirm the ingest writes with `MERGE` and only replaces dbxcarta-owned semantic metadata for the configured UC scope.

## Validation Steps

- [Complete] Validate `.env` loads into dbxcarta server and client settings.
- [Complete] Identify the live Finance Genie Unity Catalog scope in this workspace.
- [Complete] Align local dbxcarta config and finance-genie example docs with the live UC scope.
- [Complete] Run focused unit tests for the finance-genie preset and settings.
- [Complete] Run full local unit test suite.
- [Complete] Check the live Finance Genie base and Gold tables are present.
- [Complete] Upload the finance-genie client question set to the configured UC Volume.
- [Complete] Build and upload the dbxcarta wheel.
- [Complete] Upload dbxcarta job scripts.
- [Complete] Submit `run_dbxcarta.py` and monitor the Databricks job run to completion.
- [Complete] Verify the dbxcarta run summary and Neo4j semantic-layer counts.
- [Complete] Submit `run_dbxcarta_client.py` and monitor the Databricks job run to completion.
- [Complete] Verify client run summary and GraphRAG arm results.
- [Complete] Validate the local Finance Genie CLI demo preflight.
- [Complete] Run one local CLI GraphRAG question end to end against Databricks SQL.

## Current Findings

- The original local `.env` pointed to `graph-on-databricks.graph-enriched-schema`, but that catalog is not present in the active workspace.
- The expected Finance Genie table set exists in `graph-enriched-lakehouse.graph-enriched-schema`.
- Local `.env` has been updated to use `graph-enriched-lakehouse.graph-enriched-schema`.
- `examples/finance-genie/questions.json` now targets `graph-enriched-lakehouse.graph-enriched-schema`.
- Local settings validation passed for server and client settings.
- Full local test suite passed before staging fix: 155 passed, 1 skipped, 3 deselected.
- Finance Genie readiness passed with 8/8 expected tables present.
- Column metadata is visible for all expected tables: `accounts`, `merchants`, `transactions`, `account_links`, `account_labels`, `gold_accounts`, `gold_account_similarity_pairs`, `gold_fraud_ring_communities`.
- Databricks cluster/workspace validation passed for `run_dbxcarta.py`.
- Databricks secret scope `dbxcarta-neo4j` was refreshed from the current `.env`.
- Uploaded `examples/finance-genie/questions.json` to the configured `DBXCARTA_CLIENT_QUESTIONS` path.
- Built and uploaded wheel `dbxcarta-0.2.31-py3-none-any.whl`.
- Uploaded all `scripts/*.py` to the configured Databricks workspace directory.
- Submitted ingest run `786659000890206`; Databricks reported `INTERNAL_ERROR`.
- Root cause for `786659000890206`: first-run staging path `/dbxcarta/staging` did not exist, and dbutils wrapped the missing path as `ExecutionError` instead of `Py4JJavaError`.
- Local fix added: first-run staging path probing now treats Databricks missing-path wrappers as non-existent while still re-raising permission errors.
- Regression test added for staging missing-path detection.
- Full local test suite passed after staging fix: 157 passed, 1 skipped, 3 deselected.
- Rebuilt and uploaded corrected wheel `dbxcarta-0.2.32-py3-none-any.whl`.
- Re-uploaded all `scripts/*.py` after the staging fix.
- Resubmitted ingest run `92950628270385`; Databricks reported `SUCCESS`.
- Ingest summary: 1 schema, 16 tables, 158 columns, 107 value nodes, 18 `REFERENCES` edges.
- `uv run dbxcarta verify` passed with 0 violations.
- Neo4j write path uses node/relationship `MERGE`; normal ingest does not reset the database. The only delete in the normal path is stale `Value` nodes attached to columns in the current dbxcarta UC scope.
- Submitted client run `526266899294946`; Databricks reported `SUCCESS`.
- Client summary: `no_context` executed 3/12, `schema_dump` executed 12/12, `graph_rag` executed 12/12.
- `graph_rag` returned non-empty results for 12/12 questions with reported correct rate 58.3%.
- Client logs were noisy because finance inferred `REFERENCES` edges do not carry `criteria` properties. Local Finance Genie preset/env now set `DBXCARTA_INJECT_CRITERIA=false`; rerun client to confirm clean logs.
- Reran client with criteria injection disabled as run `1002789398520910`; Databricks reported `SUCCESS`.
- Clean client summary: `no_context` executed 2/12, `schema_dump` executed 12/12, `graph_rag` executed 12/12.
- Clean `graph_rag` result: 12/12 parsed, 12/12 executed, 12/12 non-empty, reported correct rate 58.3%.
- Added local CLI demo commands under `uv run dbxcarta demo finance-genie ...`.
- Full local test suite passed after adding and reviewing the local CLI demo: 170 passed, 1 skipped, 3 deselected.
- Local CLI preflight passed with warehouse `a2946a63e3a3643d`, chat endpoint `databricks-claude-sonnet-4-6`, embedding endpoint `databricks-gte-large-en`, 12 local questions, and 189 Neo4j schema lines.
- Local CLI sample run passed for `fg_q01`: retrieved graph context, generated `SELECT COUNT(*) FROM ...accounts`, executed on Databricks SQL, returned `25000`, and matched the reference SQL.

## Databricks Monitoring

Once jobs are submitted, monitor them in the Databricks console:

- **Workflows > Jobs & Pipelines > Job runs**
- Search for runs with names starting with `dbxcarta`
- Local logs can be tailed with `uv run dbxcarta logs <run_id>`

SQL metadata checks appear under:

- **SQL > Query History**
- Warehouse: `a2946a63e3a3643d`
