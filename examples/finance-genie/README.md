# Finance Genie Companion Example

This example shows dbxcarta as the semantic-layer companion to
`/Users/ryanknight/projects/databricks/graph-on-databricks/finance-genie`.

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

## Local CLI Client

The Finance Genie preset includes a local, read-only CLI client:

```bash
uv run dbxcarta demo finance-genie <command>
```

This client is different from the Databricks batch evaluation job in step 9. It
runs from your laptop, reads local `.env`, connects to Neo4j for dbxcarta graph
retrieval, calls a Databricks serving endpoint for SQL generation, and executes
the generated SQL on a Databricks SQL warehouse through the Databricks Python
SDK.

Use it when you want to demonstrate the semantic layer interactively after the
ingest job has built the Neo4j graph:

- `preflight` checks local config, warehouse connectivity, the local question
  file, chat and embedding endpoint names, and graph schema availability.
- `questions` lists the checked-in Finance Genie question set.
- `ask` runs one question end to end with GraphRAG context, SQL generation,
  SQL execution, and reference comparison when `reference_sql` is available.
- `sql` runs a direct read-only Databricks SQL statement through the configured
  warehouse.

The local client is intentionally read-only. Direct SQL and generated SQL must
be a single `SELECT`, `WITH`, or `EXPLAIN` statement; mutating statements are
rejected before execution.

Required local configuration:

- `DATABRICKS_PROFILE`, unless default Databricks SDK auth is configured.
- `DATABRICKS_WAREHOUSE_ID`.
- `DBXCARTA_CATALOG` and `DBXCARTA_SCHEMAS`.
- `DBXCARTA_CHAT_ENDPOINT` and `DBXCARTA_EMBEDDING_ENDPOINT`.
- `NEO4J_URI`, `NEO4J_USERNAME`, and `NEO4J_PASSWORD` for local graph access.

## Setup Flow

This is the complete validated operator flow for using dbxcarta with the
Finance Genie Lakehouse. Run these commands from the dbxcarta repo unless a
step says otherwise.

### 1. Prepare Finance Genie

From `finance-genie/enrichment-pipeline`, run the Finance Genie setup path that
generates data, creates Unity Catalog tables, runs GDS, and writes Gold tables.

The validated UC scope for this workspace is:

`graph-enriched-lakehouse.graph-enriched-schema`

Expected base tables:

- `accounts`
- `merchants`
- `transactions`
- `account_links`
- `account_labels`

Expected Gold tables:

- `gold_accounts`
- `gold_account_similarity_pairs`
- `gold_fraud_ring_communities`

### 2. Configure dbxcarta

Print the recommended dbxcarta overlay:

```bash
uv run dbxcarta preset finance-genie --print-env
```

Copy those values into `dbxcarta/.env`, or use
`.env.finance-genie.sample` in this directory as the reference.

The validated `.env` points dbxcarta at:

- `DBXCARTA_CATALOG=graph-enriched-lakehouse`
- `DBXCARTA_SCHEMAS=graph-enriched-schema`
- `DATABRICKS_VOLUME_PATH=/Volumes/graph-enriched-lakehouse/graph-enriched-schema/graph-enriched-volume`
- `DBXCARTA_CLIENT_QUESTIONS=/Volumes/graph-enriched-lakehouse/graph-enriched-schema/graph-enriched-volume/dbxcarta/questions.json`
- `DBXCARTA_INJECT_CRITERIA=false`

`DBXCARTA_INJECT_CRITERIA=false` avoids noisy Neo4j warnings because Finance
Genie inferred relationships do not carry literal `criteria` properties.

### 3. Confirm safety and local config

dbxcarta does not reset the Neo4j database during normal ingest. Node and
relationship writes use `MERGE`; the only normal delete is stale dbxcarta
`Value` nodes attached to columns in the current UC scope.

Do not run:

```cypher
MATCH (n) DETACH DELETE n;
```

Validate local settings and tests:

```bash
uv run python -c "from dotenv import load_dotenv; load_dotenv('.env'); from dbxcarta.settings import Settings; from dbxcarta.client.settings import ClientSettings; Settings(); ClientSettings(); print('settings=ok')"
uv run pytest
```

Expected local test result from the validated run:

`157 passed, 1 skipped, 3 deselected`

### 4. Check Finance Genie table readiness

Run strict readiness so the check fails if the Gold tables are missing:

```bash
uv run dbxcarta preset finance-genie --check-ready --strict-gold
```

Expected output:

```text
Finance Genie UC scope: graph-enriched-lakehouse.graph-enriched-schema
Present expected tables: 8/8
Base tables: ready
Gold tables: ready
Status: ready
```

### 5. Refresh Neo4j secrets

dbxcarta jobs read Neo4j credentials from the Databricks secret scope, not
directly from local `.env`. Refresh the scope before submitting jobs so the
Databricks run targets the configured Neo4j database:

```bash
./setup_secrets.sh --profile azure-rk-knight
```

This updates the `dbxcarta-neo4j` scope keys:

- `NEO4J_URI`
- `NEO4J_USERNAME`
- `NEO4J_PASSWORD`

### 6. Upload the finance question set

The client expects the question file at:

`/Volumes/graph-enriched-lakehouse/graph-enriched-schema/graph-enriched-volume/dbxcarta/questions.json`

Upload `examples/finance-genie/questions.json` to that exact path. The generic
`dbxcarta upload --data` command uploads to the volume root, so use the
Finance Genie upload helper:

```bash
uv run python examples/finance-genie/upload_questions.py
```

### 7. Build and upload dbxcarta artifacts

Validate the cluster/workspace target:

```bash
uv run dbxcarta validate run_dbxcarta.py
```

Build and upload the wheel:

```bash
uv run dbxcarta upload --wheel
```

Upload job scripts:

```bash
uv run dbxcarta upload --all
```

The validated run uploaded wheel `dbxcarta-0.2.32-py3-none-any.whl`.

### 8. Build the semantic layer

Submit the ingest job:

```bash
uv run dbxcarta submit run_dbxcarta.py
```

Monitor it in Databricks:

- **Workflows > Jobs & Pipelines > Job runs**
- Search for `dbxcarta: run_dbxcarta.py`

Or tail logs locally:

```bash
uv run dbxcarta logs <run_id>
```

Validated successful ingest run:

- Run ID: `92950628270385`
- Result: `SUCCESS`
- Summary: 1 schema, 16 tables, 158 columns, 107 value nodes, 18 `REFERENCES` edges

Verify the semantic layer:

```bash
uv run dbxcarta verify
```

Expected result:

```text
verify: run_id=local OK (0 violations)
```

### 9. Run the client evaluation

After the ingest run succeeds and vector indexes are online, run:

```bash
uv run dbxcarta submit run_dbxcarta_client.py
```

Monitor it in Databricks:

- **Workflows > Jobs & Pipelines > Job runs**
- Search for `dbxcarta: run_dbxcarta_client.py`

Validated clean client run:

- Run ID: `1002789398520910`
- Result: `SUCCESS`
- `graph_rag`: 12/12 parsed, 12/12 executed, 12/12 non-empty
- Reported `graph_rag` correct rate: 58.3%

Pull logs locally:

```bash
uv run dbxcarta logs 1002789398520910
```

Expected summary shape:

```text
[dbxcarta_client] run_id=local job=dbxcarta_client status=success catalog=graph-enriched-lakehouse
  no_context: attempted=12 ...
  schema_dump: attempted=12 parsed=12 executed=12 non_empty=12 ...
  graph_rag: attempted=12 parsed=12 executed=12 non_empty=12 ...
```

### 10. Run the local CLI demo

After the semantic layer is built, use the local CLI client to prove the flow
without submitting another Databricks job.

Run a local preflight:

```bash
uv run dbxcarta demo finance-genie preflight
```

List the checked-in Finance Genie question set:

```bash
uv run dbxcarta demo finance-genie questions
```

Ask one of the sample questions end to end:

```bash
uv run dbxcarta demo finance-genie ask --question-id fg_q01 --show-context
```

Or ask an ad hoc finance question:

```bash
uv run dbxcarta demo finance-genie ask "Which high risk merchants are connected to fraud accounts?"
```

Run a direct read-only SQL check through the same Databricks SQL warehouse:

```bash
uv run dbxcarta demo finance-genie sql "SELECT COUNT(*) AS account_count FROM `graph-enriched-lakehouse`.`graph-enriched-schema`.accounts"
```

The `sql` command allows only `SELECT`, `WITH`, and `EXPLAIN` statements. The
`ask` command applies the same read-only guard to generated SQL before it runs.

### 11. Keep the validation checklist updated

Use `docs/validate-finance-genie.md` as the running validation record. Update
that checklist whenever a run is retried, blocked, fixed, or completed.

## Preset Defaults

The preset includes both base and Gold tables by targeting the full
`graph-enriched-schema`. It enables table, column, value, schema, and database
embeddings, and turns on semantic FK inference so dbxcarta can recover join
paths from Finance Genie column names, comments, and embeddings even though the
Finance Genie tables are not created with declared foreign-key constraints.
The preset disables criteria injection because Finance Genie inferred
relationships do not carry literal join-predicate strings.

If you only want a cheaper first validation run, override the embedding flags
in `.env` and start with `DBXCARTA_INCLUDE_EMBEDDINGS_TABLES=true` only.
