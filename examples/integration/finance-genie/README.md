# dbxcarta Finance Genie Example

This directory is a standalone Python package, `dbxcarta-finance-genie-example`,
that shows how an outside application can use dbxcarta without copying runner
scripts. Its primary job is to own Finance Genie configuration. The CLI preset
is a convenience wrapper around that configuration, not a requirement for using
dbxcarta as a library.

## What lives here, and why

```
examples/integration/finance-genie/
├── pyproject.toml                                         # standalone package
├── README.md                                              # this file
├── .env.sample                                            # operator overlay reference
├── src/dbxcarta_finance_genie_example/
│   ├── __init__.py                                        # re-exports `preset`
│   ├── finance_genie.py                                   # `FinanceGeniePreset` + `preset`
│   ├── local_demo.py                                      # optional read-only local CLI
│   ├── upload_questions.py                                # optional script
│   └── questions.json                                     # demo question fixture
└── ../../../tests/examples/finance-genie/
    ├── test_preset.py
    └── test_local_demo.py
```

The package declares dbxcarta as a normal pip dependency in `pyproject.toml`.
Inside this repo, `uv` resolves it to the editable parent through
`[tool.uv.sources]`; from outside, you would pin it like
`dependencies = ["dbxcarta-spark", "dbxcarta-client"]` and pip would fetch
those distributions from your wheel index. There is nothing privileged about
the example's relationship to the dbxcarta workspace.

The Finance Genie settings are normal Python data. A direct library call can
construct `SparkIngestSettings` from the package-owned overlay and call the
Spark ingest implementation:

```python
from dbxcarta.spark import SparkIngestSettings, run_dbxcarta
from dbxcarta_finance_genie_example import preset

env = preset.env()
settings = SparkIngestSettings(
    dbxcarta_catalog=env["DBXCARTA_CATALOG"],
    dbxcarta_schemas=env["DBXCARTA_SCHEMAS"],
    dbxcarta_summary_volume=env["DBXCARTA_SUMMARY_VOLUME"],
    dbxcarta_summary_table=env["DBXCARTA_SUMMARY_TABLE"],
    dbxcarta_include_values=env["DBXCARTA_INCLUDE_VALUES"] == "true",
    dbxcarta_include_embeddings_tables=True,
    dbxcarta_include_embeddings_columns=True,
    dbxcarta_include_embeddings_values=True,
    dbxcarta_include_embeddings_schemas=True,
    dbxcarta_include_embeddings_databases=True,
    dbxcarta_infer_semantic=True,
)
run_dbxcarta(settings=settings)
```

The preset itself is a single dataclass instance, `preset`, exposed at
`dbxcarta_finance_genie_example:preset`. It implements the required dbxcarta
`Preset` protocol:

- `env()` returns the dbxcarta environment overlay.

It also implements optional CLI/demo hooks:

- `readiness(ws, warehouse_id)` returns a `ReadinessReport` describing whether
  the Finance Genie tables are present in Unity Catalog.
- `upload_questions(ws)` uploads `questions.json` to the path named by
  `DBXCARTA_CLIENT_QUESTIONS`.

## Template guidance for a new application package

To build your own application package, copy this layout and change:

1. The package name in `pyproject.toml` and the src folder name.
2. The catalog, schema, volume, and table list in `finance_genie.py`.
3. The env overlay returned by `env()` to match the dbxcarta features you
   want enabled.
4. The questions fixture, if you want a demo question set.

If you also want CLI automation, expose a small `Preset` object and document
its import path explicitly, for example `mycorp_dbxcarta_preset:preset`.

## Responsibility Boundary

Finance Genie (upstream project) owns:

- Synthetic finance data generation.
- Unity Catalog base tables: `accounts`, `merchants`, `transactions`,
  `account_links`, `account_labels`.
- Neo4j GDS enrichment for the finance demo.
- Unity Catalog Gold tables: `gold_accounts`,
  `gold_account_similarity_pairs`, `gold_fraud_ring_communities`.
- Genie Spaces and demo validation.

dbxcarta (via this preset) owns:

- Unity Catalog metadata extraction from `information_schema`.
- Table, column, value, schema, and database embeddings.
- Sample-value nodes and inferred `REFERENCES` edges.
- Neo4j semantic-layer writes and vector indexes.
- GraphRAG schema retrieval and Text2SQL evaluation.

## Setup Flow

Run these commands from the dbxcarta repo unless a step says otherwise.

### 1. Install dbxcarta and the example preset

```bash
uv sync
uv pip install -e examples/integration/finance-genie/
```

### 2. Prepare Finance Genie

From `finance-genie/enrichment-pipeline` (the upstream project), run the
Finance Genie setup path that generates data, creates Unity Catalog tables,
runs GDS, and writes Gold tables.

The validated UC scope for this workspace is
`graph-enriched-lakehouse.graph-enriched-schema`.

Expected base tables:

- `accounts`, `merchants`, `transactions`, `account_links`, `account_labels`

Expected Gold (optional) tables:

- `gold_accounts`, `gold_account_similarity_pairs`,
  `gold_fraud_ring_communities`

### 3. Configure dbxcarta

Print the recommended dbxcarta overlay:

```bash
uv run dbxcarta preset dbxcarta_finance_genie_example:preset --print-env
```

Copy those values into `dbxcarta/.env`, or use this directory's `.env.sample`
as the reference.

### 4. Check readiness

Strict readiness fails if either base or Gold tables are missing:

```bash
uv run dbxcarta preset dbxcarta_finance_genie_example:preset --check-ready --strict-optional
```

Without `--strict-optional`, only the five base tables are required for
readiness; the three Gold tables are reported as a warning.

### 5. Refresh Neo4j secrets

dbxcarta jobs read Neo4j credentials from the Databricks secret scope:

```bash
./setup_secrets.sh --profile azure-rk-knight
```

### 6. Upload the question set

```bash
uv run dbxcarta preset dbxcarta_finance_genie_example:preset --upload-questions
```

This uploads the package's `questions.json` to the path named by
`DBXCARTA_CLIENT_QUESTIONS` (typically
`/Volumes/.../graph-enriched-volume/dbxcarta/questions.json`).

### 7. Build and upload dbxcarta artifacts

```bash
uv run dbxcarta upload --wheel
uv run dbxcarta upload --all
```

### 8. Build the semantic layer

Submit the installed wheel's ingest entrypoint:

```bash
uv run dbxcarta submit-entrypoint ingest
```

Verify the result:

```bash
uv run dbxcarta verify
```

### 9. Run the client evaluation

```bash
uv run dbxcarta submit-entrypoint client
```

### 10. Run the local CLI demo

After the semantic layer is built, use the local read-only CLI in this
package to demonstrate the flow without submitting another Databricks job.

The local demo loads its own `.env` from this directory and never inherits
the parent dbxcarta repo's `.env`. Copy the sample and fill in your
workspace, warehouse, chat endpoint, and Neo4j credentials:

```bash
cp examples/integration/finance-genie/.env.sample examples/integration/finance-genie/.env
# edit examples/integration/finance-genie/.env
```

Then run any of the demo subcommands from anywhere (they resolve `.env`
relative to the package, not the current working directory):

```bash
uv run --directory examples/integration/finance-genie python -m dbxcarta_finance_genie_example.local_demo preflight
uv run --directory examples/integration/finance-genie python -m dbxcarta_finance_genie_example.local_demo questions
uv run --directory examples/integration/finance-genie python -m dbxcarta_finance_genie_example.local_demo ask --question-id fg_q01 --show-context
uv run --directory examples/integration/finance-genie python -m dbxcarta_finance_genie_example.local_demo sql "SELECT COUNT(*) FROM \`graph-enriched-lakehouse\`.\`graph-enriched-schema\`.accounts"
```

The local demo allows only `SELECT`, `WITH`, and `EXPLAIN` statements.

## Preset Defaults

The preset targets the full `graph-enriched-schema`, enables every embedding
label (tables, columns, values, schemas, databases), and turns on semantic FK
inference so dbxcarta can recover join paths from Finance Genie column names,
comments, and embeddings (the Finance Genie tables are not created with
declared foreign-key constraints). The preset disables criteria injection
because Finance Genie inferred relationships do not carry literal
join-predicate strings.

For a cheaper first validation run, override the embedding flags in `.env` and
start with `DBXCARTA_INCLUDE_EMBEDDINGS_TABLES=true` only.
