# SchemaPile v3: Final End-to-End Test Run

Checklist for deploying the v3 code changes and running the full pipeline
against a freshly reset Neo4j instance. All 20 schemas are already
materialized in Unity Catalog and all 20 question-cache files are present in
`.cache/questions/`, so the pipeline will skip upstream LLM calls and reach
the ingest step quickly.

---

## Pre-flight

- [x] Reset Neo4j: clear all nodes, relationships, and indexes in Aura.
- [x] Update root `.env` to point at the schemapile catalog (see section below).

### Root `.env` changes required

The root `.env` is currently wired to the Finance Genie lakehouse. Every
value below must be replaced before running ingest.

```dotenv
# Catalog and schemas
DBXCARTA_CATALOG=schemapile_lakehouse
DBXCARTA_SCHEMAS=sp_492424_create_adventuresoverflow,sp_310630_schema,sp_266628_tables,sp_535132_zhuanglang,sp_300813_erzeuge_zielzustand,sp_002489_project,sp_594700_takeout,sp_178181_v4_026__create_unified_user,sp_169810_init,sp_352545_capstone,sp_365427_schema,sp_435873_db,sp_596038_ddl_proyecto,sp_008582_auth_services_schema_mysql,sp_289361_schema,sp_413175_01_tabledesign_exam21oct2018,sp_422245_tableschemata,sp_020891_v01__impl_usuario,sp_359146_formasnormais,sp_033815_schema

# Volume paths
DATABRICKS_VOLUME_PATH=/Volumes/schemapile_lakehouse/_meta/schemapile_volume
DBXCARTA_SUMMARY_VOLUME=/Volumes/schemapile_lakehouse/_meta/schemapile_volume/dbxcarta/runs
DBXCARTA_SUMMARY_TABLE=schemapile_lakehouse._meta.dbxcarta_run_summary

# Client evaluation
DBXCARTA_CLIENT_QUESTIONS=/Volumes/schemapile_lakehouse/_meta/schemapile_volume/dbxcarta/questions.json

# Embedding flags (schemapile defaults differ from finance-genie)
DBXCARTA_INCLUDE_EMBEDDINGS_VALUES=false
DBXCARTA_INCLUDE_EMBEDDINGS_DATABASES=false
```

---

## Stage 1: Reinstall package

Run from the repo root.

- [x] `uv sync`
- [x] `uv pip install -e examples/schemapile/`

---

## Stage 2: Regenerate `questions.json`

Run from `examples/schemapile/`. All 20 schemas have cached LLM output in
`.cache/questions/`, so this step reads from cache only and produces a fresh
`questions.json` without making any model or warehouse calls.

- [x] `uv run dbxcarta-schemapile-generate-questions`
- [x] Confirm `questions.json` was written and is non-empty.

---

## Stage 3: Upload questions to volume

Still in `examples/schemapile/`.

- [x] `uv run dbxcarta preset dbxcarta_schemapile_example:preset --upload-questions`
- [x] Confirm upload succeeds with no validation errors.

---

## Stage 4: Upload wheel and scripts

Return to the repo root.

- [x] `uv run dbxcarta upload --wheel`
- [x] `uv run dbxcarta upload --all`
- [x] `uv run dbxcarta validate` to confirm the remote wheel and scripts landed. Skip if both uploads reported no errors.

---

## Stage 5: Run ingest

Submits the ingest job against the 20 schemapile schemas.

- [x] `uv run dbxcarta submit-entrypoint ingest`
- [x] Confirm the job completes with status `SUCCESS`.

---

## Stage 6: Verify graph

Runs graph and catalog verification against the completed ingest run.

- [x] `uv run dbxcarta verify`
- [x] Confirm all checks pass (node counts, FK edge counts, embedding counts).

---

## Stage 7: Run client evaluation

Runs the three-arm evaluation: `no_context`, `schema_dump`, `graph_rag`.

- [x] `uv run dbxcarta submit-entrypoint client`
- [x] Confirm the job completes with status `SUCCESS`.
- [x] Pull results with `uv run dbxcarta download`, or query `DBXCARTA_SUMMARY_TABLE` directly.

---

## Expected outcome

`graph_rag` should outperform `schema_dump` on schema-selection accuracy now
that the v3 retriever changes are in place. Record the arm scores in
`schemapile-v3.md` under the Phase G results section.
