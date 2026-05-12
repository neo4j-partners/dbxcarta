# dbxcarta SchemaPile Example

This directory is a standalone Python package, `dbxcarta-schemapile-example`,
that materializes a slice of [SchemaPile](https://github.com/amsterdata/schemapile)
as Delta tables in a dedicated Unity Catalog catalog and runs dbxcarta
against the result. It is a second reference consumer alongside the
finance-genie example and shares no state with it.

## What lives here

```
examples/schemapile/
├── pyproject.toml
├── README.md
├── .env.sample
├── src/dbxcarta_schemapile_example/
│   ├── __init__.py
│   ├── preset.py                # SchemaPilePreset + module-level `preset`
│   ├── config.py                # one-shot .env parser
│   ├── slice_runner.py          # shells out to upstream slice.py
│   ├── candidate_selector.py    # slice JSON -> candidate-table JSON
│   ├── bootstrap.py             # provisions the UC catalog and volume
│   ├── materialize.py           # candidate JSON -> Delta tables
│   └── question_generator.py    # LLM + SQL validation -> questions.json
└── tests/
```

## Quality caveat for the generated question set

The evaluation `questions.json` is produced by prompting a foundation model
with each candidate schema's DDL, asking for question and SQL pairs, then
executing each candidate SQL against the materialized tables and keeping
only the pairs that run cleanly, return at least one row, and do not return
a single trivial scalar.

This is useful for *relative comparison* across the three dbxcarta arms
(`no_context`, `schema_dump`, `graph_rag`). It is not a hand-curated gold
benchmark and should not be cited as evidence of absolute correctness.

## Setup flow

Run from the dbxcarta repo unless a step says otherwise.

### 1. Install dbxcarta and the example

```bash
uv sync
uv pip install -e examples/schemapile/
```

### 2. Get the upstream SchemaPile data

The example never downloads `schemapile-perm.json` itself. Clone the
upstream repo and follow its README to fetch the 286 MB JSON artifact:

```bash
git clone https://github.com/amsterdata/schemapile.git ~/projects/schemapile
# Follow upstream instructions to download schemapile-perm.json into that dir.
```

### 3. Configure the example .env

```bash
cd examples/schemapile
cp .env.sample .env
# Edit .env and fill in:
#   SCHEMAPILE_REPO=~/projects/schemapile
#   DATABRICKS_PROFILE=<your profile>
#   DATABRICKS_WAREHOUSE_ID=<your warehouse id>
#   DBXCARTA_CATALOG=schemapile_lakehouse  (or your override)
```

The example refuses to run if `DBXCARTA_CATALOG` matches a known project
catalog (e.g. `graph-enriched-lakehouse`).

### 4. Bootstrap the Unity Catalog catalog and volume

Requires catalog-create privilege on the workspace.

```bash
cd examples/schemapile
uv run dbxcarta-schemapile-bootstrap
```

This creates `<catalog>`, `<catalog>._meta`, and
`<catalog>._meta.schemapile_volume`. To tear down later:

```bash
uv run dbxcarta-schemapile-bootstrap --drop-all --yes-i-mean-it
```

### 5. Produce the SchemaPile slice (host-only, no credentials needed)

```bash
uv run dbxcarta-schemapile-slice
```

Shells out to the upstream `slice.py` with the parameters from `.env`,
writes the JSON to `SCHEMAPILE_SLICE_CACHE`, and records a sidecar params
file so subsequent runs with the same parameters are a no-op.

### 6. Pick the candidate set (host-only)

```bash
uv run dbxcarta-schemapile-select
```

Reads the slice JSON, applies the candidate filters, and writes a fixed
candidate-table JSON to `SCHEMAPILE_CANDIDATE_CACHE`. This file is the
single source of truth for the next two steps and should be committed to
the example's cache if you want a fully reproducible run.

### 7. Materialize the slice as Delta tables

```bash
uv run dbxcarta-schemapile-materialize
```

Creates one UC schema per candidate (`sp_<sanitized_id>`) and one Delta
table per table spec. Types are coerced to Delta with a documented map;
anything unrecognized falls back to `STRING`. Schemapile sample VALUES are
not inserted in v1: the materialized tables are typed-but-empty, and the
dbxcarta evaluation relies on metadata. The original source filename,
primary key list, and foreign key list are recorded as Delta table
properties on every table so the trace from a UC table to its schemapile
origin is always one query away.

The step writes `examples/schemapile/.env.generated` with the
`DBXCARTA_SCHEMAS=...` line that the dbxcarta runner needs.

### 8. Generate and validate the question set

```bash
uv run dbxcarta-schemapile-generate-questions
```

Two stages. The first stage prompts `SCHEMAPILE_QUESTION_MODEL` with each
candidate schema's DDL and asks for the configured number of question
and SQL pairs split across single-table filter, two-table join, and
aggregation. The second stage executes each candidate SQL against the
materialized tables on the configured SQL warehouse and keeps only the
pairs that run cleanly, return at least one row, and do not return a
single trivial scalar. Pairs that survive both stages are written to
`questions.json`. The LLM output is cached per (uc_schema, model, seed,
temperature) so a re-run with the same parameters does not re-bill the
model.

### 9. Run dbxcarta against the schemapile catalog

The `.env.generated` file from step 7 supplies `DBXCARTA_SCHEMAS`. Source
it together with `.env` (or copy the line into `.env`) and proceed with
the standard dbxcarta flow from the repo root:

```bash
# Confirm the preset resolves and shows the expected overlay.
uv run dbxcarta preset dbxcarta_schemapile_example:preset --print-env

# Upload the generated question set to the example volume.
SCHEMAPILE_QUESTIONS_FILE=examples/schemapile/questions.json \
  uv run dbxcarta preset dbxcarta_schemapile_example:preset --upload-questions

# Build and submit the ingest job.
uv run dbxcarta upload --wheel
uv run dbxcarta upload --all
uv run dbxcarta submit-entrypoint ingest
uv run dbxcarta verify

# Run the client evaluation arms.
uv run dbxcarta submit-entrypoint client
```

## Phase dependencies and order

This example is built in phases that build on top of each other:

1. **Phase 0 (dbxcarta core)**. Multi-schema `DBXCARTA_SCHEMAS` is supported
   end to end; FK inference is restricted to within-schema pairs.
2. **Phase 1 (questions)**. The candidate JSON is the single source of
   truth shared with the materializer and the question generator.
3. **Phase 2 (slice runner)**. Host-only; depends only on the upstream
   schemapile checkout.
4. **Phase 3 (bootstrap)**. Provisions the catalog and volume.
5. **Phase 4 (materialize)**. Reads the candidate JSON; writes Delta tables.
6. **Phase 5 (package wiring)**. This package; exposes the preset.
7. **Phase 6 (end-to-end run)**. Runs the assembled example against a live
   workspace and compares evaluation arms.

See `docs/proposal/schemapile.md` for the full phased plan and audit trail.
