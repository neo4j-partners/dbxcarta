# dbxcarta Dense Schema Example

This directory is a standalone integration example package,
`dbxcarta-dense-schema-example`. It generates a synthetic single-schema
Unity Catalog workload with hundreds of tables so dbxcarta retrieval can be
tested against dense schema context.

The package depends on `dbxcarta-spark` and `dbxcarta-client`. Its tests live
under `tests/examples/dense-schema/` and run in CI as a first-class sample
consumer.

## What lives here

```text
examples/dense-schema/
├── pyproject.toml
├── .env.sample
├── filter_questions.py
├── questions.json
├── questions_1000.json
└── src/dbxcarta_dense_schema_example/
    ├── generator.py
    ├── materialize.py
    ├── preset.py
    └── question_generator.py
```

## Quick iterate loop (testing dbxcarta changes)

Once the one-time setup is in place (example installed, the synthetic
schema generated and materialized into Unity Catalog, and the question
set uploaded), the wheel-rebuild-and-submit pipeline runs in two make
targets from the repo root — ingest first, then the client evaluation
once ingest finishes:

```bash
make e2e-dense-schema-ingest
make e2e-dense-schema-client
```

The `-ingest` target rebuilds the wheels from your current source, then
submits `ingest`; the `-client` target submits `client`, so it reflects
local edits to the dbxcarta packages on every run. The targets set
`DBXCARTA_ENV_FILE` to this directory's
`dbxcarta-overlay.local.env` (the gitignored, local-only overlay) inline
on each command, so they pick up the right dbxcarta config from any shell.
`make help` lists the targets for every example.

## Setup flow

Run these commands from the dbxcarta repo unless a step says otherwise.

```bash
uv sync
uv pip install -e examples/dense-schema/
```

Generate the synthetic schema locally:

```bash
uv run dbxcarta-dense-generate --tables 500
```

Materialize it into Unity Catalog after configuring `.env`:

```bash
uv run dbxcarta-dense-materialize
```

Then use the preset with the normal dbxcarta operational CLI:

```bash
uv run dbxcarta preset dbxcarta_dense_schema_example:preset --print-env
uv run dbxcarta preset dbxcarta_dense_schema_example:preset --upload-questions
uv run dbxcarta-submit submit-entrypoint ingest
uv run dbxcarta-submit submit-entrypoint client
```

Synthetic materialization and question generation stay inside this example.
They are not part of the `dbxcarta-spark` or `dbxcarta-client` public APIs.
