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

Once the one-time setup is in place (example installed, the shared catalog
bootstrapped, the synthetic schema generated and materialized into Unity
Catalog, and the question set uploaded), the wheel-rebuild-and-submit pipeline
runs in two make targets from the repo root — ingest first, then the client
evaluation once ingest finishes:

```bash
make e2e-dense-schema-ingest
make e2e-dense-schema-client
```

The `-ingest` target rebuilds the wheels from your current source, then
submits `ingest`; the `-client` target submits `client`, so it reflects
local edits to the dbxcarta packages on every run. The targets set
`DBXCARTA_ENV_FILE` to this directory's committed `dbxcarta-overlay.env`
inline on each command, so they pick up the right dbxcarta config from any
shell. `make help` lists the targets for every example.

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

Bootstrap the shared catalog, `_meta` schema, and volume after configuring
`.env`. The dense fixture reuses the SchemaPile lakehouse catalog and volume, so
this is idempotent and equivalent to running `dbxcarta-schemapile-bootstrap`:

```bash
uv run dbxcarta-dense-bootstrap
```

Materialize the fixture into Unity Catalog:

```bash
uv run dbxcarta-dense-materialize
```

To remove only the dense data schema later, without touching the shared catalog:

```bash
uv run dbxcarta-dense-bootstrap --drop-all --yes-i-mean-it
```

Then use the preset with the normal dbxcarta operational CLI:

```bash
uv run dbxcarta preset dbxcarta_dense_schema_example:preset --check-ready
uv run dbxcarta preset dbxcarta_dense_schema_example:preset --upload-questions
uv run dbxcarta-submit submit-entrypoint ingest
uv run dbxcarta-submit submit-entrypoint client
```

Synthetic materialization and question generation stay inside this example.
They are not part of the `dbxcarta-spark` or `dbxcarta-client` public APIs.
