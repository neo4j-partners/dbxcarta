# dbxcarta Dense Schema Example

This directory is a standalone integration example package,
`dbxcarta-dense-schema-example`. It generates a synthetic single-schema
Unity Catalog workload with hundreds of tables so dbxcarta retrieval can be
tested against dense schema context.

The package depends on `dbxcarta-core`, `dbxcarta-spark`, `dbxcarta-client`,
and `dbxcarta-presets`. Its tests live under `tests/examples/dense-schema/`
and run in CI as a first-class sample consumer.

## What lives here

```text
examples/integration/dense-schema/
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

## Setup flow

Run these commands from the dbxcarta repo unless a step says otherwise.

```bash
uv sync
uv pip install -e examples/integration/dense-schema/
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
uv run dbxcarta submit-entrypoint ingest
uv run dbxcarta submit-entrypoint client
```

Synthetic materialization and question generation stay inside this example.
They are not part of the `dbxcarta-core` public API.
