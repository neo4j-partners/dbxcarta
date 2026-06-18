# Standalone submit example

How to submit a neocarta ingest job to Databricks as a **single library call**,
instead of the operator CLI. `submit_finance_genie.py` calls
[`submit_neocarta_ingest`](../../packages/dbxcarta-submit) directly: it stages a
prebuilt neocarta connector wheel on a UC Volume and submits the ingest job that
writes the finance-genie semantic graph into Neo4j.

This is the library-call counterpart to `dbxcarta submit-entrypoint ingest`. The
other examples (`finance-genie/`, `dense-schema/`, `schemapile/`) drive the full
developer loop (the `make e2e-*` targets, readiness, bootstrap, the client
evaluation). This one does a single thing — submit ingest — and is the smallest
end-to-end use of `dbxcarta-submit` as a library.

## What lives here

```
examples/standalone-submit/
├── README.md                          # this file
├── submit_finance_genie.py            # the submit script (a library call)
└── submit_finance_genie.env.sample    # the per-integration overlay template
```

`submit_finance_genie.env.sample` is a dbxcarta CLI **overlay**, layered over the
repo-root base `.env` exactly like `examples/finance-genie/dbxcarta-overlay.env`.
It carries only the per-integration values (the neocarta ingest contract, the
ops Volume, the secret-scope name). The Databricks infra (`DATABRICKS_PROFILE`,
`DATABRICKS_CLUSTER_ID`, `DATABRICKS_WAREHOUSE_ID`, `DATABRICKS_WORKSPACE_DIR`,
`DATABRICKS_COMPUTE_MODE`) and `NEOCARTA_WHEEL_SOURCE` come from the base `.env`
and must not be repeated in the overlay. It is secret-free: the cluster reads
Neo4j credentials at runtime from the Databricks secret scope it names.

## Prerequisites

- A configured repo-root `.env` (infra + `NEOCARTA_WHEEL_SOURCE`), as for any
  other `dbxcarta` command in this repo.
- A **classic (non-serverless) cluster** with the **Neo4j Spark Connector**
  attached as a JVM library. Ingest writes through that connector, which is not
  supported on serverless compute.
- A reachable **Neo4j** instance and a **Databricks secret scope** holding its
  credentials (`NEO4J_URI`, `NEO4J_USERNAME`, `NEO4J_PASSWORD`), provisioned
  under the scope name in the overlay. Provision it from the finance-genie
  example with `./setup_secrets.sh`.
- A built **neocarta connector wheel**. Build it from the
  [neocarta](https://github.com/neo4j-field/neocarta) repo (`uv build --wheel`),
  and point `NEOCARTA_WHEEL_SOURCE` at its `dist/`.

## Run it

Run from the **repo root**, so the relative base `.env` resolves:

1. Copy the overlay template and adjust it for your deployment:

   ```bash
   cp examples/standalone-submit/submit_finance_genie.env.sample \
      examples/standalone-submit/submit_finance_genie.env
   ```

   The populated `submit_finance_genie.env` is local-only and gitignored.

2. Submit:

   ```bash
   uv run python examples/standalone-submit/submit_finance_genie.py
   ```

   The script points `DBXCARTA_ENV_FILE` at the overlay beside it, loads it over
   the base `.env`, then resolves the neocarta wheel from `NEOCARTA_WHEEL_SOURCE`
   and submits the ingest job on classic compute. To use a specific wheel
   instead of the newest in `NEOCARTA_WHEEL_SOURCE`, pass its path:

   ```bash
   uv run python examples/standalone-submit/submit_finance_genie.py \
      /path/to/neocarta-<version>-py3-none-any.whl
   ```

The script also carries [PEP 723](https://peps.python.org/pep-0723/) inline
metadata declaring `dbxcarta-submit` as its only dependency, so once
`dbxcarta-submit` is published it can run fully standalone with `uv run
examples/standalone-submit/submit_finance_genie.py` from a checkout that has the
base `.env`. In this repo, run it through the workspace with `uv run python ...`
as above.

## Embeddings

The overlay enables inline embeddings during ingest through the workspace-native
`databricks-gte-large-en` endpoint (1024-dim), so no external-model registration
is needed. It matches the finance-genie overlay, so the graph embeds with the
same model the client later queries with. For a cheaper first validation run,
set only `NEOCARTA_DATABRICKS_INCLUDE_EMBEDDINGS_TABLES=true` and leave the rest
`false`, or embed externally afterward with neocarta.
