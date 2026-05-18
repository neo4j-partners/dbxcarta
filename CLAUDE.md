# DBxCarta — CLAUDE.md

## Critical design rules

Before writing or changing Spark code, read `docs/reference/best-practices.md`. In particular:

- **Never use a Python UDF or `pandas_udf` for rule logic in the Spark pipeline.** Catalyst cannot optimize a UDF: it blocks predicate pushdown and whole-stage codegen and pays Arrow serialization per batch, which dominates large or n²-shaped joins. Logic driven by small static tables (suffix lists, type-class maps, score tables, stopwords) must be expanded into native `Column` expressions and broadcast-join lookups at plan-construction time. Python builds the plan; Spark evaluates every row. See `docs/reference/best-practices.md` §7 (Spark / Databricks).
- **Never collect catalog-scale data to the driver.** Keep columns, constraints, values, and FK candidates in DataFrames. See `docs/reference/best-practices.md` §5.

`docs/reference/best-practices.md` is the authoritative list of pipeline design rules with sources. Add to it (with a cited source) when a rule is established; do not let these rules regress.

## Environment configuration (layered)

dbxcarta loads env config in two layers. See `docs/proposals/env-layering.md`.

- The repo-root `.env` is the shared **base**: Databricks infra (profile, compute, workspace, warehouse, secret scope), shared chat/embedding endpoints, and the Neo4j secrets. It is gitignored and **never edited per integration**.
- Each integration ships a committed, secret-free `examples/<name>/dbxcarta-overlay.env` holding only the dbxcarta-scoped keys (catalog/catalogs/layer-map/schemas, volume, summary, sample/embedding flags, client arms).
- Select an integration with one flag: `uv run dbxcarta <cmd> --env-file examples/<name>/dbxcarta-overlay.env`, or export `DBXCARTA_ENV_FILE` to that path. `--env-file` wins over `DBXCARTA_ENV_FILE`.
- Precedence, lowest to highest: base `.env` → overlay → real exported process env. With no overlay selected, only the base `.env` loads (unchanged behavior). A selected overlay that does not resolve is a hard error, never a silent base-only fallback.
- Do not put `NEO4J_*` or any secret in an overlay (overlays are committed). Do not write integration-scoped values back into the root `.env`.
- The `examples/<name>/.env` and `.env.sample` files are the **separate** self-contained config for that integration's standalone tooling (local demo, slice/materialize). They never layer and are not the dbxcarta CLI overlay.
