# Plan: separate dense-schema's ops plane from its data plane

## Goal

Give dense-schema the same clean plane separation that clean-schemapile.md gives
SchemaPile. Today dense's ops (`_meta` schema, wheels volume, run-summary table)
live inside its own data catalog `dense-schema_example`, the same conflation
clean-schemapile removes from `schemapile_lakehouse`. Move dense's ops out to a
`dense_ops` schema in the shared `dbxcarta-catalog`, so the data catalog holds
only `dense_1000`, and one teardown removes dense's full footprint without
orphaning a catalog or an ops schema.

This is the follow-up sibling to clean-schemapile.md. It lands after it, and it
depends on the comma-separated `DBXCARTA_TEARDOWN_TARGET` parser that
clean-schemapile introduces.

## Progress

clean-schemapile.md is implemented: the comma-list teardown parser
(`parse_teardown_targets`), the schemapile overlay, and schemapile's
`materialize.py` `CREATE CATALOG IF NOT EXISTS` have all landed, so the
dependency is satisfied.

- [x] **Overlay** — ops moved to `dbxcarta-catalog.dense_ops`, comma-list teardown,
  stale comments rewritten.
- [x] **Materialize** — `CREATE CATALOG IF NOT EXISTS` for the data catalog.
- [x] **Standalone `config.py`** — `volume_path`/`questions_path` read env, ops fallback.
- [x] **Standalone `.env` / `.env.sample`** — ops location; `.env.sample` catalog fixed.
- [x] **Preset** — readiness queries the data catalog, not `schemapile_lakehouse`.
- [x] **Docs** — README rewritten for the standalone-catalog, split-plane design.
- [x] **Tests** — config, materialize, and preset coverage added.
- [x] **Validation** — ruff, mypy, full suite, and both dry-runs green.

## Why this is needed

The dense overlay already moved dense off SchemaPile's catalog onto its own
`dense-schema_example` catalog, but the split was left half-done: the ops plane
still lives inside the data catalog under a leftover `_meta` schema and a volume
still literally named `schemapile_volume`, the overlay comments still say it
"reuses the schemapile lakehouse catalog," the standalone `.env.sample` still
sets `DBXCARTA_CATALOG=schemapile_lakehouse`, and the preset still defaults its
catalog to `schemapile_lakehouse` so readiness queries the wrong catalog. The
teardown target `schema:dense-schema_example.dense_1000` drops only the data
schema and leaves the `_meta` schema and the catalog behind. Finishing the
split removes every one of these inconsistencies.

## The pattern to mirror: finance-genie and schemapile

finance-genie keeps ops out of its data catalog: its volume and summary live in
`dbxcarta-catalog.finance_genie_ops` with a `dbxcarta-ops` volume. After
clean-schemapile, SchemaPile mirrors this with `dbxcarta-catalog.schemapile_ops`.
dense-schema will mirror it with `dbxcarta-catalog.dense_ops`, also using a
`dbxcarta-ops` volume.

One difference from finance-genie matters. finance-genie's data catalog is
upstream-owned, so dbxcarta never creates it. dense owns its data catalog
`dense-schema_example` and must create it. See the blocker below.

## Concrete decisions

1. **Ops location:** `dbxcarta-catalog.dense_ops`, volume `dbxcarta-ops`.
   Mirrors finance-genie and schemapile naming.
2. **Teardown becomes multi-target.** dense now owns two things, the data catalog
   and the ops schema, so its teardown is the comma-separated form clean-schemapile
   adds: `catalog:dense-schema_example,schema:dbxcarta-catalog.dense_ops`. The
   first drops the data catalog with `dense_1000` inside it; the second drops only
   dense's ops schema. The shared `dbxcarta-catalog` is never itself dropped, so
   schemapile's and finance-genie's ops are safe.
3. **Keep `DBXCARTA_SCHEMAS=dense_1000` explicit.** This diverges from
   clean-schemapile, which blanks the list for auto-discovery. dense has exactly
   one stable data schema, there is no hand-maintained list to drift, and the
   preset's readiness check reads `DBXCARTA_SCHEMAS` to confirm `dense_1000` is
   present. Blanking it would turn readiness into "run materialize first" with
   nothing to verify.

## The blocker this must fix: who creates the data catalog

No `CREATE CATALOG` exists anywhere in the example source. The only thing that
creates a catalog today is `dbxcarta-submit bootstrap`, which derives it from
`DATABRICKS_VOLUME_PATH`. dense's `materialize.py` runs `CREATE SCHEMA IF NOT
EXISTS {catalog}.{schema}` and `CREATE TABLE`, never `CREATE CATALOG`.

So `dense-schema_example` exists today only because the volume path points into
it. Once `DATABRICKS_VOLUME_PATH` moves to the ops plane, bootstrap will create
`dbxcarta-catalog.dense_ops` and nothing will create `dense-schema_example`.
materialize's first `CREATE SCHEMA dense-schema_example.dense_1000` then fails on
a fresh workspace.

The fix: data-plane creation belongs to the data step, so `materialize.py`
issues `CREATE CATALOG IF NOT EXISTS {catalog}` before its first `CREATE SCHEMA`.
The catalog name is already guarded by the `_CATALOG_BLOCKLIST` check in
`load_config`. After this, bootstrap owns the ops plane and materialize owns the
data plane, each creating exactly what it writes to.

Note: clean-schemapile.md has the same latent gap. Nothing creates
`schemapile_lakehouse` once ops move out, and it works today only because that
catalog already exists from earlier runs. The same `CREATE CATALOG IF NOT EXISTS`
belongs in schemapile's `materialize.py`. Flag it there.

## Why there is no overlap between examples

The shared part is only the catalog `dbxcarta-catalog`. Each example gets its own
schema under it: `dense_ops`, `schemapile_ops`, `finance_genie_ops`. A volume is
namespaced under its schema, so `/Volumes/dbxcarta-catalog/dense_ops/dbxcarta-ops`
and `/Volumes/dbxcarta-catalog/finance_genie_ops/dbxcarta-ops` are distinct file
trees despite the shared `dbxcarta-ops` volume name. Wheels, runs, questions, and
the per-example `dbxcarta_run_summary` table all sit under the example's own
schema. Per-example ops cleanup is a single
`DROP SCHEMA dbxcarta-catalog.dense_ops CASCADE`, isolated from data and from
every other example.

## Changes

### Overlay: `examples/dense-schema/dbxcarta-overlay.env`

- Rewrite the stale comments: dense owns its own data catalog
  `dense-schema_example`; its ops live in `dbxcarta-catalog.dense_ops`.
- `DATABRICKS_VOLUME_PATH=/Volumes/dbxcarta-catalog/dense_ops/dbxcarta-ops`
- `DBXCARTA_SUMMARY_VOLUME=/Volumes/dbxcarta-catalog/dense_ops/dbxcarta-ops/dbxcarta/runs`
- `DBXCARTA_SUMMARY_TABLE=dbxcarta-catalog.dense_ops.dbxcarta_run_summary`
- `DBXCARTA_CLIENT_QUESTIONS=/Volumes/dbxcarta-catalog/dense_ops/dbxcarta-ops/dbxcarta/dense_questions.json`
- `DBXCARTA_TEARDOWN_TARGET=catalog:dense-schema_example,schema:dbxcarta-catalog.dense_ops`,
  with a comment that the first drops the data catalog and the second the ops
  schema.
- `DBXCARTA_CATALOG=dense-schema_example` and `DBXCARTA_SCHEMAS=dense_1000` stay.

### Materialize: `materialize.py`

- Issue `CREATE CATALOG IF NOT EXISTS {catalog_q}` once before the per-schema
  loop, using the existing `_execute`. This is the blocker fix above. Schemas and
  tables are otherwise unchanged.

### Teardown command

- No new code beyond clean-schemapile. dense's comma-separated target is parsed
  and dropped by the list-aware teardown that clean-schemapile lands. Confirm the
  dense target parses and dry-runs to two items.

### Standalone config: `config.py`

The `volume_path` property derives `/Volumes/{catalog}/{meta_schema}/{volume}`
from the data catalog, which conflates data and ops. Untangle it the way
clean-schemapile untangles schemapile's config:

- Make `volume_path` read `DATABRICKS_VOLUME_PATH` from env, falling back to the
  derived ops path `/Volumes/dbxcarta-catalog/dense_ops/dbxcarta-ops` rather than a
  data-catalog path.
- Make `questions_path` read `DBXCARTA_CLIENT_QUESTIONS` from env, falling back to
  the ops location, not `/Volumes/{catalog}/_meta/schemapile_volume/...`.
- The `catalog` field stays the data catalog `dense-schema_example` that
  materialize writes tables into.

### Standalone config: `.env` and `.env.sample`

- Point `DATABRICKS_VOLUME_PATH`, `DBXCARTA_SUMMARY_VOLUME`,
  `DBXCARTA_SUMMARY_TABLE`, and `DBXCARTA_CLIENT_QUESTIONS` at the ops location.
- Fix `.env.sample`, which still sets `DBXCARTA_CATALOG=schemapile_lakehouse`, to
  `DBXCARTA_CATALOG=dense-schema_example`.
- The leftover `SCHEMAPILE_META_SCHEMA` and `SCHEMAPILE_VOLUME` keys are no longer
  the source of the ops path once `volume_path` reads env directly. Drop them from
  the dense `.env`/`.env.sample`, or repurpose them as the ops fallback inputs, but
  do not let them keep deriving an ops path inside the data catalog.

### Preset: `preset.py`

- `_DEFAULT_CATALOG = "schemapile_lakehouse"` is wrong: readiness queries this
  catalog's `information_schema.schemata` for `dense_1000`, which lives in
  `dense-schema_example`. Default the preset catalog from `DBXCARTA_CATALOG`,
  falling back to `dense-schema_example`, so readiness checks the data catalog.
- `upload_questions` already reads `DBXCARTA_CLIENT_QUESTIONS`, so it follows the
  overlay to the ops plane with no code change.

### Docs: `examples/dense-schema/README.md`

- Replace every "shared catalog" and "leaves the shared catalog for schemapile"
  reference with the standalone-catalog description.
- State that ops now live in `dbxcarta-catalog.dense_ops`, consistent with
  finance-genie and schemapile, and that teardown removes both the data catalog
  and the ops schema.

### Tests

- Add a config test asserting `volume_path` and `questions_path` read the ops env
  vars and fall back to the ops path, not a data-catalog path.
- Add a materialize test asserting `CREATE CATALOG IF NOT EXISTS` is issued before
  the first `CREATE SCHEMA`.
- Add a preset test asserting readiness queries the data catalog
  `dense-schema_example`, not `schemapile_lakehouse`.

## What this removes

- The `_meta`-inside-data-catalog conflation for dense, by layout rather than by
  filter.
- The stale `schemapile_lakehouse` references in the overlay comments, the
  `.env.sample`, and the preset default.
- The teardown that orphaned dense's catalog and ops schema.

## What this keeps

- `DBXCARTA_SCHEMAS=dense_1000` explicit, so readiness stays meaningful.
- Evaluation run history and uploaded questions across data teardown and rebuild,
  since ops live outside the data catalog.

## One-time operational note

The ops plane moves, so the first run after this change must bootstrap the new
`dbxcarta-catalog.dense_ops` location and re-upload the dense questions there. The
old `dense-schema_example._meta` schema and its `schemapile_volume` volume can be
dropped by hand once nothing references them.
