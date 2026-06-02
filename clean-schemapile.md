# Plan: separate SchemaPile's ops plane from its data plane

## Status: COMPLETE

Implemented in one pass. Full suite green (488 passed, 1 skipped). Two design
gaps surfaced during implementation and were resolved (both confirmed with the
user / entailed by the agreed design):

- **Preset readiness** keyed off `DBXCARTA_SCHEMAS`, so a blank list made
  `--check-ready` always fail. Reworked `readiness()` to query the catalog for
  materialized schemas (ready when at least one non-`information_schema` schema
  exists). Test updated to drive it through a fake workspace client.
- **Data-catalog creation** was orphaned: bootstrap now provisions the ops
  plane (from the volume path), so nothing created `schemapile_lakehouse`.
  `materialize()` now runs `CREATE CATALOG IF NOT EXISTS` for its own data
  catalog. Only schemapile needed this; dense-schema keeps ops co-located, and
  finance-genie's data catalogs are upstream.

## Goal

Make `schemapile_lakehouse` hold only data, so `DBXCARTA_SCHEMAS` can be blank
and the ingest auto-discovers every schema. This removes the hand-maintained
schema list, `.env.generated`, the copy step, and the drift. No core change is
needed: a pure-data catalog means "ingest everything" is simply correct.

## Why this works now

dense-schema moved to its own catalog and finance-genie already targets its own
catalogs, so `schemapile_lakehouse` is exclusively SchemaPile: the `sp_*` data
schemas plus the `_meta` ops schema. The only thing keeping a blank list from
working is that `_meta` (the wheels volume and the run-summary table) lives
inside the data catalog. Moving ops out removes that, with no exclusion logic.

## The pattern to mirror: finance-genie

finance-genie keeps ops out of its data catalogs. Its volume and summary live
in `dbxcarta-catalog.finance_genie_ops` with a `dbxcarta-ops` volume, and its
teardown drops only that ops schema. SchemaPile will mirror this exactly, using
a `schemapile_ops` schema in the shared `dbxcarta-catalog`.

## Concrete decisions

1. **Ops location:** `dbxcarta-catalog.schemapile_ops`, volume `dbxcarta-ops`.
   Mirrors finance-genie's naming.
2. **Teardown becomes multi-target.** Plane separation means schemapile now owns
   two things, the data catalog and the ops schema, and one teardown target can
   only drop one. Extend `DBXCARTA_TEARDOWN_TARGET` to accept a comma-separated
   list so an example declares exactly what it owns and one command removes all
   of it. schemapile becomes
   `catalog:schemapile_lakehouse,schema:dbxcarta-catalog.schemapile_ops`;
   finance-genie stays a single `schema:` target (its data is upstream). The
   shared `dbxcarta-catalog` is never itself dropped, so other examples' ops are
   safe.
3. **No ingest-core change.** A pure-data catalog needs no `_meta` filter in
   extract. The only command change is the teardown list parsing.

## Why there is no overlap between examples

The shared part is only the catalog `dbxcarta-catalog`. Each example gets its
own schema under it (`schemapile_ops`, `finance_genie_ops`), and a volume is
namespaced under its schema, so `/Volumes/dbxcarta-catalog/schemapile_ops/
dbxcarta-ops` and `/Volumes/dbxcarta-catalog/finance_genie_ops/dbxcarta-ops` are
distinct file trees despite the shared `dbxcarta-ops` volume name. Wheels, runs,
questions, and the per-example `dbxcarta_run_summary` table all sit under the
example's own schema. Per-example ops cleanup is a single
`DROP SCHEMA dbxcarta-catalog.<example>_ops CASCADE`, isolated from data and
from every other example.

## Changes

### Overlay: `examples/schemapile/dbxcarta-overlay.env`

- `DBXCARTA_SCHEMAS=` (blank), with a comment that the catalog is pure data so
  every schema is auto-discovered.
- `DATABRICKS_VOLUME_PATH=/Volumes/dbxcarta-catalog/schemapile_ops/dbxcarta-ops`
- `DBXCARTA_SUMMARY_VOLUME=/Volumes/dbxcarta-catalog/schemapile_ops/dbxcarta-ops/dbxcarta/runs`
- `DBXCARTA_SUMMARY_TABLE=dbxcarta-catalog.schemapile_ops.dbxcarta_run_summary`
- `DBXCARTA_CLIENT_QUESTIONS=/Volumes/dbxcarta-catalog/schemapile_ops/dbxcarta-ops/dbxcarta/questions.json`
- `DBXCARTA_TEARDOWN_TARGET=catalog:schemapile_lakehouse,schema:dbxcarta-catalog.schemapile_ops`,
  with a comment that the first drops the data catalog and the second the ops
  schema.

### Teardown command: `uc_admin.py` and the submit handler

- Extend `DBXCARTA_TEARDOWN_TARGET` to a comma-separated list. Parse each item
  with the existing `parse_teardown_target`, run the protected-name guard per
  item, and drop each in order. A single target keeps working unchanged, so
  finance-genie and dense-schema are unaffected.
- The dry-run path prints every target it would drop.
- Update the teardown unit tests for the list form, and the cleanup.md Phase 3
  description, since this widens the just-landed single-target contract.

### Standalone config: `config.py` and `.env` / `.env.sample`

The standalone tooling derives its volume path from the data catalog
(`/Volumes/{catalog}/{meta_schema}/{volume}`), which conflates data and ops.
Untangle it:

- Make `config.volume_path` and `questions_path` read `DATABRICKS_VOLUME_PATH`
  and `DBXCARTA_CLIENT_QUESTIONS` directly from env, falling back to a derived
  ops path rather than a data-catalog path. The `catalog` field stays the data
  catalog that materialize writes tables into.
- Point `DATABRICKS_VOLUME_PATH`, `DBXCARTA_SUMMARY_*`, and
  `DBXCARTA_CLIENT_QUESTIONS` in `.env` and `.env.sample` at the ops location.
- `DBXCARTA_SCHEMAS` in `.env` is already blank; update the stale comment that
  tells the user to copy from `.env.generated`.

### Materialize: `materialize.py`

- Remove the `--env-out` argument, the `.env.generated` write, and the print
  line. Update the module docstring. Materialize still creates the `sp_*` data
  schemas and tables in the data catalog, unchanged.

### Remove `.env.generated` and its references

- Delete `examples/schemapile/.env.generated`. No `.gitignore` edit needed; the
  `.env.*` pattern already covers it.
- `scripts/dump_question_context.py`: drop the `load_dotenv(".env.generated")`
  line. The script already treats a blank schema list as "all schemas" in its
  Cypher, so auto-discovery is the correct behavior with no list to load.
- Remove `.env.generated` mentions in `scripts/README.md` and the `preset.py`
  docstring.

### Docs: `examples/schemapile/README.md`

- Remove every `.env.generated` reference and the copy step. The new flow is:
  materialize the tables, then run, with no schema list carried by hand.
- Note the ops plane now lives in `dbxcarta-catalog.schemapile_ops`, consistent
  with finance-genie.

### Tests

- `test_config.py`: update the `volume_path` assertion for the new env-driven
  derivation.
- Check `test_materialize.py` for any assertion that the step writes a schema
  list or `.env.generated`, and drop it.
- The `meta_schema="_meta"` fixtures in the example tests can stay; `_meta` is
  still the default for the standalone config's derive fallback.

## What this removes

- `.env.generated`, the manual copy, and the drift when candidates change.
- `_meta` from the ingested graph, by layout rather than by filter.

## What this keeps

- The `DBXCARTA_SCHEMAS` filter, untouched for shared-catalog integrations.
- Evaluation run history and uploaded questions across data teardown/rebuild,
  since ops live outside the data catalog.

## One-time operational note

The ops plane moved, so the first run after this change must bootstrap the new
`dbxcarta-catalog.schemapile_ops` location and re-upload the questions there.
The old `schemapile_lakehouse._meta` schema and volume can be dropped by hand
once nothing references them.
