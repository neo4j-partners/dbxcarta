# Plan: simplify SchemaPile schema selection

## The problem in one line

The SchemaPile example hand-maintains a list of 20 schema names across three
places (`.env.generated`, copied into `dbxcarta-overlay.env`, kept in sync by
a manual step), even though the dbxcarta core can already discover every
schema in a catalog on its own.

## Why this is now fixable

`DBXCARTA_SCHEMAS` is a filter, not a requirement. A blank value already means
"ingest every schema in the catalog" (`extract.py:87`). The only reason
SchemaPile could not use that before was a shared catalog: dense-schema kept
its `dense_1000` data in `schemapile_lakehouse`, so blank would have mixed the
two datasets.

That is no longer true. dense-schema now uses its own catalog
(`dense-schema_example`), and finance-genie targets its own catalogs. So
`schemapile_lakehouse` is exclusively SchemaPile: the `sp_*` data schemas plus
the `_meta` ops schema. Blank-list now selects exactly SchemaPile's data, with
one wrinkle.

## The one wrinkle: `_meta`

Bootstrap creates an ops schema to hold the volume and the run-summary table.
For SchemaPile that schema is `_meta`, and it lives inside the data catalog. A
blank list would sweep it in, so `dbxcarta_run_summary` would be ingested as a
data table and would grow on every run. The extract must skip it.

The clean way to skip it is config-derived, not a hardcoded name. The overlay
already spells out the ops location as `/Volumes/<catalog>/<schema>/<volume>`
in `DATABRICKS_VOLUME_PATH`, and the bootstrap cleanup added `parse_volume_path`
as a shared primitive that reads those three names. So the extract can exclude
the volume path's schema when that schema falls inside an ingested catalog.
This stays config-driven, matches the cleanup.md principle that the overlay is
the single source of truth, and works for every example:

- SchemaPile: volume path names `schemapile_lakehouse/_meta`; `_meta` is in the
  ingested catalog, so it is excluded.
- dense-schema: volume path names `dense-schema_example/_meta`; excluded the
  same way.
- finance-genie: volume path names `dbxcarta-catalog/finance_genie_ops`, a
  different catalog from the ingested data catalog, so nothing is excluded and
  behavior is unchanged.

## The fix

1. In the core extract, derive the ops schema from `DATABRICKS_VOLUME_PATH`
   using the existing `parse_volume_path`, and exclude that `(catalog, schema)`
   from the discovered schemata when the catalog is one being ingested. Leave
   the existing `information_schema` exclusion as-is. A non-blank
   `DBXCARTA_SCHEMAS` still wins, so other integrations are untouched.

2. Blank out `DBXCARTA_SCHEMAS` in the SchemaPile overlay.

3. Stop the materialize step from writing a schema list. It no longer needs to
   emit `.env.generated`, since nothing downstream consumes that list.

4. Delete `.env.generated` and remove it from gitignore.

5. Update the SchemaPile README: remove every mention of `.env.generated`, the
   copy step, and the instruction to paste `DBXCARTA_SCHEMAS` into the overlay.
   The new flow is materialize, then run, with no schema list carried by hand.

6. Update tests: cover the volume-path ops-schema exclusion in the core extract,
   and adjust any SchemaPile test that asserts the materialize step writes a
   schema list.

## What this removes

- The `.env.generated` file.
- The manual copy of `DBXCARTA_SCHEMAS` into the overlay.
- The drift risk when candidates are added or removed, since ingest now reads
  whatever is in the catalog at run time.

## What this keeps

- The `DBXCARTA_SCHEMAS` filter itself, untouched for integrations that point
  at shared catalogs and genuinely need to narrow the set.
- The ops schema out of the graph, for every example, by one config-derived
  rule rather than a hardcoded name.

## Decision to confirm before coding the core change

Exclude the ops schema by deriving it from `DATABRICKS_VOLUME_PATH` in the core
extract (recommended, config-driven, helps every example), or keep it out of
the core and instead leave SchemaPile with its explicit list and only remove
the `.env.generated`/copy dance.
