# Provisioning and teardown across the examples (superseded note)

This note used to argue for a per-example `bootstrap.py` in every example
package. That design shipped and was then retired. It is replaced by two
operator commands in `dbxcarta-submit`, one build and one teardown, shared by
every example. The history is kept here so the reasoning is not lost; for the
implemented design and its phasing see `cleanup.md` at the repo root.

## The problem (unchanged)

`publish-wheels` and `ingest` assume three Unity Catalog objects already exist:
a catalog, a bookkeeping schema, and a volume that holds the wheels, the
question set, and the run artifacts. When any of them is absent the run fails
with `Schema '<catalog>.<schema>' does not exist`, raised the moment
`publish-wheels` tries to upload a wheel into the volume. When an example is
finished with, those same objects need a way to be removed.

## What we do now: two commands, one implementation each

Setup and teardown are not per-example code anymore. They are two commands in
the operator toolbox (`dbxcarta-submit`), each with a single implementation that
reads the selected example's committed overlay:

- **`dbxcarta-submit bootstrap`** parses the overlay's `DATABRICKS_VOLUME_PATH`
  (`/Volumes/<catalog>/<schema>/<volume>`) and runs `CREATE CATALOG / SCHEMA /
  VOLUME IF NOT EXISTS` against a SQL warehouse. It is idempotent and is the
  first step of every `e2e-<example>-ingest` make target.
- **`dbxcarta-submit teardown`** reads the overlay's explicit
  `DBXCARTA_TEARDOWN_TARGET`, a comma-separated list of
  `schema:<catalog>.<schema>` or `catalog:<catalog>` targets, and drops each
  with `CASCADE`. It requires `--yes-i-mean-it`, is never chained into ingest,
  and is exposed as `make e2e-<example>-teardown`.

Both run locally against a SQL warehouse (needing `DATABRICKS_WAREHOUSE_ID` and
operator privilege), and both refuse a target on a shared protected-name
blocklist, so a typo can never point either at a system catalog. The pure UC
primitives live in `dbxcarta.spark.databricks` (`parse_volume_path`,
`UC_PROTECTED_NAMES`, `check_not_protected`); the operator DDL lives in
`dbxcarta.submit.uc_admin`, kept out of the cluster-installed `dbxcarta-spark`
ingest wheel.

Each example declares its own teardown target(s) because what each drops
genuinely differs and cannot be derived from the volume path:

| Example | `DBXCARTA_TEARDOWN_TARGET` | Effect |
|---------|----------------------------|--------|
| SchemaPile | `catalog:schemapile_lakehouse,schema:dbxcarta-catalog.schemapile_ops` | drops the data catalog and its ops schema; the shared `dbxcarta-catalog` is left intact |
| Dense schema | `schema:dense-schema_example.dense_1000` | drops only its own data schema |
| Finance Genie | `schema:dbxcarta-catalog.finance_genie_ops` | drops only the ops schema; upstream data catalogs are untouched |

## Why the per-example design was retired

The original plan (Option B below) mirrored SchemaPile's `bootstrap.py` into each
example. That meant three copies of the same `CREATE ... IF NOT EXISTS` and
`DROP ... CASCADE` logic, each deciding for itself which catalog/schema/volume it
was talking about, and three console scripts to keep in sync. The overlay already
holds the right answer for every example in its volume path, so a single
implementation reading the overlay removes the duplication entirely. The two
commands replace all three `bootstrap.py` scripts, their entry points, and
finance-genie's bootstrap-only `utils.py`.

## Design constraint (still honored)

`dbxcarta-spark` stays focused on data ingest: the graph contract, extract,
transform, embed, and load. Provisioning happens before ingest, not as part of
it, so it does not belong in the cluster wheel. The two commands satisfy this by
living in `dbxcarta-submit`, with only the side-effect-free primitives shared
from `dbxcarta-spark`.

## Historical options (for the record)

### Option A (rejected): a preset bootstrap capability in dbxcarta-spark

Add a `Provisionable` capability protocol and a `--bootstrap` action to the
`dbxcarta preset` CLI, mirroring `ReadinessCheckable` / `QuestionsUploadable`.
Rejected because it would push provisioning protocol, a CLI action, and a CREATE
helper into `dbxcarta-spark`, which is kept ingest-only.

### Option B (shipped, then retired): per-example bootstrap, no dbxcarta-spark change

Mirror SchemaPile's `bootstrap.py` into each example with its own
`dbxcarta-<example>-bootstrap` console script and a `--drop-all` teardown flag.
This shipped and worked, but left the duplication described above. It is now
replaced by the two shared commands.
