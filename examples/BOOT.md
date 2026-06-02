# Bootstrap automation: what is missing across the examples

This note records what each example needs provisioned before its first
`-ingest` run, what provides that today, and what we would add to automate it
the same way for every example.

## The problem

`publish-wheels` and `ingest` assume three Unity Catalog objects already exist:
a catalog, a bookkeeping schema, and a volume that holds the wheels, the
question set, and the run artifacts. When any of them is absent the run fails
with `Schema '<catalog>.<schema>' does not exist`, raised the moment
`publish-wheels` tries to upload a wheel into the volume.

Only SchemaPile ships a command that provisions these objects. Finance Genie and
dense-schema have no equivalent, so an operator has to create the objects by
hand or already know they exist. The goal is one consistent bootstrap path per
example.

## Reference implementation: SchemaPile

`examples/schemapile/src/dbxcarta_schemapile_example/bootstrap.py` is the model.
It is wired as the `dbxcarta-schemapile-bootstrap` console script in
`examples/schemapile/pyproject.toml`. It:

- loads the example dotenv and `SchemaPileConfig`,
- resolves the warehouse id through `utils.read_required_warehouse_id`,
- builds a `WorkspaceClient` with `dbxcarta.client.databricks.build_workspace_client`,
- runs three idempotent statements through the SQL warehouse:
  - `CREATE CATALOG IF NOT EXISTS <catalog>`
  - `CREATE SCHEMA IF NOT EXISTS <catalog>._meta`
  - `CREATE VOLUME IF NOT EXISTS <catalog>._meta.<volume>`
- supports `--drop-all --yes-i-mean-it` for teardown,
- guards the catalog name against a blocklist of known project catalogs in
  `config.load_config()`.

It is idempotent and safe to re-run.

## Per-example gap

| Example | Objects ingest needs | Provisioned by today | Gap |
|---------|----------------------|----------------------|-----|
| SchemaPile | `schemapile_lakehouse`, `schemapile_lakehouse._meta`, `schemapile_lakehouse._meta.schemapile_volume` | `dbxcarta-schemapile-bootstrap` | None |
| Dense schema | The same `schemapile_lakehouse`, `_meta`, and `schemapile_volume` as SchemaPile | Nothing of its own. It reuses the SchemaPile catalog, so today you must run the SchemaPile bootstrap first | No command of its own; the dependency on the SchemaPile bootstrap is implicit |
| Finance Genie | Ops plane: catalog `dbxcarta-catalog`, schema `finance_genie_ops`, volume `dbxcarta-ops`. Data catalogs come from the upstream Finance Genie project | The data catalogs are provisioned upstream. The ops plane is provisioned by nothing | No command; the ops schema and volume must be created by hand with the generic `dbxcarta-submit schema create` / `volume create` |

### What each example already exposes

- **Dense schema** has the values a bootstrap needs:
  `examples/dense-schema/src/dbxcarta_dense_schema_example/config.py` defines
  `DenseSchemaConfig.catalog`, `.meta_schema`, and `.volume`, and
  `examples/dense-schema/src/dbxcarta_dense_schema_example/utils.py` already
  provides `load_dotenv_file` and `read_required_warehouse_id`. A dense
  bootstrap would be close to a copy of the SchemaPile one. Because it targets
  the same objects as SchemaPile, a separate dense bootstrap is mostly redundant
  with the SchemaPile one.
- **Finance Genie** exposes the ops location on the preset object in
  `examples/finance-genie/src/dbxcarta_finance_genie_example/finance_genie.py`:
  `FinanceGeniePreset.ops_catalog`, `.ops_schema`, `.ops_volume`, and the
  derived `.volume_path`. It has no `config.py`, no `utils.py`, and no console
  scripts, so a bootstrap would start from the preset object rather than a
  config dataclass.

## Design constraint

`dbxcarta-spark` stays focused on data ingest: the graph contract, extract,
transform, embed, and load. Provisioning a catalog, schema, and volume is setup
that happens before ingest, not part of it, so it does not belong in
`dbxcarta-spark`. The rule for this work:

- Do not modify `dbxcarta-spark` to add a bootstrap protocol, CLI flag, or
  provisioning helper.
- Each example owns its own bootstrap, the way SchemaPile already does.
- Examples may still import the existing public helpers from `dbxcarta-spark`,
  for example `build_workspace_client` and `quote_identifier` in
  `dbxcarta.spark.databricks`. Consuming the current surface is fine; expanding
  it is not.

## What needs to be added

The two shapes below are kept for the record. The example-local one (Option B)
is the chosen path because it honors the constraint above. Option A is recorded
as rejected.

### Option A (rejected): a preset bootstrap capability in dbxcarta-spark

The preset protocol in
`packages/dbxcarta-spark/src/dbxcarta/spark/presets.py` already models optional
abilities as capability protocols: `ReadinessCheckable.readiness` and
`QuestionsUploadable.upload_questions`, each surfaced as a flag on the
`dbxcarta preset <spec>` CLI in
`packages/dbxcarta-spark/src/dbxcarta/spark/cli.py`. Bootstrapping fits the same
shape and is the only provisioning step with no hook.

Add:

1. A `Provisionable` capability protocol in `presets.py`:

   ```python
   @runtime_checkable
   class Provisionable(Protocol):
       def bootstrap(self, ws: "WorkspaceClient", warehouse_id: str) -> None:
           ...
   ```

   `bootstrap` runs the idempotent `CREATE CATALOG / SCHEMA / VOLUME IF NOT
   EXISTS` statements for whatever objects that example needs.

2. A `--bootstrap` action on the `dbxcarta preset` CLI alongside
   `--check-ready` and `--upload-questions`, with the same `--warehouse-id`
   resolution the other warehouse-touching actions use.

3. A small shared helper that runs the three `CREATE ... IF NOT EXISTS`
   statements given a catalog, schema, and volume, so each preset's `bootstrap`
   is a few lines. The SchemaPile `_provision` body is the template.

4. A `bootstrap` method on each preset:
   - **SchemaPile** and **dense-schema**: create `<catalog>`, `<catalog>._meta`,
     `<catalog>._meta.<volume>`. Since both target `schemapile_lakehouse`, the
     dense preset can delegate to the same logic; running either is enough.
   - **Finance Genie**: create only the ops plane, `ops_catalog.ops_schema` and
     the `ops_volume` under it. Do not create the data catalogs; those are
     owned upstream.

Then every example bootstraps the same way:

```bash
DBXCARTA_ENV_FILE=examples/<name>/dbxcarta-overlay.env \
  uv run dbxcarta preset <import-spec> --bootstrap
```

Keep `dbxcarta-schemapile-bootstrap` as a thin wrapper for backward
compatibility, or retire it once the docs point at `--bootstrap`.

This option is rejected: it would add a provisioning protocol, a CLI action, and
a CREATE helper to `dbxcarta-spark`, which we are keeping focused on ingest.

### Option B (chosen): per-example bootstrap, no dbxcarta-spark change

Mirror SchemaPile in each example so every example exposes its own bootstrap
console script. SchemaPile is already done and is the template.

- **Dense schema**: add `bootstrap.py` next to `materialize.py`, wire a
  `dbxcarta-dense-bootstrap` script in `examples/dense-schema/pyproject.toml`,
  and reuse the existing `config.py` (`catalog`, `meta_schema`, `volume`) and
  `utils.py` (`load_dotenv_file`, `read_required_warehouse_id`). The body is
  close to a copy of SchemaPile's `_provision`. Because it targets the same
  objects as SchemaPile, document that running either bootstrap suffices, and
  decide the shared-catalog question in Open questions before duplicating.
- **Finance Genie**: add a `utils.py` with `load_dotenv_file` and
  `read_required_warehouse_id` (it has neither today), a `bootstrap.py` that
  reads `ops_catalog`, `ops_schema`, and `ops_volume` from the preset object in
  `finance_genie.py`, and a `dbxcarta-finance-genie-bootstrap` script in its
  `pyproject.toml`. It creates only the ops plane; the data catalogs are owned
  upstream.

Each `bootstrap.py` imports `build_workspace_client` from
`dbxcarta.client.databricks` and `quote_identifier` from
`dbxcarta.spark.databricks`, then runs the three idempotent `CREATE CATALOG /
SCHEMA / VOLUME IF NOT EXISTS` statements, exactly as SchemaPile does today.

Cost and trade-off: the small CREATE-statement helper is duplicated across the
three example packages. That duplication is the price of keeping `dbxcarta-spark`
ingest-only, and it is accepted. There is no shared examples package to hold a
common helper, and creating one solely for this is not worth it. If the
duplication later becomes a burden, revisit a shared examples utility package
rather than pushing the logic back into `dbxcarta-spark`.

### Makefile wiring

Optionally make the bootstrap a prerequisite of each `-ingest` target in the
repo-root `Makefile` so the objects are guaranteed before `publish-wheels`. The
operations are idempotent, so the cost is a few warehouse calls per ingest. This
requires catalog-create or schema-create privilege on every ingest run, so it
may be better left as an explicit one-time step the README documents.

## Status: implemented

Per the design constraint, no task touched `dbxcarta-spark`. Implemented as
Option B.

- [x] Dense schema: added
      `examples/dense-schema/src/dbxcarta_dense_schema_example/bootstrap.py` and
      the `dbxcarta-dense-bootstrap` console script, reusing the existing
      `config.py` and `utils.py`.
- [x] Finance Genie: added `utils.py`, `bootstrap.py`, and the
      `dbxcarta-finance-genie-bootstrap` console script. It provisions only the
      ops plane and reads `ops_catalog`, `ops_schema`, `ops_volume` from the
      preset object.
- [x] SchemaPile unchanged; it is the template.
- [x] Updated the root `README.md` "Quick start: run an example" section and the
      dense-schema and finance-genie READMEs to name each bootstrap command.
- [ ] Decide whether the `-ingest` Makefile targets depend on bootstrap. Left
      out for now: bootstrap is an idempotent one-time step, but wiring it into
      every ingest would require catalog-create privilege on every run. Pending
      a decision.

### Teardown decision

Both new examples reuse a shared catalog (`schemapile_lakehouse` for dense,
`dbxcarta-catalog` for Finance Genie ops), so neither `--drop-all` drops the
catalog. Each drops only the schema it owns and keeps the shared catalog and
volume:

- `dbxcarta-dense-bootstrap --drop-all --yes-i-mean-it` drops the dense data
  schema (for example `dense_1000`).
- `dbxcarta-finance-genie-bootstrap --drop-all --yes-i-mean-it` drops the
  `finance_genie_ops` schema.

Full catalog teardown for the SchemaPile lakehouse stays with
`dbxcarta-schemapile-bootstrap --drop-all --yes-i-mean-it`. Both guards require
`--yes-i-mean-it`, matching SchemaPile.

## Open questions

- **Resolved.** For dense-schema, the intended model is that it shares the
  SchemaPile `schemapile_lakehouse` catalog, `_meta` schema, and
  `schemapile_volume` volume, and lives in its own `dense_*` data schema. The
  `examples/dense-schema/dbxcarta-overlay.env` comment states this. The dense
  bootstrap therefore provisions the shared objects idempotently and drops only
  the dense data schema.
- **Resolved.** Finance Genie bootstrap stays strictly ops-plane and does not
  create or assert the upstream data catalogs. Readiness of the upstream tables
  remains the job of the existing `--check-ready` check.
- Should the bootstrap reuse the same catalog blocklist guard SchemaPile has in
  `load_config()`, applied uniformly across presets? Dense reuses its
  `config.load_config()` guard already; Finance Genie reads fixed names from the
  preset object, so no guard applies there yet.
