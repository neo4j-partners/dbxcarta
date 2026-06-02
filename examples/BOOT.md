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

## What needs to be added

There are two shapes. The first removes the duplication and is the recommended
target; the second matches the SchemaPile pattern with the least new surface.

### Option A (recommended): a preset bootstrap capability

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
   `--print-env`, `--check-ready`, `--upload-questions`, and `--run`, with the
   same `--warehouse-id` resolution the other warehouse-touching actions use.

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

### Option B: per-example bootstrap console scripts

Mirror SchemaPile in each example:

- **Dense schema**: add `bootstrap.py` next to `materialize.py`, wire a
  `dbxcarta-dense-bootstrap` script in `examples/dense-schema/pyproject.toml`,
  and reuse the existing `config.py` and `utils.py`. Because it targets the same
  objects as SchemaPile, document that running either bootstrap suffices.
- **Finance Genie**: add a `utils.py` with `load_dotenv_file` and
  `read_required_warehouse_id` (it has neither today), a `bootstrap.py` that
  reads `ops_catalog`, `ops_schema`, and `ops_volume` from the preset, and a
  `dbxcarta-finance-genie-bootstrap` script in its `pyproject.toml`.

This is more code and duplicates the SchemaPile logic three times. Option A
centralizes it.

### Makefile wiring (either option)

Optionally make the bootstrap a prerequisite of each `-ingest` target in the
repo-root `Makefile` so the objects are guaranteed before `publish-wheels`. The
operations are idempotent, so the cost is a few warehouse calls per ingest. This
requires catalog-create or schema-create privilege on every ingest run, so it
may be better left as an explicit one-time step the README documents.

## Concrete task list

- [ ] Add a `Provisionable` protocol and a shared `create catalog/schema/volume`
      helper to `dbxcarta-spark`.
- [ ] Add `--bootstrap` to the `dbxcarta preset` CLI with warehouse-id
      resolution.
- [ ] Implement `bootstrap` on the SchemaPile, dense-schema, and Finance Genie
      presets, with Finance Genie creating only the ops plane.
- [ ] Decide whether `dbxcarta-schemapile-bootstrap` stays as a wrapper or is
      retired.
- [ ] Update the root `README.md` "Quick start: run an example" section and each
      example README to point at the single bootstrap command.
- [ ] Decide whether the `-ingest` Makefile targets depend on bootstrap.

## Open questions

- Should Finance Genie bootstrap also assert the upstream data catalogs exist,
  or stay strictly ops-plane and leave data provisioning to the upstream
  project? Asserting would overlap with the existing `--check-ready` readiness
  check.
- Should the bootstrap reuse the same catalog blocklist guard SchemaPile has in
  `load_config()`, applied uniformly across presets?
- For dense-schema, is the intended model that it always shares the SchemaPile
  catalog, or should it own a separate catalog so the two examples do not
  collide on `schemapile_lakehouse._meta`?
