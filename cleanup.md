# Cleanup Plan: Two Commands to Build and Tear Down Every Example

## The problem in plain words

Before the pipeline can run, three storage things must already exist in
Databricks: a catalog, a schema, and a volume. A volume is just a folder that
holds files. If any of the three is missing, the run fails right away. When an
example is finished with, those same things need a way to be removed.

Today each example carries its own setup script, and each script does two
different jobs at once: it builds the catalog/schema/volume, and (under
`--drop-all`) it tears them down. Each script also decides for itself, in a
slightly different way, which catalog/schema/volume it is talking about.
Three copies of the same `CREATE ... IF NOT EXISTS` and `DROP` logic is three
things to keep in sync, and they will drift.

There is one place that already holds the right answer for every example: the
committed overlay settings file. Its storage folder path is written as
`/Volumes/<catalog>/<schema>/<volume>`, so it spells out the catalog, the
schema, and the volume all at once, and it always points at the plane setup
needs (the ops/volume plane) even for an example like finance-genie whose data
catalog is a different, upstream-owned catalog.

## Scope of this plan

Replace the per-example setup scripts with **exactly two operator commands**:

- `dbxcarta-submit bootstrap` builds the catalog, schema, and volume.
- `dbxcarta-submit teardown` removes an example's footprint.

This is a complete switch, not a partial one. When it is done, no per-example
`bootstrap.py` script, entry point, or duplicated build/teardown logic remains.
There is one build implementation and one teardown implementation, both reading
from the overlay, both run locally against a SQL warehouse.

A small, orthogonal env cleanup rides along in Phase 1 (drop one dead alias;
the two runner-owned `DATABRICKS_`-prefixed vars stay untouched, with the
reason recorded).

## Progress

- [x] **Phase 1** — dead `DBXCARTA_EMBED_ENDPOINT` alias dropped; runner-owned
  vars kept.
- [x] **Phase 2** — shared UC primitives + `bootstrap` and `teardown` commands
  landed, with unit tests; full suite green (474 passed).
- [x] **Phase 3** — `DBXCARTA_TEARDOWN_TARGET` added to all three overlays;
  `teardown --dry-run` resolves each.
- [x] **Phase 4** — `bootstrap` wired as the first step of every ingest target;
  `e2e-<example>-teardown` targets added (run by hand, `--yes-i-mean-it`).
- [x] **Phase 5** — all three per-example `bootstrap.py`, their entry points, and
  finance-genie's `utils.py` deleted; suite green (482 passed).
- [x] **Phase 6** — READMEs + planning note rewritten to the two commands;
  dry-runs, grep sweep, and the suite (482 passed) all verified.

## Key goals

- **Two commands, one implementation each.** `bootstrap` and `teardown` live in
  the operator toolbox (`dbxcarta-submit`) and share a single UC-admin helper.
  No third copy, nothing per-example left to drift.
- **One source of truth per command, from the overlay.** `bootstrap` reads the
  volume path and creates exactly its catalog/schema/volume. `teardown` reads one
  explicit `DBXCARTA_TEARDOWN_TARGET`, because what each example drops is
  genuinely different and cannot be derived from the volume path.
- **Build is automatic and safe; teardown is deliberate.** `bootstrap` is
  idempotent (`CREATE ... IF NOT EXISTS`) and runs before every ingest.
  `teardown` never runs on its own: it requires `--yes-i-mean-it` and does
  nothing without it. The same protected-name blocklist guards both, so a typo
  can never point either command at a shared system catalog.
- **Local, like today.** Both commands build a workspace client and run their
  SQL through a warehouse, needing `DATABRICKS_WAREHOUSE_ID` and operator
  privilege. Neither spins up a cluster job.

## How the plan is phased

### Phase 1 (done): Drop the dead embed-endpoint alias; keep the runner-owned env vars as-is

An earlier draft of this plan renamed two settings off the `DATABRICKS_` prefix
(`DATABRICKS_VOLUME_PATH` -> `DBXCARTA_VOLUME_PATH`,
`DATABRICKS_SECRET_SCOPE` -> `DBXCARTA_SECRET_SCOPE`) on the theory that dbxcarta
reads them itself and the Databricks SDK never does, so the rename was
mechanically safe. That theory is wrong. The relevant reader is not the SDK; it
is the pinned `databricks-job-runner` (`==0.6.2`) that the submit path delegates
to, and it reads **both** names by exact string. They are part of its hard-coded
`CORE_KEYS`, in the same category as `DATABRICKS_WORKSPACE_DIR` and
`DATABRICKS_CLUSTER_ID`:

- `DATABRICKS_VOLUME_PATH` is how the runner derives the wheel directory
  (`{DATABRICKS_VOLUME_PATH}/wheels/`) and every upload path (`runner.py`).
  Renaming it in the overlays breaks `publish-wheels`.
- `DATABRICKS_SECRET_SCOPE` is forwarded to the cluster as a job parameter
  (`config.py`) and read on the cluster by the bootstrap's secret injection
  (`inject.py`) to fetch the `NEO4J_*` secrets. Renaming it means no scope
  reaches the cluster, so the Neo4j connection is left without credentials and
  ingest fails.

So these two belong to the same category as the standard variables we leave
alone, not to dbxcarta's own `DBXCARTA_` settings. dbxcarta does also read them
(via the `databricks_volume_path` / `databricks_secret_scope` fields in
`packages/dbxcarta-client/src/dbxcarta/client/settings.py` and in
`neo4j_utils.py`), but that is in addition to the runner, not instead of it.
Renaming would require either keeping both names in every overlay or patching
the pinned external runner, both of which are worse than the cosmetic
inconsistency they would fix. We therefore keep `DATABRICKS_VOLUME_PATH` and
`DATABRICKS_SECRET_SCOPE` exactly as they are, alongside the other standard
Databricks variables (`DATABRICKS_HOST`, `DATABRICKS_TOKEN`,
`DATABRICKS_CLUSTER_ID`, `DATABRICKS_WORKSPACE_DIR`, `DATABRICKS_JOB_RUN_ID`,
`DATABRICKS_WAREHOUSE_ID`, `DATABRICKS_PROFILE`). The per-example `SCHEMAPILE_*`
and `DENSE_*` knobs stay as they are too; they live in separate overlays and
separate packages, never collide, and only partially overlap, so a partial
merge would produce a messier mixed scheme rather than a cleaner one.

The one cleanup this phase does keep: in `embed_probe.py`, drop the stray
`DBXCARTA_EMBED_ENDPOINT` fallback alias. Nothing sets it; every env file uses
`DBXCARTA_EMBEDDING_ENDPOINT`, so the alias is dead and only invites confusion.

With no rename to coordinate, the commands below read the volume path under its
existing `DATABRICKS_VOLUME_PATH` name.

### Phase 2 (done): Build the shared UC-admin helper and the two commands

Add the shared helpers, then two thin commands on top of them. As built, the
helpers are split by layer rather than living in one module:

- **Pure UC primitives** went into `dbxcarta.spark.databricks`, next to the
  existing `validate_identifier` / `quote_identifier`: `parse_volume_path` (the
  four-part `/Volumes/<catalog>/<schema>/<volume>` split; `validate_uc_volume_subpath`
  expects a five-part path with a trailing subdir and does not fit a bare volume
  path), plus the protected-name blocklist `UC_PROTECTED_NAMES` and its
  `check_not_protected` guard (`main`, `system`, `hive_metastore`, `samples`,
  `graph-enriched-lakehouse`), refused by both commands.
- **Operator DDL** went into a new `dbxcarta.submit.uc_admin` module rather than
  into `dbxcarta.spark.databricks`. `dbxcarta-spark` is the wheel installed on
  the cluster for ingest and should not carry catalog/volume DDL it never runs
  there; both commands live in `dbxcarta-submit`, so that is the single home for
  the warehouse `execute_statement` (which waits for a terminal state and raises
  on a non-`SUCCEEDED` result, so a failed `CREATE`/`DROP` is never reported as
  success), `ensure_uc_volume` (the three `CREATE ... IF NOT EXISTS`), and the
  teardown side (`TeardownTarget`, `parse_teardown_target`, `drop_teardown_target`).
- Both command handlers load the layered env via `resolve_env_files` +
  `load_env_files`, so the documented precedence (base `.env` -> overlay ->
  process env) is reused.

**`dbxcarta-submit bootstrap`** (build):

- Reads `DATABRICKS_VOLUME_PATH` from the resolved overlay, parses the three
  names, and runs `CREATE CATALOG / SCHEMA / VOLUME IF NOT EXISTS`. A second run
  changes nothing and still succeeds.
- Prints the three names it ensured. `--dry-run` parses, runs the protected-name
  guard, and prints them without touching the workspace, so a typo'd
  `/Volumes/main/...` is refused in dry-run the same way teardown refuses one.
- Does **not** create data tables (the materialize step does) or the run-summary
  table (the ingest creates it via `saveAsTable` in append mode). It only makes
  the schema that table lives in.

**`dbxcarta-submit teardown`** (destroy):

- Reads one explicit setting, `DBXCARTA_TEARDOWN_TARGET`, written as
  `schema:<catalog>.<schema>` or `catalog:<catalog>`, and drops exactly that
  (`DROP SCHEMA ... CASCADE` or `DROP CATALOG ... CASCADE`).
- Requires `--yes-i-mean-it`; without it the command prints what it would drop
  and exits without doing anything. `--dry-run` does the same regardless.
- Refuses any target on the protected blocklist, so the explicit `catalog:` form
  still cannot name a system catalog.
- The explicit `catalog:` vs `schema:` prefix is deliberate: it makes the
  dangerous "drop the whole catalog" case a declared value in a committed file
  rather than logic buried in a script, and prevents a missing schema segment
  from silently escalating a schema drop into a catalog drop.

Both commands are registered in `dbxcarta-submit`'s `main` next to
`submit-entrypoint` and `publish-wheels`; neither name collides with the runner
pass-through commands.

### Phase 3 (done): Declare each example's teardown target in its overlay

Add `DBXCARTA_TEARDOWN_TARGET` to each overlay, reproducing exactly what each
example's `--drop-all` does today:

- dense: `DBXCARTA_TEARDOWN_TARGET=schema:schemapile_lakehouse.dense_1000`
- schemapile: `DBXCARTA_TEARDOWN_TARGET=catalog:schemapile_lakehouse`
- finance: `DBXCARTA_TEARDOWN_TARGET=schema:dbxcarta-catalog.finance_genie_ops`

The value is secret-free, so it is safe to keep in the committed,
job-param-forwarded overlay.

**Later widened to multi-target (schemapile plane separation).**
`DBXCARTA_TEARDOWN_TARGET` now accepts a comma-separated list, parsed by
`parse_teardown_targets`, so an example that owns more than one thing drops all
of it in one command. This came in with the schemapile ops/data plane split:
schemapile's ops moved out of `schemapile_lakehouse` into
`dbxcarta-catalog.schemapile_ops`, so it now owns two targets and its value is
`catalog:schemapile_lakehouse,schema:dbxcarta-catalog.schemapile_ops`. dense and
finance remain single-target. The shared `dbxcarta-catalog` is never itself a
target, so other examples' ops are safe. See `clean-schemapile.md`.

### Phase 4 (done): Wire the commands into the make targets

The repo-root `Makefile` now drives both commands:

- `dbxcarta-submit bootstrap` is the first step of the `e2e_ingest` define,
  ahead of `publish-wheels` and `submit-entrypoint ingest`, for all three
  examples. Because it is idempotent, repeated ingest runs stay fine; it does
  real work only the first time.
- A `make e2e-<example>-teardown` target was added for each example, calling
  `dbxcarta-submit teardown --yes-i-mean-it` with that overlay through a new
  `e2e_teardown` define. Teardown is never chained into ingest; it is only ever
  run by hand, and each new target is in `.PHONY` and the `help` text.

### Phase 5 (done): Delete the per-example scripts

With both commands live and wired, the old machinery was removed entirely:

- `bootstrap.py` deleted from all three example packages (dense-schema,
  schemapile, finance-genie).
- The three entry points removed from each `pyproject.toml`
  (`dbxcarta-dense-bootstrap`, `dbxcarta-finance-genie-bootstrap`,
  `dbxcarta-schemapile-bootstrap`). finance-genie's `[project.scripts]` table
  held only its bootstrap, so the now-empty table was removed with it.
- finance-genie's `utils.py` deleted: it was imported only by finance-genie's
  `bootstrap.py`. dense-schema's and schemapile's `utils.py` kept, still
  imported by their `materialize.py`, `question_generator.py`,
  `candidate_selector.py`, and `slice_runner.py`.

A grep confirmed no remaining code references to the deleted modules or entry
points; `uv sync` regenerated the console scripts (the three `*-bootstrap`
binaries are gone, the rest intact); ruff, mypy, and the full suite (482 passed)
stay green.

After this there is one build command and one teardown command in the whole
repo, and nothing per-example to drift.

### Phase 6 (done): Update the docs and verify

- The main `README.md` "Upload and submit" section now documents `bootstrap` and
  `teardown` next to `publish-wheels` (the latter's "ships the bootstrap script"
  was clarified to "cluster bootstrap script" to avoid confusion with the new
  command). Each example README's quick start and setup flow now call
  `dbxcarta-submit bootstrap` / `teardown` with `--env-file`, the retired
  `dbxcarta-<example>-bootstrap` invocations and `--drop-all` lines are gone, and
  schemapile's file tree no longer lists the deleted `bootstrap.py`.
- The schemapile pytest `live` marker comment was changed from "a
  previously-bootstrapped catalog" to "a catalog provisioned by
  `dbxcarta-submit bootstrap`".
- The older planning note `examples/BOOT.md` was rewritten as a superseded
  record: it now documents the two-command design first and keeps the retired
  per-example plan (Option B) only as history, pointing at this file.
- Verification: ruff and mypy clean, full suite 482 passed / 1 skipped. For all
  three examples, `bootstrap --dry-run` matched the overlay's
  catalog/schema/volume and `teardown --dry-run` named the expected target,
  neither touching the workspace. A repo grep confirmed the retired console-script
  names and `--drop-all` survive only in this file and `BOOT.md` as history, and
  that no `DBXCARTA_EMBED_ENDPOINT` remains in source.
