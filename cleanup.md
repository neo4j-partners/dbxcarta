# Cleanup Plan: One Simple Way to Set Up Every Example

## The problem in plain words

Before the pipeline can run, three storage things must already exist in
Databricks: a catalog, a schema, and a volume. A volume is just a folder that
holds files. If any of the three is missing, the run fails right away.

Today each example has its own little setup script, and each script looks in a
different place to decide which catalog, schema, and volume to build. The real
divergence is finance-genie. Its overlay sets `DBXCARTA_CATALOG` to an upstream,
data-owned catalog (`graph-enriched-finance-silver`), but its run actually needs
its own ops plane (`dbxcarta-catalog` / `finance_genie_ops` / `dbxcarta-ops`).
Any setup that keyed off `DBXCARTA_CATALOG` would build the wrong catalog for
finance. Dense-schema and schemapile happen to agree across their sources today,
but nothing keeps them from drifting, and three copies of the same
`CREATE ... IF NOT EXISTS` logic is three things to maintain.

There is now one place that already holds the right answer for every example:
the committed overlay settings file. It has a storage folder path in it, and
that path is written as `/Volumes/<catalog>/<schema>/<volume>`, so it spells out
the catalog, the schema, and the volume all at once. That path always points at
the same plane the setup needs to build (the ops/volume plane), even for an
example like finance-genie whose data catalog is a different, upstream-owned
catalog. So we can build a single setup step that reads that one path and
creates exactly what the run needs.

## Scope of this plan

This plan does **build only**. It adds one shared command that creates the
catalog, schema, and volume, and wires it in front of every ingest. Teardown
(removing those things) is left exactly as it is today, in the per-example
`--drop-all` scripts, and a unified teardown command is deferred to a later
phase (see Phase 5). Nothing in this plan touches or removes the teardown path,
so the automatic ingest flow can never destroy data.

## Core goals

- One build command for every example, instead of three different scripts.
- The command always reads the answer from one place: the overlay settings file.
- Setup runs by itself right before each ingest, so nobody forgets it.
- The command runs locally on the operator's machine against a SQL warehouse,
  the same way today's scripts do. It does not spin up a cluster job just to run
  three `CREATE` statements.
- The command is safe to run over and over. It never breaks anything that
  already exists.
- Building runs automatically; tearing down is never part of this command, so
  the automatic ingest path can never destroy data by accident.

## How the plan is phased

### Phase 1: Build the one shared build command

The command is named `bootstrap` and is added to the operator toolbox you
already use to publish code and start jobs (`dbxcarta-submit bootstrap`). You
choose which example you mean the same way you already do for other commands, by
pointing the usual environment switch at that example's overlay. It has one mode
only: build. There is no teardown mode in this command.

How it runs:

- It runs locally on the operator's machine, like today's per-example scripts:
  it builds a workspace client and executes the `CREATE` statements through a SQL
  warehouse. It needs `DATABRICKS_WAREHOUSE_ID` and operator catalog-create
  privilege, both of which the existing scripts already require.
- It loads config through the existing env-layering loader
  (`resolve_env_files` + `load_env_files` in `dbxcarta.spark.env`), so the
  documented precedence (base `.env` -> overlay -> real process env) is reused,
  not reinvented. From the resolved environment it reads one setting: the storage
  folder path for the run (`DATABRICKS_VOLUME_PATH`).
- Because that path is written as `/Volumes/<catalog>/<schema>/<volume>`, the
  command reads all three names straight from it. No second source is consulted,
  so there is nothing to disagree with. It parses the four-part path with the
  existing `validate_identifier` and `quote_identifier` helpers. Note that the
  existing `validate_uc_volume_subpath` helper expects a five-part path (it
  requires a trailing subdir), so it does not fit a bare
  `/Volumes/cat/schema/volume` and is not used here.
- It asks Databricks to create the catalog, the schema, and the volume, each one
  only if it is not already there, using `CREATE ... IF NOT EXISTS`. If they
  already exist, nothing changes and the command still succeeds.
- It prints the three names it ensured, so you can see it did the right thing.
- A `--dry-run` flag parses the path and prints the three names it *would*
  create, then exits without touching the workspace. This is the repeatable
  check used in Phase 4 verification.

Why it is safe:

- "Create only if missing" means a second run does no harm.
- A short list of protected names is refused, so a typo in the settings cannot
  point setup at a shared system catalog. The list preserves the names the
  current dense/schemapile config blocks: `main`, `system`, `hive_metastore`,
  `samples`, and `graph-enriched-lakehouse`.

What it does not do:

- It does not create the example's data tables. Those are still made by the
  existing materialize step.
- It does not create the run-summary table. The pipeline still makes that on its
  own during the first ingest (via `saveAsTable` in append mode). Setup only
  needs to make the schema it lives in.
- It does not tear anything down. Teardown stays in the per-example scripts for
  now (see Phase 5).

### Phase 2: Wire the build command into the make targets

- The make targets are the short commands you run from the repo root.
- Each example has an ingest target. We add `dbxcarta-submit bootstrap` as the
  very first step inside that target, before the publish step and before the
  ingest step.
- We do this the same way for all three examples, so they all behave alike.
- Because the command is safe to repeat, running the ingest target many times
  stays fine. Setup does real work only the first time, then quietly passes.
- This assumes catalog-create privilege is available whenever ingest runs.

### Phase 3: Retire only the per-example *build* path

We do not delete the per-example scripts yet, because they still carry the
teardown (`--drop-all`) logic that Phase 5 will consolidate. To avoid two copies
of build logic drifting, we trim each per-example `bootstrap.py` down to its
teardown-only path:

- Remove the `_provision` build function and the build branch of each script's
  `main`, leaving the `--drop-all` / `--yes-i-mean-it` teardown path intact.
- Keep the entry points (`dbxcarta-dense-bootstrap`,
  `dbxcarta-finance-genie-bootstrap`, `dbxcarta-schemapile-bootstrap`); they are
  now teardown-only commands until Phase 5 replaces them.
- Keep every `utils.py`. Finance-genie's is still imported by its (now
  teardown-only) `bootstrap.py`; dense-schema's and schemapile's are still
  imported by their `materialize.py`, `question_generator.py`,
  `candidate_selector.py`, and `slice_runner.py`.

After this, build lives in exactly one place, and teardown stays exactly where
it is until it gets its own command.

> If trimming the scripts now feels like too much churn, the alternative is to
> leave the per-example `bootstrap.py` fully intact and simply stop calling its
> build path from the make targets. That leaves a dead build path temporarily
> but touches nothing in those files. Pick one before implementing.

### Phase 4: Update the docs and verify

- Update the main README setup section so it names the single new build command.
- Update each example README the same way, so every example tells the same
  story. Teardown docs continue to point at the existing per-example `--drop-all`
  until Phase 5.
- Rewrite the older planning note so future readers see this simpler shared
  build command instead of the retired per-example plan.
- Run the test suite and confirm it passes.
- For each example, run `dbxcarta-submit bootstrap --dry-run` and confirm it
  picks the catalog, schema, and volume you expect from that example's overlay,
  without changing anything in the workspace.
- Search the repo to confirm the old per-example build commands are no longer
  referenced as setup steps, including README setup steps and the schemapile
  pytest marker comment that references a "previously-bootstrapped catalog."

### Phase 5 (later, separate): Unified teardown

Teardown is intentionally out of scope above. It is not the inverse of build:
build creates the `_meta`/ops schema and volume named by the volume path, but
each example tears down something different and interdependent:

- dense drops only its `dense_1000` data schema and keeps the shared catalog;
- schemapile drops the whole shared `schemapile_lakehouse` catalog;
- finance drops only its `finance_genie_ops` ops schema and keeps the catalog.

Because dense and schemapile share one catalog, these behaviors must be
preserved exactly, not derived from the volume path. A future phase can add a
separate, opt-in teardown command (for example `dbxcarta-submit teardown`) that
reads an explicit per-overlay target such as
`DBXCARTA_TEARDOWN_TARGET=schema:<catalog>.<schema>` or `catalog:<catalog>`,
paired with `--yes-i-mean-it`. Only once that exists do we delete the
per-example teardown scripts, their entry points, and finance-genie's now-unused
`utils.py`. Until then, teardown stays as it is today.
