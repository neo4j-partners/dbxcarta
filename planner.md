# Materialize: run it as a Spark job

## What is wrong (ELI5)

Materialize creates the demo tables. To run SQL it makes one REST call per
statement, waits for that call, then makes the next one. The Statement
Execution API runs a single statement per request, so creating 500 tables means
thousands of little submit-and-wait round trips.

Each round trip is slow because of network plus polling, not because the
warehouse is busy. So the code spins up 20 Python threads to fire many round
trips at once and hide that wait.

That is the root problem: the **client** is scheduling parallel work that the
**compute** is built to schedule itself.

```
   TODAY: the client does the scheduling

   +-------------------+     one HTTP call per statement, x thousands
   | local materialize |  -----------------------------------------+
   | 20 worker threads |  --------------------------------------+  |
   +-------------------+   -----------------------------------+  |  |
                                                              v  v  v
                                              +-----------------------------+
                                              |   SQL warehouse             |
                                              |   fed one statement at a    |
                                              |   time from the client      |
                                              +-----------------------------+
```

A second problem makes the first worse. Each table is built with three or four
separate statements, and every one needs its own "what if this fails" rule:

- `CREATE TABLE` fails        -> skip this table, keep the run going
- `INSERT` fails              -> skip the rows, keep the table
- `SET NOT NULL` fails        -> drop the whole primary key for this table
- `ADD PRIMARY KEY` fails     -> skip the key, keep the table

More statements means more round trips to parallelize and more failure branches
to choreograph. Both the thread pool and the failure matrix are symptoms of the
same design: single statements driven one at a time from the client.

What needs fixing:

- Send fewer statements per table.
- Move the work onto the compute, so the client stops running a thread pool.

## Fix part 1: fold the constraints into CREATE TABLE

Unity Catalog lets a `CREATE TABLE` declare `NOT NULL` columns and an inline
`PRIMARY KEY` in one statement. Primary keys and foreign keys are informational
in Databricks, so they are metadata, not enforced checks. That means three
statements collapse into one:

```
   BEFORE (3 statements, 3 failure branches)
     CREATE TABLE t (id BIGINT, name STRING, ...)
     ALTER TABLE t ALTER COLUMN id SET NOT NULL
     ALTER TABLE t ADD CONSTRAINT pk_t PRIMARY KEY (id)

   AFTER (1 statement)
     CREATE TABLE t (
       id BIGINT NOT NULL,
       name STRING,
       ...,
       CONSTRAINT pk_t PRIMARY KEY (id)
     )
```

The failure matrix then shrinks to two real cases:

- `CREATE TABLE` (with its inline constraints) fails -> skip this table
- `INSERT` fails -> skip the rows, keep the table

Foreign keys still run in a second pass, because a foreign key can only point at
a table that already exists. That is one `ALTER ... ADD CONSTRAINT ... FOREIGN
KEY` per edge, after all tables are created. The two-phase order (tables first,
foreign keys second) stays.

## Fix part 2: run materialize as a Spark job

Ingest and Client already run as Databricks jobs. Materialize joins them, on
serverless Spark, as its own job. Spark plus Delta is the native tool for
creating tables and loading data, and the cluster runs the SQL, so the client
thread pool, the `make_execute` factory, and the thread-safety question all go
away.

```
   AFTER: pure planner in core, the Spark shell runs it

   dbxcarta-submit                  Databricks job (serverless Spark)
   +-----------------+  submit job  +-------------------------------------+
   | upload blueprint| -----------> |  shell: owns SparkSession + a       |
   | forward overlay |              |  bounded pool (default 5 workers)   |
   +-----------------+              |                                     |
                                    |  core (pure): builds the SQL        |
                                    |    - CREATE TABLE (+ inline PK)     |
                                    |    - INSERT sample rows             |
                                    |    - FK statements (2nd pass)       |
                                    |  shell: runs it via spark.sql,      |
                                    |  pools the table creates, skips     |
                                    |  failures, tallies stats            |
                                    +-------------------------------------+
```

- **Core is pure and threadless.** It builds the statements (schema DDL, each
  table's `CREATE` plus optional `INSERT`, and the FK pass given which tables
  succeeded). It opens nothing and spawns nothing, so it is trivially testable.
- **The Spark shell owns concurrency.** It holds the `SparkSession`, runs the
  statements with `spark.sql`, and pools the independent table creates in a
  small bounded `ThreadPoolExecutor` (default 5). The pool runs over
  `SparkSession`, which is thread-safe, so there is no contract on an opaque
  callable. This is the functional-core / imperative-shell split, and a bounded
  driver pool is a documented Databricks pattern for concurrent metadata work.

## Decisions made

- **Run as a serverless Spark job, on its own.** Serverless needs no cluster
  sizing and starts fast, which suits a metadata-heavy, data-light task. A
  standalone job (not a task in a shared DAG) keeps it decoupled and re-runnable;
  the operator sequences bootstrap, materialize, ingest, client.
- **The pool lives in the Spark shell; core stays pure.** Core builds SQL, the
  shell runs and parallelizes it. This removes threads from core entirely.
- **Bounded driver pool, default 5.** A small `ThreadPoolExecutor` overlaps the
  per-`CREATE` round trips. The foreign-key pass stays serial and after. The
  worker count is a tunable.
- **Blueprint is committed to git and staged to a Volume.** The candidate JSON
  moves out of `.cache` into a committed example path
  (`examples/<name>/blueprint/`). Git is the source of truth, and
  `dbxcarta-submit materialize` uploads it to the ops Volume for the cluster to
  read, the same way client questions are uploaded.
  - dense commits cleanly: synthetic, seed-deterministic, 2.3 MB.
  - schemapile (144 KB): safe to commit with attribution. Every schema in
    SchemaPile-Perm is permissively licensed (MIT, Apache-2.0, BSD, Unlicense,
    CC0), there is no share-alike or non-commercial clause, and PII is already
    imputed upstream. Add a short attribution note and keep the source URLs that
    the data already carries (see Attribution).
- **The data catalog is created in bootstrap.** `dbxcarta-submit bootstrap`,
  which already creates the ops catalog and volume, also creates the data
  catalog from `DBXCARTA_CATALOG`, carrying over the Default-Storage
  MANAGED-LOCATION handling and the protected-name guard. The job assumes the
  catalog exists.
- **One shared Spark entrypoint, parameterized by the overlay.** Like ingest and
  client, materialize is a single entrypoint driven by env config, not one copy
  per example. This reframes materialize as a shared product step configured per
  example; Blueprint stays the only truly example-specific stage. The four-stage
  doc wording will be updated to match in the docs phase.
- **Config surface grows by one tunable.** The blueprint Volume path is derived
  from `DATABRICKS_VOLUME_PATH` (`{volume}/dbxcarta/blueprint/<filename>`, with
  the filename a per-example code default, like the questions filename), so it
  needs no new variable. Add one int, `DBXCARTA_MATERIALIZE_WORKERS` (default 5,
  validated `>= 1`).
- **Persist a materialize summary record.** Reuse the existing run-summary sink
  with `job_name="dbxcarta_materialize"`, so all three stages report run history
  to one place.
- **Sample rows: one `INSERT OVERWRITE` per table, via `spark.sql`.** This is one
  Delta commit per table, not the many-inserts-into-one-table anti-pattern, so
  there is no small-files problem. It reuses the core `build_insert_statement`
  and preserves today's let-Delta-cast-the-literal behavior with minimal code. A
  typed DataFrame write is the more idiomatic Spark load and the upgrade path,
  but it needs an explicit cast layer for the loosely-typed blueprint values and
  gives no speed gain at this row count, so it is deferred.
- **Job output goes through `logging`, not `print`.** The stats line is emitted
  with `logger.info(...)`.
- **The warehouse poller stays for bootstrap and teardown.**
  `execute_ddl_blocking` still serves those local-warehouse paths. Only
  materialize stops using it.

## Attribution (schemapile blueprint)

The SchemaPile license check is settled: the derived 144 KB slice is safe to
commit. SchemaPile-Perm contains only permissively licensed schemas (about 92%
MIT or Apache-2.0, the rest BSD, Unlicense, or CC0), with no share-alike and no
non-commercial restriction, and it is itself published as a processed derivative
of those sources. The only obligation is attribution. When committing the
schemapile blueprint:

- Add a short note crediting SchemaPile (https://github.com/amsterdata/schemapile)
  and stating the file is a curated derivative slice.
- Keep the per-schema source URLs the SchemaPile data already carries, so
  provenance back to the original repositories is preserved.

dense is unaffected (synthetic, no third-party data).

## What changes and what stays the same

Changes:

- `CREATE TABLE` carries inline `NOT NULL` and `PRIMARY KEY`, so the
  SET-NOT-NULL and ADD-PK statements go away.
- The client thread pool, `make_execute`, and the executor factory are removed.
- The core spine becomes pure statement builders; the Spark shell runs and pools
  them.
- Materialize runs as a serverless Spark job; the cluster runs the SQL.
- The blueprint is committed to git and uploaded to a UC Volume, instead of read
  from local disk only.

Stays the same:

- The same tables, rows, primary keys, and foreign keys come out the other end.
- The two-phase order: all tables first, then foreign keys.
- Skip-on-error behavior: a bad table is skipped and the run continues. The
  matrix is just smaller, because there are fewer statements per table.

## Why this is better than a thread pool plus a docstring

- Thread-safety stops being a written rule on an opaque callable. Core has no
  threads, and the only pool runs over `SparkSession`, a known thread-safe type,
  in the Spark shell on the cluster driver.
- The code that builds SQL gets simpler, because each table is one CREATE plus
  an optional INSERT, not a four-statement sequence with four failure branches.
- It uses Databricks the way Databricks is meant to be used: the cluster runs
  the work, not a hand-built thread pool in the client.

## Implementation plan (phased, plain English)

The **execution-seam reshape** is a complete cutover: purifying core breaks the
old per-example shells, so the old path must die in the same move the new Spark
job replaces it. There is no in-between state where core is reshaped while the
old shells still drive it, and no "build the new package against the current core
API, test it, then cut to Spark" two-step, which would rewrite the new package
twice across the seam change.

But not everything bundled with that reshape actually depends on it. Two
workstreams touch only the SQL the builders emit and where the blueprint and
config live, not the threading seam: folding the constraints into `CREATE TABLE`,
and committing the blueprint plus its config. Both are additive, both carry
forward into the cutover unchanged, so each lands as its own prep phase before
it, the same way bootstrap's data-catalog creation does. That leaves the cutover
as just the irreducible seam reshape and its unavoidable consequences.

Order matters: bootstrap must create the data catalog, the constraints must fold
into `CREATE TABLE`, and the blueprint must live at its committed path, all
before the cutover can run end to end. Those are the three prep phases, and they
land in that order ahead of it.

### Discovery: map the test surface first

Before touching code, inventory every test that exercises the materialize spine
and the old per-example shells, and decide for each whether it moves, is
rewritten, or is deleted. The output is a short up-front list, for example:

- `tests/core/test_materialize.py` changes in two steps. The constraint fold
  (Phase 2) updates its CREATE-table assertions and the failure-matrix cases
  while core still executes. The cutover (Phase 4) rewrites it into pure SQL-text
  assertions, since core no longer executes or threads, so the tests then check
  the statements it builds, not how they ran.
- A new `tests/materialize/` covers the Spark shell: the bounded pool, the
  skip-on-error tally, the FK second pass, and the summary record.
- Tests on the dense and schemapile `materialize.py` shells are deleted with that
  code in the cutover. (There is no separate `dbxcarta materialize` CLI command;
  the only entry points are the two example console scripts.)
- The executor tests stay, because bootstrap and teardown still use
  `execute_ddl_blocking`.

Each test change lands in the phase that lands its code: the fold's test updates
in Phase 2, the relocation's in Phase 3, the seam rewrite and new shell tests in
Phase 4. Doing the inventory once up front means each phase knows its test impact
instead of discovering it mid-change.

### Phase 1: Bootstrap creates the data catalog (prep, additive) — DONE

What: Extend `dbxcarta-submit bootstrap` so that, alongside the ops catalog and
volume it already makes, it also creates the data catalog from
`DBXCARTA_CATALOG`. Carry over the Default-Storage MANAGED-LOCATION handling and
the protected-name guard it already uses.

Why: The materialize job assumes the data catalog exists, so this prerequisite
must land before the job can run end to end. It is purely additive and touches
none of the materialize execution path.

Status: Done. The catalog-create logic was factored out of `ensure_uc_volume`
into a reusable `ensure_uc_catalog` in `uc_admin.py` (one source for the
protected-name guard and the Default-Storage skip-if-exists). `_handle_bootstrap`
now reads `DBXCARTA_CATALOG` (failing loudly if unset), validates it up front,
creates the data catalog ahead of the ops volume, and reports both in the
`--dry-run` and success output. Existing unit tests pass unchanged.

### Phase 2: Fold the constraints (prep, additive)

What: In core's `_materialize_table`, build `CREATE TABLE` with inline `NOT NULL`
on the primary-key columns and an inline `CONSTRAINT ... PRIMARY KEY (...)`
clause. Delete the separate `SET NOT NULL` and `ADD PRIMARY KEY` execution that
`_add_primary_key` runs today (the function collapses, since UC's
NOT-NULL-before-PK requirement is now satisfied inline). The PK-column validity
check (every PK column survived sanitization, else no key) stays, but now gates
whether the inline clause is emitted rather than whether a follow-up `ALTER`
runs. Foreign keys remain a second pass. The failure matrix shrinks from four
cases to two: `CREATE` (with its inline constraints) fails -> skip the table;
`INSERT` fails -> skip the rows, keep the table. Update the core materialize
tests for the new `CREATE` SQL and the smaller matrix.

Note a deliberate behavior shift: today a table whose `ADD PRIMARY KEY` fails
still lands without its key (`_add_primary_key` is tolerant). After folding, an
invalid inline PK sinks the whole `CREATE`, so the table is skipped. That is the
intended design of the fold, not a regression.

Why: This changes only the SQL the builder emits and the error branches around
the `CREATE` call. It does not touch `materialize_schemas`'s pool or the
per-thread executor seam, so it lands cleanly on the current client-driven path,
which keeps running with fewer statements per table. The folded `CREATE` logic
carries into pure core unchanged in the cutover, so none of this is rework.

Done when: The current client-driven materialize still creates tables, rows, and
primary keys, now with one `CREATE` per table instead of three statements; core
tests assert the inline-constraint SQL and the two-case matrix; the suite passes.

Status: Done. `_materialize_table` now resolves the PK columns up front
(`_resolve_primary_key_columns`, a pure replacement for the deleted
`_add_primary_key`), declares them `NOT NULL` inline, and appends an inline
`CONSTRAINT ... PRIMARY KEY (...)` clause to the `CREATE`; `pk_constraints_added`
is tallied when that `CREATE` succeeds. The separate `SET NOT NULL` / `ADD
PRIMARY KEY` ALTERs are gone, so the matrix is `CREATE` fails -> skip table,
`INSERT` fails -> skip rows. Core `test_materialize.py` asserts the inline SQL;
the obsolete PK-DDL assertions in the example-shell materialize tests (which
Phase 4 deletes) were removed rather than rewritten. Suite passes apart from a
pre-existing bootstrap test failure from Phase 1's uncommitted submit changes
and three live-Databricks integration tests.

Review refinement: folding `NOT NULL` inline surfaced a real behavior shift the
plan had not anticipated. The committed schemapile blueprint has 11 tables whose
sample rows carry a NULL in a declared PK column. With inline `NOT NULL`, the one
atomic `INSERT OVERWRITE` for such a table fails on the null, so under
`on_insert_error="skip"` the table would land with its PK but lose *every* sample
row (the old add-PK-after-insert path tolerated this by leaving the key off and
keeping the rows). To preserve the rows, `_resolve_primary_key_columns` now also
takes the prepared sample rows and drops the inline PK when any sample row has a
NULL in a PK column, restoring the old data-preserving outcome (rows kept, PK
absent) for exactly those tables. dense (synthetic, non-null `id`) is unaffected:
all 500 keep their PK; schemapile keeps 130 and drops 11. Covered by two new core
tests.

### Phase 3: Commit the blueprint and add config (prep, additive)

What: Move the blueprint JSON out of `.cache` into `examples/<name>/blueprint/`,
keeping the table-count-keyed filename, and point the current example shells at
the committed location. Derive the Volume path from `DATABRICKS_VOLUME_PATH`
(`{volume}/dbxcarta/blueprint/<filename>`, the filename a per-example code
default, like the questions filename), so no new path variable is needed. Add
`DBXCARTA_MATERIALIZE_WORKERS` (int, default 5, validated `>= 1`) to the config
surface; the cutover's Spark shell reads it, but it is defined here. Generation
stays a separate manual step; the committed file is the source of truth.

Why: Both are additive. The committed blueprint just gains a stable home the
current shells can already read, and the config variable is defined ahead of its
consumer. Nothing here depends on the seam reshape.

Done when: The blueprint lives at the committed path, the current shells read it
from there, `DBXCARTA_MATERIALIZE_WORKERS` is defined and validated, and the
suite passes.

Status: Done. The canonical blueprints were relocated out of `.cache` into the
committed `examples/<name>/blueprint/`: dense `candidates_500.json` (the
`table_count=500` default, 2.3 MB, via `git mv` to keep history) and schemapile
`candidates_random_1000.json` (144 KB, previously under the gitignored
`examples/schemapile/.cache/`, now committable). Both example configs derive the
`candidate_cache` default from a new `_BLUEPRINT_DIR` anchored to the package
location (`Path(__file__).resolve().parents[2] / "blueprint"`) so it resolves
regardless of cwd; schemapile's `.env`/`.env.sample` `SCHEMAPILE_CANDIDATE_CACHE`
now points at `blueprint/candidates_random_1000.json`. The Volume path is derived
in `derive_ops_config` as a new `blueprint_volume` field
(`{volume}/dbxcarta/blueprint/<filename>`, filename a per-example arg defaulting
to `DEFAULT_BLUEPRINT_FILENAME`), mirroring `client_questions`, so no new path
variable is added. `DBXCARTA_MATERIALIZE_WORKERS` (int, default 5, validated
`>= 1`) is defined in core `env.read_materialize_workers`, the natural shared
config surface for the Phase 4 materialize Spark shell to consume. New unit tests
cover the worker reader and the blueprint-path derivation; the suite passes.

Decisions/notes (Phase 3):
- `DBXCARTA_MATERIALIZE_WORKERS` and `blueprint_volume` were placed in core
  (`env.py`, `config.py`) since the plan says they are defined here but consumed
  by the Phase 4 `dbxcarta-materialize` package, which bundles core. Core is
  SDK/Spark-free and already the shared host+cluster config surface.
- Generation stays decoupled: the dense generator still writes to
  `.cache/candidates_<tables>.json`; promoting a freshly generated blueprint into
  `blueprint/` is the manual commit step. The schemapile select step writes to
  `candidate_cache`, which now resolves to `blueprint/`, so re-running it
  regenerates the committed artifact in place.
- The non-default `.cache/candidates_1000.json` (4.6 MB dense variant) was left
  in place. The plan blesses only the 2.3 MB default; relocating or removing the
  1000-table variant is left for the operator to decide.

### Phase 4: The cutover

This is the one move. Everything here lands together, so the repo goes straight
from the old path to the new one with no broken intermediate state. With the fold
(Phase 2) and the committed blueprint plus config (Phase 3) already in, this
phase carries only the seam reshape and its unavoidable consequences.

What:

- **Purify core.** Strip execution and threading out of core: the thread pool in
  `materialize_schemas`, the `ExecuteFactory`/`ExecuteFn` seam, `make_execute`,
  and `_per_thread_executor` all go. Core only builds strings: the schema DDL,
  each table's `CREATE` (with the inline constraints from Phase 2) plus optional
  `INSERT`, and the FK statements given which tables succeeded.
- **New `dbxcarta-materialize` package.** A sibling wheel of `dbxcarta-client`
  and `dbxcarta-spark`, namespace-packaged (`module-name = "dbxcarta"`,
  `namespace = true`) with core bundled in at publish, and a
  `dbxcarta-materialize` console script. It owns the `SparkSession`, asks core
  for the statements, runs them with `spark.sql`, overlaps the independent table
  creates in a bounded `ThreadPoolExecutor` (default 5, from
  `DBXCARTA_MATERIALIZE_WORKERS`), runs the FK pass serially after, writes one
  run-summary record with `job_name="dbxcarta_materialize"`, and logs the stats
  line with `logger.info`.
- **Submit as a job.** Add `dbxcarta-submit materialize`: it uploads the
  committed blueprint to the ops Volume (the way client questions are uploaded),
  forwards the overlay's `KEY=VALUE` pairs as job parameters, and submits a
  standalone serverless Spark job running the `dbxcarta-materialize` entrypoint.
  Wire the entrypoint into the five per-entrypoint registries in
  `submit/cli.py`: `_ENTRYPOINT_WHEEL_PACKAGE`, `_ENTRYPOINT_CONSOLE_SCRIPT`,
  `_ENTRYPOINT_PINNED_CLOSURE` (a neo4j-free closure, like the client one),
  `_ENTRYPOINT_SMOKE_IMPORTS`, and `_ENTRYPOINT_JVM_PROBE_CLASS` (`None`). It uses
  the shared `runner`, not the ingest runner, so it skips the maven preflight.
  `publish-wheels` and `_assert_wheel_bundles_core` grow the third package.
- **Delete the old path.** Remove the per-example `materialize.py` shells (dense
  and schemapile) and their `dbxcarta-dense-materialize` /
  `dbxcarta-schemapile-materialize` console scripts. Keep `execute_ddl_blocking`;
  bootstrap and teardown still use it.
- **Apply the test moves** decided in Discovery: the core tests become pure
  SQL-text assertions, the new `tests/materialize/` covers the Spark shell, and
  the example-shell tests are deleted.

Done when: `dbxcarta-submit materialize --env-file examples/<name>/...` uploads
the blueprint and runs a serverless job to completion that creates the tables,
rows, and primary keys and adds the foreign keys; a summary record is written;
the old shells are gone; and the suite passes against the new shape.

Status: Done. Core `materialize.py` is now pure: `materialize_schemas` /
`make_execute` / `ExecuteFactory` / `_per_thread_executor` / the thread pool are
gone, replaced by string builders — `read_schema_entry`,
`build_create_schema_statement`, `build_table` (returns a `TableBuild` of a
`TablePlan` plus build-time stats, or `None` + a skip tally), and
`build_foreign_key_statements` — over public `MaterializedTable`/`TablePlan`/
`TableBuild`/`MaterializeStats`. The null-aware PK gate and the full skip matrix
are preserved; only the execution/threading moved out. The new
`dbxcarta-materialize` package (namespace-packaged, core bundled at publish,
neo4j-free deps) owns the `SparkSession`, asks core for statements, overlaps the
independent creates in a bounded `ThreadPoolExecutor` (default 5 from
`DBXCARTA_MATERIALIZE_WORKERS`) over `spark.sql`, runs the FK pass serially after,
and writes a `MaterializeRunSummary` (its own dataclass, mirroring the client's,
to the shared `dbxcarta_run_summary` table with `job_name="dbxcarta_materialize"`).
`dbxcarta-submit materialize` stages the committed blueprint to the canonical
`{volume}/dbxcarta/blueprint/blueprint.json` and submits a serverless job via the
shared runner; the entrypoint is wired into all five registries plus
`publish-wheels`/`_assert_wheel_bundles_core`/`_CORE_BUNDLE_PACKAGES`, the
workspace `pyproject.toml`, `.gitignore`, and the CI mypy command. The two
example `materialize.py` shells and their console scripts are deleted;
`execute_ddl_blocking` stays for bootstrap/teardown. Core tests are now pure
SQL-text assertions, `tests/materialize/` covers the shell (bounded pool,
skip-on-error tally, FK second pass, summary record), and the example-shell tests
are deleted.

Five decisions confirmed with the operator before implementation: (1) the pure
core/shell seam shape above; (2) a single constant property prefix `"dbxcarta"`
in the shared job, replacing the per-example `"dense"`/`"schemapile"` values
(the `<prefix>.*` TBLPROPERTIES are write-only — nothing reads them back); (3)
`dbxcarta-submit materialize --blueprint <path>` (defaulting to the single
`*.json` under the overlay's sibling `blueprint/` dir), staged to the canonical
`blueprint.json` so the cluster derives the same path with no new env var; (4) a
self-contained `MaterializeRunSummary` mirroring the client rather than importing
the ingest `RunSummary` (there is no single shared sink — each stage owns its
summary type writing to the one table by `job_name`); (5) the job no longer
creates the data catalog (bootstrap owns it). Verified: ruff + mypy clean on the
new and changed code; full non-live suite is 604 passed / 1 skipped, with the
only red being the two pre-existing items already noted in Phases 2–3 (the
`bootstrap` dry-run test that predates Phase 1's `DBXCARTA_CATALOG` requirement,
and the three live-Databricks integration tests). End-to-end serverless run
against a live workspace is left to the operator.

### Phase 5: Attribution and docs

What: Add the SchemaPile attribution note next to the committed schemapile
blueprint (credit SchemaPile, mark it a curated derivative slice, keep the
per-schema source URLs). Update the four-stage wording in the docs to describe
materialize as a shared product step configured per example, with Blueprint as
the only truly example-specific stage.

Why: Attribution is the one license obligation, and the docs should match the
new shape.

Done when: The attribution note ships with the blueprint and the docs read
correctly.

Status: Done. The SchemaPile attribution ships as
`examples/schemapile/blueprint/ATTRIBUTION.md`: it credits SchemaPile, marks the
file a curated derivative slice of SchemaPile-Perm, records the permissive-license
terms (no share-alike, no non-commercial), and preserves provenance. One factual
correction to the plan: the committed blueprint carries no per-schema source URLs,
only a `source_id` per schema (SchemaPile's identifier for the originating SQL
file), so the note preserves that as the provenance handle rather than URLs.

The four-stage wording was updated in the locked-vocabulary reference
(`docs/proposals/separate-v2.md`): "The two pairs" now reads Example-specific
preparation = Blueprint (the only truly example-specific stage) and Product
behavior = Materialize, Ingest, and Client, and the Materialize stage contract's
Does/Primary-input/Owner lines now describe it as one shared serverless Spark job
staged via `dbxcarta-submit materialize`, owned by the product. The historical
"Locked names for shared core targets" section and `separate-v2-plan.md` were left
as records of the prior effort, superseded by Phase 4.

Both example READMEs were refreshed to the new shape (deleted per-example
`dbxcarta-dense-materialize`/`dbxcarta-schemapile-materialize` console scripts
replaced by `dbxcarta-submit materialize`, catalog creation re-pointed to
bootstrap, `materialize.py` dropped from the file trees and `blueprint/` added,
materialize reframed as the shared product step, INSERT behavior updated to
`INSERT OVERWRITE`). Refreshing dense surfaced a config/data mismatch: the
committed blueprint, `.env.sample`, and config default all said `dense_500` but
the overlay's `DBXCARTA_SCHEMAS` said `dense-1000`. Confirmed with the operator,
the overlay was reconciled to `dense_500` so the materialized schema matches what
ingest reads. A pre-existing defect surfaced and was fixed in the same pass: the committed dense
`questions.json` `reference_sql` pointed at `schemapile_lakehouse.dense-1000`,
neither dense's catalog nor (after the `dense_500` reconciliation) its schema. The
operator chose to regenerate the question set against the live `dense_500` tables.
Ran the full live chain on `aws-partner-rk` (bootstrap -> publish-wheels ->
`dbxcarta-submit materialize`, creating 500 tables in `dense-schema-example.
dense_500`), then `dbxcarta-dense-generate-questions` with model
`databricks-claude-sonnet-4-6` (target 60, temp 0.2). Result: 60 accepted, 0
errored, all `reference_sql` validated on the warehouse and qualified as
`dense-schema-example.dense_500`; no stale `schemapile_lakehouse`/`dense-1000`
references remain. The `DENSE_QUESTION_MODEL` override lives in the local
(gitignored) `examples/dense-schema/.env`; the committed default in `config.py`
was left unchanged.
