# Harden Deploy v2: publish and validation plan

## Background

See `docs/proposals/harden-deploy.md` lines 1 to 337 for the full
background: the compute decision, the core goals, the decisions, the
problem and solution, and the requirements. This document does not
restate them. It records what is now implemented, what remains, and the
recommended order for publishing databricks-job-runner and validating
dbxcarta against it.

## What has been implemented

All implementation so far is in the databricks-job-runner repository.
The committed dbxcarta pin has not moved and no dbxcarta consumer code
has changed.

Phase 1, first-class connector preflight hook:

- `preflight.py` with `PreflightResult`, the `Preflight` type,
  `run_preflights` with ordered fail-fast behavior, and
  `maven_libraries_preflight` that asserts a desired maven library list
  and skips when there is no classic cluster.
- `Runner` accepts `preflights=[...]`. Both `submit` and `validate` run
  the same hook list before a run is created.
- The new symbols are exported from the package `__init__`.

Phase 2, bootstrap install from a stable Volume path:

- `bootstrap.py`, a stdlib and pip only `SparkPythonTask` script. It
  parses a JSON blob from `argv[1]`, installs the project wheel into a
  fresh per-run target, installs the pinned closure into the shared
  driver environment under an advisory lock, runs an optional JVM
  classpath probe, reconstructs `sys.argv`, resolves the console entry
  point, and invokes it. It never calls `inject_params`.
- `submit.py` adds `BootstrapConfig`, `submit_bootstrap_job`, and the
  uploaded bootstrap script name and path.
- `upload.py` adds `upload_bootstrap_script`, `stable_wheel_name`, and
  the opt-in `publish_wheel_stable`. The legacy versioned upload path is
  untouched.
- `Runner` adds `publish_wheel_stable` and `submit_bootstrap`, and
  `upload_all` now ships the bootstrap script.

Four hardening fixes applied after review, before the Phase 2 gate:

1. Entry point resolution is scoped to the just-installed per-run
   distribution through `importlib.metadata.distributions`, and fails
   fast on a missing distribution, a missing script, or a duplicate.
2. The per-run target is cleared and recreated when it already exists,
   so a same-run task retry gets a clean directory and is never reused
   while populated.
3. The preflight cluster id is derived from the resolved compute object
   rather than raw config, so a serverless override correctly passes
   `None` and cluster-only hooks skip. Applied at all three call sites.
4. The stale lock is no longer broken by unlink. The holder records its
   pid, a live holder is never force-broken, and a non-alive holder is
   given a bounded grace before an actionable timeout. This removes the
   path that allowed concurrent shared-environment installs.

Test and tooling state:

- The runner unit suite passes, 40 tests, including new tests for
  per-run entry point resolution, the lock owner pid, crashed-holder
  no-wedge, and live-holder timeout.
- A permanent generic harness at `examples/bootstrap_smoke/` runs the
  bootstrap path end to end with one command. It was run against
  cluster `0515-141455-wb8qxgo2` and succeeded: stable publish and
  read-back, fresh per-run install, per-run-scoped entry point
  resolution, argv reconstruction, the manifest line, and a clean exit,
  with no library attached and no cluster restart.
- The `uv_build` build constraint was widened to `<0.12.0` across the
  runner repo.
- `harden-deploy.md` carries status tags, ticked checklists for the
  implemented items, the hardening-fixes block, and a Phase 5 note
  describing the smoke harness.

## Remaining implementation plan

Publishing is cheap and repeatable. A new patch version is the normal
way to iterate, so this plan publishes early, validates dbxcarta against
the published runner, and bumps the version as needed. There is no
sign-off gate and no release candidate. The bootstrap mechanics are
already proven by the runner unit suite and the `bootstrap_smoke` run on
the warm cluster, so the runner is ready to publish now.

The phases below are renumbered for v2 and supersede the Phased Plan in
`harden-deploy.md`. They are ordered. Phases V1 through V3 are the
release-and-validate path and run in sequence. Phases V4 through V6 are
independent improvements and run after V3, in any order, each followed by
a dbxcarta pin bump if the consumer wants the change.

### Phase V1: Publish the runner to PyPI [Status: done]

Goal: the hardened databricks-job-runner is available as a pinned
dependency for dbxcarta.

Steps:
- [x] Set the release version and write a changelog entry, including a
      breaking-change note that the ingest model moves off
      `PythonWheelTask` and that stable publish is a new opt-in API.
- [x] Publish to PyPI with operator approval.
- [x] Confirm the published version is installable and record the exact
      version string. Released as `databricks-job-runner==0.4.9`;
      `databricks_job_runner-0.4.9-py3-none-any.whl` and the sdist are
      live on PyPI and installable.

Done when: the version is on PyPI and installable. Done: `0.4.9` is the
recorded version string and the consumer pin target for Phase V2.

### Phase V2: Switch the dbxcarta consumer to the published runner [Status: implemented, warm-cluster validation deferred to V3]

Goal: the dbxcarta ingest path uses the bootstrap model on the published
runner. This is the original Phase 7 consumer change.

Plan corrections applied to this phase. Two defects in the v1 draft of
this phase, introduced when the original Phase 7 was renumbered, are
corrected here:

1. The v1 draft listed "the maven preflight" as a passive policy value
   alongside the closure and the probe class. It is not passive. It is a
   runner hook that must be wired into a `Runner(preflights=[...])`, and
   it must land in this phase, not later, because the same phase removes
   `_sync_cluster_libraries`, which is today the only check that fails a
   submit when the Neo4j connector is missing. Removing that without
   wiring the assert-only preflight in the same change would delete the
   safety net the proposal requires. The preflight is scoped to the
   ingest submission via a dedicated runner so the `client` and generic
   pass-through paths, which do not need the connector, are unaffected.
2. The closure-exclusion test from the original Phase 7 was dropped in
   the renumber. The curated pinned closure is now hand-owned consumer
   policy, so the test that guards it belongs in the phase that
   introduces the closure, not a later phase.

`_sync_cluster_libraries` and `_wait_for_cluster_libraries` are shared
by both the `ingest` and `client` submit-entrypoints. The proposal
requires them removed outright (Core Goal 5; Decisions, "Delete dead and
superseded code"). A function still called by `client` cannot be
removed, so both submit-entrypoints move to the generic bootstrap in
this phase. `client` gets its own curated closure, no JVM probe, and no
maven preflight; only `ingest` gets the connector probe and the maven
preflight.

The curated pinned closure excludes DBR-provided packages: `pyspark`
and `py4j` (the cluster runtime, never reinstallable) and `databricks-sdk`
with its auth subtree (DBR-managed; reinstalling it risks shadowing the
platform SDK). It includes `pydantic` and `pydantic-core` because the
ingest code requires pydantic v2 and the DBR-bundled version is not
guaranteed to match.

Steps:
- [x] Move the committed dbxcarta runner pin to `==0.4.9`
      (`packages/dbxcarta-spark/pyproject.toml` and `uv.lock`).
- [x] Switch the ingest and client submit-entrypoints from
      `PythonWheelTask` to `submit_bootstrap` with a `BootstrapConfig`.
- [x] Supply the policy values: the fully pinned dependency closure with
      binary wheels (DBR-provided packages excluded as above), the
      `org.neo4j.spark.DataSource` probe class for ingest, and the smoke
      imports.
- [x] Wire `maven_libraries_preflight` for the Neo4j connector into a
      dedicated ingest runner (`_ingest_runner`) so submit fails fast
      when the connector is missing, pending, or failed. The shared
      runner used by client and generic commands carries no preflight.
- [x] Switch the wheel upload path from the versioned upload to
      `publish_wheel_stable` plus `upload_all`. Because both
      submit-entrypoints now bootstrap, `upload --wheel` publishes a
      stable wheel for every `_ENTRYPOINT_WHEEL_PACKAGE` value
      (`dbxcarta-spark` and `dbxcarta-client`) so each path resolves its
      wheel from the fixed Volume name, then `upload_all` ships the
      runner bootstrap script the `SparkPythonTask` runs.
- [x] Remove `_sync_cluster_libraries`, `_wait_for_cluster_libraries`,
      and the `_ENTRYPOINT_CLUSTER_PYPI_PACKAGES`/
      `_ENTRYPOINT_CLUSTER_MAVEN_PACKAGES` tables, and stop installing or
      uninstalling Python libraries on the cluster as part of submit.
- [x] Add a unit test asserting the curated ingest and client closures
      exclude the DBR-provided packages (`pyspark`, `py4j`,
      `databricks-sdk`) and are fully exact-pinned
      (`tests/spark/test_cli_closure.py`); update the existing
      `tests/presets/test_cli.py` guardrail tests to the bootstrap API.

Done when: dbxcarta submits the ingest through the bootstrap path on the
published runner, the connector preflight is enforced on submit, the
closure test passes, and no consumer Python-library sync remains.
Implementation is complete and the full consumer test suite passes (360
passed, 1 skipped, 3 live-deselected). The live warm-cluster submission
itself is exercised in Phase V3; this phase does not require it.

Precondition note: `publish_wheel_stable` resolves the package wheel
from `dist/` by glob (the same `find_latest_wheel` model the prior
versioned path used), so `uv build --all-packages` must populate `dist/`
before `upload --wheel`, exactly as before this change. This is
unchanged consumer behavior and is documented in the project README; the
build contract is intentionally out of scope for this phase.

### Phase V3: Validate the full dbxcarta ingest on the warm cluster [Status: done — full ingest succeeded on runner 0.5.1, verify gate passed, no manual step, no restart]

Goal: the real ingest path works end to end, exercising the parts the
smoke harness does not: the Neo4j connector probe, the pinned closure
lock and smoke check with a real closure, and secret-scope loading.

Steps:
- [x] Run the full dbxcarta ingest on the warm cluster. Ran on
      `0515-141455-wb8qxgo2` against published `databricks-job-runner==0.5.1`.
      A real Databricks run was created (run `502035378647744`, task
      `409478094845255`); it executed end to end and failed only at the
      dbxcarta verify gate (see "Second attempt" below) — not in the
      deploy/runner path.
- [x] Confirm new code is picked up with no version bump and no cluster
      restart. Confirmed on the final rerun (run `67154826827483`, task
      `770822335752429`): the cluster was already `RUNNING` and stayed up
      (same `spark_context_id` `4985552566577751676` as the prior 0.5.1
      run, no `PENDING` waits), and the rebuilt stable wheel + verify fix
      were picked up with no version bump.
- [x] Confirm the connector probe, the closure lock and smoke check, and
      secret-scope loading all behave correctly. All validated on the
      0.5.1 run: connector preflight correctly failed-fast while the
      maven lib was `pending`, then passed (`[ok] neo4j connector`) once
      `INSTALLED`; bootstrap manifest reported `smoke_check: "ok (6)"`
      with the real closure (including `databricks-job-runner==0.5.1`);
      secret-scope `dbxcarta-neo4j` loaded (the ingest connected to and
      queried Neo4j).
- [x] If a defect is found, fix it in the runner, publish the next patch
      version, bump the dbxcarta pin, and rerun. The runner SDK defect
      was fixed and published as `databricks-job-runner==0.5.1` (operator
      published; supersedes the planned `0.4.10`). The dbxcarta pin was
      bumped `0.4.9 -> 0.5.1` consistently across
      `packages/dbxcarta-spark/pyproject.toml`, the `_INGEST_PINNED_CLOSURE`
      in `packages/dbxcarta-spark/src/dbxcarta/spark/cli.py`, and
      `uv.lock`, and the ingest was rerun. The published 0.5.1 wheel was
      verified to carry the `cluster_status` bare-list fix in both
      `libraries.py` and `clean.py`.

Done when: a full ingest succeeds against a published runner version
with no manual code step and no restart. **Met.** Run `67154826827483`
on published `databricks-job-runner==0.5.1` ended `status=success` with
`verify: ok=True violations=0` (gate on, `DBXCARTA_VERIFY_GATE=true`), no
manual code step, and no cluster restart.

#### Completed this attempt (preconditions, all green)

- Workspace auth on profile `azure-rk-knight`.
- Source readiness: all 8 finance-genie tables present in
  `graph-enriched-lakehouse.graph-enriched-schema` (5 required + 3 gold,
  strict-optional). `scripts/run_demo.py` was not needed.
- `uv build --all-packages` rebuilt the wheels, picking up the
  uncommitted Phase V2 `cli.py` change.
- `dbxcarta upload --wheel` published the stable wheels
  (`dbxcarta_spark-stable.whl`, `dbxcarta_client-stable.whl`) and shipped
  the runner bootstrap script (`_dbxrunner_bootstrap.py`).
- `dbxcarta upload --data tests/fixtures` uploaded the data fixtures.

#### Defect found (this is the expected V3 contingency)

`dbxcarta submit-entrypoint ingest --compute cluster` died client-side at
the connector preflight, before any Databricks run was created:

```
databricks_job_runner==0.4.9  libraries.py:163-164
AttributeError: 'list' object has no attribute 'library_statuses'
```

Root cause: in modern `databricks-sdk`, `LibrariesAPI.cluster_status()`
returns a bare iterator of `LibraryFullStatus` (it unwraps
`.library_statuses` itself). The runner accessed `.library_statuses` on
that list a second time. The runner's own test fakes returned the
obsolete wrapped shape, which is why its suite was green while production
crashed. The bug is reached via `maven_libraries_preflight`, wired into
`_ingest_runner()` in Phase V2; the `client` and generic paths have no
preflight and are unaffected.

#### Fix applied in databricks-job-runner (uncommitted, unpublished)

- `src/databricks_job_runner/libraries.py`: consume `cluster_status` as a
  bare list.
- `src/databricks_job_runner/clean.py`: same fix at the legacy
  reconciler call site (would have crashed identically when reached).
- `tests/test_libraries.py`, `tests/test_preflight.py`: corrected the
  masking fakes to the real bare-list shape; added an SDK-contract
  regression test so the bug cannot be re-masked.
- `CHANGELOG.md`: new; `0.4.10` fix entry plus a backfilled `0.4.9`
  entry (closes the V1 changelog item, which was ticked without a file).
- Runner suite: 43 passed.

Note: the operator published `0.5.1` (not the planned `0.4.10`); `0.5.1`
is the recorded runner version. The `_CLIENT_PINNED_CLOSURE` does not
carry a runner pin, so only `_INGEST_PINNED_CLOSURE`, `pyproject.toml`,
and `uv.lock` changed for the consumer bump.

#### Second attempt: runner 0.5.1 — deploy/runner path validated; dbxcarta verify bug found and fixed

Pin bumped `0.4.9 -> 0.5.1`, wheels rebuilt (`uv build --all-packages`),
stable wheels and bootstrap re-published (`dbxcarta upload --wheel`),
and `dbxcarta submit-entrypoint ingest --compute cluster` rerun.

Deploy/runner path: fully validated (see the V3 step notes above). The
0.5.1 SDK fix worked in production — the preflight ran with no
`AttributeError`, failed-fast while the maven lib was `pending`, then
passed once `INSTALLED`. Bootstrap, per-run install, closure lock, smoke
check (`ok (6)`), JVM probe, and secret-scope loading all behaved.

The run then failed at the **dbxcarta verify gate** with 6 violations
(Database/Schema/Table/Column/REFERENCES/Value all "Neo4j has 0, summary
reported N"). This was **not** an empty write: the unscoped
`query_counts` in `run.py` recorded the correct non-zero counts
(`Database 1, Schema 1, Table 16, Column 169, Value 111, REFERENCES 18`).

Root cause (pre-existing dbxcarta bug, outside harden-deploy scope):
node ids are normalized via `contract.generate_id` (lowercase; spaces
and hyphens become underscores), but the catalog-scoped verify checks in
`verify/graph.py:_check_node_counts`,
`verify/references.py:_check_edge_count`, and
`verify/values.py:_check_value_count` filtered `n.id` against the **raw**
`summary["catalog"]`. With catalog `graph-enriched-lakehouse` and schema
`graph-enriched-schema`, the raw-hyphen scope never matched the
underscore-normalized ids, so every scoped count read 0. The verify
package had no unit tests, so this shipped silently and was only exposed
by a workspace whose catalog/schema names contain hyphens.

Fix applied in the dbxcarta consumer (uncommitted):

- Added `verify.scoped_catalog(summary) -> (catalog_id, id_prefix)` in
  `verify/__init__.py`, normalizing the catalog through `generate_id`
  (single source of truth so the three sites cannot drift again).
- Wired it into `verify/graph.py`, `verify/references.py`, and
  `verify/values.py`.
- Added `tests/spark/test_verify_scope.py`: a hyphenated catalog/schema
  regression test (would have caught this) plus a negative test that a
  genuine count mismatch is still flagged. Full consumer suite: 366
  passed, 1 skipped, 3 deselected.

#### Closed: verify fix validated live

The consumer was rebuilt (`uv build --all-packages`), the stable wheels
re-published (`dbxcarta upload --wheel`) with the verify fix, and
`dbxcarta submit-entrypoint ingest --compute cluster` rerun against the
already-warm cluster `0515-141455-wb8qxgo2`.

Outcome (run `67154826827483`, task `770822335752429`):

- `[dbxcarta] run_id=local job=dbxcarta status=success catalog=graph-enriched-lakehouse`
- `verify: ok=True violations=0` with `DBXCARTA_VERIFY_GATE=true`.
- Closure installed `databricks-job-runner==0.5.1`; manifest
  `smoke_check: "ok (6)"`.
- No cluster restart: same `spark_context_id` as the prior 0.5.1 run,
  preflight saw `Cluster: RUNNING` with no `PENDING` waits.

Phase V3 is complete. Phases V4–V6 remain (independent improvements).

### Phase V4: Cluster configuration drift check

Goal: a run fails fast when the cluster no longer has the access mode,
runtime, or Spark configuration the connector requires. This is the
original Phase 3. The mechanism is a generic comparison in the runner
and the policy is owned by dbxcarta.

Steps:
- [ ] Add a generic cluster-shape comparison mechanism to the runner,
      surfaced as a preflight.
- [ ] Supply the dbxcarta-owned expected cluster shape as policy.
- [ ] Publish a runner release and bump the dbxcarta pin.

Done when: a drifted cluster fails preflight with an actionable message.

### Phase V5: Reduce restarts and remove legacy runner code [Status: quality-reviewed; publish and pin bump pending]

Goal: keep the warm cluster warm across the active window, and remove
the superseded library-side code. This is the original Phase 4 plus the
deferred library-side legacy cleanup.

Steps:
- [~] Reduce restart frequency for the active development window so the
      connector is not re-resolved. Deferred: the bootstrap model already
      eliminates the restart cause (no Python library reconciler, no
      cluster-library sync). Cluster auto-termination is a configuration
      concern outside the runner scope.
- [x] Remove the legacy versioned-upload and reconciler code paths from
      the runner once no consumer depends on them. Removed in the runner:
      `sync_cluster_libraries`, `build_and_upload_wheel`,
      `_bump_patch_version`, `_delete_old_wheels_from_volume`,
      `clean_cluster_wheel_libraries`, `Runner.upload_wheel`, and the
      `upload --wheel` CLI subcommand. `sync_cluster_libraries` dropped
      from the public API. CHANGELOG entry added for `0.6.0`. dbxcarta
      confirmed to have no call sites for any removed symbol.
      Quality review: stale `upload --wheel` references removed from
      `docs/commands.md` (table row, mutual-exclusivity note, description
      bullet) and `runner.py` (`wheel_package` docstring); extra blank
      lines in `upload.py` and `libraries.py` fixed. 42 tests pass.
- [ ] Publish a runner release (`0.6.0`) and bump the dbxcarta pin.

Done when: the warm cluster stays warm across the window and the legacy
paths are gone.

### Phase V6: Update runner documentation

Goal: databricks-job-runner documentation reflects the bootstrap model.
This is the original Phase 6.

Steps:
- [ ] Update the README and guidance to describe the
      bootstrap-from-Volume model, the stable Volume wheel path, the
      per-run install target, the JVM-only cluster libraries, the
      first-class preflight hook, and the JVM probe.
- [ ] Publish a runner release if the docs ship with a code change.

Done when: the documentation matches the shipped model.

## Open decisions

- Whether Phase V4 and Phase V5 land before or after the dbxcarta switch
  in Phase V2.
