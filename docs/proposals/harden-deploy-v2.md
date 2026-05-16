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

### Phase V2: Switch the dbxcarta consumer to the published runner

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
- [ ] Move the committed dbxcarta runner pin to `==0.4.9`.
- [ ] Switch the ingest and client submit-entrypoints from
      `PythonWheelTask` to `submit_bootstrap` with a `BootstrapConfig`.
- [ ] Supply the policy values: the fully pinned dependency closure with
      binary wheels (DBR-provided packages excluded as above), the
      `org.neo4j.spark.DataSource` probe class for ingest, and the smoke
      imports.
- [ ] Wire `maven_libraries_preflight` for the Neo4j connector into a
      dedicated ingest runner so submit fails fast when the connector is
      missing, pending, or failed.
- [ ] Switch the wheel upload path from the versioned upload to
      `publish_wheel_stable` plus `upload_all` so the bootstrap script
      ships and the stable Volume wheel is published.
- [ ] Remove `_sync_cluster_libraries`, `_wait_for_cluster_libraries`,
      and the `_ENTRYPOINT_CLUSTER_PYPI_PACKAGES` table, and stop
      installing or uninstalling Python libraries on the cluster as part
      of submit.
- [ ] Add a unit test asserting the curated ingest and client closures
      exclude the DBR-provided packages (`pyspark`, `py4j`,
      `databricks-sdk`).

Done when: dbxcarta submits the ingest through the bootstrap path on the
published runner, the connector preflight is enforced on submit, the
closure test passes, and no consumer Python-library sync remains.

### Phase V3: Validate the full dbxcarta ingest on the warm cluster

Goal: the real ingest path works end to end, exercising the parts the
smoke harness does not: the Neo4j connector probe, the pinned closure
lock and smoke check with a real closure, and secret-scope loading.

Steps:
- [ ] Run the full dbxcarta ingest on the warm cluster.
- [ ] Confirm new code is picked up with no version bump and no cluster
      restart.
- [ ] Confirm the connector probe, the closure lock and smoke check, and
      secret-scope loading all behave correctly.
- [ ] If a defect is found, fix it in the runner, publish the next patch
      version, bump the dbxcarta pin, and rerun. Repeat until the ingest
      is clean.

Done when: a full ingest succeeds against a published runner version
with no manual code step and no restart.

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

### Phase V5: Reduce restarts and remove legacy runner code

Goal: keep the warm cluster warm across the active window, and remove
the superseded library-side code. This is the original Phase 4 plus the
deferred library-side legacy cleanup.

Steps:
- [ ] Reduce restart frequency for the active development window so the
      connector is not re-resolved.
- [ ] Remove the legacy versioned-upload and reconciler code paths from
      the runner once no consumer depends on them.
- [ ] Publish a runner release and bump the dbxcarta pin.

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
