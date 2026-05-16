# Harden the All-Purpose Cluster Deploy Path

## Compute Decision

### Why classic compute, not per-run job compute

The ingest pipeline depends on the Neo4j Spark Connector, a JVM/Scala
library on the Spark classpath. A per-run job cluster is a fresh classic
cluster created for each run and destroyed afterwards. It can be
configured identically to the all-purpose cluster, so it would work
functionally. The cost is what makes it unworkable here: every run pays
cluster cold-start plus a fresh Maven resolution and download of the
connector. For a tight development loop that cost is paid on every
iteration and is too painful. A warm all-purpose cluster pays that cost
once and amortizes it across every run.

Decision: keep the warm all-purpose classic cluster as the compute
model. Do not move to per-run job clusters.

### Why serverless does not work for ingest

Serverless jobs compute has been tested for this ingest path and does
not work with the Neo4j Spark Connector. The ingest path depends on the
connector being available to Spark as a Scala/JVM Maven artifact, plus
the connector-specific Spark configuration needed for the write path.
That combination is not available in the tested serverless job
environment, and the ingest entrypoint already refuses serverless
explicitly.

Decision: serverless is out of scope for the ingest path and must
continue to be rejected for ingest submissions. It remains available
only for paths that have no Neo4j Spark Connector dependency.

### Why the all-purpose cluster path is currently fragile

The cluster-library mechanism is being used for two things with opposite
lifecycles. The Neo4j connector is stable and set once. The project
wheel and its Python dependencies change on every iteration. Routing the
fast-changing artifacts through the same persistent cluster-library
mechanism causes cross-run state leakage. Everything fragile in the
recent history exists to fight that leakage: bumping the wheel patch
version on every upload to dodge cache, deleting old wheels from the
Volume, scrubbing stale wheel libraries off the cluster, and a
reconciler that forces a cluster restart whenever a library changes. A
restart re-resolves the Neo4j connector from Maven, which reintroduces
the exact cost the all-purpose cluster was chosen to avoid.

### Execution model: bootstrap script, not a wheel task

Today the ingest path submits a `PythonWheelTask`. A `PythonWheelTask`
resolves its console entry point from an already-installed wheel, so the
wheel must be present on the cluster before the task starts. That is the
reason the wheel is attached as a cluster or task library today, and it
is the reason "install the wheel at run startup" cannot work while the
ingest path stays a `PythonWheelTask`. The entry-point code only runs
after the package is importable, so it cannot be the thing that installs
the package.

Decision: the ingest path stops using `PythonWheelTask`.
databricks-job-runner ships a small generic bootstrap submitted as a
`SparkPythonTask`. The bootstrap's own code depends only on the Python
standard library and pip, so it does not depend on the
databricks-job-runner package version or the project wheel and there is
no installer-installs-itself cycle. It installs the project wheel from
the Volume and the pinned dependencies, then imports `inject_params`
*from the just-installed project wheel* and calls it so .env parameters
and secret-scope keys are loaded, then imports the configured console
entry point and calls it. `inject_params` is a project-wheel symbol, not
a bootstrap dependency; it is only imported after the install step. The
bootstrap source is uploaded next to the other scripts and is not
installed as a library.

The pinned dependencies are pip-installable Python packages. Several are
not literally pure Python: pydantic v2 ships `pydantic-core` as a
compiled extension, so installation relies on a matching binary wheel
existing for the cluster's Python version and platform rather than a
source build. "Pinned dependencies" below means this pip-installable
set, binary wheels included, not strictly pure-Python packages.

A warm all-purpose cluster has one shared driver Python environment
across runs. Force-reinstalling the project wheel into shared
site-packages races when two runs overlap and can make one run execute
another run's code. The bootstrap therefore installs the project wheel,
the fast-changing artifact, into a per-run target directory and prepends
that directory to `sys.path` for the entry point. The pinned
dependencies are version-stable, so the bootstrap installs them into the
shared driver environment, where pip no-ops them once they are
satisfied. This mirrors the Neo4j connector: stable pinned dependencies
are resolved once per cluster lifetime and shared, while the
fast-changing project code is isolated per run. The first-run resolution
hits the package index once per cluster lifetime, which is acceptable
because later runs no-op against the satisfied shared environment.
Concurrent runs get independent code. The development loop is still
expected to be effectively single-run, but correctness no longer depends
on that.

The bootstrap installs on the driver only. This is correct only if no
project-owned Python runs on executors. The Neo4j Spark Connector is JVM
and runs on executors with no project Python. The phase that introduces
the bootstrap must verify the ingest job runs no project-owned Python in
executor closures such as UDFs, `mapInPandas`, or `applyInPandas`. If it
does, driver-only install is insufficient and the design must be revised
before proceeding.

## Core Goals

1. Keep the warm all-purpose classic cluster as the compute model.
2. Never require a cluster restart to pick up new project code.
3. Resolve the Neo4j Maven connector at most once, not per run and not
   per code change.
4. Make a missing or broken connector fail fast with a clear message
   before a run is submitted, not as a mid-run class-not-found error.
5. Remove the fragile version-bump, Volume-pruning, and stale-library
   subsystems entirely.
6. Each run gets the current project code with no manual steps.
7. Overlapping runs never execute each other's code, and correctness
   does not depend on serializing runs.

## Decisions

- **Adopt bootstrap-from-Volume.** The project wheel and its pinned
  dependencies are installed at run startup by a generic bootstrap
  script submitted as a `SparkPythonTask`. They are not attached as
  cluster or task libraries and the ingest path no longer uses a
  `PythonWheelTask`. This is a deliberate behavior change and is worth
  it for the simplification.
- **Minimal bootstrap.** The bootstrap depends only on the Python
  standard library and pip. It does not import the project wheel or a
  pinned databricks-job-runner version, so there is no bootstrap
  dependency cycle. databricks-job-runner ships the bootstrap source and
  the submit path uploads it alongside the scripts.
- **Per-run install target for the project wheel.** The bootstrap
  installs the project wheel into a per-run directory and prepends it to
  `sys.path` rather than `--force-reinstall` into shared cluster
  site-packages. Overlapping runs on the shared driver Python stay
  independent because each run loads its own copy of the project code.
- **Install scope.** The project wheel is installed into the per-run
  target with `--force-reinstall --no-deps`. Because pip's `--target`
  does not cleanly overwrite an existing populated directory, the per-run
  target must be a fresh directory per run, never reused. The pinned
  dependencies are installed into the shared driver environment as a
  normal pinned install that pip no-ops when already satisfied, so in
  steady state they are resolved once per cluster lifetime and later
  runs do not reinstall neo4j, pydantic, and the rest.
- **Split artifacts by lifecycle.** The Neo4j Maven connector stays a
  pinned cluster library because it is JVM and pip cannot install it.
  Everything Python moves to the per-run bootstrap install.
- **Stable Volume path, versioned build.** The local build keeps a real
  PEP 427 versioned wheel filename, which the PyPI release and any
  `package_name` resolution still need. Only the Volume copy is written
  to a fixed destination name. The per-upload patch-version bump is
  removed.
- **Assert, do not reconcile.** Before submitting a run, verify the
  required JVM cluster libraries are installed and healthy and fail fast
  otherwise. Stop installing or uninstalling Python libraries on the
  cluster.
- **Detect cluster config drift, with split ownership.** The consumer
  owns the expected cluster shape because it chooses the connector
  version and the connector Spark configuration. databricks-job-runner
  provides a generic comparison mechanism. The policy lives in dbxcarta,
  the mechanism lives in the library.
- **Preserve secret injection.** The new bootstrap must still run
  `inject_params`, which also drives secret-scope loading for the
  NEO4J_* keys, before settings construction.
- **Reduce restart frequency.** Keep the warm cluster warm for the
  active development window so the connector is not re-resolved.
- **Delete dead and superseded code.** Remove the version bump
  (`_bump_patch_version`) and the Volume wheel pruning
  (`_delete_old_wheels_from_volume`) in databricks-job-runner
  `upload.py`, the ad-hoc cluster wheel scrub
  (`clean_cluster_wheel_libraries`) in `clean.py`, the wheel and Python
  branches of the library reconciler in `libraries.py`, and the
  consumer's Python-library sync and wait loop
  (`_sync_cluster_libraries` and `_wait_for_cluster_libraries`) in
  dbxcarta `cli.py`.

## Proposal

### Problem

Running the ingest pipeline on the shared all-purpose cluster is
unreliable and slow to change. The ingest path is a `PythonWheelTask`,
which requires the wheel to be installed on the cluster before the task
starts, so new project code is not reliably picked up without a
version-bump trick that leaves uncommitted churn and gaps in the version
series on failure. The shared driver Python means overlapping runs can
collide on the same site-packages. Library changes force a cluster
restart, which re-resolves the Neo4j connector from Maven and reimposes
the cold cost the warm cluster exists to avoid. A misconfigured cluster
or a missing connector surfaces as a cryptic Spark error in the middle
of a run instead of before it starts.

### Solution

Treat the two artifact classes by their lifecycle. Submit the ingest
path as a `SparkPythonTask` that runs a generic stdlib-and-pip-only
bootstrap. The bootstrap installs the project wheel from a stable Volume
location into a per-run directory at startup, with a forced reinstall so
the newest code always wins without changing the filename or restarting
the cluster, installs the pinned dependencies into the
shared driver environment where pip no-ops them once they are satisfied,
then invokes the configured console entry point. Keep only the stable JVM Neo4j connector as a pinned cluster
library, and verify it is present and healthy before each run rather
than reconciling it. Add a preflight that checks the cluster still has
the access mode, runtime, and Spark configuration the connector
requires, with the policy owned by the consumer and the mechanism owned
by the library. Remove the version-bump, Volume-pruning, and
stale-library code paths that only existed to fight cross-run leakage.

### Requirements

- A run must execute the most recently uploaded project code without any
  manual version change and without a cluster restart.
- The ingest path must not use a `PythonWheelTask`. It must run a
  bootstrap as a `SparkPythonTask`.
- The bootstrap must depend only on the Python standard library and pip.
- The wheel must live at a stable, predictable Volume path while the
  local build keeps a real PEP 427 versioned filename.
- The project wheel must be installed at run startup into a per-run
  target so overlapping runs are isolated.
- The pinned dependencies must be installed at run startup
  into the shared driver environment, not as cluster libraries, so pip
  no-ops them once satisfied.
- Overlapping runs must not execute each other's code.
- Steady-state runs must not reinstall already-satisfied pinned
  dependencies.
- Secret-scope keys must still load under the bootstrap.
- The Neo4j Maven connector must remain a pinned cluster library and
  must be re-applied automatically if the cluster restarts.
- Submitting a run must fail fast with a clear message if the connector
  is missing, failed, or pending.
- Submitting a run must warn clearly if the cluster's access mode,
  runtime, or connector Spark configuration has drifted from what the
  connector requires.
- The version-bump, Volume wheel-pruning, ad-hoc cluster wheel scrub,
  and the wheel/Python branches of the library reconciler must be
  removed.
- The change ships as a new released version of databricks-job-runner,
  followed by updated project documentation, followed by the dbxcarta
  consumer adopting it.

## Phased Plan

Each implementation phase ends with an explicit Review gate. Do not
start the next phase until the Review gate passes. The connector
preflight ships first because it is genuinely read-only and additive: it
only adds an assertion and removes nothing, so it protects the window in
which the later phases change behavior. The removal of the existing
Python-library sync is deliberately *not* in Phase 1, because that
removal is a breaking change, not an additive one, and the bootstrap
that replaces it does not exist until Phase 2. Phase 1 and Phase 2 must
therefore ship together: until Phase 2 lands there is no mechanism to
get project Python onto the cluster, so the Python-library sync must
stay in place through the end of Phase 1. Phases 1 through 6 are
databricks-job-runner changes and are validated within that package.
Phase 7 adopts the released runner in dbxcarta and validates the full
path end to end.

### Phase 1: Connector preflight assertion

Goal: a run is not submitted unless the required JVM cluster libraries,
including the Neo4j connector, are installed and healthy.

Checklist:
- [ ] Wire the existing `check_cluster_libraries` status check into the
      validate and submit paths as a preflight that asserts only JVM
      libraries, by passing a maven-only desired list and asserting not
      `(plan.missing or plan.pending or plan.failed)`.
- [ ] Fail fast with an explicit message naming the connector when it
      is missing, failed, or pending.
- [ ] Handle the preflight/restart race: the connector can be healthy
      at preflight and then a cluster restart re-resolves it from Maven
      while the run is starting, reproducing the mid-run class-not-found
      this design exists to kill. Re-assert connector health at run
      start (not only at submit) and block until the JVM libraries are
      healthy before the entry point runs, rather than trusting the
      submit-time check.
- [ ] Do not change the existing Python-library sync in this phase. It
      must keep working until Phase 2 replaces it with the bootstrap;
      removing it here would leave ingest with no way to get project
      Python onto the cluster.

Validation:
- [ ] With the connector absent, submit refuses before creating a run
      and the message names the connector.
- [ ] With the connector healthy, submit proceeds unchanged.
- [ ] A restart between submit and run start does not produce a mid-run
      class-not-found: the run waits for the connector to be healthy.

Review gate:
- [ ] Confirm the preflight is purely additive and the existing
      Python-library sync is unchanged.
- [ ] Confirm the failure message is actionable.
- [ ] Confirm the preflight/restart race is closed by a run-start
      re-assertion.
- [ ] Confirm the plan still matches the goal and record any deviation.

### Phase 2: Bootstrap install from a stable Volume path

Goal: the ingest path stops using `PythonWheelTask`. A generic bootstrap
submitted as a `SparkPythonTask` installs the project wheel and pinned
dependencies from a stable Volume path into a per-run directory at
startup, then invokes the configured console entry point. No Python
artifact is attached as a cluster or task library.

Checklist:
- [ ] Add a generic bootstrap module to databricks-job-runner that
      depends only on the Python standard library and pip. Given a
      Volume wheel path, a pinned dependency list, a package name, and
      an entry-point name, it installs the wheel with
      `--force-reinstall --no-deps` into a fresh per-run target
      directory, installs the pinned dependencies into the shared driver
      environment, prepends the per-run directory to `sys.path`, sets
      `sys.argv` for the entry point (a console entry point built on
      click or argparse expects a script-style argv, so the bootstrap
      must construct it rather than calling the function bare), imports
      `inject_params` from the installed wheel and calls it, then
      imports and invokes the console entry point.
- [ ] Make the per-run target a fresh directory keyed by run id under a
      temp root, and have the bootstrap sweep target directories older
      than a bounded age at startup. Without this the per-run dirs grow
      unbounded on the warm cluster's local disk, reintroducing the
      cross-run cruft the Volume-pruning removal was meant to end.
- [ ] Log a one-line bootstrap manifest to the driver log: resolved
      wheel Volume path, wheel hash, per-run target directory, and the
      pinned dependency set. This is the cheap signal that answers "did
      my new code actually run" when debugging.
- [ ] Confirm the cluster's Python version and platform have matching
      binary wheels for the pinned dependencies (notably
      `pydantic-core`), since the shared-environment install relies on
      wheels rather than a source build.
- [ ] Remove the existing Python-library sync now that the bootstrap
      replaces it, and stop installing or uninstalling any Python
      libraries on the cluster as part of submit. This is the breaking
      removal deferred from Phase 1 and must land in the same change
      that introduces the bootstrap.
- [ ] Upload the bootstrap source alongside the other scripts. Do not
      attach it or the project wheel as a cluster or task library.
- [ ] Switch the ingest submit path from `PythonWheelTask` to a
      `SparkPythonTask` that runs the bootstrap with the wheel path,
      pinned dependency list, package name, and entry-point name as
      parameters.
- [ ] Publish the wheel to the Volume under a stable fixed destination
      filename, using an atomic publish: upload to a temporary name and
      then rename or replace into the fixed path, so a run that starts
      mid-upload never reads a partial or stale wheel. Keep the local
      build as a real PEP 427 versioned wheel.
- [ ] Make the version bump on upload a no-op for this path and remove
      its uncommitted side effect on the consumer project file.

Validation:
- [ ] A run after a fresh upload executes the new code with no version
      change and no cluster restart.
- [ ] The console entry point runs correctly with its expected argv;
      flags and arguments behave the same as under `PythonWheelTask`.
- [ ] The bootstrap manifest line appears in the driver log with the
      wheel hash and per-run target for the run.
- [ ] Per-run target directories older than the bounded age are swept;
      local disk does not grow unbounded across many runs.
- [ ] A run that starts while an upload is in progress never loads a
      partial or stale wheel.
- [ ] No new wheel filename appears on the Volume across iterations.
- [ ] Two overlapping runs that submit different code each execute their
      own code. Neither sees the other's wheel.
- [ ] Steady-state runs do not reinstall the pinned dependencies when
      they are already satisfied.
- [ ] The ingest job runs no project-owned Python in executor closures.
      Confirm there are no UDFs, `mapInPandas`, or `applyInPandas` that
      import the project wheel. If any exist, stop and revise:
      driver-only install is insufficient.
- [ ] Secret-scope keys NEO4J_URI, NEO4J_USERNAME, and NEO4J_PASSWORD
      are still loaded under the bootstrap and the run authenticates to
      Neo4j.

Review gate:
- [ ] Confirm the ingest path no longer uses `PythonWheelTask`.
- [ ] Confirm no cluster restart was required and no Python library was
      attached to the cluster.
- [ ] Confirm overlapping runs are isolated.
- [ ] Confirm the plan still matches the goal and record any deviation.

### Phase 3: Cluster configuration drift check

Goal: a run is not submitted silently against a cluster whose access
mode, runtime, or connector Spark configuration has drifted from what
the connector requires.

Checklist:
- [ ] Add a generic cluster-config comparison mechanism to
      databricks-job-runner that compares the live cluster against an
      expected shape passed by the caller and reports each drift.
- [ ] Declare the expected shape in dbxcarta, because the consumer owns
      the connector version and the connector Spark configuration. The
      expected shape covers the dedicated access mode, the pinned
      runtime, and the connector-related Spark configuration keys.
- [ ] Wire the comparison into the consumer submit path. Decide and
      document which drifts are warnings and which block the run.

Validation:
- [ ] A cluster with the wrong access mode produces a clear preflight
      message instead of a mid-run failure.
- [ ] A correctly configured cluster produces no drift warnings.

Review gate:
- [ ] Confirm the mechanism lives in the library and the policy lives
      in the consumer.
- [ ] Confirm the drift messages are specific enough to act on.
- [ ] Confirm blocking versus warning behavior matches intent.
- [ ] Confirm the plan still matches the goal and record any deviation.

### Phase 4: Reduce restarts and remove superseded code

Goal: the connector is re-resolved as rarely as possible and the
fragile subsystems are deleted.

Checklist:
- [ ] Provide a way to keep the warm cluster warm for an active
      development window so the connector is not re-resolved.
- [ ] Ensure the Neo4j connector is pinned so it re-applies
      automatically if the cluster does restart.
- [ ] Remove from databricks-job-runner: `_bump_patch_version` in
      `upload.py`, `_delete_old_wheels_from_volume` in `upload.py`,
      `clean_cluster_wheel_libraries` in `clean.py`, and the wheel and
      Python branches of the library reconciler in `libraries.py`.
- [ ] Keep only the JVM-assertion path from Phase 1 and the generic
      config comparison from Phase 3.

Validation:
- [ ] The full upload-then-run loop works with the removed code gone.
- [ ] No remaining call sites reference the deleted mechanisms.

Review gate:
- [ ] Confirm nothing still depends on the removed code.
- [ ] Confirm the public surface change is intentional and noted as a
      breaking change for consumers.
- [ ] Confirm the plan still matches the goal and record any deviation.

### Phase 5: Validate the consumer against the unreleased runner, then publish

Goal: the hardened databricks-job-runner is proven end to end against
dbxcarta *before* it is published, then released so the consumer can
depend on it. Publishing to PyPI is effectively irreversible for a
version string, so it must be the last step, not a step taken before
the only consumer has exercised the change.

Checklist:
- [ ] Point dbxcarta at the unreleased runner via a local path or
      pre-release dependency (not a published version) and run the full
      ingest path end to end on the warm cluster, including the
      `PythonWheelTask`-to-bootstrap switch, secret-scope loading, and
      the connector preflight.
- [ ] Only after that integration passes, prepare the release: final
      version, changelog entry, and a summary of the breaking change
      for consumers, including the move off `PythonWheelTask`.
- [ ] Record the exact released version string for the documentation
      and consumer phases.
- [ ] Ask the operator to deploy the new version to PyPI. Do not
      publish without explicit operator approval and without the
      pre-publish consumer integration passing.
- [ ] Confirm the published version is installable.

Validation:
- [ ] An end-to-end dbxcarta ingest run against the unreleased runner
      succeeds before any publish: code picked up with no version bump
      or restart, secrets loaded, connector preflight enforced.

Review gate:
- [ ] Confirm the pre-publish consumer integration passed before the
      operator published.
- [ ] Confirm the operator approved and the release is live.
- [ ] Confirm the exact version string is recorded for the
      documentation and consumer phases.

### Phase 6: Update documentation and project details

Goal: databricks-job-runner documentation reflects the new model.

Checklist:
- [ ] Update the package README to describe the bootstrap-from-Volume
      model, the stable Volume wheel filename, the per-run install
      target, the JVM-only cluster libraries, and the preflights.
- [ ] Update the package guidance file so the architecture notes match
      the new submit, upload, and clean behavior.
- [ ] Remove documentation of the version-bump and Volume-pruning
      behavior and document the breaking change and migration steps,
      including the move off `PythonWheelTask`.

Review gate:
- [ ] Confirm the docs match the shipped behavior with no stale
      references to removed mechanisms.
- [ ] Confirm the migration steps are sufficient for a consumer.

### Phase 7: Update the dbxcarta consumer project

Goal: dbxcarta adopts the hardened runner and drops its own
cluster-library reconciliation for Python artifacts.

Checklist:
- [ ] Bump the databricks-job-runner dependency to the exact version
      recorded in Phase 5.
- [ ] Switch the ingest submit-entrypoint path in dbxcarta `cli.py`
      from `PythonWheelTask` to the bootstrap `SparkPythonTask`.
- [ ] Remove `_sync_cluster_libraries` and `_wait_for_cluster_libraries`
      from dbxcarta `cli.py`.
- [ ] In `_ENTRYPOINT_CLUSTER_PYPI_PACKAGES`, drop the
      `databricks-job-runner==` pin and the pinned dependency
      entries. The pinned dependencies move into the
      bootstrap dependency list passed at submit. Keep the Neo4j Maven
      connector as the only pinned cluster library and rely on the
      Phase 1 assertion.
- [ ] Remove the consumer's now-redundant reconciliation and wait
      loops.
- [ ] Run the ingest entrypoint end to end on the warm cluster.

Validation:
- [ ] A code change is picked up by the next run with no version bump
      and no restart.
- [ ] A run with the connector absent fails fast at preflight.
- [ ] Secret-scope keys load and the run authenticates to Neo4j.
- [ ] An end-to-end ingest run succeeds.

Final review gate:
- [ ] Confirm the consumer no longer reconciles Python cluster
      libraries and no longer uses `PythonWheelTask` for ingest.
- [ ] Confirm the warm-cluster goals are met end to end.
- [ ] Confirm the plan is fully delivered and close it out.

## Completion Criteria

- A code change is reflected in the next run with no version change and
  no cluster restart.
- The Neo4j connector is resolved at most once per cluster lifetime, not
  per run or per code change. Verified by inspecting cluster library
  install events / Maven resolution events across a series of runs on
  one warm cluster: exactly one connector resolution, none per run.
- Overlapping runs never execute each other's code.
- A missing connector or drifted cluster fails fast at preflight with a
  clear message.
- The version-bump, Volume-pruning, ad-hoc scrub, and Python-library
  reconciliation code is gone, and the ingest path no longer uses
  `PythonWheelTask`.
- databricks-job-runner is released, documented, and adopted by
  dbxcarta.
