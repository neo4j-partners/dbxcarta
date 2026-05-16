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
the Volume and the pinned dependency closure, prepends the per-run
target to `sys.path`, runs a JVM classpath probe for the connector,
reconstructs `sys.argv` to exactly what the console entry point would
have seen under `PythonWheelTask`, then resolves and invokes the
configured console entry point.

The bootstrap preserves `PythonWheelTask` semantics. Under
`PythonWheelTask` today, nothing external calls `inject_params`. The
console entry point calls it itself: `dbxcarta-ingest` resolves to
`dbxcarta.spark.entrypoint:main`, which calls
`dbxcarta.spark.env.inject_params()` before `run_dbxcarta()`. The
bootstrap therefore does not call `inject_params`. It resolves the
console entry point by its `console_scripts` name through
`importlib.metadata.entry_points` against the just-installed wheel, the
same resolution `PythonWheelTask` performs, and the entry point performs
its own parameter and secret-scope injection exactly as before. This is
a deliberate deviation from the earlier draft, which had the bootstrap
import and call `inject_params`. See the Deviations section.

The pinned dependencies are pip-installable Python packages. Several are
not literally pure Python: pydantic v2 ships `pydantic-core` as a
compiled extension, so installation relies on a matching binary wheel
existing for the cluster's Python version and platform rather than a
source build. "Pinned dependencies" below means a fully pinned closure
including transitive dependencies, with binary wheels included, not a
top-level list that pip resolves at install time.

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
and runs on executors with no project Python. A source scan of
`packages/dbxcarta-spark/src` finds no `udf`, `pandas_udf`,
`mapInPandas`, `applyInPandas`, `foreachPartition`, or `.rdd` usage, so
the ingest path runs no project-owned Python in executor closures and
driver-only install is sound. The phase that introduces the bootstrap
must re-confirm this on the actual run path before proceeding.

### Interface contract

The generic bootstrap cannot hardcode any dbxcarta symbol or coordinate.
Its inputs arrive as parameters and its policy values come from the
consumer.

- **argv contract.** `SparkPythonTask` passes every parameter as a
  positional argv token, and `inject_params` consumes `KEY=VALUE`
  tokens. To keep the bootstrap's own structured inputs from colliding
  with the project's `KEY=VALUE` env params, the submit path passes a
  single JSON blob as `argv[1]` carrying the wheel Volume path, the
  pinned dependency closure, the wheel package name, the console-script
  name, and the JVM probe class. The project's `KEY=VALUE` params follow
  as the remaining argv tokens. Before invoking the entry point the
  bootstrap rebuilds `sys.argv` to exactly the argv the console entry
  point would have seen under `PythonWheelTask`: the script name
  followed by the project params, with the JSON blob removed.
- **Preflight hook.** databricks-job-runner exposes a first-class
  preflight hook, configured as `Runner(preflights=[...])`. Both the
  validate path and the submit path invoke the same ordered, fail-fast
  hook list. Each hook receives `(ws, cluster_id, compute)` and returns
  a structured result with an ok flag and actionable messages. dbxcarta
  supplies a hook that asserts the maven-only desired library list. The
  mechanism lives in the library; the policy lives in the consumer.
- **Connector probe parameter.** The bootstrap JVM classpath probe class
  is a passed parameter, not a hardcoded coordinate. dbxcarta passes
  `org.neo4j.spark.DataSource`. The bootstrap obtains the Spark session
  via `getOrCreate()`, calls
  `spark._jvm.java.lang.Class.forName(<probe class>)`, and fails fast
  with an actionable message before resolving the entry point. The entry
  point reuses the same Spark session.

## Core Goals

1. Keep the warm all-purpose classic cluster as the compute model.
2. Never require a cluster restart to pick up new project code.
3. Resolve the Neo4j Maven connector at most once, not per run and not
   per code change.
4. Make a missing or broken connector fail fast with a clear message
   before a run is submitted, not as a mid-run class-not-found error.
5. Remove the fragile version-bump, Volume-pruning, and stale-library
   subsystems from the dbxcarta path.
6. Each run gets the current project code with no manual steps.
7. Overlapping runs never execute each other's code, and correctness
   does not depend on serializing runs.

## Decisions

- **Adopt bootstrap-from-Volume.** The project wheel and its pinned
  dependency closure are installed at run startup by a generic bootstrap
  script submitted as a `SparkPythonTask`. They are not attached as
  cluster or task libraries and the ingest path no longer uses a
  `PythonWheelTask`. This is a deliberate behavior change and is worth
  it for the simplification.
- **Minimal bootstrap.** The bootstrap depends only on the Python
  standard library and pip. It does not import the project wheel or a
  pinned databricks-job-runner version, so there is no bootstrap
  dependency cycle. databricks-job-runner ships the bootstrap source and
  the submit path uploads it alongside the scripts.
- **Entry point owns injection.** The bootstrap does not call
  `inject_params`. It resolves the console entry point by its
  `console_scripts` name through `importlib.metadata.entry_points`
  against the installed wheel, matching `PythonWheelTask` resolution,
  and the entry point performs its own `inject_params` and secret-scope
  loading as it does today.
- **Per-run install target for the project wheel.** The bootstrap
  installs the project wheel into a per-run directory keyed by run id
  and prepends it to `sys.path` rather than `--force-reinstall` into
  shared cluster site-packages. Overlapping runs on the shared driver
  Python stay independent because each run loads its own copy of the
  project code.
- **Install scope.** The project wheel is installed into the per-run
  target with `--force-reinstall --no-deps`. Because pip's `--target`
  does not cleanly overwrite an existing populated directory, the
  per-run target must be a fresh directory per run, never reused. The
  pinned dependency closure is installed into the shared driver
  environment with `--no-deps` over the fully pinned closure so pip
  performs no resolution. pip no-ops the closure once it is satisfied,
  so in steady state the dependencies are installed once per cluster
  lifetime and later runs do not reinstall neo4j, pydantic, and the
  rest.
- **Concurrent shared-environment safety.** First-run installs into the
  shared driver environment are serialized by a coarse advisory file
  lock implemented with stdlib `fcntl.flock`, keyed by a hash of the
  pinned dependency closure. The lock has a timeout and breaks a stale
  lock so a crashed run does not wedge every future run. The per-run
  project wheel install does not take the lock and proceeds in parallel.
- **Post-install smoke check.** After the shared-environment install the
  bootstrap imports the pinned top-level packages and logs the result in
  the manifest line, so a broken or partial shared environment fails
  fast with a clear signal.
- **Split artifacts by lifecycle.** The Neo4j Maven connector stays a
  pinned cluster library because it is JVM and pip cannot install it.
  Everything Python moves to the per-run bootstrap install.
- **Stable Volume path, versioned build, opt-in API.** The local build
  keeps a real PEP 427 versioned wheel filename, which the PyPI release
  and any `package_name` resolution still need. The stable-Volume-path
  publish is a new opt-in databricks-job-runner API. The existing
  versioned `build_and_upload_wheel` behavior, including
  `_bump_patch_version` and `_delete_old_wheels_from_volume`, is left
  intact for other consumers and is removed only in a later broad runner
  cleanup, not by this proposal. dbxcarta stops using the version-bump
  path and uses the stable publish.
- **Plain overwrite publish.** The Volume copy is written to a fixed
  destination name with `ws.files.upload(dest, overwrite=True)`. The
  Databricks Files API has no atomic rename on UC Volumes and a file
  write is a single PUT, so the earlier temp-and-rename requirement is
  dropped. The development loop is effectively single-run, so a run that
  starts mid-upload is an accepted residual risk rather than a
  correctness requirement. See the Deviations section.
- **Assert, do not reconcile.** Before submitting a run, verify the
  required JVM cluster libraries are installed and healthy and fail fast
  otherwise. Stop installing or uninstalling Python libraries on the
  cluster.
- **First-class preflight hook.** databricks-job-runner exposes a
  preflight hook list via `Runner(preflights=[...])`. Both validate and
  submit run the same ordered fail-fast hooks. The mechanism lives in
  the library; the maven-only desired list is dbxcarta policy.
- **Detect cluster config drift, with split ownership.** The consumer
  owns the expected cluster shape because it chooses the connector
  version and the connector Spark configuration. databricks-job-runner
  provides a generic comparison mechanism. The policy lives in
  dbxcarta, the mechanism lives in the library.
- **Preserve secret injection.** Secret-scope loading for the NEO4J_*
  keys runs inside the console entry point's existing `inject_params`
  call, before settings construction, unchanged by the bootstrap switch.
- **Reduce restart frequency.** Keep the warm cluster warm for the
  active development window so the connector is not re-resolved.
- **Runner release is the handoff.** The hardened databricks-job-runner
  is proven end to end against dbxcarta before publish, then released to
  PyPI. dbxcarta stays pinned to its current databricks-job-runner
  version until the new version is published, and only the dbxcarta
  consumer phase moves the committed pin to the exact published version.
- **Delete dead and superseded code in the dbxcarta path.** Remove the
  consumer's Python-library sync and wait loop
  (`_sync_cluster_libraries` and `_wait_for_cluster_libraries`) in
  dbxcarta `cli.py`. Library-side legacy upload code is descoped from
  this proposal as noted above.

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
location into a per-run directory at startup with a forced reinstall so
the newest code always wins without changing the filename or restarting
the cluster, installs the pinned dependency closure into the shared
driver environment under a coarse lock where pip no-ops it once
satisfied, runs a JVM classpath probe for the connector, then resolves
and invokes the configured console entry point. The entry point keeps
its own `inject_params` and secret-scope loading. Keep only the stable
JVM Neo4j connector as a pinned cluster library, and verify it is
present and healthy before each run through a first-class preflight hook
rather than reconciling it. Add a preflight that checks the cluster
still has the access mode, runtime, and Spark configuration the
connector requires, with the policy owned by the consumer and the
mechanism owned by the library. Remove the dbxcarta consumer's
Python-library sync and wait loops.

### Requirements

- A run must execute the most recently uploaded project code without any
  manual version change and without a cluster restart.
- The ingest path must not use a `PythonWheelTask`. It must run a
  bootstrap as a `SparkPythonTask`.
- The bootstrap must depend only on the Python standard library and pip.
- The bootstrap must not call `inject_params`. It must resolve the
  console entry point by its `console_scripts` name and invoke it, and
  the entry point must perform its own parameter and secret injection.
- The bootstrap must reconstruct `sys.argv` to the argv the console
  entry point would have seen under `PythonWheelTask`.
- The bootstrap must run a JVM classpath probe for a caller-supplied
  probe class before invoking the entry point and fail fast with an
  actionable message if the class is absent.
- The wheel must live at a stable, predictable Volume path written with
  a plain overwrite, while the local build keeps a real PEP 427
  versioned filename. The stable publish must be a new opt-in runner
  API that leaves the existing versioned upload behavior intact.
- The project wheel must be installed at run startup into a per-run
  target so overlapping runs are isolated.
- The pinned dependency closure must be installed at run startup into
  the shared driver environment with `--no-deps`, under a coarse lock
  keyed by the closure hash, not as cluster libraries, so pip no-ops it
  once satisfied.
- Overlapping runs must not execute each other's code.
- Steady-state runs must not reinstall an already-satisfied pinned
  dependency closure.
- Secret-scope keys must still load under the bootstrap, through the
  entry point's existing injection.
- The Neo4j Maven connector must remain a pinned cluster library and
  must be re-applied automatically if the cluster restarts.
- databricks-job-runner must expose a first-class preflight hook that
  both validate and submit invoke, and submitting a run must fail fast
  with a clear message if the connector is missing, failed, or pending.
- Submitting a run must warn clearly if the cluster's access mode,
  runtime, or connector Spark configuration has drifted from what the
  connector requires.
- The dbxcarta consumer's Python-library sync and wait loops must be
  removed.
- The change ships as a new released version of databricks-job-runner.
  dbxcarta stays pinned to its current runner version until that
  release is live, then the consumer phase moves the pin to the exact
  published version.

## Phased Plan

Status legend used in the phase titles below: `not started`,
`implemented, review gate pending`, `done`. A phase is only `done` once
its Review gate has been signed off. Checklist boxes are ticked when the
work is complete in the databricks-job-runner package. Items that
require a live warm cluster are validated in Phase 5 and Phase 7 and are
called out inline as deferred. Items that belong to the dbxcarta
consumer are Phase 7 and stay unticked here.

Each implementation phase ends with an explicit Review gate. Do not
start the next phase until the Review gate passes. Phase 1 and Phase 2
ship together as a single change: Phase 1 adds the read-only preflight
hook and removes nothing, while Phase 2 introduces the bootstrap and
removes the Python-library mechanism that replaces it. Until the
bootstrap exists there is no other way to get project Python onto the
cluster, so the breaking removal cannot land before the bootstrap.
Phases 1 through 6 are databricks-job-runner changes and are validated
within that package. The runner PyPI release in Phase 5 is the hard
handoff. Phase 7 adopts the released runner in dbxcarta and validates
the full path end to end.

### Phase 1: First-class connector preflight hook [Status: implemented in runner, review gate pending]

Goal: a run is not submitted unless the required JVM cluster libraries,
including the Neo4j connector, are installed and healthy, enforced
through a first-class runner preflight hook.

Checklist:
- [x] Add a first-class preflight hook to databricks-job-runner
      configured as `Runner(preflights=[...])`. Both the validate path
      and the submit path invoke the same ordered, fail-fast hook list.
      Each hook receives `(ws, cluster_id, compute)` and returns a
      structured result with an ok flag and actionable messages.
      Implemented in `preflight.py` as `run_preflights`, `Preflight`,
      and `PreflightResult`; wired into `Runner.submit` and
      `Runner.validate`.
- [x] Provide a library-side helper that wraps the existing
      `check_cluster_libraries` so a consumer can assert a maven-only
      desired list by asserting not
      `(plan.missing or plan.pending or plan.failed)`. Implemented as
      `maven_libraries_preflight`.
- [ ] Have dbxcarta register a preflight that supplies the maven-only
      desired list and fails fast with an explicit message naming the
      connector when it is missing, failed, or pending. Deferred to
      Phase 7: this is dbxcarta consumer wiring. The runner mechanism it
      depends on is in place.
- [x] Do not change the existing Python-library sync in this phase. It
      must keep working until Phase 2 replaces it with the bootstrap.
      The library-side sync was left untouched.

Validation:
- [x] With the connector absent, submit refuses before creating a run
      and the message names the connector. Covered by unit tests in
      `tests/test_preflight.py`.
- [x] With the connector healthy, submit proceeds unchanged. Covered by
      unit tests.
- [x] Both the validate path and the submit path invoke the same hook.
      Both call `run_preflights` with the same list; covered by unit
      tests. Live warm-cluster confirmation is deferred to Phase 7.

Review gate:
- [ ] Confirm the preflight hook is first-class in the runner and
      purely additive, and the existing Python-library sync is
      unchanged.
- [ ] Confirm the mechanism lives in the library and the maven-only
      policy lives in dbxcarta.
- [ ] Confirm the failure message is actionable.
- [ ] Confirm the plan still matches the goal and record any deviation.

### Phase 2: Bootstrap install from a stable Volume path [Status: implemented in runner, four hardening fixes applied, review gate pending]

Goal: the ingest path stops using `PythonWheelTask`. A generic bootstrap
submitted as a `SparkPythonTask` installs the project wheel and pinned
dependency closure from a stable Volume path into a per-run directory at
startup, probes the connector classpath, then resolves and invokes the
configured console entry point. No Python artifact is attached as a
cluster or task library.

Checklist:
- [x] Add a generic bootstrap module to databricks-job-runner that
      depends only on the Python standard library and pip. It reads a
      single JSON blob from `argv[1]` carrying the wheel Volume path,
      the pinned dependency closure, the wheel package name, the
      console-script name, and the JVM probe class. It installs the
      wheel with `--force-reinstall --no-deps` into a fresh per-run
      target directory keyed by run id, installs the pinned closure with
      `--no-deps` into the shared driver environment, prepends the
      per-run directory to `sys.path`, runs the JVM classpath probe via
      `getOrCreate()` and `spark._jvm.java.lang.Class.forName`,
      rebuilds `sys.argv` to the script name plus the project params
      with the JSON blob removed, resolves the console entry point by
      name from the just-installed per-run distribution through
      `importlib.metadata.distributions`, and invokes it. The bootstrap
      does not call `inject_params`. Implemented as `bootstrap.py`.
- [x] Serialize the shared-environment install with a stdlib
      `fcntl.flock` advisory lock keyed by a hash of the pinned closure,
      with a timeout and crashed-holder stale-lock recovery. The per-run
      wheel install does not take the lock. Implemented as
      `_closure_lock` keyed by `closure_hash`.
- [x] After the shared-environment install, import the pinned top-level
      packages as a smoke check and include the result in the manifest
      line. Implemented over a caller-supplied `smoke_imports` list,
      since import names cannot be reliably derived from version pins.
- [x] Make the per-run target a fresh directory keyed by run id under a
      temp root, and have the bootstrap sweep target directories older
      than a bounded age at startup, never sweeping the active run's
      directory. Document the age bound. Without this the per-run dirs
      grow unbounded on the warm cluster's local disk. Age bound default
      is 24 hours; root default `/local_disk0/tmp/dbxrunner` with a
      tempdir fallback.
- [x] Log a one-line bootstrap manifest to the driver log: resolved
      wheel Volume path, wheel hash, per-run target directory, the
      pinned closure hash, and the smoke-check result. This is the cheap
      signal that answers "did my new code actually run" when debugging.
      Emitted with the `BOOTSTRAP_MANIFEST` prefix.
- [ ] Confirm the cluster's Python version and platform have matching
      binary wheels for the pinned closure, notably `pydantic-core`,
      since the shared-environment install relies on wheels rather than
      a source build. Deferred: requires the live warm cluster, verified
      in Phase 5 and Phase 7.
- [ ] Remove the dbxcarta Python-library sync now that the bootstrap
      replaces it, and stop installing or uninstalling any Python
      libraries on the cluster as part of submit. This is the breaking
      removal that must land in the same change that introduces the
      bootstrap. Deferred to Phase 7: this is dbxcarta consumer code.
      The runner change that makes it possible is complete.
- [x] Upload the bootstrap source alongside the other scripts. Do not
      attach it or the project wheel as a cluster or task library.
      Implemented as `upload_bootstrap_script`, shipped by `upload_all`;
      `submit_bootstrap_job` attaches no library.
- [x] Switch the ingest submit path from `PythonWheelTask` to a
      `SparkPythonTask` that runs the bootstrap with the JSON blob as
      `argv[1]` and the project `KEY=VALUE` params following. Runner
      capability implemented as `submit_bootstrap_job`,
      `BootstrapConfig`, and `Runner.submit_bootstrap`. The dbxcarta
      ingest call site is switched in Phase 7.
- [x] Add a new opt-in stable-publish runner API that writes the wheel
      to the Volume under a fixed destination filename with
      `ws.files.upload(dest, overwrite=True)`. Keep the local build as a
      real PEP 427 versioned wheel. Leave the existing versioned upload
      behavior intact for other consumers. Implemented as
      `publish_wheel_stable` and `stable_wheel_name`;
      `build_and_upload_wheel` left intact.

Validation: these are end-to-end checks on the live warm cluster and
are deferred to Phase 5 and Phase 7. The underlying logic is covered by
runner unit tests now: argv and JSON parsing, `sys.argv`
reconstruction, closure hash, sweep age selection, the
METADATA-derived wheel filename, the closure lock, the smoke check, and
the no-library submit path.

- [ ] A run after a fresh upload executes the new code with no version
      change and no cluster restart.
- [ ] The console entry point runs correctly with its expected argv;
      flags and arguments behave the same as under `PythonWheelTask`.
- [ ] The bootstrap manifest line appears in the driver log with the
      wheel hash, per-run target, closure hash, and smoke-check result.
- [ ] Per-run target directories older than the bound are swept; local
      disk does not grow unbounded across many runs.
- [ ] No new wheel filename appears on the Volume across iterations.
- [ ] Two overlapping runs that submit different code each execute their
      own code. Neither sees the other's wheel.
- [ ] Steady-state runs do not reinstall the pinned closure when it is
      already satisfied, and the shared-install lock serializes a cold
      first run without wedging on a stale lock.
- [ ] The ingest run path runs no project-owned Python in executor
      closures. Re-confirm there are no UDFs, `mapInPandas`, or
      `applyInPandas` that import the project wheel on the actual run
      path. If any exist, stop and revise: driver-only install is
      insufficient.
- [ ] The JVM probe fails fast with an actionable message when the
      connector class is absent.
- [ ] Secret-scope keys NEO4J_URI, NEO4J_USERNAME, and NEO4J_PASSWORD
      are still loaded through the entry point's `inject_params` and the
      run authenticates to Neo4j.

Hardening fixes applied before the review gate:

A review of the Phase 2 implementation surfaced four correctness gaps
against the Core Goals and Requirements. All four are fixed in
databricks-job-runner before the gate. They are faithful tightenings of
the proposal, not deviations.

- [x] **Per-run-scoped entry-point resolution.** Requirement at
      lines 300-302 and the Decision at lines 177-182 require resolving
      the console entry point "against the just-installed wheel," in
      service of Core Goal 7. The first cut used a global
      `entry_points(group="console_scripts")` scan, which only resolved
      the per-run code by `sys.path[0]` precedence and could bind to a
      shared or stale install. Fixed to enumerate
      `importlib.metadata.distributions(path=[per_run_target])`, select
      the distribution whose normalized name equals the wheel package,
      take its `console_scripts` entry point by name, and fail fast on a
      missing distribution, missing script, or duplicate. Stdlib only,
      so the minimal-bootstrap Decision holds.
- [x] **Fresh per-run target, never reused.** The Decision at
      lines 189-193 and Requirement at lines 312-313 state the per-run
      target must be a fresh directory because `pip --target` does not
      cleanly overwrite a populated directory. The first cut used
      `mkdir(exist_ok=True)`, which leaves stale files when a same-run
      task retry reuses `run-$DATABRICKS_RUN_ID`. Fixed to `rmtree` and
      recreate the target when it already exists, giving a clean slate
      while keeping legitimate task retries working. Recreate rather
      than hard-fail is the proposal-faithful reading: the text requires
      "fresh, never reused," not "fail if present," and a retry must
      still run.
- [x] **Compute-derived preflight cluster id.** The preflight contract
      at lines 135-141 and Requirement 305 pass `(ws, cluster_id,
      compute)` to each hook, with `cluster_id is None` as the
      non-classic skip signal the maven helper depends on. The first cut
      passed the raw configured `databricks_cluster_id`, which disagrees
      with the actual compute when a serverless mode override is in
      effect. Fixed to derive the id from the compute object
      (`ClassicCluster.cluster_id`, else `None`) through one helper used
      at all three `run_preflights` call sites, so the hook contract is
      honored from a single source of truth.
- [x] **Crashed-holder stale-lock recovery, no unlink.** The Decision
      at lines 199-204 and Requirement 314-317 require the
      shared-environment install to be serialized and a stale lock
      broken only so "a crashed run does not wedge every future run."
      The first cut broke a stale lock by `unlink`, the classic
      `flock` anti-pattern: an alive original holder keeps its lock on
      the orphaned inode while a second process acquires the new file,
      permitting concurrent shared-environment installs. Fixed to
      ownership-based recovery: the holder records its pid in the lock
      file on acquire; on timeout the waiter reads the pid and reclaims
      the same file in place only when that process is dead
      (`os.kill(pid, 0)` raises `ProcessLookupError`), otherwise it
      times out with an actionable error naming the holding pid. This
      removes the false-stale failure mode and never allows concurrent
      shared-environment installs while a holder is alive. Stdlib only.

Review gate:
- [ ] Confirm the ingest path no longer uses `PythonWheelTask`.
- [ ] Confirm no cluster restart was required and no Python library was
      attached to the cluster.
- [ ] Confirm overlapping runs are isolated.
- [ ] Confirm the bootstrap calls no dbxcarta symbol and the entry
      point owns injection.
- [ ] Confirm the plan still matches the goal and record any deviation.

### Phase 3: Cluster configuration drift check [Status: not started]

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
- [ ] Wire the comparison into the consumer preflight hook list. Decide
      and document which drifts are warnings and which block the run.

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

### Phase 4: Reduce restarts [Status: not started]

Goal: the connector is re-resolved as rarely as possible.

Checklist:
- [ ] Provide a way to keep the warm cluster warm for an active
      development window so the connector is not re-resolved.
- [ ] Ensure the Neo4j connector is pinned so it re-applies
      automatically if the cluster does restart.
- [ ] Confirm dbxcarta no longer calls the legacy version-bump or
      Volume-pruning upload path and uses only the new stable publish.
      The library-side legacy upload code stays intact for other
      consumers and is out of scope for deletion in this proposal.

Validation:
- [ ] The full upload-then-run loop works through the stable publish
      and bootstrap path.
- [ ] No dbxcarta call site references the legacy version-bump upload.

Review gate:
- [ ] Confirm dbxcarta no longer depends on the legacy upload path.
- [ ] Confirm the library-side legacy code is intentionally retained
      and the descope from the earlier draft is recorded.
- [ ] Confirm the plan still matches the goal and record any deviation.

### Phase 5: Validate the consumer against the unreleased runner, then publish [Status: not started]

Goal: the hardened databricks-job-runner is proven end to end against
dbxcarta before it is published, then released so the consumer can
depend on it. Publishing to PyPI is effectively irreversible for a
version string, so it is the last step.

Note: databricks-job-runner ships a permanent generic harness at
`examples/bootstrap_smoke/` that runs the bootstrap-from-Volume path end
to end on a warm cluster with one command, with no dbxcarta coupling and
no PyPI publish. It builds a tiny console-script wheel, publishes it to
the stable Volume path, submits the bootstrap as a `SparkPythonTask`,
and prints the driver log. It deliberately uses an empty pinned closure
and no JVM probe, so it isolates and proves the bootstrap mechanics:
stable-Volume publish and read-back, fresh per-run install target,
per-run-scoped console entry point resolution, `sys.argv`
reconstruction, and no attached cluster or task library. Use it as the
cheap first signal before the full dbxcarta integration. The pinned
closure lock, smoke check, and JVM probe stay covered by runner unit
tests, and the connector and secret-scope paths are still exercised only
by the full dbxcarta ingest run below.

Checklist:
- [ ] Point dbxcarta at the unreleased runner via a temporary local
      path or pre-release dependency override, not a committed pin
      change, and run the full ingest path end to end on the warm
      cluster, including the `PythonWheelTask`-to-bootstrap switch, the
      entry point's secret-scope loading, the JVM probe, and the
      preflight hook.
- [ ] Only after that integration passes, prepare the release: final
      version, changelog entry, and a summary of the breaking change
      for consumers, including the move off `PythonWheelTask` and the
      new opt-in stable-publish API.
- [ ] Record the exact released version string for the documentation
      and consumer phases.
- [ ] Ask the operator to deploy the new version to PyPI. Do not
      publish without explicit operator approval and without the
      pre-publish consumer integration passing.
- [ ] Confirm the published version is installable. Revert the
      temporary override; the committed dbxcarta pin still points at the
      old version until Phase 7.

Validation:
- [ ] An end-to-end dbxcarta ingest run against the unreleased runner
      succeeds before any publish: code picked up with no version bump
      or restart, secrets loaded, connector preflight enforced, JVM
      probe passing.

Review gate:
- [ ] Confirm the pre-publish consumer integration passed before the
      operator published.
- [ ] Confirm the operator approved and the release is live.
- [ ] Confirm the exact version string is recorded and the committed
      dbxcarta pin was not moved in this phase.

### Phase 6: Update documentation and project details [Status: not started]

Goal: databricks-job-runner documentation reflects the new model.

Checklist:
- [ ] Update the package README to describe the bootstrap-from-Volume
      model, the stable Volume wheel filename, the per-run install
      target, the JVM-only cluster libraries, the first-class preflight
      hook, and the JVM probe.
- [ ] Update the package guidance file so the architecture notes match
      the new submit, upload, and preflight behavior.
- [ ] Document the new opt-in stable-publish API alongside the retained
      legacy versioned upload, and document the breaking change and
      migration steps, including the move off `PythonWheelTask`.

Review gate:
- [ ] Confirm the docs match the shipped behavior with no stale
      references.
- [ ] Confirm the migration steps are sufficient for a consumer.

### Phase 7: Update the dbxcarta consumer project [Status: not started]

Goal: dbxcarta adopts the hardened runner and drops its own
cluster-library reconciliation for Python artifacts.

Checklist:
- [ ] Bump the databricks-job-runner dependency to the exact version
      recorded in Phase 5. This is the only phase that moves the
      committed pin.
- [ ] Switch the ingest submit-entrypoint path in dbxcarta `cli.py`
      from `PythonWheelTask` to the bootstrap `SparkPythonTask`,
      passing the JSON blob and project params.
- [ ] Remove `_sync_cluster_libraries` and `_wait_for_cluster_libraries`
      from dbxcarta `cli.py`.
- [ ] In `_ENTRYPOINT_CLUSTER_PYPI_PACKAGES`, remove the ingest pip
      entries including the `databricks-job-runner==` pin. The pinned
      dependency closure moves into the bootstrap JSON blob passed at
      submit as a curated, fully pinned closure that excludes
      DBR-provided packages such as `pyspark`. Keep the Neo4j Maven
      connector as the only pinned cluster library and rely on the
      Phase 1 preflight hook.
- [ ] Add a test asserting the curated closure excludes DBR-provided
      packages.
- [ ] Register the dbxcarta preflight and drift checks in the runner
      preflight hook list.
- [ ] Run the ingest entrypoint end to end on the warm cluster.

Validation:
- [ ] A code change is picked up by the next run with no version bump
      and no restart.
- [ ] A run with the connector absent fails fast at preflight.
- [ ] Secret-scope keys load through the entry point and the run
      authenticates to Neo4j.
- [ ] An end-to-end ingest run succeeds.

Final review gate:
- [ ] Confirm the consumer no longer reconciles Python cluster
      libraries and no longer uses `PythonWheelTask` for ingest.
- [ ] Confirm the warm-cluster goals are met end to end.
- [ ] Confirm the plan is fully delivered and close it out.

## Deviations

These deviations from the earlier draft are deliberate and recorded here
so the per-phase review gates can confirm them rather than treat them as
drift.

- **Bootstrap does not call `inject_params`.** The earlier draft had the
  bootstrap import `inject_params` from the project wheel and call it
  before the entry point. The actual ingest console entry point already
  calls `dbxcarta.spark.env.inject_params()` itself, and a generic
  bootstrap must not hardcode a project symbol. The bootstrap preserves
  `PythonWheelTask` semantics by resolving the console-script name and
  letting the entry point own injection. Recorded at Phase 2.
- **Plain overwrite, no temp-and-rename.** The earlier draft required an
  atomic publish via upload to a temporary name then rename. UC Volumes
  have no atomic rename and a Files API write is a single PUT, so the
  Volume copy is a plain `overwrite=True` write to the fixed path. The
  effectively single-run development loop makes a mid-upload run an
  accepted residual risk. Recorded at Phase 2.
- **Legacy runner upload code retained.** The earlier draft deleted
  `_bump_patch_version`, `_delete_old_wheels_from_volume`, the cluster
  wheel scrub, and the wheel/Python reconciler branches from
  databricks-job-runner. Removing them globally is a breaking change for
  other consumers, including the vendored copies under
  `examples/integration`. This proposal makes the stable publish an
  opt-in API, leaves the legacy code intact, and limits removal to the
  dbxcarta consumer's own sync and wait loops. Library-side legacy
  deletion is deferred to a later broad runner cleanup. Recorded at
  Phase 4.
- **Pinned dependencies are a curated full closure.** The dependency
  pins are a deliberately curated, fully pinned closure including
  transitive dependencies, installed with `--no-deps`, rather than a
  top-level list pip resolves. This avoids a steady-state run pulling a
  newer transitive that conflicts with DBR-managed packages. A generated
  wheelhouse manifest is the better long-term supply-chain answer and is
  out of scope here. Recorded at Phase 7.

## Completion Criteria

- A code change is reflected in the next run with no version change and
  no cluster restart.
- The Neo4j connector is resolved at most once per cluster lifetime, not
  per run or per code change. Verified by inspecting cluster library
  install events and Maven resolution events across a series of runs on
  one warm cluster: exactly one connector resolution, none per run.
- Overlapping runs never execute each other's code.
- A missing connector or drifted cluster fails fast at preflight with a
  clear message.
- The dbxcarta Python-library reconciliation code is gone, the ingest
  path no longer uses `PythonWheelTask`, and dbxcarta no longer calls
  the legacy version-bump upload path.
- databricks-job-runner is released with a recorded version string,
  documented, and adopted by dbxcarta with the committed pin moved only
  in the consumer phase.
