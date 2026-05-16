# Cluster Library Sync Proposal

## Goal

Add an optional cluster-library check and sync workflow so DBxCarta can
prepare classic Databricks clusters before submitting wheel entrypoints,
turning dependency setup from an implicit per-run side effect into an
explicit, observable preflight step.

The immediate target is the Finance Genie E2E flow, where
`submit-entrypoint ingest --compute cluster` failed during Databricks
library installation (Step 8 in `docs/proposals/e2epipeline.md`). The
dependency inventory behind this proposal is recorded in
`docs/proposals/e2e-setup.md` and is the source of truth for which packages
are runtime-provided versus must be installed.

## Problem

On classic clusters, DBxCarta submits a Spark Python task with the project
wheel attached as a `Library(whl=...)`. Databricks installs that wheel and
resolves its Python dependencies at run start, on the driver, before user
code executes. That install fails when:

- the cluster has no PyPI egress,
- dependency resolution selects a package that conflicts with the runtime,
  or
- the wheel forces a download of a package the runtime already provides,
  such as PySpark.

The failure happens inside Databricks library installation and the job log
truncates the underlying pip error, so the operator sees a job failure with
no actionable cause.

## Current Runner Behavior

`databricks-job-runner` already mutates cluster libraries in one direction.
This proposal extends an existing mechanism rather than introducing a new
subsystem.

- `compute.ClassicCluster.decorate_task` attaches the wheel as
  `task.libraries = [Library(whl=wheel_path)]` on every classic submit.
  This is the implicit install path that fails today.
- `clean.clean_cluster_wheel_libraries` already reads cluster library
  status via `ws.libraries.cluster_status(cluster_id)` and removes stale
  project wheels via `ws.libraries.uninstall(...)`. It matches by the
  normalised wheel-name prefix, leaving unrelated libraries untouched.
- `Runner.upload_wheel` already calls `clean_cluster_wheel_libraries`
  whenever `DATABRICKS_CLUSTER_ID` is set, so old project wheels are
  uninstalled on every `upload --wheel`.

The missing primitives are: a generalized desired-vs-current comparison
across more than just project wheels, and the install side
(`ws.libraries.install`). Uninstall, status reading, and prefix-based
ownership detection already exist and should be generalized, not rewritten.

## Command Ownership

The proposal touches two CLIs. Keeping the boundary explicit avoids
wiring the new behavior in the wrong layer.

- `databricks-job-runner` owns `upload`, `submit`, `validate`, `logs`,
  `clean`, and the Unity Catalog admin commands. It is the layer that
  already talks to `ws.libraries`.
- DBxCarta owns `submit-entrypoint` and the project dependency manifests.
  It configures a `Runner` instance and is the layer that knows which
  packages are runtime-provided.

Generic reconciliation mechanics belong in the runner. The desired
manifest and the runtime-provided decisions belong in DBxCarta.

## Proposed Approach

Generalize the runner's existing wheel cleanup into manifest-driven
library reconciliation, then have DBxCarta opt into it for its wheel
upload and `submit-entrypoint` flows.

The runner should expose two behaviors:

- A read-only cluster library check that reports missing, stale, pending,
  and failed libraries against a supplied desired manifest.
- A mutating sync that installs missing libraries and optionally removes
  stale project-owned libraries.

DBxCarta should treat sync as part of the classic-cluster wheel path. For
DBxCarta, the classic cluster must be clean and synced before wheel
entrypoint submission, and the synced wheel should not also be attached as a
task-level library. The generic runner still exposes reusable reconciliation
mechanics; consuming projects decide whether to call them automatically.

## Assumptions

These are grounded in `docs/proposals/e2e-setup.md`, last checked
2026-05-15 against cluster `1029-205109-yca7gn2n`
(`spark_version=17.3.x-scala2.13`, `use_ml_runtime=true`, Spark 4.0.0,
Python 3.12.3, Scala 2.13.16).

- Classic cluster mode remains supported for ingest because DBxCarta uses
  the Neo4j Spark Connector, which is compute-scoped and has no serverless
  equivalent.
- Serverless remains a separate path. It uses job-environment
  dependencies (`Environment.dependencies`), not compute-scoped cluster
  libraries, so it is out of scope for this proposal.
- PySpark is Databricks Runtime provided. DBxCarta must not force a PyPI
  download of `pyspark` on a cluster; the runtime owns Spark and PySpark
  compatibility.
- DBR 17.3 LTS ML does not ship `databricks-job-runner`, `neo4j`,
  `pydantic-settings`, `python-dotenv`, the dbxcarta wheels, or the Neo4j
  Spark Connector JAR. These are the install targets.
- `databricks-sdk`, `pydantic`, and `requests` are runtime-provided at
  versions older than the repo lock. The broad manifest ranges are
  satisfied by the built-ins, so they should not be installed unless the
  project explicitly requires lock-exact pinning.
- The approved Neo4j Spark Connector coordinate for this Spark 4 / Scala 2.13
  E2E path is
  `org.neo4j:neo4j-connector-apache-spark_2.13:5.3.10_for_spark_3`.
- Uninstalling a library may not take effect until the cluster restarts.

## Non-Goals

- Do not replace Databricks cluster policies or Terraform for production
  platform governance.
- Do not mutate libraries on shared clusters by default.
- Do not solve private package mirror or offline wheelhouse creation in
  the first pass. The check should report a no-egress cluster clearly, but
  building a wheelhouse is separate work.
- Do not make serverless use cluster library APIs.

## Design

### Desired Library Manifest

DBxCarta produces a desired library manifest for each operation that needs
cluster preparation. The runner consumes the manifest and does not
hard-code any DBxCarta dependency.

For Finance Genie ingest the manifest includes:

- The uploaded `dbxcarta-spark` wheel, by its UC Volume path, only after
  the wheel has been built and uploaded.
- The Python packages missing from DBR 17.3 LTS ML, pinned to exact versions
  from `uv.lock`: `databricks-job-runner`, `neo4j`, `pydantic-settings`,
  `python-dotenv`, and the transitive `typing-inspection` that ships with
  `pydantic-settings`.
- The verified Neo4j Spark Connector Maven coordinate:
  `org.neo4j:neo4j-connector-apache-spark_2.13:5.3.10_for_spark_3`.

The manifest excludes `pyspark` as a cluster-installed PyPI package, and
excludes runtime-provided packages whose built-in version satisfies the
manifest range unless DBxCarta explicitly requires lock-exact pinning.

### Library Identity Model

The runner needs a small model that normalises comparable identity across
the SDK library source types so desired and current can be compared:

- Wheel: compare by normalised wheel filename prefix, reusing the existing
  `upload._wheel_glob_prefix` logic already used by
  `clean_cluster_wheel_libraries`.
- PyPI: compare by normalised package name, and optionally by an exact
  version when the manifest marks the entry pin-exact.
- Maven: compare by `group:artifact`, version compared only when pinned.
- JAR: compare by the volume or DBFS path.

### Runner Responsibilities

`databricks-job-runner` owns generic reconciliation mechanics, built by
generalizing `clean.py`:

- Read current cluster library status (`ws.libraries.cluster_status`).
- Normalise comparable identity for wheel, PyPI, Maven, and JAR sources.
- Detect missing desired libraries.
- Detect stale project-owned wheel libraries (existing prefix logic).
- Detect libraries pending install, failed install, or pending removal,
  from the SDK install-status enum.
- Install missing desired libraries (`ws.libraries.install`).
- Uninstall stale project-owned libraries only when explicitly requested
  (existing `ws.libraries.uninstall` path).
- Report whether a restart is required.

### DBxCarta Responsibilities

DBxCarta owns project-specific dependency decisions:

- Build the desired manifest for `dbxcarta-spark` ingest.
- Build the desired manifest for `dbxcarta-client` evaluation if needed.
- Classify each dependency as runtime-provided, broad-range acceptable, or
  exact-version required, using `docs/proposals/e2e-setup.md` as the
  authority.
- Suppress task-level wheel attachment once the wheel is already synced as a
  compute-scoped library, to avoid resolving dependencies twice.

For the Finance Genie path, DBxCarta runs an explicit cluster sync before
submit, then avoids re-triggering per-run dependency installation when the
synced cluster is already prepared.

## SDK API Surface

The reconciliation work uses the Databricks SDK libraries client. The
runner already depends on the relevant types.

- `ws.libraries.cluster_status(cluster_id=...)` returns per-library status
  with an install-state enum. Already used in `clean.py`.
- `ws.libraries.uninstall(cluster_id=..., libraries=[Library(...)])`.
  Already used in `clean.py`.
- `ws.libraries.install(cluster_id=..., libraries=[Library(...)])` is the
  one new call.
- `Library` from `databricks.sdk.service.compute` carries one of `whl`,
  `pypi`, `maven`, or `jar`. Phase 1 confirms the exact field types
  (`PythonPyPiLibrary`, `MavenLibrary`) and the install-status enum values
  for installed, pending, failed, and uninstall-pending.

## Proposed CLI Shape

Cluster sync is automatic for DBxCarta classic-cluster wheel paths:

- `submit-entrypoint ingest --compute cluster` syncs required libraries,
  waits for them to become ready, and submits without task-level
  `Library(whl=...)` attachment.
- `submit-entrypoint client --compute cluster` follows the same synced
  classic-cluster path for the client wheel.
- `upload --wheel` builds and uploads the `dbxcarta-spark` wheel, keeps the
  existing stale-wheel cleanup, and then syncs the cluster libraries for
  ingest.

Read-only check can be added as a convenience command later, but the first
implementation keeps the operator flow simple: classic cluster submit and
wheel upload always reconcile the libraries they require.

## Phase Checklist

### Phase 1: Discovery and Contract

Status: Pending

Checklist:

- Confirm the SDK `Library` field types and the `cluster_status` install
  enum values for installed, pending, failed, and uninstall-pending.
- Confirm `ws.libraries.install` accepts the same `Library` list shape as
  the existing `uninstall` call.
- Define the runner-level desired library model and identity normalisation
  for wheel, PyPI, Maven, and JAR.
- Define which library identities are safe to compare exactly versus by
  range, reusing `_wheel_glob_prefix` for wheels.
- Define which stale libraries are project-owned and safe to remove,
  reusing the existing prefix-ownership rule.

Validation:

- A local unit test compares desired and current library statuses with no
  Databricks access, using fixture status objects.
- A dry-run against the Finance Genie cluster reports current libraries
  without changing them.

### Phase 2: Runner Library Reconciliation

Status: Pending

Checklist:

- Generalize `clean.py` status reading into a reusable reconciliation
  function that takes a desired manifest.
- Add missing and stale detection across all four source types.
- Add install of missing libraries via `ws.libraries.install`.
- Keep the existing optional uninstall of stale project-owned wheels.
- Add clear output for restart-required states.
- Keep serverless behavior unchanged; reconciliation is classic-only.

Validation:

- Unit tests cover identity matching for wheel, PyPI, Maven, and JAR.
- Unit tests cover installed, pending, failed, and uninstall-pending
  statuses.
- A live dry-run reports the Finance Genie cluster status without
  mutation.

### Phase 3: DBxCarta Manifest

Status: Pending

Checklist:

- Add DBxCarta-owned manifest generation for `dbxcarta-spark` ingest.
- Include the uploaded wheel path only after build and upload.
- Include the missing Python runtime dependencies from
  `docs/proposals/e2e-setup.md`.
- Exclude `pyspark` from cluster-installed PyPI dependencies.
- Include the verified Neo4j Spark Connector Maven coordinate.
- Add a read-only check path that surfaces missing dependencies before
  job submission.

Validation:

- The generated manifest matches `docs/proposals/e2e-setup.md`.
- Check mode reports PySpark as runtime-provided, not missing.
- Check mode reports the Neo4j Spark Connector coordinate by exact Maven
  version.

### Phase 4: Sync Integration

Status: Pending

Checklist:

- Wire automatic library sync into classic-cluster `submit-entrypoint` and
  `upload --wheel`.
- Ensure sync runs after wheel upload when the manifest needs the uploaded
  wheel path.
- Make output distinguish installed, missing, stale, failed, and
  restart-required libraries.
- Document that `upload --wheel` already uninstalls old project wheels
  without the flag, and that the flag adds the install side.
- Suppress task-level `Library(whl=...)` attachment after successful sync
  for classic wheel entrypoint submits.

Validation:

- Read-only check fails when a required dependency is missing.
- Sync installs the missing non-runtime dependencies on a test cluster.
- A subsequent check passes or reports only an expected restart state.
- `submit-entrypoint ingest --compute cluster` no longer fails during
  Databricks library installation for missing Python dependencies.

### Phase 5: Documentation and Operator Flow

Status: Pending

Checklist:

- Document when to use check versus sync.
- Document the restart behavior after uninstalling old libraries.
- Document how a no-egress cluster should use uploaded wheels or a
  wheelhouse.
- Document that the Spark 4 Neo4j connector coordinate is verified for this
  target cluster even though public compatibility docs do not list Spark 4.

Validation:

- A new operator can run check mode and understand every reported action.
- `docs/proposals/e2e-setup.md` and this proposal agree on dependency
  ownership.

## Risks

- Cluster library mutation can affect other users on shared clusters.
  Sync is opt-in and classic-only to limit blast radius, but the operator
  is responsible for not pointing it at a shared cluster.
- Uninstalling stale libraries may not take effect until restart, which
  can surprise an operator who expects an immediate clean state.
- Installing a newer package than the runtime provides can create runtime
  conflicts, which is why runtime-provided packages stay out of the
  manifest unless lock-exact pinning is explicitly required.
- The approved Neo4j Spark Connector coordinate is based on target-cluster
  verification rather than a public Spark 4 compatibility listing.
- Task-level wheel attachment and compute-scoped wheel installation can
  resolve dependencies twice if Phase 4 does not coordinate them.

## Open Decisions

- Should the read-only check remain a future convenience command, or should
  it be exposed immediately alongside automatic sync?

## Completion Criteria

- DBxCarta can report missing cluster libraries before submitting an
  ingest job.
- DBxCarta can optionally install the missing cluster libraries through
  `databricks-job-runner`.
- The sync path never forces a PyPI download of `pyspark`.
- The operator can see whether a cluster restart is required.
- Finance Genie E2E no longer fails at job startup due to avoidable Python
  dependency resolution.
