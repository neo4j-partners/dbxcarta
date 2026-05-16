# Clean Up the Supply-Chain Posture Across dbxcarta and databricks-job-runner

## Summary

dbxcarta already has strong build-time supply-chain controls in CI: a
locked, frozen test and build workflow, a vulnerability scan, an SBOM,
artifact inspection and provenance evidence, SHA-pinned GitHub Actions,
a split build-and-publish flow with a scoped publish token, and a
Renovate quarantine on PyPI updates. Its remaining exposure is at
runtime, not in CI. databricks-job-runner is in the opposite state. It
publishes to PyPI under a shared org, yet it has no locked CI, no
version quarantine, no vulnerability scan, actions pinned to moving
refs, and a single job that builds and publishes while holding the
PyPI publish token.

This proposal fixes dbxcarta first and databricks-job-runner second. The
dbxcarta work closes the runtime dependency-resolution gap that the
supply-chain note already flagged and that the in-flight harden-deploy
proposal explicitly deferred. The databricks-job-runner work applies a
deliberately calibrated subset of the dbxcarta CI controls, sized for a
two-dependency CLI rather than copied wholesale.

This document is supply-chain only. It does not restate or replace
`docs/proposals/harden-deploy.md`. Where the two touch the same code,
this proposal consumes harden-deploy outputs and does not re-decide
them.

## Problem

dbxcarta build-time controls stop a fresh malicious publish from
entering a build without a reviewed lockfile change. They do not control
what the dbxcarta wheel resolves at run time on the Databricks cluster.
The uploaded wheel can still cause the runtime to resolve its dependency
closure from public PyPI. The harden-deploy proposal narrows this with a
hand-curated pinned closure installed with no resolution, and it states
in its own deviations that a generated wheelhouse from the reviewed
lockfile is the correct long-term supply-chain answer and is out of
scope there. That deferred item is the open dbxcarta risk this proposal
picks up. A second, smaller dbxcarta gap is that the convenience upload
path bumps the version, builds, and uploads in one step, so a runtime
artifact is not traceable to reviewed CI build evidence.

databricks-job-runner is a published artifact other projects depend on,
including dbxcarta. Its only workflow builds and publishes on a tag with
no lockfile enforcement, no quarantine on its own dependency updates, no
vulnerability scan, and third-party Actions pinned to floating tags and
a moving release branch. The build step and the publish step share one
job that holds the PyPI OIDC token, so build-backend and dependency code
executes while the token that can publish to the shared org is present.
A compromise of any of those inputs can publish a malicious release that
flows directly into every consumer.

## Proposed Solution

For dbxcarta, generate a reviewed dependency wheelhouse on a Unity
Catalog Volume from the existing locked, reviewed dependency set, and
have the runtime install the project wheel and its closure from that
wheelhouse with resolution disabled, so a run never reaches public PyPI.
Make the reviewed CI-built artifact, not the convenience upload, the
thing a release run consumes, identified by an explicit version.

For databricks-job-runner, add a locked CI workflow that fails on
lockfile drift, syncs frozen, and runs a vulnerability scan; pin every
Action by commit SHA; split the single job so the publish token is
present only during publish; add built-in build-provenance attestation;
and add a Renovate quarantine on its own PyPI updates. Deliberately
exclude the SBOM and the custom artifact-audit script, because their
value does not justify their maintenance cost on a two-dependency
package.

The expected outcome is that neither project can take a fresh upstream
publish into a build or a run without a reviewed change, and that the
shared published artifact cannot be republished from a compromised
build input without first defeating multiple independent controls.

## Core Decisions

These are the load-bearing decisions. Each records why, so the per-phase
review gates can confirm intent rather than re-litigate it.

- **Fix dbxcarta first, then databricks-job-runner.** dbxcarta carries
  the higher residual risk right now, a runtime path that can still
  reach public PyPI, while its CI is already hardened. databricks-job-runner
  is a clean, self-contained, lower-blast-radius change that does not
  depend on the dbxcarta work. Doing dbxcarta first also resolves the
  exact item harden-deploy deferred, which keeps that in-flight work
  coherent.

- **The dbxcarta runtime closure comes from the reviewed lockfile, not
  hand curation.** harden-deploy installs a hand-curated pinned closure
  to fix a reliability problem. The supply-chain requirement is
  provenance: every runtime dependency must trace to the same reviewed,
  locked set CI already validates. A generated wheelhouse from the lock
  is the mechanism. This supersedes hand curation as the closure source
  and does not change harden-deploy's install model.

- **The wheelhouse is offline at run time.** The runtime install must
  be unable to reach a public index even on a cache miss. An install
  that can fall back to PyPI is not a control. The benefit only exists
  if the fallback path does not exist.

- **A release run consumes the reviewed CI artifact by explicit
  version.** The convenience upload that bumps, builds, and uploads in
  one step stays available for the inner development loop. It is not the
  path a reviewed release run uses. This keeps a runtime artifact
  traceable to CI build evidence without removing the fast local loop.

- **databricks-job-runner gets a calibrated subset, not a copy.** The
  dbxcarta controls are sized for an eight-layer workspace with a large
  transitive tree. databricks-job-runner has two direct dependencies.
  Lockfile enforcement, SHA pinning, token scoping, attestation, vuln
  scan, and quarantine each reduce real risk for a publisher. An SBOM
  for a two-dependency tree and a second copy of the custom
  artifact-audit script add maintenance in two repos for marginal
  benefit. Built-in attestation replaces the custom provenance script
  for near-zero maintenance.

- **Lockfile enforcement is the single highest-value job-runner
  control.** Without it, floating lower bounds plus no quarantine means
  a fresh malicious dependency publish can enter a release build with no
  source change. Everything else is defense in depth on top of this.

- **The publish token is present only during publish.** Splitting build
  from publish so the OIDC token does not exist while build-backend and
  dependency code runs is the control that most directly limits what a
  build-time compromise can do with the shared-org publish capability.

- **This proposal does not modify harden-deploy scope.** Where the two
  meet at the runtime install, this proposal supplies the wheelhouse the
  harden-deploy bootstrap installs from and changes nothing else in that
  plan.

## Requirements

dbxcarta:

- A reviewed dependency wheelhouse is generated from the existing
  locked, reviewed dependency set, not from ad hoc resolution.
- The wheelhouse contents match the locked set exactly, with no extra or
  newer artifacts than the lock allows.
- The runtime installs the project wheel and its full closure from the
  wheelhouse with dependency resolution disabled.
- The runtime install cannot reach a public package index, including on
  a cache miss, and fails closed rather than falling back.
- A reviewed release run consumes the CI-built artifact identified by an
  explicit version, not the one-step convenience upload.
- The convenience upload remains available for the development loop and
  is unchanged by this proposal.
- No change is made to harden-deploy's install model beyond replacing
  the closure source with the wheelhouse.

databricks-job-runner:

- CI fails when the lockfile and manifest disagree.
- CI installs frozen, with no dependency resolution change during
  validation, and runs the existing tests against that frozen set.
- CI runs a vulnerability scan over the locked dependency set.
- Every third-party GitHub Action is pinned by full commit SHA. No
  floating tag or moving branch ref remains in any workflow.
- The build step and the publish step are separate jobs. The publish
  token is requested only by the publish job.
- The build job produces a signed build-provenance attestation using
  the built-in attestation action.
- A Renovate configuration applies a minimum release age to this repo's
  own PyPI updates.
- No SBOM artifact and no copied custom artifact-audit script are added
  to this repo.

## Phased Plan

Status legend used in the phase titles: `not started`,
`implemented, review gate pending`, `done`. A phase is only `done` once
its review gate is signed off. Phases 1 through 3 are dbxcarta. Phases 4
through 6 are databricks-job-runner and have no dependency on the
dbxcarta phases, so they may proceed in parallel if desired, but the
default order is dbxcarta first per the core decision.

Each implementation phase ends with an explicit review gate. Do not
start the next phase until the review gate passes.

### Phase 1: Reviewed wheelhouse generated from the lock [Status: not started]

Goal: a dependency wheelhouse exists on a Unity Catalog Volume whose
contents match the existing reviewed lockfile exactly.

Checklist:
- [ ] Define the wheelhouse generation step that reads the existing
      locked, reviewed dependency set and materializes the full closure,
      including transitive dependencies and platform-correct binary
      wheels for the cluster's Python version.
- [ ] Verify the materialized contents match the lock exactly, with no
      artifact newer or broader than the lock permits, and fail
      generation otherwise.
- [ ] Publish the wheelhouse to a stable Unity Catalog Volume location
      and record the lock identity it was generated from.
- [ ] Produce a short manifest mapping the wheelhouse to the reviewed
      lock so a later run is auditable.

Validation:
- [ ] The wheelhouse closure equals the locked closure with no
      additions.
- [ ] Regenerating from an unchanged lock produces an equivalent
      wheelhouse.
- [ ] A lock change is required to change the wheelhouse contents.

Review gate:
- [ ] Confirm the wheelhouse derives only from the reviewed lock.
- [ ] Confirm the manifest ties the wheelhouse to a specific lock
      identity.
- [ ] Confirm the plan still matches the goal and record any deviation.

### Phase 2: Offline runtime install from the wheelhouse [Status: not started]

Goal: the runtime installs the project wheel and its full closure from
the wheelhouse with resolution disabled and no public-index reachability.

Checklist:
- [ ] Point the harden-deploy bootstrap install at the wheelhouse as the
      closure source, replacing the hand-curated closure source and
      changing nothing else in the harden-deploy install model.
- [ ] Install the closure with dependency resolution disabled so no
      version selection happens at run time.
- [ ] Ensure the install path cannot reach a public package index, so a
      missing artifact fails closed with an actionable message rather
      than resolving from PyPI.
- [ ] Confirm platform and Python-version compatibility of the
      wheelhouse against the actual cluster before the run proceeds.

Validation:
- [ ] A run installs entirely from the wheelhouse with no network call
      to a public index.
- [ ] An intentionally missing wheelhouse artifact fails the run closed
      with a clear message and no PyPI fallback.
- [ ] The console entry point and the connector path behave exactly as
      under the harden-deploy install model.

Review gate:
- [ ] Confirm no public-index fallback path remains.
- [ ] Confirm harden-deploy's install model is otherwise unchanged.
- [ ] Confirm the plan still matches the goal and record any deviation.

### Phase 3: Release runs consume the reviewed CI artifact [Status: not started]

Goal: a reviewed release run uses the CI-built artifact by explicit
version, while the convenience upload stays available for the local
loop.

Checklist:
- [ ] Define a release-run path that consumes the CI-built artifact
      identified by an explicit version rather than the one-step
      convenience upload.
- [ ] Keep the convenience upload available and unchanged for the
      development loop.
- [ ] Record, per release run, the artifact version and the lock and
      wheelhouse identities it used.

Validation:
- [ ] A release run is traceable end to end from reviewed CI build
      evidence to the artifact it executed.
- [ ] The development-loop upload still works unchanged.

Review gate:
- [ ] Confirm the release path consumes reviewed CI evidence by explicit
      version.
- [ ] Confirm the development loop is not degraded.
- [ ] Confirm the plan still matches the goal and record any deviation.

### Phase 4: Locked CI for databricks-job-runner [Status: implemented, review gate pending]

Goal: databricks-job-runner has a CI workflow that fails on lockfile
drift, syncs frozen, runs the existing tests against that frozen set,
and runs a vulnerability scan.

Checklist:
- [x] Add a CI workflow that fails when the lockfile and manifest
      disagree. Added `.github/workflows/ci.yml` with `uv lock --check`.
- [x] Sync frozen so dependency resolution cannot change during
      validation, and run the existing test suite against that set.
      `uv sync --frozen` then `uv run --frozen --with pytest pytest`.
      pytest is brought in ephemerally so the published manifest and
      lockfile are not modified, per the calibrated-subset decision.
- [x] Run a vulnerability scan over the locked dependency set. pip-audit
      over `uv export --frozen --no-emit-project`.
- [x] Set minimal workflow permissions and pin every Action in this new
      workflow by full commit SHA. `permissions: contents: read`; all
      Actions pinned to the SHAs already vetted in dbxcarta.

Validation:
- [x] CI installs the same versions every run with no resolution change.
      `uv lock --check`, `uv sync --frozen`, and the frozen test run all
      pass locally; 40 tests pass.
- [ ] A deliberate lockfile or manifest mismatch fails CI. Verified by
      construction (`uv lock --check`); confirm on the first PR run.
- [ ] A known-vulnerable pin is reported by the scan. pip-audit recipe
      validated to produce a clean editable-free requirements file;
      end-to-end pip-audit execution deferred to the first CI run
      because a local macOS `ensurepip` crash blocks running it here.

Deviation recorded: the proposal anticipated mirroring dbxcarta's
`--all-packages` plus `grep -v '^-e '` editable filter. dbxcarta had to
iterate on that filter repeatedly. For a two-package workspace,
`uv export --no-emit-project` produces a clean closure with no editable
lines and no grep, which is simpler and removes that failure class.
This is a faithful tightening of the requirement, not a scope change.

Review gate:
- [ ] Confirm lockfile drift fails fast.
- [ ] Confirm the frozen set is what the tests run against.
- [ ] Confirm the plan still matches the goal and record any deviation.

### Phase 5: Harden the databricks-job-runner publish workflow [Status: implemented, review gate pending]

Goal: the publish workflow pins every Action by SHA and holds the PyPI
token only during publish.

Checklist:
- [x] Replace every floating tag and moving branch ref in the publish
      workflow with a full commit SHA pin. `actions/checkout`,
      `astral-sh/setup-uv`, `actions/setup-python`,
      `actions/attest-build-provenance`, `actions/upload-artifact`,
      `actions/download-artifact`, and `pypa/gh-action-pypi-publish` are
      now SHA-pinned to the SHAs already vetted in dbxcarta.
- [x] Split the single job into a build job that does not request the
      publish token and a separate publish job that does. `build` and
      `publish-to-pypi` jobs; `publish-to-pypi` needs `build`.
- [x] Add a signed build-provenance attestation in the build job using
      the built-in attestation action. Attests both the wheel and the
      sdist, since both are published to PyPI. This is a deliberate
      improvement over dbxcarta's pattern, which attests only the wheel.
- [x] Keep the publish job behind the existing protected environment.
      `publish-to-pypi` keeps `environment: pypi`.

Validation:
- [x] No floating tag or moving branch ref remains in any workflow.
      Both workflows parse as valid YAML and every `uses:` is a 40-char
      SHA.
- [x] The PyPI publish capability is absent during build and present
      only during publish. The `pypi` protected environment and the
      PyPI trusted-publishing step exist only in `publish-to-pypi`.
- [ ] A published build carries a verifiable provenance attestation.
      Confirm on the first real tag publish.

Deviation recorded: "publish token only during publish" is read as the
PyPI publish capability, not literally all `id-token: write`. The build
job holds `id-token: write` strictly because
`actions/attest-build-provenance` requires it to sign the attestation.
That OIDC token cannot publish to PyPI: PyPI trusted publishing and the
protected `pypi` environment exist only in the separate
`publish-to-pypi` job. This mirrors dbxcarta's vetted `publish.yaml`
exactly. Without this, the Phase 5 checklist requirement to add
attestation would be unsatisfiable.

Review gate:
- [ ] Confirm the PyPI publish capability is isolated to the publish
      job and the build job's `id-token` is attestation-only.
- [ ] Confirm no custom artifact-audit script and no SBOM were added.
- [ ] Confirm the plan still matches the goal and record any deviation.

### Phase 6: Version quarantine for databricks-job-runner [Status: implemented, review gate pending]

Goal: this repo's own PyPI dependency updates are delayed by a minimum
release age.

Checklist:
- [x] Add a Renovate configuration that applies a minimum release age to
      this repo's PyPI updates, consistent with the dbxcarta quarantine
      window. `renovate.json` with `minimumReleaseAge: 3 days` and
      `internalChecksFilter: strict`, matching dbxcarta.
- [x] Label dependency updates so a fresh publish is reviewable rather
      than auto-consumed. `dependencies` and `supply-chain` labels;
      `security-sensitive` on `uv-build`, `databricks-sdk`, `pydantic`.

Validation:
- [x] Configuration is valid JSON and uses the same quarantine window
      and strict internal-checks filter as dbxcarta.
- [ ] A simulated fresh dependency publish is held until the age window
      passes. Confirm on the first Renovate run once the app is enabled
      on the repo.
- [ ] A security patch can still be taken with an explicit reviewed
      override.

Final review gate:
- [ ] Confirm the quarantine window matches the agreed policy.
- [ ] Confirm both projects meet their requirements and close the plan
      out.

## Post-Implementation Remediation (databricks-job-runner)

Verification after Phases 4 to 6 surfaced live issues that were fixed:

- The first hardened publish already shipped: tag `v0.5` published to
  PyPI through the split, SHA-pinned, attested workflow. Phase 5 is
  proven end to end in production, not just by construction.
- CI was red because the locked closure carried known CVEs. Fixed by
  bumping `urllib3` 2.6.3 to 2.7.0 (CVE-2026-44431, CVE-2026-44432) and
  `cryptography` 46.0.6 to 48.0.0 (CVE-2026-39892) in `uv.lock`. CI is
  green after the fix.
- `publish.yml` had no supply-chain gate, so `v0.5` shipped on red CI.
  Added `uv lock --check` and `pip-audit` to the publish build job so a
  tagged release cannot ship with lockfile drift or a known-vulnerable
  closure.
- Branch protection added on `main`: one required review, required and
  strict `Locked tests and audit` status check, no force-push, no
  deletion.
- The `pypi` environment had `protection_rules: []`. Deployment is now
  restricted to a `v*` tag policy with `retroryan` as the required
  reviewer.
- `v0.5.1` was released and passed through the complete hardened path:
  lockfile check, pip-audit, build, wheel and sdist attestation, then a
  reviewer-gated publish to PyPI. Every added control is now proven by a
  real release, not only by construction. `v0.5.1` supersedes the
  earlier `v0.5` that shipped on red CI.

## Out of Scope

- Any change to harden-deploy scope or its install model beyond
  supplying the wheelhouse as the closure source.
- An SBOM for databricks-job-runner.
- Copying the custom artifact-audit script into databricks-job-runner.
- Registry proxy or policy-firewall infrastructure. The note lists these
  as options, not commitments, and they are not required to meet these
  requirements.
- dbxcarta credential-policy changes. Those controls already exist and
  are not regressed by this work.

## Completion Criteria

- A dbxcarta run installs its entire dependency closure from a
  wheelhouse generated from the reviewed lock, with no reachable public
  index and no fallback.
- A reviewed dbxcarta release run is traceable from CI build evidence to
  the exact artifact it executed, while the development-loop upload is
  unchanged.
- databricks-job-runner CI fails on lockfile drift, runs tests against a
  frozen set, and runs a vulnerability scan.
- Every GitHub Action across databricks-job-runner workflows is pinned
  by commit SHA, and the PyPI token is present only during publish, with
  a verifiable build-provenance attestation.
- databricks-job-runner quarantines its own PyPI updates by a minimum
  release age.
- No SBOM and no copied artifact-audit script were added to
  databricks-job-runner, and harden-deploy scope is unchanged.
