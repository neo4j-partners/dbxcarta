# DBxCarta Supply Chain Hardening Plan

## Goal

Secure this Python and Databricks project against fresh malicious dependency
publishes, accidental non-reproducible builds, unreviewed wheel uploads, and
unnecessary credential exposure during local, CI, and Databricks job execution.

## Assumptions

- `uv.lock` is the authoritative dependency lockfile.
- CI should install with frozen resolution and fail when the lockfile is stale.
- DBxCarta jobs should run from reviewed wheel artifacts, not whichever wheel
  happens to be newest in `dist/`.
- The default dependency quarantine window is 3 days.
- npm-specific controls in `SECURITY_SUPPLY_CHAIN.md` apply only if this repo
  later adds JavaScript package management.

## Risks

- Databricks job startup can still resolve package dependencies from public
  indexes unless a locked runtime artifact set is introduced.
- The current wheel upload flow bumps versions and uploads artifacts in one
  operation, which makes review and rollback harder.
- `.env` extras are forwarded as job parameters unless they are explicitly
  treated as secrets.
- CI workflow changes need GitHub repository settings to enforce branch
  protection and required checks.

## Phase checklist

### Phase 1: Baseline CI and dependency update policy

Status: Complete

- [x] Add a CI workflow that runs frozen `uv` lock validation, frozen test sync,
      unit tests, package build, and package artifact inspection.
- [x] Add dependency update configuration with a 3-day release-age quarantine
      for PyPI updates.
- [x] Keep security tooling dependency-light so the checks can run before any
      optional scanner is added.

Validation:

- Self-check complete; full command validation deferred to Phase 4.

Notes:

- Added `.github/workflows/supply-chain.yml` and `renovate.json`.
- CI sets up Java 17 explicitly because the unit suite uses local PySpark.
- CI removes stale ignored `dist/` contents before building so artifact evidence
  covers only the current build.
- The workflow uses GitHub Actions by version tag for now; pinning by full SHA
  remains a repository policy follow-up for sensitive release workflows.

### Phase 2: Artifact inspection and provenance tooling

Status: Complete

- [x] Add a local security script that rejects wheel and source artifacts
      containing secrets, local caches, virtual environments, run outputs, or
      development-only directories.
- [x] Add artifact provenance output that records source commit, lockfile hash,
      artifact hash, package version, and build metadata.
- [x] Make the tooling usable both locally and from CI.

Validation:

- `PYTHONDONTWRITEBYTECODE=1 python -m py_compile scripts/security/artifact_audit.py`
  passed.
- `uv run python scripts/security/artifact_audit.py inspect /private/tmp/dbxcarta-sec-review-dist`
  passed for the fresh wheel and source distribution.
- Fresh artifact provenance generation passed with output written under
  `/private/tmp/dbxcarta-sec-review-dist`.

Notes:

- Provenance is audit evidence, not a guarantee that the artifact is safe.
- Added `scripts/security/artifact_audit.py`.

### Phase 3: Databricks artifact policy documentation

Status: Complete

- [x] Document the secure local build and upload flow in the README.
- [x] Update `SECURITY_SUPPLY_CHAIN.md` with Python, `uv`, wheel, and Databricks
      controls specific to this repo.
- [x] Call out the remaining Databricks runtime dependency risk and the
      recommended follow-up of a reviewed UC Volume wheelhouse.

Validation:

- Documentation reviewed in-place; full validation deferred to Phase 4.

Notes:

- This phase is documentation only; it does not change job submission behavior.
- `upload --wheel` remains convenient but not fully release-controlled because
  it still combines version bump, build, and upload.

### Phase 4: Final validation and handoff

Status: Complete

- [x] Run the lockfile, sync, test, build, artifact inspection, and provenance
      checks.
- [x] Review the resulting git diff for unrelated churn.
- [x] Summarize completed work and deferred follow-ups.

Validation:

- `uv lock --check` passed.
- `uv sync --frozen --extra test` passed.
- `uv run pytest` passed: 171 passed, 1 skipped, 3 deselected.
- `uv build --sdist --wheel` passed.
- `uv run python scripts/security/artifact_audit.py inspect dist` passed for
  37 local artifacts before the workflow was tightened to clean `dist/` first.
- `uv run python scripts/security/artifact_audit.py provenance dist --output
  dist/supply-chain-provenance.json` passed.
- `uv build --sdist --wheel --out-dir /private/tmp/dbxcarta-sec-review-dist`
  passed for a clean review build.
- `uv run python scripts/security/artifact_audit.py inspect /private/tmp/dbxcarta-sec-review-dist`
  passed for 2 fresh artifacts.
- `python -m json.tool /private/tmp/dbxcarta-sec-review-dist/supply-chain-provenance.json`
  passed.
- `python -m json.tool renovate.json` passed.
- `python -m json.tool dist/supply-chain-provenance.json` passed.

Notes:

- Live Databricks smoke testing is out of scope unless credentials and runtime
  targets are explicitly requested.
- The local `dist/` directory already contained older ignored artifacts, so the
  first local provenance covered 37 artifacts. The workflow and README now clean
  `dist/` before building so routine evidence covers only fresh artifacts.
- `uv build` still prints the existing warning that `uv-build>=0.9.3,<0.10.0`
  does not contain local `uv` 0.11.12; build output is successful, but the build
  backend pin should be reviewed separately.

## Completion criteria

- CI has an enforceable frozen-install and artifact-inspection path.
- Dependency updates are delayed by a clear quarantine policy.
- Built package artifacts can be inspected and accompanied by a provenance
  manifest.
- Project docs describe the Python and Databricks supply-chain policy clearly.
- Remaining risks are explicitly documented rather than hidden.
