# Releasing dbxcarta

How versions are bumped and how distributions reach PyPI. Two GitHub Actions workflows
do the work, chained by a git tag. Versions are never edited by hand.

## Published distributions

Two packages publish to PyPI:

- **dbxcarta-core**: Spark-free foundation. `dbxcarta-client` depends on it.
- **dbxcarta-client**: retrieval runtime and Text2SQL evaluation harness.

`client` declares a dependency on `dbxcarta-core`, and a normal `uv build` of it
carries only its own module, not a bundled core. Core must therefore ship to PyPI for a
clean `pip install dbxcarta-client` to resolve.

The Unity Catalog ingest pipeline is no longer published from dbxcarta: it moved to
[neocarta](https://github.com/neo4j-field/neocarta) and is published as the `neocarta`
connector wheel from that project, on neocarta's own release schedule. dbxcarta pulls it.

Not published: `dbxcarta-submit` and `dbxcarta-materialize` (operator-local), and the
`dense-schema`, `finance-genie`, and `schemapile` example packages (workspace members for
local dev and CI only).

> **Workflow status.** The `release.yaml` and `publish.yaml` workflows still name the
> removed `dbxcarta-spark` package; updating them is deferred until neocarta publishes its
> wheel (the alignment plan's Phase 4). The package set below is the target this doc
> describes, not yet the literal workflow contents.

## The two workflows

| Workflow | File | Trigger | Job |
|----------|------|---------|-----|
| Release | `.github/workflows/release.yaml` | Manual (`workflow_dispatch`) | Bump every package version in lockstep, commit, create a git tag |
| Publish | `.github/workflows/publish.yaml` | Push of any tag (`tags: "*"`) | Build the published distributions and publish to PyPI via trusted publishing |

## How versions get updated

```
  You: Actions ▸ Release ▸ Run workflow ▸ choose patch | minor | major
        │
        ▼
  release.yaml
    uv version --bump <type>                      # root workspace, e.g. 1.1.0 → 1.1.1
    version=$(uv version --short)
    uv version --package dbxcarta-core    "$version"
    uv version --package dbxcarta-client  "$version"
    uv version --package <three example packages> "$version"
    uv lock ; uv lock --check
    commit pyproject.toml(s) + uv.lock
    create git tag "<version>"                     # e.g. 1.1.1
        │
        ▼  (tag push)
  publish.yaml
    uv build --package dbxcarta-core
    uv build --package dbxcarta-client
    attest provenance → upload artifact → publish to PyPI (OIDC trusted publishing)
```

Every package is set to the same version on each release, so core and client always share
a version number. To cut a release, trigger the **Release** workflow and pick the bump
type. The commit it makes is tagged, and the tag push fires **Publish**.

The Release workflow needs the `GIT_PUSH_PAT` repository secret to push the bump commit
and tag back to the default branch.

## One-time setup: first-time publishing (both projects)

None of the dbxcarta packages exist on PyPI yet, so both are first-time publishes.
`publish.yaml` authenticates through PyPI trusted publishing with no API token. Trusted
publishing cannot create a brand-new project on its own, so each project name needs a
**pending publisher** registered on PyPI before the first release. Register both,
not just core, or the first run fails for the missing one.

On PyPI, under **Your account ▸ Publishing ▸ Add a pending publisher**, create one entry
for each of `dbxcarta-core` and `dbxcarta-client`, both with the same values:

- PyPI project name: `dbxcarta-core` / `dbxcarta-client` (one per entry)
- Owner / repository: `neo4j-partners/dbxcarta`
- Workflow filename: `publish.yaml`
- Environment name: `pypi`

Two more prerequisites on the GitHub side:

- The `pypi` deployment environment must exist in the repository settings, since the
  Publish job pins `environment: name: pypi`.
- Confirm both names are still available on PyPI and not taken by another project.

## Cutting the first release

Git tags `1.0.0` and `1.1.0` already exist but were never published, because the Publish
workflow only runs on a tag push and those tags predate it. Rather than delete and
re-push an old tag, cut a fresh version through the **Release** workflow (for example a
`patch` bump to `1.1.1`). That creates a new tag, which triggers Publish and ships both
packages at the same version.

## Consumer pinning note

Until the first successful publish, nothing dbxcarta is installable from PyPI. Any
external consumer that pins dbxcarta versions must pin to the **first version that
actually publishes** (for example `1.1.1` if that is the first release cut), not to
`1.1.0`, which has no artifacts on PyPI.

## Notes

- The deployment URL on the Publish job (`url: https://pypi.org/p/dbxcarta`) is a cosmetic
  link for the GitHub environment view. No project is named `dbxcarta`; the real projects
  are the two listed above. It has no effect on what gets published.
- A single `gh-action-pypi-publish` step uploads everything in `dist/`; each file is routed
  to its own PyPI project by package metadata.
