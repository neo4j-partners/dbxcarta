# Publish Fix Plan

Three independent problems block consumer deployment of `sql-semantics`. Each
has a clear fix. They can be addressed in order since Problem 2 should land
before Problem 1 (the public API is part of what gets published).

---

## Problem 1: `dbxcarta` is not on PyPI

### Root cause

`sql-semantics/pyproject.toml` resolves `dbxcarta` via a `[tool.uv.sources]`
block pointing to an absolute local path:

```toml
[tool.uv.sources]
dbxcarta = { path = "/Users/ryanknight/projects/databricks/dbxcarta", editable = true }
```

This path does not exist on Databricks. Any environment that installs
`sql-semantics` from a wheel will fail to resolve its `dbxcarta` dependency.

### Fix: mirror the neo4j-graphrag-python publishing pattern

The neo4j-graphrag-python project uses OIDC trusted publishing (no API tokens)
with a two-workflow pattern: a version-bump workflow triggers a git tag, and the
tag triggers a publish workflow. dbxcarta should follow the same pattern using
`uv build` in place of `python3 -m build`.

**Step A: One-time PyPI setup**

1. Create the `dbxcarta` project on PyPI (first publish must be manual to
   register the project name).
2. Register a trusted publisher on PyPI under the project settings:
   - Owner: `<github-org-or-user>`
   - Repository: `dbxcarta`
   - Workflow: `publish.yaml`
   - Environment: `pypi`
3. Add a `pypi` environment in the GitHub repo settings (Settings > Environments).
4. Add a `GIT_PUSH_PAT` repository secret: a GitHub personal access token with
   `contents: write` permission. The version-bump workflows push a commit and
   tag; the default `GITHUB_TOKEN` cannot trigger downstream workflow runs, so
   a PAT is required (same approach as neo4j-graphrag).

**Step B: Add `.github/workflows/publish.yaml`**

Builds the wheel on every push. Publishes to PyPI only when a version tag is
pushed. Uses OIDC; no `PYPI_API_TOKEN` secret needed.

```yaml
name: Publish Python distribution

on: push

jobs:
  build:
    name: Build distribution
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Install uv
        uses: astral-sh/setup-uv@v5
        with:
          enable-cache: true

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version-file: .python-version

      - name: Build wheel and sdist
        run: uv build --sdist --wheel

      - name: Store distribution packages
        uses: actions/upload-artifact@v4
        with:
          name: python-package-distributions
          path: dist/

  publish-to-pypi:
    name: PyPI
    if: startsWith(github.ref, 'refs/tags/')
    needs: build
    runs-on: ubuntu-latest
    environment:
      name: pypi
      url: https://pypi.org/p/dbxcarta
    permissions:
      id-token: write
    steps:
      - name: Download distributions
        uses: actions/download-artifact@v4
        with:
          name: python-package-distributions
          path: dist/

      - name: Publish to PyPI
        uses: pypa/gh-action-pypi-publish@release/v1
```

**Step C: Add version-bump workflows**

Add three workflows following the neo4j pattern exactly, substituting
`patch`, `minor`, and `major` for the bump type. Example for patch:

`.github/workflows/patch-release.yaml`:

```yaml
name: Publish a new patch release

on:
  workflow_dispatch:

jobs:
  bump-version:
    outputs:
      version: ${{ steps.get-version.outputs.version }}
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
        with:
          token: ${{ secrets.GIT_PUSH_PAT }}

      - name: Install uv
        uses: astral-sh/setup-uv@v5

      - name: Bump version
        run: uv version --bump patch --frozen

      - name: Get version
        id: get-version
        run: echo version=`uv version --short` >> "$GITHUB_OUTPUT"

      - uses: EndBug/add-and-commit@v9
        with:
          author_name: dbxcarta GitHub Action
          author_email: noreply@github.com
          message: Bump version to ${{ steps.get-version.outputs.version }}
          add: "['pyproject.toml']"
          tag: ${{ steps.get-version.outputs.version }}
```

`minor-release.yaml` and `major-release.yaml` are identical with `--bump minor`
and `--bump major` respectively.

**Step D: First manual publish (registers the project name on PyPI)**

Before trusted publishing works, PyPI needs to know the project exists:

```bash
uv build --sdist --wheel
uv publish --token $PYPI_API_TOKEN
```

After the project is registered, subsequent releases use the GitHub Actions
workflows and no token is needed locally.

**Step E: Update `sql-semantics/pyproject.toml`**

Remove the `[tool.uv.sources]` section entirely. The dependency declaration
is already correct:

```toml
dependencies = [
    "dbxcarta>=0.2.38",   # update floor to published version
    ...
]
```

Run `uv sync` to confirm it resolves from PyPI.

---

## Problem 2: `local_demo.py` imports private `_`-prefixed functions

### Root cause

`sql-semantics/src/sql_semantics/local_demo.py` imports four private symbols
from `dbxcarta.client.client`:

```python
from dbxcarta.client.client import (
    _compare_result_sets,
    _embed_questions,
    _load_questions,
    _parse_sql,
)
```

Two of the four have already been extracted to public modules:

| Symbol | Public location | Status |
|--------|----------------|--------|
| `_compare_result_sets` | `dbxcarta.client.compare.compare_result_sets` | Extracted, alias only in `client.py` |
| `_load_questions` | `dbxcarta.client.questions.load_questions` | Extracted, alias only in `client.py` |
| `_parse_sql` | `dbxcarta.client.client` | Still private, needs extraction |
| `_embed_questions` | `dbxcarta.client.client` | Still private, needs extraction |

### Fix

**Step A: Extract `_parse_sql` to `dbxcarta/client/sql.py`**

Move the function body to a new module as `parse_sql`. Update `client.py` to
import it under the existing `_parse_sql` alias so internal call sites are
unchanged.

```python
# dbxcarta/client/sql.py
from __future__ import annotations
import re

_FENCE_RE = re.compile(r"```(?:sql)?\s*(.*?)```", re.DOTALL | re.IGNORECASE)
_SQL_START_RE = re.compile(r"^\s*(SELECT|WITH|INSERT|UPDATE|DELETE|CREATE|DROP|ALTER|EXPLAIN)\b", re.IGNORECASE)

def parse_sql(text: str | None) -> tuple[str | None, bool]:
    """Strip markdown fences and check for a SQL keyword. Returns (cleaned, is_valid)."""
    ...
```

**Step B: Extract `_embed_questions` to `dbxcarta/client/embed.py`**

Move the function body to a new module as `embed_questions`. Update `client.py`
to import it under the `_embed_questions` alias.

```python
# dbxcarta/client/embed.py
def embed_questions(
    ws: WorkspaceClient,
    endpoint: str,
    questions: list[str],
) -> tuple[list[list[float]] | None, str | None]:
    ...
```

**Step C: Export from `dbxcarta/client/__init__.py`**

```python
from dbxcarta.client.compare import compare_result_sets
from dbxcarta.client.embed import embed_questions
from dbxcarta.client.questions import load_questions, Question
from dbxcarta.client.sql import parse_sql
from dbxcarta.client.client import run_client

__all__ = [
    "compare_result_sets",
    "embed_questions",
    "load_questions",
    "parse_sql",
    "run_client",
    "Question",
]
```

**Step D: Update `sql-semantics/local_demo.py`**

Replace the four private imports with the public surface:

```python
from dbxcarta.client import (
    compare_result_sets,
    embed_questions,
    load_questions,
    parse_sql,
)
```

Update every call site in `local_demo.py` accordingly.

---

## Problem 3: No Databricks Jobs deployment path for consumers

### Root cause

`dbxcarta submit-entrypoint ingest/client` submits jobs programmatically from a
local CLI. This works for dbxcarta's own development loop, but a consumer
package like `sql-semantics` has no way to wire up a Databricks Job without
duplicating that logic or shelling out to `dbxcarta` CLI commands.

Databricks recommends Databricks Asset Bundles for defining and deploying jobs.
The `python_wheel_task` job type is the right task type for a packaged
entrypoint installed from PyPI.

### Fix

Add a `databricks.yml` bundle to `sql-semantics` that defines two jobs:
`sql-semantics-ingest` and `sql-semantics-client`. Each job uses a
`python_wheel_task` that pulls `dbxcarta` (and `sql-semantics` itself) from
PyPI and calls the existing CLI entrypoints.

**`sql-semantics/databricks.yml`**

```yaml
bundle:
  name: sql-semantics

variables:
  warehouse_id:
    description: "SQL warehouse ID for client evaluation runs"

targets:
  dev:
    mode: development
    default: true
  prod:
    mode: production

resources:
  jobs:
    sql_semantics_ingest:
      name: "[${bundle.target}] sql-semantics ingest"
      tasks:
        - task_key: ingest
          python_wheel_task:
            package_name: dbxcarta
            entry_point: dbxcarta-ingest
          libraries:
            - pypi:
                package: "dbxcarta>=0.2.38"
            - pypi:
                package: "sql-semantics>=0.1.0"
          # No cluster config = serverless compute

    sql_semantics_client:
      name: "[${bundle.target}] sql-semantics client"
      tasks:
        - task_key: client
          python_wheel_task:
            package_name: dbxcarta
            entry_point: dbxcarta-client
          libraries:
            - pypi:
                package: "dbxcarta>=0.2.38"
            - pypi:
                package: "sql-semantics>=0.1.0"
```

Environment variables (Neo4j URI, warehouse ID, embedding endpoint, etc.) are
supplied via job environment variables or Databricks Secrets, not hardcoded in
the bundle. The preset's `.env()` method produces the full key set; consumers
paste those as job environment variables or reference secrets.

**Consumer workflow after this fix**

```bash
cd sql-semantics
databricks bundle validate
databricks bundle deploy
databricks bundle run sql_semantics_ingest
```

No local `submit-entrypoint` invocation needed. The job definition lives in
version control alongside the preset code.

---

## Execution order

1. Problem 2 (API extraction) first -- the public surface is what gets
   published.
2. Problem 1 (PyPI publish) second -- bump version, publish, update
   `sql-semantics` dep.
3. Problem 3 (DABs bundle) last -- depends on the package being on PyPI so the
   `libraries:` block can resolve it.

---

## Publishing setup

Releases use OIDC trusted publishing. No `PYPI_API_TOKEN` is stored in CI.
GitHub Actions exchanges an OIDC token with PyPI when a version tag is pushed.

Release flow:
1. Trigger a version-bump workflow from the GitHub Actions UI.
2. The workflow runs `uv version --bump`, commits `pyproject.toml`, and pushes a
   version tag via the `GIT_PUSH_PAT` secret.
3. The tag triggers `publish.yaml`, which builds a wheel and sdist, then
   publishes to PyPI using OIDC.

### Step 1: First manual publish

PyPI requires the project to exist before trusted publishing can be configured.
Run this once locally with a temporary API token to register the project name:

```bash
uv build --sdist --wheel
uv publish --token <your-pypi-api-token>
```

After this, all future releases are token-free.

### Step 2: Configure the PyPI trusted publisher

Navigate to:
`https://pypi.org/manage/project/dbxcarta/settings/publishing/`

Add a new trusted publisher with these values:

| Field | Value |
|-------|-------|
| Owner | your GitHub user or org name |
| Repository | `dbxcarta` |
| Workflow filename | `publish.yaml` |
| Environment name | `pypi` |

### Step 3: Create the GitHub `pypi` environment

In the GitHub repository: Settings > Environments > New environment.

Name it `pypi`. This must match `environment: name: pypi` in `publish.yaml`.
Optionally add a required reviewer to gate production publishes.

### Step 4: Add the `GIT_PUSH_PAT` secret

The version-bump workflows commit and push a tag. The default `GITHUB_TOKEN`
cannot trigger downstream workflow runs, so a PAT is required (same constraint
as neo4j-graphrag-python).

Create a GitHub Personal Access Token with `contents: write` scope, then add it
as a repository secret:

Settings > Secrets and variables > Actions > New repository secret
- Name: `GIT_PUSH_PAT`
- Value: the PAT

### Releasing

| Release type | Workflow to trigger | Example |
|---|---|---|
| Bug fix | "Publish a new patch release" | `0.2.38` to `0.2.39` |
| New feature | "Publish a new minor release" | `0.2.38` to `0.3.0` |
| Breaking change | "Publish a new major release" | `0.2.38` to `1.0.0` |

Go to Actions > select the workflow > Run workflow. No inputs needed.

Each workflow commits `pyproject.toml` with the bumped version and pushes a tag
matching the version number. The tag triggers `publish.yaml`.

### Verifying a release

Check the published package on PyPI:

```
https://pypi.org/p/dbxcarta
```

Install and verify locally:

```bash
pip install dbxcarta==<version>
python -c "import dbxcarta; print('ok')"
```
