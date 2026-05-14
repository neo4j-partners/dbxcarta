# Publish Fix Plan

Three independent problems block consumer deployment of `sql-semantics`. Each
has a clear fix. They can be addressed in order since Problem 2 should land
before Problem 1 (the public API is part of what gets published).

## Current status

| Problem | Status | Notes |
|---------|--------|-------|
| Problem 1: publish `dbxcarta` | In progress | `.github/workflows/publish.yaml` and `.github/workflows/release.yaml` are present. PyPI one-time setup, first manual publish, trusted publisher configuration, and the downstream `sql-semantics` dependency update still need to happen outside this repository. |
| Problem 2: public client API | Implemented | `parse_sql`, `embed_questions`, `compare_result_sets`, and `load_questions` are exported from `dbxcarta.client`; the Finance Genie local demo now imports the public API. |
| Problem 3: Databricks Jobs deployment path | Proposed | The recommended consumer path is a Declarative Automation Bundle in `sql-semantics`. Ingest must use classic single-user jobs compute because the Neo4j Spark Connector is not supported on serverless jobs compute. The local `dbxcarta submit-entrypoint ingest` helper now rejects serverless compute for the same reason. |

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

**Step C: Add the release workflow**

A single `.github/workflows/release.yaml` with a `workflow_dispatch` dropdown
input replaces the three separate patch/minor/major files. One workflow to find,
one place to maintain.

```yaml
name: Release

on:
  workflow_dispatch:
    inputs:
      bump_type:
        description: Version bump type
        required: true
        default: patch
        type: choice
        options:
          - patch
          - minor
          - major
```

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
    "dbxcarta>=1.0.0",   # update floor to published version
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

All four utilities now have public import paths:

| Symbol | Public location | Status |
|--------|----------------|--------|
| `_compare_result_sets` | `dbxcarta.client.compare.compare_result_sets` | Implemented, alias only in `client.py` for compatibility |
| `_load_questions` | `dbxcarta.client.questions.load_questions` | Implemented, alias only in `client.py` for compatibility |
| `_parse_sql` | `dbxcarta.client.sql.parse_sql` | Implemented, alias only in `client.py` for compatibility |
| `_embed_questions` | `dbxcarta.client.embed.embed_questions` | Implemented, alias only in `client.py` for compatibility |

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

Databricks recommends Declarative Automation Bundles for defining and deploying
jobs. The `python_wheel_task` job type is the right task type for a packaged
entrypoint installed from PyPI, but this workload must run on classic jobs
compute because dbxcarta writes graph data through the Neo4j Spark Connector.
Neo4j's Databricks quickstart calls for a compute cluster with `Single user`
access mode and notes that shared access modes are not supported. Databricks
also documents classic jobs compute as a supported compute type for Python
wheel tasks and recommends task-level libraries when jobs share compute.

### Fix

Add a `databricks.yml` bundle to `sql-semantics` that defines two jobs:
`sql-semantics-ingest` and `sql-semantics-client`. Each job uses a
`python_wheel_task` that pulls `dbxcarta` (and `sql-semantics` itself) from
PyPI and calls the existing wheel entrypoints. The ingest job uses classic
single-user jobs compute with the Neo4j Spark Connector installed as a Maven
library. The client job can also use classic jobs compute for a uniform
deployment path; if a consumer later proves it does not need the Spark
connector, it can be moved to serverless with an explicit `environment_key`.

**`sql-semantics/databricks.yml`**

```yaml
bundle:
  name: sql-semantics

variables:
  dbxcarta_version:
    description: "Published dbxcarta version to install"
    default: "1.0.0"
  sql_semantics_version:
    description: "Published sql-semantics version to install"
    default: "0.1.0"
  spark_version:
    description: "Classic Databricks Runtime with Scala version matching the Neo4j connector"
    default: "14.3.x-scala2.12"
  node_type_id:
    description: "Classic jobs compute node type"
    default: "i3.xlarge"
  neo4j_connector_maven:
    description: "Neo4j Spark Connector Maven coordinate matching Spark/Scala runtime"
    default: "org.neo4j:neo4j-connector-apache-spark_2.12:5.3.6_for_spark_3"
  warehouse_id:
    description: "SQL warehouse ID for client evaluation runs"
  catalog:
    description: "Unity Catalog catalog to ingest"
  schemas:
    description: "Comma-separated schemas to ingest"
    default: ""
  summary_volume:
    description: "UC Volume path for run summary JSON"
  summary_table:
    description: "catalog.schema.table for run summary history"
  volume_path:
    description: "UC Volume root used by the client job"
  secret_scope:
    description: "Databricks secret scope containing NEO4J_URI, NEO4J_USERNAME, NEO4J_PASSWORD"
    default: "dbxcarta-neo4j"
  chat_endpoint:
    description: "Serving endpoint used by client generation"
    default: ""
  embedding_endpoint:
    description: "Embedding endpoint used by ingest and client retrieval"
    default: "databricks-gte-large-en"

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
          new_cluster:
            spark_version: ${var.spark_version}
            node_type_id: ${var.node_type_id}
            num_workers: 1
            data_security_mode: SINGLE_USER
          python_wheel_task:
            package_name: dbxcarta
            entry_point: dbxcarta-ingest
            parameters:
              - "DBXCARTA_CATALOG={{job.parameters.dbxcarta_catalog}}"
              - "DBXCARTA_SCHEMAS={{job.parameters.dbxcarta_schemas}}"
              - "DBXCARTA_SUMMARY_VOLUME={{job.parameters.dbxcarta_summary_volume}}"
              - "DBXCARTA_SUMMARY_TABLE={{job.parameters.dbxcarta_summary_table}}"
              - "DATABRICKS_SECRET_SCOPE={{job.parameters.databricks_secret_scope}}"
              - "DATABRICKS_WAREHOUSE_ID={{job.parameters.databricks_warehouse_id}}"
              - "DBXCARTA_EMBEDDING_ENDPOINT={{job.parameters.dbxcarta_embedding_endpoint}}"
          libraries:
            - pypi:
                package: "dbxcarta==${var.dbxcarta_version}"
            - pypi:
                package: "sql-semantics==${var.sql_semantics_version}"
            - maven:
                coordinates: ${var.neo4j_connector_maven}
          timeout_seconds: 7200
      parameters:
        - name: dbxcarta_catalog
          default: ${var.catalog}
        - name: dbxcarta_schemas
          default: ${var.schemas}
        - name: dbxcarta_summary_volume
          default: ${var.summary_volume}
        - name: dbxcarta_summary_table
          default: ${var.summary_table}
        - name: databricks_secret_scope
          default: ${var.secret_scope}
        - name: databricks_warehouse_id
          default: ${var.warehouse_id}
        - name: dbxcarta_embedding_endpoint
          default: ${var.embedding_endpoint}

    sql_semantics_client:
      name: "[${bundle.target}] sql-semantics client"
      tasks:
        - task_key: client
          new_cluster:
            spark_version: ${var.spark_version}
            node_type_id: ${var.node_type_id}
            num_workers: 1
            data_security_mode: SINGLE_USER
          python_wheel_task:
            package_name: dbxcarta
            entry_point: dbxcarta-client
            parameters:
              - "DBXCARTA_CATALOG={{job.parameters.dbxcarta_catalog}}"
              - "DBXCARTA_SCHEMAS={{job.parameters.dbxcarta_schemas}}"
              - "DBXCARTA_SUMMARY_VOLUME={{job.parameters.dbxcarta_summary_volume}}"
              - "DBXCARTA_SUMMARY_TABLE={{job.parameters.dbxcarta_summary_table}}"
              - "DATABRICKS_VOLUME_PATH={{job.parameters.databricks_volume_path}}"
              - "DATABRICKS_SECRET_SCOPE={{job.parameters.databricks_secret_scope}}"
              - "DATABRICKS_WAREHOUSE_ID={{job.parameters.databricks_warehouse_id}}"
              - "DBXCARTA_CHAT_ENDPOINT={{job.parameters.dbxcarta_chat_endpoint}}"
              - "DBXCARTA_EMBEDDING_ENDPOINT={{job.parameters.dbxcarta_embedding_endpoint}}"
          libraries:
            - pypi:
                package: "dbxcarta==${var.dbxcarta_version}"
            - pypi:
                package: "sql-semantics==${var.sql_semantics_version}"
          timeout_seconds: 3600
      parameters:
        - name: dbxcarta_catalog
          default: ${var.catalog}
        - name: dbxcarta_schemas
          default: ${var.schemas}
        - name: dbxcarta_summary_volume
          default: ${var.summary_volume}
        - name: dbxcarta_summary_table
          default: ${var.summary_table}
        - name: databricks_volume_path
          default: ${var.volume_path}
        - name: databricks_secret_scope
          default: ${var.secret_scope}
        - name: databricks_warehouse_id
          default: ${var.warehouse_id}
        - name: dbxcarta_chat_endpoint
          default: ${var.chat_endpoint}
        - name: dbxcarta_embedding_endpoint
          default: ${var.embedding_endpoint}
```

Runtime configuration is passed as Databricks job parameters into
`python_wheel_task.parameters`. The dbxcarta wheel entrypoints already parse
`KEY=VALUE` positional parameters into `os.environ`, so consumers do not need
wrapper scripts. Neo4j credentials stay in Databricks Secrets; the bundle passes
only `DATABRICKS_SECRET_SCOPE`, and the job reads `NEO4J_URI`,
`NEO4J_USERNAME`, and `NEO4J_PASSWORD` at runtime.

Dependency versions are pinned in the job libraries. Databricks recommends
specifying exact package versions for jobs so repeated runs do not silently pick
up newly published package versions. The Neo4j connector Maven coordinate must
match the cluster's Spark and Scala runtime.

If the workspace enforces cluster policies, use a policy that allows classic
jobs compute with `SINGLE_USER` access mode and the Neo4j connector Maven
coordinate. Neo4j's quickstart names an unrestricted policy because the
connector is not supported on shared access modes.

**Consumer workflow after this fix**

```bash
cd sql-semantics
databricks bundle validate \
  --var="catalog=<catalog>" \
  --var="summary_volume=/Volumes/<catalog>/<schema>/<volume>/summaries" \
  --var="summary_table=<catalog>.<schema>.dbxcarta_runs" \
  --var="volume_path=/Volumes/<catalog>/<schema>/<volume>" \
  --var="warehouse_id=<warehouse-id>"
databricks bundle deploy
databricks bundle run sql_semantics_ingest
```

No local `submit-entrypoint` invocation needed. The job definition lives in
version control alongside the preset code.

### Databricks and Neo4j references

- [Databricks Jobs compute guidance](https://docs.databricks.com/aws/en/jobs/compute):
  Python wheel tasks support classic jobs compute, serverless jobs compute, and
  all-purpose compute; all-purpose compute is not recommended for production
  jobs.
- [Databricks classic jobs compute guidance](https://docs.databricks.com/aws/en/jobs/compute#share-compute-across-tasks):
  shared job clusters are scoped to a single job run, and libraries must be
  declared in task settings rather than shared cluster settings.
- [Databricks library guidance](https://docs.databricks.com/aws/en/libraries/package-repositories):
  job libraries should pin package versions for reproducible runs, and Maven
  coordinates are the right mechanism for Spark connectors.
- [Databricks bundle task guidance](https://docs.databricks.com/aws/en/dev-tools/bundles/job-task-types#python-wheel-task):
  `python_wheel_task` requires `package_name` and `entry_point`;
  `parameters` is the supported positional-argument path.
- [Neo4j Spark Connector Databricks quickstart](https://neo4j.com/docs/spark/current/databricks/):
  use `Single user` access mode, install the connector from Maven or Spark
  Packages, match the connector to the cluster Scala runtime, and use
  Databricks Secrets instead of plaintext credentials.

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

Go to Actions > **Release** > Run workflow > choose the bump type:

| Bump type | When to use | Example |
|---|---|---|
| `patch` | Bug fixes | `0.2.38` to `0.2.39` |
| `minor` | New features, backward compatible | `0.2.38` to `0.3.0` |
| `major` | Breaking changes | `0.2.38` to `1.0.0` |

The workflow commits `pyproject.toml` with the bumped version and pushes a tag
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
