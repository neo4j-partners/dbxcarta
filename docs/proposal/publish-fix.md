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

### Fix

1. Publish `dbxcarta` to PyPI using `uv publish`.
2. In `sql-semantics/pyproject.toml`, remove the `[tool.uv.sources]` section
   entirely. The `dependencies` line already has the correct form:
   `dbxcarta>=0.2.35` -- update the floor to the published version.
3. Run `uv sync` in `sql-semantics` to confirm the dependency resolves from
   PyPI with no local-path override.

### Pre-publish checklist

- Confirm `pyproject.toml` `description`, `authors`, and `requires-python` are
  accurate.
- Add `license`, `homepage`, and `classifiers` fields if this is intended as a
  public package. If it is internal-only, a private index (Azure Artifacts,
  Nexus) is the better target and the same `uv publish` command applies.
- The `dist/` directory already has `dbxcarta-0.2.38-py3-none-any.whl`. Run
  `uv build` to regenerate after any changes, then `uv publish`.

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
