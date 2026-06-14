# dbxcarta-submit

Operator tooling for building, uploading, and submitting
[dbxcarta](https://github.com/neo4j-field/neocarta) Databricks jobs. It wraps
[`databricks-job-runner`](https://pypi.org/project/databricks-job-runner/) and
knows how to stage a [neocarta](https://github.com/neo4j-field/neocarta)
connector wheel onto a Unity Catalog Volume and submit its ingest job.

The distribution installs:

- the `dbxcarta` console command (the operator CLI), and
- the `dbxcarta.submit` import path, including the `submit_neocarta_ingest`
  library function.

## Install

```bash
pip install dbxcarta-submit
```

The wheel is self-contained: its only external dependency is
`databricks-job-runner`. The `dbxcarta.core` modules it uses are bundled into
the wheel, so no separate install is required.

## Library use

```python
from pathlib import Path

from dbxcarta.submit import submit_neocarta_ingest

# Stage a prebuilt neocarta connector wheel and submit the ingest job.
# The catalog, Volume, profile, and Neo4j secrets come from the selected
# dbxcarta overlay (DBXCARTA_ENV_FILE or --env-file), exactly as the CLI
# resolves them.
submit_neocarta_ingest(Path("dist/neocarta-1.0.0-py3-none-any.whl"))
```

The ingest job uses the Neo4j Spark Connector, a JVM cluster library, so it must
run on classic (non-serverless) compute. `submit_neocarta_ingest` defaults to
`compute_mode="cluster"`.

## CLI use

```bash
dbxcarta --help
```

See the [dbxcarta repository](https://github.com/neo4j-field/neocarta) for the
operator workflow.
