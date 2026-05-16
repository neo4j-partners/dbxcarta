# Finance Genie E2E Cluster Dependency Setup

Last checked: 2026-05-15.

This dependency list is for the target cluster used in the E2E audit:
`1029-205109-yca7gn2n` (`Small Spark 4.0`), reported by Databricks as
`spark_version=17.3.x-scala2.13`, `use_ml_runtime=true`.

Databricks Runtime 17.3 LTS ML is built on Databricks Runtime 17.3 LTS.
The base runtime is powered by Apache Spark 4.0.0 and uses Python 3.12.3 and
Scala 2.13.16. The ML runtime adds many Python packages, but it does not add
the dbxcarta wheels, `databricks-job-runner`, the Neo4j Python driver,
`pydantic-settings`, `python-dotenv`, or the Neo4j Spark Connector JAR.

## Cluster Install Summary

Install these explicitly on the cluster, or make them available in an offline
wheelhouse if the cluster cannot reach PyPI:

| Dependency | Source | Version to use | Should already be on DBR 17.3 LTS ML? | Cluster action |
| --- | --- | --- | --- | --- |
| `dbxcarta-spark` | local wheel in UC Volume | `1.0.0` from this repo | No | Install for `submit-entrypoint ingest`. |
| `dbxcarta-client` | local wheel in UC Volume | `1.0.0` from this repo | No | Install for `submit-entrypoint client`. |
| `databricks-job-runner` | PyPI | latest `0.4.8` | No | Install. Required by `dbxcarta-spark`. |
| `neo4j` | PyPI | latest `6.2.0` | No | Install. Required by both dbxcarta packages. |
| `pydantic-settings` | PyPI | latest `2.14.1` | No | Install. Required by both dbxcarta packages. |
| `python-dotenv` | PyPI | latest `1.2.2` | No | Install. Required by `dbxcarta-spark` and the Finance Genie example. |
| `pyspark` | Databricks Runtime | Runtime Spark 4.0.0 | Yes, runtime-provided | Do not install from PyPI on Databricks. Treat as provided by the runtime. |
| Neo4j Spark Connector | Maven | `org.neo4j:neo4j-connector-apache-spark_2.13:5.3.10_for_spark_3` | No | Install as a cluster Maven library if running ingest. |

Compatibility note: the current public Neo4j Spark Connector docs do not list
a Spark 4 coordinate in the compatibility table. The coordinate above has been
verified against the target Spark 4.0 / Scala 2.13 Databricks cluster and is
the approved cluster Maven library for this E2E path.

## Direct Python Dependencies

These come from:

- `packages/dbxcarta-spark/pyproject.toml`
- `packages/dbxcarta-client/pyproject.toml`
- `examples/integration/finance-genie/pyproject.toml`
- `uv tree --package dbxcarta-finance-genie-example`

The "locked" column is what the current repo lock resolves locally. The
"latest PyPI" column was checked against PyPI package metadata on
2026-05-15.

| Dependency | Used by | Manifest range | Locked | Latest PyPI | DBR 17.3 LTS ML package | Should be available? | Cluster action |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `databricks-job-runner` | `dbxcarta-spark` | `>=0.4.8` | `0.4.8` | `0.4.8` | Not listed | No | Install. |
| `databricks-sdk` | all packages | `>=0.40` | `0.103.0` | `0.108.0` | `0.49.0` | Yes, but older than lock | Built-in satisfies the manifest range. Install only if pinning to lock/latest behavior. |
| `neo4j` | `dbxcarta-spark`, `dbxcarta-client` | `>=5.0` | `6.1.0` | `6.2.0` | Not listed | No | Install. |
| `pydantic` | `dbxcarta-spark`, `dbxcarta-client` | `>=2` | `2.13.3` | `2.13.4` | `2.10.6` | Yes, but older than lock | Built-in satisfies the manifest range. Install only if pinning to lock/latest behavior. |
| `pydantic-settings` | `dbxcarta-spark`, `dbxcarta-client` | `>=2` | `2.14.0` | `2.14.1` | Not listed | No | Install. |
| `pyspark` | `dbxcarta-spark`, client eval runtime | `>=3.5` | `4.1.1` | `4.1.1` | Spark 4.0.0 runtime | Yes | Treat as provided. This should not be a hard cluster-library download. |
| `python-dotenv` | `dbxcarta-spark`, Finance Genie example | unpinned | `1.2.2` | `1.2.2` | Not listed | No | Install. |
| `requests` | `dbxcarta-client`, transitive through SDK | unpinned | `2.33.1` | `2.34.2` | `2.32.3` | Yes | Built-in is enough for the broad manifest range. |

## Transitive Python Dependencies

If Databricks can use its built-in packages and the top-level manifests remain
broad, not every transitive package below needs to be preinstalled manually.
If the cluster has no PyPI egress and the install path resolves against the
repo lock or latest PyPI packages, include the missing or too-old transitive
wheels in the wheelhouse.

| Dependency | Pulled by | Locked | Latest PyPI | DBR 17.3 LTS ML package | Should be available? | Cluster action |
| --- | --- | --- | --- | --- | --- | --- |
| `google-auth` | `databricks-sdk` | `2.49.2` | `2.53.0` | `2.40.3` | Yes, but older than lock | Usually no action unless pinning. |
| `cryptography` | `google-auth` | `46.0.7` | `48.0.0` | `43.0.3` | Yes, but older than lock | Usually no action unless pinning. |
| `cffi` | `cryptography` | `2.0.0` | `2.0.0` | `1.17.1` | Yes, but older than lock | Include if installing locked `cryptography`. |
| `pycparser` | `cffi` | `3.0` | `3.0` | `2.21` | Yes, but older than lock | Include if installing locked `cffi`. |
| `pyasn1-modules` | `google-auth` | `0.4.2` | `0.4.2` | `0.2.8` | Yes, but older than lock | Usually no action unless pinning. |
| `pyasn1` | `pyasn1-modules` | `0.6.3` | `0.6.3` | `0.4.8` | Yes, but older than lock | Usually no action unless pinning. |
| `protobuf` | `databricks-sdk` | `6.33.6` | `7.34.1` | `5.29.4` | Yes, but older than lock | Usually no action unless pinning. |
| `certifi` | `requests` | `2026.2.25` | `2026.4.22` | `2025.1.31` | Yes, but older than lock | Usually no action unless pinning. |
| `charset-normalizer` | `requests` | `3.4.7` | `3.4.7` | `3.3.2` | Yes, but older than lock | Usually no action unless pinning. |
| `idna` | `requests` | `3.12` | `3.15` | `3.7` | Yes, but older than lock | Usually no action unless pinning. |
| `urllib3` | `requests` | `2.7.0` | `2.7.0` | `2.3.0` | Yes, but older than lock | Usually no action unless pinning. |
| `pydantic-core` | `pydantic` | `2.46.3` | `2.46.4` | `2.27.2` | Yes, but older than lock | Include if installing locked/latest `pydantic`. |
| `typing-extensions` | `pydantic`, `typing-inspection` | `4.15.0` | `4.15.0` | `4.12.2` | Yes, but older than lock | Usually no action unless pinning. |
| `typing-inspection` | `pydantic`, `pydantic-settings` | `0.4.2` | `0.4.2` | Not listed | No | Install with `pydantic-settings`. |
| `annotated-types` | `pydantic` | `0.7.0` | `0.7.0` | `0.7.0` | Yes | No action. |
| `pytz` | `neo4j` | `2026.1.post1` | `2026.2` | `2024.1` | Yes, but older than lock | Install if required by the selected `neo4j` wheel. |
| `py4j` | `pyspark` | `0.10.9.9` | `0.10.9.9` | Runtime Spark component | Yes | Treat as runtime-provided with Spark. |

## Operational Guidance

The Step 8 failure in `e2epipeline.md` happened during Databricks library
installation, before the ingest task executed. Attaching
`dbxcarta_spark-1.0.0-py3-none-any.whl` makes Databricks run pip on the
driver, which must either see already-installed compatible distributions or
download every missing dependency.

For this cluster, the least fragile setup is:

1. Install the verified Neo4j Spark Connector coordinate as a compute-scoped
   Maven library:
   `org.neo4j:neo4j-connector-apache-spark_2.13:5.3.10_for_spark_3`.
2. Install `dbxcarta-spark` and `dbxcarta-client` wheels from the UC Volume.
3. Preinstall or wheelhouse the missing Python packages:
   `databricks-job-runner`, `neo4j`, `pydantic-settings`,
   `python-dotenv`, and their missing transitive dependencies.
4. Do not force a PyPI install of `pyspark` on Databricks. The runtime already
   owns Spark and PySpark compatibility.

If dependency resolution must be exact to the local `uv.lock`, do not rely on
Databricks built-ins for packages such as `databricks-sdk`, `pydantic`,
`requests`, `google-auth`, `protobuf`, or `urllib3`; the built-in versions are
older than the current lock.

## Sources

- Databricks cluster metadata: `databricks clusters get 1029-205109-yca7gn2n`
  reported `17.3.x-scala2.13` and `use_ml_runtime=true`.
- Databricks library installation docs:
  https://docs.databricks.com/aws/en/libraries/
- Databricks Runtime 17.3 LTS release notes and system environment:
  https://docs.databricks.com/aws/en/release-notes/runtime/17.3lts
- Databricks Runtime 17.3 LTS ML release notes and Python library table:
  https://docs.databricks.com/aws/en/release-notes/runtime/17.3lts-ml
- Neo4j Spark Connector installation and compatibility table:
  https://neo4j.com/docs/spark/current/installation/
- PyPI package metadata:
  https://pypi.org/project/databricks-job-runner/,
  https://pypi.org/project/databricks-sdk/,
  https://pypi.org/project/neo4j/,
  https://pypi.org/project/pydantic/,
  https://pypi.org/project/pydantic-settings/,
  https://pypi.org/project/pyspark/,
  https://pypi.org/project/python-dotenv/,
  https://pypi.org/project/requests/
