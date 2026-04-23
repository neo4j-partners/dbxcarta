"""Pre-run checks: catalog accessibility, volume/table provisioning,
embedding-endpoint reachability.

Extracted from pipeline.py during Phase 3.6 foundation rewrite. Runs before
any UC read or Neo4j write. A preflight failure means the rest of the run
cannot succeed, so the caller should not catch these.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from dbxcarta.staging import parse_volume_path

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from dbxcarta.settings import Settings

logger = logging.getLogger(__name__)


def preflight(spark: "SparkSession", settings: "Settings") -> None:
    """Fail fast on any mis-provisioned dependency.

    Three checks:
      1. Catalog + information_schema readable.
      2. Summary volume + summary table exist (create if not).
      3. When any embedding flag is on, the endpoint answers a trivial ai_query
         and returns a vector of the expected dimension.
    """
    from py4j.protocol import Py4JJavaError
    from pyspark.errors import AnalysisException

    catalog = settings.dbxcarta_catalog
    spark.sql(
        f"SELECT 1 FROM `{catalog}`.information_schema.schemata LIMIT 1"
    ).collect()

    parts = parse_volume_path(settings.dbxcarta_summary_volume)
    vol_catalog, vol_schema, vol_name = parts[1], parts[2], parts[3]
    spark.sql(
        f"CREATE VOLUME IF NOT EXISTS `{vol_catalog}`.`{vol_schema}`.`{vol_name}`"
    )

    table = settings.dbxcarta_summary_table
    quoted_table = ".".join(f"`{p}`" for p in table.split("."))
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {quoted_table} (
            run_id STRING NOT NULL,
            job_name STRING,
            contract_version STRING,
            catalog STRING,
            schemas ARRAY<STRING>,
            started_at TIMESTAMP,
            ended_at TIMESTAMP,
            status STRING,
            row_counts MAP<STRING, BIGINT>,
            neo4j_counts MAP<STRING, BIGINT>,
            error STRING,
            embedding_model STRING,
            embedding_flags MAP<STRING, BOOLEAN>,
            embedding_attempts MAP<STRING, BIGINT>,
            embedding_successes MAP<STRING, BIGINT>,
            embedding_failure_rate_per_label MAP<STRING, DOUBLE>,
            embedding_failure_rate DOUBLE,
            embedding_failure_threshold DOUBLE,
            embedding_ledger_hits MAP<STRING, BIGINT>
        ) USING DELTA
    """)

    any_embeddings = any([
        settings.dbxcarta_include_embeddings_tables,
        settings.dbxcarta_include_embeddings_columns,
        settings.dbxcarta_include_embeddings_values,
        settings.dbxcarta_include_embeddings_schemas,
        settings.dbxcarta_include_embeddings_databases,
    ])
    if any_embeddings:
        endpoint = settings.dbxcarta_embedding_endpoint
        # ai_query surfaces failures through the Spark execution layer, so
        # narrow to the two types Spark actually raises: AnalysisException
        # for SQL/plan errors, Py4JJavaError for runtime JVM exceptions.
        # Anything else (e.g. KeyboardInterrupt) propagates unchanged.
        try:
            rows = spark.sql(
                f"SELECT ai_query('{endpoint}', 'preflight', failOnError => false) AS response"
            ).collect()
        except (AnalysisException, Py4JJavaError) as exc:
            raise RuntimeError(
                f"[dbxcarta] preflight: embedding endpoint '{endpoint}' unreachable"
                f" or missing invoke permission: {exc}"
            ) from exc

        resp = rows[0]["response"]
        if resp["errorMessage"] is not None:
            raise RuntimeError(
                f"[dbxcarta] preflight: embedding endpoint '{endpoint}' returned an error:"
                f" {resp['errorMessage']}"
            )
        vec = resp["result"]
        if vec is None or len(vec) != settings.dbxcarta_embedding_dimension:
            actual = len(vec) if vec is not None else 0
            raise RuntimeError(
                f"[dbxcarta] preflight: embedding endpoint '{endpoint}' returned a vector of"
                f" length {actual}, expected {settings.dbxcarta_embedding_dimension}."
                f" Set DBXCARTA_EMBEDDING_DIMENSION to match the endpoint."
            )

    logger.info(
        "[dbxcarta] preflight passed: %s.information_schema accessible, volume and table ready",
        catalog,
    )
