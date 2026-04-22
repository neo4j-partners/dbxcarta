"""Phase 1 — Schema Graph Job."""

from __future__ import annotations

import logging
import os
import re

from pydantic import field_validator
from pydantic_settings import BaseSettings

from dbxcarta.contract import (
    CONTRACT_VERSION,
    LABEL_COLUMN,
    LABEL_DATABASE,
    LABEL_SCHEMA,
    LABEL_TABLE,
    REL_HAS_COLUMN,
    REL_HAS_SCHEMA,
    REL_HAS_TABLE,
    generate_id,
    id_expr,
)
from dbxcarta.summary import RunSummary
from dbxcarta.writer import Neo4jConfig, write_nodes, write_relationship

logger = logging.getLogger(__name__)

_IDENTIFIER_RE = re.compile(r"^[a-zA-Z0-9_.`\-]+$")


class Settings(BaseSettings):
    databricks_secret_scope: str = "dbxcarta-neo4j"
    dbxcarta_catalog: str
    dbxcarta_schemas: str = ""
    dbxcarta_summary_volume: str
    dbxcarta_summary_table: str
    dbxcarta_write_partitions: int = 8

    @field_validator("dbxcarta_catalog", "dbxcarta_summary_table")
    @classmethod
    def _validate_identifier(cls, v: str) -> str:
        if not _IDENTIFIER_RE.match(v):
            raise ValueError(f"Invalid Databricks identifier: {v!r}")
        return v


def run_schema() -> None:
    settings = Settings()

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    run_id = os.environ.get("DATABRICKS_JOB_RUN_ID", "local")
    schema_list = [s.strip() for s in settings.dbxcarta_schemas.split(",") if s.strip()]

    summary = RunSummary(
        run_id=run_id,
        job_name="schema_graph",
        contract_version=CONTRACT_VERSION,
        catalog=settings.dbxcarta_catalog,
        schemas=schema_list,
    )

    try:
        _run(spark, settings, schema_list, summary)
        summary.finish(status="success")
    except Exception as exc:
        summary.finish(status="failure", error=str(exc))
        raise
    finally:
        summary.emit(spark, settings.dbxcarta_summary_volume, settings.dbxcarta_summary_table)


def _run(spark, settings: Settings, schema_list: list[str], summary: RunSummary) -> None:
    from databricks.sdk.runtime import dbutils
    from pyspark.sql.functions import col, lit, when

    scope = settings.databricks_secret_scope
    neo4j = Neo4jConfig(
        uri=dbutils.secrets.get(scope=scope, key="uri"),
        username=dbutils.secrets.get(scope=scope, key="username"),
        password=dbutils.secrets.get(scope=scope, key="password"),
    )

    _preflight(spark, settings)
    _bootstrap_constraints(neo4j)

    catalog = settings.dbxcarta_catalog
    partitions = settings.dbxcarta_write_partitions
    version = lit(CONTRACT_VERSION)

    # --- R1: schemata ---
    schemata_df = spark.sql(
        f"SELECT catalog_name, schema_name, comment"
        f" FROM `{catalog}`.information_schema.schemata"
    )
    schemata_df = schemata_df.filter(col("schema_name") != "information_schema")
    if schema_list:
        schemata_df = schemata_df.filter(col("schema_name").isin(schema_list))
    schemata_df = schemata_df.cache()

    # --- R2: tables ---
    tables_df = spark.sql(
        f"SELECT table_catalog, table_schema, table_name, table_type,"
        f"       comment, created, last_altered"
        f" FROM `{catalog}`.information_schema.tables"
    )
    tables_df = tables_df.filter(col("table_schema") != "information_schema")
    if schema_list:
        tables_df = tables_df.filter(col("table_schema").isin(schema_list))
    tables_df = tables_df.cache()

    # --- R3: columns ---
    columns_df = spark.sql(
        f"SELECT table_catalog, table_schema, table_name, column_name,"
        f"       data_type, is_nullable, ordinal_position, comment"
        f" FROM `{catalog}`.information_schema.columns"
    )
    columns_df = columns_df.filter(col("table_schema") != "information_schema")
    if schema_list:
        columns_df = columns_df.filter(col("table_schema").isin(schema_list))
    columns_df = columns_df.cache()

    # R4 (FK resolution) — pending investigation; W8 not implemented
    summary.row_counts["fk_references"] = 0

    summary.row_counts["schemas"] = schemata_df.count()
    summary.row_counts["tables"] = tables_df.count()
    summary.row_counts["columns"] = columns_df.count()
    logger.info(
        "[dbxcarta] read: schemas=%d tables=%d columns=%d",
        summary.row_counts["schemas"],
        summary.row_counts["tables"],
        summary.row_counts["columns"],
    )

    # --- Build node DataFrames ---
    from pyspark.sql import Row

    database_df = spark.createDataFrame(
        [Row(id=generate_id(catalog), name=catalog, contract_version=CONTRACT_VERSION)]
    )

    schema_node_df = (
        schemata_df
        .withColumn("id", id_expr("catalog_name", "schema_name"))
        .withColumn("name", col("schema_name"))
        .withColumn("contract_version", version)
        .select("id", "name", "comment", "contract_version")
    )

    table_node_df = (
        tables_df
        .withColumn("id", id_expr("table_catalog", "table_schema", "table_name"))
        .withColumn("name", col("table_name"))
        .withColumn("contract_version", version)
        .select("id", "name", "comment", "table_type", "created", "last_altered", "contract_version")
    )

    column_node_df = (
        columns_df
        .withColumn("id", id_expr("table_catalog", "table_schema", "table_name", "column_name"))
        .withColumn("name", col("column_name"))
        .withColumn(
            "is_nullable",
            when(col("is_nullable") == "YES", True).when(col("is_nullable") == "NO", False),
        )
        .withColumn("contract_version", version)
        .select("id", "name", "data_type", "is_nullable", "ordinal_position", "comment", "contract_version")
    )

    # --- W1–W4: node writes ---
    logger.info("[dbxcarta] writing nodes: Database (1)")
    write_nodes(database_df, neo4j, LABEL_DATABASE)

    logger.info("[dbxcarta] writing nodes: Schema (%d)", summary.row_counts["schemas"])
    write_nodes(schema_node_df.repartition(partitions), neo4j, LABEL_SCHEMA)

    logger.info("[dbxcarta] writing nodes: Table (%d)", summary.row_counts["tables"])
    write_nodes(table_node_df.repartition(partitions), neo4j, LABEL_TABLE)

    logger.info("[dbxcarta] writing nodes: Column (%d)", summary.row_counts["columns"])
    write_nodes(column_node_df.repartition(partitions), neo4j, LABEL_COLUMN)

    # --- Build relationship DataFrames ---
    has_schema_df = (
        schemata_df
        .withColumn("source_id", lit(generate_id(catalog)))
        .withColumn("target_id", id_expr("catalog_name", "schema_name"))
        .select("source_id", "target_id")
    )

    has_table_df = (
        tables_df
        .withColumn("source_id", id_expr("table_catalog", "table_schema"))
        .withColumn("target_id", id_expr("table_catalog", "table_schema", "table_name"))
        .select("source_id", "target_id")
    )

    has_column_df = (
        columns_df
        .withColumn("source_id", id_expr("table_catalog", "table_schema", "table_name"))
        .withColumn("target_id", id_expr("table_catalog", "table_schema", "table_name", "column_name"))
        .select("source_id", "target_id")
    )

    # --- W5–W7: relationship writes (W8 pending FK investigation) ---
    logger.info("[dbxcarta] writing relationships: HAS_SCHEMA")
    write_relationship(has_schema_df.repartition(partitions), neo4j, REL_HAS_SCHEMA, LABEL_DATABASE, LABEL_SCHEMA)

    logger.info("[dbxcarta] writing relationships: HAS_TABLE")
    write_relationship(has_table_df.repartition(partitions), neo4j, REL_HAS_TABLE, LABEL_SCHEMA, LABEL_TABLE)

    logger.info("[dbxcarta] writing relationships: HAS_COLUMN")
    write_relationship(has_column_df.repartition(partitions), neo4j, REL_HAS_COLUMN, LABEL_TABLE, LABEL_COLUMN)

    # W8: REFERENCES — not implemented; pending FK resolution investigation

    schemata_df.unpersist()
    tables_df.unpersist()
    columns_df.unpersist()

    # --- Neo4j sanity query ---
    summary.neo4j_counts = _query_neo4j_counts(neo4j)
    logger.info("[dbxcarta] neo4j counts: %s", summary.neo4j_counts)


def _preflight(spark, settings: Settings) -> None:
    catalog = settings.dbxcarta_catalog
    spark.sql(
        f"SELECT 1 FROM `{catalog}`.information_schema.schemata LIMIT 1"
    ).collect()

    # Create UC Volume if not exists.
    # DBXCARTA_SUMMARY_VOLUME must be a /Volumes/<catalog>/<schema>/<volume>[/...] path.
    volume_path = settings.dbxcarta_summary_volume
    parts = volume_path.lstrip("/").split("/")
    if len(parts) < 4 or parts[0] != "Volumes":
        raise RuntimeError(
            f"[dbxcarta] DBXCARTA_SUMMARY_VOLUME must be a /Volumes/<catalog>/<schema>/<volume> path,"
            f" got {volume_path!r}"
        )
    vol_catalog, vol_schema, vol_name = parts[1], parts[2], parts[3]
    spark.sql(
        f"CREATE VOLUME IF NOT EXISTS `{vol_catalog}`.`{vol_schema}`.`{vol_name}`"
    )

    # Create the run-summary Delta table with the full schema on first run.
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
            error STRING
        ) USING DELTA
    """)

    logger.info(
        "[dbxcarta] preflight passed: %s.information_schema accessible, volume and table grants confirmed",
        catalog,
    )


def _bootstrap_constraints(config: Neo4jConfig) -> None:
    from neo4j import GraphDatabase
    from neo4j.exceptions import ClientError

    with GraphDatabase.driver(config.uri, auth=(config.username, config.password)) as driver:
        with driver.session() as session:
            for label in (LABEL_DATABASE, LABEL_SCHEMA, LABEL_TABLE, LABEL_COLUMN):
                try:
                    session.run(
                        f"CREATE CONSTRAINT {label.lower()}_id IF NOT EXISTS "
                        f"FOR (n:{label}) REQUIRE n.id IS UNIQUE"
                    )
                except ClientError as exc:
                    if "ConstraintAlreadyExists" not in (exc.code or ""):
                        raise
                    logger.info("[dbxcarta] constraint for %s already satisfied by existing constraint, skipping", label)
            session.run(
                f"CREATE INDEX {LABEL_COLUMN.lower()}_data_type IF NOT EXISTS "
                f"FOR (n:{LABEL_COLUMN}) ON (n.data_type)"
            )
    logger.info("[dbxcarta] neo4j constraints bootstrapped")


def _query_neo4j_counts(config: Neo4jConfig) -> dict[str, int]:
    from neo4j import GraphDatabase

    counts: dict[str, int] = {}
    with GraphDatabase.driver(config.uri, auth=(config.username, config.password)) as driver:
        with driver.session() as session:
            for record in session.run(
                "MATCH (n) RETURN labels(n)[0] AS label, count(*) AS cnt"
            ):
                counts[record["label"]] = record["cnt"]
            for rel_type in (REL_HAS_SCHEMA, REL_HAS_TABLE, REL_HAS_COLUMN):
                result = session.run(f"MATCH ()-[r:{rel_type}]->() RETURN count(r) AS cnt")
                counts[rel_type] = result.single()["cnt"]
    return counts
