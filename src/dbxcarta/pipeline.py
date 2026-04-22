"""Unified DBxCarta v5 pipeline.

This is the only module that:
  - loads Settings
  - runs the preflight
  - opens and closes the Neo4j driver
  - bootstraps constraints and vector indexes
  - issues all Spark Connector writes
  - purges stale Value nodes before writing new ones
  - emits the run summary

schema_graph, sample_values, and embeddings are pure transforms — they take
DataFrames and return DataFrames. No external connections in those modules.
"""

from __future__ import annotations

import logging
import os
import re
from dataclasses import fields as dc_fields

from pydantic import field_validator
from pydantic_settings import BaseSettings

from dbxcarta.contract import (
    CONTRACT_VERSION,
    LABEL_COLUMN,
    LABEL_DATABASE,
    LABEL_SCHEMA,
    LABEL_TABLE,
    LABEL_VALUE,
    REL_HAS_COLUMN,
    REL_HAS_SCHEMA,
    REL_HAS_TABLE,
    REL_HAS_VALUE,
    REL_REFERENCES,
)
from dbxcarta.summary import RunSummary
from dbxcarta.writer import Neo4jConfig, write_nodes, write_relationship
import dbxcarta.embeddings as emb
import dbxcarta.sample_values as sv
import dbxcarta.schema_graph as sg

logger = logging.getLogger(__name__)

_IDENTIFIER_RE = re.compile(r"^[a-zA-Z0-9_.`\-]+$")

# Text fed to the embedding model for Table nodes on the first green run.
# Schema-qualified name + pipe separator + comment (empty comment omitted cleanly).
_TABLE_EMBEDDING_TEXT_EXPR = (
    "concat_ws(' | ', concat_ws('.', table_schema, name), nullif(trim(comment), ''))"
)
_COLUMN_EMBEDDING_TEXT_EXPR = (
    "concat_ws(' | ', concat_ws('.', table_schema, table_name, name),"
    " data_type, nullif(trim(comment), ''))"
)
_SCHEMA_EMBEDDING_TEXT_EXPR = (
    "concat_ws(' | ', concat_ws('.', catalog_name, name), nullif(trim(comment), ''))"
)
_DATABASE_EMBEDDING_TEXT_EXPR = "name"
_VALUE_EMBEDDING_TEXT_EXPR = "value"


class Settings(BaseSettings):
    databricks_secret_scope: str = "dbxcarta-neo4j"
    dbxcarta_catalog: str
    dbxcarta_schemas: str = ""
    dbxcarta_summary_volume: str
    dbxcarta_summary_table: str
    # Sample values
    dbxcarta_include_values: bool = True
    dbxcarta_sample_limit: int = 10
    dbxcarta_sample_cardinality_threshold: int = 50
    dbxcarta_stack_chunk_size: int = 50
    # Embedding feature flags — all off by default; turn on one label at a time.
    dbxcarta_include_embeddings_tables: bool = False
    dbxcarta_include_embeddings_columns: bool = False
    dbxcarta_include_embeddings_values: bool = False
    dbxcarta_include_embeddings_schemas: bool = False
    dbxcarta_include_embeddings_databases: bool = False
    dbxcarta_embedding_failure_threshold: float = 0.05
    dbxcarta_embedding_endpoint: str = "databricks-gte-large-en"
    dbxcarta_embedding_dimension: int = 1024
    # Stage 2: materialize-once between ai_query and downstream actions.
    # If blank, derived at runtime as sibling to DBXCARTA_SUMMARY_VOLUME.
    dbxcarta_staging_path: str = ""
    # Stage 2: Neo4j Spark Connector batch.size (per best-practices Neo4j §2).
    dbxcarta_neo4j_batch_size: int = 20000

    @field_validator("dbxcarta_catalog", "dbxcarta_summary_table")
    @classmethod
    def _validate_identifier(cls, v: str) -> str:
        if not _IDENTIFIER_RE.match(v):
            raise ValueError(f"Invalid Databricks identifier: {v!r}")
        return v


def run_dbxcarta() -> None:
    settings = Settings()

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    run_id = os.environ.get("DATABRICKS_JOB_RUN_ID", "local")
    schema_list = [s.strip() for s in settings.dbxcarta_schemas.split(",") if s.strip()]

    embedding_flags = {
        LABEL_TABLE: settings.dbxcarta_include_embeddings_tables,
        LABEL_COLUMN: settings.dbxcarta_include_embeddings_columns,
        LABEL_VALUE: settings.dbxcarta_include_embeddings_values,
        LABEL_SCHEMA: settings.dbxcarta_include_embeddings_schemas,
        LABEL_DATABASE: settings.dbxcarta_include_embeddings_databases,
    }

    summary = RunSummary(
        run_id=run_id,
        job_name="dbxcarta",
        contract_version=CONTRACT_VERSION,
        catalog=settings.dbxcarta_catalog,
        schemas=schema_list,
        embedding_model=settings.dbxcarta_embedding_endpoint,
        embedding_flags=embedding_flags,
        embedding_failure_threshold=settings.dbxcarta_embedding_failure_threshold,
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
    from pyspark.sql.functions import col

    scope = settings.databricks_secret_scope
    neo4j = Neo4jConfig(
        uri=dbutils.secrets.get(scope=scope, key="uri"),
        username=dbutils.secrets.get(scope=scope, key="username"),
        password=dbutils.secrets.get(scope=scope, key="password"),
        batch_size=settings.dbxcarta_neo4j_batch_size,
    )

    _preflight(spark, settings)
    staging_path = _resolve_staging_path(settings)
    _truncate_staging_root(staging_path)
    _bootstrap_constraints(neo4j, settings)

    catalog = settings.dbxcarta_catalog

    # --- Extract ---
    schemata_df = (
        spark.sql(
            f"SELECT catalog_name, schema_name, comment"
            f" FROM `{catalog}`.information_schema.schemata"
        )
        .filter(col("schema_name") != "information_schema")
    )
    if schema_list:
        schemata_df = schemata_df.filter(col("schema_name").isin(schema_list))
    schemata_df = schemata_df.cache()

    tables_df = (
        spark.sql(
            f"SELECT table_catalog, table_schema, table_name, table_type,"
            f"       comment, created, last_altered"
            f" FROM `{catalog}`.information_schema.tables"
        )
        .filter(col("table_schema") != "information_schema")
    )
    if schema_list:
        tables_df = tables_df.filter(col("table_schema").isin(schema_list))
    tables_df = tables_df.cache()

    columns_df = (
        spark.sql(
            f"SELECT table_catalog, table_schema, table_name, column_name,"
            f"       data_type, is_nullable, ordinal_position, comment"
            f" FROM `{catalog}`.information_schema.columns"
        )
        .filter(col("table_schema") != "information_schema")
    )
    if schema_list:
        columns_df = columns_df.filter(col("table_schema").isin(schema_list))
    columns_df = columns_df.cache()

    fk_pairs_df = spark.sql(
        f"SELECT rc.constraint_schema AS fk_schema,"
        f"       rc.constraint_name   AS fk_name,"
        f"       src.table_catalog AS src_catalog, src.table_schema AS src_schema,"
        f"       src.table_name    AS src_table,   src.column_name  AS src_column,"
        f"       tgt.table_catalog AS tgt_catalog, tgt.table_schema AS tgt_schema,"
        f"       tgt.table_name    AS tgt_table,   tgt.column_name  AS tgt_column,"
        f"       src.ordinal_position AS ord"
        f" FROM `{catalog}`.information_schema.referential_constraints rc"
        f" JOIN `{catalog}`.information_schema.key_column_usage src"
        f"   ON src.constraint_catalog = rc.constraint_catalog"
        f"  AND src.constraint_schema  = rc.constraint_schema"
        f"  AND src.constraint_name    = rc.constraint_name"
        f" JOIN `{catalog}`.information_schema.key_column_usage tgt"
        f"   ON tgt.constraint_catalog = rc.unique_constraint_catalog"
        f"  AND tgt.constraint_schema  = rc.unique_constraint_schema"
        f"  AND tgt.constraint_name    = rc.unique_constraint_name"
        f"  AND tgt.ordinal_position   = src.position_in_unique_constraint"
    )
    declared_df = spark.sql(
        f"SELECT constraint_schema, constraint_name"
        f" FROM `{catalog}`.information_schema.referential_constraints"
    )
    if schema_list:
        # Both sides of a cross-schema FK must be in scope; otherwise the target
        # Column node won't exist and the edge would dangle. Drop dangling edges
        # at read time so the skip is explicit and auditable.
        fk_pairs_df = fk_pairs_df.filter(
            col("fk_schema").isin(schema_list) & col("tgt_schema").isin(schema_list)
        )
        declared_df = declared_df.filter(col("constraint_schema").isin(schema_list))
    fk_pairs_df = fk_pairs_df.cache()
    declared_df = declared_df.cache()

    fk_declared = declared_df.count()
    fk_references = fk_pairs_df.count()
    fk_resolved = fk_pairs_df.select("fk_schema", "fk_name").distinct().count()
    fk_skipped = fk_declared - fk_resolved
    _log_unresolved_fks(fk_skipped, fk_pairs_df, declared_df)

    summary.row_counts["schemas"] = schemata_df.count()
    summary.row_counts["tables"] = tables_df.count()
    summary.row_counts["columns"] = columns_df.count()
    summary.row_counts["fk_declared"] = fk_declared
    summary.row_counts["fk_resolved"] = fk_resolved
    summary.row_counts["fk_skipped"] = fk_skipped
    summary.row_counts["fk_references"] = fk_references

    logger.info(
        "[dbxcarta] read: schemas=%d tables=%d columns=%d"
        " fk_declared=%d fk_resolved=%d fk_skipped=%d fk_references=%d",
        summary.row_counts["schemas"],
        summary.row_counts["tables"],
        summary.row_counts["columns"],
        fk_declared, fk_resolved, fk_skipped, fk_references,
    )

    # --- Transform: schema graph ---
    database_df = sg.build_database_node(spark, catalog)
    schema_node_df = sg.build_schema_nodes(schemata_df)
    table_node_df = sg.build_table_nodes(tables_df)
    column_node_df = sg.build_column_nodes(columns_df)
    has_schema_df = sg.build_has_schema_rel(schemata_df, catalog)
    has_table_df = sg.build_has_table_rel(tables_df)
    has_column_df = sg.build_has_column_rel(columns_df)
    references_df = sg.build_references_rel(fk_pairs_df)

    # --- Transform: embeddings ---
    # Enabled labels are enriched via ai_query, then materialized to a Delta
    # staging table (best-practices Spark §4) so the failure-rate agg and the
    # Neo4j node write both consume the staged data — ai_query is invoked
    # exactly once. Threshold is checked per-label and in aggregate; a breach
    # on either aborts before any Neo4j write.
    total_emb_attempts = 0
    total_emb_successes = 0

    if settings.dbxcarta_include_embeddings_tables:
        enriched = emb.add_embedding_column(
            table_node_df,
            _TABLE_EMBEDDING_TEXT_EXPR,
            settings.dbxcarta_embedding_endpoint,
            settings.dbxcarta_embedding_dimension,
            label=LABEL_TABLE,
        )
        # Materialize-once: writing to Delta staging before downstream actions
        # prevents ai_query re-execution across the failure-rate aggregation
        # and the Neo4j node write (best-practices Spark §4).
        table_node_df = _stage_embedded_nodes(
            enriched, staging_path, LABEL_TABLE,
        )
        rate, attempts, successes = emb.compute_failure_stats(table_node_df)
        summary.embedding_attempts[LABEL_TABLE] = attempts
        summary.embedding_successes[LABEL_TABLE] = successes
        summary.embedding_failure_rate_per_label[LABEL_TABLE] = rate
        total_emb_attempts += attempts
        total_emb_successes += successes
        logger.info(
            "[dbxcarta] Table embeddings: attempts=%d successes=%d failure_rate=%.2f%%",
            attempts, successes, rate * 100,
        )

    if settings.dbxcarta_include_embeddings_columns:
        enriched = emb.add_embedding_column(
            column_node_df,
            _COLUMN_EMBEDDING_TEXT_EXPR,
            settings.dbxcarta_embedding_endpoint,
            settings.dbxcarta_embedding_dimension,
            label=LABEL_COLUMN,
        )
        column_node_df = _stage_embedded_nodes(enriched, staging_path, LABEL_COLUMN)
        rate, attempts, successes = emb.compute_failure_stats(column_node_df)
        summary.embedding_attempts[LABEL_COLUMN] = attempts
        summary.embedding_successes[LABEL_COLUMN] = successes
        summary.embedding_failure_rate_per_label[LABEL_COLUMN] = rate
        total_emb_attempts += attempts
        total_emb_successes += successes
        logger.info(
            "[dbxcarta] Column embeddings: attempts=%d successes=%d failure_rate=%.2f%%",
            attempts, successes, rate * 100,
        )

    if settings.dbxcarta_include_embeddings_schemas:
        enriched = emb.add_embedding_column(
            schema_node_df,
            _SCHEMA_EMBEDDING_TEXT_EXPR,
            settings.dbxcarta_embedding_endpoint,
            settings.dbxcarta_embedding_dimension,
            label=LABEL_SCHEMA,
        )
        schema_node_df = _stage_embedded_nodes(enriched, staging_path, LABEL_SCHEMA)
        rate, attempts, successes = emb.compute_failure_stats(schema_node_df)
        summary.embedding_attempts[LABEL_SCHEMA] = attempts
        summary.embedding_successes[LABEL_SCHEMA] = successes
        summary.embedding_failure_rate_per_label[LABEL_SCHEMA] = rate
        total_emb_attempts += attempts
        total_emb_successes += successes
        logger.info(
            "[dbxcarta] Schema embeddings: attempts=%d successes=%d failure_rate=%.2f%%",
            attempts, successes, rate * 100,
        )

    if settings.dbxcarta_include_embeddings_databases:
        enriched = emb.add_embedding_column(
            database_df,
            _DATABASE_EMBEDDING_TEXT_EXPR,
            settings.dbxcarta_embedding_endpoint,
            settings.dbxcarta_embedding_dimension,
            label=LABEL_DATABASE,
        )
        database_df = _stage_embedded_nodes(enriched, staging_path, LABEL_DATABASE)
        rate, attempts, successes = emb.compute_failure_stats(database_df)
        summary.embedding_attempts[LABEL_DATABASE] = attempts
        summary.embedding_successes[LABEL_DATABASE] = successes
        summary.embedding_failure_rate_per_label[LABEL_DATABASE] = rate
        total_emb_attempts += attempts
        total_emb_successes += successes
        logger.info(
            "[dbxcarta] Database embeddings: attempts=%d successes=%d failure_rate=%.2f%%",
            attempts, successes, rate * 100,
        )

    # --- Transform: sample values ---
    # Runs before the threshold check so Value embeddings can be included in the
    # aggregate before any Neo4j write is attempted.
    sample_stats: sv.SampleStats | None = None
    value_node_df = None
    has_value_df = None
    if settings.dbxcarta_include_values:
        value_node_df, has_value_df, sample_stats = sv.sample(
            spark, columns_df, catalog, schema_list,
            settings.dbxcarta_sample_limit,
            settings.dbxcarta_sample_cardinality_threshold,
            settings.dbxcarta_stack_chunk_size,
        )
        for f in dc_fields(sample_stats):
            if f.name == "candidate_col_ids":
                continue
            val = getattr(sample_stats, f.name)
            if val is not None:
                summary.row_counts[f.name] = val
        logger.info(
            "[dbxcarta] sample values: candidates=%d sampled=%d value_nodes=%d",
            sample_stats.candidate_columns,
            sample_stats.sampled_columns,
            sample_stats.value_nodes,
        )

    if settings.dbxcarta_include_embeddings_values and value_node_df is not None:
        enriched = emb.add_embedding_column(
            value_node_df,
            _VALUE_EMBEDDING_TEXT_EXPR,
            settings.dbxcarta_embedding_endpoint,
            settings.dbxcarta_embedding_dimension,
            label=LABEL_VALUE,
        )
        value_node_df = _stage_embedded_nodes(enriched, staging_path, LABEL_VALUE)
        rate, attempts, successes = emb.compute_failure_stats(value_node_df)
        summary.embedding_attempts[LABEL_VALUE] = attempts
        summary.embedding_successes[LABEL_VALUE] = successes
        summary.embedding_failure_rate_per_label[LABEL_VALUE] = rate
        total_emb_attempts += attempts
        total_emb_successes += successes
        logger.info(
            "[dbxcarta] Value embeddings: attempts=%d successes=%d failure_rate=%.2f%%",
            attempts, successes, rate * 100,
        )

    # Per-label check first so the error message names the specific label.
    threshold = settings.dbxcarta_embedding_failure_threshold
    for lbl, per_label_rate in summary.embedding_failure_rate_per_label.items():
        if per_label_rate > threshold:
            raise RuntimeError(
                f"[dbxcarta] {lbl} embedding failure rate {per_label_rate:.2%} exceeds"
                f" threshold {threshold:.2%}; aborting before Neo4j write"
            )

    # Aggregate check catches the case where no single label trips but the
    # combined rate does (e.g. 2% Table + 8% Column aggregates above threshold).
    if total_emb_attempts > 0:
        aggregate_rate = (total_emb_attempts - total_emb_successes) / total_emb_attempts
        summary.embedding_failure_rate = aggregate_rate
        if aggregate_rate > threshold:
            raise RuntimeError(
                f"[dbxcarta] Aggregate embedding failure rate {aggregate_rate:.2%} exceeds"
                f" threshold {threshold:.2%}; aborting before Neo4j write"
            )

    # --- Load: nodes ---
    logger.info("[dbxcarta] writing nodes: Database (1)")
    database_write_df = database_df
    if "embedding_error" in database_write_df.columns:
        database_write_df = database_write_df.drop("embedding_error")
    write_nodes(database_write_df, neo4j, LABEL_DATABASE)

    # Stage 2: all writes land on Neo4j as a single partition. Relationship
    # writes lock both endpoint nodes, so parallel partitions cause lock
    # contention (best-practices Neo4j §1); node writes match for first green
    # run until throughput benchmarking in Stage 7.
    logger.info("[dbxcarta] writing nodes: Schema (%d)", summary.row_counts["schemas"])
    schema_write_df = schema_node_df
    for _c in ("catalog_name", "embedding_error"):
        if _c in schema_write_df.columns:
            schema_write_df = schema_write_df.drop(_c)
    write_nodes(schema_write_df.coalesce(1), neo4j, LABEL_SCHEMA)

    logger.info("[dbxcarta] writing nodes: Table (%d)", summary.row_counts["tables"])
    table_write_df = table_node_df
    if "embedding_error" in table_write_df.columns:
        table_write_df = table_write_df.drop("embedding_error")
    write_nodes(table_write_df.coalesce(1), neo4j, LABEL_TABLE)

    logger.info("[dbxcarta] writing nodes: Column (%d)", summary.row_counts["columns"])
    column_write_df = column_node_df
    for _c in ("table_schema", "table_name", "embedding_error"):
        if _c in column_write_df.columns:
            column_write_df = column_write_df.drop(_c)
    write_nodes(column_write_df.coalesce(1), neo4j, LABEL_COLUMN)

    if settings.dbxcarta_include_values and sample_stats is not None:
        _purge_stale_values(neo4j, sample_stats.candidate_col_ids)
        if sample_stats.value_nodes > 0:
            logger.info("[dbxcarta] writing nodes: Value (%d)", sample_stats.value_nodes)
            value_write_df = value_node_df
            if "embedding_error" in value_write_df.columns:
                value_write_df = value_write_df.drop("embedding_error")
            write_nodes(value_write_df.coalesce(1), neo4j, LABEL_VALUE)
            logger.info("[dbxcarta] writing relationships: HAS_VALUE (%d)", sample_stats.has_value_edges)
            write_relationship(has_value_df.coalesce(1), neo4j, REL_HAS_VALUE, LABEL_COLUMN, LABEL_VALUE)

    # --- Load: relationships ---
    logger.info("[dbxcarta] writing relationships: HAS_SCHEMA")
    write_relationship(has_schema_df.coalesce(1), neo4j, REL_HAS_SCHEMA, LABEL_DATABASE, LABEL_SCHEMA)

    logger.info("[dbxcarta] writing relationships: HAS_TABLE")
    write_relationship(has_table_df.coalesce(1), neo4j, REL_HAS_TABLE, LABEL_SCHEMA, LABEL_TABLE)

    logger.info("[dbxcarta] writing relationships: HAS_COLUMN")
    write_relationship(has_column_df.coalesce(1), neo4j, REL_HAS_COLUMN, LABEL_TABLE, LABEL_COLUMN)

    logger.info("[dbxcarta] writing relationships: REFERENCES (%d)", fk_references)
    if fk_references > 0:
        write_relationship(
            references_df.coalesce(1), neo4j, REL_REFERENCES, LABEL_COLUMN, LABEL_COLUMN,
        )

    # --- Cleanup ---
    schemata_df.unpersist()
    tables_df.unpersist()
    columns_df.unpersist()
    fk_pairs_df.unpersist()
    declared_df.unpersist()

    summary.neo4j_counts = _query_neo4j_counts(neo4j)
    logger.info("[dbxcarta] neo4j counts: %s", summary.neo4j_counts)


def _resolve_staging_path(settings: Settings) -> str:
    """Return the Delta staging root.

    If DBXCARTA_STAGING_PATH is set, use it verbatim. Otherwise derive a sibling
    "staging" directory under the same UC volume as DBXCARTA_SUMMARY_VOLUME by
    stripping the summary subdir. The derivation requires summary_volume to be
    of the form /Volumes/<catalog>/<schema>/<volume>/<subdir> (six segments);
    a bare volume root has no sibling position and is rejected so we never
    silently escape the volume into the schema directory.
    """
    configured = settings.dbxcarta_staging_path.strip()
    if configured:
        return configured.rstrip("/")
    summary_root = settings.dbxcarta_summary_volume.rstrip("/")
    parts = summary_root.split("/")
    # Expected shape: ['', 'Volumes', cat, schema, volume, subdir, ...]
    if len(parts) < 6 or parts[1] != "Volumes":
        raise RuntimeError(
            "[dbxcarta] cannot derive DBXCARTA_STAGING_PATH from"
            f" DBXCARTA_SUMMARY_VOLUME={settings.dbxcarta_summary_volume!r};"
            " expected /Volumes/<cat>/<schema>/<volume>/<subdir>."
            " Set DBXCARTA_STAGING_PATH explicitly."
        )
    parent = "/".join(parts[:-1])
    return f"{parent}/staging"


def _truncate_staging_root(staging_root: str) -> None:
    """Remove the staging root so each run starts clean.

    Overwrite-per-label would leave orphan subdirs when a label's embedding
    flag flips off between runs; truncating the whole root once per run keeps
    the staging volume faithful to the current config.
    """
    from databricks.sdk.runtime import dbutils

    try:
        dbutils.fs.rm(staging_root, recurse=True)
        logger.info("[dbxcarta] truncated staging root %s", staging_root)
    except Exception as exc:  # noqa: BLE001 — missing path is not fatal
        logger.info("[dbxcarta] staging root %s not present (%s)", staging_root, exc)


def _stage_embedded_nodes(df, staging_root: str, label: str):
    """Write df to <staging_root>/<label>_nodes as Delta and read back.

    overwriteSchema is enabled so future column additions (e.g. new transform
    outputs) don't require a manual drop of the staging table. The returned
    DataFrame is a fresh read off the staging table, so the failure-rate
    aggregation and the Neo4j write do not re-invoke ai_query.
    """
    out = f"{staging_root.rstrip('/')}/{label.lower()}_nodes"
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(out)
    )
    logger.info("[dbxcarta] staged %s nodes at %s", label, out)
    return df.sparkSession.read.format("delta").load(out)


def _preflight(spark, settings: Settings) -> None:
    catalog = settings.dbxcarta_catalog
    spark.sql(
        f"SELECT 1 FROM `{catalog}`.information_schema.schemata LIMIT 1"
    ).collect()

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

    table = settings.dbxcarta_summary_table
    quoted_table = ".".join(f"`{p}`" for p in table.split("."))
    # v5 schema — ALTER TABLE ADD COLUMN if upgrading from v4 (which lacks the
    # embedding_* columns). emit_delta uses mergeSchema=true to handle existing tables.
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
            embedding_failure_threshold DOUBLE
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
        try:
            spark.sql(
                f"SELECT ai_query('{endpoint}', 'preflight', failOnError => false)"
            ).collect()
        except Exception as exc:
            raise RuntimeError(
                f"[dbxcarta] preflight: embedding endpoint '{endpoint}' unreachable"
                f" or missing invoke permission: {exc}"
            ) from exc

    logger.info(
        "[dbxcarta] preflight passed: %s.information_schema accessible, volume and table ready",
        catalog,
    )


def _bootstrap_constraints(config: Neo4jConfig, settings: Settings) -> None:
    from neo4j import GraphDatabase
    from neo4j.exceptions import ClientError

    with GraphDatabase.driver(config.uri, auth=(config.username, config.password)) as driver:
        with driver.session() as session:
            for label in (LABEL_DATABASE, LABEL_SCHEMA, LABEL_TABLE, LABEL_COLUMN, LABEL_VALUE):
                try:
                    session.run(
                        f"CREATE CONSTRAINT {label.lower()}_id IF NOT EXISTS "
                        f"FOR (n:{label}) REQUIRE n.id IS UNIQUE"
                    )
                except ClientError as exc:
                    if "ConstraintAlreadyExists" not in (exc.code or ""):
                        raise
                    logger.info(
                        "[dbxcarta] constraint for %s already satisfied, skipping", label,
                    )

            session.run(
                f"CREATE INDEX {LABEL_COLUMN.lower()}_data_type IF NOT EXISTS "
                f"FOR (n:{LABEL_COLUMN}) ON (n.data_type)"
            )

            embedding_label_flags = [
                (settings.dbxcarta_include_embeddings_tables, LABEL_TABLE),
                (settings.dbxcarta_include_embeddings_columns, LABEL_COLUMN),
                (settings.dbxcarta_include_embeddings_values, LABEL_VALUE),
                (settings.dbxcarta_include_embeddings_schemas, LABEL_SCHEMA),
                (settings.dbxcarta_include_embeddings_databases, LABEL_DATABASE),
            ]
            dim = settings.dbxcarta_embedding_dimension
            for enabled, label in embedding_label_flags:
                if enabled:
                    session.run(
                        f"CREATE VECTOR INDEX {label.lower()}_embedding IF NOT EXISTS "
                        f"FOR (n:{label}) ON n.embedding "
                        f"OPTIONS {{indexConfig: {{`vector.dimensions`: {dim},"
                        f" `vector.similarity_function`: 'cosine'}}}}"
                    )

    logger.info("[dbxcarta] neo4j constraints and indexes bootstrapped")


def _purge_stale_values(config: Neo4jConfig, col_ids: list[str]) -> None:
    """Delete Value nodes attached to columns the current run will replace.

    Scoped to col_ids (post-schema-probe, pre-cardinality-filter) so columns
    that went all-NULL or rose above the cardinality threshold also lose their
    stale Values. Columns in skipped schemas are left untouched.
    """
    if not col_ids:
        return
    from neo4j import GraphDatabase

    with GraphDatabase.driver(config.uri, auth=(config.username, config.password)) as driver:
        with driver.session() as session:
            session.run(
                "MATCH (c:Column)-[:HAS_VALUE]->(v:Value) "
                "WHERE c.id IN $col_ids "
                "DETACH DELETE v",
                col_ids=col_ids,
            )
    logger.info("[dbxcarta] purged stale Values for %d columns", len(col_ids))


def _log_unresolved_fks(fk_skipped: int, fk_pairs_df, declared_df) -> None:
    if fk_skipped <= 0:
        return
    resolved_names = fk_pairs_df.select("fk_schema", "fk_name").distinct()
    skipped_rows = (
        declared_df
        .join(
            resolved_names,
            (declared_df.constraint_schema == resolved_names.fk_schema)
            & (declared_df.constraint_name == resolved_names.fk_name),
            "left_anti",
        )
        .collect()
    )
    for row in skipped_rows:
        logger.warning(
            "[dbxcarta] FK unresolved or out-of-scope (no target column pair in result, skipping): %s.%s",
            row["constraint_schema"],
            row["constraint_name"],
        )


def _query_neo4j_counts(config: Neo4jConfig) -> dict[str, int]:
    from neo4j import GraphDatabase

    counts: dict[str, int] = {}
    with GraphDatabase.driver(config.uri, auth=(config.username, config.password)) as driver:
        with driver.session() as session:
            for record in session.run(
                "MATCH (n) RETURN labels(n)[0] AS label, count(*) AS cnt"
            ):
                counts[record["label"]] = record["cnt"]
            for rel_type in (REL_HAS_SCHEMA, REL_HAS_TABLE, REL_HAS_COLUMN, REL_HAS_VALUE, REL_REFERENCES):
                result = session.run(f"MATCH ()-[r:{rel_type}]->() RETURN count(r) AS cnt")
                counts[rel_type] = result.single()["cnt"]
    return counts
