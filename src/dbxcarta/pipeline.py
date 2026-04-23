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
from dataclasses import dataclass
from typing import TYPE_CHECKING

from pydantic import field_validator
from pydantic_settings import BaseSettings

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

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
    REFERENCES_PROPERTIES,
    generate_id,
)
from dbxcarta.summary import RunSummary
from dbxcarta.writer import Neo4jConfig, write_nodes, write_relationship
from dbxcarta.fk_inference import infer_fk_pairs
import dbxcarta.embeddings as emb
import dbxcarta.sample_values as sv
import dbxcarta.schema_graph as sg

logger = logging.getLogger(__name__)

_IDENTIFIER_RE = re.compile(r"^[a-zA-Z0-9_.`\-]+$")

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

_EMBEDDING_TEXT_EXPRS = {
    LABEL_TABLE: _TABLE_EMBEDDING_TEXT_EXPR,
    LABEL_COLUMN: _COLUMN_EMBEDDING_TEXT_EXPR,
    LABEL_SCHEMA: _SCHEMA_EMBEDDING_TEXT_EXPR,
    LABEL_DATABASE: _DATABASE_EMBEDDING_TEXT_EXPR,
    LABEL_VALUE: _VALUE_EMBEDDING_TEXT_EXPR,
}


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
    # Materialize-once between ai_query and downstream actions.
    # If blank, derived at runtime as sibling to DBXCARTA_SUMMARY_VOLUME.
    dbxcarta_staging_path: str = ""
    # Neo4j Spark Connector batch.size (per best-practices Neo4j §2).
    dbxcarta_neo4j_batch_size: int = 20000
    # Re-embedding ledger (Stage 7.1): skip ai_query for unchanged nodes.
    # DBXCARTA_LEDGER_PATH defaults to <DBXCARTA_SUMMARY_VOLUME's parent>/ledger when unset.
    dbxcarta_ledger_enabled: bool = False
    dbxcarta_ledger_path: str = ""
    # Phase 3 metadata FK inference toggle; default on per worklog ablation matrix.
    dbxcarta_infer_metadata: bool = True

    @field_validator("dbxcarta_catalog", "dbxcarta_summary_table")
    @classmethod
    def _validate_identifier(cls, v: str) -> str:
        if not _IDENTIFIER_RE.match(v):
            raise ValueError(f"Invalid Databricks identifier: {v!r}")
        return v


# ---------------------------------------------------------------------------
# Container types for decomposed _run phases
# ---------------------------------------------------------------------------

@dataclass
class _ExtractResult:
    database_df: DataFrame
    schema_node_df: DataFrame
    table_node_df: DataFrame
    column_node_df: DataFrame
    has_schema_df: DataFrame
    has_table_df: DataFrame
    has_column_df: DataFrame
    references_df: DataFrame
    inferred_references_df: "DataFrame | None"
    schemata_df: DataFrame
    tables_df: DataFrame
    columns_df: DataFrame
    fk_pairs_df: DataFrame
    declared_df: DataFrame
    fk_references: int
    fk_inferred_metadata_references: int


@dataclass
class _ValueResult:
    value_node_df: DataFrame | None
    has_value_df: DataFrame | None
    sample_stats: sv.SampleStats | None


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def _run(spark, settings: Settings, schema_list: list[str], summary: RunSummary) -> None:
    from databricks.sdk.runtime import dbutils
    from neo4j import GraphDatabase

    scope = settings.databricks_secret_scope
    neo4j = Neo4jConfig(
        uri=dbutils.secrets.get(scope=scope, key="uri"),
        username=dbutils.secrets.get(scope=scope, key="username"),
        password=dbutils.secrets.get(scope=scope, key="password"),
        batch_size=settings.dbxcarta_neo4j_batch_size,
    )

    _preflight(spark, settings)
    staging_path = _resolve_staging_path(settings)
    ledger_path = _resolve_ledger_path(settings)
    _truncate_staging_root(staging_path)

    with GraphDatabase.driver(neo4j.uri, auth=(neo4j.username, neo4j.password)) as driver:
        _bootstrap_constraints(driver, settings)
        extract = _extract(spark, settings, schema_list, summary)
        _transform_embeddings(settings, extract, staging_path, ledger_path, summary)
        values = _transform_sample_values(spark, settings, schema_list, extract, staging_path, ledger_path, summary)
        _check_thresholds(settings, summary)
        _load(neo4j, driver, settings, extract, values, summary)
        summary.neo4j_counts = _query_neo4j_counts(driver)

    _cleanup(extract)
    logger.info("[dbxcarta] neo4j counts: %s", summary.neo4j_counts)


# ---------------------------------------------------------------------------
# Phase functions
# ---------------------------------------------------------------------------

def _extract(
    spark, settings: Settings, schema_list: list[str], summary: RunSummary,
) -> _ExtractResult:
    from pyspark.sql.functions import col

    catalog = settings.dbxcarta_catalog

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

    database_df = sg.build_database_node(spark, catalog)
    schema_node_df = sg.build_schema_nodes(schemata_df)
    table_node_df = sg.build_table_nodes(tables_df)
    column_node_df = sg.build_column_nodes(columns_df)
    has_schema_df = sg.build_has_schema_rel(schemata_df, catalog)
    has_table_df = sg.build_has_table_rel(tables_df)
    has_column_df = sg.build_has_column_rel(columns_df)
    references_df = sg.build_references_rel(fk_pairs_df)

    inferred_references_df, fk_inferred = _infer_metadata_references(
        spark, settings, schema_list, columns_df, fk_pairs_df, summary,
    )

    return _ExtractResult(
        database_df=database_df,
        schema_node_df=schema_node_df,
        table_node_df=table_node_df,
        column_node_df=column_node_df,
        has_schema_df=has_schema_df,
        has_table_df=has_table_df,
        has_column_df=has_column_df,
        references_df=references_df,
        inferred_references_df=inferred_references_df,
        schemata_df=schemata_df,
        tables_df=tables_df,
        columns_df=columns_df,
        fk_pairs_df=fk_pairs_df,
        declared_df=declared_df,
        fk_references=fk_references,
        fk_inferred_metadata_references=fk_inferred,
    )


def _infer_metadata_references(
    spark, settings: Settings, schema_list: list[str],
    columns_df, fk_pairs_df, summary: RunSummary,
) -> "tuple[DataFrame | None, int]":
    """Phase 3: run metadata FK inference and package as a REFERENCES DataFrame.

    Returns (None, 0) when DBXCARTA_INFER_METADATA is off so _load can skip
    the write cleanly. Counters land in summary.row_counts under the
    fk_inferred_metadata_* prefix; matches existing fk_* accounting style.
    """
    if not settings.dbxcarta_infer_metadata:
        return None, 0

    from pyspark.sql.functions import col

    catalog = settings.dbxcarta_catalog

    pk_rows_df = spark.sql(
        f"SELECT kcu.table_catalog, kcu.table_schema, kcu.table_name,"
        f"       kcu.column_name, tc.constraint_type, kcu.ordinal_position,"
        f"       kcu.constraint_name"
        f" FROM `{catalog}`.information_schema.table_constraints tc"
        f" JOIN `{catalog}`.information_schema.key_column_usage kcu"
        f"   ON tc.constraint_catalog = kcu.constraint_catalog"
        f"  AND tc.constraint_schema  = kcu.constraint_schema"
        f"  AND tc.constraint_name    = kcu.constraint_name"
        f" WHERE tc.constraint_type IN ('PRIMARY KEY', 'UNIQUE')"
    )
    if schema_list:
        pk_rows_df = pk_rows_df.filter(col("table_schema").isin(schema_list))

    columns = [
        {
            "catalog": r["table_catalog"],
            "schema": r["table_schema"],
            "table": r["table_name"],
            "column": r["column_name"],
            "data_type": r["data_type"],
            "comment": r["comment"],
        }
        for r in columns_df.collect()
    ]

    pk_cols, unique_leftmost, composite_pk_count = _partition_pk_constraints(
        pk_rows_df.collect(),
    )

    declared_pairs = {
        (
            generate_id(r["src_catalog"], r["src_schema"], r["src_table"], r["src_column"]),
            generate_id(r["tgt_catalog"], r["tgt_schema"], r["tgt_table"], r["tgt_column"]),
        )
        for r in fk_pairs_df.collect()
    }

    rows, counters = infer_fk_pairs(
        columns, pk_cols, unique_leftmost, declared_pairs,
    )

    for name, val in counters.items():
        summary.row_counts[f"fk_inferred_metadata_{name}"] = val
    summary.row_counts["fk_inferred_metadata_composite_pk_skipped"] = composite_pk_count

    logger.info(
        "[dbxcarta] metadata inference: candidates=%d accepted=%d"
        " composite_pks_skipped=%d",
        counters["candidates"], counters["accepted"], composite_pk_count,
    )

    if not rows:
        return None, 0

    inferred_df = sg.build_inferred_metadata_references_rel(spark, rows)
    return inferred_df, len(rows)


def _partition_pk_constraints(
    rows,
) -> tuple[dict[str, set[str]], dict[str, set[str]], int]:
    """Group raw constraint rows into (pk_cols_single, unique_leftmost, composite_count).

    Composite PKs (multi-column) are dropped from pk_cols_single — a single
    column that is only unique in combination with others is not FK-target
    material. Their count feeds the run summary so the evaluator can judge
    coverage loss (worklog: composite-key non-goal).
    """
    pk_by_constraint: dict[tuple, list[str]] = {}
    pk_table_by_constraint: dict[tuple, str] = {}
    unique_leftmost: dict[str, set[str]] = {}

    for r in rows:
        table_key = generate_id(r["table_catalog"], r["table_schema"], r["table_name"])
        constraint_key = (r["table_catalog"], r["table_schema"], r["constraint_name"])
        if r["constraint_type"] == "PRIMARY KEY":
            pk_by_constraint.setdefault(constraint_key, []).append(r["column_name"])
            pk_table_by_constraint[constraint_key] = table_key
        elif r["constraint_type"] == "UNIQUE" and r["ordinal_position"] == 1:
            unique_leftmost.setdefault(table_key, set()).add(r["column_name"])

    pk_cols: dict[str, set[str]] = {}
    composite_count = 0
    for ckey, cols in pk_by_constraint.items():
        table_key = pk_table_by_constraint[ckey]
        if len(cols) == 1:
            pk_cols.setdefault(table_key, set()).add(cols[0])
        else:
            composite_count += 1
    return pk_cols, unique_leftmost, composite_count


def _transform_embeddings(
    settings: Settings, extract: _ExtractResult,
    staging_path: str, ledger_path: str, summary: RunSummary,
) -> None:
    """Enrich node DataFrames with embeddings in-place on `extract`.

    Materialize-once: each enabled label is written to a Delta staging table
    so the failure-rate aggregation and the Neo4j node write both consume the
    staged data — ai_query is invoked exactly once (best-practices Spark §4).
    Threshold is checked after all labels are processed.
    """
    enabled = {
        LABEL_TABLE: settings.dbxcarta_include_embeddings_tables,
        LABEL_COLUMN: settings.dbxcarta_include_embeddings_columns,
        LABEL_SCHEMA: settings.dbxcarta_include_embeddings_schemas,
        LABEL_DATABASE: settings.dbxcarta_include_embeddings_databases,
    }
    node_dfs = {
        LABEL_TABLE: extract.table_node_df,
        LABEL_COLUMN: extract.column_node_df,
        LABEL_SCHEMA: extract.schema_node_df,
        LABEL_DATABASE: extract.database_df,
    }

    for label in (LABEL_TABLE, LABEL_COLUMN, LABEL_SCHEMA, LABEL_DATABASE):
        if not enabled[label]:
            continue
        node_dfs[label] = _embed_and_stage(
            node_dfs[label], _EMBEDDING_TEXT_EXPRS[label], label,
            settings, staging_path, ledger_path, summary,
        )

    extract.table_node_df = node_dfs[LABEL_TABLE]
    extract.column_node_df = node_dfs[LABEL_COLUMN]
    extract.schema_node_df = node_dfs[LABEL_SCHEMA]
    extract.database_df = node_dfs[LABEL_DATABASE]


def _transform_sample_values(
    spark, settings: Settings, schema_list: list[str],
    extract: _ExtractResult, staging_path: str, ledger_path: str, summary: RunSummary,
) -> _ValueResult:
    """Sample distinct values and optionally embed the Value nodes."""
    if not settings.dbxcarta_include_values:
        if settings.dbxcarta_include_embeddings_values:
            logger.warning(
                "[dbxcarta] DBXCARTA_INCLUDE_EMBEDDINGS_VALUES=true but"
                " DBXCARTA_INCLUDE_VALUES=false; Value embeddings will be skipped."
                " Set DBXCARTA_INCLUDE_VALUES=true to enable."
            )
        return _ValueResult(value_node_df=None, has_value_df=None, sample_stats=None)

    value_node_df, has_value_df, sample_stats = sv.sample(
        spark, extract.columns_df, settings.dbxcarta_catalog, schema_list,
        settings.dbxcarta_sample_limit,
        settings.dbxcarta_sample_cardinality_threshold,
        settings.dbxcarta_stack_chunk_size,
    )
    for name in (
        "candidate_columns", "sampled_columns", "skipped_columns", "skipped_schemas",
        "cardinality_failed_tables", "cardinality_wall_clock_ms", "sample_wall_clock_ms",
        "value_nodes", "has_value_edges",
        "cardinality_min", "cardinality_p25", "cardinality_p50",
        "cardinality_p75", "cardinality_p95", "cardinality_max",
    ):
        val = getattr(sample_stats, name)
        if val is not None:
            summary.row_counts[name] = val
    logger.info(
        "[dbxcarta] sample values: candidates=%d sampled=%d value_nodes=%d",
        sample_stats.candidate_columns,
        sample_stats.sampled_columns,
        sample_stats.value_nodes,
    )

    if settings.dbxcarta_include_embeddings_values and sample_stats.value_nodes > 0:
        value_node_df = _embed_and_stage(
            value_node_df, _EMBEDDING_TEXT_EXPRS[LABEL_VALUE], LABEL_VALUE,
            settings, staging_path, ledger_path, summary,
        )

    return _ValueResult(
        value_node_df=value_node_df,
        has_value_df=has_value_df,
        sample_stats=sample_stats,
    )


def _check_thresholds(settings: Settings, summary: RunSummary) -> None:
    """Raise RuntimeError if any per-label or aggregate embedding failure rate exceeds the threshold."""
    threshold = settings.dbxcarta_embedding_failure_threshold
    for lbl, rate in summary.embedding_failure_rate_per_label.items():
        if rate > threshold:
            raise RuntimeError(
                f"[dbxcarta] {lbl} embedding failure rate {rate:.2%} exceeds"
                f" threshold {threshold:.2%}; aborting before Neo4j write"
            )

    total_attempts = sum(summary.embedding_attempts.values())
    total_successes = sum(summary.embedding_successes.values())
    if total_attempts > 0:
        aggregate_rate = (total_attempts - total_successes) / total_attempts
        summary.embedding_failure_rate = aggregate_rate
        if aggregate_rate > threshold:
            raise RuntimeError(
                f"[dbxcarta] Aggregate embedding failure rate {aggregate_rate:.2%} exceeds"
                f" threshold {threshold:.2%}; aborting before Neo4j write"
            )


def _load(
    neo4j: Neo4jConfig,
    driver,
    settings: Settings,
    extract: _ExtractResult,
    values: _ValueResult,
    summary: RunSummary,
) -> None:
    """Node writes omit coalesce(1); connector parallelizes at batch_size (Neo4j §2).
    Relationship writes keep coalesce(1) to prevent lock contention (Neo4j §1)."""
    # Always purge regardless of INCLUDE_VALUES — UC is the only source of truth (Project §1).
    candidate_col_ids = sv.get_candidate_col_ids(extract.columns_df)
    _purge_stale_values(driver, candidate_col_ids)

    logger.info("[dbxcarta] writing nodes: Database (1)")
    write_nodes(_drop_cols(extract.database_df, "embedding_error"), neo4j, LABEL_DATABASE)

    logger.info("[dbxcarta] writing nodes: Schema (%d)", summary.row_counts["schemas"])
    write_nodes(_drop_cols(extract.schema_node_df, "catalog_name", "embedding_error"), neo4j, LABEL_SCHEMA)

    logger.info("[dbxcarta] writing nodes: Table (%d)", summary.row_counts["tables"])
    write_nodes(_drop_cols(extract.table_node_df, "embedding_error"), neo4j, LABEL_TABLE)

    logger.info("[dbxcarta] writing nodes: Column (%d)", summary.row_counts["columns"])
    write_nodes(_drop_cols(extract.column_node_df, "table_schema", "table_name", "embedding_error"), neo4j, LABEL_COLUMN)

    if values.sample_stats is not None and values.sample_stats.value_nodes > 0:
        logger.info("[dbxcarta] writing nodes: Value (%d)", values.sample_stats.value_nodes)
        write_nodes(_drop_cols(values.value_node_df, "embedding_error"), neo4j, LABEL_VALUE)
        logger.info("[dbxcarta] writing relationships: HAS_VALUE (%d)", values.sample_stats.has_value_edges)
        write_relationship(
            values.has_value_df.coalesce(1), neo4j, REL_HAS_VALUE, LABEL_COLUMN, LABEL_VALUE,
        )

    logger.info("[dbxcarta] writing relationships: HAS_SCHEMA")
    write_relationship(
        extract.has_schema_df.coalesce(1), neo4j, REL_HAS_SCHEMA, LABEL_DATABASE, LABEL_SCHEMA,
    )

    logger.info("[dbxcarta] writing relationships: HAS_TABLE")
    write_relationship(
        extract.has_table_df.coalesce(1), neo4j, REL_HAS_TABLE, LABEL_SCHEMA, LABEL_TABLE,
    )

    logger.info("[dbxcarta] writing relationships: HAS_COLUMN")
    write_relationship(
        extract.has_column_df.coalesce(1), neo4j, REL_HAS_COLUMN, LABEL_TABLE, LABEL_COLUMN,
    )

    logger.info("[dbxcarta] writing relationships: REFERENCES (%d)", extract.fk_references)
    if extract.fk_references > 0:
        write_relationship(
            extract.references_df.coalesce(1), neo4j, REL_REFERENCES, LABEL_COLUMN, LABEL_COLUMN,
            properties=REFERENCES_PROPERTIES,
        )

    if extract.fk_inferred_metadata_references > 0 and extract.inferred_references_df is not None:
        logger.info(
            "[dbxcarta] writing relationships: REFERENCES inferred_metadata (%d)",
            extract.fk_inferred_metadata_references,
        )
        write_relationship(
            extract.inferred_references_df.coalesce(1), neo4j,
            REL_REFERENCES, LABEL_COLUMN, LABEL_COLUMN,
            properties=REFERENCES_PROPERTIES,
        )


def _cleanup(extract: _ExtractResult) -> None:
    extract.schemata_df.unpersist()
    extract.tables_df.unpersist()
    extract.columns_df.unpersist()
    extract.fk_pairs_df.unpersist()
    extract.declared_df.unpersist()


# ---------------------------------------------------------------------------
# DataFrame helpers
# ---------------------------------------------------------------------------

def _drop_cols(df, *names: str) -> DataFrame:
    """Drop named columns from df if they exist; return the (possibly modified) df."""
    for name in names:
        if name in df.columns:
            df = df.drop(name)
    return df


# ---------------------------------------------------------------------------
# Embedding helper
# ---------------------------------------------------------------------------

def _embed_and_stage(
    df, text_expr: str, label: str,
    settings: Settings, staging_path: str, ledger_path: str, summary: RunSummary,
) -> DataFrame:
    """Embed df, stage to Delta once, compute failure stats into summary.

    When DBXCARTA_LEDGER_ENABLED, rows whose embedding_text_hash and model
    already exist in the per-label ledger are served from the ledger (no
    ai_query call). Only misses call the endpoint; the ledger is then upserted
    with the newly-computed vectors (excluding error rows).
    """
    from pyspark.sql.functions import col, expr as spark_expr, lit, sha2
    from pyspark.sql.types import ArrayType, DoubleType

    endpoint = settings.dbxcarta_embedding_endpoint
    dimension = settings.dbxcarta_embedding_dimension

    if settings.dbxcarta_ledger_enabled:
        spark = df.sparkSession
        ledger_df = _read_ledger(spark, ledger_path, label)

        if ledger_df is not None:
            df_hashed = df.withColumn("_curr_hash", sha2(spark_expr(text_expr), 256))
            hits_df, misses_df = _split_by_ledger(df_hashed, ledger_df, endpoint)
            hit_count = hits_df.count()
            summary.embedding_ledger_hits[label] = hit_count

            # Hit branch: synthesize the five embedding columns from the ledger row.
            # Cast embedding to ARRAY<DOUBLE> to match the type add_embedding_column produces.
            hit_final = hits_df.select(
                *[col(c) for c in df.columns],
                col("_curr_hash").alias("embedding_text_hash"),
                col("_led_embedding").cast(ArrayType(DoubleType())).alias("embedding"),
                lit(None).cast("string").alias("embedding_error"),
                col("_led_model").alias("embedding_model"),
                col("_led_embedded_at").alias("embedded_at"),
            )

            # Miss branch: call ai_query only on rows whose hash or model changed.
            embedded_misses = emb.add_embedding_column(
                misses_df, text_expr, endpoint, dimension, label=label,
            )

            enriched = hit_final.unionByName(embedded_misses)
        else:
            # Ledger doesn't exist yet — treat everything as a miss.
            summary.embedding_ledger_hits[label] = 0
            enriched = emb.add_embedding_column(df, text_expr, endpoint, dimension, label=label)
    else:
        enriched = emb.add_embedding_column(df, text_expr, endpoint, dimension, label=label)

    staged = _stage_embedded_nodes(enriched, staging_path, label)
    rate, attempts, successes = emb.compute_failure_stats(staged)
    summary.embedding_attempts[label] = attempts
    summary.embedding_successes[label] = successes
    summary.embedding_failure_rate_per_label[label] = rate

    if settings.dbxcarta_ledger_enabled:
        _upsert_ledger(staged, ledger_path, label)

    logger.info(
        "[dbxcarta] %s embeddings: attempts=%d successes=%d failure_rate=%.2f%% ledger_hits=%d",
        label, attempts, successes, rate * 100,
        summary.embedding_ledger_hits.get(label, 0),
    )
    return staged


# ---------------------------------------------------------------------------
# Path / staging helpers
# ---------------------------------------------------------------------------

def _parse_volume_path(path: str) -> list[str]:
    """Validate a /Volumes path and return its parts (after lstrip).

    Requires at least /Volumes/<cat>/<schema>/<vol>/<subdir> (5 segments).
    Raises RuntimeError for bare volume roots (/Volumes/cat/schema/vol) so
    neither preflight nor _resolve_staging_path silently accepts a path
    that will fail at runtime (Project §3).
    """
    parts = path.lstrip("/").split("/")
    if len(parts) < 5 or parts[0] != "Volumes":
        raise RuntimeError(
            f"[dbxcarta] volume path must be at least"
            f" /Volumes/<cat>/<schema>/<vol>/<subdir>, got {path!r}."
            f" Set DBXCARTA_SUMMARY_VOLUME to a path with a subdirectory"
            f" under the volume root (e.g. /Volumes/cat/schema/vol/dbxcarta)."
        )
    return parts


def _resolve_staging_path(settings: Settings) -> str:
    """Return the Delta staging root.

    If DBXCARTA_STAGING_PATH is set, use it verbatim. Otherwise derive a sibling
    "staging" directory under the same UC volume as DBXCARTA_SUMMARY_VOLUME by
    stripping the summary subdir.
    """
    configured = settings.dbxcarta_staging_path.strip()
    if configured:
        return configured.rstrip("/")
    summary_root = settings.dbxcarta_summary_volume.rstrip("/")
    parts = _parse_volume_path(summary_root)
    parent = "/" + "/".join(parts[:-1])
    return f"{parent}/staging"


def _resolve_ledger_path(settings: Settings) -> str:
    """Return the Delta ledger root.

    If DBXCARTA_LEDGER_PATH is set, use it verbatim. Otherwise derive a sibling
    "ledger" directory under the same UC volume as DBXCARTA_SUMMARY_VOLUME.
    The ledger is never truncated between runs — it persists as a durable cache.
    """
    configured = settings.dbxcarta_ledger_path.strip()
    if configured:
        return configured.rstrip("/")
    summary_root = settings.dbxcarta_summary_volume.rstrip("/")
    parts = _parse_volume_path(summary_root)
    parent = "/" + "/".join(parts[:-1])
    return f"{parent}/ledger"


def _read_ledger(spark, ledger_root: str, label: str) -> "DataFrame | None":
    """Return the ledger DataFrame for label, or None if it doesn't exist yet."""
    ledger_path = f"{ledger_root.rstrip('/')}/{label.lower()}"
    try:
        return spark.read.format("delta").load(ledger_path)
    except Exception:  # noqa: BLE001 — missing table is expected on first run
        return None


def _split_by_ledger(df_hashed, ledger_df, endpoint: str) -> tuple["DataFrame", "DataFrame"]:
    """Split df_hashed into (hits_df, misses_df) against the ledger.

    A hit requires: the ledger has a row with the same id, the same
    embedding_model as the current endpoint, and a matching embedding_text_hash.
    Hits carry five _led_* columns from the ledger row. Misses have those
    columns dropped so the result is schema-compatible with the original df.
    """
    from pyspark.sql.functions import col

    ledger_filtered = (
        ledger_df
        .filter(col("embedding_model") == endpoint)
        .select(
            col("id").alias("_led_id"),
            col("embedding_text_hash").alias("_led_hash"),
            col("embedding").alias("_led_embedding"),
            col("embedding_model").alias("_led_model"),
            col("embedded_at").alias("_led_embedded_at"),
        )
    )

    joined = df_hashed.join(
        ledger_filtered, df_hashed["id"] == ledger_filtered["_led_id"], "left",
    )
    hit_cond = col("_led_id").isNotNull() & (col("_curr_hash") == col("_led_hash"))

    ledger_cols = ["_curr_hash", "_led_id", "_led_hash", "_led_embedding", "_led_model", "_led_embedded_at"]
    hits_df = joined.filter(hit_cond)
    misses_df = joined.filter(~hit_cond).drop(*ledger_cols)

    return hits_df, misses_df


def _upsert_ledger(staged_df, ledger_root: str, label: str) -> None:
    """Upsert newly-embedded rows (excluding errors) into the per-label ledger.

    Merge key: (id, embedding_model). Rows where embedding_error is non-null
    are excluded so failed rows are re-attempted on every subsequent run.
    Schema: id STRING, embedding_text_hash STRING, embedding ARRAY<DOUBLE>,
    embedding_model STRING, embedded_at TIMESTAMP.
    """
    from delta.tables import DeltaTable
    from pyspark.sql.functions import col

    ledger_path = f"{ledger_root.rstrip('/')}/{label.lower()}"
    spark = staged_df.sparkSession

    # Deduplicate on merge key — Delta MERGE requires at most one source row per
    # target row; while node IDs should be unique in practice, guard explicitly.
    rows_to_upsert = (
        staged_df
        .filter(col("embedding_error").isNull())
        .select("id", "embedding_text_hash", "embedding", "embedding_model", "embedded_at")
        .dropDuplicates(["id", "embedding_model"])
    )

    # Use forPath in a try/except rather than isDeltaTable to avoid AnalysisException
    # edge cases observed with isDeltaTable on /Volumes paths (Delta docs community reports).
    # Only the forPath call is wrapped — a merge failure propagates as-is.
    try:
        existing = DeltaTable.forPath(spark, ledger_path)
    except Exception:  # noqa: BLE001 — path doesn't exist on first run
        (
            rows_to_upsert.write
            .format("delta")
            .mode("overwrite")
            .save(ledger_path)
        )
        logger.info("[dbxcarta] created ledger for %s at %s", label, ledger_path)
        return

    (
        existing.alias("existing")
        .merge(
            rows_to_upsert.alias("updates"),
            "existing.id = updates.id AND existing.embedding_model = updates.embedding_model",
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    logger.info("[dbxcarta] upserted ledger for %s at %s", label, ledger_path)


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

    overwriteSchema is enabled so future column additions don't require a
    manual drop of the staging table. The returned DataFrame is a fresh read
    off the staging table, so the failure-rate aggregation and the Neo4j write
    do not re-invoke ai_query.
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

    parts = _parse_volume_path(settings.dbxcarta_summary_volume)
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
        try:
            rows = spark.sql(
                f"SELECT ai_query('{endpoint}', 'preflight', failOnError => false) AS response"
            ).collect()
        except Exception as exc:
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


# ---------------------------------------------------------------------------
# Neo4j helpers
# ---------------------------------------------------------------------------

def _bootstrap_constraints(driver, settings: Settings) -> None:
    from neo4j.exceptions import ClientError

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


def _purge_stale_values(driver, col_ids: list[str]) -> None:
    """Delete Value nodes attached to columns the current run will replace.

    Scoped to col_ids (post-schema-probe, pre-cardinality-filter) so columns
    that went all-NULL or rose above the cardinality threshold also lose their
    stale Values. Columns in skipped schemas are left untouched.
    """
    if not col_ids:
        return

    with driver.session() as session:
        session.run(
            "MATCH (c:Column)-[:HAS_VALUE]->(v:Value) "
            "WHERE c.id IN $col_ids "
            "DETACH DELETE v",
            col_ids=col_ids,
        )
    logger.info("[dbxcarta] purged stale Values for %d columns", len(col_ids))


def _query_neo4j_counts(driver) -> dict[str, int]:
    counts: dict[str, int] = {}
    with driver.session() as session:
        for label in (LABEL_DATABASE, LABEL_SCHEMA, LABEL_TABLE, LABEL_COLUMN, LABEL_VALUE):
            result = session.run(f"MATCH (n:{label}) RETURN count(n) AS cnt")
            counts[label] = result.single()["cnt"]
        for rel_type in (REL_HAS_SCHEMA, REL_HAS_TABLE, REL_HAS_COLUMN, REL_HAS_VALUE, REL_REFERENCES):
            result = session.run(f"MATCH ()-[r:{rel_type}]->() RETURN count(r) AS cnt")
            counts[rel_type] = result.single()["cnt"]
    return counts


# ---------------------------------------------------------------------------
# FK logging
# ---------------------------------------------------------------------------

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
