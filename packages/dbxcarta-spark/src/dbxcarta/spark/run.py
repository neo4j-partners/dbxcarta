"""DBxCarta ingestion orchestrator.

Thin coordinator for the ingest run. Its job is limited to:
  1. Construct Settings (fails loudly at boundary via cross-field validators).
  2. Run preflight (fails before any destructive action).
  3. Call extract → transform (embed, sample) → FK discovery → load.
  4. Emit the RunSummary.

The module intentionally keeps domain work in focused ingest modules under
`fk`, `transform`, and `load`. Pipeline functions here wire those modules
together and update run-level counters so the Databricks task has one clear
entry point.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import TYPE_CHECKING

import dbxcarta.spark.ingest.transform.embeddings as emb
import dbxcarta.spark.ingest.transform.sample_values as sv
from dbxcarta.core.contract import CONTRACT_VERSION, NodeLabel, REFERENCES_PROPERTIES, RelType
from dbxcarta.spark.ingest.extract import ExtractResult, extract
from dbxcarta.spark.ingest.fk.discovery import FKDiscoveryResult, run_fk_discovery
from dbxcarta.spark.ingest.transform.ledger import read_ledger, split_by_ledger, upsert_ledger
from dbxcarta.spark.ingest.load.neo4j_io import (
    bootstrap_constraints,
    purge_stale_values,
    query_counts,
    write_node,
    write_rel,
)
from dbxcarta.spark.ingest.preflight import preflight
from dbxcarta.spark.settings import SparkIngestSettings
from dbxcarta.spark.ingest.transform.staging import (
    resolve_ledger_path,
    resolve_staging_path,
    stage_embedded_nodes,
    truncate_staging_root,
)
from dbxcarta.spark.ingest.summary import EmbeddingCounts, RunSummary, SampleValueCounts
from dbxcarta.spark.ingest.load.writer import Neo4jConfig

if TYPE_CHECKING:
    from neo4j import Driver
    from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)


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

_EMBEDDING_TEXT_EXPRS: dict[NodeLabel, str] = {
    NodeLabel.TABLE: _TABLE_EMBEDDING_TEXT_EXPR,
    NodeLabel.COLUMN: _COLUMN_EMBEDDING_TEXT_EXPR,
    NodeLabel.SCHEMA: _SCHEMA_EMBEDDING_TEXT_EXPR,
    NodeLabel.DATABASE: _DATABASE_EMBEDDING_TEXT_EXPR,
    NodeLabel.VALUE: _VALUE_EMBEDDING_TEXT_EXPR,
}


@dataclass
class ValueResult:
    """Sample-values output passed from transform into FK discovery and load.

    The pipeline uses `None` instead of an empty ValueResult when sampling is
    disabled, so downstream branches can cleanly skip Value nodes and HAS_VALUE
    relationships without inspecting Spark DataFrames.
    """

    value_node_df: "DataFrame"
    has_value_df: "DataFrame"
    sample_stats: sv.SampleStats


def run_dbxcarta(
    *,
    settings: SparkIngestSettings | None = None,
    spark: "SparkSession | None" = None,
) -> None:
    """Run a complete ingest.

    When called with no arguments, this remains the Databricks wheel entrypoint:
    settings are loaded from environment variables and the active Spark session
    is resolved lazily. Library consumers can pass explicit settings and, when
    they already own Spark setup, an existing Spark session.
    """
    resolved_settings = settings if settings is not None else SparkIngestSettings()  # type: ignore[call-arg]

    if spark is None:
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.getOrCreate()
    run_id = os.environ.get("DATABRICKS_JOB_RUN_ID", "local")
    schema_list = [
        s.strip()
        for s in resolved_settings.dbxcarta_schemas.split(",")
        if s.strip()
    ]

    summary = _build_summary(run_id, resolved_settings, schema_list)
    primary_error: BaseException | None = None

    try:
        _run(spark, resolved_settings, schema_list, summary)
        summary.finish(status="success")
    except Exception as exc:
        primary_error = exc
        # Top-level catch-all is deliberate: the purpose here is to record
        # _any_ failure into RunSummary.error (stdout + JSON + Delta) before
        # re-raising. Narrowing would silently lose novel failure modes. The
        # exception propagates via `raise` so the Databricks job still fails.
        summary.finish(status="failure", error=str(exc))
        raise
    finally:
        _emit_summary(
            summary,
            spark,
            resolved_settings.dbxcarta_summary_volume,
            resolved_settings.dbxcarta_summary_table,
            primary_error=primary_error,
        )


def _build_summary(run_id: str, settings: SparkIngestSettings, schema_list: list[str]) -> RunSummary:
    """Create the run summary shell before any Spark or Neo4j work begins.

    Embedding flags are copied from Settings at startup so the emitted summary
    records the requested configuration even if no rows are eligible for a
    particular label.
    """
    flags: dict[NodeLabel, bool] = {
        NodeLabel.TABLE: settings.dbxcarta_include_embeddings_tables,
        NodeLabel.COLUMN: settings.dbxcarta_include_embeddings_columns,
        NodeLabel.VALUE: settings.dbxcarta_include_embeddings_values,
        NodeLabel.SCHEMA: settings.dbxcarta_include_embeddings_schemas,
        NodeLabel.DATABASE: settings.dbxcarta_include_embeddings_databases,
    }
    return RunSummary(
        run_id=run_id,
        job_name="dbxcarta",
        contract_version=CONTRACT_VERSION,
        catalog=settings.dbxcarta_catalog,
        schemas=schema_list,
        embeddings=EmbeddingCounts(
            model=settings.dbxcarta_embedding_endpoint,
            failure_threshold=settings.dbxcarta_embedding_failure_threshold,
            flags=flags,
        ),
    )


def _emit_summary(
    summary: RunSummary,
    spark: "SparkSession",
    volume_path: str,
    table_name: str,
    *,
    primary_error: BaseException | None,
) -> None:
    """Emit the summary without masking an existing run failure."""
    try:
        summary.emit(spark, volume_path, table_name)
    except Exception:
        if primary_error is not None:
            logger.exception(
                "[dbxcarta] failed to emit run summary after run failure"
            )
            return
        raise


def _run(
    spark: "SparkSession",
    settings: SparkIngestSettings,
    schema_list: list[str],
    summary: RunSummary,
) -> None:
    """Execute the ingest workflow after settings and summary are initialized.

    This method owns resource ordering: preflight and staging cleanup happen
    before the Neo4j driver opens, graph constraints are bootstrapped before
    writes, and cached extraction DataFrames are unpersisted after verification.
    """
    from databricks.sdk.runtime import dbutils
    from neo4j import GraphDatabase

    extract_result: ExtractResult | None = None
    scope = settings.databricks_secret_scope
    neo4j = Neo4jConfig(
        uri=dbutils.secrets.get(scope=scope, key="NEO4J_URI"),
        username=dbutils.secrets.get(scope=scope, key="NEO4J_USERNAME"),
        password=dbutils.secrets.get(scope=scope, key="NEO4J_PASSWORD"),
        batch_size=settings.dbxcarta_neo4j_batch_size,
    )

    preflight(spark, settings)
    staging_path = resolve_staging_path(settings)
    ledger_path = resolve_ledger_path(settings)
    truncate_staging_root(staging_path)

    run_error: BaseException | None = None
    try:
        with GraphDatabase.driver(neo4j.uri, auth=(neo4j.username, neo4j.password)) as driver:
            bootstrap_constraints(driver, settings)

            extract_result = extract(spark, settings, schema_list, summary)
            _transform_embeddings(settings, extract_result, staging_path, ledger_path, summary)
            values = _transform_sample_values(
                spark, settings, schema_list, extract_result,
                staging_path, ledger_path, summary,
            )
            _check_thresholds(settings, summary)

            fk_result = run_fk_discovery(
                spark, settings, schema_list, extract_result,
                values.sample_stats if values else None,
                values.value_node_df if values else None,
                values.has_value_df if values else None,
                summary,
            )

            _load(neo4j, driver, extract_result, fk_result, values, summary)
            summary.neo4j_counts = query_counts(driver)
            _verify(driver, settings, summary)
    except Exception as exc:
        run_error = exc
        raise
    finally:
        if extract_result is not None:
            try:
                extract_result.unpersist_cached()
            except Exception:
                if run_error is not None:
                    logger.exception(
                        "[dbxcarta] failed to unpersist cached extraction DataFrames"
                    )
                else:
                    raise
    logger.info("[dbxcarta] neo4j counts: %s", summary.neo4j_counts)


def _verify(driver: "Driver", settings: SparkIngestSettings, summary: RunSummary) -> None:
    """Final pipeline step: re-run dbxcarta.core.verify against the run summary just
    built. Records the outcome on summary.verify (durable in JSON + Delta).
    Warn-only by default; raises when settings.dbxcarta_verify_gate is True.

    The summary dict is materialised here with status forced to 'success' — the
    enclosing run_dbxcarta() has not yet called summary.finish(), but verify's
    summary-shape check expects a successful run. If we raise, the outer
    catch-all in run_dbxcarta() records the failure via summary.finish(failure).
    """
    from databricks.sdk import WorkspaceClient
    from dbxcarta.spark.ingest.summary import VerifyResult
    from dbxcarta.core.verify import verify_run

    summary_dict = summary.to_dict()
    summary_dict["status"] = "success"
    ws = WorkspaceClient()
    report = verify_run(
        summary=summary_dict,
        neo4j_driver=driver,
        ws=ws,
        warehouse_id=settings.databricks_warehouse_id,
        catalog=settings.dbxcarta_catalog,
        sample_limit=settings.dbxcarta_sample_limit,
    )
    summary.verify = VerifyResult(
        ok=report.ok,
        violations=[{"code": v.code, "message": v.message} for v in report.violations],
    )
    if report.ok:
        logger.info("[dbxcarta] verify: OK (0 violations)")
        return
    if settings.dbxcarta_verify_gate:
        raise RuntimeError(
            f"[dbxcarta] verify gate failed: {len(report.violations)} violation(s)\n"
            + report.format()
        )
    logger.warning(
        "[dbxcarta] verify reported %d violation(s) (warn-only; set"
        " DBXCARTA_VERIFY_GATE=true to gate):\n%s",
        len(report.violations), report.format(),
    )


def _transform_embeddings(
    settings: SparkIngestSettings, extract_result: ExtractResult,
    staging_path: str, ledger_path: str, summary: RunSummary,
) -> None:
    """Enrich node DataFrames with embeddings in-place on `extract_result`.

    Materialize-once: each enabled label is written to a Delta staging table
    so failure-rate aggregation and the Neo4j write consume the staged data —
    ai_query is invoked exactly once. Threshold is checked after all labels.
    """
    enabled: dict[NodeLabel, bool] = {
        NodeLabel.TABLE: settings.dbxcarta_include_embeddings_tables,
        NodeLabel.COLUMN: settings.dbxcarta_include_embeddings_columns,
        NodeLabel.SCHEMA: settings.dbxcarta_include_embeddings_schemas,
        NodeLabel.DATABASE: settings.dbxcarta_include_embeddings_databases,
    }
    node_dfs: dict[NodeLabel, "DataFrame"] = {
        NodeLabel.TABLE: extract_result.table_node_df,
        NodeLabel.COLUMN: extract_result.column_node_df,
        NodeLabel.SCHEMA: extract_result.schema_node_df,
        NodeLabel.DATABASE: extract_result.database_df,
    }

    for label in (NodeLabel.TABLE, NodeLabel.COLUMN, NodeLabel.SCHEMA, NodeLabel.DATABASE):
        if not enabled[label]:
            continue
        node_dfs[label] = _embed_and_stage(
            node_dfs[label], _EMBEDDING_TEXT_EXPRS[label], label,
            settings, staging_path, ledger_path, summary,
        )

    extract_result.table_node_df = node_dfs[NodeLabel.TABLE]
    extract_result.column_node_df = node_dfs[NodeLabel.COLUMN]
    extract_result.schema_node_df = node_dfs[NodeLabel.SCHEMA]
    extract_result.database_df = node_dfs[NodeLabel.DATABASE]


def _transform_sample_values(
    spark: "SparkSession", settings: SparkIngestSettings, schema_list: list[str],
    extract_result: ExtractResult, staging_path: str, ledger_path: str,
    summary: RunSummary,
) -> ValueResult | None:
    """Sample distinct values and optionally embed the Value nodes.

    Returns None when DBXCARTA_INCLUDE_VALUES is off; the Settings
    cross-field validator already rejects the embedding-values-without-
    include-values incoherence, so no warning branch is needed here.
    """
    if not settings.dbxcarta_include_values:
        return None

    value_node_df, has_value_df, sample_stats = sv.sample(
        spark, extract_result.columns_df, settings.dbxcarta_catalog, schema_list,
        settings.dbxcarta_sample_limit,
        settings.dbxcarta_sample_cardinality_threshold,
        settings.dbxcarta_stack_chunk_size,
    )
    summary.sample_values = SampleValueCounts.from_sample_stats(sample_stats)
    logger.info(
        "[dbxcarta] sample values: candidates=%d sampled=%d value_nodes=%d",
        sample_stats.candidate_columns,
        sample_stats.sampled_columns,
        sample_stats.value_nodes,
    )

    if settings.dbxcarta_include_embeddings_values and sample_stats.value_nodes > 0:
        value_node_df = _embed_and_stage(
            value_node_df, _EMBEDDING_TEXT_EXPRS[NodeLabel.VALUE], NodeLabel.VALUE,
            settings, staging_path, ledger_path, summary,
        )

    return ValueResult(
        value_node_df=value_node_df,
        has_value_df=has_value_df,
        sample_stats=sample_stats,
    )


def _check_thresholds(settings: SparkIngestSettings, summary: RunSummary) -> None:
    """Raise RuntimeError if any per-label or aggregate embedding failure
    rate exceeds the configured threshold.

    Called after embedding transforms but before Neo4j writes so a bad
    endpoint run does not partially refresh the graph with missing vectors.
    """
    threshold = settings.dbxcarta_embedding_failure_threshold
    for label, rate in summary.embeddings.failure_rate_per_label.items():
        if rate > threshold:
            raise RuntimeError(
                f"[dbxcarta] {label.value} embedding failure rate {rate:.2%} exceeds"
                f" threshold {threshold:.2%}; aborting before Neo4j write"
            )

    total_attempts = sum(summary.embeddings.attempts.values())
    total_successes = sum(summary.embeddings.successes.values())
    if total_attempts > 0:
        aggregate_rate = (total_attempts - total_successes) / total_attempts
        summary.embeddings.aggregate_failure_rate = aggregate_rate
        if aggregate_rate > threshold:
            raise RuntimeError(
                f"[dbxcarta] Aggregate embedding failure rate {aggregate_rate:.2%} exceeds"
                f" threshold {threshold:.2%}; aborting before Neo4j write"
            )


def _load(
    neo4j: Neo4jConfig,
    driver: "Driver",
    extract_result: ExtractResult,
    fk_result: FKDiscoveryResult,
    values: ValueResult | None,
    summary: RunSummary,
) -> None:
    """Write extracted, inferred, and sampled graph artifacts to Neo4j.

    Node writes omit coalesce(1) so the connector can parallelize at
    `batch_size`. Relationship writes keep coalesce(1) to reduce lock
    contention when multiple Spark partitions would merge adjacent edges.
    """
    candidate_col_ids = sv.get_candidate_col_ids(extract_result.columns_df)
    purge_stale_values(driver, candidate_col_ids)

    logger.info("[dbxcarta] writing nodes: Database (1)")
    write_node(_drop_cols(extract_result.database_df, "embedding_error"), neo4j, NodeLabel.DATABASE)

    logger.info("[dbxcarta] writing nodes: Schema (%d)", summary.extract.schemas)
    write_node(
        _drop_cols(extract_result.schema_node_df, "catalog_name", "embedding_error"),
        neo4j, NodeLabel.SCHEMA,
    )

    logger.info("[dbxcarta] writing nodes: Table (%d)", summary.extract.tables)
    write_node(_drop_cols(extract_result.table_node_df, "embedding_error"), neo4j, NodeLabel.TABLE)

    logger.info("[dbxcarta] writing nodes: Column (%d)", summary.extract.columns)
    write_node(
        _drop_cols(extract_result.column_node_df, "table_schema", "table_name", "embedding_error"),
        neo4j, NodeLabel.COLUMN,
    )

    if values is not None and values.sample_stats.value_nodes > 0:
        logger.info("[dbxcarta] writing nodes: Value (%d)", values.sample_stats.value_nodes)
        write_node(_drop_cols(values.value_node_df, "embedding_error"), neo4j, NodeLabel.VALUE)
        logger.info("[dbxcarta] writing relationships: HAS_VALUE (%d)", values.sample_stats.has_value_edges)
        write_rel(
            values.has_value_df.coalesce(1), neo4j,
            RelType.HAS_VALUE, NodeLabel.COLUMN, NodeLabel.VALUE,
        )

    logger.info("[dbxcarta] writing relationships: HAS_SCHEMA")
    write_rel(
        extract_result.has_schema_df.coalesce(1), neo4j,
        RelType.HAS_SCHEMA, NodeLabel.DATABASE, NodeLabel.SCHEMA,
    )

    logger.info("[dbxcarta] writing relationships: HAS_TABLE")
    write_rel(
        extract_result.has_table_df.coalesce(1), neo4j,
        RelType.HAS_TABLE, NodeLabel.SCHEMA, NodeLabel.TABLE,
    )

    logger.info("[dbxcarta] writing relationships: HAS_COLUMN")
    write_rel(
        extract_result.has_column_df.coalesce(1), neo4j,
        RelType.HAS_COLUMN, NodeLabel.TABLE, NodeLabel.COLUMN,
    )

    if fk_result.declared_edge_count > 0 and fk_result.declared_edges_df is not None:
        logger.info(
            "[dbxcarta] writing relationships: REFERENCES declared (%d)",
            fk_result.declared_edge_count,
        )
        write_rel(
            fk_result.declared_edges_df.coalesce(1), neo4j,
            RelType.REFERENCES, NodeLabel.COLUMN, NodeLabel.COLUMN,
            properties=REFERENCES_PROPERTIES,
        )

    if fk_result.metadata_edge_count > 0 and fk_result.metadata_edges_df is not None:
        logger.info(
            "[dbxcarta] writing relationships: REFERENCES inferred_metadata (%d)",
            fk_result.metadata_edge_count,
        )
        write_rel(
            fk_result.metadata_edges_df.coalesce(1), neo4j,
            RelType.REFERENCES, NodeLabel.COLUMN, NodeLabel.COLUMN,
            properties=REFERENCES_PROPERTIES,
        )

    if fk_result.semantic_edge_count > 0 and fk_result.semantic_edges_df is not None:
        logger.info(
            "[dbxcarta] writing relationships: REFERENCES semantic (%d)",
            fk_result.semantic_edge_count,
        )
        write_rel(
            fk_result.semantic_edges_df.coalesce(1), neo4j,
            RelType.REFERENCES, NodeLabel.COLUMN, NodeLabel.COLUMN,
            properties=REFERENCES_PROPERTIES,
        )


def _drop_cols(df: "DataFrame", *names: str) -> "DataFrame":
    """Drop optional pipeline-only columns before connector writes.

    The helper is intentionally tolerant because embedding columns only exist
    for labels whose embedding flag was enabled for this run.
    """
    for name in names:
        if name in df.columns:
            df = df.drop(name)
    return df


def _embed_and_stage(
    df: "DataFrame", text_expr: str, label: NodeLabel,
    settings: SparkIngestSettings, staging_path: str, ledger_path: str, summary: RunSummary,
) -> "DataFrame":
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
        ledger_df = read_ledger(spark, ledger_path, label)

        if ledger_df is not None:
            df_hashed = df.withColumn("_curr_hash", sha2(spark_expr(text_expr), 256))
            hits_df, misses_df = split_by_ledger(df_hashed, ledger_df, endpoint)
            hit_count = hits_df.count()
            summary.embeddings.ledger_hits[label] = hit_count

            hit_final = hits_df.select(
                *[col(c) for c in df.columns],
                col("_curr_hash").alias("embedding_text_hash"),
                col("_led_embedding").cast(ArrayType(DoubleType())).alias("embedding"),
                lit(None).cast("string").alias("embedding_error"),
                col("_led_model").alias("embedding_model"),
                col("_led_embedded_at").alias("embedded_at"),
            )

            embedded_misses = emb.add_embedding_column(
                misses_df, text_expr, endpoint, dimension, label=label.value,
            )
            enriched = hit_final.unionByName(embedded_misses)
        else:
            summary.embeddings.ledger_hits[label] = 0
            enriched = emb.add_embedding_column(
                df, text_expr, endpoint, dimension, label=label.value,
            )
    else:
        enriched = emb.add_embedding_column(
            df, text_expr, endpoint, dimension, label=label.value,
        )

    staged = stage_embedded_nodes(enriched, staging_path, label)
    rate, attempts, successes = emb.compute_failure_stats(staged)
    summary.embeddings.attempts[label] = attempts
    summary.embeddings.successes[label] = successes
    summary.embeddings.failure_rate_per_label[label] = rate

    if settings.dbxcarta_ledger_enabled:
        upsert_ledger(staged, ledger_path, label)

    logger.info(
        "[dbxcarta] %s embeddings: attempts=%d successes=%d failure_rate=%.2f%% ledger_hits=%d",
        label.value, attempts, successes, rate * 100,
        summary.embeddings.ledger_hits.get(label, 0),
    )
    return staged
