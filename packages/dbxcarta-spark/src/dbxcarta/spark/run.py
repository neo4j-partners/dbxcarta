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
from typing import TYPE_CHECKING

import dbxcarta.spark.ingest.transform.sample_values as sv
from dbxcarta.spark.contract import (
    CONTRACT_VERSION,
    NODE_PROPERTIES,
    NodeLabel,
    REFERENCES_PROPERTIES,
    RelType,
)
from dbxcarta.spark.ingest.extract import ExtractResult, extract
from dbxcarta.spark.ingest.fk.discovery import FKDiscoveryResult, run_fk_discovery
from dbxcarta.spark.ingest.load.neo4j_io import (
    bootstrap_constraints,
    purge_stale_values,
    query_counts,
    write_node,
    write_rel,
)
from dbxcarta.spark.ingest.preflight import preflight
from dbxcarta.spark.settings import SparkIngestSettings
from dbxcarta.spark.ingest.transform.embed_stage import (
    check_thresholds,
    transform_embeddings,
)
from dbxcarta.spark.ingest.transform.staging import (
    resolve_ledger_path,
    resolve_staging_path,
    truncate_staging_root,
)
from dbxcarta.spark.ingest.transform.value_stage import (
    ValueResult,
    transform_sample_values,
)
import dbxcarta.spark.ingest.summary_io as summary_io
from dbxcarta.spark.ingest.summary import EmbeddingCounts, RunSummary
from dbxcarta.spark.ingest.load.writer import Neo4jConfig

if TYPE_CHECKING:
    from neo4j import Driver
    from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)


def run_dbxcarta(
    *,
    settings: SparkIngestSettings | None = None,
    spark: "SparkSession | None" = None,
) -> RunSummary:
    """Run a complete ingest and return the finished RunSummary.

    When called with no arguments, this is the Databricks wheel entrypoint:
    settings are loaded from environment variables and the active Spark session
    is resolved lazily. Library consumers can pass an explicit SparkIngestSettings.
    """
    resolved_settings = settings if settings is not None else SparkIngestSettings()

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
    return summary


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
        summary_io.emit(summary, spark, volume_path, table_name)
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
    values: ValueResult | None = None
    fk_result: FKDiscoveryResult | None = None
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
            transform_embeddings(settings, extract_result, staging_path, ledger_path, summary)
            values = transform_sample_values(
                spark, settings, schema_list, extract_result,
                staging_path, ledger_path, summary,
            )
            check_thresholds(settings, summary)

            fk_result = run_fk_discovery(
                spark, settings, schema_list, extract_result,
                values.sample_stats if values else None,
                values.value_node_df if values else None,
                values.has_value_df if values else None,
                summary,
            )

            _load(neo4j, driver, settings, extract_result, fk_result, values, summary)
            summary.neo4j_counts = query_counts(driver)
            _verify(driver, settings, summary)
    except Exception as exc:
        run_error = exc
        raise
    finally:
        # Release cached snapshots after run_fk_discovery and _load have
        # finished reading them. Each release is guarded independently so a
        # failure in one still attempts the others; on the success path a
        # release failure is re-raised, on the failure path it is logged so it
        # does not mask the original error.
        if fk_result is not None:
            try:
                fk_result.unpersist_cached()
            except Exception:
                if run_error is not None:
                    logger.exception(
                        "[dbxcarta] failed to unpersist cached FK edge DataFrames"
                    )
                else:
                    raise
        if values is not None:
            try:
                values.unpersist_cached()
            except Exception:
                if run_error is not None:
                    logger.exception(
                        "[dbxcarta] failed to unpersist cached sampled-value DataFrame"
                    )
                else:
                    raise
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
    """Final pipeline step: re-run dbxcarta.spark.verify against the run summary just
    built. Records the outcome on summary.verify (durable in JSON + Delta).
    Warn-only by default; raises when settings.dbxcarta_verify_gate is True.

    The summary dict is materialised here with status forced to 'success' — the
    enclosing run_dbxcarta() has not yet called summary.finish(), but verify's
    summary-shape check expects a successful run. If we raise, the outer
    catch-all in run_dbxcarta() records the failure via summary.finish(failure).
    """
    from databricks.sdk import WorkspaceClient
    from dbxcarta.spark.ingest.summary import VerifyResult
    from dbxcarta.spark.verify import verify_run

    summary_dict = summary.to_dict()
    summary_dict["status"] = "success"
    ws = WorkspaceClient()
    report = verify_run(
        summary=summary_dict,
        neo4j_driver=driver,
        ws=ws,
        warehouse_id=settings.databricks_warehouse_id,
        # The count invariants (graph node counts, REFERENCES, Value) scope to
        # every resolved catalog so a multi-catalog run is checked against its
        # aggregate summary totals. `catalog` remains the single primary for
        # the deferred information_schema sampling in verify.catalog (tracked
        # in SparkIngestSettings.dbxcarta_catalogs as part-2).
        catalog=settings.dbxcarta_catalog,
        catalogs=settings.resolved_catalogs(),
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


def _load(
    neo4j: Neo4jConfig,
    driver: "Driver",
    settings: SparkIngestSettings,
    extract_result: ExtractResult,
    fk_result: FKDiscoveryResult,
    values: ValueResult | None,
    summary: RunSummary,
) -> None:
    """Write extracted, inferred, and sampled graph artifacts to Neo4j.

    Node writes omit coalesce(1) so the connector can parallelize at
    `batch_size`. Relationship writes are partitioned by `_rel_partition`
    per `dbxcarta_rel_write_partitions`: the default of 1 coalesces to a
    single partition (byte-for-byte identical to the historical write, the
    safe default for Neo4j lock contention); a higher value repartitions
    for tuned parallel relationship writes.
    """
    candidate_col_ids = sv.get_candidate_col_ids(extract_result.columns_df)
    purge_stale_values(driver, candidate_col_ids)

    # Database nodes are one per resolved catalog; use the settings-derived
    # catalog count instead of a logging-only count() action on database_df.
    logger.info(
        "[dbxcarta] writing nodes: Database (%d)",
        len(settings.resolved_catalogs()),
    )
    write_node(_project(extract_result.database_df, NodeLabel.DATABASE), neo4j, NodeLabel.DATABASE)

    logger.info("[dbxcarta] writing nodes: Schema (%d)", summary.extract.schemas)
    write_node(
        _project(extract_result.schema_node_df, NodeLabel.SCHEMA),
        neo4j, NodeLabel.SCHEMA,
    )

    logger.info("[dbxcarta] writing nodes: Table (%d)", summary.extract.tables)
    write_node(
        _project(extract_result.table_node_df, NodeLabel.TABLE),
        neo4j, NodeLabel.TABLE,
    )

    logger.info("[dbxcarta] writing nodes: Column (%d)", summary.extract.columns)
    write_node(
        _project(extract_result.column_node_df, NodeLabel.COLUMN),
        neo4j, NodeLabel.COLUMN,
    )

    if values is not None and values.sample_stats.value_nodes > 0:
        logger.info("[dbxcarta] writing nodes: Value (%d)", values.sample_stats.value_nodes)
        write_node(_project(values.value_node_df, NodeLabel.VALUE), neo4j, NodeLabel.VALUE)
        logger.info("[dbxcarta] writing relationships: HAS_VALUE (%d)", values.sample_stats.has_value_edges)
        write_rel(
            _rel_partition(values.has_value_df, settings.dbxcarta_rel_write_partitions), neo4j,
            RelType.HAS_VALUE, NodeLabel.COLUMN, NodeLabel.VALUE,
        )

    logger.info("[dbxcarta] writing relationships: HAS_SCHEMA")
    write_rel(
        _rel_partition(extract_result.has_schema_df, settings.dbxcarta_rel_write_partitions), neo4j,
        RelType.HAS_SCHEMA, NodeLabel.DATABASE, NodeLabel.SCHEMA,
    )

    logger.info("[dbxcarta] writing relationships: HAS_TABLE")
    write_rel(
        _rel_partition(extract_result.has_table_df, settings.dbxcarta_rel_write_partitions), neo4j,
        RelType.HAS_TABLE, NodeLabel.SCHEMA, NodeLabel.TABLE,
    )

    logger.info("[dbxcarta] writing relationships: HAS_COLUMN")
    write_rel(
        _rel_partition(extract_result.has_column_df, settings.dbxcarta_rel_write_partitions), neo4j,
        RelType.HAS_COLUMN, NodeLabel.TABLE, NodeLabel.COLUMN,
    )

    if fk_result.declared_edge_count > 0 and fk_result.declared_edges_df is not None:
        logger.info(
            "[dbxcarta] writing relationships: REFERENCES declared (%d)",
            fk_result.declared_edge_count,
        )
        write_rel(
            _rel_partition(fk_result.declared_edges_df, settings.dbxcarta_rel_write_partitions), neo4j,
            RelType.REFERENCES, NodeLabel.COLUMN, NodeLabel.COLUMN,
            properties=REFERENCES_PROPERTIES,
        )

    if fk_result.metadata_edge_count > 0 and fk_result.metadata_edges_df is not None:
        logger.info(
            "[dbxcarta] writing relationships: REFERENCES inferred_metadata (%d)",
            fk_result.metadata_edge_count,
        )
        write_rel(
            _rel_partition(fk_result.metadata_edges_df, settings.dbxcarta_rel_write_partitions), neo4j,
            RelType.REFERENCES, NodeLabel.COLUMN, NodeLabel.COLUMN,
            properties=REFERENCES_PROPERTIES,
        )

    if fk_result.semantic_edge_count > 0 and fk_result.semantic_edges_df is not None:
        logger.info(
            "[dbxcarta] writing relationships: REFERENCES semantic (%d)",
            fk_result.semantic_edge_count,
        )
        write_rel(
            _rel_partition(fk_result.semantic_edges_df, settings.dbxcarta_rel_write_partitions), neo4j,
            RelType.REFERENCES, NodeLabel.COLUMN, NodeLabel.COLUMN,
            properties=REFERENCES_PROPERTIES,
        )


def _project(df: "DataFrame", label: NodeLabel) -> "DataFrame":
    """Project a node DataFrame to its declared per-label property set.

    This is the fail-closed write boundary: a column reaches Neo4j if and
    only if it is listed in `contract.NODE_PROPERTIES[label]`. Helper
    columns and embedding bookkeeping (`embedding_text`,
    `embedding_text_hash`, `embedding_model`, `embedded_at`,
    `embedding_error`) are dropped by construction because they are not
    declared — no hand-maintained denylist.

    `embedding` is the only declared column that may be legitimately absent
    (present only when the label was embedded this run); the projection
    selects the intersection. Any other missing declared column is a real
    contract violation and fails loudly rather than being silently skipped.
    """
    declared = NODE_PROPERTIES[label]
    present = set(df.columns)
    missing = [c for c in declared if c != "embedding" and c not in present]
    if missing:
        raise RuntimeError(
            f"[dbxcarta] {label.value} node DataFrame is missing declared"
            f" properties {missing}; columns present: {sorted(present)}"
        )
    return df.select(*[c for c in declared if c in present])


def _rel_partition(df: "DataFrame", n: int) -> "DataFrame":
    """Set relationship-write partitioning per `dbxcarta_rel_write_partitions`.

    `repartition(1)` is deliberately not the `n <= 1` branch: it forces a
    shuffle and is not equivalent to `coalesce(1)`.
    """
    return df.coalesce(1) if n <= 1 else df.repartition(n)
