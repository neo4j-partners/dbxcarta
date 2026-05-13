"""FK discovery orchestrator: runs declared → metadata → semantic.

Pipeline boundary for all FK-discovery work. Owns the Spark Row → dataclass
conversions for columns, constraints, embeddings, and sampled values.
Produces `FKDiscoveryResult` — ready-to-write REFERENCES DataFrames tagged
with `EdgeSource.{DECLARED, INFERRED_METADATA, SEMANTIC}`.

Each strategy receives `prior_pairs: frozenset[DeclaredPair]` accumulated
from earlier strategies and skips pairs already covered. Declared receives
an empty set; metadata receives declared's pairs; semantic receives
declared + metadata.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

import dbxcarta.ingest.schema_graph as sg
from dbxcarta.ingest.fk.common import (
    ColumnMeta,
    ConstraintRow,
    DeclaredPair,
    FKEdge,
    PKIndex,
)
from dbxcarta.ingest.fk.declared import discover_declared
from dbxcarta.ingest.fk.metadata import infer_fk_pairs
from dbxcarta.ingest.fk.semantic import (
    ColumnEmbedding,
    ValueIndex,
    infer_semantic_pairs,
)
from dbxcarta.ingest.summary import RunSummary

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

    from dbxcarta.ingest.extract import ExtractResult
    from dbxcarta.settings import Settings
    from dbxcarta.ingest.transform.sample_values import SampleStats

logger = logging.getLogger(__name__)


@dataclass
class FKDiscoveryResult:
    """Post-discovery DataFrames ready for the Neo4j REFERENCES write.

    None when the corresponding strategy produced zero edges (or was gated
    off). The pipeline's load step skips writes whose DataFrame is None."""

    declared_edges_df: "DataFrame | None"
    declared_edge_count: int
    metadata_edges_df: "DataFrame | None"
    metadata_edge_count: int
    semantic_edges_df: "DataFrame | None"
    semantic_edge_count: int


def run_fk_discovery(
    spark: "SparkSession",
    settings: "Settings",
    schema_list: list[str],
    extract: "ExtractResult",
    sample_stats: "SampleStats | None",
    value_node_df: "DataFrame | None",
    has_value_df: "DataFrame | None",
    summary: RunSummary,
) -> FKDiscoveryResult:
    """Run declared → metadata → semantic, threading prior_pairs uniformly.

    Semantic is gated on DBXCARTA_INFER_SEMANTIC and on a catalog-size floor
    (DBXCARTA_SEMANTIC_MIN_TABLES). Column embeddings are required and
    already validated present by Settings' cross-field validator.
    """
    column_metas = [
        ColumnMeta.from_row(r.asDict()) for r in extract.columns_df.collect()
    ]
    pk_index = _build_pk_index(spark, settings, schema_list)

    prior_pairs: frozenset[DeclaredPair] = frozenset()

    declared_edges, declared_counters = discover_declared(
        spark, settings, schema_list, prior_pairs=prior_pairs,
    )
    summary.fk_declared = declared_counters
    declared_edges_df = (
        sg.build_references_rel(spark, declared_edges) if declared_edges else None
    )
    prior_pairs = prior_pairs | _edges_as_pairs(declared_edges)

    metadata_edges, metadata_counters = infer_fk_pairs(
        column_metas, pk_index, prior_pairs,
    )
    summary.fk_metadata = metadata_counters
    logger.info(
        "[dbxcarta] metadata inference: candidates=%d accepted=%d"
        " composite_pks_skipped=%d",
        metadata_counters.candidates, metadata_counters.accepted,
        metadata_counters.composite_pk_skipped,
    )
    metadata_edges_df = (
        sg.build_references_rel(spark, metadata_edges) if metadata_edges else None
    )
    prior_pairs = prior_pairs | _edges_as_pairs(metadata_edges)

    semantic_edges_df: "DataFrame | None" = None
    semantic_edge_count = 0

    if _should_run_semantic(settings, summary.extract.tables):
        embeddings = _collect_column_embeddings(extract.column_node_df)
        value_index = _build_value_index(value_node_df, has_value_df, sample_stats)

        semantic_edges, semantic_counters = infer_semantic_pairs(
            columns=column_metas,
            embeddings=embeddings,
            pk_index=pk_index,
            prior_pairs=prior_pairs,
            value_index=value_index,
            threshold=settings.dbxcarta_semantic_threshold,
        )
        summary.fk_semantic = semantic_counters
        logger.info(
            "[dbxcarta] semantic inference: considered=%d accepted=%d"
            " value_corroborated=%d",
            semantic_counters.considered, semantic_counters.accepted,
            semantic_counters.value_corroborated,
        )
        if semantic_edges:
            semantic_edges_df = sg.build_references_rel(spark, semantic_edges)
            semantic_edge_count = len(semantic_edges)

    return FKDiscoveryResult(
        declared_edges_df=declared_edges_df,
        declared_edge_count=len(declared_edges),
        metadata_edges_df=metadata_edges_df,
        metadata_edge_count=len(metadata_edges),
        semantic_edges_df=semantic_edges_df,
        semantic_edge_count=semantic_edge_count,
    )


def _edges_as_pairs(edges: list[FKEdge]) -> frozenset[DeclaredPair]:
    return frozenset(
        DeclaredPair(source_id=e.source_id, target_id=e.target_id) for e in edges
    )


def _should_run_semantic(settings: "Settings", n_tables: int) -> bool:
    """Semantic runs iff enabled and the catalog exceeds the min-tables floor.

    On tiny catalogs, embedding similarity fires on unrelated pairs more often
    than it helps; the gate blocks that while letting small fixtures exercise
    the code path via a per-test override of DBXCARTA_SEMANTIC_MIN_TABLES.
    """
    if not settings.dbxcarta_infer_semantic:
        return False
    if n_tables < settings.dbxcarta_semantic_min_tables:
        logger.info(
            "[dbxcarta] semantic inference skipped: %d tables < min %d",
            n_tables, settings.dbxcarta_semantic_min_tables,
        )
        return False
    return True


def _build_pk_index(
    spark: "SparkSession", settings: "Settings", schema_list: list[str],
) -> PKIndex:
    """Pull information_schema constraints and build the PKIndex."""
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

    constraint_rows = [ConstraintRow.from_row(r.asDict()) for r in pk_rows_df.collect()]
    return PKIndex.from_constraints(constraint_rows)


def _collect_column_embeddings(column_node_df: "DataFrame") -> dict[str, ColumnEmbedding]:
    """Pipeline-edge conversion: staged column node DataFrame →
    dict[col_id → ColumnEmbedding].

    Rows with null embedding (ai_query failures, dimension mismatches) are
    skipped — semantic discovery never considers them as candidates."""
    from pyspark.sql.functions import col

    rows = (
        column_node_df
        .filter(col("embedding").isNotNull())
        .select("id", "embedding")
        .collect()
    )
    return {
        r["id"]: ColumnEmbedding.from_vector(r["id"], list(r["embedding"]))
        for r in rows
    }


def _build_value_index(
    value_node_df: "DataFrame | None",
    has_value_df: "DataFrame | None",
    sample_stats: "SampleStats | None",
) -> ValueIndex | None:
    """Pipeline-edge conversion: Value node + HAS_VALUE edges → ValueIndex.

    Returns None when values weren't sampled (DBXCARTA_INCLUDE_VALUES off,
    or zero value_nodes). Semantic interprets None as "no corroboration
    signal available" and the value-bonus simply doesn't apply.
    """
    from pyspark.sql.functions import col

    if (
        value_node_df is None
        or has_value_df is None
        or sample_stats is None
        or sample_stats.value_nodes == 0
    ):
        return None

    joined = (
        has_value_df.alias("h")
        .join(value_node_df.alias("v"), col("h.target_id") == col("v.id"))
        .select(col("h.source_id").alias("col_id"), col("v.value").alias("val"))
    )
    by_col: dict[str, set[str]] = {}
    for r in joined.collect():
        by_col.setdefault(r["col_id"], set()).add(str(r["val"]))
    return ValueIndex(values_by_col_id={k: frozenset(v) for k, v in by_col.items()})
