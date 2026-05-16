"""FK discovery orchestrator: runs declared → metadata → semantic.

Pipeline boundary for all FK-discovery work. Declared FK is read from
information_schema as a bounded Spark frame; metadata and semantic
inference run entirely in Spark via `fk.inference` — no catalog-scale
collect to the driver, no Python all-pairs loop. Produces
`FKDiscoveryResult` — ready-to-write REFERENCES DataFrames tagged with
`EdgeSource.{DECLARED, INFERRED_METADATA, SEMANTIC}`.

Each strategy is suppressed against the edges earlier strategies already
emitted. Declared receives nothing; metadata anti-joins declared-only;
semantic anti-joins declared ∪ metadata. The prior set is threaded as a
`(source_id, target_id)` DataFrame, never collected.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

import dbxcarta.spark.ingest.schema_graph as sg
from dbxcarta.spark.ingest.fk.declared import discover_declared
from dbxcarta.spark.ingest.fk.inference import (
    build_columns_frame,
    build_pk_gate,
    infer_metadata_edges,
    infer_semantic_edges,
)
from dbxcarta.spark.ingest.summary import RunSummary

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

    from dbxcarta.spark.ingest.extract import ExtractResult
    from dbxcarta.spark.settings import SparkIngestSettings
    from dbxcarta.spark.ingest.transform.sample_values import SampleStats

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
    settings: "SparkIngestSettings",
    schema_list: list[str],
    extract: "ExtractResult",
    sample_stats: "SampleStats | None",
    value_node_df: "DataFrame | None",
    has_value_df: "DataFrame | None",
    summary: RunSummary,
) -> FKDiscoveryResult:
    """Run declared → metadata → semantic, threading prior edges in Spark.

    Semantic is gated on DBXCARTA_INFER_SEMANTIC and on a catalog-size floor
    (DBXCARTA_SEMANTIC_MIN_TABLES). Column embeddings are required and
    already validated present by Settings' cross-field validator.
    """
    constraints_df = _constraints_df(spark, settings, schema_list)
    columns_frame = build_columns_frame(
        extract.columns_df, extract.column_node_df,
    )
    pk_gate, composite_pk_count = build_pk_gate(columns_frame, constraints_df)

    # Declared FK: bounded by catalog-declared FKs, not n²; collect retained
    # by design (see proposal). build_references_rel emits the canonical
    # 5-col REFERENCES schema, identical to the inferred edge frames.
    declared_edges, declared_counters = discover_declared(
        spark, settings, schema_list,
    )
    summary.fk_declared = declared_counters
    declared_edges_df = (
        sg.build_references_rel(spark, declared_edges) if declared_edges else None
    )

    metadata_edges_df, metadata_counts, composite_skipped = infer_metadata_edges(
        spark, columns_frame, pk_gate, declared_edges_df,
        composite_pk_count=composite_pk_count,
    )
    summary.fk_metadata = metadata_counts
    logger.info(
        "[dbxcarta] metadata inference: candidates=%d accepted=%d"
        " composite_pks_skipped=%d",
        metadata_counts.candidates, metadata_counts.accepted, composite_skipped,
    )
    metadata_out = metadata_edges_df if metadata_counts.accepted else None

    # declared ∪ metadata, for the semantic anti-join only.
    prior_edges_df = _union_pairs(declared_edges_df, metadata_out)

    semantic_out: "DataFrame | None" = None
    semantic_count = 0
    if _should_run_semantic(settings, summary.extract.tables):
        sv_node_df, sv_has_df = _semantic_value_frames(
            value_node_df, has_value_df, sample_stats,
        )
        semantic_edges_df, semantic_counts = infer_semantic_edges(
            columns_frame, pk_gate, prior_edges_df, sv_node_df, sv_has_df,
            threshold=settings.dbxcarta_semantic_threshold,
        )
        summary.fk_semantic = semantic_counts
        logger.info(
            "[dbxcarta] semantic inference: candidates=%d accepted=%d"
            " value_corroborated=%d",
            semantic_counts.candidates, semantic_counts.accepted,
            semantic_counts.value_corroborated,
        )
        if semantic_counts.accepted:
            semantic_out = semantic_edges_df
            semantic_count = semantic_counts.accepted

    return FKDiscoveryResult(
        declared_edges_df=declared_edges_df,
        declared_edge_count=len(declared_edges),
        metadata_edges_df=metadata_out,
        metadata_edge_count=metadata_counts.accepted,
        semantic_edges_df=semantic_out,
        semantic_edge_count=semantic_count,
    )


def _union_pairs(
    a: "DataFrame | None", b: "DataFrame | None",
) -> "DataFrame | None":
    """`(source_id, target_id)` union of two edge frames, dropping None."""
    frames = [
        f.select("source_id", "target_id") for f in (a, b) if f is not None
    ]
    if not frames:
        return None
    out = frames[0]
    for f in frames[1:]:
        out = out.unionByName(f)
    return out.distinct()


def _semantic_value_frames(
    value_node_df: "DataFrame | None",
    has_value_df: "DataFrame | None",
    sample_stats: "SampleStats | None",
) -> tuple["DataFrame | None", "DataFrame | None"]:
    """Pass the sampled-value frames through only when values were sampled.

    None signals "no corroboration signal available" to the semantic
    overlap join — the value bonus then simply does not apply.
    """
    if (
        value_node_df is None
        or has_value_df is None
        or sample_stats is None
        or sample_stats.value_nodes == 0
    ):
        return None, None
    return value_node_df, has_value_df


def _should_run_semantic(settings: "SparkIngestSettings", n_tables: int) -> bool:
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


def _constraints_df(
    spark: "SparkSession", settings: "SparkIngestSettings", schema_list: list[str],
) -> "DataFrame":
    """PK/UNIQUE constraints across every ingested catalog as a Spark frame.

    Never collected — fed straight into `build_pk_gate`. Cross-catalog FK
    pairing stays blocked downstream (inference equi-joins on
    catalog/schema), so unioning per-catalog reads only makes per-catalog
    PKs visible.
    """
    from functools import reduce

    from pyspark.sql.functions import col

    rows = reduce(
        lambda a, b: a.unionByName(b),
        [
            spark.sql(
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
            for catalog in settings.resolved_catalogs()
        ],
    )
    if schema_list:
        rows = rows.filter(col("table_schema").isin(schema_list))
    return rows
