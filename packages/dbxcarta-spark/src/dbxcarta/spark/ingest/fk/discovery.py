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
from dbxcarta.spark.ingest.summary import FKSkipCounts, RunSummary

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

    def unpersist_cached(self) -> None:
        """Release inferred edge caches created for count/write reuse."""
        for df in (self.metadata_edges_df, self.semantic_edges_df):
            if df is not None:
                df.unpersist()


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
    from pyspark import StorageLevel

    if _fk_guardrail_tripped(settings, summary):
        return _skipped_result()

    constraints_df = _constraints_df(spark, settings, schema_list)
    columns_frame = build_columns_frame(
        extract.columns_df, extract.column_node_df,
    )
    # columns_frame and pk_gate each feed up to three jobs (build_pk_gate,
    # then the two strategy actions). Persist once so the n²-shaped lineage
    # is not re-run per strategy. MEMORY_AND_DISK (not cache/MEMORY_ONLY) so
    # eviction at the 10k-table target cannot cause a silent recompute.
    # Marking persisted does not materialize; both fill lazily on first use.
    columns_frame.persist(StorageLevel.MEMORY_AND_DISK)
    # pk_gate is internal to this function; its lifecycle ends here. Wrap the
    # body in try/finally so both caches are released on the failure path
    # too — otherwise a failed FK discovery leaks them into the session for
    # the rest of the job. This is separate from FKDiscoveryResult's edge
    # caches (FKDiscoveryResult.unpersist_cached), which the caller owns.
    pk_gate: "DataFrame | None" = None
    try:
        pk_gate, composite_pk_count = build_pk_gate(
            columns_frame, constraints_df,
        )
        pk_gate.cache()

        # Declared FK: bounded by catalog-declared FKs, not n²; collect
        # retained by design (see proposal). build_references_rel emits the
        # canonical 5-col REFERENCES schema, identical to the inferred edge
        # frames.
        declared_edges, declared_counters = discover_declared(
            spark, settings, schema_list,
        )
        summary.fk_declared = declared_counters
        declared_edges_df = (
            sg.build_references_rel(spark, declared_edges)
            if declared_edges else None
        )

        metadata_edges_df, metadata_counts, composite_skipped = (
            infer_metadata_edges(
                spark, columns_frame, pk_gate, declared_edges_df,
                composite_pk_count=composite_pk_count,
            )
        )
        summary.fk_metadata = metadata_counts
        logger.info(
            "[dbxcarta] metadata inference: accepted=%d"
            " composite_pks_skipped=%d",
            metadata_counts.accepted, composite_skipped,
        )
        metadata_out = metadata_edges_df if metadata_counts.accepted else None
        if metadata_out is None:
            metadata_edges_df.unpersist()

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
                "[dbxcarta] semantic inference: accepted=%d",
                semantic_counts.accepted,
            )
            if semantic_counts.accepted:
                semantic_out = semantic_edges_df
                semantic_count = semantic_counts.accepted
            else:
                semantic_edges_df.unpersist()

        return FKDiscoveryResult(
            declared_edges_df=declared_edges_df,
            declared_edge_count=len(declared_edges),
            metadata_edges_df=metadata_out,
            metadata_edge_count=metadata_counts.accepted,
            semantic_edges_df=semantic_out,
            semantic_edge_count=semantic_count,
        )
    finally:
        columns_frame.unpersist()
        if pk_gate is not None:
            pk_gate.unpersist()


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


def _fk_guardrail_tripped(
    settings: "SparkIngestSettings", summary: RunSummary,
) -> bool:
    """Skip FK discovery when the catalog is absurdly wide.

    `dbxcarta_fk_max_columns == 0` disables the guardrail. Otherwise, skip
    when the extracted column count exceeds it and record the trip on
    `summary`. Reads the already-materialized `summary.extract.columns`
    scalar, so no driver action is added.
    """
    limit = settings.dbxcarta_fk_max_columns
    if limit <= 0:
        return False
    columns = summary.extract.columns
    if columns <= limit:
        return False
    logger.warning(
        "[dbxcarta] FK discovery skipped by guardrail: %d columns > limit %d"
        " (DBXCARTA_FK_MAX_COLUMNS)",
        columns, limit,
    )
    summary.fk_skip = FKSkipCounts(column_count=columns, column_limit=limit)
    return True


def _skipped_result() -> FKDiscoveryResult:
    """All-`None` result so the load step writes no REFERENCES edges.

    Reuses the existing FKDiscoveryResult None-edge contract rather than a
    separate skip path: `_load` already guards each REFERENCES write on a
    non-None DataFrame and a positive count.
    """
    return FKDiscoveryResult(
        declared_edges_df=None,
        declared_edge_count=0,
        metadata_edges_df=None,
        metadata_edge_count=0,
        semantic_edges_df=None,
        semantic_edge_count=0,
    )


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
