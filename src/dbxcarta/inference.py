"""Inference orchestration: Phase 3 (metadata) + Phase 4 (semantic).

Pipeline boundary for all FK-inference work. Owns the Spark Row → dataclass
conversions for columns, constraints, declared FK pairs, embeddings, and
sampled values. Produces `InferenceResult` — ready-to-write REFERENCES
DataFrames tagged with `EdgeSource.INFERRED_METADATA` / `EdgeSource.SEMANTIC`.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

import dbxcarta.schema_graph as sg
from dbxcarta.contract import generate_id
from dbxcarta.fk_common import (
    ColumnMeta,
    ConstraintRow,
    DeclaredPair,
    PKIndex,
)
from dbxcarta.fk_inference import infer_fk_pairs
from dbxcarta.fk_semantic import (
    ColumnEmbedding,
    ValueIndex,
    infer_semantic_pairs,
)
from dbxcarta.summary import RunSummary

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

    from dbxcarta.extract import ExtractResult
    from dbxcarta.settings import Settings
    from dbxcarta.sample_values import SampleStats

logger = logging.getLogger(__name__)


@dataclass
class InferenceResult:
    """Post-inference DataFrames ready for the Neo4j REFERENCES write.

    None when the corresponding phase produced zero rows (or was gated off).
    The pipeline's load step skips writes whose DataFrame is None."""

    metadata_inferred_df: "DataFrame | None"
    metadata_edge_count: int
    semantic_inferred_df: "DataFrame | None"
    semantic_edge_count: int


def run_inferences(
    spark: "SparkSession",
    settings: "Settings",
    schema_list: list[str],
    extract: "ExtractResult",
    sample_stats: "SampleStats | None",
    value_node_df: "DataFrame | None",
    has_value_df: "DataFrame | None",
    summary: RunSummary,
) -> InferenceResult:
    """Run Phase 3 and Phase 4 against the extracted material.

    Phase 3 is gated on DBXCARTA_INFER_METADATA. When off, Phase 4 still
    sees an empty metadata_inferred_pairs set; this is correct — Phase 4
    just has more candidates to consider.

    Phase 4 is gated on DBXCARTA_INFER_SEMANTIC and on a catalog-size floor
    (DBXCARTA_SEMANTIC_MIN_TABLES). Column embeddings are required and
    already validated present by Settings' cross-field validator.
    """
    column_metas = [ColumnMeta.from_row(r) for r in extract.columns_df.collect()]
    declared_pairs = _build_declared_pairs(extract.fk_pairs_df)
    pk_index = _build_pk_index(spark, settings, schema_list)

    # --- Phase 3 ------------------------------------------------------------
    metadata_inferred_df: "DataFrame | None" = None
    metadata_edge_count = 0
    metadata_pairs: frozenset[DeclaredPair] = frozenset()

    if settings.dbxcarta_infer_metadata:
        refs, counters = infer_fk_pairs(column_metas, pk_index, declared_pairs)
        summary.fk_metadata = counters
        logger.info(
            "[dbxcarta] metadata inference: candidates=%d accepted=%d"
            " composite_pks_skipped=%d",
            counters.candidates, counters.accepted, counters.composite_pk_skipped,
        )
        if refs:
            metadata_inferred_df = sg.build_inferred_references_rel(spark, refs)
            metadata_edge_count = len(refs)
            metadata_pairs = frozenset(
                DeclaredPair(source_id=r.source_id, target_id=r.target_id)
                for r in refs
            )

    # --- Phase 4 ------------------------------------------------------------
    semantic_inferred_df: "DataFrame | None" = None
    semantic_edge_count = 0

    if _should_run_semantic(settings, summary.extract.tables):
        embeddings = _collect_column_embeddings(extract.column_node_df)
        value_index = _build_value_index(value_node_df, has_value_df, sample_stats)

        refs, counters = infer_semantic_pairs(
            columns=column_metas,
            embeddings=embeddings,
            pk_index=pk_index,
            declared_pairs=declared_pairs,
            metadata_inferred_pairs=metadata_pairs,
            value_index=value_index,
            threshold=settings.dbxcarta_semantic_threshold,
        )
        summary.fk_semantic = counters
        logger.info(
            "[dbxcarta] semantic inference: considered=%d accepted=%d"
            " value_corroborated=%d",
            counters.considered, counters.accepted, counters.value_corroborated,
        )
        if refs:
            semantic_inferred_df = sg.build_inferred_references_rel(spark, refs)
            semantic_edge_count = len(refs)

    return InferenceResult(
        metadata_inferred_df=metadata_inferred_df,
        metadata_edge_count=metadata_edge_count,
        semantic_inferred_df=semantic_inferred_df,
        semantic_edge_count=semantic_edge_count,
    )


def _should_run_semantic(settings: "Settings", n_tables: int) -> bool:
    """Phase 4 runs iff enabled and the catalog exceeds the min-tables floor.

    On tiny catalogs, embedding similarity fires on unrelated pairs more often
    than it helps; the gate blocks that while letting small fixtures exercise
    the code path via a per-test override of DBXCARTA_SEMANTIC_MIN_TABLES.
    Logging the skip is a side effect of the runner, not this predicate.
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


def _build_declared_pairs(fk_pairs_df: "DataFrame") -> frozenset[DeclaredPair]:
    """Pipeline-edge conversion: Spark Row → frozenset[DeclaredPair]."""
    return frozenset(
        DeclaredPair(
            source_id=generate_id(
                r["src_catalog"], r["src_schema"], r["src_table"], r["src_column"],
            ),
            target_id=generate_id(
                r["tgt_catalog"], r["tgt_schema"], r["tgt_table"], r["tgt_column"],
            ),
        )
        for r in fk_pairs_df.collect()
    )


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

    constraint_rows = [ConstraintRow.from_row(r) for r in pk_rows_df.collect()]
    return PKIndex.from_constraints(constraint_rows)


def _collect_column_embeddings(column_node_df: "DataFrame") -> dict[str, ColumnEmbedding]:
    """Pipeline-edge conversion: staged column node DataFrame →
    dict[col_id → ColumnEmbedding].

    Rows with null embedding (ai_query failures, dimension mismatches) are
    skipped — Phase 4 never considers them as candidates."""
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
    or zero value_nodes). Phase 4 interprets None as "no corroboration
    signal available" and the value-bonus simply doesn't apply.

    Joins the Value nodes (`id`, `value`) against HAS_VALUE edges
    (`source_id` = col_id, `target_id` = value id) so the returned index is
    col_id → frozenset[value-text]. Single driver-side collect; acceptable
    at the Value-node cap set by DBXCARTA_SAMPLE_LIMIT."""
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
