"""Sample-values transform stage.

Samples distinct column values, optionally embeds the resulting Value nodes
through the shared embedding path, and packages the output for FK discovery
and the Neo4j load.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

import dbxcarta.spark.ingest.transform.sample_values as sv
from dbxcarta.spark.contract import NodeLabel
from dbxcarta.spark.ingest.extract import ExtractResult
from dbxcarta.spark.ingest.summary import RunSummary, SampleValueCounts
from dbxcarta.spark.ingest.transform.embed_stage import embed_label
from dbxcarta.spark.settings import SparkIngestSettings

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)


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
    # Cached sampled-value DataFrame backing value_node_df and has_value_df.
    # None when nothing was cached. The pipeline releases this only after FK
    # discovery and the Neo4j load have finished, mirroring
    # ExtractResult.unpersist_cached().
    cache_handle: "DataFrame | None" = None

    def unpersist_cached(self) -> None:
        """Release the cached sampled-value DataFrame after the ingest run.

        Sampling caches raw_df because the semantic value-index build and the
        HAS_VALUE write both read it. The pipeline calls this once those steps
        no longer need the cache. Idempotent and a no-op when nothing cached.
        """
        if self.cache_handle is not None:
            self.cache_handle.unpersist()


def transform_sample_values(
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

    value_node_df, has_value_df, sample_stats, cache_handle = sv.sample(
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
        value_node_df = embed_label(
            value_node_df, NodeLabel.VALUE,
            settings, staging_path, ledger_path, summary,
        )

    return ValueResult(
        value_node_df=value_node_df,
        has_value_df=has_value_df,
        sample_stats=sample_stats,
        cache_handle=cache_handle,
    )
