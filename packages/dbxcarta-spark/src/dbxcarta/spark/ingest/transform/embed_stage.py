"""Embedding transform stage.

Owns the materialize-once embed+stage step (with optional ledger reuse) and
the post-embedding failure-rate gate. The per-label embedding-text
expressions are no longer here: each node builder attaches a single
`embedding_text` column from the one source of truth
(contract.EMBEDDING_TEXT_EXPR), and this stage simply hashes and embeds it.
``run.py`` calls ``transform_embeddings`` / ``check_thresholds``; the
sample-values stage calls ``embed_label`` to embed Value nodes through the
same path.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import dbxcarta.spark.ingest.transform.embeddings as emb
from dbxcarta.spark.contract import NodeLabel
from dbxcarta.spark.ingest.extract import ExtractResult
from dbxcarta.spark.ingest.summary import RunSummary
from dbxcarta.spark.ingest.transform.ledger import (
    read_ledger,
    split_by_ledger,
    upsert_ledger,
)
from dbxcarta.spark.ingest.transform.staging import stage_embedded_nodes
from dbxcarta.spark.settings import SparkIngestSettings

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)


def embed_label(
    df: "DataFrame", label: NodeLabel,
    settings: SparkIngestSettings, staging_path: str, ledger_path: str,
    summary: RunSummary,
) -> "DataFrame":
    """Embed `df` for `label` (reading its builder-attached `embedding_text`
    column), stage to Delta once, and record failure stats into `summary`."""
    return _embed_and_stage(
        df, label, settings, staging_path, ledger_path, summary,
    )


def transform_embeddings(
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
        node_dfs[label] = embed_label(
            node_dfs[label], label,
            settings, staging_path, ledger_path, summary,
        )

    extract_result.table_node_df = node_dfs[NodeLabel.TABLE]
    extract_result.column_node_df = node_dfs[NodeLabel.COLUMN]
    extract_result.schema_node_df = node_dfs[NodeLabel.SCHEMA]
    extract_result.database_df = node_dfs[NodeLabel.DATABASE]


def check_thresholds(settings: SparkIngestSettings, summary: RunSummary) -> None:
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


def _embed_and_stage(
    df: "DataFrame", label: NodeLabel,
    settings: SparkIngestSettings, staging_path: str, ledger_path: str,
    summary: RunSummary,
) -> "DataFrame":
    """Embed df, stage to Delta once, compute failure stats into summary.

    `df` carries a builder-attached `embedding_text` column; the hash and
    the ai_query input both derive from it, so the ledger never churns from
    a hash/embed mismatch.

    When DBXCARTA_LEDGER_ENABLED, rows whose embedding_text_hash and model
    already exist in the per-label ledger are served from the ledger (no
    ai_query call). Only misses call the endpoint; the ledger is then upserted
    with the newly-computed vectors (excluding error rows).
    """
    from pyspark.sql.functions import col, lit, sha2
    from pyspark.sql.types import ArrayType, DoubleType

    endpoint = settings.dbxcarta_embedding_endpoint
    dimension = settings.dbxcarta_embedding_dimension

    if settings.dbxcarta_ledger_enabled:
        spark = df.sparkSession
        ledger_df = read_ledger(spark, ledger_path, label)

        if ledger_df is not None:
            df_hashed = df.withColumn("_curr_hash", sha2(col("embedding_text"), 256))
            hits_df, misses_df = split_by_ledger(df_hashed, ledger_df, endpoint)
            hit_count = hits_df.count()
            summary.embeddings.ledger_hits[label] = hit_count

            # `embedding_text` is now a builder column, so it is in
            # df.columns. The miss branch (add_embedding_column) drops it;
            # exclude it here too so both unionByName arms — and the Delta
            # staging schema — match.
            hit_final = hits_df.select(
                *[col(c) for c in df.columns if c != "embedding_text"],
                col("_curr_hash").alias("embedding_text_hash"),
                col("_led_embedding").cast(ArrayType(DoubleType())).alias("embedding"),
                lit(None).cast("string").alias("embedding_error"),
                col("_led_model").alias("embedding_model"),
                col("_led_embedded_at").alias("embedded_at"),
            )

            embedded_misses = emb.add_embedding_column(
                misses_df, endpoint, dimension, label=label.value,
            )
            enriched = hit_final.unionByName(embedded_misses)
        else:
            summary.embeddings.ledger_hits[label] = 0
            enriched = emb.add_embedding_column(
                df, endpoint, dimension, label=label.value,
            )
    else:
        enriched = emb.add_embedding_column(
            df, endpoint, dimension, label=label.value,
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
