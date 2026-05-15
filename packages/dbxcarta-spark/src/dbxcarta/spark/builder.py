"""Spark implementation of the SemanticLayerBuilder protocol.

Adapts a backend-neutral `SemanticLayerConfig` to the Spark-specific
`SparkIngestSettings` and invokes `run_dbxcarta`. Settings the neutral config
does not expose (summary sinks, secret scope, feature flags) come from the
process environment via `SparkIngestSettings.from_semantic_config()`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from dbxcarta.core.builder import SemanticLayerResult
from dbxcarta.core.contract import NodeLabel, RelType
from dbxcarta.core.settings import SemanticLayerConfig
from dbxcarta.spark.run import run_dbxcarta
from dbxcarta.spark.settings import SparkIngestSettings

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from dbxcarta.spark.ingest.summary import RunSummary


class SparkSemanticLayerBuilder:
    """Build a dbxcarta semantic layer using the Databricks Spark pipeline."""

    def __init__(self, *, spark: "SparkSession | None" = None) -> None:
        self._spark = spark

    def build_semantic_layer(self, config: SemanticLayerConfig) -> SemanticLayerResult:
        settings = SparkIngestSettings.from_semantic_config(config)
        summary = run_dbxcarta(settings=settings, spark=self._spark)
        return _result_from_summary(summary)


def _result_from_summary(summary: "RunSummary") -> SemanticLayerResult:
    """Project a RunSummary onto the backend-neutral SemanticLayerResult."""
    node_labels = {label.value for label in NodeLabel}
    rel_types = {rel.value for rel in RelType}
    node_counts = {k: v for k, v in summary.neo4j_counts.items() if k in node_labels}
    edge_counts = {k: v for k, v in summary.neo4j_counts.items() if k in rel_types}
    return SemanticLayerResult(
        run_id=summary.run_id,
        contract_version=summary.contract_version,
        catalog=summary.catalog,
        schemas=tuple(summary.schemas),
        node_counts=node_counts,
        edge_counts=edge_counts,
    )
