"""Tests for `dbxcarta.spark.builder.SparkSemanticLayerBuilder`."""

from __future__ import annotations

import pytest

import dbxcarta.spark.builder as builder_module
from dbxcarta.core import (
    DEFAULT_EMBEDDING_ENDPOINT,
    NodeLabel,
    RelType,
    SemanticLayerBuilder,
    SemanticLayerConfig,
    SemanticLayerResult,
)
from dbxcarta.spark.builder import SparkSemanticLayerBuilder
from dbxcarta.spark.ingest.summary import RunSummary
from dbxcarta.spark.settings import SparkIngestSettings


def _baseline_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Provide the non-overridable fields SparkIngestSettings requires."""
    monkeypatch.setenv("DBXCARTA_CATALOG", "env_catalog")
    monkeypatch.setenv("DBXCARTA_SCHEMAS", "env_schema")
    monkeypatch.setenv(
        "DBXCARTA_SUMMARY_VOLUME", "/Volumes/main/default/dbxcarta/summaries"
    )
    monkeypatch.setenv("DBXCARTA_SUMMARY_TABLE", "main.default.dbxcarta_runs")


def test_spark_builder_satisfies_protocol() -> None:
    assert isinstance(SparkSemanticLayerBuilder(), SemanticLayerBuilder)


def test_build_overrides_catalog_and_schemas_from_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _baseline_env(monkeypatch)
    captured: dict[str, SparkIngestSettings] = {}

    def fake_run(*, settings: SparkIngestSettings, spark: object | None) -> RunSummary:
        captured["settings"] = settings
        return RunSummary(
            run_id="r1",
            job_name="dbxcarta",
            contract_version="1.0",
            catalog=settings.dbxcarta_catalog,
            schemas=[s for s in settings.dbxcarta_schemas.split(",") if s],
        )

    monkeypatch.setattr(builder_module, "run_dbxcarta", fake_run)

    config = SemanticLayerConfig(
        source_catalog="from_config",
        source_schemas="alpha,beta",
    )
    result = SparkSemanticLayerBuilder().build_semantic_layer(config)

    settings = captured["settings"]
    assert settings.dbxcarta_catalog == "from_config"
    assert settings.dbxcarta_schemas == "alpha,beta"
    # embedding endpoint defaulted from core; matches the spark default.
    assert settings.dbxcarta_embedding_endpoint == DEFAULT_EMBEDDING_ENDPOINT
    assert isinstance(result, SemanticLayerResult)
    assert result.run_id == "r1"
    assert result.catalog == "from_config"
    assert result.schemas == ("alpha", "beta")


def test_blank_config_schemas_falls_back_to_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _baseline_env(monkeypatch)
    captured: dict[str, SparkIngestSettings] = {}

    def fake_run(*, settings: SparkIngestSettings, spark: object | None) -> RunSummary:
        captured["settings"] = settings
        return RunSummary(
            run_id="r2",
            job_name="dbxcarta",
            contract_version="1.0",
            catalog=settings.dbxcarta_catalog,
            schemas=[s for s in settings.dbxcarta_schemas.split(",") if s],
        )

    monkeypatch.setattr(builder_module, "run_dbxcarta", fake_run)

    config = SemanticLayerConfig(source_catalog="from_config")
    SparkSemanticLayerBuilder().build_semantic_layer(config)

    assert captured["settings"].dbxcarta_schemas == "env_schema"


def test_result_partitions_neo4j_counts_by_label_and_rel(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _baseline_env(monkeypatch)

    def fake_run(*, settings: SparkIngestSettings, spark: object | None) -> RunSummary:
        summary = RunSummary(
            run_id="r3",
            job_name="dbxcarta",
            contract_version="1.0",
            catalog=settings.dbxcarta_catalog,
            schemas=[],
        )
        summary.neo4j_counts = {
            NodeLabel.TABLE.value: 7,
            NodeLabel.COLUMN.value: 42,
            RelType.HAS_COLUMN.value: 42,
            RelType.REFERENCES.value: 3,
            "OtherUntrackedKey": 99,
        }
        return summary

    monkeypatch.setattr(builder_module, "run_dbxcarta", fake_run)

    result = SparkSemanticLayerBuilder().build_semantic_layer(
        SemanticLayerConfig(source_catalog="c"),
    )

    assert result.node_counts == {"Table": 7, "Column": 42}
    assert result.edge_counts == {"HAS_COLUMN": 42, "REFERENCES": 3}
