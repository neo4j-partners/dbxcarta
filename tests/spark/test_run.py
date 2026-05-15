from __future__ import annotations

from typing import Any

import pytest

import dbxcarta.spark.run as pipeline
from dbxcarta.core import SemanticLayerConfig
from dbxcarta.spark.settings import SparkIngestSettings


def test_run_dbxcarta_accepts_explicit_settings_and_spark(monkeypatch) -> None:
    settings = SparkIngestSettings(
        dbxcarta_catalog="main",
        dbxcarta_schemas="schema_one,schema_two",
        dbxcarta_summary_volume="/Volumes/main/default/dbxcarta/summaries",
        dbxcarta_summary_table="main.default.dbxcarta_runs",
    )
    fake_spark = object()
    calls: dict[str, Any] = {}

    def fake_run(spark, passed_settings, schema_list, summary) -> None:
        calls["run"] = (spark, passed_settings, schema_list, summary.status)

    def fake_emit(summary, spark, volume_path, table_name) -> None:
        calls["emit"] = (spark, volume_path, table_name, summary.status)

    monkeypatch.setattr(pipeline, "_run", fake_run)
    monkeypatch.setattr(pipeline.RunSummary, "emit", fake_emit)

    pipeline.run_dbxcarta(settings=settings, spark=fake_spark)

    assert calls["run"] == (
        fake_spark,
        settings,
        ["schema_one", "schema_two"],
        "running",
    )
    assert calls["emit"] == (
        fake_spark,
        "/Volumes/main/default/dbxcarta/summaries",
        "main.default.dbxcarta_runs",
        "success",
    )


def test_run_dbxcarta_accepts_semantic_config(
    monkeypatch,
    tmp_path,
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("DBXCARTA_CATALOG", raising=False)
    monkeypatch.delenv("DBXCARTA_SCHEMAS", raising=False)
    monkeypatch.setenv(
        "DBXCARTA_SUMMARY_VOLUME", "/Volumes/main/default/dbxcarta/summaries"
    )
    monkeypatch.setenv("DBXCARTA_SUMMARY_TABLE", "main.default.dbxcarta_runs")

    fake_spark = object()
    calls: dict[str, Any] = {}

    def fake_run(spark, passed_settings, schema_list, summary) -> None:
        calls["run"] = (spark, passed_settings, schema_list, summary.catalog)

    def fake_emit(summary, spark, volume_path, table_name) -> None:
        calls["emit"] = (spark, volume_path, table_name, summary.status)

    monkeypatch.setattr(pipeline, "_run", fake_run)
    monkeypatch.setattr(pipeline.RunSummary, "emit", fake_emit)

    pipeline.run_dbxcarta(
        semantic_config=SemanticLayerConfig(
            source_catalog="from_core",
            source_schemas="alpha,beta",
        ),
        spark=fake_spark,
    )

    _, passed_settings, schema_list, catalog = calls["run"]
    assert passed_settings.dbxcarta_catalog == "from_core"
    assert schema_list == ["alpha", "beta"]
    assert catalog == "from_core"
    assert calls["emit"] == (
        fake_spark,
        "/Volumes/main/default/dbxcarta/summaries",
        "main.default.dbxcarta_runs",
        "success",
    )


def test_run_dbxcarta_rejects_settings_and_semantic_config_together() -> None:
    settings = SparkIngestSettings(
        dbxcarta_catalog="main",
        dbxcarta_summary_volume="/Volumes/main/default/dbxcarta/summaries",
        dbxcarta_summary_table="main.default.dbxcarta_runs",
    )

    with pytest.raises(ValueError, match="either settings or semantic_config"):
        pipeline.run_dbxcarta(
            settings=settings,
            semantic_config=SemanticLayerConfig(source_catalog="main"),
            spark=object(),
        )
