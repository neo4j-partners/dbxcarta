from __future__ import annotations

from pathlib import Path

import pytest
from dotenv import dotenv_values

import dbxcarta_finance_genie_example.finance_genie as preset_module
from dbxcarta.spark.presets import (
    QuestionsUploadable,
    ReadinessReport,
    ReadinessCheckable,
)
from dbxcarta.spark.loader import load_preset
from dbxcarta.spark.presets import Preset
from dbxcarta.spark.settings import SparkIngestSettings
from dbxcarta_finance_genie_example.finance_genie import (
    FinanceGeniePreset,
    _EXPECTED_TABLES,
    preset,
)

# The committed overlay is now the single source of per-example dbxcarta config.
_OVERLAY = (
    Path(__file__).resolve().parents[3]
    / "examples"
    / "finance-genie"
    / "dbxcarta-overlay.env"
)


def test_preset_satisfies_required_protocol() -> None:
    assert isinstance(preset, Preset)


def test_preset_satisfies_optional_capabilities() -> None:
    assert isinstance(preset, ReadinessCheckable)
    assert isinstance(preset, QuestionsUploadable)


def test_minimal_object_satisfies_marker_protocol() -> None:
    """Preset is a marker protocol; any object satisfies it. Behavior comes
    from the optional capability protocols."""

    class Minimal:
        pass

    minimal = Minimal()
    assert isinstance(minimal, Preset)
    assert not isinstance(minimal, ReadinessCheckable)
    assert not isinstance(minimal, QuestionsUploadable)


def test_preset_resolvable_via_import_path() -> None:
    resolved = load_preset("dbxcarta_finance_genie_example:preset")
    assert resolved is preset


def test_overlay_builds_valid_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    for key, value in dotenv_values(_OVERLAY).items():
        if value is not None:
            monkeypatch.setenv(key, value)
    settings = SparkIngestSettings()
    assert settings.databricks_secret_scope == "dbxcarta-neo4j-finance-genie"
    assert settings.dbxcarta_catalog == "graph-enriched-finance-silver"
    assert settings.dbxcarta_schemas == "graph-enriched-schema"
    assert settings.resolved_catalogs() == [
        "graph-enriched-finance-bronze",
        "graph-enriched-finance-silver",
        "graph-enriched-finance-gold",
    ]
    assert settings.layer_map() == {
        "graph-enriched-finance-bronze": "bronze",
        "graph-enriched-finance-silver": "silver",
        "graph-enriched-finance-gold": "gold",
    }


def test_overlay_pins_known_keys() -> None:
    env = dotenv_values(_OVERLAY)
    assert env["DBXCARTA_INJECT_CRITERIA"] == "false"
    assert env["DBXCARTA_CLIENT_ARMS"] == "no_context,schema_dump,graph_rag"
    assert env["DBXCARTA_CLIENT_QUESTIONS"].endswith("/dbxcarta/questions.json")


def test_volume_path_is_volumes_subpath() -> None:
    assert preset.volume_path.startswith("/Volumes/")
    assert preset.volume_path.count("/") == 4


def test_readiness_all_present(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        preset_module, "_fetch_table_names",
        lambda ws, wh, c, s: list(_EXPECTED_TABLES),
    )
    report = preset.readiness(ws=None, warehouse_id="abc")  # type: ignore[arg-type]
    assert report.ok(strict_optional=True)
    assert report.missing_required == ()
    assert report.missing_optional == ()
    assert len(report.present) == len(_EXPECTED_TABLES)


def test_readiness_missing_required(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        preset_module, "_fetch_table_names",
        lambda ws, wh, c, s: ["accounts", "merchants"],
    )
    report = preset.readiness(ws=None, warehouse_id="abc")  # type: ignore[arg-type]
    assert not report.ok()
    assert set(report.missing_required) == {"transactions", "account_links", "account_labels"}


def test_readiness_missing_optional_lenient_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        preset_module, "_fetch_table_names",
        lambda ws, wh, c, s: [
            "accounts", "merchants", "transactions",
            "account_links", "account_labels",
        ],
    )
    report = preset.readiness(ws=None, warehouse_id="abc")  # type: ignore[arg-type]
    assert report.ok()
    assert not report.ok(strict_optional=True)
    assert set(report.missing_optional) == {
        "gold_accounts",
        "gold_account_similarity_pairs",
        "gold_fraud_ring_communities",
    }


def test_readiness_report_format_contains_expected_lines() -> None:
    report = ReadinessReport(
        catalog="c",
        schema="s",
        present=("a", "b"),
        missing_required=("x",),
        missing_optional=("y",),
    )
    formatted = report.format()
    assert "scope: c.s" in formatted
    assert "missing required: x" in formatted
    assert "status: not ready" in formatted


def test_preset_rejects_invalid_identifier() -> None:
    with pytest.raises(ValueError):
        FinanceGeniePreset(catalog="invalid catalog name with spaces")
