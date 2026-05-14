from __future__ import annotations

import pytest

from dbxcarta.presets import Preset, ReadinessReport
from dbxcarta.presets import QuestionsUploadable, ReadinessCheckable
from dbxcarta_schemapile_example.preset import (
    _QUESTIONS_FILE,
    _validate_questions_file,
    SchemaPilePreset,
    preset,
)


def test_preset_satisfies_required_protocol():
    assert isinstance(preset, Preset)


def test_preset_satisfies_optional_capabilities():
    assert isinstance(preset, ReadinessCheckable)
    assert isinstance(preset, QuestionsUploadable)


def test_env_overlay_defaults():
    env = preset.env()
    assert env["DBXCARTA_CATALOG"] == "schemapile_lakehouse"
    assert env["DATABRICKS_VOLUME_PATH"] == (
        "/Volumes/schemapile_lakehouse/_meta/schemapile_volume"
    )
    assert env["DBXCARTA_SUMMARY_TABLE"] == (
        "schemapile_lakehouse._meta.dbxcarta_run_summary"
    )
    assert env["DBXCARTA_INJECT_CRITERIA"] == "false"
    assert env["DBXCARTA_INFER_SEMANTIC"] == "true"


def test_env_overlay_picks_up_schemas_env(monkeypatch):
    monkeypatch.setenv("DBXCARTA_SCHEMAS", "sp_one,sp_two")
    env = preset.env()
    assert env["DBXCARTA_SCHEMAS"] == "sp_one,sp_two"


def test_env_overlay_empty_schemas_when_unset(monkeypatch):
    monkeypatch.delenv("DBXCARTA_SCHEMAS", raising=False)
    env = preset.env()
    assert env["DBXCARTA_SCHEMAS"] == ""


def test_preset_rejects_invalid_catalog():
    with pytest.raises(ValueError):
        SchemaPilePreset(catalog="bad catalog name")


def test_schemas_list_rejects_invalid_names(monkeypatch):
    monkeypatch.setenv("DBXCARTA_SCHEMAS", "good, bad name")
    with pytest.raises(ValueError):
        preset.schemas_list()


def test_readiness_reports_missing_when_schemas_empty(monkeypatch):
    monkeypatch.delenv("DBXCARTA_SCHEMAS", raising=False)
    report = preset.readiness(ws=None, warehouse_id="abc")  # type: ignore[arg-type]
    assert not report.ok()
    assert any("materialize" in m for m in report.missing_required)


def test_readiness_report_format_contains_status():
    report = ReadinessReport(
        catalog="schemapile_lakehouse", schema="sp_a",
        present=("sp_a",), missing_required=(), missing_optional=(),
    )
    formatted = report.format()
    assert "status: ready" in formatted
    assert "scope: schemapile_lakehouse.sp_a" in formatted


def test_default_questions_file_points_to_example_fixture():
    assert _QUESTIONS_FILE.name == "questions.json"
    assert _QUESTIONS_FILE.parent.name == "schemapile"


def test_validate_questions_file_rejects_empty_array(tmp_path):
    path = tmp_path / "questions.json"
    path.write_text("[]")
    with pytest.raises(ValueError):
        _validate_questions_file(path)


def test_validate_questions_file_requires_question_fields(tmp_path):
    path = tmp_path / "questions.json"
    path.write_text('[{"question_id": "q1"}]')
    with pytest.raises(ValueError):
        _validate_questions_file(path)
