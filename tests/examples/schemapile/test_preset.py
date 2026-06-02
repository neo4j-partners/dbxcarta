from __future__ import annotations

import pytest

from dbxcarta.spark.presets import (
    QuestionsUploadable,
    ReadinessCheckable,
    ReadinessReport,
)
from dbxcarta.spark.presets import Preset
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


def test_preset_rejects_invalid_catalog():
    with pytest.raises(ValueError):
        SchemaPilePreset(catalog="bad catalog name")


class _FakeStatementExecution:
    def __init__(self, schema_rows):
        self._schema_rows = schema_rows

    def execute_statement(self, **kwargs):
        data_array = [[name] for name in self._schema_rows]
        return type(
            "Resp",
            (),
            {"result": type("Result", (), {"data_array": data_array})()},
        )()


class _FakeWorkspaceClient:
    def __init__(self, schema_rows):
        self.statement_execution = _FakeStatementExecution(schema_rows)


def test_readiness_reports_missing_when_only_information_schema():
    ws = _FakeWorkspaceClient(["information_schema"])
    report = preset.readiness(ws=ws, warehouse_id="abc")  # type: ignore[arg-type]
    assert not report.ok()
    assert any("materialize" in m for m in report.missing_required)


def test_readiness_ready_when_schemas_present():
    ws = _FakeWorkspaceClient(["information_schema", "sp_a", "sp_b"])
    report = preset.readiness(ws=ws, warehouse_id="abc")  # type: ignore[arg-type]
    assert report.ok()
    assert report.present == ("sp_a", "sp_b")


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
