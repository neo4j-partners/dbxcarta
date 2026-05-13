"""Unit tests for client question loading and upload shape."""

from __future__ import annotations

import json
from types import SimpleNamespace

from dbxcarta.client.client import manage_questions
from dbxcarta.client.questions import Question, load_questions


def test_question_validates_schema_alias_and_blank_optionals() -> None:
    q = Question.model_validate({
        "question_id": " q1 ",
        "question": " who? ",
        "reference_sql": " ",
        "schema": "s1",
    })

    assert q.question_id == "q1"
    assert q.question == "who?"
    assert q.reference_sql is None
    assert q.schema_ == "s1"
    assert q["schema"] == "s1"


def test_load_questions_from_json_preserves_schema(tmp_path) -> None:
    path = tmp_path / "questions.json"
    path.write_text(json.dumps([
        {
            "question_id": "q1",
            "question": "Who?",
            "reference_sql": "SELECT 1",
            "schema": "target_schema",
        }
    ]))

    questions = load_questions(str(path))

    assert len(questions) == 1
    assert questions[0].schema_ == "target_schema"


def test_manage_questions_writes_schema_column(tmp_path) -> None:
    path = tmp_path / "questions.json"
    path.write_text(json.dumps([
        {
            "question_id": "q1",
            "question": "Who?",
            "notes": "note",
            "reference_sql": "SELECT 1",
            "schema": "target_schema",
        }
    ]))

    spark = _SparkStub()
    settings = SimpleNamespace(dbxcarta_summary_table="cat.meta.run_summary")

    manage_questions(spark, settings, str(path))

    assert spark.rows == [(
        "q1",
        "Who?",
        "note",
        "SELECT 1",
        "target_schema",
    )]
    assert [field.name for field in spark.schema.fields] == [
        "question_id",
        "question",
        "notes",
        "reference_sql",
        "schema",
    ]


class _SparkStub:
    def __init__(self) -> None:
        self.rows = None
        self.schema = None

    def createDataFrame(self, rows, schema):
        self.rows = rows
        self.schema = schema
        return _DataFrameStub()


class _DataFrameStub:
    @property
    def write(self):
        return _WriterStub()


class _WriterStub:
    def format(self, _format: str):
        return self

    def mode(self, _mode: str):
        return self

    def option(self, _key: str, _value: str):
        return self

    def saveAsTable(self, _table: str) -> None:
        return None
