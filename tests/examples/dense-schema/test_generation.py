"""Tests for the Databricks-connected question-generation helpers."""

from __future__ import annotations

from pathlib import Path

from dbxcarta_dense_schema_example.questions.config import QuestionConfig
from dbxcarta_dense_schema_example.questions.generation import (
    _default_cache_dir,
    _format_sample_rows,
)


def test_question_cache_dir_defaults_to_table_count():
    config = QuestionConfig(
        catalog="dense-schema-example",
        table_count=1000,
        candidate_cache=Path(".cache/candidates_1000.json"),
        question_model="databricks-meta-llama-3-3-70b-instruct",
        questions_target=60,
        questions_per_batch=3,
        question_temperature=0.2,
        seed=42,
    )

    assert _default_cache_dir(config).as_posix() == ".cache/questions_1000"


def test_question_prompt_includes_sample_rows():
    sample = _format_sample_rows(
        [("id", {"type": "INT"}), ("status", {"type": "STRING"})],
        [[1, "approved"], [2, "pending"]],
    )

    assert '"status": "approved"' in sample
    assert '"status": "pending"' in sample
