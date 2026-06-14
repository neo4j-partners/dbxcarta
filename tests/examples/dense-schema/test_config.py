from __future__ import annotations

import pytest
from dbxcarta_dense_schema_example.questions.config import load_question_config


def _base_env(**overrides: str) -> dict[str, str]:
    env = {"DBXCARTA_CATALOG": "dense-schema-example"}
    env.update(overrides)
    return env


def test_load_question_config_defaults() -> None:
    cfg = load_question_config(_base_env())
    assert cfg.catalog == "dense-schema-example"
    assert cfg.table_count == 500
    assert cfg.questions_target == 60
    # The blueprint path resolves against the committed blueprint dir, not cwd.
    assert cfg.candidate_cache.name == "candidates_500.json"


def test_candidate_cache_tracks_table_count() -> None:
    cfg = load_question_config(_base_env(DENSE_TABLE_COUNT="1000"))
    assert cfg.table_count == 1000
    assert cfg.candidate_cache.name == "candidates_1000.json"


def test_load_question_config_rejects_project_catalog() -> None:
    with pytest.raises(ValueError, match="collides"):
        load_question_config(_base_env(DBXCARTA_CATALOG="graph-enriched-lakehouse"))


def test_load_question_config_rejects_shared_ops_catalog() -> None:
    # The data catalog must never be the shared ops catalog, or materialize
    # would write data schemas into the ops plane and collapse the separation.
    with pytest.raises(ValueError, match="collides"):
        load_question_config(_base_env(DBXCARTA_CATALOG="dbxcarta-catalog"))


def test_load_question_config_requires_catalog() -> None:
    with pytest.raises(ValueError, match="DBXCARTA_CATALOG"):
        load_question_config({})
