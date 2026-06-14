from __future__ import annotations

import pytest
from dbxcarta_schemapile_example.dataset.config import (
    load_candidate_config,
    load_slice_config,
)
from dbxcarta_schemapile_example.questions.config import load_question_config


def test_load_slice_config_defaults(tmp_path):
    cfg = load_slice_config({"SCHEMAPILE_REPO": str(tmp_path)})
    assert cfg.target_tables == 1000
    assert cfg.strategy == "random"
    assert cfg.seed == 42
    assert cfg.repo == tmp_path.resolve()


def test_load_slice_config_requires_repo():
    with pytest.raises(ValueError, match="SCHEMAPILE_REPO"):
        load_slice_config({"SCHEMAPILE_REPO": ""})


def test_load_slice_config_truthy_parsing(tmp_path):
    base = {"SCHEMAPILE_REPO": str(tmp_path)}
    assert load_slice_config({**base, "SCHEMAPILE_REQUIRE_SELF_CONTAINED": "false"}).require_self_contained is False
    assert load_slice_config({**base, "SCHEMAPILE_REQUIRE_SELF_CONTAINED": "YES"}).require_self_contained is True


def test_load_slice_config_needs_no_catalog_or_volume(tmp_path):
    # The host-only slice step reads neither DBXCARTA_CATALOG nor an ops volume;
    # it must load with only the SCHEMAPILE_* parameters present.
    cfg = load_slice_config({"SCHEMAPILE_REPO": str(tmp_path)})
    assert cfg.min_tables == 2


def test_load_candidate_config_defaults():
    cfg = load_candidate_config({})
    assert cfg.candidate_min_tables == 3
    assert cfg.candidate_limit == 20
    assert cfg.candidate_cache.name == "candidates_random_1000.json"


def test_load_question_config_defaults():
    cfg = load_question_config({"DBXCARTA_CATALOG": "schemapile_lakehouse"})
    assert cfg.catalog == "schemapile_lakehouse"
    assert cfg.question_model == "databricks-meta-llama-3-3-70b-instruct"
    assert cfg.seed == 42


def test_load_question_config_requires_catalog():
    with pytest.raises(ValueError, match="DBXCARTA_CATALOG"):
        load_question_config({})


def test_load_question_config_rejects_project_catalog():
    with pytest.raises(ValueError, match="collides"):
        load_question_config({"DBXCARTA_CATALOG": "graph-enriched-lakehouse"})


def test_load_question_config_rejects_shared_ops_catalog():
    # The data catalog must never be the shared ops catalog, or the generated
    # SQL would target the ops plane instead of the example's own tables.
    with pytest.raises(ValueError, match="collides"):
        load_question_config({"DBXCARTA_CATALOG": "dbxcarta-catalog"})


def test_load_question_config_rejects_project_catalog_case_insensitive():
    with pytest.raises(ValueError, match="collides"):
        load_question_config({"DBXCARTA_CATALOG": "Graph-Enriched-Lakehouse"})
