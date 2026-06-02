from __future__ import annotations

import pytest

from dbxcarta_dense_schema_example.config import load_config


def _base_env(**overrides: str) -> dict[str, str]:
    env = {"DBXCARTA_CATALOG": "dense-schema_example"}
    env.update(overrides)
    return env


def test_load_config_defaults() -> None:
    cfg = load_config(_base_env())
    assert cfg.catalog == "dense-schema_example"
    assert cfg.meta_schema == "_meta"
    assert cfg.volume == "schemapile_volume"


def test_load_config_rejects_project_catalog() -> None:
    with pytest.raises(ValueError, match="collides"):
        load_config(_base_env(DBXCARTA_CATALOG="graph-enriched-lakehouse"))


def test_volume_path_uses_env_ops_path() -> None:
    cfg = load_config(
        _base_env(
            DATABRICKS_VOLUME_PATH="/Volumes/dbxcarta-catalog/dense_ops/dbxcarta-ops",
        )
    )
    assert cfg.volume_path == "/Volumes/dbxcarta-catalog/dense_ops/dbxcarta-ops"
    # questions_path defaults under the ops volume, not the data catalog.
    assert cfg.questions_path == (
        "/Volumes/dbxcarta-catalog/dense_ops/dbxcarta-ops/dbxcarta/dense_questions.json"
    )


def test_volume_path_falls_back_to_in_catalog_derivation() -> None:
    # With no DATABRICKS_VOLUME_PATH the legacy in-catalog path is the fallback.
    cfg = load_config(_base_env())
    assert cfg.volume_path == "/Volumes/dense-schema_example/_meta/schemapile_volume"
