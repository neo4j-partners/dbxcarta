from __future__ import annotations

import pytest

from dbxcarta_schemapile_example.config import load_config
from dbxcarta_schemapile_example.utils import read_required_warehouse_id


def _base_env(tmp_path, **overrides):
    env = {
        "SCHEMAPILE_REPO": str(tmp_path),
        "DBXCARTA_CATALOG": "schemapile_lakehouse",
    }
    env.update(overrides)
    return env


def test_load_config_defaults(tmp_path):
    cfg = load_config(_base_env(tmp_path))
    assert cfg.target_tables == 1000
    assert cfg.strategy == "random"
    assert cfg.seed == 42
    assert cfg.catalog == "schemapile_lakehouse"
    assert cfg.meta_schema == "_meta"
    assert cfg.volume == "schemapile_volume"


def test_load_config_rejects_project_catalog(tmp_path):
    with pytest.raises(ValueError) as exc:
        load_config(_base_env(tmp_path, DBXCARTA_CATALOG="graph-enriched-lakehouse"))
    assert "collides" in str(exc.value)


def test_load_config_rejects_project_catalog_case_insensitive(tmp_path):
    with pytest.raises(ValueError) as exc:
        load_config(_base_env(tmp_path, DBXCARTA_CATALOG="Graph-Enriched-Lakehouse"))
    assert "collides" in str(exc.value)


def test_load_config_requires_repo(tmp_path):
    env = _base_env(tmp_path)
    env["SCHEMAPILE_REPO"] = ""
    with pytest.raises(ValueError) as exc:
        load_config(env)
    assert "SCHEMAPILE_REPO" in str(exc.value)


def test_load_config_truthy_parsing(tmp_path):
    cfg = load_config(_base_env(tmp_path, SCHEMAPILE_REQUIRE_SELF_CONTAINED="false"))
    assert cfg.require_self_contained is False
    cfg2 = load_config(_base_env(tmp_path, SCHEMAPILE_REQUIRE_SELF_CONTAINED="YES"))
    assert cfg2.require_self_contained is True


def test_volume_path_is_volumes_subpath(tmp_path):
    cfg = load_config(_base_env(tmp_path))
    assert cfg.volume_path == "/Volumes/schemapile_lakehouse/_meta/schemapile_volume"


def test_read_required_warehouse_id_strips_override(monkeypatch):
    monkeypatch.setenv("DATABRICKS_WAREHOUSE_ID", "ignored")

    warehouse_id = read_required_warehouse_id(" wh-1 ", operation="test")

    assert warehouse_id == "wh-1"


def test_read_required_warehouse_id_rejects_blank_env(monkeypatch):
    monkeypatch.setenv("DATABRICKS_WAREHOUSE_ID", "   ")

    with pytest.raises(ValueError, match="DATABRICKS_WAREHOUSE_ID"):
        read_required_warehouse_id(None, operation="test")
