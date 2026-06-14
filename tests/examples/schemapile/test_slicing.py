from __future__ import annotations

import json

import pytest
from dbxcarta_schemapile_example.dataset.config import SliceConfig
from dbxcarta_schemapile_example.dataset.slicing import (
    _cache_is_current,
    _params_fingerprint,
    _params_sidecar,
    preflight,
)


def _make_config(tmp_path, **overrides) -> SliceConfig:
    repo = tmp_path / "schemapile-repo"
    repo.mkdir(exist_ok=True)
    (repo / "slice.py").write_text("# upstream slice.py placeholder")
    (repo / "schemapile-perm.json").write_text("{}")
    defaults = {
        "repo": repo,
        "input_filename": "schemapile-perm.json",
        "target_tables": 1000,
        "strategy": "random",
        "seed": 42,
        "min_tables": 2,
        "max_tables": 100,
        "min_fk_edges": 1,
        "require_self_contained": True,
        "require_data": False,
        "slice_cache": tmp_path / "cache" / "slice.json",
    }
    defaults.update(overrides)
    return SliceConfig(**defaults)


def test_preflight_ok(tmp_path):
    config = _make_config(tmp_path)
    preflight(config)


def test_preflight_missing_repo(tmp_path):
    config = _make_config(tmp_path)
    config = SliceConfig(**{**config.__dict__, "repo": tmp_path / "missing"})
    with pytest.raises(FileNotFoundError) as exc:
        preflight(config)
    assert "https://github.com/amsterdata/schemapile" in str(exc.value)


def test_preflight_missing_slice_script(tmp_path):
    config = _make_config(tmp_path)
    (config.repo / "slice.py").unlink()
    with pytest.raises(FileNotFoundError) as exc:
        preflight(config)
    assert "slice.py" in str(exc.value)


def test_preflight_missing_data_file(tmp_path):
    config = _make_config(tmp_path)
    config.input_path.unlink()
    with pytest.raises(FileNotFoundError) as exc:
        preflight(config)
    assert "schemapile-perm.json" in str(exc.value)


def test_cache_is_current_round_trip(tmp_path):
    config = _make_config(tmp_path)
    config.slice_cache.parent.mkdir(parents=True, exist_ok=True)
    config.slice_cache.write_text("{}")
    _params_sidecar(config.slice_cache).write_text(json.dumps(_params_fingerprint(config)))
    assert _cache_is_current(config) is True


def test_cache_is_not_current_when_params_change(tmp_path):
    config = _make_config(tmp_path)
    config.slice_cache.parent.mkdir(parents=True, exist_ok=True)
    config.slice_cache.write_text("{}")
    other = _make_config(tmp_path, target_tables=500)
    _params_sidecar(other.slice_cache).write_text(json.dumps(_params_fingerprint(other)))
    # cache file matches `config`, but sidecar was written for `other` params.
    # Reading sidecar back for `config` should report not-current.
    assert _cache_is_current(config) is False


def test_cache_is_not_current_when_sidecar_missing(tmp_path):
    config = _make_config(tmp_path)
    config.slice_cache.parent.mkdir(parents=True, exist_ok=True)
    config.slice_cache.write_text("{}")
    assert _cache_is_current(config) is False
