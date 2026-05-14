"""Unit tests for `dbxcarta.entrypoints._bootstrap.inject_params`."""

from __future__ import annotations

import sys
from dataclasses import dataclass

import pytest

from dbxcarta.entrypoints import _bootstrap


@dataclass(frozen=True)
class _FakePreset:
    """Test double satisfying the Preset protocol."""

    payload: dict[str, str]

    def env(self) -> dict[str, str]:
        return dict(self.payload)


def test_inject_params_overlays_key_value_args(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(sys, "argv", ["prog", "FOO=bar", "BAZ=qux", "leftover"])
    monkeypatch.delenv("FOO", raising=False)
    monkeypatch.delenv("BAZ", raising=False)

    _bootstrap.inject_params()

    import os

    assert os.environ["FOO"] == "bar"
    assert os.environ["BAZ"] == "qux"
    assert sys.argv[1:] == ["leftover"]


def test_inject_params_existing_env_wins(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sys, "argv", ["prog", "FOO=from_argv"])
    monkeypatch.setenv("FOO", "from_env")

    _bootstrap.inject_params()

    import os

    assert os.environ["FOO"] == "from_env"


def test_preset_overlay_populates_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        _bootstrap,
        "_overlay_preset_env",
        _make_fake_loader(monkeypatch, {"DBXCARTA_CATALOG": "c", "DBXCARTA_SCHEMAS": "s"}),
    )
    monkeypatch.setattr(sys, "argv", ["prog", "DBXCARTA_PRESET=pkg:preset"])
    monkeypatch.delenv("DBXCARTA_CATALOG", raising=False)
    monkeypatch.delenv("DBXCARTA_SCHEMAS", raising=False)
    monkeypatch.delenv("DBXCARTA_PRESET", raising=False)

    _bootstrap.inject_params()

    import os

    assert os.environ["DBXCARTA_PRESET"] == "pkg:preset"
    assert os.environ["DBXCARTA_CATALOG"] == "c"
    assert os.environ["DBXCARTA_SCHEMAS"] == "s"


def test_key_value_args_override_preset_values(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        _bootstrap,
        "_overlay_preset_env",
        _make_fake_loader(monkeypatch, {"DBXCARTA_CATALOG": "preset_default"}),
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "prog",
            "DBXCARTA_PRESET=pkg:preset",
            "DBXCARTA_CATALOG=override",
        ],
    )
    monkeypatch.delenv("DBXCARTA_CATALOG", raising=False)
    monkeypatch.delenv("DBXCARTA_PRESET", raising=False)

    _bootstrap.inject_params()

    import os

    assert os.environ["DBXCARTA_CATALOG"] == "override"


def test_no_preset_spec_skips_loader(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []

    def _record(spec: str) -> None:
        calls.append(spec)

    monkeypatch.setattr(_bootstrap, "_overlay_preset_env", _record)
    monkeypatch.setattr(sys, "argv", ["prog", "FOO=bar"])
    monkeypatch.delenv("DBXCARTA_PRESET", raising=False)

    _bootstrap.inject_params()

    assert calls == []


def test_preset_spec_from_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[str] = []
    monkeypatch.setattr(_bootstrap, "_overlay_preset_env", seen.append)
    monkeypatch.setattr(sys, "argv", ["prog"])
    monkeypatch.setenv("DBXCARTA_PRESET", "pkg:preset")

    _bootstrap.inject_params()

    assert seen == ["pkg:preset"]


def _make_fake_loader(monkeypatch: pytest.MonkeyPatch, payload: dict[str, str]):
    """Build a `_overlay_preset_env` stand-in backed by a fake preset.

    Bypasses real `load_preset` so the test does not need a published preset
    module on sys.path.
    """

    fake_preset = _FakePreset(payload)

    def _overlay(spec: str) -> None:
        assert spec
        import os

        for key, value in fake_preset.env().items():
            os.environ.setdefault(key, value)

    return _overlay
