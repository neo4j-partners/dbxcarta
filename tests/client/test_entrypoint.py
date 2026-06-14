"""Unit tests for the ``dbxcarta-client`` local entrypoint.

These guard Phase 3.5: the client must load the selected example overlay
itself (the same way the operator CLI does) before running the harness, and a
bad/missing selected overlay must be a clean ``error: ...`` + exit 2 rather
than an uncaught traceback.
"""

from __future__ import annotations

import os
import sys
from typing import TYPE_CHECKING

import pytest
from dbxcarta.client.eval import entrypoint
from dbxcarta.core import env as _env

if TYPE_CHECKING:
    from pathlib import Path

_OVERLAY_KEY = "DBXCARTA_TEST_ENTRYPOINT_KEY"


def _write_env(path: Path, key: str, value: str) -> None:
    path.write_text(f"{key}={value}\n", encoding="utf-8")


def _stub_run_client(monkeypatch: pytest.MonkeyPatch) -> list[bool]:
    calls: list[bool] = []
    monkeypatch.setattr(entrypoint, "run_client", lambda: calls.append(True))
    return calls


def test_main_loads_overlay_from_cli_option(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The ``--env-file`` CLI flag selects the overlay (parity with operator CLI)."""
    overlay = tmp_path / "overlay.env"
    _write_env(overlay, _OVERLAY_KEY, "from_cli_overlay")
    monkeypatch.delenv(_OVERLAY_KEY, raising=False)
    monkeypatch.delenv(_env._ENV_FILE_KEY, raising=False)
    monkeypatch.setattr(sys, "argv", ["dbxcarta-client", "--env-file", str(overlay)])
    calls = _stub_run_client(monkeypatch)

    entrypoint.main()

    assert os.environ[_OVERLAY_KEY] == "from_cli_overlay"
    assert calls == [True]
    # The consumed --env-file option is stripped before run_client.
    assert sys.argv[1:] == []


def test_main_loads_overlay_from_env_var(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    """``DBXCARTA_ENV_FILE`` selects the overlay when no CLI flag is given."""
    overlay = tmp_path / "overlay.env"
    _write_env(overlay, _OVERLAY_KEY, "from_env_var")
    monkeypatch.delenv(_OVERLAY_KEY, raising=False)
    monkeypatch.setenv(_env._ENV_FILE_KEY, str(overlay))
    monkeypatch.setattr(sys, "argv", ["dbxcarta-client"])
    calls = _stub_run_client(monkeypatch)

    entrypoint.main()

    assert os.environ[_OVERLAY_KEY] == "from_env_var"
    assert calls == [True]


def test_main_bad_overlay_is_clean_error(tmp_path, monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    """A selected overlay that does not resolve exits 2, not a traceback, and never runs."""
    missing = tmp_path / "does-not-exist.env"
    monkeypatch.delenv(_env._ENV_FILE_KEY, raising=False)
    monkeypatch.setattr(sys, "argv", ["dbxcarta-client", "--env-file", str(missing)])
    calls = _stub_run_client(monkeypatch)

    with pytest.raises(SystemExit) as excinfo:
        entrypoint.main()

    assert excinfo.value.code == 2
    assert "error:" in capsys.readouterr().err
    assert calls == []
