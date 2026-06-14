"""Unit tests for the core dbxcarta CLI (verify/ready/upload-questions path)."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from dbxcarta.spark import cli

if TYPE_CHECKING:
    from pathlib import Path


class _Secret:
    def __init__(self, value: str | None) -> None:
        self.value = value


class _Secrets:
    def __init__(self, values: dict[str, str | None]) -> None:
        self._values = values

    def get_secret(self, *, scope: str, key: str) -> _Secret:
        return _Secret(self._values.get(key))


class _Ws:
    def __init__(self, values: dict[str, str | None]) -> None:
        self.secrets = _Secrets(values)


class _Settings:
    databricks_secret_scope = "dbxcarta"


def test_build_neo4j_driver_raises_on_missing_secret() -> None:
    # NEO4J_URI is resolved first; a None value must surface as an explicit
    # error naming the key and scope, not an opaque b64decode TypeError.
    ws = _Ws({"NEO4J_URI": None})

    with pytest.raises(RuntimeError, match="'NEO4J_URI'.*'dbxcarta'"):
        cli._build_neo4j_driver(ws, _Settings())  # type: ignore[arg-type]


def test_resolve_questions_file_uses_explicit_override(tmp_path: Path) -> None:
    path = tmp_path / "custom.json"
    path.write_text("[]")
    assert cli._resolve_questions_file(str(path)) == path


def test_resolve_questions_file_defaults_beside_overlay(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    overlay = tmp_path / "dbxcarta-overlay.env"
    overlay.write_text("")
    questions = tmp_path / "questions.json"
    questions.write_text("[]")
    monkeypatch.setattr(cli.sys, "argv", ["dbxcarta", "upload-questions", "--env-file", str(overlay)])
    assert cli._resolve_questions_file("") == questions


def test_resolve_questions_file_errors_without_override_or_overlay(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(cli.sys, "argv", ["dbxcarta", "upload-questions"])
    monkeypatch.delenv("DBXCARTA_ENV_FILE", raising=False)
    assert cli._resolve_questions_file("") is None


def test_resolve_questions_file_errors_when_missing(tmp_path: Path) -> None:
    assert cli._resolve_questions_file(str(tmp_path / "nope.json")) is None
