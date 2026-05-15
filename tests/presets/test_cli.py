"""Unit tests for dbxcarta CLI guardrails."""

from __future__ import annotations

from pathlib import Path

import pytest

from databricks_job_runner.errors import RunnerError

from dbxcarta.spark import cli


class _ConfigStub:
    databricks_volume_path = ""

    def env_params(self, secret_keys: list[str]) -> list[str]:
        return []


class Serverless:
    def validate(self, ws: object) -> None:
        raise AssertionError("validate should not be called")


class _RunnerStub:
    wheel_volume_dir = "/Volumes/catalog/schema/volume/wheels"
    run_name_prefix = "dbxcarta"
    cli_command = "uv run dbxcarta"
    secret_keys: list[str] = []
    config = _ConfigStub()
    ws = object()

    def __init__(self, project_dir: Path) -> None:
        self.project_dir = project_dir

    def _compute(self, mode_override: str | None = None) -> object:
        return Serverless()


def _make_runner_stub(tmp_path: Path, wheel_name: str = "dbxcarta_spark-1.0.0-py3-none-any.whl") -> _RunnerStub:
    dist = tmp_path / "dist"
    dist.mkdir()
    (dist / wheel_name).write_bytes(b"")
    return _RunnerStub(tmp_path)


def test_submit_ingest_rejects_serverless_compute(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(cli, "runner", _make_runner_stub(tmp_path))

    with pytest.raises(RunnerError, match="Neo4j Spark Connector"):
        cli._submit_wheel_entrypoint(
            "ingest",
            "dbxcarta-ingest",
            compute_mode="serverless",
            no_wait=True,
        )


def test_submit_unknown_entrypoint_raises(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(cli, "runner", _make_runner_stub(tmp_path))

    with pytest.raises(RunnerError, match="unknown wheel entrypoint"):
        cli._submit_wheel_entrypoint(
            "bogus",
            "dbxcarta-bogus",
            compute_mode=None,
            no_wait=True,
        )


def test_submit_missing_wheel_raises(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    (tmp_path / "dist").mkdir()
    monkeypatch.setattr(cli, "runner", _RunnerStub(tmp_path))

    with pytest.raises(RunnerError, match="no dbxcarta-spark wheel found"):
        cli._submit_wheel_entrypoint(
            "ingest",
            "dbxcarta-ingest",
            compute_mode=None,
            no_wait=True,
        )
