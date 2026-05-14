"""Unit tests for dbxcarta CLI guardrails."""

from __future__ import annotations

import pytest

from databricks_job_runner.errors import RunnerError

from dbxcarta.presets import cli


class _ConfigStub:
    databricks_volume_path = ""

    def env_params(self, secret_keys: list[str]) -> list[str]:
        return []


class Serverless:
    def validate(self, ws: object) -> None:
        raise AssertionError("validate should not be called")


class _RunnerStub:
    wheel_package = "dbxcarta"
    wheel_volume_dir = "/Volumes/catalog/schema/volume/wheels"
    run_name_prefix = "dbxcarta"
    cli_command = "uv run dbxcarta"
    secret_keys: list[str] = []
    config = _ConfigStub()
    ws = object()

    def find_wheel(self) -> str:
        return "dbxcarta-1.0.0-py3-none-any.whl"

    def _compute(self, mode_override: str | None = None) -> object:
        return Serverless()


def test_submit_ingest_rejects_serverless_compute(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli, "runner", _RunnerStub())

    with pytest.raises(RunnerError, match="Neo4j Spark Connector"):
        cli._submit_wheel_entrypoint(
            "ingest",
            "dbxcarta-ingest",
            compute_mode="serverless",
            no_wait=True,
        )
