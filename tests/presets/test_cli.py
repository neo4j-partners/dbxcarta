"""Unit tests for dbxcarta CLI guardrails."""

from __future__ import annotations

import pytest

from databricks_job_runner import BootstrapConfig, ClassicCluster, Serverless
from databricks_job_runner.errors import RunnerError

from dbxcarta.spark import cli


class _RunnerStub:
    wheel_volume_dir = "/Volumes/catalog/schema/volume/wheels"
    run_name_prefix = "dbxcarta"
    cli_command = "uv run dbxcarta"

    def __init__(self, compute: object) -> None:
        self._compute_obj = compute
        self.submitted: list[tuple[BootstrapConfig, dict]] = []

    def _compute(self, mode_override: str | None = None) -> object:
        return self._compute_obj

    def submit_bootstrap(self, bootstrap: BootstrapConfig, **kwargs: object) -> None:
        self.submitted.append((bootstrap, kwargs))


def test_submit_ingest_rejects_serverless_compute(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stub = _RunnerStub(Serverless())
    monkeypatch.setattr(cli, "_ingest_runner", lambda: stub)

    with pytest.raises(RunnerError, match="Neo4j Spark Connector"):
        cli._submit_bootstrap_entrypoint(
            "ingest", compute_mode="serverless", no_wait=True
        )
    assert stub.submitted == []


def test_submit_unknown_entrypoint_raises() -> None:
    with pytest.raises(RunnerError, match="unknown wheel entrypoint"):
        cli._submit_bootstrap_entrypoint(
            "bogus", compute_mode=None, no_wait=True
        )


def test_submit_ingest_builds_bootstrap_with_probe_and_closure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stub = _RunnerStub(ClassicCluster(cluster_id="c-1"))
    monkeypatch.setattr(cli, "_ingest_runner", lambda: stub)

    cli._submit_bootstrap_entrypoint("ingest", compute_mode=None, no_wait=True)

    assert len(stub.submitted) == 1
    bootstrap, kwargs = stub.submitted[0]
    assert bootstrap.wheel_package == "dbxcarta-spark"
    assert bootstrap.console_script == "dbxcarta-ingest"
    assert bootstrap.jvm_probe_class == cli._INGEST_JVM_PROBE_CLASS
    assert bootstrap.pinned_closure == list(cli._INGEST_PINNED_CLOSURE)
    assert bootstrap.wheel_volume_path.endswith("dbxcarta_spark-stable.whl")
    assert kwargs["run_name_suffix"] == "ingest"


def test_submit_client_uses_shared_runner_without_probe(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stub = _RunnerStub(ClassicCluster(cluster_id="c-1"))
    monkeypatch.setattr(cli, "runner", stub)
    monkeypatch.setattr(
        cli, "_ingest_runner", lambda: pytest.fail("client must not use ingest runner")
    )

    cli._submit_bootstrap_entrypoint("client", compute_mode=None, no_wait=True)

    bootstrap, _ = stub.submitted[0]
    assert bootstrap.wheel_package == "dbxcarta-client"
    assert bootstrap.console_script == "dbxcarta-client"
    assert bootstrap.jvm_probe_class is None
    assert bootstrap.pinned_closure == list(cli._CLIENT_PINNED_CLOSURE)


def test_is_serverless_compute_discriminates_real_compute_types() -> None:
    assert cli._is_serverless_compute(Serverless()) is True
    assert cli._is_serverless_compute(ClassicCluster(cluster_id="c-1")) is False


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
