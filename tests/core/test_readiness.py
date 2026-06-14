"""Tests for the shared readiness check.

The readiness rule and the relabeled report are exercised here once rather than
per example, because every example drives them through the same core function.
"""

from __future__ import annotations

import pytest
from dbxcarta.core.readiness import (
    ReadinessReport,
    check_readiness,
)


class _FakeStatementExecution:
    """Returns the schema rows mapped to whichever catalog the SQL targets."""

    def __init__(self, schemas_by_catalog: dict[str, list[str]]) -> None:
        self._schemas_by_catalog = schemas_by_catalog

    def execute_statement(self, **kwargs: object) -> object:
        statement = str(kwargs["statement"])
        rows: list[list[str]] = []
        for catalog, schemas in self._schemas_by_catalog.items():
            if f"`{catalog}`" in statement:
                rows = [[name] for name in schemas]
                break
        return type(
            "Resp",
            (),
            {"result": type("Result", (), {"data_array": rows})()},
        )()


class _FakeWorkspaceClient:
    def __init__(self, schemas_by_catalog: dict[str, list[str]]) -> None:
        self.statement_execution = _FakeStatementExecution(schemas_by_catalog)


# --- Readiness rule: a catalog needs a schema beyond information_schema/default


def test_readiness_not_ready_with_only_auto_schemas(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DBXCARTA_CATALOG", "cat")
    monkeypatch.delenv("DBXCARTA_CATALOGS", raising=False)
    ws = _FakeWorkspaceClient({"cat": ["information_schema", "default"]})
    report = check_readiness(ws=ws, warehouse_id="wh")  # type: ignore[arg-type]
    assert not report.ok()
    assert report.missing_required == ("cat",)
    assert report.present == ()


def test_readiness_ready_with_one_data_schema(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DBXCARTA_CATALOG", "cat")
    monkeypatch.delenv("DBXCARTA_CATALOGS", raising=False)
    ws = _FakeWorkspaceClient({"cat": ["information_schema", "default", "sales"]})
    report = check_readiness(ws=ws, warehouse_id="wh")  # type: ignore[arg-type]
    assert report.ok()
    assert report.present == ("cat",)
    assert report.catalog == "cat"
    assert report.schema == ""


def test_readiness_checks_every_catalog_in_the_list(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DBXCARTA_CATALOG", "silver")
    monkeypatch.setenv("DBXCARTA_CATALOGS", "silver:silver,gold:gold")
    ws = _FakeWorkspaceClient(
        {
            "silver": ["information_schema", "default", "graph_schema"],
            "gold": ["information_schema", "default"],  # nothing materialized
        }
    )
    report = check_readiness(ws=ws, warehouse_id="wh")  # type: ignore[arg-type]
    assert report.present == ("silver",)
    assert report.missing_required == ("gold",)
    assert report.catalog == "silver,gold"
    assert not report.ok()


def test_readiness_fails_loud_without_catalog(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("DBXCARTA_CATALOG", raising=False)
    monkeypatch.delenv("DBXCARTA_CATALOGS", raising=False)
    ws = _FakeWorkspaceClient({})
    with pytest.raises(RuntimeError, match="DBXCARTA_CATALOG is not set"):
        check_readiness(ws=ws, warehouse_id="wh")  # type: ignore[arg-type]


# --- ReadinessReport.format relabel ------------------------------------------


def test_format_omits_empty_schema_and_relabels_catalogs() -> None:
    report = ReadinessReport(
        catalog="silver,gold",
        schema="",
        present=("silver", "gold"),
        missing_required=(),
        missing_optional=(),
    )
    formatted = report.format()
    assert "scope: silver,gold" in formatted
    assert "present catalogs: 2" in formatted
    assert "catalogs: ready" in formatted
    assert "status: ready" in formatted


def test_format_reports_missing_catalogs() -> None:
    report = ReadinessReport(
        catalog="silver,gold",
        schema="",
        present=("silver",),
        missing_required=("gold",),
        missing_optional=(),
    )
    formatted = report.format()
    assert "missing required: gold" in formatted
    assert "status: not ready" in formatted
