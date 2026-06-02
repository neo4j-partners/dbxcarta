"""Unit tests for the bootstrap/teardown UC-admin helpers."""

from __future__ import annotations

import pytest

from databricks.sdk.service.sql import StatementState

from dbxcarta.submit.uc_admin import (
    TeardownKind,
    TeardownTarget,
    drop_teardown_target,
    ensure_uc_volume,
    execute_statement,
    parse_teardown_target,
    read_required_warehouse_id,
)


class _FakeError:
    def __init__(self, message: str) -> None:
        self.message = message


class _FakeStatus:
    def __init__(self, state: StatementState, error: _FakeError | None) -> None:
        self.state = state
        self.error = error


class _FakeResponse:
    def __init__(self, state: StatementState, error: _FakeError | None) -> None:
        self.status = _FakeStatus(state, error)
        self.statement_id = "stmt-1"


class _FakeStatementExecution:
    def __init__(
        self, state: StatementState, error: _FakeError | None
    ) -> None:
        self.statements: list[str] = []
        self._state = state
        self._error = error

    def execute_statement(
        self, *, statement: str, **kwargs: object
    ) -> _FakeResponse:
        self.statements.append(statement)
        return _FakeResponse(self._state, self._error)


class _FakeWorkspaceClient:
    """Captures executed SQL without touching a real warehouse.

    Returns a synthetic statement response so :func:`execute_statement` can
    inspect its terminal state. Defaults to ``SUCCEEDED``; pass a different
    state (and optional error) to exercise the failure path.
    """

    def __init__(
        self,
        *,
        state: StatementState = StatementState.SUCCEEDED,
        error_message: str | None = None,
    ) -> None:
        error = _FakeError(error_message) if error_message is not None else None
        self.statement_execution = _FakeStatementExecution(state, error)


def test_parse_teardown_target_catalog() -> None:
    target = parse_teardown_target("catalog:schemapile_lakehouse")
    assert target == TeardownTarget(
        kind=TeardownKind.CATALOG, catalog="schemapile_lakehouse"
    )
    assert target.describe() == "catalog schemapile_lakehouse"


def test_parse_teardown_target_schema() -> None:
    target = parse_teardown_target("schema:dbxcarta-catalog.finance_genie_ops")
    assert target.kind is TeardownKind.SCHEMA
    assert target.catalog == "dbxcarta-catalog"
    assert target.schema == "finance_genie_ops"
    assert target.describe() == "schema dbxcarta-catalog.finance_genie_ops"


@pytest.mark.parametrize(
    "value",
    [
        "schemapile_lakehouse",  # no prefix
        "schema:onlycatalog",    # schema form missing .schema
        "bogus:catalog.schema",  # unknown prefix
    ],
)
def test_parse_teardown_target_rejects_malformed(value: str) -> None:
    with pytest.raises(ValueError):
        parse_teardown_target(value)


def test_parse_teardown_target_refuses_protected_catalog() -> None:
    with pytest.raises(ValueError, match="protected"):
        parse_teardown_target("catalog:main")
    with pytest.raises(ValueError, match="protected"):
        parse_teardown_target("schema:system.foo")


def test_ensure_uc_volume_emits_idempotent_ddl() -> None:
    ws = _FakeWorkspaceClient()
    ensure_uc_volume(ws, "wh1", catalog="cat", schema="sch", volume="vol")
    stmts = ws.statement_execution.statements
    assert len(stmts) == 3
    assert stmts[0].startswith("CREATE CATALOG IF NOT EXISTS `cat`")
    assert stmts[1].startswith("CREATE SCHEMA IF NOT EXISTS `cat`.`sch`")
    assert stmts[2].startswith("CREATE VOLUME IF NOT EXISTS `cat`.`sch`.`vol`")


def test_ensure_uc_volume_refuses_protected_catalog() -> None:
    ws = _FakeWorkspaceClient()
    with pytest.raises(ValueError, match="protected"):
        ensure_uc_volume(ws, "wh1", catalog="main", schema="s", volume="v")
    assert ws.statement_execution.statements == []


def test_drop_teardown_target_schema_cascades() -> None:
    ws = _FakeWorkspaceClient()
    drop_teardown_target(ws, "wh1", TeardownTarget(TeardownKind.SCHEMA, "cat", "sch"))
    assert ws.statement_execution.statements == [
        "DROP SCHEMA IF EXISTS `cat`.`sch` CASCADE"
    ]


def test_drop_teardown_target_catalog_cascades() -> None:
    ws = _FakeWorkspaceClient()
    drop_teardown_target(ws, "wh1", TeardownTarget(TeardownKind.CATALOG, "cat"))
    assert ws.statement_execution.statements == [
        "DROP CATALOG IF EXISTS `cat` CASCADE"
    ]


def test_execute_statement_raises_on_failed_state() -> None:
    ws = _FakeWorkspaceClient(
        state=StatementState.FAILED, error_message="permission denied"
    )
    with pytest.raises(RuntimeError, match="permission denied"):
        execute_statement(ws, "wh1", "CREATE CATALOG `cat`")


def test_ensure_uc_volume_raises_when_create_fails() -> None:
    ws = _FakeWorkspaceClient(state=StatementState.FAILED, error_message="boom")
    with pytest.raises(RuntimeError, match="boom"):
        ensure_uc_volume(ws, "wh1", catalog="cat", schema="sch", volume="vol")


def test_read_required_warehouse_id_prefers_override() -> None:
    assert (
        read_required_warehouse_id("wh-override", operation="bootstrap")
        == "wh-override"
    )


def test_read_required_warehouse_id_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DATABRICKS_WAREHOUSE_ID", raising=False)
    with pytest.raises(ValueError, match="DATABRICKS_WAREHOUSE_ID"):
        read_required_warehouse_id(None, operation="bootstrap")
