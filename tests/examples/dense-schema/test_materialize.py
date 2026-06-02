from __future__ import annotations

from databricks.sdk.service.sql import StatementState

from dbxcarta_dense_schema_example.config import load_config
from dbxcarta_dense_schema_example.materialize import materialize


class _FakeStatus:
    state = StatementState.SUCCEEDED
    error = None


class _FakeResponse:
    def __init__(self) -> None:
        self.status = _FakeStatus()
        self.statement_id = "stmt-1"


class _FakeStatementExecution:
    def __init__(self) -> None:
        self.statements: list[str] = []

    def execute_statement(self, *, statement: str, **kwargs: object) -> _FakeResponse:
        self.statements.append(statement)
        return _FakeResponse()

    def get_statement(self, statement_id: str) -> _FakeResponse:
        return _FakeResponse()


class _FakeWorkspaceClient:
    def __init__(self) -> None:
        self.statement_execution = _FakeStatementExecution()


def test_materialize_creates_data_catalog_before_schemas() -> None:
    ws = _FakeWorkspaceClient()
    cfg = load_config({"DBXCARTA_CATALOG": "dense-schema_example"})

    # No schemas: only the data-catalog provisioning runs, so it is the sole
    # statement and the assertion does not depend on schema ordering.
    materialize(ws, "wh1", cfg, schemas=[])

    stmts = ws.statement_execution.statements
    assert stmts, "expected materialize to provision the data catalog"
    assert stmts[0].startswith(
        "CREATE CATALOG IF NOT EXISTS `dense-schema_example`"
    )
