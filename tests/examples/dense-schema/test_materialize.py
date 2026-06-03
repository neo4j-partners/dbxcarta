from __future__ import annotations

from databricks.sdk.service.sql import StatementState

from dbxcarta_dense_schema_example.config import load_config
from dbxcarta_dense_schema_example.materialize import materialize


class _FakeStatus:
    def __init__(self, state: StatementState = StatementState.SUCCEEDED) -> None:
        self.state = state
        self.error = None


class _FakeResult:
    def __init__(self, rows: list[list[str]]) -> None:
        self.data_array = rows
        self.next_chunk_index = None


class _FakeManifest:
    def __init__(self, columns: list[str]) -> None:
        self.schema = type("_S", (), {"columns": [
            type("_C", (), {"name": name})() for name in columns
        ]})()


class _FakeResponse:
    def __init__(
        self,
        *,
        result: _FakeResult | None = None,
        manifest: _FakeManifest | None = None,
    ) -> None:
        self.status = _FakeStatus()
        self.statement_id = "stmt-1"
        self.result = result
        self.manifest = manifest


class _FakeStatementExecution:
    def __init__(self, catalogs: list[str]) -> None:
        self.statements: list[str] = []
        self._catalogs = catalogs

    def execute_statement(self, *, statement: str, **kwargs: object) -> _FakeResponse:
        self.statements.append(statement)
        if statement.strip().upper().startswith("SHOW CATALOGS"):
            return _FakeResponse(
                result=_FakeResult([[name] for name in self._catalogs]),
                manifest=_FakeManifest(["catalog"]),
            )
        return _FakeResponse()

    def get_statement(self, statement_id: str) -> _FakeResponse:
        return _FakeResponse()


class _FakeWorkspaceClient:
    def __init__(self, catalogs: list[str] | None = None) -> None:
        self.statement_execution = _FakeStatementExecution(catalogs or [])


def _dense_config():
    return load_config({
        "DBXCARTA_CATALOG": "dense-schema-example",
        "DATABRICKS_VOLUME_PATH": "/Volumes/dbxcarta-catalog/dense-ops/dbxcarta-ops",
    })


def test_materialize_creates_data_catalog_before_schemas() -> None:
    ws = _FakeWorkspaceClient()  # catalog absent → CREATE runs

    # No schemas: only the existence check and data-catalog provisioning run.
    materialize(ws, "wh1", _dense_config(), schemas=[])

    stmts = ws.statement_execution.statements
    assert stmts[0].startswith("SHOW CATALOGS")
    assert any(
        s.startswith("CREATE CATALOG IF NOT EXISTS `dense-schema-example`")
        for s in stmts
    )


def test_materialize_skips_create_when_catalog_exists() -> None:
    ws = _FakeWorkspaceClient(catalogs=["dense-schema-example"])

    materialize(ws, "wh1", _dense_config(), schemas=[])

    stmts = ws.statement_execution.statements
    assert not any(s.startswith("CREATE CATALOG") for s in stmts)
