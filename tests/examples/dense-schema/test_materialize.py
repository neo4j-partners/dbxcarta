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
        self.schema = type(
            "_S", (), {"columns": [type("_C", (), {"name": name})() for name in columns]}
        )()


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
    return load_config(
        {
            "DBXCARTA_CATALOG": "dense-schema-example",
            "DATABRICKS_VOLUME_PATH": "/Volumes/dbxcarta-catalog/dense-ops/dbxcarta-ops",
        }
    )


def test_materialize_creates_data_catalog_before_schemas() -> None:
    ws = _FakeWorkspaceClient()  # catalog absent → CREATE runs

    # No schemas: only the existence check and data-catalog provisioning run.
    materialize(ws, "wh1", _dense_config(), schemas=[])

    stmts = ws.statement_execution.statements
    assert stmts[0].startswith("SHOW CATALOGS")
    assert any(s.startswith("CREATE CATALOG IF NOT EXISTS `dense-schema-example`") for s in stmts)


def test_materialize_skips_create_when_catalog_exists() -> None:
    ws = _FakeWorkspaceClient(catalogs=["dense-schema-example"])

    materialize(ws, "wh1", _dense_config(), schemas=[])

    stmts = ws.statement_execution.statements
    assert not any(s.startswith("CREATE CATALOG") for s in stmts)


def _two_table_schema() -> list[dict[str, object]]:
    """Parent (hr_employees) + child with a self-FK and a cross-FK."""
    return [
        {
            "source_id": "src-1",
            "uc_schema": "dense-1000",
            "tables": [
                {
                    "name": "hr_employees",
                    "columns": [
                        {"name": "id", "type": "BIGINT"},
                        {"name": "parent_id", "type": "BIGINT"},
                        {"name": "name", "type": "VARCHAR(100)"},
                    ],
                    "primary_keys": ["id"],
                    "foreign_keys": [
                        {
                            "columns": ["parent_id"],
                            "foreign_table": "hr_employees",
                            "referred_columns": ["id"],
                        }
                    ],
                    "rows": [[1, None, "Ada"], [2, 1, "Bob"]],
                },
                {
                    "name": "hr_tasks",
                    "columns": [
                        {"name": "id", "type": "BIGINT"},
                        {"name": "created_by_id", "type": "BIGINT"},
                    ],
                    "primary_keys": ["id"],
                    "foreign_keys": [
                        {
                            "columns": ["created_by_id"],
                            "foreign_table": "hr_employees",
                            "referred_columns": ["id"],
                        }
                    ],
                    "rows": [[10, 1], [11, 2]],
                },
            ],
        }
    ]


def test_materialize_emits_primary_key_ddl() -> None:
    ws = _FakeWorkspaceClient(catalogs=["dense-schema-example"])

    stats = materialize(ws, "wh1", _dense_config(), _two_table_schema(), workers=1)

    stmts = ws.statement_execution.statements
    assert any("ALTER TABLE" in s and "ALTER COLUMN `id` SET NOT NULL" in s for s in stmts)
    assert any("ADD CONSTRAINT `pk_hr_employees` PRIMARY KEY (`id`)" in s for s in stmts)
    assert stats.pk_constraints_added == 2


def test_materialize_emits_foreign_key_ddl_in_second_pass() -> None:
    ws = _FakeWorkspaceClient(catalogs=["dense-schema-example"])

    stats = materialize(ws, "wh1", _dense_config(), _two_table_schema(), workers=1)

    stmts = ws.statement_execution.statements
    cross_fk = next(s for s in stmts if "ADD CONSTRAINT `fk_hr_tasks__created_by_id`" in s)
    assert "FOREIGN KEY (`created_by_id`)" in cross_fk
    assert "REFERENCES `dense-schema-example`.`dense-1000`.`hr_employees` (`id`)" in cross_fk

    # Self-referential FK on hr_employees is handled in the same pass.
    assert any(
        "ADD CONSTRAINT `fk_hr_employees__parent_id`" in s and "FOREIGN KEY (`parent_id`)" in s
        for s in stmts
    )
    assert stats.fk_constraints_added == 2

    # FK ALTERs come after every PK ALTER (second pass).
    last_pk = max(i for i, s in enumerate(stmts) if "PRIMARY KEY" in s)
    first_fk = min(i for i, s in enumerate(stmts) if "FOREIGN KEY" in s)
    assert first_fk > last_pk
