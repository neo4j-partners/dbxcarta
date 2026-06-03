from __future__ import annotations

from types import SimpleNamespace
from typing import Any

from databricks.sdk.service.sql import StatementState
from dbxcarta.core.identifiers import quote_identifier
from dbxcarta_schemapile_example.materialize import materialize

# The materialize plumbing (coerce_type, sanitize_identifier, escape/render,
# build_insert_statement, constraint_name) now lives in dbxcarta.core and is
# unit-tested in tests/core/test_materialize.py. These tests cover only what
# schemapile still owns: provisioning the data catalog and the DDL the spine
# emits for this example's specs.


# --- statement-capture fakes -------------------------------------------------


class _FakeStatementExecution:
    """Records every executed statement and answers SHOW CATALOGS so the
    catalog-existence guard treats the data catalog as already present."""

    def __init__(self, existing_catalogs: list[str]):
        self.statements: list[str] = []
        self._existing_catalogs = existing_catalogs

    def execute_statement(self, statement: str, **_: Any) -> Any:
        self.statements.append(statement)
        status = SimpleNamespace(state=StatementState.SUCCEEDED, error=None)
        if statement.strip().upper().startswith("SHOW CATALOGS"):
            columns = [SimpleNamespace(name="catalog")]
            manifest = SimpleNamespace(schema=SimpleNamespace(columns=columns))
            result = SimpleNamespace(
                data_array=[[c] for c in self._existing_catalogs],
                next_chunk_index=None,
            )
            return SimpleNamespace(
                status=status,
                manifest=manifest,
                result=result,
                statement_id="stmt-1",
            )
        return SimpleNamespace(status=status, manifest=None, result=None, statement_id="stmt-1")


class _FakeWorkspaceClient:
    def __init__(self, existing_catalogs: list[str]):
        self.statement_execution = _FakeStatementExecution(existing_catalogs)


def _two_table_schemas() -> list[dict[str, Any]]:
    return [
        {
            "uc_schema": "shop",
            "source_id": "shop_src",
            "tables": [
                {
                    "name": "users",
                    "columns": [
                        {"name": "id", "type": "INT"},
                        {"name": "name", "type": "VARCHAR(50)"},
                    ],
                    "primary_keys": ["id"],
                    "foreign_keys": [],
                    "rows": [],
                },
                {
                    "name": "orders",
                    "columns": [
                        {"name": "id", "type": "INT"},
                        {"name": "user_id", "type": "INT"},
                    ],
                    "primary_keys": ["id"],
                    "foreign_keys": [
                        {
                            "columns": ["user_id"],
                            "foreign_table": "users",
                            "referred_columns": ["id"],
                        }
                    ],
                    "rows": [],
                },
            ],
        }
    ]


def _run_materialize(schemas: list[dict[str, Any]]):
    ws = _FakeWorkspaceClient(existing_catalogs=["dbxcarta_data"])
    config = SimpleNamespace(catalog="dbxcarta_data")
    stats = materialize(ws, "wh-1", config, schemas)
    return ws.statement_execution.statements, stats


def test_materialize_emits_primary_key_ddl():
    statements, stats = _run_materialize(_two_table_schemas())
    cat = quote_identifier("dbxcarta_data")

    # PK columns are set NOT NULL.
    assert any(
        f"ALTER TABLE {cat}.`shop`.`users` ALTER COLUMN `id` SET NOT NULL" in s for s in statements
    )
    # PRIMARY KEY constraint is added.
    assert any(
        f"ALTER TABLE {cat}.`shop`.`users` ADD CONSTRAINT `pk_users` PRIMARY KEY (`id`)" in s
        for s in statements
    )
    assert stats.pk_constraints_added == 2


def test_materialize_emits_foreign_key_ddl_in_second_pass():
    statements, stats = _run_materialize(_two_table_schemas())
    cat = quote_identifier("dbxcarta_data")

    fk_stmt = next((s for s in statements if "FOREIGN KEY" in s), None)
    assert fk_stmt is not None
    assert (
        f"ALTER TABLE {cat}.`shop`.`orders` ADD CONSTRAINT `fk_orders__user_id`"
        " FOREIGN KEY (`user_id`)"
        f" REFERENCES {cat}.`shop`.`users` (`id`)" in fk_stmt
    )
    assert stats.fk_constraints_added == 1

    # The FK must be emitted only after the parent table's PK exists.
    parent_pk_idx = next(i for i, s in enumerate(statements) if "ADD CONSTRAINT `pk_users`" in s)
    fk_idx = statements.index(fk_stmt)
    assert fk_idx > parent_pk_idx


def test_materialize_skips_fk_when_target_not_materialized():
    schemas = _two_table_schemas()
    # Point the FK at a table that does not exist.
    schemas[0]["tables"][1]["foreign_keys"][0]["foreign_table"] = "missing"
    statements, stats = _run_materialize(schemas)
    assert not any("FOREIGN KEY" in s for s in statements)
    assert stats.fk_constraints_added == 0


def test_materialize_handles_self_referential_fk():
    schemas = [
        {
            "uc_schema": "org",
            "source_id": "org_src",
            "tables": [
                {
                    "name": "employee",
                    "columns": [
                        {"name": "id", "type": "INT"},
                        {"name": "manager_id", "type": "INT"},
                    ],
                    "primary_keys": ["id"],
                    "foreign_keys": [
                        {
                            "columns": ["manager_id"],
                            "foreign_table": "employee",
                            "referred_columns": ["id"],
                        }
                    ],
                    "rows": [],
                }
            ],
        }
    ]
    statements, stats = _run_materialize(schemas)
    cat = quote_identifier("dbxcarta_data")
    assert any(
        f"ALTER TABLE {cat}.`org`.`employee` ADD CONSTRAINT"
        " `fk_employee__manager_id` FOREIGN KEY (`manager_id`)"
        f" REFERENCES {cat}.`org`.`employee` (`id`)" in s
        for s in statements
    )
    assert stats.fk_constraints_added == 1
    assert stats.pk_constraints_added == 1
