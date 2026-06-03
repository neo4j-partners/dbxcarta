from __future__ import annotations

from types import SimpleNamespace
from typing import Any

from dbxcarta.core.identifiers import quote_identifier
from databricks.sdk.service.sql import StatementState

from dbxcarta_schemapile_example.materialize import (
    _build_insert,
    _coerce_type,
    _constraint_name,
    _render_value,
    _sanitize_column_name,
    _sanitize_table_name,
    _sql_escape,
    materialize,
)


def test_coerce_type_int_families():
    assert _coerce_type("INT") == ("INT", False)
    assert _coerce_type("INTEGER") == ("INT", False)
    assert _coerce_type("BIGINT") == ("BIGINT", False)
    assert _coerce_type("SMALLINT") == ("SMALLINT", False)
    assert _coerce_type("TINYINT") == ("TINYINT", False)


def test_coerce_type_decimal_with_precision():
    delta, fellback = _coerce_type("DECIMAL(12, 4)")
    assert delta == "DECIMAL(12,4)"
    assert fellback is False


def test_coerce_type_decimal_clamps_to_38():
    delta, _ = _coerce_type("DECIMAL(50, 10)")
    assert delta == "DECIMAL(38,10)"


def test_coerce_type_varchar_becomes_string():
    assert _coerce_type("VARCHAR(255)") == ("STRING", False)
    assert _coerce_type("CHAR(8)") == ("STRING", False)


def test_coerce_type_unknown_falls_back_to_string():
    delta, fellback = _coerce_type("GEOMETRY")
    assert delta == "STRING"
    assert fellback is True


def test_coerce_type_empty_is_string_fallback():
    assert _coerce_type("") == ("STRING", True)


def test_coerce_type_datetime_to_timestamp():
    assert _coerce_type("DATETIME") == ("TIMESTAMP", False)
    assert _coerce_type("TIMESTAMP WITH TIME ZONE") == ("TIMESTAMP", False)


def test_sanitize_table_name_cleans_punctuation():
    assert _sanitize_table_name("My-Table 1") == "my_table_1"
    assert _sanitize_table_name("123_orders") == "t_123_orders"
    assert _sanitize_table_name("???") == ""


def test_sanitize_column_name_cleans_punctuation():
    assert _sanitize_column_name("First Name") == "first_name"
    assert _sanitize_column_name("9th_col") == "c_9th_col"


def test_sql_escape_quotes():
    assert _sql_escape("O'Brien") == "O''Brien"
    assert _sql_escape("a\\b") == "a\\\\b"


def test_render_value_null_unquoted():
    assert _render_value(None) == "NULL"


def test_render_value_string_quoted():
    assert _render_value("alice") == "'alice'"


def test_render_value_int_quoted_as_string():
    assert _render_value(42) == "'42'"


def test_render_value_escapes_single_quote():
    assert _render_value("O'Brien") == "'O''Brien'"


def test_build_insert_multi_row():
    sql = _build_insert(
        "`cat`.`sch`.`t`",
        ["id", "name"],
        [(1, "alice"), (2, None)],
    )
    assert sql.startswith("INSERT INTO `cat`.`sch`.`t` (`id`, `name`) VALUES")
    assert "('1', 'alice')" in sql
    assert "('2', NULL)" in sql


def test_constraint_name_short_passthrough():
    assert _constraint_name("pk", ["orders"]) == "pk_orders"
    assert _constraint_name("fk", ["orders", "user_id"]) == "fk_orders__user_id"


def test_constraint_name_long_gets_hash_suffix():
    long_part = "x" * 300
    name = _constraint_name("fk", [long_part])
    assert len(name) <= 255
    # Deterministic for the same input.
    assert name == _constraint_name("fk", [long_part])


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
            manifest = SimpleNamespace(
                schema=SimpleNamespace(columns=columns)
            )
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
        return SimpleNamespace(
            status=status, manifest=None, result=None, statement_id="stmt-1"
        )


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
        f"ALTER TABLE {cat}.`shop`.`users` ALTER COLUMN `id` SET NOT NULL" in s
        for s in statements
    )
    # PRIMARY KEY constraint is added.
    assert any(
        f"ALTER TABLE {cat}.`shop`.`users` ADD CONSTRAINT `pk_users`"
        " PRIMARY KEY (`id`)" in s
        for s in statements
    )
    assert stats.primary_keys_added == 2


def test_materialize_emits_foreign_key_ddl_in_second_pass():
    statements, stats = _run_materialize(_two_table_schemas())
    cat = quote_identifier("dbxcarta_data")

    fk_stmt = next(
        (s for s in statements if "FOREIGN KEY" in s), None
    )
    assert fk_stmt is not None
    assert (
        f"ALTER TABLE {cat}.`shop`.`orders` ADD CONSTRAINT `fk_orders__user_id`"
        " FOREIGN KEY (`user_id`)"
        f" REFERENCES {cat}.`shop`.`users` (`id`)" in fk_stmt
    )
    assert stats.foreign_keys_added == 1

    # The FK must be emitted only after the parent table's PK exists.
    parent_pk_idx = next(
        i for i, s in enumerate(statements)
        if "ADD CONSTRAINT `pk_users`" in s
    )
    fk_idx = statements.index(fk_stmt)
    assert fk_idx > parent_pk_idx


def test_materialize_skips_fk_when_target_not_materialized():
    schemas = _two_table_schemas()
    # Point the FK at a table that does not exist.
    schemas[0]["tables"][1]["foreign_keys"][0]["foreign_table"] = "missing"
    statements, stats = _run_materialize(schemas)
    assert not any("FOREIGN KEY" in s for s in statements)
    assert stats.foreign_keys_added == 0


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
    assert stats.foreign_keys_added == 1
    assert stats.primary_keys_added == 1
