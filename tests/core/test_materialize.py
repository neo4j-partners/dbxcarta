from __future__ import annotations

from typing import Any

import pytest

from dbxcarta.core.materialize import (
    MaterializeStats,
    build_insert_statement,
    coerce_type,
    constraint_name,
    escape_sql_string,
    materialize_schemas,
    render_sql_value,
    sanitize_identifier,
)


# --- coerce_type ---------------------------------------------------------


def test_coerce_type_maps_known_types() -> None:
    assert coerce_type("integer") == ("INT", False)
    assert coerce_type("BIGINT") == ("BIGINT", False)
    assert coerce_type("text") == ("STRING", False)


def test_coerce_type_decimal_is_clamped() -> None:
    assert coerce_type("DECIMAL(40,50)") == ("DECIMAL(38,38)", False)
    assert coerce_type("numeric(5,2)") == ("DECIMAL(5,2)", False)


def test_coerce_type_sized_varchar_becomes_string() -> None:
    assert coerce_type("VARCHAR(255)") == ("STRING", False)


def test_coerce_type_strips_args_then_matches_base() -> None:
    assert coerce_type("int(11)") == ("INT", False)


def test_coerce_type_falls_back_to_string() -> None:
    assert coerce_type("") == ("STRING", True)
    assert coerce_type("WIDGET") == ("STRING", True)


def test_coerce_type_honours_custom_map() -> None:
    assert coerce_type("GEOMETRY", {"GEOMETRY": "BINARY"}) == ("BINARY", False)


# --- sanitize_identifier -------------------------------------------------


def test_sanitize_identifier_cleans_and_lowercases() -> None:
    assert sanitize_identifier("Order Items!") == "order_items"


def test_sanitize_identifier_prefixes_leading_digit() -> None:
    assert sanitize_identifier("9lives", prefix="t") == "t_9lives"
    assert sanitize_identifier("1col", prefix="c") == "c_1col"


def test_sanitize_identifier_empty_when_nothing_usable() -> None:
    assert sanitize_identifier("!!!") == ""


# --- escape / render -----------------------------------------------------


def test_escape_sql_string() -> None:
    assert escape_sql_string("a'b\\c") == "a''b\\\\c"


def test_render_sql_value() -> None:
    assert render_sql_value(None) == "NULL"
    assert render_sql_value(5) == "'5'"
    assert render_sql_value("o'brien") == "'o''brien'"


# --- build_insert_statement ----------------------------------------------


def test_build_insert_uses_insert_overwrite() -> None:
    sql = build_insert_statement("`c`.`s`.`t`", ["a", "b"], [(1, None), ("x", 2)])
    assert sql.startswith("INSERT OVERWRITE TABLE `c`.`s`.`t` (`a`, `b`) VALUES")
    assert "('1', NULL)" in sql
    assert "('x', '2')" in sql


# --- constraint_name -----------------------------------------------------


def test_constraint_name_short_is_verbatim() -> None:
    assert constraint_name("pk", ["orders"]) == "pk_orders"
    assert constraint_name("fk", ["child", "col_a", "col_b"]) == "fk_child__col_a__col_b"


def test_constraint_name_skips_empty_parts() -> None:
    assert constraint_name("pk", ["", "orders", ""]) == "pk_orders"


def test_constraint_name_long_keeps_prefix_and_hash_suffix() -> None:
    name = constraint_name("fk", ["x" * 400])
    assert len(name) == 255
    assert name.startswith("fk_")
    # 12-char hex suffix preserved at the tail.
    suffix = name.rsplit("_", 1)[-1]
    assert len(suffix) == 12
    assert all(ch in "0123456789abcdef" for ch in suffix)


def test_constraint_name_long_is_deterministic() -> None:
    assert constraint_name("fk", ["y" * 400]) == constraint_name("fk", ["y" * 400])


# --- MaterializeStats ----------------------------------------------------


def test_materialize_stats_add_merges_fields() -> None:
    total = MaterializeStats(tables_created=2, rows_inserted=10) + MaterializeStats(
        tables_created=3, fk_constraints_added=1
    )
    assert total.tables_created == 5
    assert total.rows_inserted == 10
    assert total.fk_constraints_added == 1


def test_materialize_stats_add_rejects_other_types() -> None:
    with pytest.raises(TypeError):
        MaterializeStats() + 1  # type: ignore[operator]


# --- the spine -----------------------------------------------------------


class _Recorder:
    """Fake ``execute`` that records statements and can fail matching ones."""

    def __init__(self, fail_on: str | None = None) -> None:
        self.statements: list[str] = []
        self._fail_on = fail_on

    def __call__(self, statement: str, label: str) -> None:
        self.statements.append(statement)
        if self._fail_on is not None and self._fail_on in statement:
            raise RuntimeError(f"forced failure: {label}")


def _two_table_schema() -> list[dict[str, Any]]:
    return [
        {
            "uc_schema": "shop",
            "source_id": "shop_db",
            "tables": [
                {
                    "name": "customers",
                    "columns": [{"name": "id", "type": "int"},
                                {"name": "name", "type": "text"}],
                    "primary_keys": ["id"],
                    "foreign_keys": [],
                    "rows": [[1, "Ada"], [2, "Grace"]],
                },
                {
                    "name": "orders",
                    "columns": [{"name": "id", "type": "int"},
                                {"name": "customer_id", "type": "int"}],
                    "primary_keys": ["id"],
                    "foreign_keys": [
                        {
                            "columns": ["customer_id"],
                            "foreign_table": "customers",
                            "referred_columns": ["id"],
                        }
                    ],
                    "rows": [[10, 1]],
                },
            ],
        }
    ]


@pytest.mark.parametrize("workers", [1, 4])
def test_spine_materializes_schema_with_constraints(workers: int) -> None:
    rec = _Recorder()
    stats = materialize_schemas(
        _two_table_schema(), catalog="cat", execute=rec,
        property_prefix="ex", workers=workers,
    )
    assert stats.schemas_created == 1
    assert stats.tables_created == 2
    assert stats.rows_inserted == 3
    assert stats.pk_constraints_added == 2
    assert stats.fk_constraints_added == 1

    joined = "\n".join(rec.statements)
    assert "CREATE SCHEMA IF NOT EXISTS `cat`.`shop`" in joined
    assert "COMMENT 'ex source: shop_db'" in joined
    assert "INSERT OVERWRITE TABLE `cat`.`shop`.`orders`" in joined
    assert "ADD CONSTRAINT `pk_orders` PRIMARY KEY" in joined
    assert "FOREIGN KEY (`customer_id`) REFERENCES `cat`.`shop`.`customers`" in joined
    assert "'ex.source_id' = 'shop_db'" in joined


def test_spine_skips_table_with_no_columns() -> None:
    schemas = [{
        "uc_schema": "s", "source_id": "src",
        "tables": [{"name": "empty", "columns": [], "primary_keys": [],
                    "foreign_keys": [], "rows": []}],
    }]
    stats = materialize_schemas(
        schemas, catalog="cat", execute=_Recorder(), property_prefix="ex"
    )
    assert stats.tables_created == 0
    assert stats.tables_skipped == 1


def test_spine_counts_type_fallbacks() -> None:
    schemas = [{
        "uc_schema": "s", "source_id": "src",
        "tables": [{"name": "t", "columns": [{"name": "c", "type": "mystery"}],
                    "primary_keys": [], "foreign_keys": [], "rows": []}],
    }]
    stats = materialize_schemas(
        schemas, catalog="cat", execute=_Recorder(), property_prefix="ex"
    )
    assert stats.type_fallbacks == 1


def test_spine_skip_mode_tolerates_failed_insert() -> None:
    rec = _Recorder(fail_on="INSERT OVERWRITE")
    stats = materialize_schemas(
        _two_table_schema(), catalog="cat", execute=rec,
        property_prefix="ex", on_insert_error="skip",
    )
    assert stats.rows_inserted == 0
    assert stats.tables_created == 2  # tables still created despite insert failures


def test_spine_raise_mode_propagates_failed_insert() -> None:
    rec = _Recorder(fail_on="INSERT OVERWRITE")
    with pytest.raises(RuntimeError):
        materialize_schemas(
            _two_table_schema(), catalog="cat", execute=rec,
            property_prefix="ex", on_insert_error="raise",
        )


def test_spine_skip_mode_tolerates_failed_table_create() -> None:
    rec = _Recorder(fail_on="CREATE TABLE")
    stats = materialize_schemas(
        _two_table_schema(), catalog="cat", execute=rec,
        property_prefix="ex", on_table_error="skip",
    )
    assert stats.tables_created == 0


def test_spine_raise_mode_propagates_failed_table_create() -> None:
    rec = _Recorder(fail_on="CREATE TABLE")
    with pytest.raises(RuntimeError):
        materialize_schemas(
            _two_table_schema(), catalog="cat", execute=rec,
            property_prefix="ex", on_table_error="raise",
        )


def test_spine_skip_mode_does_not_swallow_non_warehouse_errors() -> None:
    """skip mode tolerates warehouse failures, not programming errors.

    A non-warehouse exception (here a ValueError standing in for a bug or a
    malformed spec) must propagate even with on_table_error="skip", rather than
    being logged as a skipped table and hiding the defect.
    """
    def execute(statement: str, _label: str) -> None:
        if "CREATE TABLE" in statement:
            raise ValueError("not a warehouse error")

    with pytest.raises(ValueError, match="not a warehouse error"):
        materialize_schemas(
            _two_table_schema(), catalog="cat", execute=execute,
            property_prefix="ex", on_table_error="skip",
        )


def test_spine_skips_pk_when_column_dropped() -> None:
    schemas = [{
        "uc_schema": "s", "source_id": "src",
        "tables": [{
            "name": "t",
            "columns": [{"name": "keep", "type": "int"}],
            "primary_keys": ["keep", "!!!"],  # one PK column sanitizes away
            "foreign_keys": [], "rows": [],
        }],
    }]
    stats = materialize_schemas(
        schemas, catalog="cat", execute=_Recorder(), property_prefix="ex"
    )
    assert stats.pk_constraints_added == 0
