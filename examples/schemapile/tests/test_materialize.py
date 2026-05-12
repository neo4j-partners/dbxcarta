from __future__ import annotations

from dbxcarta_schemapile_example.materialize import (
    _coerce_type,
    _sanitize_column_name,
    _sanitize_table_name,
    _sql_escape,
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
