from __future__ import annotations

from dbxcarta.core.identifiers import sanitize_identifier

# --- sanitize_identifier -------------------------------------------------


def test_sanitize_identifier_cleans_and_lowercases() -> None:
    assert sanitize_identifier("Order Items!") == "order_items"


def test_sanitize_identifier_prefixes_leading_digit() -> None:
    assert sanitize_identifier("9lives", prefix="t") == "t_9lives"
    assert sanitize_identifier("1col", prefix="c") == "c_1col"


def test_sanitize_identifier_empty_when_nothing_usable() -> None:
    assert sanitize_identifier("!!!") == ""
