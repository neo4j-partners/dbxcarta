"""Helpers for parsing dbxcarta catalog node identifiers."""

from __future__ import annotations


def schema_from_node_id(node_id: str) -> str | None:
    """Extract the schema component from a catalog.schema.table[.column] ID."""
    parts = node_id.split(".")
    return parts[1] if len(parts) >= 3 else None
