"""Shared entrypoint helpers for the SchemaPile example."""

from __future__ import annotations

import os
from pathlib import Path


def load_dotenv_file(path: Path) -> None:
    """Load a dotenv file when python-dotenv is installed and the file exists."""
    try:
        from dotenv import load_dotenv
    except ImportError:
        return
    if path.is_file():
        load_dotenv(path, override=False)


def read_required_warehouse_id(
    override: str | None,
    *,
    operation: str,
    extra_hint: str = "",
) -> str:
    """Return the SQL warehouse id from CLI override or environment."""
    warehouse_id = (override or os.environ.get("DATABRICKS_WAREHOUSE_ID", "")).strip()
    if not warehouse_id:
        hint = f" {extra_hint}" if extra_hint else ""
        raise ValueError(
            f"DATABRICKS_WAREHOUSE_ID is required for {operation};"
            f" set it in .env or pass --warehouse-id{hint}"
        )
    return warehouse_id
