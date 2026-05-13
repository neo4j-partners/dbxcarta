"""Shared helpers for dense-schema entrypoints."""

from __future__ import annotations

from pathlib import Path


def load_dotenv_file(path: Path) -> None:
    try:
        from dotenv import load_dotenv
    except ImportError:
        return
    if path.is_file():
        load_dotenv(path, override=True)


def read_required_warehouse_id(
    override: str | None,
    operation: str,
    extra_hint: str = "",
) -> str:
    import os

    wid = (override or os.environ.get("DATABRICKS_WAREHOUSE_ID", "")).strip()
    if not wid:
        hint = f" {extra_hint}" if extra_hint else ""
        raise ValueError(
            f"DATABRICKS_WAREHOUSE_ID is required for {operation}.{hint}"
        )
    return wid
