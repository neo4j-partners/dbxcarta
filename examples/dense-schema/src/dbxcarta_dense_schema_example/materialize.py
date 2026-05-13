"""Materialize dense-schema candidates as Delta tables in Unity Catalog."""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, TYPE_CHECKING

from dbxcarta.databricks import build_workspace_client, quote_identifier
from dbxcarta_dense_schema_example.config import DenseSchemaConfig, load_config
from dbxcarta_dense_schema_example.utils import (
    load_dotenv_file,
    read_required_warehouse_id,
)

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient


logger = logging.getLogger(__name__)

_DECIMAL_RE = re.compile(r"^(?:DECIMAL|NUMERIC|NUMBER|DEC)\s*\((\d+)\s*,\s*(\d+)\)$")
_VARCHAR_RE = re.compile(r"^(?:VARCHAR|CHAR|CHARACTER|NVARCHAR|NCHAR)\s*\(\s*\d+\s*\)$")
_NAME_SANITIZE_RE = re.compile(r"[^a-zA-Z0-9_]+")

_DELTA_TYPE_MAP: dict[str, str] = {
    "INT": "INT", "INTEGER": "INT", "INT4": "INT",
    "BIGINT": "BIGINT", "INT8": "BIGINT", "LONG": "BIGINT",
    "SMALLINT": "SMALLINT", "TINYINT": "TINYINT", "MEDIUMINT": "INT",
    "FLOAT": "FLOAT", "REAL": "FLOAT", "DOUBLE": "DOUBLE",
    "DOUBLE PRECISION": "DOUBLE",
    "DECIMAL": "DECIMAL(18,4)", "NUMERIC": "DECIMAL(18,4)",
    "NUMBER": "DECIMAL(18,4)",
    "BOOLEAN": "BOOLEAN", "BOOL": "BOOLEAN", "BIT": "BOOLEAN",
    "TEXT": "STRING", "LONGTEXT": "STRING", "MEDIUMTEXT": "STRING",
    "TINYTEXT": "STRING", "CHARACTER VARYING": "STRING",
    "VARCHAR": "STRING", "CHAR": "STRING", "NVARCHAR": "STRING",
    "NCHAR": "STRING", "STRING": "STRING",
    "DATE": "DATE", "DATETIME": "TIMESTAMP", "TIMESTAMP": "TIMESTAMP",
    "TIMESTAMPTZ": "TIMESTAMP",
    "TIMESTAMP WITHOUT TIME ZONE": "TIMESTAMP",
    "TIMESTAMP WITH TIME ZONE": "TIMESTAMP",
    "TIME": "STRING", "TIME WITHOUT TIME ZONE": "STRING",
    "BLOB": "BINARY", "BINARY": "BINARY", "VARBINARY": "BINARY",
    "BYTEA": "BINARY", "JSON": "STRING", "JSONB": "STRING",
    "UUID": "STRING", "ENUM": "STRING", "SET": "STRING",
}


@dataclass
class MaterializeStats:
    schemas_created: int = 0
    tables_created: int = 0
    rows_inserted: int = 0
    tables_skipped: int = 0
    type_fallbacks: int = 0


def main() -> int:
    parser = argparse.ArgumentParser(prog="dbxcarta-dense-materialize")
    parser.add_argument("--dotenv", type=Path, default=Path(".env"))
    parser.add_argument("--warehouse-id", type=str, default=None)
    args = parser.parse_args()

    load_dotenv_file(args.dotenv)
    config = load_config()

    if not config.candidate_cache.is_file():
        raise FileNotFoundError(
            f"Candidate JSON not found at {config.candidate_cache}."
            " Run dbxcarta-dense-generate first."
        )

    warehouse_id = read_required_warehouse_id(
        args.warehouse_id, operation="materialize tables"
    )
    payload = json.loads(config.candidate_cache.read_text())
    schemas = payload.get("schemas") or []
    if not schemas:
        print("[dense] no schemas in candidates JSON", file=sys.stderr)
        return 1

    ws = build_workspace_client()
    stats = materialize(ws, warehouse_id, config, schemas)
    print(
        f"[dense] materialized tables={stats.tables_created}"
        f" rows={stats.rows_inserted} skipped={stats.tables_skipped}"
        f" type_fallbacks={stats.type_fallbacks}",
        file=sys.stderr,
    )
    return 0


def materialize(
    ws: "WorkspaceClient",
    warehouse_id: str,
    config: DenseSchemaConfig,
    schemas: list[dict[str, Any]],
) -> MaterializeStats:
    stats = MaterializeStats()
    catalog_q = quote_identifier(config.catalog)
    for schema_entry in schemas:
        uc_schema = schema_entry["uc_schema"]
        source_id = schema_entry["source_id"]
        schema_q = quote_identifier(uc_schema)
        _execute(
            ws, warehouse_id,
            f"CREATE SCHEMA IF NOT EXISTS {catalog_q}.{schema_q}"
            f" COMMENT 'dense-schema source: {_sql_escape(source_id)}'",
        )
        stats.schemas_created += 1
        for table in schema_entry.get("tables", []):
            _materialize_table(
                ws, warehouse_id, catalog_q, schema_q, source_id, table, stats
            )
    return stats


def _materialize_table(
    ws: "WorkspaceClient",
    warehouse_id: str,
    catalog_q: str,
    schema_q: str,
    source_id: str,
    table: dict[str, Any],
    stats: MaterializeStats,
) -> None:
    raw_name = table.get("name", "")
    safe_name = _sanitize_name(raw_name)
    if not safe_name:
        stats.tables_skipped += 1
        return
    table_q = quote_identifier(safe_name)
    fq = f"{catalog_q}.{schema_q}.{table_q}"

    columns = table.get("columns") or []
    if not columns:
        stats.tables_skipped += 1
        return

    col_defs: list[str] = []
    safe_col_names: list[str] = []
    keep_col_mask: list[bool] = []
    for col in columns:
        col_name = col.get("name", "")
        safe_col = _sanitize_name(col_name)
        if not safe_col:
            keep_col_mask.append(False)
            continue
        raw_type = str(col.get("type", "")).strip()
        delta_type, fellback = _coerce_type(raw_type)
        if fellback:
            stats.type_fallbacks += 1
        col_defs.append(f"{quote_identifier(safe_col)} {delta_type}")
        safe_col_names.append(safe_col)
        keep_col_mask.append(True)

    if not col_defs:
        stats.tables_skipped += 1
        return

    fks_json = json.dumps(table.get("foreign_keys") or [])
    pks_json = json.dumps(table.get("primary_keys") or [])
    tbl_properties = (
        f"'dense.source_id' = '{_sql_escape(source_id)}',"
        f"'dense.original_name' = '{_sql_escape(raw_name)}',"
        f"'dense.primary_keys' = '{_sql_escape(pks_json)}',"
        f"'dense.foreign_keys' = '{_sql_escape(fks_json)}'"
    )
    create_sql = (
        f"CREATE TABLE IF NOT EXISTS {fq} (\n  "
        + ",\n  ".join(col_defs)
        + f"\n) USING DELTA TBLPROPERTIES ({tbl_properties})"
    )
    _execute(ws, warehouse_id, create_sql)
    stats.tables_created += 1

    raw_rows = table.get("rows") or []
    if raw_rows:
        kept_rows = [
            tuple(v for v, keep in zip(row, keep_col_mask) if keep)
            for row in raw_rows
        ]
        kept_rows = [r for r in kept_rows if len(r) == len(safe_col_names)]
        if kept_rows:
            _execute(ws, warehouse_id, f"DELETE FROM {fq}")
            insert_sql = _build_insert(fq, safe_col_names, kept_rows)
            try:
                _execute(ws, warehouse_id, insert_sql)
                stats.rows_inserted += len(kept_rows)
            except Exception as exc:
                logger.warning("[dense] insert failed for %s: %s", fq, exc)


def _coerce_type(raw: str) -> tuple[str, bool]:
    if not raw:
        return "STRING", True
    normalized = raw.upper().strip().strip(";").strip()
    m = _DECIMAL_RE.match(normalized)
    if m:
        precision = max(1, min(int(m.group(1)), 38))
        scale = max(0, min(int(m.group(2)), precision))
        return f"DECIMAL({precision},{scale})", False
    if _VARCHAR_RE.match(normalized):
        return "STRING", False
    if normalized in _DELTA_TYPE_MAP:
        return _DELTA_TYPE_MAP[normalized], False
    base = normalized.split("(", 1)[0].strip()
    if base in _DELTA_TYPE_MAP:
        return _DELTA_TYPE_MAP[base], False
    return "STRING", True


def _sanitize_name(name: str) -> str:
    cleaned = _NAME_SANITIZE_RE.sub("_", name).strip("_").lower()
    if not cleaned:
        return ""
    if cleaned[0].isdigit():
        cleaned = f"t_{cleaned}"
    return cleaned


def _sql_escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace("'", "''")


def _render_value(value: Any) -> str:
    if value is None:
        return "NULL"
    return f"'{_sql_escape(str(value))}'"


def _build_insert(fq_table: str, col_names: list[str], rows) -> str:
    columns_clause = ", ".join(quote_identifier(c) for c in col_names)
    values_clauses = [
        "(" + ", ".join(_render_value(v) for v in row) + ")" for row in rows
    ]
    return (
        f"INSERT INTO {fq_table} ({columns_clause}) VALUES\n  "
        + ",\n  ".join(values_clauses)
    )


def _execute(ws: "WorkspaceClient", warehouse_id: str, statement: str) -> None:
    from databricks.sdk.service.sql import ExecuteStatementRequestOnWaitTimeout

    ws.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=statement,
        wait_timeout="50s",
        on_wait_timeout=ExecuteStatementRequestOnWaitTimeout.CONTINUE,
    )


if __name__ == "__main__":
    raise SystemExit(main())
