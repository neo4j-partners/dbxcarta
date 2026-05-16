"""Materialize dense-schema candidates as Delta tables in Unity Catalog."""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from databricks.sdk import WorkspaceClient
from databricks.sdk.config import Config
from dbxcarta.spark.databricks import quote_identifier
from dbxcarta_dense_schema_example.config import DenseSchemaConfig, load_config
from dbxcarta_dense_schema_example.utils import (
    load_dotenv_file,
    read_required_warehouse_id,
)


logger = logging.getLogger(__name__)

_HTTP_TIMEOUT_S = 300
_POLL_INTERVAL_S = 3.0
_STATEMENT_TIMEOUT_S = 600.0
_DEFAULT_WORKERS = 20

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
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False, compare=False)

    def _inc(self, **kwargs: int) -> None:
        with self._lock:
            for attr, delta in kwargs.items():
                setattr(self, attr, getattr(self, attr) + delta)


def main() -> int:
    parser = argparse.ArgumentParser(prog="dbxcarta-dense-materialize")
    parser.add_argument("--dotenv", type=Path, default=Path(".env"))
    parser.add_argument("--warehouse-id", type=str, default=None)
    parser.add_argument("--workers", type=int, default=_DEFAULT_WORKERS,
                        help="parallel worker threads for table creation (default: 20)")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )

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

    profile = os.environ.get("DATABRICKS_PROFILE")
    ws = WorkspaceClient(
        config=Config(profile=profile, http_timeout_seconds=_HTTP_TIMEOUT_S)
    )
    logger.info("[dense] workers=%d", args.workers)
    stats = materialize(ws, warehouse_id, config, schemas, workers=args.workers)
    print(
        f"[dense] materialized tables={stats.tables_created}"
        f" rows={stats.rows_inserted} skipped={stats.tables_skipped}"
        f" type_fallbacks={stats.type_fallbacks}",
        file=sys.stderr,
    )
    return 0


def materialize(
    ws: WorkspaceClient,
    warehouse_id: str,
    config: DenseSchemaConfig,
    schemas: list[dict[str, Any]],
    *,
    workers: int = _DEFAULT_WORKERS,
) -> MaterializeStats:
    stats = MaterializeStats()
    catalog_q = quote_identifier(config.catalog)
    total_tables = sum(len(s.get("tables", [])) for s in schemas)
    table_idx = 0
    for schema_entry in schemas:
        uc_schema = schema_entry["uc_schema"]
        source_id = schema_entry["source_id"]
        schema_q = quote_identifier(uc_schema)
        logger.info("[dense] creating schema %s.%s", config.catalog, uc_schema)
        _execute(
            ws, warehouse_id,
            f"CREATE SCHEMA IF NOT EXISTS {catalog_q}.{schema_q}"
            f" COMMENT 'dense-schema source: {_sql_escape(source_id)}'",
            label=f"CREATE SCHEMA {uc_schema}",
        )
        stats._inc(schemas_created=1)
        tables = schema_entry.get("tables", [])
        labeled: list[tuple[str, dict[str, Any]]] = []
        for table in tables:
            table_idx += 1
            labeled.append((f"{table_idx}/{total_tables}", table))

        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {
                pool.submit(
                    _materialize_table,
                    ws, warehouse_id, catalog_q, schema_q, source_id, table, stats,
                    progress=progress,
                ): table
                for progress, table in labeled
            }
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as exc:
                    name = futures[future].get("name", "?")
                    logger.error("[dense] table %s failed: %s", name, exc)
    return stats


def _materialize_table(
    ws: WorkspaceClient,
    warehouse_id: str,
    catalog_q: str,
    schema_q: str,
    source_id: str,
    table: dict[str, Any],
    stats: MaterializeStats,
    *,
    progress: str = "",
) -> None:
    raw_name = table.get("name", "")
    safe_name = _sanitize_name(raw_name)
    if not safe_name:
        stats._inc(tables_skipped=1)
        return
    table_q = quote_identifier(safe_name)
    fq = f"{catalog_q}.{schema_q}.{table_q}"
    logger.info("[dense] table %s %s", progress, fq)

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
            stats._inc(type_fallbacks=1)
        col_defs.append(f"{quote_identifier(safe_col)} {delta_type}")
        safe_col_names.append(safe_col)
        keep_col_mask.append(True)

    if not col_defs:
        stats._inc(tables_skipped=1)
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
    _execute(ws, warehouse_id, create_sql, label=f"CREATE TABLE {fq}")
    stats._inc(tables_created=1)

    raw_rows = table.get("rows") or []
    if raw_rows:
        kept_rows = [
            tuple(v for v, keep in zip(row, keep_col_mask) if keep)
            for row in raw_rows
        ]
        kept_rows = [r for r in kept_rows if len(r) == len(safe_col_names)]
        if kept_rows:
            insert_sql = _build_insert(fq, safe_col_names, kept_rows)
            try:
                _execute(ws, warehouse_id, insert_sql, label=f"INSERT OVERWRITE {fq}")
                stats._inc(rows_inserted=len(kept_rows))
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
        f"INSERT OVERWRITE TABLE {fq_table} ({columns_clause}) VALUES\n  "
        + ",\n  ".join(values_clauses)
    )


def _execute(
    ws: WorkspaceClient,
    warehouse_id: str,
    statement: str,
    *,
    label: str = "",
) -> None:
    from databricks.sdk.service.sql import (
        ExecuteStatementRequestOnWaitTimeout,
        StatementState,
    )

    t0 = time.monotonic()
    response = ws.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=statement,
        wait_timeout="50s",
        on_wait_timeout=ExecuteStatementRequestOnWaitTimeout.CONTINUE,
    )
    state = response.status.state if response.status else StatementState.SUCCEEDED
    statement_id = response.statement_id

    deadline = t0 + _STATEMENT_TIMEOUT_S
    while state in (StatementState.PENDING, StatementState.RUNNING):
        if time.monotonic() > deadline:
            raise TimeoutError(
                f"Statement timed out after {_STATEMENT_TIMEOUT_S:.0f}s: {label or statement[:80]}"
            )
        time.sleep(_POLL_INTERVAL_S)
        response = ws.statement_execution.get_statement(statement_id)
        state = response.status.state if response.status else StatementState.SUCCEEDED

    if state != StatementState.SUCCEEDED:
        err = response.status.error if response.status else None
        raise RuntimeError(
            f"Statement failed (state={state}): {label or statement[:80]}"
            + (f" — {err}" if err else "")
        )

    logger.debug("[dense] %.1fs %s", time.monotonic() - t0, label or statement[:80])


if __name__ == "__main__":
    raise SystemExit(main())
