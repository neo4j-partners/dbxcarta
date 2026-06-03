"""Materialize dense-schema candidates as Delta tables in Unity Catalog."""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import re
import sys
import threading
import time
from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from databricks.sdk import WorkspaceClient
from databricks.sdk.config import Config
from dbxcarta.core.executor import catalog_exists
from dbxcarta.core.identifiers import quote_identifier
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
    pk_constraints_added: int = 0
    fk_constraints_added: int = 0
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False, compare=False)

    def _inc(self, **kwargs: int) -> None:
        with self._lock:
            for attr, delta in kwargs.items():
                setattr(self, attr, getattr(self, attr) + delta)


def main() -> int:
    parser = argparse.ArgumentParser(prog="dbxcarta-dense-materialize")
    parser.add_argument("--dotenv", type=Path, default=Path(__file__).resolve().parents[2] / ".env")
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
        f" type_fallbacks={stats.type_fallbacks}"
        f" pks={stats.pk_constraints_added} fks={stats.fk_constraints_added}",
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

    # Provision the data catalog itself. The ops plane (volume, summary) lives
    # in a separate catalog that `dbxcarta-submit bootstrap` creates from
    # DATABRICKS_VOLUME_PATH; the data catalog is this example's own, so
    # materialize owns creating it. load_config already refuses a
    # protected/project catalog name, so this never targets a shared catalog.
    # Skip the create when the catalog already exists: on Default-Storage
    # accounts CREATE CATALOG fails without a MANAGED LOCATION even with IF NOT
    # EXISTS, so a pre-created (e.g. UI-created) catalog must not be re-created.
    if not catalog_exists(ws, warehouse_id, config.catalog):
        _execute(
            ws, warehouse_id,
            f"CREATE CATALOG IF NOT EXISTS {catalog_q}"
            " COMMENT 'dense-schema materialize: data catalog'",
            label=f"CREATE CATALOG {config.catalog}",
        )

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

        materialized: dict[str, _MaterializedTable] = {}
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
                    result = future.result()
                except Exception as exc:
                    name = futures[future].get("name", "?")
                    logger.error("[dense] table %s failed: %s", name, exc)
                    continue
                if result is not None:
                    materialized[result.safe_name] = result

        # Second pass: add foreign keys only after every table and its PK
        # exists. Tables are created concurrently above, so a FK target's PK
        # may not exist until the pool has fully drained.
        _add_foreign_keys(
            ws, warehouse_id, catalog_q, schema_q, materialized, stats,
        )
    return stats


@dataclass
class _MaterializedTable:
    """What was actually created for one table, for the FK second pass.

    Carries sanitized identifiers so the FK pass references the
    actually-created columns/tables, not the original names.
    """

    safe_name: str
    columns: frozenset[str]
    pk_columns: tuple[str, ...]
    foreign_keys: list[dict[str, Any]]


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
) -> _MaterializedTable | None:
    raw_name = table.get("name", "")
    safe_name = _sanitize_name(raw_name)
    if not safe_name:
        stats._inc(tables_skipped=1)
        return None
    table_q = quote_identifier(safe_name)
    fq = f"{catalog_q}.{schema_q}.{table_q}"
    logger.info("[dense] table %s %s", progress, fq)

    columns = table.get("columns") or []
    if not columns:
        stats.tables_skipped += 1
        return None

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
        return None

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

    safe_col_set = frozenset(safe_col_names)
    pk_columns = _add_primary_key(
        ws, warehouse_id, fq, safe_name, table.get("primary_keys") or [],
        safe_col_set, stats,
    )
    return _MaterializedTable(
        safe_name=safe_name,
        columns=safe_col_set,
        pk_columns=pk_columns,
        foreign_keys=table.get("foreign_keys") or [],
    )


_MAX_CONSTRAINT_NAME = 255


def _constraint_name(prefix: str, *parts: str) -> str:
    """Deterministic, length-guarded constraint name.

    Builds ``<prefix>_<parts...>`` from already-sanitized identifiers. If the
    result exceeds the UC limit, truncate the variable portion and append a
    stable hash suffix so distinct inputs stay distinct.
    """
    body = "__".join(p for p in parts if p)
    name = f"{prefix}_{body}" if body else prefix
    if len(name) <= _MAX_CONSTRAINT_NAME:
        return name
    digest = hashlib.sha1(name.encode("utf-8")).hexdigest()[:8]
    keep = _MAX_CONSTRAINT_NAME - len(prefix) - 1 - len(digest) - 1
    return f"{prefix}_{name[len(prefix) + 1:][:keep]}_{digest}"


def _add_primary_key(
    ws: WorkspaceClient,
    warehouse_id: str,
    fq: str,
    safe_table: str,
    primary_keys: list[str],
    column_set: frozenset[str],
    stats: MaterializeStats,
) -> tuple[str, ...]:
    """Add a PRIMARY KEY constraint; return the sanitized PK columns.

    Returns an empty tuple when no valid PK can be declared. Each ALTER is
    tolerant: a failure logs a warning and continues so one bad table does
    not abort the run.
    """
    safe_pk_cols = [_sanitize_name(c) for c in primary_keys]
    safe_pk_cols = [c for c in safe_pk_cols if c and c in column_set]
    if len(safe_pk_cols) != len(primary_keys):
        # A PK column was dropped during sanitization or is missing: a partial
        # PK would be wrong, so skip the whole constraint.
        return ()
    if not safe_pk_cols:
        return ()

    for col in safe_pk_cols:
        col_q = quote_identifier(col)
        try:
            _execute(
                ws, warehouse_id,
                f"ALTER TABLE {fq} ALTER COLUMN {col_q} SET NOT NULL",
                label=f"SET NOT NULL {fq}.{col}",
            )
        except Exception as exc:
            logger.warning("[dense] set not null failed for %s.%s: %s", fq, col, exc)
            return ()

    pk_name = _constraint_name("pk", safe_table)
    cols_clause = ", ".join(quote_identifier(c) for c in safe_pk_cols)
    try:
        _execute(
            ws, warehouse_id,
            f"ALTER TABLE {fq} ADD CONSTRAINT {quote_identifier(pk_name)}"
            f" PRIMARY KEY ({cols_clause})",
            label=f"ADD PRIMARY KEY {fq}",
        )
        stats._inc(pk_constraints_added=1)
    except Exception as exc:
        logger.warning("[dense] add primary key failed for %s: %s", fq, exc)
        return ()
    return tuple(safe_pk_cols)


def _add_foreign_keys(
    ws: WorkspaceClient,
    warehouse_id: str,
    catalog_q: str,
    schema_q: str,
    materialized: dict[str, "_MaterializedTable"],
    stats: MaterializeStats,
) -> None:
    """Second pass: add FK constraints after all tables and PKs exist.

    Skips any FK whose child/parent columns were dropped during sanitization
    or whose target table/columns do not exist among the materialized tables.
    Each ALTER is tolerant: a failure logs a warning and continues.
    """
    for child in materialized.values():
        child_fq = f"{catalog_q}.{schema_q}.{quote_identifier(child.safe_name)}"
        for fk in child.foreign_keys:
            src_cols = [_sanitize_name(c) for c in (fk.get("columns") or [])]
            ref_cols = [_sanitize_name(c) for c in (fk.get("referred_columns") or [])]
            parent_name = _sanitize_name(fk.get("foreign_table", ""))
            raw_src = fk.get("columns") or []
            raw_ref = fk.get("referred_columns") or []

            if not parent_name or parent_name not in materialized:
                continue
            if len(src_cols) != len(raw_src) or len(ref_cols) != len(raw_ref):
                continue
            if not src_cols or len(src_cols) != len(ref_cols):
                continue
            if any(c not in child.columns for c in src_cols):
                continue
            parent = materialized[parent_name]
            if any(c not in parent.columns for c in ref_cols):
                continue

            parent_fq = f"{catalog_q}.{schema_q}.{quote_identifier(parent_name)}"
            fk_name = _constraint_name("fk", child.safe_name, *src_cols)
            src_clause = ", ".join(quote_identifier(c) for c in src_cols)
            ref_clause = ", ".join(quote_identifier(c) for c in ref_cols)
            try:
                _execute(
                    ws, warehouse_id,
                    f"ALTER TABLE {child_fq} ADD CONSTRAINT"
                    f" {quote_identifier(fk_name)} FOREIGN KEY ({src_clause})"
                    f" REFERENCES {parent_fq} ({ref_clause})",
                    label=f"ADD FOREIGN KEY {child_fq} -> {parent_fq}",
                )
                stats._inc(fk_constraints_added=1)
            except Exception as exc:
                logger.warning(
                    "[dense] add foreign key failed for %s -> %s: %s",
                    child_fq, parent_fq, exc,
                )


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


def _build_insert(
    fq_table: str, col_names: list[str], rows: Sequence[Sequence[Any]]
) -> str:
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
    if statement_id is None:
        raise RuntimeError(
            f"No statement id returned for: {label or statement[:80]}"
        )

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
