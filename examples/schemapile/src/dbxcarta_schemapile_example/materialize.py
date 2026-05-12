"""Materialize the candidate-table JSON as Delta tables in Unity Catalog.

Reads the candidate JSON (produced by the candidate selector), creates one
UC schema per candidate entry under the schemapile catalog, then creates one
Delta table per table spec. Sample row values from schemapile are inserted
when present. The list of materialized UC schemas is written to
`.env.generated` for the dbxcarta run.

Type coercion from arbitrary schemapile DDL types to Delta types uses a
small documented map. Anything that does not match falls back to STRING.
That choice keeps the materializer simple: every table that has data lands
in UC; downstream evaluation reads the data as text where types were
ambiguous.
"""

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
from dbxcarta_schemapile_example.config import SchemaPileConfig, load_config

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient


logger = logging.getLogger(__name__)


_DECIMAL_RE = re.compile(r"^(?:DECIMAL|NUMERIC|NUMBER|DEC)\s*\((\d+)\s*,\s*(\d+)\)$")
_VARCHAR_RE = re.compile(r"^(?:VARCHAR|CHAR|CHARACTER|NVARCHAR|NCHAR)\s*\(\s*\d+\s*\)$")

_DELTA_TYPE_MAP: dict[str, str] = {
    "INT": "INT",
    "INTEGER": "INT",
    "INT4": "INT",
    "BIGINT": "BIGINT",
    "INT8": "BIGINT",
    "LONG": "BIGINT",
    "SMALLINT": "SMALLINT",
    "TINYINT": "TINYINT",
    "MEDIUMINT": "INT",
    "FLOAT": "FLOAT",
    "REAL": "FLOAT",
    "DOUBLE": "DOUBLE",
    "DOUBLE PRECISION": "DOUBLE",
    "DECIMAL": "DECIMAL(18,4)",
    "NUMERIC": "DECIMAL(18,4)",
    "NUMBER": "DECIMAL(18,4)",
    "BOOLEAN": "BOOLEAN",
    "BOOL": "BOOLEAN",
    "BIT": "BOOLEAN",
    "TEXT": "STRING",
    "LONGTEXT": "STRING",
    "MEDIUMTEXT": "STRING",
    "TINYTEXT": "STRING",
    "CHARACTER VARYING": "STRING",
    "VARCHAR": "STRING",
    "CHAR": "STRING",
    "NVARCHAR": "STRING",
    "NCHAR": "STRING",
    "STRING": "STRING",
    "DATE": "DATE",
    "DATETIME": "TIMESTAMP",
    "TIMESTAMP": "TIMESTAMP",
    "TIMESTAMPTZ": "TIMESTAMP",
    "TIMESTAMP WITHOUT TIME ZONE": "TIMESTAMP",
    "TIMESTAMP WITH TIME ZONE": "TIMESTAMP",
    "TIME": "STRING",
    "TIME WITHOUT TIME ZONE": "STRING",
    "BLOB": "BINARY",
    "BINARY": "BINARY",
    "VARBINARY": "BINARY",
    "BYTEA": "BINARY",
    "JSON": "STRING",
    "JSONB": "STRING",
    "UUID": "STRING",
    "ENUM": "STRING",
    "SET": "STRING",
}


@dataclass
class MaterializeStats:
    schemas_created: int = 0
    tables_created: int = 0
    rows_inserted: int = 0
    tables_skipped: int = 0
    type_fallbacks: int = 0


def main() -> int:
    parser = argparse.ArgumentParser(
        prog="dbxcarta-schemapile-materialize",
        description=(
            "Read the candidate-table JSON and materialize it as Delta tables"
            " in the schemapile Unity Catalog catalog."
        ),
    )
    parser.add_argument(
        "--dotenv", type=Path, default=Path(".env"),
        help="Path to the .env file to load (default: .env)",
    )
    parser.add_argument(
        "--env-out",
        type=Path,
        default=Path(".env.generated"),
        help="Path to write the generated DBXCARTA_SCHEMAS overlay (default: .env.generated)",
    )
    parser.add_argument(
        "--warehouse-id",
        type=str,
        default=None,
        help="Override DATABRICKS_WAREHOUSE_ID for this run.",
    )
    args = parser.parse_args()

    _load_dotenv(args.dotenv)
    config = load_config()

    if not config.candidate_cache.is_file():
        raise FileNotFoundError(
            f"Candidate JSON not found at {config.candidate_cache}."
            " Run dbxcarta-schemapile-select first."
        )

    import os
    warehouse_id = args.warehouse_id or os.environ.get("DATABRICKS_WAREHOUSE_ID", "")
    if not warehouse_id:
        raise ValueError(
            "DATABRICKS_WAREHOUSE_ID is required to materialize tables;"
            " set it in .env or pass --warehouse-id"
        )

    payload = json.loads(config.candidate_cache.read_text())
    schemas = payload.get("schemas") or []
    if not schemas:
        print("[schemapile] candidate JSON has no schemas; nothing to do.",
              file=sys.stderr)
        return 1

    ws = build_workspace_client()
    stats = materialize(ws, warehouse_id, config, schemas)

    uc_schemas = [s["uc_schema"] for s in schemas]
    args.env_out.write_text(f"DBXCARTA_SCHEMAS={','.join(uc_schemas)}\n")
    print(
        f"[schemapile] materialized schemas={stats.schemas_created}"
        f" tables={stats.tables_created} rows={stats.rows_inserted}"
        f" skipped={stats.tables_skipped} type_fallbacks={stats.type_fallbacks}",
        file=sys.stderr,
    )
    print(f"[schemapile] wrote {args.env_out} with DBXCARTA_SCHEMAS list",
          file=sys.stderr)
    return 0


def materialize(
    ws: "WorkspaceClient",
    warehouse_id: str,
    config: SchemaPileConfig,
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
            f" COMMENT 'schemapile source: {source_id}'",
        )
        stats.schemas_created += 1

        for table in schema_entry.get("tables", []):
            _materialize_table(
                ws, warehouse_id, config, catalog_q, schema_q,
                source_id, table, stats,
            )
    return stats


def _materialize_table(
    ws: "WorkspaceClient",
    warehouse_id: str,
    config: SchemaPileConfig,
    catalog_q: str,
    schema_q: str,
    source_id: str,
    table: dict[str, Any],
    stats: MaterializeStats,
) -> None:
    raw_name = table.get("name", "")
    safe_name = _sanitize_table_name(raw_name)
    if not safe_name:
        stats.tables_skipped += 1
        logger.warning("[schemapile] skipping table with unusable name: %r", raw_name)
        return
    table_q = quote_identifier(safe_name)
    fq = f"{catalog_q}.{schema_q}.{table_q}"

    columns = table.get("columns") or []
    if not columns:
        stats.tables_skipped += 1
        logger.warning("[schemapile] skipping table without columns: %s", raw_name)
        return

    col_defs: list[str] = []
    delta_types: dict[str, str] = {}
    for col in columns:
        col_name = col.get("name", "")
        safe_col = _sanitize_column_name(col_name)
        if not safe_col:
            continue
        raw_type = str(col.get("type", "")).strip()
        delta_type, fellback = _coerce_type(raw_type)
        if fellback:
            stats.type_fallbacks += 1
        col_defs.append(f"{quote_identifier(safe_col)} {delta_type}")
        delta_types[safe_col] = delta_type

    if not col_defs:
        stats.tables_skipped += 1
        logger.warning("[schemapile] skipping table without coercible columns: %s", raw_name)
        return

    fks_json = json.dumps(table.get("foreign_keys") or [])
    pks_json = json.dumps(table.get("primary_keys") or [])
    tbl_properties = (
        f"'schemapile.source_id' = '{_sql_escape(source_id)}',"
        f"'schemapile.original_name' = '{_sql_escape(raw_name)}',"
        f"'schemapile.primary_keys' = '{_sql_escape(pks_json)}',"
        f"'schemapile.foreign_keys' = '{_sql_escape(fks_json)}'"
    )

    create_sql = (
        f"CREATE TABLE IF NOT EXISTS {fq} (\n  "
        + ",\n  ".join(col_defs)
        + f"\n) USING DELTA TBLPROPERTIES ({tbl_properties})"
    )
    _execute(ws, warehouse_id, create_sql)
    stats.tables_created += 1

    # No VALUES insertion in v1: schemapile values are typed by the source
    # database's grammar and unreliable to coerce one-by-one. The materialized
    # table is empty-but-typed and downstream evaluation reads metadata. A
    # future enhancement can pull VALUES from the candidate JSON; the
    # candidate selector already records `has_values` per table.


def _coerce_type(raw: str) -> tuple[str, bool]:
    """Map a schemapile DDL type to a Delta type. Returns (delta_type, fellback)."""
    if not raw:
        return "STRING", True
    normalized = raw.upper().strip().strip(";").strip()
    if _DECIMAL_RE.match(normalized):
        m = _DECIMAL_RE.match(normalized)
        precision = int(m.group(1))
        scale = int(m.group(2))
        precision = max(1, min(precision, 38))
        scale = max(0, min(scale, precision))
        return f"DECIMAL({precision},{scale})", False
    if _VARCHAR_RE.match(normalized):
        return "STRING", False
    if normalized in _DELTA_TYPE_MAP:
        return _DELTA_TYPE_MAP[normalized], False
    base = normalized.split("(", 1)[0].strip()
    if base in _DELTA_TYPE_MAP:
        return _DELTA_TYPE_MAP[base], False
    return "STRING", True


_NAME_SANITIZE_RE = re.compile(r"[^a-zA-Z0-9_]+")


def _sanitize_table_name(name: str) -> str:
    cleaned = _NAME_SANITIZE_RE.sub("_", name).strip("_").lower()
    if not cleaned:
        return ""
    if cleaned[0].isdigit():
        cleaned = f"t_{cleaned}"
    return cleaned


def _sanitize_column_name(name: str) -> str:
    cleaned = _NAME_SANITIZE_RE.sub("_", name).strip("_").lower()
    if not cleaned:
        return ""
    if cleaned[0].isdigit():
        cleaned = f"c_{cleaned}"
    return cleaned


def _sql_escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace("'", "''")


def _execute(ws: "WorkspaceClient", warehouse_id: str, statement: str) -> None:
    from databricks.sdk.service.sql import ExecuteStatementRequestOnWaitTimeout

    ws.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=statement,
        wait_timeout="50s",
        on_wait_timeout=ExecuteStatementRequestOnWaitTimeout.CONTINUE,
    )


def _load_dotenv(path: Path) -> None:
    try:
        from dotenv import load_dotenv
    except ImportError:
        return
    if path.is_file():
        load_dotenv(path, override=False)


if __name__ == "__main__":
    raise SystemExit(main())
