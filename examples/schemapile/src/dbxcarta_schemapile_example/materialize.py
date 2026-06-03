"""Materialize the candidate-table JSON as Delta tables in Unity Catalog.

Reads the candidate JSON (produced by the candidate selector), creates one
UC schema per candidate entry under the schemapile catalog, then creates one
Delta table per table spec. Sample row values from schemapile are inserted
when present. No schema list is emitted: schemapile_lakehouse is a dedicated,
data-only catalog, so the dbxcarta run auto-discovers every materialized
schema from a blank `DBXCARTA_SCHEMAS`.

Type coercion from arbitrary schemapile DDL types to Delta types uses a
small documented map. Anything that does not match falls back to STRING.
That choice keeps the materializer simple: every table that has data lands
in UC; downstream evaluation reads the data as text where types were
ambiguous.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, TYPE_CHECKING

from dbxcarta.core.executor import catalog_exists, execute_ddl
from dbxcarta.core.identifiers import quote_identifier
from dbxcarta.core.workspace import build_workspace_client
from dbxcarta_schemapile_example.config import SchemaPileConfig, load_config
from dbxcarta_schemapile_example.utils import (
    load_dotenv_file,
    read_required_warehouse_id,
)

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
    primary_keys_added: int = 0
    foreign_keys_added: int = 0


# UC constraint names must stay within the identifier length limit (~255 chars).
# Derived names are truncated and disambiguated with a stable hash suffix when
# they would exceed this bound.
_CONSTRAINT_NAME_LIMIT = 255


def main() -> int:
    parser = argparse.ArgumentParser(
        prog="dbxcarta-schemapile-materialize",
        description=(
            "Read the candidate-table JSON and materialize it as Delta tables"
            " in the schemapile Unity Catalog catalog."
        ),
    )
    parser.add_argument(
        "--dotenv", type=Path, default=Path(__file__).resolve().parents[2] / ".env",
        help="Path to the .env file to load (default: example directory .env)",
    )
    parser.add_argument(
        "--warehouse-id",
        type=str,
        default=None,
        help="Override DATABRICKS_WAREHOUSE_ID for this run.",
    )
    args = parser.parse_args()

    load_dotenv_file(args.dotenv)
    config = load_config()

    if not config.candidate_cache.is_file():
        raise FileNotFoundError(
            f"Candidate JSON not found at {config.candidate_cache}."
            " Run dbxcarta-schemapile-select first."
        )

    warehouse_id = read_required_warehouse_id(
        args.warehouse_id,
        operation="materialize tables",
    )

    payload = json.loads(config.candidate_cache.read_text())
    schemas = payload.get("schemas") or []
    if not schemas:
        print("[schemapile] candidate JSON has no schemas; nothing to do.",
              file=sys.stderr)
        return 1

    ws = build_workspace_client()
    stats = materialize(ws, warehouse_id, config, schemas)

    print(
        f"[schemapile] materialized schemas={stats.schemas_created}"
        f" tables={stats.tables_created} rows={stats.rows_inserted}"
        f" skipped={stats.tables_skipped} type_fallbacks={stats.type_fallbacks}"
        f" primary_keys={stats.primary_keys_added}"
        f" foreign_keys={stats.foreign_keys_added}",
        file=sys.stderr,
    )
    return 0


def materialize(
    ws: "WorkspaceClient",
    warehouse_id: str,
    config: SchemaPileConfig,
    schemas: list[dict[str, Any]],
) -> MaterializeStats:
    stats = MaterializeStats()
    catalog_q = quote_identifier(config.catalog)

    # Provision the data catalog itself. The ops plane (volume, summary) lives
    # in a separate catalog that `dbxcarta-submit bootstrap` creates from
    # DATABRICKS_VOLUME_PATH; the data catalog is this example's own, so
    # materialize owns creating it. config.load_config already refuses a
    # protected/project catalog name, so this never targets a shared catalog.
    # Skip the create when the catalog already exists: on Default-Storage
    # accounts CREATE CATALOG fails without a MANAGED LOCATION even with IF NOT
    # EXISTS, so a pre-created (e.g. UI-created) catalog must not be re-created.
    if not catalog_exists(ws, warehouse_id, config.catalog):
        _execute(
            ws, warehouse_id,
            f"CREATE CATALOG IF NOT EXISTS {catalog_q}"
            " COMMENT 'schemapile materialize: data catalog'",
        )

    for schema_entry in schemas:
        uc_schema = schema_entry["uc_schema"]
        source_id = schema_entry["source_id"]
        schema_q = quote_identifier(uc_schema)

        _execute(
            ws, warehouse_id,
            f"CREATE SCHEMA IF NOT EXISTS {catalog_q}.{schema_q}"
            f" COMMENT 'schemapile source: {_sql_escape(source_id)}'",
        )
        stats.schemas_created += 1

        # First pass: create every table, insert rows, and add its PRIMARY KEY.
        # `materialized` maps the sanitized table name to the set of sanitized
        # column names that were actually created, so the FK pass can skip any
        # foreign key whose target table or referred columns never landed in UC.
        materialized: dict[str, _MaterializedTable] = {}
        for table in schema_entry.get("tables", []):
            result = _materialize_table(
                ws, warehouse_id, config, catalog_q, schema_q,
                source_id, table, stats,
            )
            if result is not None:
                materialized[result.safe_name] = result

        # Second pass: add FOREIGN KEYs only after all tables and their PKs
        # exist. A FK cannot reference a table whose PK is not yet declared,
        # and self-referential FKs are valid here because the PK is in place.
        for result in materialized.values():
            _add_foreign_keys(
                ws, warehouse_id, catalog_q, schema_q, result, materialized, stats,
            )
    return stats


@dataclass
class _MaterializedTable:
    """A table that was actually created in UC, with the identifiers needed to
    declare its constraints. `safe_columns` is the set of sanitized column names
    present in the created table; `foreign_keys` is the source spec's raw FK
    list, deferred to the second pass."""

    safe_name: str
    safe_columns: set[str]
    foreign_keys: list[dict[str, Any]]


def _materialize_table(
    ws: "WorkspaceClient",
    warehouse_id: str,
    config: SchemaPileConfig,
    catalog_q: str,
    schema_q: str,
    source_id: str,
    table: dict[str, Any],
    stats: MaterializeStats,
) -> _MaterializedTable | None:
    raw_name = table.get("name", "")
    safe_name = _sanitize_table_name(raw_name)
    if not safe_name:
        stats.tables_skipped += 1
        logger.warning("[schemapile] skipping table with unusable name: %r", raw_name)
        return None
    table_q = quote_identifier(safe_name)
    fq = f"{catalog_q}.{schema_q}.{table_q}"

    columns = table.get("columns") or []
    if not columns:
        stats.tables_skipped += 1
        logger.warning("[schemapile] skipping table without columns: %s", raw_name)
        return None

    col_defs: list[str] = []
    safe_col_names: list[str] = []
    keep_col_mask: list[bool] = []
    for col in columns:
        col_name = col.get("name", "")
        safe_col = _sanitize_column_name(col_name)
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
        logger.warning("[schemapile] skipping table without coercible columns: %s", raw_name)
        return None

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
                logger.warning(
                    "[schemapile] insert failed for %s.%s.%s: %s",
                    catalog_q, schema_q, table_q, exc,
                )

    safe_col_set = set(safe_col_names)
    _add_primary_key(
        ws, warehouse_id, fq, safe_name, table, safe_col_set, stats,
    )

    return _MaterializedTable(
        safe_name=safe_name,
        safe_columns=safe_col_set,
        foreign_keys=list(table.get("foreign_keys") or []),
    )


def _add_primary_key(
    ws: "WorkspaceClient",
    warehouse_id: str,
    fq: str,
    safe_name: str,
    table: dict[str, Any],
    safe_columns: set[str],
    stats: MaterializeStats,
) -> None:
    """Declare an informational PRIMARY KEY for the table.

    UC requires PK columns to be NOT NULL, so each PK column is set NOT NULL
    before the constraint is added. PK columns that were dropped during
    sanitization (or that never landed as real columns) are skipped; if any
    PK column is missing the PK is not declared. Each ALTER is tolerant: a
    failure is logged and the run continues, mirroring the INSERT handling.
    """
    raw_pks = table.get("primary_keys") or []
    if not raw_pks:
        return
    safe_pks: list[str] = []
    for raw_col in raw_pks:
        safe_col = _sanitize_column_name(str(raw_col))
        if not safe_col or safe_col not in safe_columns:
            logger.warning(
                "[schemapile] skipping PK on %s: column %r not materialized",
                safe_name, raw_col,
            )
            return
        safe_pks.append(safe_col)
    if not safe_pks:
        return

    for safe_col in safe_pks:
        try:
            _execute(
                ws, warehouse_id,
                f"ALTER TABLE {fq} ALTER COLUMN {quote_identifier(safe_col)}"
                " SET NOT NULL",
            )
        except Exception as exc:
            logger.warning(
                "[schemapile] SET NOT NULL failed for %s.%s: %s",
                safe_name, safe_col, exc,
            )
            return

    pk_name = _constraint_name("pk", [safe_name])
    cols_clause = ", ".join(quote_identifier(c) for c in safe_pks)
    try:
        _execute(
            ws, warehouse_id,
            f"ALTER TABLE {fq} ADD CONSTRAINT {quote_identifier(pk_name)}"
            f" PRIMARY KEY ({cols_clause})",
        )
        stats.primary_keys_added += 1
    except Exception as exc:
        logger.warning(
            "[schemapile] ADD PRIMARY KEY failed for %s: %s", safe_name, exc,
        )


def _add_foreign_keys(
    ws: "WorkspaceClient",
    warehouse_id: str,
    catalog_q: str,
    schema_q: str,
    child: "_MaterializedTable",
    materialized: dict[str, "_MaterializedTable"],
    stats: MaterializeStats,
) -> None:
    """Declare informational FOREIGN KEYs for one child table.

    Runs in the second pass so the parent table's PRIMARY KEY already exists.
    Each FK references a table within the same schema (`foreign_table`). FKs
    whose child or referred columns were dropped during sanitization, or whose
    target table was not materialized, are skipped. Each ALTER is tolerant of
    failure to keep one bad table from aborting the run.
    """
    child_fq = f"{catalog_q}.{schema_q}.{quote_identifier(child.safe_name)}"
    for fk in child.foreign_keys:
        safe_cols = _safe_columns(fk.get("columns") or [])
        if safe_cols is None or not all(c in child.safe_columns for c in safe_cols):
            logger.warning(
                "[schemapile] skipping FK on %s: child columns not materialized",
                child.safe_name,
            )
            continue

        parent_name = _sanitize_table_name(str(fk.get("foreign_table") or ""))
        parent = materialized.get(parent_name)
        if parent is None:
            logger.warning(
                "[schemapile] skipping FK on %s: target table %r not materialized",
                child.safe_name, fk.get("foreign_table"),
            )
            continue

        safe_referred = _safe_columns(fk.get("referred_columns") or [])
        if (
            safe_referred is None
            or len(safe_referred) != len(safe_cols)
            or not all(c in parent.safe_columns for c in safe_referred)
        ):
            logger.warning(
                "[schemapile] skipping FK on %s -> %s: referred columns not"
                " materialized or arity mismatch",
                child.safe_name, parent_name,
            )
            continue

        fk_name = _constraint_name("fk", [child.safe_name, *safe_cols])
        parent_fq = f"{catalog_q}.{schema_q}.{quote_identifier(parent_name)}"
        child_cols = ", ".join(quote_identifier(c) for c in safe_cols)
        referred_cols = ", ".join(quote_identifier(c) for c in safe_referred)
        try:
            _execute(
                ws, warehouse_id,
                f"ALTER TABLE {child_fq} ADD CONSTRAINT {quote_identifier(fk_name)}"
                f" FOREIGN KEY ({child_cols})"
                f" REFERENCES {parent_fq} ({referred_cols})",
            )
            stats.foreign_keys_added += 1
        except Exception as exc:
            logger.warning(
                "[schemapile] ADD FOREIGN KEY failed for %s -> %s: %s",
                child.safe_name, parent_name, exc,
            )


def _safe_columns(raw_cols: list[Any]) -> list[str] | None:
    """Sanitize a list of column names. Returns None if any name is unusable,
    so a constraint that loses a column is skipped rather than silently
    referencing the wrong columns."""
    safe: list[str] = []
    for raw_col in raw_cols:
        cleaned = _sanitize_column_name(str(raw_col))
        if not cleaned:
            return None
        safe.append(cleaned)
    return safe or None


def _constraint_name(prefix: str, parts: list[str]) -> str:
    """Build a deterministic constraint name from sanitized identifier parts,
    guarding the UC identifier length limit with a stable hash suffix."""
    base = f"{prefix}_{'__'.join(parts)}"
    if len(base) <= _CONSTRAINT_NAME_LIMIT:
        return base
    digest = hashlib.sha1(base.encode("utf-8")).hexdigest()[:12]
    keep = _CONSTRAINT_NAME_LIMIT - len(digest) - 1
    return f"{base[:keep]}_{digest}"


def _coerce_type(raw: str) -> tuple[str, bool]:
    """Map a schemapile DDL type to a Delta type. Returns (delta_type, fellback)."""
    if not raw:
        return "STRING", True
    normalized = raw.upper().strip().strip(";").strip()
    decimal_match = _DECIMAL_RE.match(normalized)
    if decimal_match:
        precision = int(decimal_match.group(1))
        scale = int(decimal_match.group(2))
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


def _render_value(value: Any) -> str:
    """Render a schemapile sample value as a SQL literal.

    Strings are quoted and escaped. None becomes NULL. Bools, ints, and
    floats are quoted as strings so Delta auto-casts on insert; columns
    that fell back to STRING accept them verbatim, and typed columns
    coerce as needed.
    """
    if value is None:
        return "NULL"
    return f"'{_sql_escape(str(value))}'"


def _build_insert(
    fq_table: str,
    col_names: list[str],
    rows: list[tuple[Any, ...]],
) -> str:
    columns_clause = ", ".join(quote_identifier(c) for c in col_names)
    values_clauses = [
        "(" + ", ".join(_render_value(v) for v in row) + ")"
        for row in rows
    ]
    return (
        f"INSERT INTO {fq_table} ({columns_clause}) VALUES\n  "
        + ",\n  ".join(values_clauses)
    )


def _execute(ws: "WorkspaceClient", warehouse_id: str, statement: str) -> None:
    """Run one statement on the warehouse and raise on any non-success.

    Delegates to the shared client executor, so a FAILED or timed-out statement
    is surfaced as an error instead of being silently counted as a successful
    materialization.
    """
    succeeded, error = execute_ddl(ws, warehouse_id, statement)
    if not succeeded:
        raise RuntimeError(f"statement failed ({error}):\n{statement}")


if __name__ == "__main__":
    raise SystemExit(main())
