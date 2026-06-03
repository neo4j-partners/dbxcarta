"""Shared, example-agnostic plumbing for turning a table spec into real tables.

Both the dense-schema and schemapile examples carried near-identical copies of
the create-schema, create-table, insert-rows, add-constraints sequence and its
supporting helpers. This module is the single home for that plumbing. Example
choices (the source type map, the table-property prefix, table-build
concurrency, and row-insert/table-create error handling) are passed in as
arguments rather than hardcoded, so one copy serves every example.

The module is deliberately free of any ``WorkspaceClient`` dependency: the
spine runs SQL through an injected ``execute`` callable that raises on failure.
That keeps core light and the spine unit-testable, and lets each example bind
its own warehouse executor.
"""

from __future__ import annotations

import functools
import hashlib
import json
import logging
import re
from collections.abc import Callable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Literal

from dbxcarta.core.identifiers import quote_identifier


logger = logging.getLogger(__name__)

# An injected SQL runner: ``execute(statement, label)`` runs one statement and
# raises on any non-success. The label is for logging only.
ExecuteFn = Callable[[str, str], None]

InsertErrorMode = Literal["raise", "skip"]
TableErrorMode = Literal["raise", "skip"]

# The failure types an injected ``execute`` raises for a warehouse-side error:
# ``RuntimeError`` for a FAILED/CANCELED statement, ``TimeoutError`` for one
# that never reached a terminal state. The skip-on-error paths catch exactly
# these so a genuine warehouse failure is tolerated while a programming error
# (a malformed spec, a bug) still propagates instead of masquerading as a
# skipped table.
_WAREHOUSE_ERRORS = (RuntimeError, TimeoutError)

_CONSTRAINT_NAME_LIMIT = 255

_DECIMAL_RE = re.compile(r"^(?:DECIMAL|NUMERIC|NUMBER|DEC)\s*\((\d+)\s*,\s*(\d+)\)$")
_VARCHAR_RE = re.compile(r"^(?:VARCHAR|CHAR|CHARACTER|NVARCHAR|NCHAR)\s*\(\s*\d+\s*\)$")
_NAME_SANITIZE_RE = re.compile(r"[^a-zA-Z0-9_]+")

# The one shared source-type -> Delta-type map. Both examples carried this map
# with identical entries, so core owns it as the default. An example overrides
# it only if its source ever needs different entries; this keeps a single map
# today without hardcoding that the two sources must always agree.
DEFAULT_DELTA_TYPE_MAP: Mapping[str, str] = MappingProxyType({
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
})


@dataclass
class MaterializeStats:
    """Tally of one materialize run.

    Holds counts only. Parallelism is the caller's concern: a parallel
    materialize has each worker return its own ``MaterializeStats`` and the
    spine sums them with :meth:`__add__`, so the shape carries no shared
    mutable state.
    """

    schemas_created: int = 0
    tables_created: int = 0
    rows_inserted: int = 0
    tables_skipped: int = 0
    type_fallbacks: int = 0
    pk_constraints_added: int = 0
    fk_constraints_added: int = 0

    def __add__(self, other: MaterializeStats) -> MaterializeStats:
        if not isinstance(other, MaterializeStats):
            return NotImplemented
        return MaterializeStats(
            schemas_created=self.schemas_created + other.schemas_created,
            tables_created=self.tables_created + other.tables_created,
            rows_inserted=self.rows_inserted + other.rows_inserted,
            tables_skipped=self.tables_skipped + other.tables_skipped,
            type_fallbacks=self.type_fallbacks + other.type_fallbacks,
            pk_constraints_added=self.pk_constraints_added + other.pk_constraints_added,
            fk_constraints_added=self.fk_constraints_added + other.fk_constraints_added,
        )


def coerce_type(
    raw: str, type_map: Mapping[str, str] = DEFAULT_DELTA_TYPE_MAP
) -> tuple[str, bool]:
    """Map a source column type to a Delta type.

    Returns ``(delta_type, fellback)`` where ``fellback`` is True when the type
    could not be matched and defaulted to ``STRING``. ``DECIMAL(p, s)`` is
    clamped to Delta's bounds; sized ``VARCHAR``/``CHAR`` becomes ``STRING``.
    """
    if not raw:
        return "STRING", True
    normalized = raw.upper().strip().strip(";").strip()
    decimal_match = _DECIMAL_RE.match(normalized)
    if decimal_match:
        precision = max(1, min(int(decimal_match.group(1)), 38))
        scale = max(0, min(int(decimal_match.group(2)), precision))
        return f"DECIMAL({precision},{scale})", False
    if _VARCHAR_RE.match(normalized):
        return "STRING", False
    if normalized in type_map:
        return type_map[normalized], False
    base = normalized.split("(", 1)[0].strip()
    if base in type_map:
        return type_map[base], False
    return "STRING", True


def sanitize_identifier(name: str, *, prefix: str = "t") -> str:
    """Clean a name into a legal identifier.

    Non-identifier runs collapse to ``_``; the result is stripped of leading
    and trailing underscores and lowercased. A cleaned name that starts with a
    digit is prefixed with ``f"{prefix}_"`` so it is a legal identifier. Returns
    ``""`` when nothing usable remains. Callers pass ``prefix="t"`` for tables
    and ``prefix="c"`` for columns.
    """
    cleaned = _NAME_SANITIZE_RE.sub("_", name).strip("_").lower()
    if not cleaned:
        return ""
    if cleaned[0].isdigit():
        cleaned = f"{prefix}_{cleaned}"
    return cleaned


def escape_sql_string(value: str) -> str:
    """Escape backslash and single-quote for use inside a SQL string literal."""
    return value.replace("\\", "\\\\").replace("'", "''")


def render_sql_value(value: Any) -> str:
    """Render a value into its statement form.

    ``None`` becomes ``NULL``; everything else becomes a quoted, escaped string
    and relies on Delta casting the literal to the column type on insert.
    """
    if value is None:
        return "NULL"
    return f"'{escape_sql_string(str(value))}'"


def build_insert_statement(
    fq_table: str,
    col_names: Sequence[str],
    rows: Sequence[Sequence[Any]],
) -> str:
    """Build one ``INSERT OVERWRITE TABLE`` statement for the given rows.

    ``INSERT OVERWRITE`` is atomic, idempotent on re-run, and a single round
    trip, so it replaces a freshly created table's contents in one statement.
    """
    columns_clause = ", ".join(quote_identifier(c) for c in col_names)
    values_clauses = [
        "(" + ", ".join(render_sql_value(v) for v in row) + ")" for row in rows
    ]
    return (
        f"INSERT OVERWRITE TABLE {fq_table} ({columns_clause}) VALUES\n  "
        + ",\n  ".join(values_clauses)
    )


def constraint_name(
    prefix: str, parts: Sequence[str], *, max_len: int = _CONSTRAINT_NAME_LIMIT
) -> str:
    """Build a deterministic, length-guarded constraint name.

    Joins ``prefix`` with the (already-sanitized) ``parts``. When the result
    exceeds ``max_len`` it keeps the readable prefix outside the truncated
    window and appends a stable 12-character SHA-1 suffix so distinct inputs
    stay distinct.
    """
    body = "__".join(p for p in parts if p)
    name = f"{prefix}_{body}" if body else prefix
    if len(name) <= max_len:
        return name
    digest = hashlib.sha1(name.encode("utf-8")).hexdigest()[:12]
    keep = max_len - len(prefix) - 1 - len(digest) - 1
    return f"{prefix}_{name[len(prefix) + 1:][:keep]}_{digest}"


@dataclass
class _MaterializedTable:
    """A table that was actually created, with the identifiers the FK pass needs.

    ``columns`` is the set of sanitized column names that landed in the table;
    ``foreign_keys`` is the source spec's raw FK list, deferred to the second
    pass.
    """

    safe_name: str
    columns: frozenset[str]
    foreign_keys: list[dict[str, Any]]


# A zero-arg callable that builds one table and returns its record and tally.
# Both a ``functools.partial`` (serial) and a future's ``result`` (parallel)
# satisfy it, so one collection loop handles both modes.
_BuildFn = Callable[[], tuple["_MaterializedTable | None", MaterializeStats]]


def materialize_schemas(
    schemas: list[dict[str, Any]],
    *,
    catalog: str,
    execute: ExecuteFn,
    property_prefix: str,
    type_map: Mapping[str, str] = DEFAULT_DELTA_TYPE_MAP,
    workers: int = 1,
    on_insert_error: InsertErrorMode = "raise",
    on_table_error: TableErrorMode = "raise",
    log: logging.Logger | None = None,
) -> MaterializeStats:
    """Materialize candidate schemas as Delta tables and return the tally.

    For each entry in ``schemas`` (each a dict with ``uc_schema``,
    ``source_id``, and ``tables``), creates the schema, creates every table
    (inserting sample rows and adding the primary key in a first pass), then
    adds foreign-key constraints in a second pass after all tables exist. The
    caller is responsible for creating the catalog itself before calling this.

    Choices passed in by the example:

    - ``property_prefix`` names the ``TBLPROPERTIES`` keys (``<prefix>.source_id``)
      and the schema comment.
    - ``type_map`` is the source-to-Delta type map.
    - ``workers`` selects table-build concurrency: ``1`` walks tables serially,
      ``>1`` builds them with a thread pool. Foreign keys are always a second
      pass, which the parallel path requires.
    - ``on_insert_error`` and ``on_table_error`` choose ``"raise"`` (abort the
      run) or ``"skip"`` (log and continue) for a failed row insert or a failed
      table create, respectively.
    """
    log = log or logger
    stats = MaterializeStats()
    catalog_q = quote_identifier(catalog)

    for schema_entry in schemas:
        uc_schema = schema_entry["uc_schema"]
        source_id = schema_entry["source_id"]
        schema_q = quote_identifier(uc_schema)
        log.info("creating schema %s.%s", catalog, uc_schema)
        execute(
            f"CREATE SCHEMA IF NOT EXISTS {catalog_q}.{schema_q}"
            f" COMMENT '{property_prefix} source: {escape_sql_string(source_id)}'",
            f"CREATE SCHEMA {uc_schema}",
        )
        stats.schemas_created += 1

        tables = schema_entry.get("tables", [])
        materialized: dict[str, _MaterializedTable] = {}
        total = len(tables)
        thunks: list[_BuildFn] = [
            functools.partial(
                _materialize_table,
                execute, catalog_q, schema_q, source_id, table,
                property_prefix=property_prefix, type_map=type_map,
                on_insert_error=on_insert_error, log=log, progress=f"{idx}/{total}",
            )
            for idx, table in enumerate(tables, 1)
        ]

        # Serial and parallel share one collection loop: each entry is a
        # zero-arg callable returning (record, stats). In parallel mode the
        # thread pool drains first (in completion order); the bound
        # ``future.result`` then returns instantly when collected below.
        builds: list[_BuildFn]
        if workers > 1:
            with ThreadPoolExecutor(max_workers=workers) as pool:
                futures = [pool.submit(thunk) for thunk in thunks]
                builds = [future.result for future in as_completed(futures)]
        else:
            builds = thunks

        for build in builds:
            result, local = _collect(build, on_table_error, log)
            stats = stats + local
            if result is not None:
                materialized[result.safe_name] = result

        # Second pass: foreign keys, only after every table and its PK exists.
        stats = stats + _add_foreign_keys(execute, catalog_q, schema_q, materialized, log)

    return stats


def _collect(
    produce: _BuildFn,
    on_table_error: TableErrorMode,
    log: logging.Logger,
) -> tuple[_MaterializedTable | None, MaterializeStats]:
    """Run one table build, honouring ``on_table_error`` on a create failure.

    Returns the per-table result and tally. On ``"skip"`` a failed build is
    logged and reported as an empty result with an empty tally so the rest of
    the catalog still materializes; on ``"raise"`` the error propagates.
    """
    try:
        return produce()
    except _WAREHOUSE_ERRORS as exc:
        if on_table_error == "raise":
            raise
        log.error("table failed: %s", exc)
        return None, MaterializeStats()


def _materialize_table(
    execute: ExecuteFn,
    catalog_q: str,
    schema_q: str,
    source_id: str,
    table: dict[str, Any],
    *,
    property_prefix: str,
    type_map: Mapping[str, str],
    on_insert_error: InsertErrorMode,
    log: logging.Logger,
    progress: str = "",
) -> tuple[_MaterializedTable | None, MaterializeStats]:
    """Create one table, insert its rows, and add its primary key.

    Returns ``(record, stats)``; ``record`` is ``None`` when the table is
    skipped (unusable name, no columns, or no coercible columns). ``stats`` is
    this table's own tally so a parallel caller can sum partial tallies without
    shared mutable state.
    """
    stats = MaterializeStats()
    raw_name = table.get("name", "")
    safe_name = sanitize_identifier(raw_name, prefix="t")
    if not safe_name:
        stats.tables_skipped += 1
        log.warning("skipping table with unusable name: %r", raw_name)
        return None, stats
    table_q = quote_identifier(safe_name)
    fq = f"{catalog_q}.{schema_q}.{table_q}"
    log.info("table %s %s", progress, fq)

    columns = table.get("columns") or []
    if not columns:
        stats.tables_skipped += 1
        log.warning("skipping table without columns: %s", raw_name)
        return None, stats

    col_defs: list[str] = []
    safe_col_names: list[str] = []
    keep_col_mask: list[bool] = []
    for col in columns:
        safe_col = sanitize_identifier(col.get("name", ""), prefix="c")
        if not safe_col:
            keep_col_mask.append(False)
            continue
        raw_type = str(col.get("type", "")).strip()
        delta_type, fellback = coerce_type(raw_type, type_map)
        if fellback:
            stats.type_fallbacks += 1
        col_defs.append(f"{quote_identifier(safe_col)} {delta_type}")
        safe_col_names.append(safe_col)
        keep_col_mask.append(True)

    if not col_defs:
        stats.tables_skipped += 1
        log.warning("skipping table without coercible columns: %s", raw_name)
        return None, stats

    fks_json = json.dumps(table.get("foreign_keys") or [])
    pks_json = json.dumps(table.get("primary_keys") or [])
    tbl_properties = (
        f"'{property_prefix}.source_id' = '{escape_sql_string(source_id)}',"
        f"'{property_prefix}.original_name' = '{escape_sql_string(raw_name)}',"
        f"'{property_prefix}.primary_keys' = '{escape_sql_string(pks_json)}',"
        f"'{property_prefix}.foreign_keys' = '{escape_sql_string(fks_json)}'"
    )
    create_sql = (
        f"CREATE TABLE IF NOT EXISTS {fq} (\n  "
        + ",\n  ".join(col_defs)
        + f"\n) USING DELTA TBLPROPERTIES ({tbl_properties})"
    )
    execute(create_sql, f"CREATE TABLE {fq}")
    stats.tables_created += 1

    raw_rows = table.get("rows") or []
    if raw_rows:
        kept_rows = [
            tuple(v for v, keep in zip(row, keep_col_mask) if keep)
            for row in raw_rows
        ]
        kept_rows = [r for r in kept_rows if len(r) == len(safe_col_names)]
        if kept_rows:
            insert_sql = build_insert_statement(fq, safe_col_names, kept_rows)
            try:
                execute(insert_sql, f"INSERT OVERWRITE {fq}")
                stats.rows_inserted += len(kept_rows)
            except _WAREHOUSE_ERRORS as exc:
                if on_insert_error == "raise":
                    raise
                log.warning("insert failed for %s: %s", fq, exc)

    safe_col_set = frozenset(safe_col_names)
    _add_primary_key(
        execute, fq, safe_name, table.get("primary_keys") or [],
        safe_col_set, stats, log,
    )
    return (
        _MaterializedTable(
            safe_name=safe_name,
            columns=safe_col_set,
            foreign_keys=list(table.get("foreign_keys") or []),
        ),
        stats,
    )


def _add_primary_key(
    execute: ExecuteFn,
    fq: str,
    safe_name: str,
    primary_keys: list[str],
    column_set: frozenset[str],
    stats: MaterializeStats,
    log: logging.Logger,
) -> None:
    """Add a PRIMARY KEY constraint, setting its columns NOT NULL first.

    UC requires PK columns to be NOT NULL. If any PK column was dropped during
    sanitization or never landed as a real column, the whole constraint is
    skipped, since a partial PK would be wrong. Each ALTER is tolerant: a
    failure logs a warning and continues so one bad table does not abort the run.
    """
    safe_pk_cols = [sanitize_identifier(c, prefix="c") for c in primary_keys]
    safe_pk_cols = [c for c in safe_pk_cols if c and c in column_set]
    if len(safe_pk_cols) != len(primary_keys) or not safe_pk_cols:
        return

    for col in safe_pk_cols:
        try:
            execute(
                f"ALTER TABLE {fq} ALTER COLUMN {quote_identifier(col)} SET NOT NULL",
                f"SET NOT NULL {fq}.{col}",
            )
        except _WAREHOUSE_ERRORS as exc:
            log.warning("set not null failed for %s.%s: %s", fq, col, exc)
            return

    pk_name = constraint_name("pk", [safe_name])
    cols_clause = ", ".join(quote_identifier(c) for c in safe_pk_cols)
    try:
        execute(
            f"ALTER TABLE {fq} ADD CONSTRAINT {quote_identifier(pk_name)}"
            f" PRIMARY KEY ({cols_clause})",
            f"ADD PRIMARY KEY {fq}",
        )
        stats.pk_constraints_added += 1
    except _WAREHOUSE_ERRORS as exc:
        log.warning("add primary key failed for %s: %s", fq, exc)


def _add_foreign_keys(
    execute: ExecuteFn,
    catalog_q: str,
    schema_q: str,
    materialized: dict[str, _MaterializedTable],
    log: logging.Logger,
) -> MaterializeStats:
    """Second pass: add FK constraints after all tables and PKs exist.

    Skips any FK whose child or parent columns were dropped during sanitization,
    whose arity does not match, or whose target table/columns do not exist among
    the materialized tables. Each ALTER is tolerant of failure.
    """
    stats = MaterializeStats()
    for child in materialized.values():
        child_fq = f"{catalog_q}.{schema_q}.{quote_identifier(child.safe_name)}"
        for fk in child.foreign_keys:
            src_cols = [sanitize_identifier(c, prefix="c") for c in (fk.get("columns") or [])]
            ref_cols = [
                sanitize_identifier(c, prefix="c")
                for c in (fk.get("referred_columns") or [])
            ]
            parent_name = sanitize_identifier(fk.get("foreign_table", ""), prefix="t")

            if not parent_name or parent_name not in materialized:
                continue
            if not src_cols or len(src_cols) != len(ref_cols):
                continue
            if any(c not in child.columns for c in src_cols):
                continue
            parent = materialized[parent_name]
            if any(c not in parent.columns for c in ref_cols):
                continue

            fk_name = constraint_name("fk", [child.safe_name, *src_cols])
            parent_fq = f"{catalog_q}.{schema_q}.{quote_identifier(parent_name)}"
            src_clause = ", ".join(quote_identifier(c) for c in src_cols)
            ref_clause = ", ".join(quote_identifier(c) for c in ref_cols)
            try:
                execute(
                    f"ALTER TABLE {child_fq} ADD CONSTRAINT {quote_identifier(fk_name)}"
                    f" FOREIGN KEY ({src_clause})"
                    f" REFERENCES {parent_fq} ({ref_clause})",
                    f"ADD FOREIGN KEY {child_fq} -> {parent_fq}",
                )
                stats.fk_constraints_added += 1
            except _WAREHOUSE_ERRORS as exc:
                log.warning(
                    "add foreign key failed for %s -> %s: %s",
                    child_fq, parent_fq, exc,
                )
    return stats
