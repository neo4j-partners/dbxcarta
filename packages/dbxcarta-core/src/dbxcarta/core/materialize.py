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
import threading
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

# A factory that produces an :data:`ExecuteFn`. The spine calls it once for the
# main thread and once per pool worker, so a single executor instance is never
# shared across threads. A caller whose underlying resource is already
# thread-safe (the Databricks ``WorkspaceClient``) returns the same closure each
# call; a caller backed by a non-thread-safe resource (a DB-API connection or
# cursor) returns a fresh per-thread one. The concurrency contract is thus
# enforced by construction rather than assumed of the caller.
ExecuteFactory = Callable[[], ExecuteFn]

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
DEFAULT_DELTA_TYPE_MAP: Mapping[str, str] = MappingProxyType(
    {
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
)


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


def coerce_type(raw: str, type_map: Mapping[str, str] = DEFAULT_DELTA_TYPE_MAP) -> tuple[str, bool]:
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
    values_clauses = ["(" + ", ".join(render_sql_value(v) for v in row) + ")" for row in rows]
    return f"INSERT OVERWRITE TABLE {fq_table} ({columns_clause}) VALUES\n  " + ",\n  ".join(
        values_clauses
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
    # Disambiguation only, not security: same digest bytes, silences S324.
    digest = hashlib.sha1(name.encode("utf-8"), usedforsecurity=False).hexdigest()[:12]
    keep = max_len - len(prefix) - 1 - len(digest) - 1
    return f"{prefix}_{name[len(prefix) + 1 :][:keep]}_{digest}"


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
    make_execute: ExecuteFactory,
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
    (with its primary key folded inline into the CREATE and its sample rows
    inserted, in a first pass), then adds foreign-key constraints in a second
    pass after all tables exist. The caller is responsible for creating the
    catalog itself before calling this.

    Choices passed in by the example:

    - ``make_execute`` is an :data:`ExecuteFactory`. The schema-create and
      foreign-key passes run on the calling thread through one executor; in
      parallel mode each pool worker gets its own executor from the same
      factory, so an injected SQL runner is never shared across threads.
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
    # One executor for the calling thread: it runs the schema-create and the
    # foreign-key second pass, neither of which is parallelized. Workers below
    # get their own.
    main_execute = make_execute()

    for schema_entry in schemas:
        missing = [k for k in ("uc_schema", "source_id") if not schema_entry.get(k)]
        if missing:
            raise ValueError(
                f"blueprint schema entry missing required key(s) {missing}: {schema_entry!r}"
            )
        uc_schema = schema_entry["uc_schema"]
        source_id = schema_entry["source_id"]
        schema_q = quote_identifier(uc_schema)
        log.info("creating schema %s.%s", catalog, uc_schema)
        main_execute(
            f"CREATE SCHEMA IF NOT EXISTS {catalog_q}.{schema_q}"
            f" COMMENT '{property_prefix} source: {escape_sql_string(source_id)}'",
            f"CREATE SCHEMA {uc_schema}",
        )
        stats = stats + MaterializeStats(schemas_created=1)

        # Serial reuses the main executor; parallel hands each pool thread its
        # own via the factory so the injected runner is thread-confined.
        table_execute = _per_thread_executor(make_execute) if workers > 1 else main_execute
        tables = schema_entry.get("tables", [])
        materialized: dict[str, _MaterializedTable] = {}
        total = len(tables)
        thunks: list[_BuildFn] = [
            functools.partial(
                _materialize_table,
                table_execute,
                catalog_q,
                schema_q,
                source_id,
                table,
                property_prefix=property_prefix,
                type_map=type_map,
                on_insert_error=on_insert_error,
                on_table_error=on_table_error,
                log=log,
                progress=f"{idx}/{total}",
            )
            for idx, table in enumerate(tables, 1)
        ]

        # Serial and parallel share one collection loop: each entry is a
        # zero-arg callable returning (record, stats). Each build applies its
        # own table-create and row-insert error policy internally, so the loop
        # only sums tallies and records the materialized tables. A build that
        # re-raises (a ``"raise"`` policy, or any non-warehouse error such as a
        # bug) propagates out unchanged; in parallel mode the pool drains in the
        # list comprehension first, so the bound ``future.result`` returns or
        # re-raises instantly when called below.
        builds: list[_BuildFn]
        if workers > 1:
            with ThreadPoolExecutor(max_workers=workers) as pool:
                futures = [pool.submit(thunk) for thunk in thunks]
                builds = [future.result for future in as_completed(futures)]
        else:
            builds = thunks

        for build in builds:
            result, local = build()
            stats = stats + local
            if result is not None:
                materialized[result.safe_name] = result

        # Second pass: foreign keys, only after every table and its PK exists.
        stats = stats + _add_foreign_keys(main_execute, catalog_q, schema_q, materialized, log)

    return stats


def _per_thread_executor(make_execute: ExecuteFactory) -> ExecuteFn:
    """Wrap a factory so each calling thread lazily gets its own executor.

    The thread pool reuses its threads, so the factory is called at most once
    per worker thread (not once per table), and the returned executor is only
    ever touched by the thread that created it. This is the per-thread-resource
    pattern: it makes the "one executor per worker" contract true by
    construction instead of trusting the injected runner to be thread-safe.
    """
    local = threading.local()

    def execute(statement: str, label: str) -> None:
        runner: ExecuteFn | None = getattr(local, "runner", None)
        if runner is None:
            runner = local.runner = make_execute()
        runner(statement, label)

    return execute


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
    on_table_error: TableErrorMode,
    log: logging.Logger,
    progress: str = "",
) -> tuple[_MaterializedTable | None, MaterializeStats]:
    """Create one table (with its primary key folded into the CREATE) and insert its rows.

    The primary key is declared inline: its columns carry ``NOT NULL`` and the
    statement ends with an inline ``CONSTRAINT ... PRIMARY KEY (...)`` clause, so
    one ``CREATE`` replaces the former create / set-not-null / add-constraint
    sequence. A key whose columns did not all survive sanitization is dropped
    (no clause emitted) rather than partially declared. Foreign keys remain a
    second pass, since they reference tables that may not exist yet.

    Returns ``(record, stats)``; ``record`` is ``None`` when the table is
    skipped (unusable name, no columns, no coercible columns, or a tolerated
    create failure). ``stats`` is this table's own tally so a parallel caller
    can sum partial tallies without shared mutable state.

    ``on_table_error`` and ``on_insert_error`` are applied at the statements
    they govern (the ``CREATE TABLE`` and the row insert respectively), so the
    two policies are independent: a failed insert under ``on_insert_error=
    "raise"`` propagates regardless of ``on_table_error``, and vice versa.
    Only warehouse errors (:data:`_WAREHOUSE_ERRORS`) are subject to a ``"skip"``
    policy; any other exception always propagates so a real bug is never masked.
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

    typed_cols: list[tuple[str, str]] = []  # (safe column name, Delta type)
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
        typed_cols.append((safe_col, delta_type))
        safe_col_names.append(safe_col)
        keep_col_mask.append(True)

    if not typed_cols:
        stats.tables_skipped += 1
        log.warning("skipping table without coercible columns: %s", raw_name)
        return None, stats

    safe_col_set = frozenset(safe_col_names)
    # Primary key, folded into the CREATE: a valid key declares its columns NOT
    # NULL and an inline CONSTRAINT clause in the one statement, so UC's
    # NOT-NULL-before-PK requirement is satisfied without a follow-up ALTER. An
    # invalid key resolves to no columns, so no clause is emitted.
    pk_cols = _resolve_primary_key_columns(table.get("primary_keys") or [], safe_col_set)
    pk_col_set = frozenset(pk_cols)
    col_defs = [
        f"{quote_identifier(name)} {delta_type}" + (" NOT NULL" if name in pk_col_set else "")
        for name, delta_type in typed_cols
    ]
    if pk_cols:
        pk_name = constraint_name("pk", [safe_name])
        pk_cols_clause = ", ".join(quote_identifier(c) for c in pk_cols)
        col_defs.append(f"CONSTRAINT {quote_identifier(pk_name)} PRIMARY KEY ({pk_cols_clause})")

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
    try:
        execute(create_sql, f"CREATE TABLE {fq}")
    except _WAREHOUSE_ERRORS as exc:
        if on_table_error == "raise":
            raise
        log.warning("table create failed for %s: %s", fq, exc)
        return None, MaterializeStats()
    stats.tables_created += 1
    # The PK landed with the table, since it is part of the CREATE that just
    # succeeded, so it is counted here rather than after a separate ALTER.
    if pk_cols:
        stats.pk_constraints_added += 1

    raw_rows = table.get("rows") or []
    if raw_rows:
        kept_rows = [
            tuple(v for v, keep in zip(row, keep_col_mask, strict=False) if keep)
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

    return (
        _MaterializedTable(
            safe_name=safe_name,
            columns=safe_col_set,
            foreign_keys=list(table.get("foreign_keys") or []),
        ),
        stats,
    )


def _resolve_primary_key_columns(primary_keys: list[str], column_set: frozenset[str]) -> list[str]:
    """Return the sanitized PK columns to fold into the CREATE, or ``[]``.

    UC requires PK columns to be NOT NULL, which the inline CREATE declares
    alongside the constraint. If any PK column was dropped during sanitization
    or never landed as a real column, the whole key is dropped (an empty list,
    so no inline clause is emitted), since a partial PK would be wrong. Pure: it
    decides which columns the caller declares, and runs no SQL.
    """
    safe_pk_cols = [sanitize_identifier(c, prefix="c") for c in primary_keys]
    safe_pk_cols = [c for c in safe_pk_cols if c and c in column_set]
    if not safe_pk_cols or len(safe_pk_cols) != len(primary_keys):
        return []
    return safe_pk_cols


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
                sanitize_identifier(c, prefix="c") for c in (fk.get("referred_columns") or [])
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
                    child_fq,
                    parent_fq,
                    exc,
                )
    return stats
