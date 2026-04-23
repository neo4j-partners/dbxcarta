"""Warehouse SQL executor: run statements and report results or success."""

from __future__ import annotations

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import Disposition, Format, StatementState


def execute_sql(
    ws: WorkspaceClient,
    warehouse_id: str,
    sql: str,
    timeout_sec: int = 30,
) -> tuple[bool, bool, str | None]:
    """Execute *sql* against *warehouse_id*.

    Returns (executed, non_empty, error_message).
    - executed: True when the statement completed without a warehouse error.
    - non_empty: True when at least one row was returned.
    - error_message: set when executed is False.
    """
    response = ws.statement_execution.execute_statement(
        statement=sql,
        warehouse_id=warehouse_id,
        wait_timeout=f"{timeout_sec}s",
        disposition=Disposition.INLINE,
        format=Format.JSON_ARRAY,
    )

    state = response.status.state

    if state == StatementState.SUCCEEDED:
        data = (response.result and response.result.data_array) or []
        return True, len(data) > 0, None

    if state in (StatementState.FAILED, StatementState.CANCELED):
        msg = (
            response.status.error.message
            if response.status and response.status.error
            else str(state)
        )
        return False, False, msg

    # PENDING or RUNNING means the timeout elapsed before completion.
    return False, False, f"statement did not complete within {timeout_sec}s (state={state})"


def fetch_rows(
    ws: WorkspaceClient,
    warehouse_id: str,
    sql: str,
    timeout_sec: int = 30,
) -> tuple[list[str] | None, list[list] | None, str | None]:
    """Execute SQL and return (column_names, rows, error).

    Returns (None, None, error_message) on failure.
    Column names come from the statement manifest; rows are returned as-is from
    the JSON_ARRAY disposition.
    """
    response = ws.statement_execution.execute_statement(
        statement=sql,
        warehouse_id=warehouse_id,
        wait_timeout=f"{timeout_sec}s",
        disposition=Disposition.INLINE,
        format=Format.JSON_ARRAY,
    )

    state = response.status.state

    if state == StatementState.SUCCEEDED:
        columns: list[str] = []
        if (
            response.manifest
            and response.manifest.schema
            and response.manifest.schema.columns
        ):
            columns = [col.name for col in response.manifest.schema.columns]
        data = (response.result and response.result.data_array) or []
        return columns, data, None

    if state in (StatementState.FAILED, StatementState.CANCELED):
        msg = (
            response.status.error.message
            if response.status and response.status.error
            else str(state)
        )
        return None, None, msg

    return None, None, f"statement did not complete within {timeout_sec}s (state={state})"


def execute_ddl(
    ws: WorkspaceClient,
    warehouse_id: str,
    sql: str,
    timeout_sec: int = 60,
) -> tuple[bool, str | None]:
    """Execute a DDL statement (CREATE, ALTER, DROP, USE) and return success.

    Returns (succeeded, error_message). DDL produces no result set so
    Disposition and Format are omitted.
    """
    response = ws.statement_execution.execute_statement(
        statement=sql,
        warehouse_id=warehouse_id,
        wait_timeout=f"{timeout_sec}s",
    )
    state = response.status.state
    if state == StatementState.SUCCEEDED:
        return True, None
    if state in (StatementState.FAILED, StatementState.CANCELED):
        msg = (
            response.status.error.message
            if response.status and response.status.error
            else str(state)
        )
        return False, msg
    return False, f"statement did not complete within {timeout_sec}s (state={state})"


def split_sql_statements(sql: str) -> list[str]:
    """Split a SQL script on semicolons, discarding comment-only and empty segments.

    Inline SQL comments within statements are preserved — they are valid SQL
    that the warehouse engine handles correctly. Only segments whose entire
    content is comments or whitespace are discarded.

    Note: splits on literal semicolons only. Does not handle semicolons inside
    string literals or block comments. This is sufficient for DDL scripts where
    those constructs do not appear.
    """
    result = []
    for segment in sql.split(";"):
        stripped = segment.strip()
        has_sql = any(
            line and not line.startswith("--")
            for line in (l.strip() for l in stripped.splitlines())
        )
        if has_sql:
            result.append(stripped)
    return result


def preflight_warehouse(ws: WorkspaceClient, warehouse_id: str) -> None:
    """Raise RuntimeError if the warehouse is unreachable or returns an error."""
    executed, _, error = execute_sql(ws, warehouse_id, "SELECT 1", timeout_sec=30)
    if not executed:
        raise RuntimeError(f"Warehouse preflight failed: {error}")
