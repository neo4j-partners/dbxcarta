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
    timeout_sec: int = 50,
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

    Tracks -- line-comment state and single-quoted string literals so semicolons
    inside comments or strings are not treated as statement boundaries. Does not
    handle block comments (/* */); this is sufficient for DDL scripts.
    """
    result = []
    current: list[str] = []
    in_line_comment = False
    in_string = False

    i = 0
    n = len(sql)
    while i < n:
        ch = sql[i]
        if in_string:
            current.append(ch)
            if ch == "'":
                if i + 1 < n and sql[i + 1] == "'":
                    # SQL escaped quote: '' — consume both chars
                    i += 1
                    current.append(sql[i])
                else:
                    in_string = False
        elif in_line_comment:
            current.append(ch)
            if ch == "\n":
                in_line_comment = False
        elif ch == "'":
            in_string = True
            current.append(ch)
        elif ch == "-" and i + 1 < n and sql[i + 1] == "-":
            in_line_comment = True
            current.append(ch)
        elif ch == ";":
            segment = "".join(current).strip()
            has_sql = any(
                line and not line.startswith("--")
                for line in (l.strip() for l in segment.splitlines())
            )
            if has_sql:
                result.append(segment)
            current = []
        else:
            current.append(ch)
        i += 1

    # trailing segment with no final semicolon
    segment = "".join(current).strip()
    has_sql = any(
        line and not line.startswith("--")
        for line in (l.strip() for l in segment.splitlines())
    )
    if has_sql:
        result.append(segment)

    return result


def preflight_warehouse(ws: WorkspaceClient, warehouse_id: str) -> None:
    """Raise RuntimeError if the warehouse is unreachable or returns an error."""
    executed, _, error = execute_sql(ws, warehouse_id, "SELECT 1", timeout_sec=30)
    if not executed:
        raise RuntimeError(f"Warehouse preflight failed: {error}")
