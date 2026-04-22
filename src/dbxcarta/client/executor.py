"""Warehouse SQL executor: run a statement and report whether it returned rows."""

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


def preflight_warehouse(ws: WorkspaceClient, warehouse_id: str) -> None:
    """Raise RuntimeError if the warehouse is unreachable or returns an error."""
    executed, _, error = execute_sql(ws, warehouse_id, "SELECT 1", timeout_sec=30)
    if not executed:
        raise RuntimeError(f"Warehouse preflight failed: {error}")
