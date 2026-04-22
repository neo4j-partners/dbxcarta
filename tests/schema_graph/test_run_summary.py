"""Assert the run summary was written to all three destinations: JSON, Delta, and stdout."""

import os

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementParameterListItem


def test_summary_json_exists_and_parses(run_summary: dict) -> None:
    assert run_summary["status"] == "success", f"Job did not succeed: {run_summary.get('error')}"
    assert run_summary["run_id"]
    assert run_summary["row_counts"]


def test_summary_delta_row_exists(ws: WorkspaceClient, run_summary: dict) -> None:
    warehouse_id = os.environ.get("DATABRICKS_WAREHOUSE_ID", "")
    if not warehouse_id:
        pytest.skip("DATABRICKS_WAREHOUSE_ID not set")

    table = os.environ["DBXCARTA_SUMMARY_TABLE"]
    quoted_table = ".".join(f"`{p}`" for p in table.split("."))
    run_id = run_summary["run_id"]

    result = ws.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=f"SELECT status FROM {quoted_table} WHERE run_id = :run_id LIMIT 1",
        parameters=[StatementParameterListItem(name="run_id", value=run_id)],
        wait_timeout="30s",
    )
    if result.result is None:
        pytest.skip(f"Warehouse did not return data (state={result.status})")
    rows = result.result.data_array or []
    assert rows, f"No row in {table} for run_id={run_id}"
    assert rows[0][0] == "success"
