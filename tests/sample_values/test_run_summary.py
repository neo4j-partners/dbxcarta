"""Assert the sample_values run summary was written to JSON and Delta."""

import os

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementParameterListItem


def test_summary_json_exists_and_parses(run_summary: dict) -> None:
    assert run_summary["status"] == "success", f"Job did not succeed: {run_summary.get('error')}"
    assert run_summary["run_id"]
    assert run_summary["job_name"] == "sample_values"
    assert run_summary["row_counts"]


def test_summary_delta_row_exists(ws: WorkspaceClient, run_summary: dict) -> None:
    warehouse_id = os.environ.get("DATABRICKS_WAREHOUSE_ID", "")
    if not warehouse_id:
        pytest.skip("DATABRICKS_WAREHOUSE_ID not set")

    table = os.environ["DBXCARTA_SUMMARY_TABLE"]
    run_id = run_summary["run_id"]

    result = ws.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=(
            f"SELECT status, job_name FROM {table} "
            f"WHERE run_id = :run_id AND job_name = 'sample_values' LIMIT 1"
        ),
        parameters=[StatementParameterListItem(name="run_id", value=run_id)],
        wait_timeout="30s",
    )
    if result.status is None or result.result is None:
        pytest.skip(f"Warehouse did not return data (status={result.status})")
    rows = result.result.data_array or []
    assert rows, f"No row in {table} for run_id={run_id}, job_name=sample_values"
    assert rows[0][0] == "success"
    assert rows[0][1] == "sample_values"
