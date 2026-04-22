"""Assert that resampled columns drop stale Values when top-K drifts.

Marked slow — provisions a temporary table in the target catalog,
submits Phase 1 (to register its Column nodes) and Phase 2 (twice,
across a data swap), and asserts the second run replaces the prior
Values rather than accumulating them.

Run with: pytest tests/sample_values -m slow -k drift
"""

from __future__ import annotations

import base64
import os
import time
from pathlib import Path

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementParameterListItem
from neo4j import Driver

_DRIFT_TABLE = "_dbxcarta_drift_test"
_SEED_VALUES_A = ["alpha", "alpha", "alpha", "beta", "beta", "gamma"]
_SEED_VALUES_B = ["delta", "delta", "delta", "epsilon", "epsilon", "zeta"]


def _exec_sql(ws: WorkspaceClient, sql: str) -> None:
    warehouse_id = os.environ["DATABRICKS_WAREHOUSE_ID"]
    result = ws.statement_execution.execute_statement(
        warehouse_id=warehouse_id, statement=sql, wait_timeout="50s",
    )
    state = result.status.state.value if result.status and result.status.state else "UNKNOWN"
    if state != "SUCCEEDED":
        err = result.status.error.message if result.status and result.status.error else state
        raise RuntimeError(f"SQL failed ({state}): {err}\nSQL: {sql}")


def _column_values(session, col_id: str) -> set[str]:
    rows = session.run(
        "MATCH (c:Column {id: $cid})-[:HAS_VALUE]->(v:Value) RETURN v.value AS val",
        cid=col_id,
    )
    return {r["val"] for r in rows}


def _column_id_for(catalog: str, schema: str, table: str, column: str) -> str:
    from dbxcarta.contract import generate_id
    return generate_id(catalog, schema, table, column)


@pytest.mark.slow
def test_value_drift_replaces_stale_values(
    ws: WorkspaceClient, neo4j_driver: Driver,
) -> None:
    from dbxcarta.cli import runner

    catalog = os.environ["DBXCARTA_CATALOG"]
    # Pick the schema from the configured volume path.
    summary_volume = os.environ["DBXCARTA_SUMMARY_VOLUME"]
    schema = summary_volume.lstrip("/").split("/")[2]
    fq = f"`{catalog}`.`{schema}`.`{_DRIFT_TABLE}`"
    col_id = _column_id_for(catalog, schema, _DRIFT_TABLE, "label")

    def _seed(values: list[str]) -> None:
        rows = ", ".join(f"('{v}')" for v in values)
        _exec_sql(ws, f"CREATE OR REPLACE TABLE {fq} (label STRING) USING DELTA")
        _exec_sql(ws, f"INSERT INTO {fq} VALUES {rows}")

    try:
        _seed(_SEED_VALUES_A)

        os.environ["DBXCARTA_JOB"] = "schema"
        runner.submit("run_dbxcarta.py")
        os.environ["DBXCARTA_JOB"] = "sample"
        runner.submit("run_dbxcarta.py")

        with neo4j_driver.session() as s:
            after_a = _column_values(s, col_id)
        assert after_a == set(_SEED_VALUES_A), (
            f"Run 1 missing seeded values. expected={set(_SEED_VALUES_A)} got={after_a}"
        )

        _seed(_SEED_VALUES_B)
        runner.submit("run_dbxcarta.py")

        with neo4j_driver.session() as s:
            after_b = _column_values(s, col_id)
        assert after_b == set(_SEED_VALUES_B), (
            f"Run 2 did not replace stale Values. "
            f"expected={set(_SEED_VALUES_B)} got={after_b} "
            f"(stale leftovers: {after_b & set(_SEED_VALUES_A)})"
        )
    finally:
        try:
            _exec_sql(ws, f"DROP TABLE IF EXISTS {fq}")
        except Exception:
            pass
