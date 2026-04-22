"""Assert generate_id() produces IDs that exist in Neo4j, catching normalization drift.

Samples 50 column nodes from the run summary catalog, reconstructs their expected
IDs using contract.generate_id, and verifies each node exists at that exact ID.
Also verifies the Python generate_id() output is byte-identical to the Spark SQL
id_expr() equivalent, so drift between the two implementations is caught early.
"""

import os
import random

from databricks.sdk import WorkspaceClient
from neo4j import Driver

from dbxcarta.contract import generate_id


def test_sampled_column_ids_exist(neo4j_driver: Driver, run_summary: dict, ws: WorkspaceClient) -> None:
    catalog = run_summary["catalog"]
    warehouse_id = os.environ["DATABRICKS_WAREHOUSE_ID"]

    result = ws.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=(
            f"SELECT table_catalog, table_schema, table_name, column_name"
            f" FROM {catalog}.information_schema.columns"
            f" LIMIT 500"
        ),
        wait_timeout="30s",
    )
    rows = result.result.data_array or []
    sample = random.sample(rows, min(50, len(rows)))

    missing = []
    with neo4j_driver.session() as s:
        for row in sample:
            table_catalog, table_schema, table_name, column_name = row
            expected_id = generate_id(table_catalog, table_schema, table_name, column_name)
            found = s.run(
                "MATCH (n:Column {id: $id}) RETURN n.id", id=expected_id
            ).single()
            if not found:
                missing.append(expected_id)

    assert not missing, f"{len(missing)} column IDs not found in Neo4j: {missing[:5]}"


def test_python_id_matches_spark_sql_id(run_summary: dict, ws: WorkspaceClient) -> None:
    """Python generate_id() must produce byte-identical output to the Spark SQL id_expr()."""
    catalog = run_summary["catalog"]
    warehouse_id = os.environ["DATABRICKS_WAREHOUSE_ID"]

    result = ws.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=(
            f"SELECT table_catalog, table_schema, table_name, column_name,"
            f" lower(translate(concat_ws('.', table_catalog, table_schema, table_name, column_name), ' -', '__')) AS sql_id"
            f" FROM {catalog}.information_schema.columns"
            f" LIMIT 100"
        ),
        wait_timeout="30s",
    )
    rows = result.result.data_array or []

    mismatches = []
    for row in rows:
        table_catalog, table_schema, table_name, column_name, sql_id = row
        py_id = generate_id(table_catalog, table_schema, table_name, column_name)
        if py_id != sql_id:
            mismatches.append((py_id, sql_id))

    assert not mismatches, f"Python/Spark ID mismatch in {len(mismatches)} rows: {mismatches[:3]}"
