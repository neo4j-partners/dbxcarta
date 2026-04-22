"""Assert compound column types (STRUCT, ARRAY, MAP, VARIANT, INTERVAL) round-trip correctly.

For each non-primitive data_type family present in the catalog, asserts the Column
node exists and data_type is stored verbatim. Skips families absent from the catalog.
"""

import os

import pytest
from databricks.sdk import WorkspaceClient
from neo4j import Driver

from dbxcarta.contract import generate_id

_FAMILIES = ("STRUCT", "ARRAY", "MAP", "VARIANT", "INTERVAL")


def _sample_column_by_type_prefix(ws: WorkspaceClient, catalog: str, warehouse_id: str, prefix: str):
    result = ws.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=(
            f"SELECT table_catalog, table_schema, table_name, column_name, data_type"
            f" FROM `{catalog}`.information_schema.columns"
            f" WHERE upper(data_type) LIKE '{prefix}%'"
            f" LIMIT 1"
        ),
        wait_timeout="30s",
    )
    if result.result is None:
        pytest.skip(f"Warehouse did not return data (state={result.status})")
    rows = result.result.data_array or []
    return rows[0] if rows else None


@pytest.mark.parametrize("type_prefix", _FAMILIES)
def test_complex_type_round_trips(
    type_prefix: str,
    neo4j_driver: Driver,
    run_summary: dict,
    ws: WorkspaceClient,
) -> None:
    warehouse_id = os.environ.get("DATABRICKS_WAREHOUSE_ID", "")
    if not warehouse_id:
        pytest.skip("DATABRICKS_WAREHOUSE_ID not set")

    catalog = run_summary["catalog"]
    row = _sample_column_by_type_prefix(ws, catalog, warehouse_id, type_prefix)
    if not row:
        pytest.skip(f"No {type_prefix} columns in {catalog}")

    table_catalog, table_schema, table_name, column_name, data_type = row
    expected_id = generate_id(table_catalog, table_schema, table_name, column_name)

    with neo4j_driver.session() as s:
        node = s.run(
            "MATCH (n:Column {id: $id}) RETURN n.data_type AS dt", id=expected_id
        ).single()

    assert node is not None, f"Column node not found for id={expected_id}"
    assert node["dt"] == data_type, (
        f"data_type mismatch for {expected_id}: stored={node['dt']!r}, expected={data_type!r}"
    )
