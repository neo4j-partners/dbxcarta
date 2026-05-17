"""Catalog-vs-graph contract checks: id normalization + complex-type round-trip.

Uses the Databricks SQL warehouse to sample from `information_schema.columns`,
then verifies the corresponding Neo4j nodes have the expected id and data_type.
"""

from __future__ import annotations

import random
from typing import TYPE_CHECKING, Any

from dbxcarta.spark.contract import generate_id
from dbxcarta.spark.verify import Violation

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient
    from neo4j import Driver


_COMPLEX_TYPE_FAMILIES = ("STRUCT", "ARRAY", "MAP", "VARIANT", "INTERVAL")


def check(
    driver: "Driver",
    summary: dict[str, Any],
    *,
    ws: "WorkspaceClient",
    warehouse_id: str,
    catalog: str,
) -> list[Violation]:
    if not warehouse_id:
        return [Violation(
            code="catalog.no_warehouse",
            message="DATABRICKS_WAREHOUSE_ID not set; catalog-vs-graph checks skipped.",
        )]
    out: list[Violation] = []
    out.extend(_check_id_normalization(driver, summary, ws=ws, warehouse_id=warehouse_id, catalog=catalog))
    out.extend(_check_complex_type_round_trip(driver, summary, ws=ws, warehouse_id=warehouse_id, catalog=catalog))
    return out


def _exec(ws: "WorkspaceClient", warehouse_id: str, statement: str) -> list[list[Any]]:
    """Execute a SQL statement; return data_array rows or [] if no result."""
    result = ws.statement_execution.execute_statement(
        warehouse_id=warehouse_id, statement=statement, wait_timeout="30s",
    )
    if result.result is None:
        return []
    return list(result.result.data_array or [])


def _check_id_normalization(
    driver: "Driver",
    summary: dict[str, Any],
    *,
    ws: "WorkspaceClient",
    warehouse_id: str,
    catalog: str,
) -> list[Violation]:
    """Sampled column IDs must exist in Neo4j; Python generate_id must equal the
    Spark SQL id_expr equivalent byte-for-byte."""
    out: list[Violation] = []
    schemas = summary.get("schemas") or []
    schema_filter = (
        f" AND table_schema IN ({', '.join(repr(s) for s in schemas)})" if schemas else ""
    )

    rows = _exec(ws, warehouse_id, (
        f"SELECT table_catalog, table_schema, table_name, column_name"
        f" FROM `{catalog}`.information_schema.columns"
        f" WHERE table_schema != 'information_schema'{schema_filter}"
        f" LIMIT 500"
    ))
    if rows:
        sample = random.sample(rows, min(50, len(rows)))
        missing: list[str] = []
        with driver.session() as s:
            for row in sample:
                table_catalog, table_schema, table_name, column_name = row
                expected_id = generate_id(table_catalog, table_schema, table_name, column_name)
                found = s.run("MATCH (n:Column {id: $id}) RETURN n.id", id=expected_id).single()
                if not found:
                    missing.append(expected_id)
        if missing:
            out.append(Violation(
                code="catalog.column_id_missing_in_neo4j",
                message=f"{len(missing)} column id(s) sampled from {catalog} not found in Neo4j.",
                details={"missing_count": len(missing), "examples": missing[:5]},
            ))

    sql_rows = _exec(ws, warehouse_id, (
        f"SELECT table_catalog, table_schema, table_name, column_name,"
        f" lower(translate(concat_ws('.', table_catalog, table_schema, table_name, column_name), ' -', '__')) AS sql_id"
        f" FROM `{catalog}`.information_schema.columns"
        f" LIMIT 100"
    ))
    mismatches: list[tuple[str, str]] = []
    for row in sql_rows:
        table_catalog, table_schema, table_name, column_name, sql_id = row
        py_id = generate_id(table_catalog, table_schema, table_name, column_name)
        if py_id != sql_id:
            mismatches.append((py_id, sql_id))
    if mismatches:
        out.append(Violation(
            code="catalog.python_spark_id_drift",
            message=f"Python generate_id() differs from Spark SQL id_expr() for {len(mismatches)} row(s).",
            details={"count": len(mismatches), "examples": mismatches[:3]},
        ))
    return out


def _check_complex_type_round_trip(
    driver: "Driver",
    summary: dict[str, Any],
    *,
    ws: "WorkspaceClient",
    warehouse_id: str,
    catalog: str,
) -> list[Violation]:
    """For each complex data_type family present in the run's scope, the
    corresponding Column node in Neo4j must store data_type verbatim. Families
    absent from the in-scope schemas skip. Sampling is constrained to
    `summary["schemas"]` so columns belonging to objects the run did not load
    (e.g. the run-summary Delta table itself, which lives outside the test
    fixture schemas) cannot surface as false-positive "missing node" violations.
    """
    out: list[Violation] = []
    schemas = summary.get("schemas") or []
    schema_filter = (
        f" AND table_schema IN ({', '.join(repr(s) for s in schemas)})" if schemas else ""
    )
    with driver.session() as s:
        for prefix in _COMPLEX_TYPE_FAMILIES:
            rows = _exec(ws, warehouse_id, (
                f"SELECT table_catalog, table_schema, table_name, column_name, data_type"
                f" FROM `{catalog}`.information_schema.columns"
                f" WHERE upper(data_type) LIKE '{prefix}%'{schema_filter}"
                f" LIMIT 1"
            ))
            if not rows:
                continue
            table_catalog, table_schema, table_name, column_name, data_type = rows[0]
            expected_id = generate_id(table_catalog, table_schema, table_name, column_name)
            node = s.run(
                "MATCH (n:Column {id: $id}) RETURN n.data_type AS dt", id=expected_id
            ).single()
            if node is None:
                out.append(Violation(
                    code=f"catalog.complex_type_column_missing.{prefix}",
                    message=f"Column node not found for {prefix} sample id={expected_id}.",
                    details={"id": expected_id, "family": prefix},
                ))
            elif node["dt"] != data_type:
                out.append(Violation(
                    code=f"catalog.complex_type_data_type_drift.{prefix}",
                    message=f"data_type mismatch for {prefix} sample id={expected_id}.",
                    details={"id": expected_id, "stored": node["dt"], "expected": data_type},
                ))
    return out
