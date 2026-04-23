"""DBxCarta demo setup script.

Sets up the test catalog schemas defined in tests/fixtures/setup_test_catalog.sql
against a Databricks SQL warehouse, then prints instructions for running the
full graph_rag demo using the pre-seeded question set.

Usage:
    python scripts/run_demo.py [--catalog CATALOG] [--volume-path VOL_PATH] [--teardown]

Required env vars:
    DATABRICKS_WAREHOUSE_ID    SQL warehouse to execute DDL against

Optional env vars (overridden by the corresponding CLI flags):
    DBXCARTA_CATALOG           Catalog to set up schemas in (default: main)
    DATABRICKS_VOLUME_PATH     UC Volume base path for external tables
                               (e.g. /Volumes/main/default/dbxcarta)
                               Required only for dbxcarta_test_external schema.

After setup, run the full demo via run_dbxcarta_client.py with:
    DBXCARTA_CATALOG=<catalog>
    DBXCARTA_SCHEMAS=dbxcarta_test_sales,dbxcarta_test_inventory,dbxcarta_test_hr,dbxcarta_test_events
    DBXCARTA_CLIENT_QUESTIONS=tests/fixtures/demo_questions.json
    DBXCARTA_CLIENT_ARMS=graph_rag
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

_SETUP_SQL = Path(__file__).parent.parent / "tests" / "fixtures" / "setup_test_catalog.sql"
_DEMO_SCHEMAS = [
    "dbxcarta_test_sales",
    "dbxcarta_test_inventory",
    "dbxcarta_test_hr",
    "dbxcarta_test_events",
]
# Teardown includes external schema and legacy backward-compat schemas for full cleanup.
_TEARDOWN_SCHEMAS = _DEMO_SCHEMAS + [
    "dbxcarta_test_external",
    "dbxcarta_fk_test",
    "dbxcarta_fk_test_b",
]
_DEMO_QUESTIONS_REL = "tests/fixtures/demo_questions.json"


def _setup(
    ws: "WorkspaceClient",
    warehouse_id: str,
    catalog: str,
    volume_path: str,
) -> None:
    from dbxcarta.client.executor import execute_ddl, split_sql_statements

    raw = _SETUP_SQL.read_text()
    sql = raw.replace("${catalog}", catalog).replace("${volume_path}", volume_path)
    statements = split_sql_statements(sql)
    total = len(statements)

    print(f"Setting up demo schemas in catalog '{catalog}' ({total} statements)...")

    for i, stmt in enumerate(statements, 1):
        preview = stmt[:70].replace("\n", " ")
        succeeded, error = execute_ddl(ws, warehouse_id, stmt, timeout_sec=60)
        if succeeded:
            print(f"  [{i}/{total}] OK  {preview}")
        else:
            print(f"  [{i}/{total}] ERR {preview}")
            print(f"  Error: {error}")
            print(
                f"\nSetup failed at statement {i}/{total}. "
                f"Subsequent FK constraints will cascade from this failure.\n"
                f"Fix the error and re-run, or tear down first for a clean start:\n"
                f"  python scripts/run_demo.py --catalog {catalog} --teardown"
            )
            sys.exit(1)

    print(f"\nDone. {total} statement(s) executed successfully.")


def _teardown(
    ws: "WorkspaceClient",
    warehouse_id: str,
    catalog: str,
) -> None:
    from dbxcarta.client.executor import execute_ddl

    statements = [
        f"DROP SCHEMA IF EXISTS {catalog}.{schema} CASCADE"
        for schema in _TEARDOWN_SCHEMAS
    ]
    total = len(statements)

    print(f"Tearing down demo schemas in catalog '{catalog}'...")

    warnings = []
    for i, stmt in enumerate(statements, 1):
        succeeded, error = execute_ddl(ws, warehouse_id, stmt, timeout_sec=60)
        if succeeded:
            print(f"  [{i}/{total}] OK  {stmt}")
        else:
            print(f"  [{i}/{total}] WARN {stmt}")
            print(f"         {error}")
            warnings.append(error)

    if warnings:
        print(f"\n{len(warnings)} drop(s) reported warnings (schemas may not have existed).")
    else:
        print(f"\nDone. {total} schema(s) removed.")


def _print_next_steps(catalog: str) -> None:
    schemas = ",".join(_DEMO_SCHEMAS)
    print("""
Next steps
----------
1. Run the dbxcarta pipeline job against the new schemas:

   DBXCARTA_CATALOG={catalog}
   DBXCARTA_SCHEMAS={schemas}

   (or leave DBXCARTA_SCHEMAS blank to include all schemas in the catalog)

2. After the pipeline completes, run the graph_rag demo:

   DBXCARTA_CATALOG={catalog}
   DBXCARTA_SCHEMAS={schemas}
   DBXCARTA_CLIENT_QUESTIONS={questions}
   DBXCARTA_CLIENT_ARMS=graph_rag

   python scripts/run_dbxcarta_client.py

   The question set exercises cross-schema joins (sales->inventory, sales->hr),
   self-referential FKs (hr.employees.manager_id), the composite FK path
   (product_suppliers), and intra-schema event analytics.

3. To tear down the schemas after the demo:

   python scripts/run_demo.py --catalog {catalog} --teardown
""".format(catalog=catalog, schemas=schemas, questions=_DEMO_QUESTIONS_REL))


def main() -> None:
    parser = argparse.ArgumentParser(description="DBxCarta demo schema setup")
    parser.add_argument(
        "--catalog",
        default=os.getenv("DBXCARTA_CATALOG", "main"),
        help="Target UC catalog (default: DBXCARTA_CATALOG env var, else 'main')",
    )
    parser.add_argument(
        "--volume-path",
        default=os.getenv("DATABRICKS_VOLUME_PATH", ""),
        help="UC Volume base path for external tables (e.g. /Volumes/cat/schema/vol)",
    )
    parser.add_argument(
        "--teardown",
        action="store_true",
        help="Drop all demo schemas instead of creating them",
    )
    args = parser.parse_args()

    warehouse_id = os.environ.get("DATABRICKS_WAREHOUSE_ID", "")
    if not warehouse_id:
        print("ERROR: DATABRICKS_WAREHOUSE_ID is not set.", file=sys.stderr)
        sys.exit(1)

    from databricks.sdk import WorkspaceClient
    from dbxcarta.client.executor import preflight_warehouse

    ws = WorkspaceClient()
    preflight_warehouse(ws, warehouse_id)

    if args.teardown:
        _teardown(ws, warehouse_id, args.catalog)
    else:
        _setup(ws, warehouse_id, args.catalog, args.volume_path)
        _print_next_steps(args.catalog)


if __name__ == "__main__":
    main()
