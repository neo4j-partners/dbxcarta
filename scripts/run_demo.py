"""DBxCarta demo setup script.

Sets up the test catalog schemas defined in tests/fixtures/setup_test_catalog.sql
against a Databricks SQL warehouse, then prints instructions for running the
full graph_rag demo using the pre-seeded question set.

Usage:
    python scripts/run_demo.py [--catalog CATALOG] [--volume-path VOL_PATH] [--teardown]

The script reads credentials from the environment or a .env file (same as the
pipeline scripts). At minimum, DATABRICKS_WAREHOUSE_ID must be set.

Required env vars:
    DATABRICKS_WAREHOUSE_ID   SQL warehouse to execute DDL against

Optional env vars:
    DBXCARTA_DEMO_CATALOG     Catalog to set up schemas in (default: main)
    DBXCARTA_DEMO_VOLUME      UC Volume base path for external tables
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
# Includes external schema and legacy backward-compat schemas for full cleanup.
_TEARDOWN_SCHEMAS = _DEMO_SCHEMAS + [
    "dbxcarta_test_external",
    "dbxcarta_fk_test",
    "dbxcarta_fk_test_b",
]
_DEMO_QUESTIONS_REL = "tests/fixtures/demo_questions.json"


def _has_sql(stmt: str) -> bool:
    return any(
        s and not s.startswith("--")
        for s in (l.strip() for l in stmt.splitlines())
    )


def _execute_sql_file(
    warehouse_id: str,
    catalog: str,
    volume_path: str,
    teardown: bool,
) -> None:
    from databricks.sdk import WorkspaceClient

    ws = WorkspaceClient()

    if teardown:
        statements = [
            f"DROP SCHEMA IF EXISTS {catalog}.{schema} CASCADE"
            for schema in _TEARDOWN_SCHEMAS
        ]
        action = "Tearing down"
    else:
        raw = _SETUP_SQL.read_text()
        sql = raw.replace("${catalog}", catalog).replace("${volume_path}", volume_path)
        statements = [s.strip() for s in sql.split(";") if _has_sql(s)]
        action = "Setting up"

    print(f"{action} demo schemas in catalog '{catalog}'...")

    errors = []
    for i, stmt in enumerate(statements, 1):
        preview = stmt[:60].replace("\n", " ")
        try:
            ws.statement_execution.execute_statement(
                warehouse_id=warehouse_id,
                statement=stmt,
                wait_timeout="60s",
            )
            print(f"  [{i}/{len(statements)}] OK  {preview}...")
        except Exception as exc:
            msg = str(exc)
            print(f"  [{i}/{len(statements)}] ERR {preview}...")
            print(f"           {msg}")
            errors.append((stmt, msg))

    if errors:
        print(f"\n{len(errors)} statement(s) failed. See errors above.")
        sys.exit(1)

    print(f"\nDone. {len(statements)} statement(s) executed successfully.")


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

   python scripts/run_demo.py --teardown
""".format(catalog=catalog, schemas=schemas, questions=_DEMO_QUESTIONS_REL))


def main() -> None:
    parser = argparse.ArgumentParser(description="DBxCarta demo schema setup")
    parser.add_argument("--catalog", default=os.getenv("DBXCARTA_DEMO_CATALOG", "main"))
    parser.add_argument("--volume-path", default=os.getenv("DBXCARTA_DEMO_VOLUME", ""))
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

    _execute_sql_file(
        warehouse_id=warehouse_id,
        catalog=args.catalog,
        volume_path=args.volume_path,
        teardown=args.teardown,
    )

    if not args.teardown:
        _print_next_steps(args.catalog)


if __name__ == "__main__":
    main()
