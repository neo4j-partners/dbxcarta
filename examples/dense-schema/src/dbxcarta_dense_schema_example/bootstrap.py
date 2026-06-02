"""Provision the Unity Catalog catalog, _meta schema, and volume for dense-schema.

Idempotent. The dense-schema fixture reuses the SchemaPile lakehouse catalog,
the `_meta` schema, and the `schemapile_volume` volume, and lives in its own
data schema (for example `dense_1000`). This command provisions only the shared
catalog/schema/volume; the data schema and its tables are created by
`dbxcarta-dense-materialize`.

Because the catalog, `_meta` schema, and volume are shared with SchemaPile,
`--drop-all` drops only the dense data schema and never the shared catalog.
Tear the shared catalog down with `dbxcarta-schemapile-bootstrap --drop-all`.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import TYPE_CHECKING

from dbxcarta.client.databricks import build_workspace_client
from dbxcarta.spark.databricks import quote_identifier
from dbxcarta_dense_schema_example.config import DenseSchemaConfig, load_config
from dbxcarta_dense_schema_example.utils import (
    load_dotenv_file,
    read_required_warehouse_id,
)

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient


def main() -> int:
    parser = argparse.ArgumentParser(
        prog="dbxcarta-dense-bootstrap",
        description=(
            "Create the shared catalog, the _meta schema, and the volume the"
            " dense-schema fixture reuses from SchemaPile. Requires"
            " catalog-create privilege on the workspace."
        ),
    )
    parser.add_argument(
        "--dotenv", type=Path, default=Path(__file__).resolve().parents[2] / ".env",
        help="Path to the .env file to load (default: example directory .env)",
    )
    parser.add_argument(
        "--drop-all",
        action="store_true",
        help=(
            "Drop the dense data schema and everything under it. The shared"
            " catalog, _meta schema, and volume are preserved. Requires"
            " confirmation."
        ),
    )
    parser.add_argument(
        "--yes-i-mean-it",
        action="store_true",
        help="Required with --drop-all so accidental invocations no-op.",
    )
    parser.add_argument(
        "--warehouse-id",
        type=str,
        default=None,
        help="Override DATABRICKS_WAREHOUSE_ID for this run.",
    )
    args = parser.parse_args()

    load_dotenv_file(args.dotenv)
    config = load_config()

    warehouse_id = read_required_warehouse_id(args.warehouse_id, operation="bootstrap")

    ws = build_workspace_client()
    if args.drop_all:
        if not args.yes_i_mean_it:
            print(
                "[dense] --drop-all requires --yes-i-mean-it; not dropping.",
                file=sys.stderr,
            )
            return 2
        return _drop(ws, warehouse_id, config)
    return _provision(ws, warehouse_id, config)


def _provision(ws: "WorkspaceClient", warehouse_id: str, config: DenseSchemaConfig) -> int:
    catalog_q = quote_identifier(config.catalog)
    meta_q = quote_identifier(config.meta_schema)
    volume_q = quote_identifier(config.volume)

    _execute(
        ws, warehouse_id,
        f"CREATE CATALOG IF NOT EXISTS {catalog_q}"
        f" COMMENT 'dbxcarta dense-schema example catalog (shared with schemapile)'",
    )
    _execute(
        ws, warehouse_id,
        f"CREATE SCHEMA IF NOT EXISTS {catalog_q}.{meta_q}"
        f" COMMENT 'dbxcarta dense-schema example bookkeeping schema'",
    )
    _execute(
        ws, warehouse_id,
        f"CREATE VOLUME IF NOT EXISTS {catalog_q}.{meta_q}.{volume_q}"
        f" COMMENT 'dbxcarta dense-schema example volume'",
    )
    print(
        f"[dense] bootstrap complete:"
        f" {config.catalog}.{config.meta_schema}.{config.volume}",
        file=sys.stderr,
    )
    return 0


def _drop(ws: "WorkspaceClient", warehouse_id: str, config: DenseSchemaConfig) -> int:
    catalog_q = quote_identifier(config.catalog)
    schema_q = quote_identifier(config.uc_schema)
    _execute(ws, warehouse_id, f"DROP SCHEMA IF EXISTS {catalog_q}.{schema_q} CASCADE")
    print(
        f"[dense] dropped data schema {config.catalog}.{config.uc_schema};"
        f" shared catalog and volume preserved",
        file=sys.stderr,
    )
    return 0


def _execute(ws: "WorkspaceClient", warehouse_id: str, statement: str) -> None:
    from databricks.sdk.service.sql import ExecuteStatementRequestOnWaitTimeout

    ws.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=statement,
        wait_timeout="50s",
        on_wait_timeout=ExecuteStatementRequestOnWaitTimeout.CONTINUE,
    )


if __name__ == "__main__":
    raise SystemExit(main())
