"""Provision the Finance Genie ops plane: catalog, schema, and volume.

Idempotent. Finance Genie's medallion *data* catalogs, schemas, and tables are
owned by the upstream Finance Genie project and are not created here. This
command provisions only the dbxcarta ops plane that holds run summaries, the
generation cache, and the uploaded question set: the ops catalog, the
`finance_genie_ops` schema, and the `dbxcarta-ops` volume, read from the preset.

The ops catalog is shared with other dbxcarta integrations, so `--drop-all`
drops only the `finance_genie_ops` schema and never the catalog.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import TYPE_CHECKING

from dbxcarta.client.databricks import build_workspace_client
from dbxcarta.spark.databricks import quote_identifier
from dbxcarta_finance_genie_example.finance_genie import FinanceGeniePreset, preset
from dbxcarta_finance_genie_example.utils import (
    load_dotenv_file,
    read_required_warehouse_id,
)

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient


def main() -> int:
    parser = argparse.ArgumentParser(
        prog="dbxcarta-finance-genie-bootstrap",
        description=(
            "Create the Finance Genie ops catalog, the finance_genie_ops schema,"
            " and the dbxcarta-ops volume. The upstream medallion data catalogs"
            " are not created here. Requires catalog-create privilege."
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
            "Drop the finance_genie_ops schema and everything under it. The"
            " shared ops catalog is preserved. Requires confirmation."
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

    warehouse_id = read_required_warehouse_id(args.warehouse_id, operation="bootstrap")

    ws = build_workspace_client()
    if args.drop_all:
        if not args.yes_i_mean_it:
            print(
                "[finance-genie] --drop-all requires --yes-i-mean-it; not dropping.",
                file=sys.stderr,
            )
            return 2
        return _drop(ws, warehouse_id, preset)
    return _provision(ws, warehouse_id, preset)


def _provision(ws: "WorkspaceClient", warehouse_id: str, cfg: FinanceGeniePreset) -> int:
    catalog_q = quote_identifier(cfg.ops_catalog)
    schema_q = quote_identifier(cfg.ops_schema)
    volume_q = quote_identifier(cfg.ops_volume)

    _execute(
        ws, warehouse_id,
        f"CREATE CATALOG IF NOT EXISTS {catalog_q}"
        f" COMMENT 'dbxcarta ops catalog'",
    )
    _execute(
        ws, warehouse_id,
        f"CREATE SCHEMA IF NOT EXISTS {catalog_q}.{schema_q}"
        f" COMMENT 'dbxcarta Finance Genie ops schema'",
    )
    _execute(
        ws, warehouse_id,
        f"CREATE VOLUME IF NOT EXISTS {catalog_q}.{schema_q}.{volume_q}"
        f" COMMENT 'dbxcarta Finance Genie ops volume'",
    )
    print(
        f"[finance-genie] bootstrap complete:"
        f" {cfg.ops_catalog}.{cfg.ops_schema}.{cfg.ops_volume}",
        file=sys.stderr,
    )
    return 0


def _drop(ws: "WorkspaceClient", warehouse_id: str, cfg: FinanceGeniePreset) -> int:
    catalog_q = quote_identifier(cfg.ops_catalog)
    schema_q = quote_identifier(cfg.ops_schema)
    _execute(ws, warehouse_id, f"DROP SCHEMA IF EXISTS {catalog_q}.{schema_q} CASCADE")
    print(
        f"[finance-genie] dropped ops schema {cfg.ops_catalog}.{cfg.ops_schema};"
        f" shared ops catalog preserved",
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
