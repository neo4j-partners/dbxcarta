"""Materialize the candidate-table JSON as Delta tables in Unity Catalog.

Reads the candidate JSON (produced by the candidate selector), creates one
UC schema per candidate entry under the schemapile catalog, then creates one
Delta table per table spec via the shared core materialize spine. Sample row
values from schemapile are inserted when present. No schema list is emitted:
schemapile_lakehouse is a dedicated, data-only catalog, so the dbxcarta run
auto-discovers every materialized schema from a blank `DBXCARTA_SCHEMAS`.

The create-schema, create-table, insert-rows, add-constraints sequence and its
helpers (type coercion, identifier cleanup, value rendering, insert building,
constraint naming, and the `MaterializeStats` tally) live in
:mod:`dbxcarta.core.materialize`. Type coercion uses the shared default map;
anything that does not match falls back to STRING, so every table that has
data lands in UC and downstream evaluation reads it as text where the type was
ambiguous. This module keeps only what is specific to schemapile: the CLI,
provisioning the data catalog, and the warehouse executor.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Any, TYPE_CHECKING

from dbxcarta.core.executor import catalog_exists, execute_ddl
from dbxcarta.core.identifiers import quote_identifier
from dbxcarta.core.materialize import MaterializeStats, materialize_schemas
from dbxcarta.core.workspace import build_workspace_client
from dbxcarta_schemapile_example.config import SchemaPileConfig, load_config
from dbxcarta_schemapile_example.utils import (
    load_dotenv_file,
    read_required_warehouse_id,
)

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient


logger = logging.getLogger(__name__)


def main() -> int:
    parser = argparse.ArgumentParser(
        prog="dbxcarta-schemapile-materialize",
        description=(
            "Read the candidate-table JSON and materialize it as Delta tables"
            " in the schemapile Unity Catalog catalog."
        ),
    )
    parser.add_argument(
        "--dotenv", type=Path, default=Path(__file__).resolve().parents[2] / ".env",
        help="Path to the .env file to load (default: example directory .env)",
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

    if not config.candidate_cache.is_file():
        raise FileNotFoundError(
            f"Candidate JSON not found at {config.candidate_cache}."
            " Run dbxcarta-schemapile-select first."
        )

    warehouse_id = read_required_warehouse_id(
        args.warehouse_id,
        operation="materialize tables",
    )

    payload = json.loads(config.candidate_cache.read_text())
    schemas = payload.get("schemas") or []
    if not schemas:
        print("[schemapile] candidate JSON has no schemas; nothing to do.",
              file=sys.stderr)
        return 1

    ws = build_workspace_client()
    stats = materialize(ws, warehouse_id, config, schemas)

    print(
        f"[schemapile] materialized schemas={stats.schemas_created}"
        f" tables={stats.tables_created} rows={stats.rows_inserted}"
        f" skipped={stats.tables_skipped} type_fallbacks={stats.type_fallbacks}"
        f" primary_keys={stats.pk_constraints_added}"
        f" foreign_keys={stats.fk_constraints_added}",
        file=sys.stderr,
    )
    return 0


def materialize(
    ws: "WorkspaceClient",
    warehouse_id: str,
    config: SchemaPileConfig,
    schemas: list[dict[str, Any]],
) -> MaterializeStats:
    catalog_q = quote_identifier(config.catalog)

    # Provision the data catalog itself. The ops plane (volume, summary) lives
    # in a separate catalog that `dbxcarta-submit bootstrap` creates from
    # DATABRICKS_VOLUME_PATH; the data catalog is this example's own, so
    # materialize owns creating it. config.load_config already refuses a
    # protected/project catalog name, so this never targets a shared catalog.
    # Skip the create when the catalog already exists: on Default-Storage
    # accounts CREATE CATALOG fails without a MANAGED LOCATION even with IF NOT
    # EXISTS, so a pre-created (e.g. UI-created) catalog must not be re-created.
    if not catalog_exists(ws, warehouse_id, config.catalog):
        _execute(
            ws, warehouse_id,
            f"CREATE CATALOG IF NOT EXISTS {catalog_q}"
            " COMMENT 'schemapile materialize: data catalog'",
        )

    def execute(statement: str, label: str) -> None:
        _execute(ws, warehouse_id, statement)

    # schemapile walks tables serially (workers=1) and tolerates a failed table
    # create or row insert (log and continue) so one bad table never aborts the
    # run.
    return materialize_schemas(
        schemas,
        catalog=config.catalog,
        execute=execute,
        property_prefix="schemapile",
        workers=1,
        on_insert_error="skip",
        on_table_error="skip",
        log=logger,
    )


def _execute(ws: "WorkspaceClient", warehouse_id: str, statement: str) -> None:
    """Run one statement on the warehouse and raise on any non-success.

    Delegates to the shared client executor, so a FAILED or timed-out statement
    is surfaced as an error instead of being silently counted as a successful
    materialization.
    """
    succeeded, error = execute_ddl(ws, warehouse_id, statement)
    if not succeeded:
        raise RuntimeError(f"statement failed ({error}):\n{statement}")


if __name__ == "__main__":
    raise SystemExit(main())
