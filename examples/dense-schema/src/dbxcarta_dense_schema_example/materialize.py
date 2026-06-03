"""Materialize dense-schema candidates as Delta tables in Unity Catalog.

The create-schema, create-table, insert-rows, add-constraints sequence and all
of its helpers (type coercion, identifier cleanup, value rendering, insert
building, constraint naming, and the ``MaterializeStats`` tally) live in
:mod:`dbxcarta.core.materialize`. This module keeps only what is specific to
dense-schema: the CLI, provisioning the data catalog, and the warehouse
executor. It binds the shared spine with parallel table builds
(``workers``) and skip-on-error handling to match this example's behavior.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from pathlib import Path
from typing import Any

from databricks.sdk import WorkspaceClient
from databricks.sdk.config import Config
from dbxcarta.core.executor import catalog_exists
from dbxcarta.core.identifiers import quote_identifier
from dbxcarta.core.materialize import MaterializeStats, materialize_schemas
from dbxcarta_dense_schema_example.config import DenseSchemaConfig, load_config
from dbxcarta_dense_schema_example.utils import (
    load_dotenv_file,
    read_required_warehouse_id,
)


logger = logging.getLogger(__name__)

_HTTP_TIMEOUT_S = 300
_POLL_INTERVAL_S = 3.0
_STATEMENT_TIMEOUT_S = 600.0
_DEFAULT_WORKERS = 20


def main() -> int:
    parser = argparse.ArgumentParser(prog="dbxcarta-dense-materialize")
    parser.add_argument("--dotenv", type=Path, default=Path(__file__).resolve().parents[2] / ".env")
    parser.add_argument("--warehouse-id", type=str, default=None)
    parser.add_argument("--workers", type=int, default=_DEFAULT_WORKERS,
                        help="parallel worker threads for table creation (default: 20)")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )

    load_dotenv_file(args.dotenv)
    config = load_config()

    if not config.candidate_cache.is_file():
        raise FileNotFoundError(
            f"Candidate JSON not found at {config.candidate_cache}."
            " Run dbxcarta-dense-generate first."
        )

    warehouse_id = read_required_warehouse_id(
        args.warehouse_id, operation="materialize tables"
    )
    payload = json.loads(config.candidate_cache.read_text())
    schemas = payload.get("schemas") or []
    if not schemas:
        print("[dense] no schemas in candidates JSON", file=sys.stderr)
        return 1

    profile = os.environ.get("DATABRICKS_PROFILE")
    ws = WorkspaceClient(
        config=Config(profile=profile, http_timeout_seconds=_HTTP_TIMEOUT_S)
    )
    logger.info("[dense] workers=%d", args.workers)
    stats = materialize(ws, warehouse_id, config, schemas, workers=args.workers)
    print(
        f"[dense] materialized tables={stats.tables_created}"
        f" rows={stats.rows_inserted} skipped={stats.tables_skipped}"
        f" type_fallbacks={stats.type_fallbacks}"
        f" pks={stats.pk_constraints_added} fks={stats.fk_constraints_added}",
        file=sys.stderr,
    )
    return 0


def materialize(
    ws: WorkspaceClient,
    warehouse_id: str,
    config: DenseSchemaConfig,
    schemas: list[dict[str, Any]],
    *,
    workers: int = _DEFAULT_WORKERS,
) -> MaterializeStats:
    catalog_q = quote_identifier(config.catalog)

    # Provision the data catalog itself. The ops plane (volume, summary) lives
    # in a separate catalog that `dbxcarta-submit bootstrap` creates from
    # DATABRICKS_VOLUME_PATH; the data catalog is this example's own, so
    # materialize owns creating it. load_config already refuses a
    # protected/project catalog name, so this never targets a shared catalog.
    # Skip the create when the catalog already exists: on Default-Storage
    # accounts CREATE CATALOG fails without a MANAGED LOCATION even with IF NOT
    # EXISTS, so a pre-created (e.g. UI-created) catalog must not be re-created.
    if not catalog_exists(ws, warehouse_id, config.catalog):
        _execute(
            ws, warehouse_id,
            f"CREATE CATALOG IF NOT EXISTS {catalog_q}"
            " COMMENT 'dense-schema materialize: data catalog'",
            label=f"CREATE CATALOG {config.catalog}",
        )

    def execute(statement: str, label: str) -> None:
        _execute(ws, warehouse_id, statement, label=label)

    # Dense tolerates a failed table create or row insert (log and continue) so
    # one bad table never aborts a large run, and builds tables in parallel.
    return materialize_schemas(
        schemas,
        catalog=config.catalog,
        execute=execute,
        property_prefix="dense",
        workers=workers,
        on_insert_error="skip",
        on_table_error="skip",
        log=logger,
    )


def _execute(
    ws: WorkspaceClient,
    warehouse_id: str,
    statement: str,
    *,
    label: str = "",
) -> None:
    from databricks.sdk.service.sql import (
        ExecuteStatementRequestOnWaitTimeout,
        StatementState,
    )

    t0 = time.monotonic()
    response = ws.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=statement,
        wait_timeout="50s",
        on_wait_timeout=ExecuteStatementRequestOnWaitTimeout.CONTINUE,
    )
    state = response.status.state if response.status else StatementState.SUCCEEDED
    statement_id = response.statement_id
    if statement_id is None:
        raise RuntimeError(
            f"No statement id returned for: {label or statement[:80]}"
        )

    deadline = t0 + _STATEMENT_TIMEOUT_S
    while state in (StatementState.PENDING, StatementState.RUNNING):
        if time.monotonic() > deadline:
            raise TimeoutError(
                f"Statement timed out after {_STATEMENT_TIMEOUT_S:.0f}s: {label or statement[:80]}"
            )
        time.sleep(_POLL_INTERVAL_S)
        response = ws.statement_execution.get_statement(statement_id)
        state = response.status.state if response.status else StatementState.SUCCEEDED

    if state != StatementState.SUCCEEDED:
        err = response.status.error if response.status else None
        raise RuntimeError(
            f"Statement failed (state={state}): {label or statement[:80]}"
            + (f" — {err}" if err else "")
        )

    logger.debug("[dense] %.1fs %s", time.monotonic() - t0, label or statement[:80])


if __name__ == "__main__":
    raise SystemExit(main())
