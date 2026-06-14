"""Readiness check for dbxcarta integrations.

The check is driven entirely by the per-example
``examples/<name>/dbxcarta-overlay.env`` the CLI has already loaded; there is no
per-example Python object to publish. :func:`check_readiness` reads the catalog
list from the environment. The CLI (``dbxcarta ready``) calls it directly.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import TYPE_CHECKING

from dbxcarta.core.catalogs import resolve_catalogs
from dbxcarta.core.identifiers import quote_identifier

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient


# Unity Catalog auto-creates these in every catalog, so they are not evidence
# that anything has been materialized. Readiness excludes both.
_NON_DATA_SCHEMAS = frozenset({"information_schema", "default"})


@dataclass(frozen=True)
class ReadinessReport:
    """Result of a readiness check.

    ``present`` and ``missing_required`` hold catalog names: a catalog is
    present when it holds a data schema and missing when it does not.
    ``catalog`` holds the comma-joined resolved catalog list and ``schema`` is
    empty, because readiness no longer targets a single schema.
    """

    catalog: str
    schema: str
    present: tuple[str, ...]
    missing_required: tuple[str, ...]
    missing_optional: tuple[str, ...]

    def ok(self, *, strict_optional: bool = False) -> bool:
        if self.missing_required:
            return False
        return not (strict_optional and self.missing_optional)

    def format(self, *, strict_optional: bool = False) -> str:
        scope = self.catalog if not self.schema else f"{self.catalog}.{self.schema}"
        lines = [
            f"scope: {scope}",
            f"present catalogs: {len(self.present)}",
        ]
        if self.missing_required:
            lines.append("missing required: " + ", ".join(self.missing_required))
        else:
            lines.append("catalogs: ready")
        # No example populates missing_optional under the one-rule design: every
        # listed catalog is required. The field and the strict_optional flag are
        # kept so ok() and the CLI --strict-optional flag need no signature
        # change, but the report stays silent on optional catalogs unless one is
        # ever surfaced, rather than printing a category the design removed.
        if self.missing_optional:
            label = "missing optional"
            if not strict_optional:
                label += " (warning)"
            lines.append(f"{label}: " + ", ".join(self.missing_optional))
        lines.append(
            "status: ready" if self.ok(strict_optional=strict_optional) else "status: not ready"
        )
        return "\n".join(lines)


def check_readiness(ws: WorkspaceClient, warehouse_id: str) -> ReadinessReport:
    """Report whether each ingested catalog holds a data schema.

    Resolves the catalog list from DBXCARTA_CATALOG and DBXCARTA_CATALOGS
    through the same parser the pipeline uses, then checks each catalog for a
    schema other than the UC-auto-created information_schema and default. A
    catalog holding only those two has nothing materialized and is not ready.
    Fails loud when DBXCARTA_CATALOG is unset, the same way an ingest run would.
    """
    catalog = os.environ.get("DBXCARTA_CATALOG", "").strip()
    if not catalog:
        raise RuntimeError(
            "DBXCARTA_CATALOG is not set; cannot resolve the catalogs to"
            " check. Select an integration overlay with --env-file."
        )
    catalogs = resolve_catalogs(catalog, os.environ.get("DBXCARTA_CATALOGS", ""))
    present: list[str] = []
    missing: list[str] = []
    for cat in catalogs:
        if _has_data_schema(ws, warehouse_id, cat):
            present.append(cat)
        else:
            missing.append(cat)
    return ReadinessReport(
        catalog=",".join(catalogs),
        schema="",
        present=tuple(present),
        missing_required=tuple(missing),
        missing_optional=(),
    )


def _has_data_schema(
    ws: WorkspaceClient,
    warehouse_id: str,
    catalog: str,
) -> bool:
    """True when the catalog holds a schema beyond information_schema/default."""
    return any(
        (cleaned := name.strip()) and cleaned not in _NON_DATA_SCHEMAS
        for name in _fetch_schema_names(ws, warehouse_id, catalog)
    )


def _fetch_schema_names(
    ws: WorkspaceClient,
    warehouse_id: str,
    catalog: str,
) -> list[str]:
    from databricks.sdk.service.sql import ExecuteStatementRequestOnWaitTimeout

    response = ws.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=(
            f"SELECT schema_name FROM {quote_identifier(catalog)}.information_schema.schemata"
        ),
        wait_timeout="50s",
        on_wait_timeout=ExecuteStatementRequestOnWaitTimeout.CANCEL,
    )
    rows = getattr(getattr(response, "result", None), "data_array", None) or []
    return [row[0] for row in rows if row]
