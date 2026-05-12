"""DBxCarta: Unity Catalog to Neo4j semantic-layer ingestion.

This module is the deliberate public surface of the package. Anything not
re-exported here is internal even if importable.

Preset authors and external consumers should depend only on the names below.
"""

from dbxcarta.contract import (
    CONTRACT_VERSION,
    REFERENCES_PROPERTIES,
    EdgeSource,
    NodeLabel,
    RelType,
)
from dbxcarta.databricks import (
    build_workspace_client,
    validate_identifier,
    validate_uc_volume_subpath,
)
from dbxcarta.ingest import run_dbxcarta
from dbxcarta.client import run_client
from dbxcarta.preset_loader import load_preset
from dbxcarta.presets import (
    Preset,
    ReadinessReport,
    format_env,
)
from dbxcarta.settings import Settings
from dbxcarta.verify import Report, Violation, verify_run

__all__ = [
    "CONTRACT_VERSION",
    "EdgeSource",
    "NodeLabel",
    "Preset",
    "REFERENCES_PROPERTIES",
    "ReadinessReport",
    "Report",
    "RelType",
    "Settings",
    "Violation",
    "build_workspace_client",
    "format_env",
    "load_preset",
    "run_client",
    "run_dbxcarta",
    "validate_identifier",
    "validate_uc_volume_subpath",
    "verify_run",
]
