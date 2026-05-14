"""Public core API for dbxcarta.

Core owns the stable semantic graph contract, validation helpers, lightweight
configuration protocols, and verification primitives. It intentionally excludes
client evaluation arms, question upload helpers, readiness checks, Spark job
orchestration, and synthetic materialization utilities.
"""

from dbxcarta.core.builder import SemanticLayerBuilder, SemanticLayerResult
from dbxcarta.core.contract import (
    CONTRACT_VERSION,
    DEFAULT_EMBEDDING_ENDPOINT,
    REFERENCES_PROPERTIES,
    EdgeSource,
    NodeLabel,
    RelType,
    generate_id,
    generate_value_id,
)
from dbxcarta.core.databricks import (
    quote_identifier,
    quote_qualified_name,
    split_qualified_name,
    uc_volume_parent,
    uc_volume_parts,
    validate_identifier,
    validate_serving_endpoint_name,
    validate_uc_volume_subpath,
)
from dbxcarta.core.env import EnvOverlay, apply_env_overlay, inject_params, load_env_overlay
from dbxcarta.core.settings import SemanticLayerConfig
from dbxcarta.core.verify import Report, Violation, verify_run

__all__ = [
    "CONTRACT_VERSION",
    "DEFAULT_EMBEDDING_ENDPOINT",
    "EdgeSource",
    "EnvOverlay",
    "NodeLabel",
    "REFERENCES_PROPERTIES",
    "RelType",
    "Report",
    "SemanticLayerBuilder",
    "SemanticLayerConfig",
    "SemanticLayerResult",
    "Violation",
    "apply_env_overlay",
    "generate_id",
    "generate_value_id",
    "inject_params",
    "load_env_overlay",
    "quote_identifier",
    "quote_qualified_name",
    "split_qualified_name",
    "uc_volume_parent",
    "uc_volume_parts",
    "validate_identifier",
    "validate_serving_endpoint_name",
    "validate_uc_volume_subpath",
    "verify_run",
]
