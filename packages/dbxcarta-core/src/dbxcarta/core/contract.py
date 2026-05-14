"""Graph contract: node/relationship/edge-source enums and identifier generation.

All identifier production goes through generate_id or generate_value_id.
No call site builds an ID inline. All label and relationship references
go through NodeLabel / RelType / EdgeSource enums — no magic strings.
"""

from __future__ import annotations

import hashlib
from enum import StrEnum

CONTRACT_VERSION = "1.0"

DEFAULT_EMBEDDING_ENDPOINT = "databricks-gte-large-en"


class NodeLabel(StrEnum):
    """Neo4j node labels. `.value` yields the literal label string used in
    Cypher (e.g., 'Column'); StrEnum members are str subclasses so they
    interpolate cleanly into f-strings."""

    DATABASE = "Database"
    SCHEMA = "Schema"
    TABLE = "Table"
    COLUMN = "Column"
    VALUE = "Value"


class RelType(StrEnum):
    """Neo4j relationship types."""

    HAS_SCHEMA = "HAS_SCHEMA"
    HAS_TABLE = "HAS_TABLE"
    HAS_COLUMN = "HAS_COLUMN"
    HAS_VALUE = "HAS_VALUE"
    REFERENCES = "REFERENCES"


class EdgeSource(StrEnum):
    """Provenance tag on REFERENCES edges. DECLARED is the Unity Catalog
    declared-FK source; INFERRED_METADATA is name/PK heuristic inference;
    SEMANTIC is embedding cosine similarity."""

    DECLARED = "declared"
    INFERRED_METADATA = "inferred_metadata"
    SEMANTIC = "semantic"


# REFERENCES edge properties (additive in contract v1.0). All three are
# nullable; readers treat absence as (1.0, "declared", null) via COALESCE.
REFERENCES_PROPERTIES: tuple[str, ...] = ("confidence", "source", "criteria")


def generate_id(*parts: str) -> str:
    """Return a normalized dot-separated identifier.

    Lowercases each part and replaces spaces and hyphens with underscores,
    then joins with dots. Must produce byte-identical output to the Spark
    expression in `dbxcarta.spark.ingest.contract_expr.id_expr`.
    """
    return ".".join(p.lower().replace(" ", "_").replace("-", "_") for p in parts)


def generate_value_id(column_id: str, value: object) -> str:
    """Return the Value node id for a sampled distinct value.

    The value portion is md5-hashed so long strings, booleans, and repeated
    literal samples produce compact stable ids under their owning Column id.
    """
    digest = hashlib.md5(str(value).encode()).hexdigest()
    return f"{column_id}.{digest}"
