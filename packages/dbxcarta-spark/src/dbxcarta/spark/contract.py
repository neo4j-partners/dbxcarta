"""Graph contract: node/relationship/edge-source enums and identifier generation.

All identifier production goes through generate_id or generate_value_id.
No call site builds an ID inline. All label and relationship references
go through NodeLabel / RelType / EdgeSource enums — no magic strings.
"""

from __future__ import annotations

import hashlib
from enum import StrEnum

# 1.1 adds the additive Table node `layer` property (bronze/silver/gold),
# derived at ingest from a configurable catalog->layer map. Readers treat a
# missing `layer` as null.
CONTRACT_VERSION = "1.1"

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


# Per-label declared node properties: the exact, complete set of columns
# that may be written to a Neo4j node. This is the single source of truth
# for the graph contract. The write boundary (run.py:_load) projects each
# node DataFrame to this tuple before the connector write, so a column is a
# graph property if and only if it is listed here. `embedding` is the only
# member that may be legitimately absent (present only when the label was
# embedded this run); it is kept last and the projection selects the
# intersection with the DataFrame columns. Everything not listed —
# information_schema helper columns, the transient `embedding_text`, and the
# embedding bookkeeping `embedding_text_hash` / `embedding_model` /
# `embedded_at` / `embedding_error` — is staging/ledger-only and never a
# graph property. Structural membership (catalog/schema/table) stays
# edge-based and is deliberately not duplicated here as scalar properties.
NODE_PROPERTIES: dict[NodeLabel, tuple[str, ...]] = {
    NodeLabel.DATABASE: ("id", "name", "contract_version", "embedding"),
    NodeLabel.SCHEMA: ("id", "name", "comment", "contract_version", "embedding"),
    NodeLabel.TABLE: (
        "id", "name", "layer", "comment", "table_type", "created",
        "last_altered", "contract_version", "embedding",
    ),
    NodeLabel.COLUMN: (
        "id", "name", "data_type", "is_nullable", "ordinal_position",
        "comment", "contract_version", "embedding",
    ),
    NodeLabel.VALUE: ("id", "value", "count", "contract_version", "embedding"),
}


# Per-label embedding-text SQL expressions. Evaluated inside the node
# builder (schema_graph.py / sample_values.py) while the raw
# information_schema helper columns are still in scope, producing one
# `embedding_text` column per node. The embed stage hashes and embeds that
# column; it no longer holds these expressions. Kept here so the builder,
# the embed stage, and the tests share one definition. Catalog leads every
# qualified name so `bronze.sales.orders` and `gold.sales.orders` never
# embed identically in a multi-catalog graph; in a single-catalog graph it
# is a constant prefix. Expression content is byte-identical to the
# pre-refactor in-place expressions, so embedding_text_hash is unchanged.
EMBEDDING_TEXT_EXPR: dict[NodeLabel, str] = {
    NodeLabel.TABLE: (
        "concat_ws(' | ', concat_ws('.', table_catalog, table_schema, name),"
        " nullif(trim(comment), ''))"
    ),
    NodeLabel.COLUMN: (
        "concat_ws(' | ', concat_ws('.', table_catalog, table_schema, table_name, name),"
        " data_type, nullif(trim(comment), ''))"
    ),
    NodeLabel.SCHEMA: (
        "concat_ws(' | ', concat_ws('.', catalog_name, name), nullif(trim(comment), ''))"
    ),
    NodeLabel.DATABASE: "name",
    NodeLabel.VALUE: "value",
}


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
