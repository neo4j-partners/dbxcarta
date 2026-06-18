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
# 1.2 makes structural identity first-class: Table nodes gain `catalog`,
# `schema`; Column nodes gain `catalog`, `schema`, `table`. Previously the
# only structural signal was the HAS_* edges plus an opaque hashed `id`, so
# every consumer needing "which catalog/schema/table does this node belong
# to" (batch-by-table-range, FK locality) had to re-join the cached
# information_schema frames. Additive and readers treat the new properties
# as authoritative scalar identity.
# 1.3 stamps every Value node with `last_run` (the run-start timestamp),
# `catalog`, and `schema`. This replaces the driver-collected stale-Value
# purge (which paged catalog-scale column ids back to the driver) with a
# single scoped server-side Cypher delete keyed on `last_run` < run-start
# within the run's catalogs/schemas. Additive; readers treat the new
# properties as authoritative.
# 1.4 made Column key-likeness a first-class boolean `is_key_like` property
# with a per-run `:KeyColumn` label projection. Both existed only to serve
# the semantic-FK same-schema pre-filter and were removed in 1.5.
# 1.5 removes semantic-similarity FK inference and everything that existed
# only to support it: the `EdgeSource.SEMANTIC` provenance value, the
# Column `is_key_like` property, the `:KeyColumn` label, and the
# `keycolumn_embedding` vector index. Declared and metadata FK inference are
# unchanged. Column embeddings and the per-label vector indexes used by
# graph-RAG retrieval are unaffected. Readers of an older graph treat a
# lingering `is_key_like`/`:KeyColumn`/`semantic` edge as inert.
# 1.6 makes the node `id` injective: `generate_id`/`id_expr` now lowercase
# and dot-join only, no longer folding spaces and hyphens to underscores.
# The fold collapsed distinct Unity Catalog securables that differ only by
# hyphen-vs-underscore (e.g. `graph-enriched` and `graph_enriched`) onto one
# id, silently merging them on the Neo4j MERGE. Since UC forbids a literal
# '.' in an unquoted identifier, the dotted join is injective without the
# fold. This re-keys every node whose name contains a space or hyphen, so it
# is a breaking change that requires a full rebuild; ids without those
# characters are unchanged. The Spark `id_expr`, the driver `generate_id`,
# the `verify.catalog` SQL drift check, and the client `_normalize_id_part`
# all move in lockstep. Also additive: Schema/Table/Column gain a
# `{label}_full_text_index` over `name`/`comment`/`id` backing keyword search.
CONTRACT_VERSION = "1.6"

DEFAULT_EMBEDDING_ENDPOINT = "databricks-gte-large-en"


class NodeLabel(StrEnum):
    """Neo4j node labels. `.value` yields the literal label string used in
    Cypher (e.g., 'Column'); StrEnum members are str subclasses so they
    interpolate cleanly into f-strings.
    """

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
    declared-FK source; INFERRED_METADATA is name/PK heuristic inference.
    """

    DECLARED = "declared"
    INFERRED_METADATA = "inferred_metadata"


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
# graph property. Structural membership is ALSO edge-based (the HAS_* rels),
# but as of contract 1.2 the catalog/schema/table identity of Table and
# Column nodes is additionally carried as authoritative scalar properties:
# the `id` is an opaque hash, so re-deriving structure from edges or the
# cached information_schema frames on every consumer (batching, FK locality)
# was the actual smell. The edges and these scalars agree by construction
# (both derive from the same information_schema row).
NODE_PROPERTIES: dict[NodeLabel, tuple[str, ...]] = {
    NodeLabel.DATABASE: ("id", "name", "contract_version", "embedding"),
    NodeLabel.SCHEMA: ("id", "name", "comment", "contract_version", "embedding"),
    NodeLabel.TABLE: (
        "id",
        "name",
        "catalog",
        "schema",
        "layer",
        "comment",
        "table_type",
        "created",
        "last_altered",
        "contract_version",
        "embedding",
    ),
    NodeLabel.COLUMN: (
        "id",
        "name",
        "catalog",
        "schema",
        "table",
        "data_type",
        "is_nullable",
        "ordinal_position",
        "comment",
        "contract_version",
        "embedding",
    ),
    NodeLabel.VALUE: (
        "id",
        "value",
        "count",
        "catalog",
        "schema",
        "last_run",
        "contract_version",
        "embedding",
    ),
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
    """Return a lowercased dot-separated identifier.

    Lowercases each part and joins with dots; no other character folding.
    Must produce byte-identical output to the Spark expression in
    `dbxcarta.spark.ingest.contract_expr.id_expr`. Unity Catalog forbids a
    literal '.' in an unquoted identifier, so the dotted join is injective:
    distinct (catalog, schema, table, column) tuples always yield distinct
    ids. Hyphens and spaces are NOT folded to underscores — doing so
    (contract <=1.5) collapsed `graph-enriched` and `graph_enriched`, two
    distinct UC securables, onto one id and silently merged them on the
    Neo4j MERGE (fixed in contract 1.6).
    """
    return ".".join(p.lower() for p in parts)


def generate_value_id(column_id: str, value: object) -> str:
    """Return the Value node id for a sampled distinct value.

    The value portion is md5-hashed so long strings, booleans, and repeated
    literal samples produce compact stable ids under their owning Column id.
    """
    digest = hashlib.md5(str(value).encode(), usedforsecurity=False).hexdigest()
    return f"{column_id}.{digest}"
