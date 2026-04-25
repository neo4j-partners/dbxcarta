"""DBxCarta configuration (env-var boundary).

Pydantic is used here — this is one of the two places the Python Focus skill
explicitly calls out (the other being external-data parsing): cross-field
validation at a trust boundary, with a generated schema.

Internal DTOs elsewhere are `@dataclass`, per the skill's decision table.
"""

from __future__ import annotations

import re

from pydantic import field_validator, model_validator
from pydantic_settings import BaseSettings


# Strict Databricks identifier: alphabetic/underscore start, then alphanumerics/
# underscores/hyphens. Excludes backticks, dots, and spaces. Hyphens are valid
# in catalog and schema names but require backtick-quoting in SQL. Backticks
# are the actual injection vector and remain excluded.
# Dotted names (`schema.table`, `cat.schema.table`) are split on `.` by their
# field validator and validated per part.
_IDENTIFIER_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_-]*$")


class Settings(BaseSettings):
    databricks_secret_scope: str = "dbxcarta-neo4j"
    dbxcarta_catalog: str
    dbxcarta_schemas: str = ""
    dbxcarta_summary_volume: str
    dbxcarta_summary_table: str
    # Sample values
    dbxcarta_include_values: bool = True
    dbxcarta_sample_limit: int = 10
    dbxcarta_sample_cardinality_threshold: int = 50
    dbxcarta_stack_chunk_size: int = 50
    # Embedding feature flags — all off by default; turn on one label at a time.
    dbxcarta_include_embeddings_tables: bool = False
    dbxcarta_include_embeddings_columns: bool = False
    dbxcarta_include_embeddings_values: bool = False
    dbxcarta_include_embeddings_schemas: bool = False
    dbxcarta_include_embeddings_databases: bool = False
    dbxcarta_embedding_failure_threshold: float = 0.05
    dbxcarta_embedding_endpoint: str = "databricks-gte-large-en"
    dbxcarta_embedding_dimension: int = 1024
    # Materialize-once between ai_query and downstream actions.
    dbxcarta_staging_path: str = ""
    # Neo4j Spark Connector batch.size.
    dbxcarta_neo4j_batch_size: int = 20000
    # Re-embedding ledger: skip ai_query for unchanged nodes.
    dbxcarta_ledger_enabled: bool = False
    dbxcarta_ledger_path: str = ""
    # Semantic FK discovery.
    # Default False because column embeddings also default off, and semantic
    # discovery requires them — the cross-field validator below rejects the
    # incoherent combination. Deployments that want semantic set both
    # DBXCARTA_INFER_SEMANTIC=true and DBXCARTA_INCLUDE_EMBEDDINGS_COLUMNS=true.
    dbxcarta_infer_semantic: bool = False
    dbxcarta_semantic_min_tables: int = 10
    dbxcarta_semantic_threshold: float = 0.85

    @field_validator("dbxcarta_catalog")
    @classmethod
    def _validate_catalog(cls, v: str) -> str:
        if not _IDENTIFIER_RE.match(v):
            raise ValueError(f"Invalid Databricks identifier: {v!r}")
        return v

    @field_validator("dbxcarta_summary_table")
    @classmethod
    def _validate_summary_table(cls, v: str) -> str:
        # Accepts `table`, `schema.table`, or `catalog.schema.table`. Each dot-
        # separated part must be a strict Databricks identifier; this is the
        # only place where a dotted value is accepted.
        for part in v.split("."):
            if not _IDENTIFIER_RE.match(part):
                raise ValueError(
                    f"Invalid identifier part {part!r} in table {v!r}"
                )
        return v

    @model_validator(mode="after")
    def _validate_feature_coherence(self) -> "Settings":
        """Cross-field sanity checks.

        Fail at Settings construction (i.e., job startup) rather than halfway
        through a run. Two known incoherence cases today:

        1. Value embeddings require sample-values to be enabled — there are
           no Value nodes to embed otherwise.
        2. Semantic FK discovery requires column embeddings to be enabled —
           cosine similarity needs vectors.
        """
        if (
            self.dbxcarta_include_embeddings_values
            and not self.dbxcarta_include_values
        ):
            raise ValueError(
                "DBXCARTA_INCLUDE_EMBEDDINGS_VALUES=true requires"
                " DBXCARTA_INCLUDE_VALUES=true (nothing to embed otherwise)"
            )
        if (
            self.dbxcarta_infer_semantic
            and not self.dbxcarta_include_embeddings_columns
        ):
            raise ValueError(
                "DBXCARTA_INFER_SEMANTIC=true requires"
                " DBXCARTA_INCLUDE_EMBEDDINGS_COLUMNS=true"
                " (Phase 4 consumes column embeddings)"
            )
        return self
