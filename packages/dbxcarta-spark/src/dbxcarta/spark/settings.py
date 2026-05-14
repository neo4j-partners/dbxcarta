"""DBxCarta configuration (env-var boundary).

Pydantic is used here — this is one of the two places the Python Focus skill
explicitly calls out (the other being external-data parsing): cross-field
validation at a trust boundary, with a generated schema.

Internal DTOs elsewhere are `@dataclass`, per the skill's decision table.
"""

from __future__ import annotations

from pydantic import field_validator, model_validator
from pydantic_settings import BaseSettings

from dbxcarta.core.databricks import (
    split_qualified_name,
    validate_identifier,
    validate_serving_endpoint_name,
    validate_uc_volume_subpath,
)


class SparkIngestSettings(BaseSettings):
    """Environment-backed configuration for the ingest job.

    Required fields describe the Databricks catalog, summary sinks, and Neo4j
    secret scope. Validators reject unsafe identifiers and incoherent feature
    flag combinations before Spark or Neo4j work starts.
    """

    databricks_secret_scope: str = "dbxcarta-neo4j"
    # Used by verify's catalog-vs-graph checks (information_schema queries).
    # When unset, those checks self-skip with a `catalog.no_warehouse` violation.
    databricks_warehouse_id: str = ""
    dbxcarta_catalog: str
    # Comma-separated list of bare schema names under dbxcarta_catalog.
    # Blank string means "every schema in the catalog". Whitespace around each
    # name is stripped. FK inference (metadata and semantic) is restricted to
    # column pairs within the same (catalog, schema), so listing many schemas
    # produces disjoint subgraphs by design.
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
    # When False (default), verify violations are logged as warnings and the run
    # completes with status='success'. When True, any violation raises and the
    # task fails. Flip after two consecutive zero-violation warn-only runs.
    dbxcarta_verify_gate: bool = False

    @field_validator("dbxcarta_catalog")
    @classmethod
    def _validate_catalog(cls, v: str) -> str:
        """Require a single safe Databricks catalog identifier."""
        return validate_identifier(v)

    @field_validator("dbxcarta_summary_table")
    @classmethod
    def _validate_summary_table(cls, v: str) -> str:
        """Require summary history to target catalog.schema.table explicitly."""
        # Persisted run history is a UC artifact, so require an explicit
        # catalog.schema.table target rather than relying on workspace defaults.
        split_qualified_name(v, expected_parts=3, label="summary table")
        return v

    @field_validator("dbxcarta_summary_volume")
    @classmethod
    def _validate_summary_volume(cls, v: str) -> str:
        """Require a UC Volume subpath for JSON summary output."""
        return validate_uc_volume_subpath(v, label="DBXCARTA_SUMMARY_VOLUME")

    @field_validator("dbxcarta_staging_path", "dbxcarta_ledger_path")
    @classmethod
    def _validate_optional_volume_subpath(cls, v: str) -> str:
        """Normalize optional UC Volume paths while allowing unset values."""
        if not v.strip():
            return ""
        return validate_uc_volume_subpath(v.strip())

    @field_validator("dbxcarta_embedding_endpoint")
    @classmethod
    def _validate_embedding_endpoint(cls, v: str) -> str:
        """Reject endpoint names that cannot be safely interpolated into SQL."""
        return validate_serving_endpoint_name(v)

    @model_validator(mode="after")
    def _validate_feature_coherence(self) -> "SparkIngestSettings":
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
                " (semantic FK discovery consumes column embeddings)"
            )
        return self
