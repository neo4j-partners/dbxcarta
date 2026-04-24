"""DBxCarta configuration (env-var boundary).

Pydantic is used here — this is one of the two places the Python Focus skill
explicitly calls out (the other being external-data parsing): cross-field
validation at a trust boundary, with a generated schema.

Internal DTOs elsewhere are `@dataclass`, per the skill's decision table.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

from pydantic import field_validator, model_validator
from pydantic_settings import BaseSettings

if TYPE_CHECKING:
    from dbxcarta.fk_config import FKInferenceConfig


# Strict Databricks identifier: alphabetic/underscore start, then alphanumerics/
# underscores. Deliberately excludes backticks, dots, hyphens, and spaces.
# Dotted names (`schema.table`, `cat.schema.table`) are split on `.` by their
# field validator and validated per part. The old permissive regex allowed
# backticks inside identifier segments, which combined with f"`{catalog}`"
# SQL interpolation could escape the intended quoting.
_IDENTIFIER_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


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
    # Phase 3: metadata FK inference
    dbxcarta_infer_metadata: bool = True
    # Phase 4: semantic FK inference.
    # Default False because column embeddings also default off, and semantic
    # inference requires them — the cross-field validator below rejects the
    # incoherent combination. Deployments that want Phase 4 set both
    # DBXCARTA_INFER_SEMANTIC=true and DBXCARTA_INCLUDE_EMBEDDINGS_COLUMNS=true.
    # (The worklog ablation matrix's `true` default assumed embeddings on.)
    dbxcarta_infer_semantic: bool = False
    dbxcarta_semantic_min_tables: int = 10
    dbxcarta_semantic_threshold: float = 0.85

    # --- FK inference tuning knobs (Phase 3 & 4) ----------------------------
    # All default to the values previously hard-coded in the inference modules.
    # Operators set these env vars to tune behavior without touching source code.

    # Phase 3: minimum attenuated score to emit a metadata-inferred reference.
    dbxcarta_fk_metadata_threshold: float = 0.8

    # Phase 3: tie-break attenuation exponent. Default 0.5 = sqrt (original).
    # Lower → more permissive; higher → more aggressive.
    dbxcarta_fk_attenuation_exponent: float = 0.5

    # Phase 3: optional hard top-N cap per source column (0 = no cap).
    # When set to a positive integer, at most this many candidates per source
    # column survive tie-break attenuation regardless of their score.
    dbxcarta_fk_attenuation_top_n: int = 0

    # Phase 3: stem suffixes for name matching (comma-separated, no spaces).
    # Default: "_id,_fk,_ref". Extend for catalogs with custom conventions.
    dbxcarta_fk_stem_suffixes: str = "_id,_fk,_ref"

    # Phase 4: confidence floor for clamp(similarity, floor, cap).
    dbxcarta_fk_semantic_floor: float = 0.80

    # Phase 4: confidence cap for clamp(similarity, floor, cap).
    dbxcarta_fk_semantic_cap: float = 0.90

    # Phase 4: confidence bonus when value-overlap corroborates.
    dbxcarta_fk_semantic_value_bonus: float = 0.05

    # Phase 4: minimum source-values-in-target ratio for corroboration.
    dbxcarta_fk_semantic_overlap_threshold: float = 0.5

    # Shared: extra type-equivalence entries (comma-separated TYPE=FAMILY pairs,
    # e.g. "NUMBER=INTEGER,TEXT=STRING"). Merged with the built-in _TYPE_EQUIV.
    dbxcarta_fk_extra_type_equiv: str = ""

    # Shared: extra PK heuristic column-name patterns (comma-separated regex
    # strings, matched case-insensitively). Columns matching any pattern are
    # classified as UNIQUE_OR_HEUR when not covered by declared PKs or the
    # built-in id / {table}_id rule.
    dbxcarta_fk_pk_extra_patterns: str = ""

    @property
    def fk_config(self) -> "FKInferenceConfig":
        """Build an FKInferenceConfig from the current Settings.

        Constructs a fresh config object each call (cheap — just a dataclass).
        Callers that need a stable reference should capture it once.
        """
        from dbxcarta.fk_config import FKInferenceConfig

        extra_type_equiv: dict[str, str] = {}
        if self.dbxcarta_fk_extra_type_equiv:
            for pair in self.dbxcarta_fk_extra_type_equiv.split(","):
                pair = pair.strip()
                if "=" in pair:
                    k, v = pair.split("=", 1)
                    extra_type_equiv[k.strip().upper()] = v.strip().upper()

        pk_extra_patterns: list[str] = []
        if self.dbxcarta_fk_pk_extra_patterns:
            pk_extra_patterns = [
                p.strip()
                for p in self.dbxcarta_fk_pk_extra_patterns.split(",")
                if p.strip()
            ]

        stem_suffixes = tuple(
            s.strip()
            for s in self.dbxcarta_fk_stem_suffixes.split(",")
            if s.strip()
        )

        return FKInferenceConfig(
            metadata_threshold=self.dbxcarta_fk_metadata_threshold,
            attenuation_exponent=self.dbxcarta_fk_attenuation_exponent,
            attenuation_top_n=(
                self.dbxcarta_fk_attenuation_top_n
                if self.dbxcarta_fk_attenuation_top_n > 0
                else None
            ),
            stem_suffixes=stem_suffixes,
            semantic_threshold=self.dbxcarta_semantic_threshold,
            semantic_floor=self.dbxcarta_fk_semantic_floor,
            semantic_cap=self.dbxcarta_fk_semantic_cap,
            semantic_value_bonus=self.dbxcarta_fk_semantic_value_bonus,
            semantic_overlap_threshold=self.dbxcarta_fk_semantic_overlap_threshold,
            extra_type_equiv=extra_type_equiv,
            pk_extra_patterns=pk_extra_patterns,
        )

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
        2. Phase 4 (semantic FK inference) requires column embeddings to be
           enabled — cosine similarity needs vectors.
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
