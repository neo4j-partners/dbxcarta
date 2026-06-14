from __future__ import annotations

from dbxcarta.core.catalogs import resolve_catalogs
from dbxcarta.core.identifiers import (
    parse_volume_path,
    validate_identifier,
    validate_serving_endpoint_name,
)
from pydantic import field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class ClientSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # Shared with server
    dbxcarta_catalog: str
    # Comma-separated multi-catalog list, mirroring the server's
    # DBXCARTA_CATALOGS. Blank falls back to the single dbxcarta_catalog so
    # single-catalog examples are unaffected. Used to make the schema_dump
    # baseline cover every ingested catalog (bronze/silver/gold), not just the
    # primary anchor; the graph_rag arm is already catalog-qualified per node.
    dbxcarta_catalogs: str = ""
    dbxcarta_schemas: str = ""
    databricks_warehouse_id: str
    # Required, per-integration: set in each examples/<name>/dbxcarta-overlay.env
    # (dbxcarta-neo4j-<example>). No default, so a run without a selected overlay
    # fails at config load instead of silently reading an unprovisioned scope.
    databricks_secret_scope: str
    databricks_volume_path: str

    # Client-specific — generation
    dbxcarta_chat_endpoint: str = ""
    dbxcarta_embedding_endpoint: str = "databricks-gte-large-en"
    dbxcarta_embed_endpoint: str = ""  # defaults to dbxcarta_embedding_endpoint

    # Client-specific — runtime
    # Local path to the questions JSON the client reads directly. The client
    # runs locally, so this is a required repo-relative (or absolute) file path,
    # set by the example overlay; a blank value fails preflight loudly.
    dbxcarta_client_questions: str = ""
    # 0 = run the full question set; set to a small N (e.g. 5) to evaluate only
    # the first N questions as a quick post-ingest smoke check.
    dbxcarta_client_max_questions: int = 0
    dbxcarta_client_arms: str = "no_context,schema_dump,graph_rag"
    dbxcarta_client_top_k: int = 5
    # Force re-inference even when a matching local cache file exists.
    # The cache keys on (endpoint, arm, ordered question prompts); set this
    # when the endpoint's underlying model changed but the prompts did not.
    dbxcarta_client_refresh: bool = False
    dbxcarta_client_timeout_sec: int = 30
    # Tail-truncate the schema_dump prompt. 0 = no limit. Default 7500 chars
    # (~2k tokens at ~3.7 chars/token for dense schema text) makes schema_dump
    # a token-matched fairness baseline against graph_rag's retrieved context
    # rather than a full multi-catalog dump.
    dbxcarta_schema_dump_max_chars: int = 7500
    # 0 = no cap; set to 10-20 for dense single-schema fixtures to prevent
    # REFERENCES expansion from returning dozens of neighbor tables
    dbxcarta_client_max_expansion_tables: int = 0
    # Blend ratio for combined FK-confidence + cosine-similarity re-ranking.
    # 0.0 = pure cosine, 1.0 = pure FK confidence, 0.5 = equal weight.
    dbxcarta_client_expansion_alpha: float = 0.5

    # REFERENCES-edge confidence filter. Inclusive `>=` comparison; edges with
    # no confidence property are treated as 1.0 via COALESCE so they are never
    # silently dropped.
    dbxcarta_confidence_threshold: float = 0.8
    dbxcarta_inject_criteria: bool = True

    @field_validator("dbxcarta_catalog")
    @classmethod
    def _validate_catalog(cls, v: str) -> str:
        return validate_identifier(v)

    @field_validator("dbxcarta_catalogs")
    @classmethod
    def _validate_catalogs(cls, v: str) -> str:
        """Validate the multi-catalog list through the shared catalog rule.

        Routes through :func:`resolve_catalogs` so the client accepts the same
        ``catalog`` / ``catalog:layer`` list the pipeline does, stripping the
        ``:layer`` suffix identically. A blank or separator-only list is the
        single-catalog fallback and is left untouched, exactly as before.
        """
        names = [part.split(":", 1)[0].strip() for part in v.split(",")]
        if any(names):
            resolve_catalogs("", v)
        return v

    @field_validator("databricks_volume_path")
    @classmethod
    def _validate_volume_root(cls, v: str) -> str:
        # Validate through the shared core rule (exactly /Volumes/<cat>/<schema>/
        # <volume>, each part a safe identifier) rather than re-deriving it here.
        parse_volume_path(v)
        return v.rstrip("/")

    @field_validator(
        "dbxcarta_chat_endpoint",
        "dbxcarta_embedding_endpoint",
        "dbxcarta_embed_endpoint",
    )
    @classmethod
    def _validate_serving_endpoints(cls, v: str) -> str:
        if not v.strip():
            return ""
        return validate_serving_endpoint_name(v.strip())

    @model_validator(mode="after")
    def _resolve_defaults(self) -> ClientSettings:
        # The embed endpoint defaults to the embedding endpoint. The client runs
        # locally and persists nothing remote, so there are no ops-Volume sinks
        # to derive here: the questions path is a local file set by the overlay.
        if not self.dbxcarta_embed_endpoint:
            self.dbxcarta_embed_endpoint = self.dbxcarta_embedding_endpoint
        return self

    @property
    def resolved_catalogs(self) -> list[str]:
        """Catalogs the graph spans, order-preserving and de-duplicated.

        Delegates to the shared :func:`resolve_catalogs` so the client strips
        the ``:layer`` suffix and falls back to the single ``dbxcarta_catalog``
        the same way the pipeline does. This is the fix that unblocks a
        ``catalog:layer`` client run.
        """
        return resolve_catalogs(self.dbxcarta_catalog, self.dbxcarta_catalogs)

    @property
    def schemas_list(self) -> list[str]:
        return [s.strip() for s in self.dbxcarta_schemas.split(",") if s.strip()]

    @property
    def arms(self) -> list[str]:
        return [a.strip() for a in self.dbxcarta_client_arms.split(",") if a.strip()]
