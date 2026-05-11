from __future__ import annotations

from pydantic import model_validator
from pydantic import field_validator
from pydantic_settings import BaseSettings

from dbxcarta.databricks import (
    split_qualified_name,
    validate_identifier,
    validate_serving_endpoint_name,
    validate_uc_volume_subpath,
)


class ClientSettings(BaseSettings):
    # Shared with server
    dbxcarta_catalog: str
    dbxcarta_schemas: str = ""
    databricks_warehouse_id: str
    databricks_secret_scope: str = "dbxcarta-neo4j"
    dbxcarta_summary_volume: str
    dbxcarta_summary_table: str
    databricks_volume_path: str

    # Client-specific — generation
    dbxcarta_chat_endpoint: str = ""
    dbxcarta_embedding_endpoint: str = "databricks-gte-large-en"
    dbxcarta_embed_endpoint: str = ""  # defaults to dbxcarta_embedding_endpoint

    # Client-specific — runtime
    dbxcarta_client_questions: str = ""  # defaults to {volume_path}/questions.json
    dbxcarta_client_arms: str = "no_context,schema_dump,graph_rag"
    dbxcarta_client_top_k: int = 5
    dbxcarta_client_timeout_sec: int = 30

    # REFERENCES-edge confidence filter. Inclusive `>=` comparison; edges with
    # no confidence property are treated as 1.0 via COALESCE so they are never
    # silently dropped.
    dbxcarta_confidence_threshold: float = 0.8
    dbxcarta_inject_criteria: bool = True

    @field_validator("dbxcarta_catalog")
    @classmethod
    def _validate_catalog(cls, v: str) -> str:
        return validate_identifier(v)

    @field_validator("dbxcarta_summary_table")
    @classmethod
    def _validate_summary_table(cls, v: str) -> str:
        split_qualified_name(v, expected_parts=3, label="summary table")
        return v

    @field_validator("dbxcarta_summary_volume")
    @classmethod
    def _validate_summary_volume(cls, v: str) -> str:
        return validate_uc_volume_subpath(v, label="DBXCARTA_SUMMARY_VOLUME")

    @field_validator("databricks_volume_path")
    @classmethod
    def _validate_volume_root(cls, v: str) -> str:
        parts = v.rstrip("/").lstrip("/").split("/")
        if len(parts) != 4 or parts[0] != "Volumes":
            raise ValueError(
                "DATABRICKS_VOLUME_PATH must be /Volumes/<catalog>/<schema>/<volume>"
            )
        for part in parts[1:]:
            validate_identifier(part, label="volume path part")
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
        if not self.dbxcarta_embed_endpoint:
            self.dbxcarta_embed_endpoint = self.dbxcarta_embedding_endpoint
        if not self.dbxcarta_client_questions:
            self.dbxcarta_client_questions = (
                f"{self.databricks_volume_path}/questions.json"
            )
        return self

    @property
    def schemas_list(self) -> list[str]:
        return [s.strip() for s in self.dbxcarta_schemas.split(",") if s.strip()]

    @property
    def arms(self) -> list[str]:
        return [a.strip() for a in self.dbxcarta_client_arms.split(",") if a.strip()]

    model_config = {"env_file": ".env", "extra": "ignore"}
