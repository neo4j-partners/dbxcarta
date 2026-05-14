"""Core semantic-layer configuration.

Core settings describe the semantic graph contract without constructing any
runtime objects or importing a concrete backend. Spark, client, and preset
packages compose this model with their own settings at their application
boundaries.
"""

from __future__ import annotations

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from dbxcarta.core.contract import DEFAULT_EMBEDDING_ENDPOINT
from dbxcarta.core.databricks import validate_identifier


class SemanticLayerConfig(BaseSettings):
    """Backend-neutral semantic-layer configuration."""

    source_catalog: str
    source_schemas: str = ""
    embedding_endpoint: str = DEFAULT_EMBEDDING_ENDPOINT

    model_config = SettingsConfigDict(
        env_prefix="DBXCARTA_CORE_",
        env_file=".env",
        extra="ignore",
    )

    @field_validator("source_catalog")
    @classmethod
    def _validate_source_catalog(cls, value: str) -> str:
        return validate_identifier(value, label="source catalog")

    @property
    def schema_list(self) -> list[str]:
        return [name.strip() for name in self.source_schemas.split(",") if name.strip()]
