"""Tests for `dbxcarta.core.settings.SemanticLayerConfig`."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from dbxcarta.core import DEFAULT_EMBEDDING_ENDPOINT, SemanticLayerConfig


def test_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    for var in ("DBXCARTA_CORE_SOURCE_SCHEMAS", "DBXCARTA_CORE_EMBEDDING_ENDPOINT"):
        monkeypatch.delenv(var, raising=False)

    config = SemanticLayerConfig(source_catalog="my_catalog")

    assert config.source_catalog == "my_catalog"
    assert config.source_schemas == ""
    assert config.schema_list == []
    assert config.embedding_endpoint == DEFAULT_EMBEDDING_ENDPOINT


def test_schema_list_strips_and_splits() -> None:
    config = SemanticLayerConfig(
        source_catalog="my_catalog",
        source_schemas=" sales , inventory ,  ,hr",
    )

    assert config.schema_list == ["sales", "inventory", "hr"]


def test_rejects_unsafe_catalog_identifier() -> None:
    with pytest.raises(ValidationError):
        SemanticLayerConfig(source_catalog="my catalog; DROP TABLE x")


def test_env_prefix_loads_from_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DBXCARTA_CORE_SOURCE_CATALOG", "env_catalog")
    monkeypatch.setenv("DBXCARTA_CORE_SOURCE_SCHEMAS", "a,b")

    config = SemanticLayerConfig()  # type: ignore[call-arg]

    assert config.source_catalog == "env_catalog"
    assert config.schema_list == ["a", "b"]
