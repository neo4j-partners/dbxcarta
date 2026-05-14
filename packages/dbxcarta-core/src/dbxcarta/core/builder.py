"""Backend-neutral semantic-layer builder protocol."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from dbxcarta.core.settings import SemanticLayerConfig


@runtime_checkable
class SemanticLayerBuilder(Protocol):
    """Concrete backends implement semantic-layer creation."""

    def build_semantic_layer(self, config: SemanticLayerConfig) -> None:
        """Build or refresh the semantic layer described by *config*."""
        ...
