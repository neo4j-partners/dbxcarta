"""Backend-neutral semantic-layer builder protocol and result type."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol, runtime_checkable

from dbxcarta.core.settings import SemanticLayerConfig


@dataclass(frozen=True)
class SemanticLayerResult:
    """Backend-neutral summary of a semantic-layer build.

    Counts use the `NodeLabel.value` / `RelType.value` strings so the dataclass
    stays free of backend imports. A backend reports the totals it actually
    wrote; missing labels or relationship types mean "not produced".
    """

    run_id: str
    contract_version: str
    catalog: str
    schemas: tuple[str, ...] = ()
    node_counts: dict[str, int] = field(default_factory=dict)
    edge_counts: dict[str, int] = field(default_factory=dict)


@runtime_checkable
class SemanticLayerBuilder(Protocol):
    """Concrete backends implement semantic-layer creation."""

    def build_semantic_layer(self, config: SemanticLayerConfig) -> SemanticLayerResult:
        """Build or refresh the semantic layer described by *config*."""
        ...
