"""Retriever ABC and ContextBundle for the graph_rag arm."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field

_PER_COLUMN_SAMPLE_LIMIT = 10


@dataclass
class ColumnEntry:
    table_fqn: str
    column_name: str
    data_type: str
    comment: str = ""
    column_id: str = ""


@dataclass
class ContextBundle:
    columns: list[ColumnEntry] = field(default_factory=list)
    values: dict[str, list[str]] = field(default_factory=dict)
    seed_ids: list[str] = field(default_factory=list)
    criteria: list[str] = field(default_factory=list)
    # Phase E diagnostics — populated by GraphRetriever, empty for other arms.
    col_seed_ids: list[str] = field(default_factory=list)
    col_seed_scores: list[float] = field(default_factory=list)
    tbl_seed_ids: list[str] = field(default_factory=list)
    tbl_seed_scores: list[float] = field(default_factory=list)
    expansion_tbl_ids: list[str] = field(default_factory=list)

    def to_text(self) -> str:
        if not self.columns:
            return ""
        lines: list[str] = []
        current_table: str | None = None
        for col in self.columns:
            if col.table_fqn != current_table:
                if current_table is not None:
                    lines.append("")
                lines.append(f"Table: {col.table_fqn}")
                current_table = col.table_fqn
            parts: list[str] = []
            if col.comment:
                parts.append(col.comment)
            samples = self.values.get(col.column_id, []) if col.column_id else []
            if samples:
                joined = ", ".join(samples[:_PER_COLUMN_SAMPLE_LIMIT])
                parts.append(f"Sample values: {joined}")
            suffix = " — " + ". ".join(parts) if parts else ""
            lines.append(f"  {col.column_name} ({col.data_type}){suffix}")
        if self.criteria:
            lines.append("")
            lines.append("Join predicates:")
            for predicate in self.criteria:
                lines.append(f"  {predicate}")
        return "\n".join(lines)


class Retriever(ABC):
    @abstractmethod
    def retrieve(self, question: str, embedding: list[float]) -> ContextBundle:
        ...
