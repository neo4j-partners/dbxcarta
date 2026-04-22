"""Retriever ABC and ContextBundle for the graph_rag arm."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field


@dataclass
class ColumnEntry:
    table_fqn: str
    column_name: str
    data_type: str
    comment: str = ""


@dataclass
class ContextBundle:
    columns: list[ColumnEntry] = field(default_factory=list)
    values: list[str] = field(default_factory=list)
    seed_ids: list[str] = field(default_factory=list)

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
            suffix = f" — {col.comment}" if col.comment else ""
            lines.append(f"  {col.column_name} ({col.data_type}){suffix}")
        if self.values:
            lines.append("")
            lines.append("Sample values: " + ", ".join(self.values[:20]))
        return "\n".join(lines)


class Retriever(ABC):
    @abstractmethod
    def retrieve(self, question: str, embedding: list[float]) -> ContextBundle:
        ...
