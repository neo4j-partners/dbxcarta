"""Preset capability protocols for dbxcarta clients.

These protocols are implemented by downstream preset packages to opt in to
readiness checking and question upload. They are checked by the dbxcarta CLI
(`dbxcarta preset --check-ready` / `--upload-questions`).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient


@dataclass(frozen=True)
class ReadinessReport:
    """Result of a preset's readiness check."""

    catalog: str
    schema: str
    present: tuple[str, ...]
    missing_required: tuple[str, ...]
    missing_optional: tuple[str, ...]

    def ok(self, *, strict_optional: bool = False) -> bool:
        if self.missing_required:
            return False
        return not (strict_optional and self.missing_optional)

    def format(self, *, strict_optional: bool = False) -> str:
        lines = [
            f"scope: {self.catalog}.{self.schema}",
            f"present expected tables: {len(self.present)}",
        ]
        if self.missing_required:
            lines.append("missing required: " + ", ".join(self.missing_required))
        else:
            lines.append("required tables: ready")
        if self.missing_optional:
            label = "missing optional"
            if not strict_optional:
                label += " (warning)"
            lines.append(f"{label}: " + ", ".join(self.missing_optional))
        else:
            lines.append("optional tables: ready")
        lines.append(
            "status: ready"
            if self.ok(strict_optional=strict_optional)
            else "status: not ready"
        )
        return "\n".join(lines)


@runtime_checkable
class ReadinessCheckable(Protocol):
    """Optional capability: a preset can report on the UC tables it expects."""

    def readiness(self, ws: "WorkspaceClient", warehouse_id: str) -> ReadinessReport:
        ...


@runtime_checkable
class QuestionsUploadable(Protocol):
    """Optional capability: a preset can ship and upload a demo question set."""

    def upload_questions(self, ws: "WorkspaceClient") -> None:
        ...
