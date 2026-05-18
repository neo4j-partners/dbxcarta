"""Preset protocols for dbxcarta ingest pipelines and CLI integrations.

A preset is a single Python object that packages the dbxcarta environment
overlay for a particular data source. Downstream projects publish a preset
object in their own package and pass its import path to the dbxcarta CLI.

The required contract is `Preset` (a single `env()` method). The optional
capabilities let the operational CLI check upstream readiness and upload
client evaluation questions without requiring the Spark package to depend on
the client package.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol, runtime_checkable

from dbxcarta.spark.env import EnvOverlay

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient


@runtime_checkable
class Preset(EnvOverlay, Protocol):
    """Required structural contract for a dbxcarta preset."""

    def env(self) -> dict[str, str]:
        """Return the dbxcarta env overlay."""
        ...


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


def format_env(values: dict[str, str]) -> str:
    """Format an env overlay as KEY=VALUE lines in dict-insertion order."""
    return "\n".join(f"{key}={values[key]}" for key in values) + "\n"
