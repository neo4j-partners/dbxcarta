"""Preset protocol for dbxcarta.

A preset is a single Python object that packages the dbxcarta environment
overlay for a particular consumer plus optional helpers for checking that the
consumer's Unity Catalog data is ready and for uploading the consumer's
demo-question file.

dbxcarta core ships no preset implementations. Downstream projects publish a
preset object in their own package and pass its import path
(`some_pkg.some_module:preset`) to the dbxcarta CLI.

The required contract is `Preset` (a single `env()` method). The optional
capabilities are separate `runtime_checkable` protocols (`ReadinessCheckable`,
`QuestionsUploadable`) so a preset can opt in to either, both, or neither
without ever needing to define a method that raises NotImplementedError.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol, runtime_checkable

from dbxcarta.core import EnvOverlay

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient


@dataclass(frozen=True)
class ReadinessReport:
    """Result of a preset's readiness check.

    Required and optional tables are kept distinct so a preset can model
    progressive enrichment: required tables block ingest readiness, optional
    tables only fail readiness when the caller passes `strict_optional=True`.

    `present` is the subset of expected tables that were observed and is used
    only by `format()`; readiness logic is driven by `missing_required` and,
    when strict, `missing_optional`.
    """

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
class Preset(EnvOverlay, Protocol):
    """Required structural contract for a dbxcarta preset.

    A preset must implement `env()` returning the dbxcarta environment overlay.
    Optional capabilities are declared by separately satisfying
    `ReadinessCheckable` or `QuestionsUploadable`.
    """

    def env(self) -> dict[str, str]:
        """Return the dbxcarta env overlay (keys are `DBXCARTA_*` / `DATABRICKS_*`)."""
        ...


@runtime_checkable
class ReadinessCheckable(Protocol):
    """Optional capability: a preset can report on the UC tables it expects."""

    def readiness(
        self, ws: "WorkspaceClient", warehouse_id: str
    ) -> ReadinessReport:
        """Check that the preset's Unity Catalog tables are present."""
        ...


@runtime_checkable
class QuestionsUploadable(Protocol):
    """Optional capability: a preset can ship and upload a demo question set."""

    def upload_questions(self, ws: "WorkspaceClient") -> None:
        """Upload the preset's demo question file to `DBXCARTA_CLIENT_QUESTIONS`."""
        ...


def format_env(values: dict[str, str]) -> str:
    """Format an env overlay as KEY=VALUE lines in dict-insertion order.

    Iteration follows the order the preset returned keys in. Callers that want
    sorted output should sort before passing.
    """
    return "\n".join(f"{key}={values[key]}" for key in values) + "\n"
