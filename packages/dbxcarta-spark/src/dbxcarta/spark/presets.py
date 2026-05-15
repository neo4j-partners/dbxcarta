"""Preset protocol for dbxcarta ingest pipelines.

A preset is a single Python object that packages the dbxcarta environment
overlay for a particular data source. Downstream projects publish a preset
object in their own package and pass its import path to the dbxcarta CLI.

The required contract is `Preset` (a single `env()` method). The optional
capabilities (`ReadinessCheckable`, `QuestionsUploadable`) live in
`dbxcarta.client.presets` since they concern eval and retrieval readiness.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from dbxcarta.spark.env import EnvOverlay


@runtime_checkable
class Preset(EnvOverlay, Protocol):
    """Required structural contract for a dbxcarta preset."""

    def env(self) -> dict[str, str]:
        """Return the dbxcarta env overlay."""
        ...


def format_env(values: dict[str, str]) -> str:
    """Format an env overlay as KEY=VALUE lines in dict-insertion order."""
    return "\n".join(f"{key}={values[key]}" for key in values) + "\n"
