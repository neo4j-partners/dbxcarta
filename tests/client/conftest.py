"""Fixtures for tests/client/.

Integration tests reuse neo4j_driver, ws, and run_summary from the
schema_graph sibling conftest via direct import.
"""

from __future__ import annotations

from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent.parent / ".env")

from tests.schema_graph.conftest import neo4j_driver, run_summary, ws  # noqa: F401, E402
