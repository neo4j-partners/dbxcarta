"""Fixtures for tests/integration/.

Live fixtures (neo4j_driver, ws, run_summary) sourced from the schema_graph
sibling conftest so integration tests share the same session-scoped connections.
"""

from __future__ import annotations

from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent.parent / ".env")

from tests.schema_graph.conftest import neo4j_driver, run_summary, ws  # noqa: F401, E402
