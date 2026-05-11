"""Fixtures for tests/integration/.

This conftest holds the live `ws`, `neo4j_driver`, and `run_summary` fixtures
used by `test_semantic_search.py`. Phase 4 (worklog/cleanup-v3.md) replaces
that test with an MLflow eval and deletes this directory entirely.
"""

from __future__ import annotations

import base64
import os
from pathlib import Path
from typing import Iterator

import pytest
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent.parent / ".env")


@pytest.fixture(scope="session")
def ws():
    from dbxcarta.databricks import build_workspace_client

    return build_workspace_client()


@pytest.fixture(scope="session")
def neo4j_driver(ws) -> Iterator:
    from neo4j import GraphDatabase

    scope = os.environ["DATABRICKS_SECRET_SCOPE"]

    def _secret(key: str) -> str:
        return base64.b64decode(ws.secrets.get_secret(scope=scope, key=key).value).decode()

    driver = GraphDatabase.driver(
        _secret("NEO4J_URI"),
        auth=(_secret("NEO4J_USERNAME"), _secret("NEO4J_PASSWORD")),
    )
    yield driver
    driver.close()


@pytest.fixture(scope="session")
def run_summary(ws) -> dict:
    """Load the most recent successful run-summary JSON from the UC Volume."""
    from dbxcarta.ingest.summary import LoadSummaryError, load_summary_from_volume

    volume_path = os.environ["DBXCARTA_SUMMARY_VOLUME"]
    try:
        summary = load_summary_from_volume(ws, volume_path)
    except LoadSummaryError as e:
        pytest.skip(f"Could not load run summary: {e}")
    if summary is None:
        pytest.skip("No dbxcarta run summary found — run the job first")
    return summary
