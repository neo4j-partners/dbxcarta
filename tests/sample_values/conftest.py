"""Shared fixtures for the sample_values pytest suite.

Requires:
  - DATABRICKS_PROFILE and DATABRICKS_SECRET_SCOPE in .env (for Neo4j credentials)
  - DBXCARTA_SUMMARY_VOLUME in .env (to find the latest run-summary JSON)
  - DATABRICKS_WAREHOUSE_ID in .env (for run_summary Delta assertion)
  - A completed sample_values job run against the target catalog

Run after a job submit:
  uv run pytest tests/sample_values
"""

import base64
import json
import os
from pathlib import Path
from typing import Iterator

import pytest
from databricks.sdk import WorkspaceClient
from dotenv import load_dotenv
from neo4j import Driver, GraphDatabase

load_dotenv(Path(__file__).parent.parent.parent / ".env")


@pytest.fixture(scope="session")
def ws() -> WorkspaceClient:
    return WorkspaceClient(profile=os.environ.get("DATABRICKS_PROFILE"))


@pytest.fixture(scope="session")
def neo4j_driver(ws: WorkspaceClient) -> Iterator[Driver]:
    scope = os.environ["DATABRICKS_SECRET_SCOPE"]

    def _secret(key: str) -> str:
        return base64.b64decode(ws.secrets.get_secret(scope=scope, key=key).value).decode()

    driver = GraphDatabase.driver(_secret("uri"), auth=(_secret("username"), _secret("password")))
    yield driver
    driver.close()


@pytest.fixture(scope="session")
def run_summary(ws: WorkspaceClient) -> dict:
    """Load the most recent sample_values run-summary JSON from the UC Volume."""
    volume_path = os.environ["DBXCARTA_SUMMARY_VOLUME"]
    entries = list(ws.files.list_directory_contents(directory_path=volume_path))
    files = [e for e in entries if e.name.startswith("dbxcarta_") and e.name.endswith(".json")]
    if not files:
        pytest.skip("No dbxcarta run summary found — run the job first")
    latest = max(files, key=lambda e: e.name)
    content = ws.files.download(file_path=f"{volume_path}/{latest.name}").contents.read()
    return json.loads(content)
