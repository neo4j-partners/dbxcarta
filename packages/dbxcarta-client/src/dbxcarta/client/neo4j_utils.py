"""Shared Neo4j credential helper for client modules."""

from __future__ import annotations

import os

from dbxcarta.client.settings import ClientSettings


def neo4j_credentials(settings: ClientSettings) -> tuple[str, str, str]:
    """Return (uri, username, password) from secret scope or env vars."""
    try:
        from databricks.sdk.runtime import dbutils

        scope = settings.databricks_secret_scope
        return (
            dbutils.secrets.get(scope=scope, key="NEO4J_URI"),
            dbutils.secrets.get(scope=scope, key="NEO4J_USERNAME"),
            dbutils.secrets.get(scope=scope, key="NEO4J_PASSWORD"),
        )
    except Exception:
        return (
            os.environ["NEO4J_URI"],
            os.environ["NEO4J_USERNAME"],
            os.environ["NEO4J_PASSWORD"],
        )
