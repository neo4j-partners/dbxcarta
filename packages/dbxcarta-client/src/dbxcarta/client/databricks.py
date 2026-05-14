"""Databricks SDK helpers for client and operational workflows."""

from __future__ import annotations

import os

from databricks.sdk import WorkspaceClient


def build_workspace_client() -> WorkspaceClient:
    """Build a WorkspaceClient from DATABRICKS_PROFILE or default SDK auth."""
    profile = os.environ.get("DATABRICKS_PROFILE")
    return WorkspaceClient(profile=profile) if profile else WorkspaceClient()
