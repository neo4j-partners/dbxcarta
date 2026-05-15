"""Embedding utilities for the dbxcarta client."""

from __future__ import annotations

import logging
from typing import Any

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import DatabricksError

logger = logging.getLogger(__name__)


def embed_questions(
    ws: WorkspaceClient,
    endpoint: str,
    texts: list[str],
) -> tuple[list[list[float]] | None, str | None]:
    """Embed a list of texts in a single batch call.

    Returns (embeddings, error). On failure the first element is None so
    callers can record a warning without aborting the run.
    """
    try:
        data = ws.api_client.do(
            "POST",
            f"/serving-endpoints/{endpoint}/invocations",
            body={"input": texts},
        )
        if not isinstance(data, dict):
            actual_type = type(data).__name__
            raise TypeError(f"embedding response must be a dict, got {actual_type}")
        raw_items = data.get("data")
        if not isinstance(raw_items, list):
            raise TypeError("embedding response missing list field 'data'")

        items: list[dict[str, Any]] = []
        for item in raw_items:
            if not isinstance(item, dict):
                raise TypeError("embedding response 'data' items must be objects")
            items.append(item)
        items.sort(key=lambda x: x["index"])
        return [item["embedding"] for item in items], None
    except (DatabricksError, KeyError, TypeError) as exc:
        logger.warning("embedding call to %s failed: %s", endpoint, exc)
        return None, str(exc)
