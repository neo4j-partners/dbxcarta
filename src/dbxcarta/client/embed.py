"""Embedding utilities for the dbxcarta client."""

from __future__ import annotations

from typing import Any

from databricks.sdk import WorkspaceClient


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
        items: list[dict[str, Any]] = data["data"]
        items.sort(key=lambda x: x["index"])
        return [item["embedding"] for item in items], None
    except Exception as exc:
        return None, str(exc)
