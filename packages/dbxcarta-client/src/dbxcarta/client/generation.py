"""Batch SQL generation via local serving-endpoint calls with a local cache.

Each question's prompt is sent straight to the chat serving endpoint with the
operator's normal Databricks credentials (see ``client/local_generation.py``),
the same plain-web-call pattern ``client/embed.py`` uses for embeddings. No
cluster, no Spark, and no remote staging table: responses are cached in a small
local JSON file keyed by an input hash so an identical rerun skips inference.
"""

from __future__ import annotations

import hashlib
import json
import logging
from pathlib import Path
from typing import TYPE_CHECKING

from dbxcarta.client.local_generation import LocalGenerationError, generate_sql_local
from dbxcarta.core.identifiers import validate_serving_endpoint_name

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient

logger = logging.getLogger(__name__)


def _input_hash(endpoint: str, arm: str, questions_with_prompts: list[dict]) -> str:
    """Stable hash of everything that determines the model output.

    Question order is preserved (not sorted) because it is deterministic from
    the loaded question set; any change to a prompt, the endpoint name, the
    arm, or the set/order of questions changes the hash and invalidates the
    cache by construction.
    """
    payload = json.dumps(
        {
            "endpoint": endpoint,
            "arm": arm,
            "items": [[q["question_id"], q["prompt"]] for q in questions_with_prompts],
        },
        ensure_ascii=False,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _cache_file(cache_dir: str, arm: str) -> Path:
    return Path(cache_dir) / f"{arm}.json"


def _read_cached(
    cache_dir: str,
    arm: str,
    want_hash: str,
    want_ids: set[str],
) -> dict[str, tuple[str | None, str | None]] | None:
    """Return prior responses if the local cache file matches this exact input.

    A miss (file absent, unreadable, different input hash, or a different
    question set) returns None so the caller re-infers.
    """
    try:
        payload = json.loads(_cache_file(cache_dir, arm).read_text())
    except (OSError, ValueError):
        return None
    if not isinstance(payload, dict) or payload.get("input_hash") != want_hash:
        return None
    results = payload.get("results")
    if not isinstance(results, dict) or set(results) != want_ids:
        return None
    return {qid: (entry.get("sql"), entry.get("error")) for qid, entry in results.items()}


def _write_cache(
    cache_dir: str,
    arm: str,
    want_hash: str,
    results: dict[str, tuple[str | None, str | None]],
) -> None:
    path = _cache_file(cache_dir, arm)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "input_hash": want_hash,
        "results": {qid: {"sql": sql, "error": error} for qid, (sql, error) in results.items()},
    }
    path.write_text(json.dumps(payload, indent=2))


def generate_sql_batch(
    ws: WorkspaceClient,
    endpoint: str,
    questions_with_prompts: list[dict],
    cache_dir: str,
    arm: str,
    *,
    refresh: bool = False,
) -> dict[str, tuple[str | None, str | None]]:
    """Generate SQL for every question with one serving-endpoint call each.

    *questions_with_prompts* is a list of dicts with keys ``question_id`` and
    ``prompt``. Returns a mapping from question_id to ``(sql_text,
    error_message)``; on a failed call the SQL is None and the error is the
    failure message, matching the prior per-row ai_query contract.

    Results are cached in ``<cache_dir>/<arm>.json`` keyed by an input hash; an
    identical re-run (same endpoint, arm, and question prompts) reuses the
    cached responses without calling the endpoint. Pass ``refresh=True`` to
    force re-inference (e.g. the endpoint's model changed but prompts did not).
    """
    validate_serving_endpoint_name(endpoint)

    want_hash = _input_hash(endpoint, arm, questions_with_prompts)
    want_ids = {q["question_id"] for q in questions_with_prompts}

    if not refresh:
        cached = _read_cached(cache_dir, arm, want_hash, want_ids)
        if cached is not None:
            logger.info(
                "[dbxcarta] %s: cache hit, skipping generation (%d questions)",
                arm,
                len(want_ids),
            )
            return cached

    logger.info(
        "[dbxcarta] %s: generating SQL for %d questions",
        arm,
        len(want_ids),
    )
    results: dict[str, tuple[str | None, str | None]] = {}
    for q in questions_with_prompts:
        qid = q["question_id"]
        try:
            results[qid] = (generate_sql_local(ws, endpoint, q["prompt"]), None)
        except LocalGenerationError as exc:
            results[qid] = (None, str(exc))

    _write_cache(cache_dir, arm, want_hash, results)
    return results
