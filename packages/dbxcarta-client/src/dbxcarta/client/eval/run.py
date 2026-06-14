"""DBxCarta client — Text2SQL evaluation harness.

This module owns orchestration: settings, preflight, arm dispatch, and the
truncated stdout report. The per-arm logic and arm semantics (reference /
no_context / schema_dump / graph_rag) live in ``dbxcarta.client.eval.arms``.

The client is plain local Python: model calls go straight to the serving
endpoint, questions come from a local JSON file, responses are cached on local
disk, and the run prints a truncated summary. There is no Spark session, no
warehouse write, and no Delta output.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import TYPE_CHECKING

from dbxcarta.client.eval.arms import (
    _LLM_ARMS,
    _REFERENCE_ARM,
    _STAGING_ARMS,
    _ReferenceCache,
    _run_graph_rag_arm,
    _run_llm_arm,
    _run_reference_arm,
)
from dbxcarta.client.questions import (
    load_questions as _load_questions,
)
from dbxcarta.client.settings import ClientSettings
from dbxcarta.client.summary import ClientRunSummary
from dbxcarta.core.executor import preflight_warehouse
from dbxcarta.core.workspace import build_workspace_client

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient

logger = logging.getLogger(__name__)

# Arms whose primary input is the Neo4j graph from Ingest. The graph
# precondition is gated on one of these being active: a run configured with
# only no_context/reference arms legitimately never touches the graph, so
# requiring one would change behavior on a valid in-order path.
_GRAPH_ARMS = frozenset({"graph_rag", "schema_dump"})

# Local directory for the per-arm response cache. An identical rerun (same
# endpoint, arm, and ordered prompts) reuses the cached file instead of paying
# for inference again. Overridable so concurrent example runs need not collide.
_CACHE_DIR_ENV = "DBXCARTA_CLIENT_CACHE_DIR"
_DEFAULT_CACHE_DIR = ".dbxcarta_cache"


def _resolve_cache_dir() -> str:
    return os.environ.get(_CACHE_DIR_ENV, "").strip() or _DEFAULT_CACHE_DIR


def _preflight(ws: WorkspaceClient, settings: ClientSettings) -> None:
    preflight_warehouse(ws, settings.databricks_warehouse_id)

    questions_path = Path(settings.dbxcarta_client_questions)
    if not questions_path.exists():
        raise RuntimeError(f"Questions file not found: {settings.dbxcarta_client_questions}")

    active_arms = settings.arms
    if any(a in _STAGING_ARMS for a in active_arms) and not settings.dbxcarta_chat_endpoint:
        raise RuntimeError("DBXCARTA_CHAT_ENDPOINT is required for LLM arms but is not set.")

    if any(a in _GRAPH_ARMS for a in active_arms):
        _assert_graph_populated(settings)


def _assert_graph_populated(settings: ClientSettings) -> None:
    """Confirm Ingest populated the Neo4j graph before a graph-consuming arm runs.

    The Client's primary input is the semantic graph; the graph_rag and
    schema_dump arms read it. Running them before Ingest otherwise yields empty
    retrievals and meaningless scores instead of a clear error. Checks for at
    least one Table node, the unit those arms seed and dump from.
    """
    from dbxcarta.client.neo4j_utils import neo4j_credentials
    from neo4j import GraphDatabase

    uri, username, password = neo4j_credentials(settings)
    with GraphDatabase.driver(uri, auth=(username, password)) as driver:
        with driver.session() as session:
            record = session.run("MATCH (t:Table) RETURN count(t) AS c").single()
    count = record["c"] if record is not None else 0
    if count == 0:
        raise RuntimeError(
            "Neo4j graph has no Table nodes; run the Ingest job before the "
            "Client. The graph_rag and schema_dump arms read the ingested "
            "graph, so an empty graph produces empty retrievals rather than "
            "scores."
        )


def run_client() -> None:
    settings = ClientSettings()

    run_id = os.environ.get("DATABRICKS_JOB_RUN_ID", "local")
    ws = build_workspace_client()

    _preflight(ws, settings)

    questions = _load_questions(settings.dbxcarta_client_questions)
    if settings.dbxcarta_client_max_questions > 0:
        capped = settings.dbxcarta_client_max_questions
        if capped < len(questions):
            logger.info(
                "[dbxcarta_client] capping question set to first %d of %d "
                "(DBXCARTA_CLIENT_MAX_QUESTIONS)",
                capped,
                len(questions),
            )
            questions = questions[:capped]
    active_arms = settings.arms
    cache_dir = _resolve_cache_dir()

    summary = ClientRunSummary(
        run_id=run_id,
        job_name="dbxcarta_client",
        catalog=settings.dbxcarta_catalog,
        schemas=settings.schemas_list,
        arms=active_arms,
    )

    # Fetch schema dump once if needed — avoids per-question Neo4j round trips.
    schema_text: str | None = None
    if "schema_dump" in active_arms:
        from dbxcarta.client.schema_dump import fetch_schema_dump

        schema_text = fetch_schema_dump(settings)

    # Shared across arms: each question's reference SQL runs at most once.
    ref_cache = _ReferenceCache(
        ws,
        settings.databricks_warehouse_id,
        settings.dbxcarta_client_timeout_sec,
    )

    primary_error: BaseException | None = None
    try:
        for arm in active_arms:
            if arm == _REFERENCE_ARM:
                _run_reference_arm(questions, summary, ref_cache)
            elif arm in _LLM_ARMS:
                _run_llm_arm(
                    ws,
                    settings,
                    questions,
                    summary,
                    ref_cache,
                    arm,
                    cache_dir,
                    schema_text=schema_text if arm == "schema_dump" else None,
                )
            elif arm == "graph_rag":
                _run_graph_rag_arm(
                    ws,
                    settings,
                    questions,
                    summary,
                    ref_cache,
                    cache_dir,
                )
            else:
                raise ValueError(f"Unknown arm: {arm!r}")

        summary.finish(status="success")

    except Exception as exc:
        primary_error = exc
        summary.finish(status="error", error=str(exc))
        raise

    finally:
        _emit_summary(summary, primary_error=primary_error)


def _emit_summary(
    summary: ClientRunSummary,
    *,
    primary_error: BaseException | None,
) -> None:
    """Print the truncated run summary without masking an existing failure."""
    try:
        summary.emit_stdout()
    except Exception:
        if primary_error is not None:
            logger.exception("[dbxcarta_client] failed to print run summary after run failure")
            return
        raise
