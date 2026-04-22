"""DBxCarta client — Text2SQL evaluation harness.

Phase 1: reference arm — feeds reference_sql through the executor to validate
         the grading path end to end.
Phase 2: no_context and schema_dump arms — LLM generation via ai_query batch,
         SQL parsing, warehouse execution, and pass-rate comparison.
"""

from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any

from databricks.sdk import WorkspaceClient

from dbxcarta.client.executor import execute_sql, preflight_warehouse
from dbxcarta.client.settings import ClientSettings
from dbxcarta.client.summary import ClientRunSummary

_REFERENCE_ARM = "reference"
_LLM_ARMS = {"no_context", "schema_dump"}

# Matches optional ```sql ... ``` or ``` ... ``` fences.
_FENCE_RE = re.compile(r"^```(?:sql)?\s*\n?(.*?)\n?```\s*$", re.DOTALL | re.IGNORECASE)
_SQL_START_RE = re.compile(r"^\s*(SELECT|WITH|INSERT|UPDATE|DELETE|CREATE|EXPLAIN)\b", re.IGNORECASE)


def _load_questions(path: str) -> list[dict[str, Any]]:
    text = Path(path).read_text()
    data = json.loads(text)
    if not isinstance(data, list):
        raise ValueError(f"questions file must be a JSON array, got {type(data)}")
    return data


def _parse_sql(text: str | None) -> tuple[str | None, bool]:
    """Strip markdown fences and check for a SQL keyword.

    Returns (cleaned_sql, is_valid).
    """
    if not text:
        return None, False
    cleaned = text.strip()
    m = _FENCE_RE.match(cleaned)
    if m:
        cleaned = m.group(1).strip()
    if not _SQL_START_RE.match(cleaned):
        return cleaned or None, False
    return cleaned, True


def _resolve_staging_path(settings: ClientSettings) -> str:
    summary_root = settings.dbxcarta_summary_volume.rstrip("/")
    parts = summary_root.split("/")
    if len(parts) < 6 or parts[1] != "Volumes":
        raise RuntimeError(
            "Cannot derive client staging path from "
            f"DBXCARTA_SUMMARY_VOLUME={settings.dbxcarta_summary_volume!r}. "
            "Expected /Volumes/<cat>/<schema>/<volume>/<subdir>."
        )
    parent = "/".join(parts[:-1])
    return f"{parent}/client_staging"


def _preflight(ws: WorkspaceClient, settings: ClientSettings) -> None:
    preflight_warehouse(ws, settings.databricks_warehouse_id)

    questions_path = Path(settings.dbxcarta_client_questions)
    if not questions_path.exists():
        raise RuntimeError(
            f"Questions file not found: {settings.dbxcarta_client_questions}\n"
            "Upload it with: dbxcarta upload --data examples/client/questions/"
        )

    active_arms = settings.arms
    if any(a in _LLM_ARMS for a in active_arms) and not settings.dbxcarta_chat_endpoint:
        raise RuntimeError(
            "DBXCARTA_CHAT_ENDPOINT is required for LLM arms but is not set."
        )


def _run_reference_arm(
    ws: WorkspaceClient,
    settings: ClientSettings,
    questions: list[dict[str, Any]],
    summary: ClientRunSummary,
) -> None:
    for q in questions:
        if not q.get("reference_sql"):
            continue
        executed, non_empty, error = execute_sql(
            ws,
            settings.databricks_warehouse_id,
            q["reference_sql"],
            settings.dbxcarta_client_timeout_sec,
        )
        summary.add_result(
            question_id=q["question_id"],
            question=q["question"],
            arm=_REFERENCE_ARM,
            sql=q["reference_sql"],
            parsed=True,
            executed=executed,
            non_empty=non_empty,
            error=error,
        )


def _run_llm_arm(
    spark: Any,
    ws: WorkspaceClient,
    settings: ClientSettings,
    questions: list[dict[str, Any]],
    summary: ClientRunSummary,
    arm: str,
    staging_path: str,
    schema_text: str | None = None,
) -> None:
    from dbxcarta.client.generation import generate_sql_batch
    from dbxcarta.client.prompt import no_context_prompt, schema_dump_prompt

    catalog = settings.dbxcarta_catalog
    schemas = settings.schemas_list

    questions_with_prompts = []
    for q in questions:
        if arm == "no_context":
            prompt = no_context_prompt(q["question"], catalog, schemas)
        elif arm == "schema_dump":
            if schema_text is None:
                raise RuntimeError("schema_text required for schema_dump arm")
            prompt = schema_dump_prompt(q["question"], catalog, schemas, schema_text)
        else:
            raise ValueError(f"Unknown LLM arm: {arm!r}")
        questions_with_prompts.append({"question_id": q["question_id"], "prompt": prompt})

    responses = generate_sql_batch(
        spark,
        settings.dbxcarta_chat_endpoint,
        questions_with_prompts,
        staging_path,
        arm,
    )

    for q in questions:
        qid = q["question_id"]
        raw_sql, ai_error = responses.get(qid, (None, "no response"))

        cleaned_sql, parse_ok = _parse_sql(raw_sql)

        if not parse_ok:
            summary.add_result(
                question_id=qid,
                question=q["question"],
                arm=arm,
                sql=raw_sql,
                parsed=False,
                executed=False,
                non_empty=False,
                error=ai_error or "response did not contain valid SQL",
            )
            continue

        executed, non_empty, exec_error = execute_sql(
            ws,
            settings.databricks_warehouse_id,
            cleaned_sql,
            settings.dbxcarta_client_timeout_sec,
        )
        summary.add_result(
            question_id=qid,
            question=q["question"],
            arm=arm,
            sql=cleaned_sql,
            parsed=True,
            executed=executed,
            non_empty=non_empty,
            error=exec_error,
        )


def run_client() -> None:
    settings = ClientSettings()

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    run_id = os.environ.get("DATABRICKS_JOB_RUN_ID", "local")
    ws = WorkspaceClient()

    _preflight(ws, settings)

    questions = _load_questions(settings.dbxcarta_client_questions)
    active_arms = settings.arms
    staging_path = _resolve_staging_path(settings) if any(
        a in _LLM_ARMS for a in active_arms
    ) else ""

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

    try:
        for arm in active_arms:
            if arm == _REFERENCE_ARM:
                _run_reference_arm(ws, settings, questions, summary)
            elif arm in _LLM_ARMS:
                _run_llm_arm(
                    spark, ws, settings, questions, summary, arm, staging_path,
                    schema_text=schema_text if arm == "schema_dump" else None,
                )
            elif arm == "graph_rag":
                print("[dbxcarta_client] graph_rag arm skipped — not yet implemented (Phase 3)")
            else:
                raise ValueError(f"Unknown arm: {arm!r}")

        summary.finish(status="success")

    except Exception as exc:
        summary.finish(status="error", error=str(exc))
        raise

    finally:
        summary.emit(spark, settings.dbxcarta_summary_volume, settings.dbxcarta_summary_table)
