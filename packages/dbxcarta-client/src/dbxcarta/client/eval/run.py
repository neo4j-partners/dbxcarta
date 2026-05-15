"""DBxCarta client — Text2SQL evaluation harness.

This module owns orchestration: settings, preflight, arm dispatch, and summary
emission. The per-arm logic and arm semantics (reference / no_context /
schema_dump / graph_rag) live in ``dbxcarta.client.eval.arms``.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any

from databricks.sdk import WorkspaceClient

from dbxcarta.client.databricks import (
    build_workspace_client,
    quote_qualified_name,
    split_qualified_name,
)
from dbxcarta.client.eval.arms import (
    _LLM_ARMS,
    _REFERENCE_ARM,
    _STAGING_ARMS,
    _run_graph_rag_arm,
    _run_llm_arm,
    _run_reference_arm,
)
from dbxcarta.client.executor import preflight_warehouse
from dbxcarta.client.questions import (
    is_table_ref as _is_table_ref,
    load_questions as _load_questions,
)
from dbxcarta.client.settings import ClientSettings
from dbxcarta.client.summary import ClientRunSummary

logger = logging.getLogger(__name__)


def _resolve_staging_table(settings: ClientSettings) -> str:
    parts = split_qualified_name(
        settings.dbxcarta_summary_table,
        expected_parts=3,
        label="summary table",
    )
    return f"{parts[0]}.{parts[1]}.client_staging"


def _preflight(ws: WorkspaceClient, settings: ClientSettings) -> None:
    preflight_warehouse(ws, settings.databricks_warehouse_id)

    source = settings.dbxcarta_client_questions
    if not _is_table_ref(source):
        questions_path = Path(source)
        if not questions_path.exists():
            raise RuntimeError(
                f"Questions file not found: {source}\n"
                "Upload it with: dbxcarta upload --data examples/client/questions/"
            )

    active_arms = settings.arms
    if any(a in _STAGING_ARMS for a in active_arms) and not settings.dbxcarta_chat_endpoint:
        raise RuntimeError(
            "DBXCARTA_CHAT_ENDPOINT is required for LLM arms but is not set."
        )


def run_client() -> None:
    settings = ClientSettings()

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    run_id = os.environ.get("DATABRICKS_JOB_RUN_ID", "local")
    ws = build_workspace_client()

    _preflight(ws, settings)

    questions = _load_questions(settings.dbxcarta_client_questions, spark)
    active_arms = settings.arms
    staging_table = _resolve_staging_table(settings) if any(
        a in _STAGING_ARMS for a in active_arms
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

    primary_error: BaseException | None = None
    try:
        for arm in active_arms:
            if arm == _REFERENCE_ARM:
                _run_reference_arm(ws, settings, questions, summary)
            elif arm in _LLM_ARMS:
                _run_llm_arm(
                    spark, ws, settings, questions, summary, arm, staging_table,
                    schema_text=schema_text if arm == "schema_dump" else None,
                )
            elif arm == "graph_rag":
                _run_graph_rag_arm(
                    spark, ws, settings, questions, summary, staging_table,
                )
            else:
                raise ValueError(f"Unknown arm: {arm!r}")

        summary.finish(status="success")

    except Exception as exc:
        primary_error = exc
        summary.finish(status="error", error=str(exc))
        raise

    finally:
        _emit_summary(
            summary,
            spark,
            settings.dbxcarta_summary_volume,
            settings.dbxcarta_summary_table,
            primary_error=primary_error,
        )


def _emit_summary(
    summary: ClientRunSummary,
    spark: Any,
    volume_path: str,
    table_name: str,
    *,
    primary_error: BaseException | None,
) -> None:
    """Emit the summary without masking an existing client-run failure."""
    try:
        summary.emit(spark, volume_path, table_name)
    except Exception:
        if primary_error is not None:
            logger.exception(
                "[dbxcarta_client] failed to emit run summary after run failure"
            )
            return
        raise


def manage_questions(spark: Any, settings: ClientSettings, questions_path: str) -> None:
    """Write a questions JSON file to a managed Delta table alongside run_summary.

    The target table is derived from dbxcarta_summary_table: same catalog and
    schema, table name client_questions. Overwrites existing data so the table
    stays in sync with the checked-in source file.
    """
    from pyspark.sql.types import StringType, StructField, StructType

    parts = settings.dbxcarta_summary_table.split(".")
    if len(parts) != 3:
        raise RuntimeError(
            f"Cannot derive questions table from "
            f"DBXCARTA_SUMMARY_TABLE={settings.dbxcarta_summary_table!r}. "
            "Expected catalog.schema.table."
        )
    target_table = f"{parts[0]}.{parts[1]}.client_questions"

    questions = _load_questions(questions_path)

    schema = StructType([
        StructField("question_id", StringType(), nullable=False),
        StructField("question", StringType()),
        StructField("notes", StringType()),
        StructField("reference_sql", StringType()),
        StructField("schema", StringType()),
    ])
    rows = [
        (
            q.question_id,
            q.question,
            q.notes,
            q.reference_sql,
            q.schema_,
        )
        for q in questions
    ]
    quoted = quote_qualified_name(target_table, expected_parts=3)
    (
        spark.createDataFrame(rows, schema=schema)
        .write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(quoted)
    )
