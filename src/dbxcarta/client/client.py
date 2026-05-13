"""DBxCarta client — Text2SQL evaluation harness.

Evaluation arms:
  - reference: executes reference_sql directly to validate the grading path.
  - no_context/schema_dump: generate SQL with ai_query, parse it, execute it
    on the warehouse, and compare the result set against reference_sql.
  - graph_rag: embed the question, retrieve graph context from Neo4j, then
    generate and grade SQL through the same execution path.
"""

from __future__ import annotations

import logging
import os
import re
import requests
from pathlib import Path
from typing import Any

from databricks.sdk import WorkspaceClient

from dbxcarta.client.compare import compare_result_sets as _compare_result_sets
from dbxcarta.client.executor import execute_sql, preflight_warehouse
from dbxcarta.client.questions import (
    Question,
    is_table_ref as _is_table_ref,
    load_questions as _load_questions,
)
from dbxcarta.client.settings import ClientSettings
from dbxcarta.client.summary import ClientRunSummary
from dbxcarta.databricks import build_workspace_client, quote_qualified_name, uc_volume_parent

logger = logging.getLogger(__name__)

_REFERENCE_ARM = "reference"
_LLM_ARMS = {"no_context", "schema_dump"}
_STAGING_ARMS = _LLM_ARMS | {"graph_rag"}

# Matches optional ```sql ... ``` or ``` ... ``` fences.
_FENCE_RE = re.compile(r"^```(?:sql)?\s*\n?(.*?)\n?```\s*$", re.DOTALL | re.IGNORECASE)
_SQL_START_RE = re.compile(r"^\s*(SELECT|WITH|INSERT|UPDATE|DELETE|CREATE|EXPLAIN)\b", re.IGNORECASE)

def _client_retrieval_table(settings: ClientSettings) -> str:
    parts = settings.dbxcarta_summary_table.split(".")
    return f"{parts[0]}.{parts[1]}.client_retrieval"


def _grade_correct(
    generated_sql: str,
    reference_sql: str,
    ws: WorkspaceClient,
    warehouse_id: str,
    timeout_sec: int,
) -> tuple[bool, str | None]:
    """Execute both SQL statements and compare result sets.

    Returns (correct, error). correct is False when either statement fails or the
    result sets do not match.
    """
    from dbxcarta.client.executor import fetch_rows

    gen_cols, gen_rows, gen_err = fetch_rows(ws, warehouse_id, generated_sql, timeout_sec)
    if gen_rows is None:
        return False, f"generated SQL failed: {gen_err}"

    ref_cols, ref_rows, ref_err = fetch_rows(ws, warehouse_id, reference_sql, timeout_sec)
    if ref_rows is None:
        return False, f"reference SQL failed: {ref_err}"

    return _compare_result_sets(gen_cols or [], gen_rows, ref_cols or [], ref_rows)


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
    try:
        parent = uc_volume_parent(summary_root)
    except ValueError as exc:
        raise RuntimeError(
            "Cannot derive client staging path from "
            f"DBXCARTA_SUMMARY_VOLUME={settings.dbxcarta_summary_volume!r}. "
            "Expected /Volumes/<cat>/<schema>/<volume>/<subdir>."
        ) from exc
    return f"{parent}/client_staging"


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


def _run_reference_arm(
    ws: WorkspaceClient,
    settings: ClientSettings,
    questions: list[Question],
    summary: ClientRunSummary,
) -> None:
    for q in questions:
        if not q.reference_sql:
            continue
        executed, non_empty, error = execute_sql(
            ws,
            settings.databricks_warehouse_id,
            q.reference_sql,
            settings.dbxcarta_client_timeout_sec,
        )
        summary.add_result(
            question_id=q.question_id,
            question=q.question,
            arm=_REFERENCE_ARM,
            sql=q.reference_sql,
            parsed=True,
            executed=executed,
            non_empty=non_empty,
            error=error,
        )


def _run_llm_arm(
    spark: Any,
    ws: WorkspaceClient,
    settings: ClientSettings,
    questions: list[Question],
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
            prompt = no_context_prompt(q.question, catalog, schemas)
        elif arm == "schema_dump":
            if schema_text is None:
                raise RuntimeError("schema_text required for schema_dump arm")
            prompt = schema_dump_prompt(q.question, catalog, schemas, schema_text)
        else:
            raise ValueError(f"Unknown LLM arm: {arm!r}")
        questions_with_prompts.append({"question_id": q.question_id, "prompt": prompt})

    responses = generate_sql_batch(
        spark,
        settings.dbxcarta_chat_endpoint,
        questions_with_prompts,
        staging_path,
        arm,
    )

    for q in questions:
        qid = q.question_id
        raw_sql, ai_error = responses.get(qid, (None, "no response"))

        cleaned_sql, parse_ok = _parse_sql(raw_sql)

        if not parse_ok:
            summary.add_result(
                question_id=qid,
                question=q.question,
                arm=arm,
                sql=raw_sql,
                parsed=False,
                executed=False,
                non_empty=False,
                error=ai_error or "response did not contain valid SQL",
            )
            continue

        assert cleaned_sql is not None
        executed, non_empty, exec_error = execute_sql(
            ws,
            settings.databricks_warehouse_id,
            cleaned_sql,
            settings.dbxcarta_client_timeout_sec,
        )
        reference_sql = q.reference_sql
        gradable = bool(reference_sql) and executed
        correct = False
        if gradable:
            assert reference_sql is not None
            correct, _ = _grade_correct(
                cleaned_sql,
                reference_sql,
                ws,
                settings.databricks_warehouse_id,
                settings.dbxcarta_client_timeout_sec,
            )
        summary.add_result(
            question_id=qid,
            question=q.question,
            arm=arm,
            sql=cleaned_sql,
            parsed=True,
            executed=executed,
            non_empty=non_empty,
            correct=correct,
            gradable=gradable,
            error=exec_error,
        )


def _embed_questions(
    ws: WorkspaceClient, endpoint: str, texts: list[str]
) -> tuple[list[list[float]] | None, str | None]:
    """Embed all questions in a single batch call.

    Returns (embeddings, error). On HTTP or network failure the first element
    is None so callers can record a per-question warning without aborting the run.
    """
    headers = ws.config.authenticate()
    try:
        resp = requests.post(
            f"{ws.config.host.rstrip('/')}/serving-endpoints/{endpoint}/invocations",
            headers=headers,
            json={"input": texts},
            timeout=60,
        )
        resp.raise_for_status()
    except requests.RequestException as exc:
        return None, str(exc)
    data = resp.json()["data"]
    data.sort(key=lambda x: x["index"])
    return [item["embedding"] for item in data], None


def _run_graph_rag_arm(
    spark: Any,
    ws: WorkspaceClient,
    settings: ClientSettings,
    questions: list[Question],
    summary: ClientRunSummary,
    staging_path: str,
) -> None:
    from dbxcarta.client.generation import generate_sql_batch
    from dbxcarta.client.graph_retriever import GraphRetriever
    from dbxcarta.client.prompt import graph_rag_prompt
    from dbxcarta.client.trace import (
        RetrievalTrace,
        chosen_schemas_from_columns,
        compute_retrieval_metrics,
        emit_retrieval_traces,
        schema_scores_from_seeds,
    )

    catalog = settings.dbxcarta_catalog
    schemas = settings.schemas_list

    texts = [q.question for q in questions]
    embeddings, embed_error = _embed_questions(ws, settings.dbxcarta_embed_endpoint, texts)
    if embeddings is None:
        for q in questions:
            summary.add_result(
                question_id=q.question_id,
                question=q.question,
                arm="graph_rag",
                parsed=False,
                executed=False,
                non_empty=False,
                error=f"embedding failed: {embed_error}",
            )
        return

    retriever = GraphRetriever(settings)
    try:
        questions_with_prompts = []
        traces: dict[str, RetrievalTrace] = {}
        for q, emb in zip(questions, embeddings):
            qid = q.question_id
            bundle = retriever.retrieve(q.question, emb)
            context_text = bundle.to_text()
            prompt = graph_rag_prompt(q.question, catalog, schemas, context_text)
            questions_with_prompts.append({"question_id": qid, "prompt": prompt})

            final_col_ids = [c.column_id for c in bundle.columns if c.column_id]
            sch_scores = schema_scores_from_seeds(
                bundle.col_seed_ids,
                bundle.col_seed_scores,
                bundle.tbl_seed_ids,
                bundle.tbl_seed_scores,
            )
            trace = RetrievalTrace(
                run_id=summary.run_id,
                question_id=qid,
                question=q.question,
                target_schema=q.schema_,
                col_seed_ids=bundle.col_seed_ids,
                col_seed_scores=bundle.col_seed_scores,
                tbl_seed_ids=bundle.tbl_seed_ids,
                tbl_seed_scores=bundle.tbl_seed_scores,
                schema_scores=sch_scores,
                chosen_schemas=(
                    bundle.selected_schemas
                    if bundle.selected_schemas
                    else chosen_schemas_from_columns(bundle.columns)
                ),
                expansion_tbl_ids=bundle.expansion_tbl_ids,
                final_col_ids=final_col_ids,
                rendered_context=context_text,
                reference_sql=q.reference_sql,
            )
            compute_retrieval_metrics(trace)
            traces[qid] = trace
    finally:
        retriever.close()

    responses = generate_sql_batch(
        spark,
        settings.dbxcarta_chat_endpoint,
        questions_with_prompts,
        staging_path,
        "graph_rag",
    )

    for q in questions:
        qid = q.question_id
        raw_sql, ai_error = responses.get(qid, (None, "no response"))
        cleaned_sql, parse_ok = _parse_sql(raw_sql)
        trace = traces[qid]

        if not parse_ok:
            trace.generated_sql = raw_sql
            trace.execution_error = ai_error or "response did not contain valid SQL"
            summary.add_result(
                question_id=qid,
                question=q.question,
                arm="graph_rag",
                sql=raw_sql,
                context_ids=trace.col_seed_ids + trace.tbl_seed_ids,
                parsed=False,
                executed=False,
                non_empty=False,
                error=ai_error or "response did not contain valid SQL",
                top1_schema_match=trace.top1_schema_match,
                schema_in_context=trace.schema_in_context,
                context_purity=trace.context_purity,
            )
            continue

        assert cleaned_sql is not None
        executed, non_empty, exec_error = execute_sql(
            ws,
            settings.databricks_warehouse_id,
            cleaned_sql,
            settings.dbxcarta_client_timeout_sec,
        )
        reference_sql = q.reference_sql
        gradable = bool(reference_sql) and executed
        correct = False
        if gradable:
            assert reference_sql is not None
            correct, _ = _grade_correct(
                cleaned_sql,
                reference_sql,
                ws,
                settings.databricks_warehouse_id,
                settings.dbxcarta_client_timeout_sec,
            )

        trace.generated_sql = cleaned_sql
        trace.parsed = True
        trace.executed = executed
        trace.correct = correct
        trace.execution_error = exec_error

        summary.add_result(
            question_id=qid,
            question=q.question,
            arm="graph_rag",
            sql=cleaned_sql,
            context_ids=trace.col_seed_ids + trace.tbl_seed_ids,
            parsed=True,
            executed=executed,
            non_empty=non_empty,
            correct=correct,
            gradable=gradable,
            error=exec_error,
            top1_schema_match=trace.top1_schema_match,
            schema_in_context=trace.schema_in_context,
            context_purity=trace.context_purity,
        )

    emit_retrieval_traces(
        spark, list(traces.values()), _client_retrieval_table(settings)
    )


def run_client() -> None:
    settings = ClientSettings()  # type: ignore[call-arg]

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    run_id = os.environ.get("DATABRICKS_JOB_RUN_ID", "local")
    ws = build_workspace_client()

    _preflight(ws, settings)

    questions = _load_questions(settings.dbxcarta_client_questions, spark)
    active_arms = settings.arms
    staging_path = _resolve_staging_path(settings) if any(
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
                    spark, ws, settings, questions, summary, arm, staging_path,
                    schema_text=schema_text if arm == "schema_dump" else None,
                )
            elif arm == "graph_rag":
                _run_graph_rag_arm(
                    spark, ws, settings, questions, summary, staging_path,
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
