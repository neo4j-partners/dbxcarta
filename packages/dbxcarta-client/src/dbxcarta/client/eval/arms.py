"""Per-arm evaluation runners for the Text2SQL harness.

Each ``_run_*_arm`` function evaluates one arm against the question set and
records results on the shared ``ClientRunSummary``. ``run.py`` owns the
orchestration (settings, preflight, summary emission); this module owns the
arm-specific generation/execution/grading logic.
"""

from __future__ import annotations

from typing import Any

from databricks.sdk import WorkspaceClient

from dbxcarta.client.compare import compare_result_sets as _compare_result_sets
from dbxcarta.client.embed import embed_questions as _embed_questions
from dbxcarta.client.executor import execute_sql
from dbxcarta.client.questions import Question
from dbxcarta.client.settings import ClientSettings
from dbxcarta.client.sql import parse_sql as _parse_sql
from dbxcarta.client.summary import ClientRunSummary

_REFERENCE_ARM = "reference"
_LLM_ARMS = {"no_context", "schema_dump"}
_STAGING_ARMS = _LLM_ARMS | {"graph_rag"}


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
    staging_table: str,
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
        staging_table,
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

        if cleaned_sql is None:
            raise RuntimeError("parser reported success but produced no SQL")
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
            if reference_sql is None:
                raise RuntimeError("reference_sql missing despite gradable=True")
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


def _run_graph_rag_arm(
    spark: Any,
    ws: WorkspaceClient,
    settings: ClientSettings,
    questions: list[Question],
    summary: ClientRunSummary,
    staging_table: str,
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
        staging_table,
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

        if cleaned_sql is None:
            raise RuntimeError("parser reported success but produced no SQL")
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
            if reference_sql is None:
                raise RuntimeError("reference_sql missing despite gradable=True")
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
