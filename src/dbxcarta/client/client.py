"""DBxCarta client — Text2SQL evaluation harness.

Evaluation arms:
  - reference: executes reference_sql directly to validate the grading path.
  - no_context/schema_dump: generate SQL with ai_query, parse it, execute it
    on the warehouse, and compare the result set against reference_sql.
  - graph_rag: embed the question, retrieve graph context from Neo4j, then
    generate and grade SQL through the same execution path.
"""

from __future__ import annotations

import json
import os
import re
import requests
from pathlib import Path
from typing import Any

from databricks.sdk import WorkspaceClient

from dbxcarta.client.executor import execute_sql, preflight_warehouse
from dbxcarta.client.settings import ClientSettings
from dbxcarta.client.summary import ClientRunSummary
from dbxcarta.databricks import build_workspace_client, quote_qualified_name, uc_volume_parent

_REFERENCE_ARM = "reference"
_LLM_ARMS = {"no_context", "schema_dump"}
_STAGING_ARMS = _LLM_ARMS | {"graph_rag"}

# Matches optional ```sql ... ``` or ``` ... ``` fences.
_FENCE_RE = re.compile(r"^```(?:sql)?\s*\n?(.*?)\n?```\s*$", re.DOTALL | re.IGNORECASE)
_SQL_START_RE = re.compile(r"^\s*(SELECT|WITH|INSERT|UPDATE|DELETE|CREATE|EXPLAIN)\b", re.IGNORECASE)

# Result-set comparison algorithm boundaries. Not runtime tunables.
_COMPARE_ROW_THRESHOLD = 500      # rows below this → exact set equality; at or above → sampled
_LARGE_COUNT_TOLERANCE = 0.10     # row count divergence above 10% → immediately False
_LARGE_SAMPLE_MATCH_RATE = 0.80   # fraction of stride-sampled rows that must match exactly


def _client_retrieval_table(settings: ClientSettings) -> str:
    parts = settings.dbxcarta_summary_table.split(".")
    return f"{parts[0]}.{parts[1]}.client_retrieval"


def _is_table_ref(source: str) -> bool:
    """Return True when source looks like a three-part catalog.schema.table name."""
    return len(source.split(".")) == 3 and not source.startswith(("/", "."))


def _load_questions(source: str, spark: Any = None) -> list[dict[str, Any]]:
    if _is_table_ref(source):
        if spark is None:
            raise RuntimeError("spark is required to load questions from a Delta table")
        return [row.asDict() for row in spark.table(source).collect()]
    text = Path(source).read_text()
    data = json.loads(text)
    if not isinstance(data, list):
        raise ValueError(f"questions file must be a JSON array, got {type(data)}")
    return data


def _stringify_cell(value: Any) -> str:
    """Stringify a result-set cell, casefolding strings so the comparator is
    case-insensitive on string values (does not affect numeric or NULL cells)."""
    if value is None:
        return "NULL"
    return str(value).casefold()


def _normalize_row(row: list, col_names: list[str]) -> tuple:
    """Reorder row values by sorted column name, stringifying each value.

    Normalises differences in column ordering between generated and reference SQL.
    Falls back to value-sort when column names are unavailable or mismatched.
    String values are casefolded so 'High' and 'high' compare equal.
    """
    if col_names and len(col_names) == len(row):
        return tuple(_stringify_cell(v) for _, v in sorted(zip(col_names, row)))
    return tuple(sorted(_stringify_cell(v) for v in row))


def _normalize_result_set(col_names: list[str], rows: list[list]) -> list[tuple]:
    normalized = [_normalize_row(row, col_names) for row in rows]
    return sorted(normalized)


def _project_to_ref_columns(
    gen_cols: list[str],
    gen_rows: list[list],
    ref_cols: list[str],
) -> tuple[list[str], list[list]]:
    """If every ref column name exists in gen columns (case-insensitive),
    project gen rows down to ref's columns. Otherwise return gen unchanged.

    Handles cases like generated `SELECT *` against a reference that selects
    a subset of columns: as long as the reference's columns are a subset of
    what was generated, the extra generated columns are dropped before
    comparison.
    """
    if not ref_cols or not gen_cols or len(ref_cols) >= len(gen_cols):
        return gen_cols, gen_rows
    gen_lower = [c.casefold() for c in gen_cols]
    ref_lower = [c.casefold() for c in ref_cols]
    if not set(ref_lower).issubset(set(gen_lower)):
        return gen_cols, gen_rows
    idx = [gen_lower.index(c) for c in ref_lower]
    projected_rows = [[row[i] for i in idx] for row in gen_rows]
    return list(ref_cols), projected_rows


def _is_row_superset(ref_norm: list[tuple], gen_norm: list[tuple]) -> bool:
    """True when every reference row appears in gen at least as many times.

    Counter semantics so duplicate rows in reference must be matched by
    duplicates in generated. Lets the comparator accept generated SQL that
    returned a superset of the reference rows (e.g., a larger LIMIT) while
    still rejecting generations that miss rows.
    """
    from collections import Counter

    ref_counter = Counter(ref_norm)
    gen_counter = Counter(gen_norm)
    for row, count in ref_counter.items():
        if gen_counter[row] < count:
            return False
    return True


def _compare_result_sets(
    gen_cols: list[str],
    gen_rows: list[list],
    ref_cols: list[str],
    ref_rows: list[list],
) -> tuple[bool, str | None]:
    """Compare two result sets, ignoring column ordering and case.

    Tolerances applied, in order:
      1. Casefolded string comparison: 'High' == 'high'.
      2. Reference-as-subset column projection: if ref's columns are a
         subset of gen's, project gen down to ref's columns before comparing
         (handles generated `SELECT *` vs reference that picks specific columns).
      3. Row-superset semantics: if gen has more rows than ref but every ref
         row appears in gen (Counter semantics), mark correct. Handles
         generated `LIMIT 20` vs reference `LIMIT 10` and similar.

    Small result sets (< _COMPARE_ROW_THRESHOLD): exact equality or
    row-superset after normalization.
    Large result sets: short-circuit on >10% row-count divergence below
    reference count, then require 80% of stride-sampled sorted rows to match.
    """
    gen_cols, gen_rows = _project_to_ref_columns(gen_cols, gen_rows, ref_cols)

    gen_count = len(gen_rows)
    ref_count = len(ref_rows)

    if gen_count >= _COMPARE_ROW_THRESHOLD or ref_count >= _COMPARE_ROW_THRESHOLD:
        if gen_count < ref_count and ref_count > 0 and (ref_count - gen_count) / ref_count > _LARGE_COUNT_TOLERANCE:
            return False, (
                f"row count divergence >10% below reference: generated={gen_count} reference={ref_count}"
            )
        gen_sorted = _normalize_result_set(gen_cols, gen_rows)
        ref_sorted = _normalize_result_set(ref_cols, ref_rows)
        if gen_count > ref_count and _is_row_superset(ref_sorted, gen_sorted):
            return True, None
        sample_size = min(50, gen_count)
        stride = max(1, gen_count // sample_size) if sample_size else 1
        gen_sample = gen_sorted[::stride][:sample_size]
        ref_sample = ref_sorted[::stride][:sample_size]
        if not gen_sample:
            return True, None
        match_rate = sum(g == r for g, r in zip(gen_sample, ref_sample)) / len(gen_sample)
        if match_rate < _LARGE_SAMPLE_MATCH_RATE:
            return False, f"sampled match rate {match_rate:.1%} < {_LARGE_SAMPLE_MATCH_RATE:.0%}"
        return True, None

    gen_sorted = _normalize_result_set(gen_cols, gen_rows)
    ref_sorted = _normalize_result_set(ref_cols, ref_rows)

    if gen_count == ref_count:
        if gen_sorted == ref_sorted:
            return True, None
        return False, "result set values differ"

    if gen_count > ref_count and _is_row_superset(ref_sorted, gen_sorted):
        return True, None

    return False, f"row count mismatch: generated={gen_count} reference={ref_count}"


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

    return _compare_result_sets(gen_cols, gen_rows, ref_cols, ref_rows)


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
        reference_sql = q.get("reference_sql")
        gradable = bool(reference_sql) and executed
        correct = False
        if gradable:
            correct, _ = _grade_correct(
                cleaned_sql,
                reference_sql,
                ws,
                settings.databricks_warehouse_id,
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
    questions: list[dict[str, Any]],
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

    texts = [q["question"] for q in questions]
    embeddings, embed_error = _embed_questions(ws, settings.dbxcarta_embed_endpoint, texts)
    if embeddings is None:
        for q in questions:
            summary.add_result(
                question_id=q["question_id"],
                question=q["question"],
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
            qid = q["question_id"]
            bundle = retriever.retrieve(q["question"], emb)
            context_text = bundle.to_text()
            prompt = graph_rag_prompt(q["question"], catalog, schemas, context_text)
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
                question=q["question"],
                target_schema=q.get("schema"),
                col_seed_ids=bundle.col_seed_ids,
                col_seed_scores=bundle.col_seed_scores,
                tbl_seed_ids=bundle.tbl_seed_ids,
                tbl_seed_scores=bundle.tbl_seed_scores,
                schema_scores=sch_scores,
                chosen_schemas=chosen_schemas_from_columns(bundle.columns),
                expansion_tbl_ids=bundle.expansion_tbl_ids,
                final_col_ids=final_col_ids,
                rendered_context=context_text,
                reference_sql=q.get("reference_sql"),
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
        qid = q["question_id"]
        raw_sql, ai_error = responses.get(qid, (None, "no response"))
        cleaned_sql, parse_ok = _parse_sql(raw_sql)
        trace = traces.get(qid)

        if not parse_ok:
            if trace:
                trace.generated_sql = raw_sql
                trace.execution_error = ai_error or "response did not contain valid SQL"
            summary.add_result(
                question_id=qid,
                question=q["question"],
                arm="graph_rag",
                sql=raw_sql,
                context_ids=traces[qid].col_seed_ids + traces[qid].tbl_seed_ids if trace else [],
                parsed=False,
                executed=False,
                non_empty=False,
                error=ai_error or "response did not contain valid SQL",
                top1_schema_match=trace.top1_schema_match if trace else None,
                schema_in_context=trace.schema_in_context if trace else None,
                context_purity=trace.context_purity if trace else None,
            )
            continue

        executed, non_empty, exec_error = execute_sql(
            ws,
            settings.databricks_warehouse_id,
            cleaned_sql,
            settings.dbxcarta_client_timeout_sec,
        )
        reference_sql = q.get("reference_sql")
        gradable = bool(reference_sql) and executed
        correct = False
        if gradable:
            correct, _ = _grade_correct(
                cleaned_sql,
                reference_sql,
                ws,
                settings.databricks_warehouse_id,
                settings.dbxcarta_client_timeout_sec,
            )

        if trace:
            trace.generated_sql = cleaned_sql
            trace.parsed = True
            trace.executed = executed
            trace.correct = correct
            trace.execution_error = exec_error

        summary.add_result(
            question_id=qid,
            question=q["question"],
            arm="graph_rag",
            sql=cleaned_sql,
            context_ids=trace.col_seed_ids + trace.tbl_seed_ids if trace else [],
            parsed=True,
            executed=executed,
            non_empty=non_empty,
            correct=correct,
            gradable=gradable,
            error=exec_error,
            top1_schema_match=trace.top1_schema_match if trace else None,
            schema_in_context=trace.schema_in_context if trace else None,
            context_purity=trace.context_purity if trace else None,
        )

    emit_retrieval_traces(
        spark, list(traces.values()), _client_retrieval_table(settings)
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
        summary.finish(status="error", error=str(exc))
        raise

    finally:
        summary.emit(spark, settings.dbxcarta_summary_volume, settings.dbxcarta_summary_table)


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
    ])
    rows = [
        (
            q.get("question_id"),
            q.get("question"),
            q.get("notes"),
            q.get("reference_sql"),
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
