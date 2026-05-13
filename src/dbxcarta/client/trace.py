"""RetrievalTrace: per-question diagnostic record for the graph_rag arm."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from dbxcarta.databricks import quote_qualified_name

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


def _schema_from_id(node_id: str) -> str | None:
    """Extract the schema component from a catalog.schema.table[.column] ID."""
    parts = node_id.split(".")
    return parts[1] if len(parts) >= 3 else None


def schema_scores_from_seeds(
    col_seed_ids: list[str],
    col_seed_scores: list[float],
    tbl_seed_ids: list[str],
    tbl_seed_scores: list[float],
) -> dict[str, float]:
    """Aggregate seed scores onto schemas, normalized to sum to 1."""
    raw: dict[str, float] = {}
    for node_id, score in zip(col_seed_ids, col_seed_scores):
        schema = _schema_from_id(node_id)
        if schema:
            raw[schema] = raw.get(schema, 0.0) + score
    for node_id, score in zip(tbl_seed_ids, tbl_seed_scores):
        schema = _schema_from_id(node_id)
        if schema:
            raw[schema] = raw.get(schema, 0.0) + score
    total = sum(raw.values()) or 1.0
    return {k: v / total for k, v in raw.items()}


def chosen_schemas_from_columns(
    columns: list,
) -> list[str]:
    """Return deduplicated schemas present in the final column set, in order."""
    seen: list[str] = []
    for col in columns:
        schema = _schema_from_id(col.table_fqn)
        if schema and schema not in seen:
            seen.append(schema)
    return seen


@dataclass
class RetrievalTrace:
    run_id: str
    question_id: str
    question: str
    target_schema: str | None
    col_seed_ids: list[str]
    col_seed_scores: list[float]
    tbl_seed_ids: list[str]
    tbl_seed_scores: list[float]
    schema_scores: dict[str, float]
    chosen_schemas: list[str]
    expansion_tbl_ids: list[str]
    final_col_ids: list[str]
    rendered_context: str
    generated_sql: str | None = None
    reference_sql: str | None = None
    parsed: bool = False
    executed: bool = False
    correct: bool = False
    execution_error: str | None = None
    top1_schema_match: bool | None = None
    schema_in_context: bool | None = None
    context_purity: float | None = None


def compute_retrieval_metrics(trace: RetrievalTrace) -> None:
    """Compute top1_schema_match, schema_in_context, context_purity in-place."""
    if not trace.target_schema:
        return
    if trace.schema_scores:
        top1 = max(trace.schema_scores, key=trace.schema_scores.__getitem__)
        trace.top1_schema_match = top1 == trace.target_schema
    trace.schema_in_context = trace.target_schema in trace.chosen_schemas
    if trace.final_col_ids:
        from_target = sum(
            1 for cid in trace.final_col_ids
            if _schema_from_id(cid) == trace.target_schema
        )
        trace.context_purity = from_target / len(trace.final_col_ids)


def emit_retrieval_traces(
    spark: SparkSession,
    traces: list[RetrievalTrace],
    table_name: str,
) -> None:
    if not traces:
        return

    from pyspark.sql import Row
    from pyspark.sql.types import (
        ArrayType,
        BooleanType,
        DoubleType,
        MapType,
        StringType,
        StructField,
        StructType,
    )

    schema = StructType([
        StructField("run_id", StringType(), nullable=False),
        StructField("question_id", StringType()),
        StructField("question", StringType()),
        StructField("target_schema", StringType()),
        StructField("col_seed_ids", ArrayType(StringType())),
        StructField("col_seed_scores", ArrayType(DoubleType())),
        StructField("tbl_seed_ids", ArrayType(StringType())),
        StructField("tbl_seed_scores", ArrayType(DoubleType())),
        StructField("schema_scores", MapType(StringType(), DoubleType())),
        StructField("chosen_schemas", ArrayType(StringType())),
        StructField("expansion_tbl_ids", ArrayType(StringType())),
        StructField("final_col_ids", ArrayType(StringType())),
        StructField("rendered_context", StringType()),
        StructField("generated_sql", StringType()),
        StructField("reference_sql", StringType()),
        StructField("parsed", BooleanType()),
        StructField("executed", BooleanType()),
        StructField("correct", BooleanType()),
        StructField("execution_error", StringType()),
        StructField("top1_schema_match", BooleanType()),
        StructField("schema_in_context", BooleanType()),
        StructField("context_purity", DoubleType()),
    ])

    rows = [
        Row(
            run_id=t.run_id,
            question_id=t.question_id,
            question=t.question,
            target_schema=t.target_schema,
            col_seed_ids=t.col_seed_ids,
            col_seed_scores=t.col_seed_scores,
            tbl_seed_ids=t.tbl_seed_ids,
            tbl_seed_scores=t.tbl_seed_scores,
            schema_scores=t.schema_scores,
            chosen_schemas=t.chosen_schemas,
            expansion_tbl_ids=t.expansion_tbl_ids,
            final_col_ids=t.final_col_ids,
            rendered_context=t.rendered_context,
            generated_sql=t.generated_sql,
            reference_sql=t.reference_sql,
            parsed=t.parsed,
            executed=t.executed,
            correct=t.correct,
            execution_error=t.execution_error,
            top1_schema_match=t.top1_schema_match,
            schema_in_context=t.schema_in_context,
            context_purity=t.context_purity,
        )
        for t in traces
    ]

    quoted = quote_qualified_name(table_name, expected_parts=3)
    (
        spark.createDataFrame(rows, schema=schema)
        .write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(quoted)
    )
