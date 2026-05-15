"""Batch SQL generation via ai_query (best-practices Spark §1 and §4)."""

from __future__ import annotations

from typing import TYPE_CHECKING

from dbxcarta.client.databricks import quote_qualified_name, validate_serving_endpoint_name

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


def generate_sql_batch(
    spark: SparkSession,
    endpoint: str,
    questions_with_prompts: list[dict],
    staging_table: str,
    arm: str,
) -> dict[str, tuple[str | None, str | None]]:
    """Generate SQL for every question in one ai_query pass.

    *questions_with_prompts* is a list of dicts with keys ``question_id``
    and ``prompt``.  Returns a mapping from question_id to
    ``(sql_text, error_message)``.

    Materializes to a UC-managed Delta table after ai_query (best-practices
    Spark §4) so the downstream execute loop never re-triggers inference.
    """
    # Guard before interpolation: ai_query requires the endpoint as a string
    # literal. validate_serving_endpoint_name rejects characters that would
    # break the SQL expression.
    validate_serving_endpoint_name(endpoint)

    from pyspark.sql import Row
    from pyspark.sql.functions import expr

    rows = [
        Row(question_id=q["question_id"], prompt=q["prompt"])
        for q in questions_with_prompts
    ]
    df = spark.createDataFrame(rows)

    enriched = df.withColumn(
        "response",
        expr(f"ai_query('{endpoint}', prompt, failOnError => false)"),
    )

    full_table = quote_qualified_name(f"{staging_table}_{arm}", expected_parts=3)
    (
        enriched.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(full_table)
    )
    result_df = spark.table(full_table)

    results: dict[str, tuple[str | None, str | None]] = {}
    for row in result_df.collect():
        qid = row["question_id"]
        response = row["response"]
        if response is None:
            results[qid] = (None, "ai_query returned null struct")
        else:
            sql_text = response["result"]
            error = response["errorMessage"]
            results[qid] = (sql_text, error if error else None)

    return results
