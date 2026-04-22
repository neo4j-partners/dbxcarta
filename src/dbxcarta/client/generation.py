"""Batch SQL generation via ai_query (best-practices Spark §1 and §4)."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


def generate_sql_batch(
    spark: SparkSession,
    endpoint: str,
    questions_with_prompts: list[dict],
    staging_path: str,
    arm: str,
) -> dict[str, tuple[str | None, str | None]]:
    """Generate SQL for every question in one ai_query pass.

    *questions_with_prompts* is a list of dicts with keys ``question_id``
    and ``prompt``.  Returns a mapping from question_id to
    ``(sql_text, error_message)``.

    Materializesto Delta after ai_query (best-practices Spark §4) so the
    downstream execute loop never re-triggers inference.
    """
    from pyspark.sql import Row
    from pyspark.sql.functions import col, expr

    rows = [
        Row(question_id=q["question_id"], prompt=q["prompt"])
        for q in questions_with_prompts
    ]
    df = spark.createDataFrame(rows)

    enriched = df.withColumn(
        "response",
        expr(f"ai_query('{endpoint}', prompt, failOnError => false)"),
    )

    stage_table = f"{staging_path}/{arm}_generation"
    enriched.write.format("delta").mode("overwrite").save(stage_table)
    result_df = spark.read.format("delta").load(stage_table)

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
