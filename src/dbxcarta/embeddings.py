"""Transform: add embedding columns to a node DataFrame via ai_query()."""

from __future__ import annotations

from typing import TYPE_CHECKING

from dbxcarta.contract import LABEL_TABLE

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def add_embedding_column(
    df: "DataFrame",
    text_expr: str,
    endpoint: str,
    expected_dimension: int,
    *,
    label: str,
) -> "DataFrame":
    """Append embedding_text, embedding_text_hash, embedding, embedding_model,
    embedded_at to df.

    embedding_text_hash is sha256 hex of the input text (always stored).
    embedding is nulled when ai_query() fails OR the returned vector length
    does not match expected_dimension — both count as failures in
    compute_failure_stats(). embedding_text is dropped for every label
    except Table; the hash alone is sufficient for drift detection on
    labels where storing the full text is not warranted.
    """
    from pyspark.sql.functions import col, current_timestamp, expr, lit, sha2, size, when

    df = (
        df
        .withColumn("embedding_text", expr(text_expr))
        .withColumn("embedding_text_hash", sha2(col("embedding_text"), 256))
        .withColumn(
            "_emb_raw",
            expr(f"ai_query('{endpoint}', embedding_text, failOnError => false)"),
        )
        .withColumn(
            "embedding",
            when(
                col("_emb_raw.result").isNotNull()
                & (size(col("_emb_raw.result")) == expected_dimension),
                col("_emb_raw.result"),
            ),
        )
        .withColumn("embedding_model", lit(endpoint))
        .withColumn("embedded_at", current_timestamp())
        .drop("_emb_raw")
    )

    if label != LABEL_TABLE:
        df = df.drop("embedding_text")

    return df


def compute_failure_stats(df: "DataFrame") -> tuple[float, int, int]:
    """Return (failure_rate, attempts, successes). Triggers a Spark action.

    Expects df to contain an 'embedding' column produced by add_embedding_column().
    A null embedding (from ai_query failure or dimension mismatch) counts as a failure.
    """
    from pyspark.sql.functions import col, count

    row = df.agg(
        count("*").alias("attempts"),
        count(col("embedding")).alias("successes"),
    ).collect()[0]
    attempts = int(row["attempts"])
    successes = int(row["successes"])
    rate = (attempts - successes) / attempts if attempts > 0 else 0.0
    return rate, attempts, successes
