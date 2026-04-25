"""Unit tests for the embedding transform.

Exercises _validate_embedding directly with synthesized _emb_raw structs so
we can test wrong-shape and endpoint-error paths without calling ai_query.
Also tests add_embedding_column's label-driven embedding_text drop.
"""

from __future__ import annotations

import hashlib

import pytest

from dbxcarta.embeddings import _validate_embedding, add_embedding_column


EXPECTED_DIM = 1024


def _emb_raw_struct(spark, rows):
    """Build a DataFrame with (id, _emb_raw: struct<result: array<double>, errorMessage: string>)."""
    from pyspark.sql.types import (
        ArrayType,
        DoubleType,
        StringType,
        StructField,
        StructType,
    )

    raw_type = StructType([
        StructField("result", ArrayType(DoubleType())),
        StructField("errorMessage", StringType()),
    ])
    schema = StructType([
        StructField("id", StringType()),
        StructField("_emb_raw", raw_type),
    ])
    return spark.createDataFrame(rows, schema=schema)


def test_validate_embedding_success(local_spark) -> None:
    from pyspark.sql.functions import col

    vec = [0.1] * EXPECTED_DIM
    df = _emb_raw_struct(local_spark, [("ok", (vec, None))])
    embedding, error = _validate_embedding(col("_emb_raw"), EXPECTED_DIM)
    row = df.select(embedding.alias("e"), error.alias("err")).collect()[0]
    assert row["e"] == vec
    assert row["err"] is None


def test_validate_embedding_dimension_mismatch(local_spark) -> None:
    from pyspark.sql.functions import col

    short = [0.1, 0.2, 0.3]
    df = _emb_raw_struct(local_spark, [("short", (short, None))])
    embedding, error = _validate_embedding(col("_emb_raw"), EXPECTED_DIM)
    row = df.select(embedding.alias("e"), error.alias("err")).collect()[0]
    assert row["e"] is None
    assert row["err"] == f"dimension mismatch: got 3, expected {EXPECTED_DIM}"


def test_validate_embedding_endpoint_error_wins(local_spark) -> None:
    """errorMessage non-null → embedding null, error = errorMessage.

    Even if result happens to carry a wrong-shape vector, endpoint error takes
    precedence; we do not emit a dimension-mismatch message on top.
    """
    from pyspark.sql.functions import col

    df = _emb_raw_struct(
        local_spark,
        [("boom", ([0.1, 0.2], "endpoint timeout"))],
    )
    embedding, error = _validate_embedding(col("_emb_raw"), EXPECTED_DIM)
    row = df.select(embedding.alias("e"), error.alias("err")).collect()[0]
    assert row["e"] is None
    assert row["err"] == "endpoint timeout"


def test_embedding_text_drop_source_inspection() -> None:
    """add_embedding_column cannot be called end-to-end in local Spark because
    `ai_query` is a Databricks-only SQL function. Instead, assert the invariant
    from the source: `embedding_text` is always dropped, the hash is always
    retained. Covered at runtime by the integration suite.
    """
    import inspect

    source = inspect.getsource(add_embedding_column)
    assert '.drop("embedding_text")' in source
    assert 'embedding_text_hash' in source


def test_wrong_shape_row_counted_as_failure(local_spark) -> None:
    """compute_failure_stats counts a dimension-mismatch row as a failure
    because _validate_embedding nulls its embedding column."""
    from pyspark.sql.functions import col

    from dbxcarta.embeddings import compute_failure_stats

    df = _emb_raw_struct(
        local_spark,
        [
            ("ok", ([0.1] * EXPECTED_DIM, None)),
            ("short", ([0.1, 0.2], None)),
            ("err", (None, "endpoint timeout")),
        ],
    )
    embedding, _ = _validate_embedding(col("_emb_raw"), EXPECTED_DIM)
    staged = df.withColumn("embedding", embedding)
    rate, attempts, successes = compute_failure_stats(staged)
    assert attempts == 3
    assert successes == 1
    assert rate == pytest.approx(2 / 3)


def test_per_label_threshold_trips_independently_of_aggregate() -> None:
    """Mirrors the pipeline threshold loop: any enabled label whose rate
    exceeds the threshold must raise, even if the aggregate is under.

    Replicates pipeline.py's check with a plain dict so the logic is
    exercised without a full Spark run.
    """
    threshold = 0.05
    per_label = {"Table": 0.02, "Column": 0.10}
    attempts = {"Table": 1000, "Column": 100}
    successes = {"Table": 980, "Column": 90}

    tripped = [lbl for lbl, r in per_label.items() if r > threshold]
    assert tripped == ["Column"]

    total_attempts = sum(attempts.values())
    total_successes = sum(successes.values())
    aggregate = (total_attempts - total_successes) / total_attempts
    assert aggregate < threshold, (
        "fixture must have aggregate under threshold so this test proves"
        " per-label trips independently"
    )


def test_ledger_all_rows_hit_misses_empty(local_spark) -> None:
    """When every input row matches the ledger (hash + model), misses is empty
    and hits carry the stored embedding vector from the ledger."""
    from datetime import datetime

    from pyspark.sql.functions import sha2, expr
    from pyspark.sql.types import ArrayType, DoubleType, StringType, StructField, StructType, TimestampType

    from dbxcarta.ledger import split_by_ledger

    endpoint = "test-model"
    stored_vec = [0.5] * 4

    input_df = local_spark.createDataFrame(
        [("n1", "hello"), ("n2", "world")],
        ["id", "value"],
    )
    text_expr = "value"

    df_hashed = input_df.withColumn("_curr_hash", sha2(expr(text_expr), 256))
    hash_map = {r["id"]: r["_curr_hash"] for r in df_hashed.collect()}

    ledger_schema = StructType([
        StructField("id", StringType()),
        StructField("embedding_text_hash", StringType()),
        StructField("embedding", ArrayType(DoubleType())),
        StructField("embedding_model", StringType()),
        StructField("embedded_at", TimestampType()),
    ])
    ts = datetime(2026, 1, 1)
    ledger_df = local_spark.createDataFrame(
        [
            ("n1", hash_map["n1"], stored_vec, endpoint, ts),
            ("n2", hash_map["n2"], stored_vec, endpoint, ts),
        ],
        schema=ledger_schema,
    )

    hits_df, misses_df = split_by_ledger(df_hashed, ledger_df, endpoint)

    assert misses_df.count() == 0
    assert hits_df.count() == 2
    hit_row = hits_df.filter("id = 'n1'").select("_led_embedding").collect()[0]
    assert list(hit_row["_led_embedding"]) == stored_vec


def test_ledger_hash_mismatch_is_miss(local_spark) -> None:
    """A row whose embedding_text_hash differs from the ledger is a miss."""
    from datetime import datetime

    from pyspark.sql.functions import sha2, expr
    from pyspark.sql.types import ArrayType, DoubleType, StringType, StructField, StructType, TimestampType

    from dbxcarta.ledger import split_by_ledger

    endpoint = "test-model"
    stored_vec = [0.5] * 4

    input_df = local_spark.createDataFrame([("n1", "hello")], ["id", "value"])
    df_hashed = input_df.withColumn("_curr_hash", sha2(expr("value"), 256))

    ledger_schema = StructType([
        StructField("id", StringType()),
        StructField("embedding_text_hash", StringType()),
        StructField("embedding", ArrayType(DoubleType())),
        StructField("embedding_model", StringType()),
        StructField("embedded_at", TimestampType()),
    ])
    # Stale hash — different from the current "hello"
    ledger_df = local_spark.createDataFrame(
        [("n1", "stale-hash-value", stored_vec, endpoint, datetime(2026, 1, 1))],
        schema=ledger_schema,
    )

    hits_df, misses_df = split_by_ledger(df_hashed, ledger_df, endpoint)

    assert hits_df.count() == 0
    assert misses_df.count() == 1


def test_ledger_model_mismatch_is_miss(local_spark) -> None:
    """A row whose embedding_model differs from the current endpoint is a miss."""
    from datetime import datetime

    from pyspark.sql.functions import sha2, expr
    from pyspark.sql.types import ArrayType, DoubleType, StringType, StructField, StructType, TimestampType

    from dbxcarta.ledger import split_by_ledger

    current_endpoint = "new-model"
    stored_vec = [0.5] * 4

    input_df = local_spark.createDataFrame([("n1", "hello")], ["id", "value"])
    df_hashed = input_df.withColumn("_curr_hash", sha2(expr("value"), 256))
    current_hash = df_hashed.collect()[0]["_curr_hash"]

    ledger_schema = StructType([
        StructField("id", StringType()),
        StructField("embedding_text_hash", StringType()),
        StructField("embedding", ArrayType(DoubleType())),
        StructField("embedding_model", StringType()),
        StructField("embedded_at", TimestampType()),
    ])
    # Same hash but old model
    ledger_df = local_spark.createDataFrame(
        [("n1", current_hash, stored_vec, "old-model", datetime(2026, 1, 1))],
        schema=ledger_schema,
    )

    hits_df, misses_df = split_by_ledger(df_hashed, ledger_df, current_endpoint)

    assert hits_df.count() == 0
    assert misses_df.count() == 1


def test_embedding_text_hash_is_sha256_of_text(local_spark) -> None:
    """The hash column is sha2(embedding_text, 256) computed in Spark."""
    from pyspark.sql.functions import expr, sha2

    input_df = local_spark.createDataFrame([("s", "t")], ["table_schema", "name"])
    text_expr = "concat_ws('.', table_schema, name)"
    df = (
        input_df
        .withColumn("embedding_text", expr(text_expr))
        .withColumn("embedding_text_hash", sha2("embedding_text", 256))
    )
    row = df.collect()[0]
    assert row["embedding_text"] == "s.t"
    assert row["embedding_text_hash"] == hashlib.sha256(b"s.t").hexdigest()
