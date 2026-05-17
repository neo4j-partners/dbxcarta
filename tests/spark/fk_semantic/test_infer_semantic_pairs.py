"""Spark-native semantic FK inference (`fk.inference`).

Candidate generation now comes from the injectable nearest-neighbor seam
(`semantic_nn_pairs`, a per-source exact structural pre-filter then
`vector.similarity.cosine` over the survivors in production). These tests
feed a canned `(source_id, target_id, score)`
frame straight into `infer_semantic_edges` — the deterministic correctness
pipeline — so the suite runs with no Neo4j. The hand-crafted vectors (see
conftest `phase4_vectors`) give exact cosines: buyer_ref→customers.id 0.88,
purchase_ref→orders.id 0.87, unrelated 0. Confidence is the floor/cap clamp
`clamp(score, 0.80, 0.90)`; there is no value-overlap bonus (removed).
"""

from __future__ import annotations

import math

import pytest

from dbxcarta.spark.contract import EdgeSource
from dbxcarta.spark.ingest.fk.inference import (
    build_columns_frame,
    build_pk_gate,
    infer_semantic_edges,
)

_CAT = "main"
_SCHEMA = "shop"

_COL_FIELDS = (
    "table_catalog", "table_schema", "table_name", "column_name",
    "data_type", "comment",
)


def _columns_schema():
    from pyspark.sql.types import StringType, StructField, StructType

    return StructType([StructField(n, StringType(), True) for n in _COL_FIELDS])


def _constraints_schema():
    from pyspark.sql.types import (
        IntegerType,
        StringType,
        StructField,
        StructType,
    )

    return StructType([
        StructField("table_catalog", StringType(), True),
        StructField("table_schema", StringType(), True),
        StructField("table_name", StringType(), True),
        StructField("column_name", StringType(), True),
        StructField("constraint_type", StringType(), True),
        StructField("ordinal_position", IntegerType(), True),
        StructField("constraint_name", StringType(), True),
    ])


def _node_schema():
    from pyspark.sql.types import (
        ArrayType,
        DoubleType,
        StringType,
        StructField,
        StructType,
    )

    return StructType([
        StructField("id", StringType(), False),
        StructField("embedding", ArrayType(DoubleType()), True),
    ])


def _split(col_id: str) -> tuple[str, str, str, str]:
    cat, schema, table, column = col_id.split(".")
    return cat, schema, table, column


def _fixture_columns() -> list[tuple]:
    ids = [
        f"{_CAT}.{_SCHEMA}.customers.id",
        f"{_CAT}.{_SCHEMA}.orders.id",
        f"{_CAT}.{_SCHEMA}.orders.buyer_ref",
        f"{_CAT}.{_SCHEMA}.order_items.purchase_ref",
        f"{_CAT}.{_SCHEMA}.unrelated.payload",
    ]
    return [(*_split(i), "BIGINT", None) for i in ids]


def _fixture_constraints() -> list[tuple]:
    return [
        (_CAT, _SCHEMA, t, "id", "PRIMARY KEY", 1, f"{t}_pk")
        for t in ("customers", "orders")
    ]


def _node_rows(vectors: dict[str, list[float]], only: set[str] | None = None):
    return [
        (cid, [float(x) for x in vec])
        for cid, vec in vectors.items()
        if only is None or cid in only
    ]


def _cos(a: list[float], b: list[float]) -> float:
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(y * y for y in b))
    if na == 0.0 or nb == 0.0:
        return 0.0
    return dot / (na * nb)


def _nn_pairs(spark, vectors: dict[str, list[float]]):
    """Stand in for `semantic_nn_pairs`: every ordered (src, tgt) pair with
    its exact cosine. The real per-source query pre-filters then returns the
    top-k key-like neighbors by cosine; here the deterministic pipeline
    applies the key-like / catalog-schema / threshold filters, so an
    unfiltered all-pairs frame is the strongest input (it proves the
    filters, not the candidate query, do the work).
    """
    ids = list(vectors)
    rows = [
        (s, t, float(_cos(vectors[s], vectors[t])))
        for s in ids
        for t in ids
        if s != t
    ]
    return spark.createDataFrame(rows, schema=["source_id", "target_id", "score"])


def _run(
    spark, vectors, *, columns=None, constraints=None,
    prior=None, node_only=None,
):
    columns_df = spark.createDataFrame(
        columns or _fixture_columns(), schema=_columns_schema(),
    )
    constraints_df = spark.createDataFrame(
        constraints if constraints is not None else _fixture_constraints(),
        schema=_constraints_schema(),
    )
    column_node_df = spark.createDataFrame(
        _node_rows(vectors, node_only), schema=_node_schema(),
    )
    cf = build_columns_frame(columns_df, column_node_df)
    pk_gate, _ = build_pk_gate(cf, constraints_df)
    edges_df, counts = infer_semantic_edges(
        cf, pk_gate, prior, _nn_pairs(spark, vectors),
    )
    rows = edges_df.collect()
    by_pair = {(r["source_id"], r["target_id"]): r for r in rows}
    return by_pair, counts, edges_df


_BUYER = (f"{_CAT}.{_SCHEMA}.orders.buyer_ref", f"{_CAT}.{_SCHEMA}.customers.id")
_PURCH = (
    f"{_CAT}.{_SCHEMA}.order_items.purchase_ref", f"{_CAT}.{_SCHEMA}.orders.id",
)


# --- Renamed FKs recovered ---------------------------------------------------

def test_recovers_renamed_edges_above_threshold(
    local_spark, phase4_vectors,
) -> None:
    by_pair, counts, edges_df = _run(local_spark, phase4_vectors)
    assert _BUYER in by_pair
    assert _PURCH in by_pair
    assert by_pair[_BUYER]["confidence"] == pytest.approx(0.88, abs=1e-3)
    assert by_pair[_PURCH]["confidence"] == pytest.approx(0.87, abs=1e-3)
    for r in edges_df.collect():
        assert r["source"] == EdgeSource.SEMANTIC.value
        assert r["criteria"] is None
    assert counts.accepted == 2


def test_unrelated_pair_rejected_sub_threshold(
    local_spark, phase4_vectors,
) -> None:
    by_pair, _counts, _df = _run(local_spark, phase4_vectors)
    unrelated = (
        f"{_CAT}.{_SCHEMA}.unrelated.payload", f"{_CAT}.{_SCHEMA}.customers.id",
    )
    assert unrelated not in by_pair


# --- Covered-pair suppression -----------------------------------------------

def test_suppresses_edges_already_covered(local_spark, phase4_vectors) -> None:
    """A prior (declared ∪ metadata) pair never re-emits as SEMANTIC."""
    prior = local_spark.createDataFrame([_BUYER], schema=["source_id", "target_id"])
    by_pair, _counts, _df = _run(local_spark, phase4_vectors, prior=prior)
    assert _BUYER not in by_pair
    assert _PURCH in by_pair


# --- Missing embeddings skipped ---------------------------------------------

def test_columns_without_embeddings_are_skipped(
    local_spark, phase4_vectors,
) -> None:
    """A column with no embedding row never appears in the NN frame, so it
    is never a src or tgt — the seam excludes it before any inference."""
    cols = _fixture_columns() + [
        (_CAT, _SCHEMA, "ghost", "id", "BIGINT", None),
    ]
    cons = _fixture_constraints() + [
        (_CAT, _SCHEMA, "ghost", "id", "PRIMARY KEY", 1, "ghost_pk"),
    ]
    by_pair, _counts, _df = _run(
        local_spark, phase4_vectors, columns=cols, constraints=cons,
    )
    for s, t in by_pair:
        assert "ghost" not in s
        assert "ghost" not in t


# --- Within-schema scope ----------------------------------------------------

def test_cross_schema_pairs_skipped(local_spark) -> None:
    """A perfect-cosine pair across two schemas produces no semantic edge."""
    vec = [1.0] + [0.0] * 1023
    vectors = {
        f"{_CAT}.primary.customers.id": vec,
        f"{_CAT}.replica.orders.customer_id": vec,
    }
    cols = [
        (_CAT, "primary", "customers", "id", "BIGINT", None),
        (_CAT, "replica", "orders", "customer_id", "BIGINT", None),
    ]
    cons = [
        (_CAT, "primary", "customers", "id", "PRIMARY KEY", 1, "p_cust_pk"),
    ]
    by_pair, counts, _df = _run(
        local_spark, vectors, columns=cols, constraints=cons,
    )
    assert by_pair == {}
    assert counts.accepted == 0


def test_generic_id_exact_matches_are_not_inferred(local_spark) -> None:
    """Perfectly similar standalone `id` columns are still not FK evidence."""
    vec = [1.0] + [0.0] * 1023
    vectors = {
        f"{_CAT}.{_SCHEMA}.customers.id": vec,
        f"{_CAT}.{_SCHEMA}.orders.id": vec,
    }
    cols = [
        (_CAT, _SCHEMA, "customers", "id", "BIGINT", None),
        (_CAT, _SCHEMA, "orders", "id", "BIGINT", None),
    ]
    cons = [
        (_CAT, _SCHEMA, "customers", "id", "PRIMARY KEY", 1, "customers_pk"),
        (_CAT, _SCHEMA, "orders", "id", "PRIMARY KEY", 1, "orders_pk"),
    ]
    by_pair, counts, _df = _run(
        local_spark, vectors, columns=cols, constraints=cons,
    )
    assert by_pair == {}
    assert counts.accepted == 0
