"""Spark-native semantic FK inference (`fk.inference`).

Drives `infer_semantic_edges` the way the orchestrator does: an
information_schema-shaped `columns_df`, a `column_node_df` carrying the
`embedding` array, and a `constraints_df` in; a REFERENCES DataFrame out.
The embedding column stays a DataFrame column end to end — no driver
collect, no Python cosine.

The hand-crafted vectors (see conftest `phase4_vectors`) give exact
cosines: buyer_ref→customers.id 0.88, purchase_ref→orders.id 0.87,
unrelated 0. Confidence is clamp(sim, 0.80, 0.90); value overlap adds
+0.05 capped at 0.90.
"""

from __future__ import annotations

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
_CON_FIELDS = (
    "table_catalog", "table_schema", "table_name", "column_name",
    "constraint_type", "ordinal_position", "constraint_name",
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


def _run(
    spark, vectors, *, columns=None, constraints=None,
    prior=None, value_node_df=None, has_value_df=None, node_only=None,
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
        cf, pk_gate, prior, value_node_df, has_value_df,
    )
    rows = edges_df.collect()
    by_pair = {(r["source_id"], r["target_id"]): r for r in rows}
    return by_pair, counts, edges_df


def _value_frames(spark, values_by_col: dict[str, set[str]]):
    """Build (value_node_df, has_value_df) from col_id → value set."""
    all_values = sorted({v for vs in values_by_col.values() for v in vs})
    value_node_df = spark.createDataFrame(
        [(f"val:{v}", v) for v in all_values], schema=["id", "value"],
    )
    has_rows = [
        (col_id, f"val:{v}")
        for col_id, vs in values_by_col.items()
        for v in vs
    ]
    has_value_df = spark.createDataFrame(
        has_rows, schema=["source_id", "target_id"],
    )
    return value_node_df, has_value_df


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
    assert counts.rejected == max(0, counts.candidates - counts.accepted)


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


# --- Value-overlap corroboration --------------------------------------------

def test_value_overlap_adds_bonus_up_to_cap(local_spark, phase4_vectors) -> None:
    """75% asymmetric overlap → +0.05 bonus, clamped at 0.90."""
    src, tgt = _BUYER
    vnode, hvalue = _value_frames(local_spark, {
        src: {"v1", "v2", "v3", "v4"},
        tgt: {"v1", "v2", "v3", "v9", "v10"},  # |src∩tgt|/|src| = 3/4
    })
    by_pair, counts, _df = _run(
        local_spark, phase4_vectors, value_node_df=vnode, has_value_df=hvalue,
    )
    assert by_pair[_BUYER]["confidence"] == pytest.approx(0.90, abs=1e-3)
    assert counts.value_corroborated >= 1


def test_value_overlap_below_threshold_no_bonus(
    local_spark, phase4_vectors,
) -> None:
    """25% overlap < 50% threshold → no bonus."""
    src, tgt = _BUYER
    vnode, hvalue = _value_frames(local_spark, {
        src: {"v1", "v2", "v3", "v4"},
        tgt: {"v1", "v9", "v10", "v11"},  # 1/4 = 25%
    })
    by_pair, counts, _df = _run(
        local_spark, phase4_vectors, value_node_df=vnode, has_value_df=hvalue,
    )
    assert by_pair[_BUYER]["confidence"] == pytest.approx(0.88, abs=1e-3)
    assert counts.value_corroborated == 0


def test_value_overlap_is_asymmetric(local_spark, phase4_vectors) -> None:
    """Denominator is the *source* distinct count, not target and not union.

    src has 2 values both in tgt → ratio = 2/2 = 1.0 (corroborates).
    By target count it would be 2/5 = 0.4 and by union 2/5 = 0.4 — both
    below 0.5 — so only the source-count divisor lifts the confidence.
    """
    src, tgt = _BUYER
    vnode, hvalue = _value_frames(local_spark, {
        src: {"v1", "v2"},
        tgt: {"v1", "v2", "v3", "v4", "v5"},
    })
    by_pair, counts, _df = _run(
        local_spark, phase4_vectors, value_node_df=vnode, has_value_df=hvalue,
    )
    assert by_pair[_BUYER]["confidence"] == pytest.approx(0.90, abs=1e-3)
    assert counts.value_corroborated >= 1


# --- Missing embeddings skipped ---------------------------------------------

def test_columns_without_embeddings_are_skipped(
    local_spark, phase4_vectors,
) -> None:
    """A column with no embedding row is left-joined to null and filtered
    out before any vector math — it never appears as src or tgt."""
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
