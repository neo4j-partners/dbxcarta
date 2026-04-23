"""Phase 2 deliverable: build_references_rel sets provenance on declared edges.

Every row out of build_references_rel must carry the three new properties
(confidence=1.0, source="declared", criteria=null). When Phases 3/4/6 add
inferred edges, they write their own DataFrames with the same schema and
different values, so this test stays the source of truth for declared
provenance.

Uses three representative FK pairs from the test fixture schemas created by
tests/fixtures/setup_test_catalog.sql, including the cross-schema pair
(sales.order_items.product_id -> inventory.products.id).

See worklog/fk-gap-v3-build.md Phase 2.
"""

from __future__ import annotations

from dbxcarta.contract import REFERENCES_PROPERTIES
from dbxcarta.schema_graph import build_references_rel


_EXPECTED_COLUMNS = ("source_id", "target_id", "confidence", "source", "criteria")


def _fk_pairs_rows(local_spark):
    """Three declared FK pairs from the test fixture schemas.

    Covers intra-schema (orders->customers, order_items->orders) and
    cross-schema (order_items.product_id -> inventory.products.id) cases.
    """
    rows = [
        (
            "main", "dbxcarta_test_sales", "orders", "customer_id",
            "main", "dbxcarta_test_sales", "customers", "id",
        ),
        (
            "main", "dbxcarta_test_sales", "order_items", "order_id",
            "main", "dbxcarta_test_sales", "orders", "id",
        ),
        (
            "main", "dbxcarta_test_sales", "order_items", "product_id",
            "main", "dbxcarta_test_inventory", "products", "id",
        ),
    ]
    schema = (
        "src_catalog STRING, src_schema STRING, src_table STRING, src_column STRING, "
        "tgt_catalog STRING, tgt_schema STRING, tgt_table STRING, tgt_column STRING"
    )
    return local_spark.createDataFrame(rows, schema=schema)


def test_build_references_emits_provenance_columns(local_spark) -> None:
    df = build_references_rel(_fk_pairs_rows(local_spark))
    assert tuple(df.columns) == _EXPECTED_COLUMNS


def test_build_references_declared_values(local_spark) -> None:
    rows = build_references_rel(_fk_pairs_rows(local_spark)).collect()
    assert len(rows) == 3
    for row in rows:
        assert row["confidence"] == 1.0
        assert row["source"] == "declared"
        assert row["criteria"] is None


def test_build_references_ids_are_normalized(local_spark) -> None:
    """Sanity that the id_expr wiring survived the provenance additions."""
    rows = sorted(
        build_references_rel(_fk_pairs_rows(local_spark)).collect(),
        key=lambda r: (r["source_id"], r["target_id"]),
    )
    source_ids = {r["source_id"] for r in rows}
    target_ids = {r["target_id"] for r in rows}
    assert "main.dbxcarta_test_sales.orders.customer_id" in source_ids
    assert "main.dbxcarta_test_sales.customers.id" in target_ids


def test_references_properties_tuple_matches_dataframe(local_spark) -> None:
    """Contract's REFERENCES_PROPERTIES must be the DataFrame prop columns.

    The writer passes REFERENCES_PROPERTIES to the Neo4j Spark Connector's
    `relationship.properties` option. If the DataFrame columns drift from
    that tuple, property writes silently stop.
    """
    df_columns = set(build_references_rel(_fk_pairs_rows(local_spark)).columns)
    assert set(REFERENCES_PROPERTIES).issubset(df_columns)
