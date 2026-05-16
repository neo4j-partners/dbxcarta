"""Node builders under multi-catalog: Database fan-out, layer map, and the
catalog-qualified embedding text.

Pure DataFrame builders, exercised in local Spark. Asserts the multi-catalog
contract: one Database node per catalog, catalog-derived `layer`, catalog
retained as an embedding-text helper, and HAS_SCHEMA linking each schema to
its own Database. Also pins the Table/Column embedding text expressions so
two catalogs with an identical schema.table no longer collide.
"""

from __future__ import annotations

from dbxcarta.spark.contract import generate_id
from dbxcarta.spark.ingest.schema_graph import (
    build_column_nodes,
    build_database_nodes,
    build_has_schema_rel,
    build_table_nodes,
)
from dbxcarta.spark.ingest.transform.embed_stage import (
    _COLUMN_EMBEDDING_TEXT_EXPR,
    _TABLE_EMBEDDING_TEXT_EXPR,
)


def test_build_database_nodes_one_per_catalog(local_spark) -> None:
    rows = build_database_nodes(local_spark, ["bronze", "silver", "gold"]).collect()
    assert {r["name"] for r in rows} == {"bronze", "silver", "gold"}
    assert {r["id"] for r in rows} == {
        generate_id("bronze"), generate_id("silver"), generate_id("gold"),
    }


def test_build_database_nodes_single_catalog_one_node(local_spark) -> None:
    """A single-catalog run passes a one-element list — historical behavior."""
    rows = build_database_nodes(local_spark, ["main"]).collect()
    assert len(rows) == 1
    assert rows[0]["name"] == "main"


_TABLES_SCHEMA = (
    "table_catalog string, table_schema string, table_name string,"
    " table_type string, comment string, created string, last_altered string"
)
_COLUMNS_SCHEMA = (
    "table_catalog string, table_schema string, table_name string,"
    " column_name string, data_type string, is_nullable string,"
    " ordinal_position int, comment string"
)


def _tables_df(local_spark):
    return local_spark.createDataFrame(
        [
            ("bronze", "sales", "orders", "TABLE", "raw orders", None, None),
            ("gold", "sales", "orders", "TABLE", "curated orders", None, None),
        ],
        _TABLES_SCHEMA,
    )


def test_build_table_nodes_layer_from_catalog(local_spark) -> None:
    rows = build_table_nodes(
        _tables_df(local_spark), {"bronze": "bronze", "gold": "gold"},
    ).collect()
    by_cat = {r["table_catalog"]: r for r in rows}
    assert by_cat["bronze"]["layer"] == "bronze"
    assert by_cat["gold"]["layer"] == "gold"
    # ids stay catalog-distinct even though schema.table is identical.
    assert by_cat["bronze"]["id"] != by_cat["gold"]["id"]


def test_build_table_nodes_unmapped_catalog_null_layer(local_spark) -> None:
    rows = build_table_nodes(_tables_df(local_spark), {"bronze": "bronze"}).collect()
    by_cat = {r["table_catalog"]: r for r in rows}
    assert by_cat["bronze"]["layer"] == "bronze"
    assert by_cat["gold"]["layer"] is None


def test_build_table_nodes_retains_catalog_for_embedding(local_spark) -> None:
    df = build_table_nodes(_tables_df(local_spark), None)
    assert "table_catalog" in df.columns


def test_build_column_nodes_retains_catalog_for_embedding(local_spark) -> None:
    columns_df = local_spark.createDataFrame(
        [
            ("bronze", "sales", "orders", "id", "bigint", "NO", 1, None),
            ("gold", "sales", "orders", "id", "bigint", "NO", 1, None),
        ],
        _COLUMNS_SCHEMA,
    )
    df = build_column_nodes(columns_df)
    assert "table_catalog" in df.columns
    ids = {r["id"] for r in df.collect()}
    assert len(ids) == 2  # catalog-qualified, so no collision


def test_build_has_schema_rel_links_each_schema_to_its_own_database(
    local_spark,
) -> None:
    schemata_df = local_spark.createDataFrame(
        [("bronze", "sales", None), ("gold", "sales", None)],
        "catalog_name string, schema_name string, comment string",
    )
    rows = build_has_schema_rel(schemata_df).collect()
    pairs = {(r["source_id"], r["target_id"]) for r in rows}
    assert (generate_id("bronze"), generate_id("bronze", "sales")) in pairs
    assert (generate_id("gold"), generate_id("gold", "sales")) in pairs


def test_table_embedding_text_distinguishes_catalogs(local_spark) -> None:
    """The Table embedding text now leads with catalog, so two catalogs with
    an identical schema.table no longer embed identically."""
    df = build_table_nodes(_tables_df(local_spark), None)
    texts = {
        r["table_catalog"]: r["t"]
        for r in df.selectExpr(
            "table_catalog", f"{_TABLE_EMBEDDING_TEXT_EXPR} AS t",
        ).collect()
    }
    assert texts["bronze"].startswith("bronze.sales.orders")
    assert texts["gold"].startswith("gold.sales.orders")
    assert texts["bronze"] != texts["gold"]


def test_column_embedding_text_distinguishes_catalogs(local_spark) -> None:
    columns_df = local_spark.createDataFrame(
        [
            ("bronze", "sales", "orders", "id", "bigint", "NO", 1, "pk"),
            ("gold", "sales", "orders", "id", "bigint", "NO", 1, "pk"),
        ],
        _COLUMNS_SCHEMA,
    )
    df = build_column_nodes(columns_df)
    texts = {
        r["table_catalog"]: r["t"]
        for r in df.selectExpr(
            "table_catalog", f"{_COLUMN_EMBEDDING_TEXT_EXPR} AS t",
        ).collect()
    }
    assert texts["bronze"].startswith("bronze.sales.orders.id")
    assert texts["gold"].startswith("gold.sales.orders.id")
    assert texts["bronze"] != texts["gold"]
