"""Schema graph transforms: pure DataFrame builders for Unity Catalog metadata.

Each function accepts DataFrames shaped like Unity Catalog information_schema
queries and returns connector-ready node or relationship DataFrames. Identifier
columns are generated through the shared contract helpers so Python and Spark
produce the same ids.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from dbxcarta.spark.contract import CONTRACT_VERSION, generate_id
from dbxcarta.spark.ingest.contract_expr import id_expr

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

    from dbxcarta.spark.ingest.fk.common import FKEdge


def build_database_nodes(
    spark: "SparkSession", catalogs: "list[str]",
) -> "DataFrame":
    """Build one Database node per ingested catalog.

    A single-catalog run passes a one-element list, so the historical
    one-Database-node behavior is preserved.
    """
    from pyspark.sql import Row

    return spark.createDataFrame([
        Row(id=generate_id(c), name=c, contract_version=CONTRACT_VERSION)
        for c in catalogs
    ])


def build_schema_nodes(schemata_df: "DataFrame") -> "DataFrame":
    """Build Schema nodes from information_schema.schemata rows.

    Keeps `catalog_name` only for downstream embedding text construction; the
    load step drops it before writing to Neo4j.
    """
    from pyspark.sql.functions import col, lit

    return (
        schemata_df
        .withColumn("id", id_expr("catalog_name", "schema_name"))
        .withColumn("name", col("schema_name"))
        .withColumn("contract_version", lit(CONTRACT_VERSION))
        .select("id", "name", "catalog_name", "comment", "contract_version")
    )


def build_table_nodes(
    tables_df: "DataFrame", layer_map: "dict[str, str] | None" = None,
) -> "DataFrame":
    """Build Table nodes from information_schema.tables rows.

    The selected columns form the public graph payload plus `table_catalog`
    and `table_schema`, which are retained only until embedding text has been
    generated (the load step drops `table_catalog` before the Neo4j write, so
    the graph contract is unchanged). The `layer` property is derived from
    `table_catalog` through the configured catalog->layer map; catalogs absent
    from the map (or an empty map) yield a null `layer` (contract v1.1,
    additive).
    """
    from pyspark.sql.functions import col, lit, when
    from pyspark.sql.types import StringType

    layer_expr = lit(None).cast(StringType())
    for catalog, layer in (layer_map or {}).items():
        layer_expr = when(col("table_catalog") == catalog, lit(layer)).otherwise(
            layer_expr
        )

    return (
        tables_df
        .withColumn("id", id_expr("table_catalog", "table_schema", "table_name"))
        .withColumn("name", col("table_name"))
        .withColumn("layer", layer_expr)
        .withColumn("contract_version", lit(CONTRACT_VERSION))
        # table_catalog + table_schema are retained for the embedding text
        # expression; table_catalog is dropped before the Neo4j write.
        .select("id", "name", "layer", "table_catalog", "table_schema", "comment", "table_type", "created", "last_altered", "contract_version")
    )


def build_column_nodes(columns_df: "DataFrame") -> "DataFrame":
    """Build Column nodes from information_schema.columns rows.

    Converts Databricks YES/NO nullability strings into booleans while
    preserving ordinal position, data type, and comments for retrieval context.
    `table_catalog`/`table_schema`/`table_name` are retained only for the
    embedding text expression; the load step drops them before the Neo4j
    write, so the graph contract is unchanged.
    """
    from pyspark.sql.functions import col, lit, when

    return (
        columns_df
        .withColumn("id", id_expr("table_catalog", "table_schema", "table_name", "column_name"))
        .withColumn("name", col("column_name"))
        .withColumn(
            "is_nullable",
            when(col("is_nullable") == "YES", True).when(col("is_nullable") == "NO", False),
        )
        .withColumn("contract_version", lit(CONTRACT_VERSION))
        .select("id", "name", "table_catalog", "table_schema", "table_name", "data_type", "is_nullable", "ordinal_position", "comment", "contract_version")
    )


def build_has_schema_rel(schemata_df: "DataFrame") -> "DataFrame":
    """Build Database -> Schema edges for every schema in scope.

    The Database source id is derived from each row's `catalog_name`, so a
    multi-catalog snapshot links every schema to its own Database node. This
    matches `build_database_nodes`, whose id is `generate_id(catalog)` and
    `id_expr("catalog_name")` applies the same normalization.
    """
    return (
        schemata_df
        .withColumn("source_id", id_expr("catalog_name"))
        .withColumn("target_id", id_expr("catalog_name", "schema_name"))
        .select("source_id", "target_id")
    )


def build_has_table_rel(tables_df: "DataFrame") -> "DataFrame":
    """Build Schema -> Table edges for every table in scope."""
    return (
        tables_df
        .withColumn("source_id", id_expr("table_catalog", "table_schema"))
        .withColumn("target_id", id_expr("table_catalog", "table_schema", "table_name"))
        .select("source_id", "target_id")
    )


def build_has_column_rel(columns_df: "DataFrame") -> "DataFrame":
    """Build Table -> Column edges for every column in scope."""
    return (
        columns_df
        .withColumn("source_id", id_expr("table_catalog", "table_schema", "table_name"))
        .withColumn("target_id", id_expr("table_catalog", "table_schema", "table_name", "column_name"))
        .select("source_id", "target_id")
    )


def build_references_rel(
    spark: "SparkSession", edges: "list[FKEdge]",
) -> "DataFrame":
    """Wrap FKEdge dataclasses in the canonical REFERENCES 5-col schema.

    Source-agnostic: accepts edges with any EdgeSource tag (DECLARED,
    INFERRED_METADATA, SEMANTIC). The enum `.value` is serialized at this
    tuple boundary — no magic strings downstream.
    """
    from pyspark.sql.types import DoubleType, StringType, StructField, StructType

    schema = StructType([
        StructField("source_id", StringType(), False),
        StructField("target_id", StringType(), False),
        StructField("confidence", DoubleType(), False),
        StructField("source", StringType(), False),
        StructField("criteria", StringType(), True),
    ])
    tuples = [
        (e.source_id, e.target_id, e.confidence, e.source.value, e.criteria)
        for e in edges
    ]
    return spark.createDataFrame(tuples, schema=schema)
