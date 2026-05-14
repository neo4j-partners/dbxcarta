"""Schema graph transforms: pure DataFrame builders for Unity Catalog metadata.

Each function accepts DataFrames shaped like Unity Catalog information_schema
queries and returns connector-ready node or relationship DataFrames. Identifier
columns are generated through the shared contract helpers so Python and Spark
produce the same ids.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from dbxcarta.core.contract import CONTRACT_VERSION, generate_id, id_expr

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

    from dbxcarta.spark.ingest.fk.common import FKEdge


def build_database_node(spark: "SparkSession", catalog: str) -> "DataFrame":
    """Build the single Database node representing the configured catalog."""
    from pyspark.sql import Row

    return spark.createDataFrame(
        [Row(id=generate_id(catalog), name=catalog, contract_version=CONTRACT_VERSION)]
    )


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


def build_table_nodes(tables_df: "DataFrame") -> "DataFrame":
    """Build Table nodes from information_schema.tables rows.

    The selected columns form the public graph payload plus `table_schema`,
    which is retained until embedding text has been generated.
    """
    from pyspark.sql.functions import col, lit

    return (
        tables_df
        .withColumn("id", id_expr("table_catalog", "table_schema", "table_name"))
        .withColumn("name", col("table_name"))
        .withColumn("contract_version", lit(CONTRACT_VERSION))
        # table_schema is retained for the embedding text expression.
        .select("id", "name", "table_schema", "comment", "table_type", "created", "last_altered", "contract_version")
    )


def build_column_nodes(columns_df: "DataFrame") -> "DataFrame":
    """Build Column nodes from information_schema.columns rows.

    Converts Databricks YES/NO nullability strings into booleans while
    preserving ordinal position, data type, and comments for retrieval context.
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
        .select("id", "name", "table_schema", "table_name", "data_type", "is_nullable", "ordinal_position", "comment", "contract_version")
    )


def build_has_schema_rel(schemata_df: "DataFrame", catalog: str) -> "DataFrame":
    """Build Database -> Schema edges for every schema in scope."""
    from pyspark.sql.functions import lit

    return (
        schemata_df
        .withColumn("source_id", lit(generate_id(catalog)))
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
