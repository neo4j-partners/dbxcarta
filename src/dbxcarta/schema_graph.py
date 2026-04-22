"""Schema graph transforms: pure DataFrame builders for Unity Catalog metadata."""

from __future__ import annotations

from typing import TYPE_CHECKING

from dbxcarta.contract import CONTRACT_VERSION, generate_id, id_expr

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


def build_database_node(spark: "SparkSession", catalog: str) -> "DataFrame":
    from pyspark.sql import Row

    return spark.createDataFrame(
        [Row(id=generate_id(catalog), name=catalog, contract_version=CONTRACT_VERSION)]
    )


def build_schema_nodes(schemata_df: "DataFrame") -> "DataFrame":
    from pyspark.sql.functions import col, lit

    return (
        schemata_df
        .withColumn("id", id_expr("catalog_name", "schema_name"))
        .withColumn("name", col("schema_name"))
        .withColumn("contract_version", lit(CONTRACT_VERSION))
        .select("id", "name", "catalog_name", "comment", "contract_version")
    )


def build_table_nodes(tables_df: "DataFrame") -> "DataFrame":
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
    from pyspark.sql.functions import lit

    return (
        schemata_df
        .withColumn("source_id", lit(generate_id(catalog)))
        .withColumn("target_id", id_expr("catalog_name", "schema_name"))
        .select("source_id", "target_id")
    )


def build_has_table_rel(tables_df: "DataFrame") -> "DataFrame":
    return (
        tables_df
        .withColumn("source_id", id_expr("table_catalog", "table_schema"))
        .withColumn("target_id", id_expr("table_catalog", "table_schema", "table_name"))
        .select("source_id", "target_id")
    )


def build_has_column_rel(columns_df: "DataFrame") -> "DataFrame":
    return (
        columns_df
        .withColumn("source_id", id_expr("table_catalog", "table_schema", "table_name"))
        .withColumn("target_id", id_expr("table_catalog", "table_schema", "table_name", "column_name"))
        .select("source_id", "target_id")
    )


def build_references_rel(fk_pairs_df: "DataFrame") -> "DataFrame":
    return (
        fk_pairs_df
        .withColumn("source_id", id_expr("src_catalog", "src_schema", "src_table", "src_column"))
        .withColumn("target_id", id_expr("tgt_catalog", "tgt_schema", "tgt_table", "tgt_column"))
        .select("source_id", "target_id")
    )
