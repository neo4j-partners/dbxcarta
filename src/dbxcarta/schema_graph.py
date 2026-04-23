"""Schema graph transforms: pure DataFrame builders for Unity Catalog metadata."""

from __future__ import annotations

from typing import TYPE_CHECKING

from dbxcarta.contract import CONTRACT_VERSION, generate_id, id_expr

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

    from dbxcarta.fk_inference import InferredRef


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
    """Declared-FK edges carry provenance (1.0, "declared", null).

    Inferred-source phases (metadata / semantic / query-log) write their own
    DataFrames with the same three columns and different values.
    """
    from pyspark.sql.functions import lit
    from pyspark.sql.types import StringType

    return (
        fk_pairs_df
        .withColumn("source_id", id_expr("src_catalog", "src_schema", "src_table", "src_column"))
        .withColumn("target_id", id_expr("tgt_catalog", "tgt_schema", "tgt_table", "tgt_column"))
        .withColumn("confidence", lit(1.0))
        .withColumn("source", lit("declared"))
        .withColumn("criteria", lit(None).cast(StringType()))
        .select("source_id", "target_id", "confidence", "source", "criteria")
    )


def build_inferred_metadata_references_rel(
    spark: "SparkSession", refs: "list[InferredRef]",
) -> "DataFrame":
    """Wrap inferred-reference dataclasses in the canonical REFERENCES 5-col schema.

    Separate write from build_references_rel so declared-duplicate suppression
    stays in the inference layer; this module only owns DataFrame shape.

    This is the single InferredRef → Spark-tuple conversion point (Phase 3.5
    boundary rule: one conversion per Spark edge).
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
        (r.source_id, r.target_id, r.confidence, r.source, r.criteria)
        for r in refs
    ]
    return spark.createDataFrame(tuples, schema=schema)
