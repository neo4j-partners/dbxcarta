"""Unity Catalog extraction — pure information_schema reads + node/rel DataFrame build.

Post-Phase-3.6 split: this module does *only* UC extraction and DataFrame
assembly. Inference (Phase 3 metadata, Phase 4 semantic) lives in
`inference.py` and runs against the extracted material. Embedding enrichment
happens later in the pipeline, before the Neo4j load.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

import dbxcarta.schema_graph as sg
from dbxcarta.summary import ExtractCounts, RunSummary

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

    from dbxcarta.settings import Settings

logger = logging.getLogger(__name__)


@dataclass
class ExtractResult:
    """Raw UC extraction + node/rel DataFrames. No inferred edges, no counters.

    Fields are mutable only because embedding enrichment replaces the node
    DataFrames in place after staging (same semantics as before the split)."""

    database_df: "DataFrame"
    schema_node_df: "DataFrame"
    table_node_df: "DataFrame"
    column_node_df: "DataFrame"
    has_schema_df: "DataFrame"
    has_table_df: "DataFrame"
    has_column_df: "DataFrame"
    references_df: "DataFrame"
    schemata_df: "DataFrame"
    tables_df: "DataFrame"
    columns_df: "DataFrame"
    fk_pairs_df: "DataFrame"
    declared_df: "DataFrame"

    def unpersist_cached(self) -> None:
        self.schemata_df.unpersist()
        self.tables_df.unpersist()
        self.columns_df.unpersist()
        self.fk_pairs_df.unpersist()
        self.declared_df.unpersist()


def extract(
    spark: "SparkSession",
    settings: "Settings",
    schema_list: list[str],
    summary: RunSummary,
) -> ExtractResult:
    """Pull information_schema metadata for the configured catalog/schemas.

    Populates `summary.extract` in the process — this is the one place the
    extract counts are recorded."""
    from pyspark.sql.functions import col

    catalog = settings.dbxcarta_catalog

    schemata_df = (
        spark.sql(
            f"SELECT catalog_name, schema_name, comment"
            f" FROM `{catalog}`.information_schema.schemata"
        )
        .filter(col("schema_name") != "information_schema")
    )
    if schema_list:
        schemata_df = schemata_df.filter(col("schema_name").isin(schema_list))
    schemata_df = schemata_df.cache()

    tables_df = (
        spark.sql(
            f"SELECT table_catalog, table_schema, table_name, table_type,"
            f"       comment, created, last_altered"
            f" FROM `{catalog}`.information_schema.tables"
        )
        .filter(col("table_schema") != "information_schema")
    )
    if schema_list:
        tables_df = tables_df.filter(col("table_schema").isin(schema_list))
    tables_df = tables_df.cache()

    columns_df = (
        spark.sql(
            f"SELECT table_catalog, table_schema, table_name, column_name,"
            f"       data_type, is_nullable, ordinal_position, comment"
            f" FROM `{catalog}`.information_schema.columns"
        )
        .filter(col("table_schema") != "information_schema")
    )
    if schema_list:
        columns_df = columns_df.filter(col("table_schema").isin(schema_list))
    columns_df = columns_df.cache()

    fk_pairs_df = spark.sql(
        f"SELECT rc.constraint_schema AS fk_schema,"
        f"       rc.constraint_name   AS fk_name,"
        f"       src.table_catalog AS src_catalog, src.table_schema AS src_schema,"
        f"       src.table_name    AS src_table,   src.column_name  AS src_column,"
        f"       tgt.table_catalog AS tgt_catalog, tgt.table_schema AS tgt_schema,"
        f"       tgt.table_name    AS tgt_table,   tgt.column_name  AS tgt_column,"
        f"       src.ordinal_position AS ord"
        f" FROM `{catalog}`.information_schema.referential_constraints rc"
        f" JOIN `{catalog}`.information_schema.key_column_usage src"
        f"   ON src.constraint_catalog = rc.constraint_catalog"
        f"  AND src.constraint_schema  = rc.constraint_schema"
        f"  AND src.constraint_name    = rc.constraint_name"
        f" JOIN `{catalog}`.information_schema.key_column_usage tgt"
        f"   ON tgt.constraint_catalog = rc.unique_constraint_catalog"
        f"  AND tgt.constraint_schema  = rc.unique_constraint_schema"
        f"  AND tgt.constraint_name    = rc.unique_constraint_name"
        f"  AND tgt.ordinal_position   = src.position_in_unique_constraint"
    )
    declared_df = spark.sql(
        f"SELECT constraint_schema, constraint_name"
        f" FROM `{catalog}`.information_schema.referential_constraints"
    )
    if schema_list:
        fk_pairs_df = fk_pairs_df.filter(
            col("fk_schema").isin(schema_list) & col("tgt_schema").isin(schema_list)
        )
        declared_df = declared_df.filter(col("constraint_schema").isin(schema_list))
    fk_pairs_df = fk_pairs_df.cache()
    declared_df = declared_df.cache()

    fk_declared = declared_df.count()
    fk_references = fk_pairs_df.count()
    fk_resolved = fk_pairs_df.select("fk_schema", "fk_name").distinct().count()
    fk_skipped = fk_declared - fk_resolved
    _log_unresolved_fks(fk_skipped, fk_pairs_df, declared_df)

    summary.extract = ExtractCounts(
        schemas=schemata_df.count(),
        tables=tables_df.count(),
        columns=columns_df.count(),
        fk_declared=fk_declared,
        fk_resolved=fk_resolved,
        fk_skipped=fk_skipped,
        fk_references=fk_references,
    )

    logger.info(
        "[dbxcarta] read: schemas=%d tables=%d columns=%d"
        " fk_declared=%d fk_resolved=%d fk_skipped=%d fk_references=%d",
        summary.extract.schemas,
        summary.extract.tables,
        summary.extract.columns,
        fk_declared, fk_resolved, fk_skipped, fk_references,
    )

    database_df = sg.build_database_node(spark, catalog)
    schema_node_df = sg.build_schema_nodes(schemata_df)
    table_node_df = sg.build_table_nodes(tables_df)
    column_node_df = sg.build_column_nodes(columns_df)
    has_schema_df = sg.build_has_schema_rel(schemata_df, catalog)
    has_table_df = sg.build_has_table_rel(tables_df)
    has_column_df = sg.build_has_column_rel(columns_df)
    references_df = sg.build_references_rel(fk_pairs_df)

    return ExtractResult(
        database_df=database_df,
        schema_node_df=schema_node_df,
        table_node_df=table_node_df,
        column_node_df=column_node_df,
        has_schema_df=has_schema_df,
        has_table_df=has_table_df,
        has_column_df=has_column_df,
        references_df=references_df,
        schemata_df=schemata_df,
        tables_df=tables_df,
        columns_df=columns_df,
        fk_pairs_df=fk_pairs_df,
        declared_df=declared_df,
    )


def _log_unresolved_fks(
    fk_skipped: int, fk_pairs_df: "DataFrame", declared_df: "DataFrame",
) -> None:
    if fk_skipped <= 0:
        return
    resolved_names = fk_pairs_df.select("fk_schema", "fk_name").distinct()
    skipped_rows = (
        declared_df
        .join(
            resolved_names,
            (declared_df.constraint_schema == resolved_names.fk_schema)
            & (declared_df.constraint_name == resolved_names.fk_name),
            "left_anti",
        )
        .collect()
    )
    for row in skipped_rows:
        logger.warning(
            "[dbxcarta] FK unresolved or out-of-scope (no target column pair in result, skipping): %s.%s",
            row["constraint_schema"],
            row["constraint_name"],
        )
