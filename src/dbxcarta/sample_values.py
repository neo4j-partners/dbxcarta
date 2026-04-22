"""Phase 2 — Sample Value Job.

For every STRING/BOOLEAN column in Neo4j whose approx_count_distinct is
below DBXCARTA_SAMPLE_CARDINALITY_THRESHOLD, sample up to
DBXCARTA_SAMPLE_LIMIT most-frequent distinct values and emit them as
:Value nodes with (:Column)-[:HAS_VALUE]->(:Value) edges.

All data-plane work is per-table batched via LATERAL VIEW STACK so the
whole job is one Spark SQL statement per table, not per column.
"""

from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass

from pydantic import field_validator
from pydantic_settings import BaseSettings

from dbxcarta.contract import (
    CONTRACT_VERSION,
    LABEL_COLUMN,
    LABEL_VALUE,
    REL_HAS_VALUE,
    generate_value_id,
)
from dbxcarta.summary import RunSummary
from dbxcarta.writer import Neo4jConfig, write_nodes, write_relationship

logger = logging.getLogger(__name__)

_IDENTIFIER_RE = re.compile(r"^[a-zA-Z0-9_.`\-]+$")


class Settings(BaseSettings):
    databricks_secret_scope: str = "dbxcarta-neo4j"
    dbxcarta_catalog: str
    dbxcarta_schemas: str = ""
    dbxcarta_summary_volume: str
    dbxcarta_summary_table: str
    dbxcarta_write_partitions: int = 8
    dbxcarta_sample_limit: int = 10
    dbxcarta_sample_cardinality_threshold: int = 50
    dbxcarta_stack_chunk_size: int = 50

    @field_validator("dbxcarta_catalog", "dbxcarta_summary_table")
    @classmethod
    def _validate_identifier(cls, v: str) -> str:
        if not _IDENTIFIER_RE.match(v):
            raise ValueError(f"Invalid Databricks identifier: {v!r}")
        return v


@dataclass
class TableCandidate:
    catalog: str
    schema_name: str
    table_name: str
    column_names: list[str]
    column_ids: list[str]

    def fq(self) -> str:
        return f"`{self.catalog}`.`{self.schema_name}`.`{self.table_name}`"


def run_sample() -> None:
    settings = Settings()

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    run_id = os.environ.get("DATABRICKS_JOB_RUN_ID", "local")
    schema_list = [s.strip() for s in settings.dbxcarta_schemas.split(",") if s.strip()]

    summary = RunSummary(
        run_id=run_id,
        job_name="sample_values",
        contract_version=CONTRACT_VERSION,
        catalog=settings.dbxcarta_catalog,
        schemas=schema_list,
    )

    try:
        _run(spark, settings, schema_list, summary)
        summary.finish(status="success")
    except Exception as exc:
        summary.finish(status="failure", error=str(exc))
        raise
    finally:
        summary.emit(spark, settings.dbxcarta_summary_volume, settings.dbxcarta_summary_table)


def _run(spark, settings: Settings, schema_list: list[str], summary: RunSummary) -> None:
    from databricks.sdk.runtime import dbutils

    scope = settings.databricks_secret_scope
    neo4j = Neo4jConfig(
        uri=dbutils.secrets.get(scope=scope, key="uri"),
        username=dbutils.secrets.get(scope=scope, key="username"),
        password=dbutils.secrets.get(scope=scope, key="password"),
    )

    _preflight(spark, settings, neo4j)
    _bootstrap_constraints(neo4j)

    candidates = _read_candidates(neo4j, settings.dbxcarta_catalog, schema_list)
    total_candidate_cols = sum(len(c.column_names) for c in candidates)
    summary.row_counts["candidate_columns"] = total_candidate_cols
    logger.info(
        "[dbxcarta] candidates: tables=%d columns=%d",
        len(candidates), total_candidate_cols,
    )

    # Per-schema read probe: drop schemas we cannot SELECT from.
    candidates, skipped_schemas = _filter_readable_schemas(spark, candidates)
    summary.row_counts["skipped_schemas"] = skipped_schemas

    # --- Cardinality pre-pass ---
    sampled_candidates, cardinality_values = _cardinality_filter(
        spark, candidates,
        settings.dbxcarta_sample_cardinality_threshold,
        settings.dbxcarta_stack_chunk_size,
    )
    sampled_cols = sum(len(c.column_names) for c in sampled_candidates)
    summary.row_counts["sampled_columns"] = sampled_cols
    summary.row_counts["skipped_columns"] = total_candidate_cols - sampled_cols
    _record_cardinality_stats(summary, cardinality_values)
    logger.info(
        "[dbxcarta] cardinality filter: kept=%d dropped=%d",
        sampled_cols, total_candidate_cols - sampled_cols,
    )

    # --- Sampling ---
    rows = _sample_values(
        spark, sampled_candidates,
        settings.dbxcarta_sample_limit,
        settings.dbxcarta_stack_chunk_size,
    )
    summary.row_counts["value_nodes"] = len(rows)
    logger.info("[dbxcarta] sampled value rows: %d", len(rows))

    if not rows:
        logger.warning("[dbxcarta] no value rows produced; skipping writes")
        summary.neo4j_counts = _query_neo4j_counts(neo4j)
        return

    # --- Build node + relationship DataFrames ---
    from pyspark.sql import Row
    from pyspark.sql.types import LongType, StringType, StructField, StructType

    value_node_rows = []
    has_value_rows = []
    seen_value_ids: set[str] = set()
    for (col_id, col_name, val, cnt) in rows:
        vid = generate_value_id(col_id, val)
        has_value_rows.append(Row(source_id=col_id, target_id=vid))
        if vid in seen_value_ids:
            continue
        seen_value_ids.add(vid)
        value_node_rows.append(
            Row(id=vid, value=val, count=int(cnt), contract_version=CONTRACT_VERSION)
        )

    value_schema = StructType([
        StructField("id", StringType(), nullable=False),
        StructField("value", StringType()),
        StructField("count", LongType()),
        StructField("contract_version", StringType()),
    ])
    rel_schema = StructType([
        StructField("source_id", StringType(), nullable=False),
        StructField("target_id", StringType(), nullable=False),
    ])

    value_node_df = spark.createDataFrame(value_node_rows, schema=value_schema)
    has_value_df = spark.createDataFrame(has_value_rows, schema=rel_schema)

    partitions = settings.dbxcarta_write_partitions
    logger.info("[dbxcarta] writing nodes: Value (%d)", len(value_node_rows))
    write_nodes(value_node_df.repartition(partitions), neo4j, LABEL_VALUE)

    logger.info("[dbxcarta] writing relationships: HAS_VALUE (%d)", len(has_value_rows))
    write_relationship(
        has_value_df.repartition(partitions), neo4j, REL_HAS_VALUE, LABEL_COLUMN, LABEL_VALUE,
    )

    summary.neo4j_counts = _query_neo4j_counts(neo4j)
    logger.info("[dbxcarta] neo4j counts: %s", summary.neo4j_counts)


def _chunk(lst: list, n: int):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def _cardinality_query(fq_table: str, column_names: list[str]) -> str:
    """Flat single-row aggregate: one approx_count_distinct per column."""
    aggs = ", ".join(
        f"approx_count_distinct(`{c}`) AS `card_{i}`"
        for i, c in enumerate(column_names)
    )
    return f"SELECT {aggs} FROM {fq_table}"


def _sample_query(fq_table: str, column_names: list[str]) -> str:
    n = len(column_names)
    stack_expr = ", ".join(f"'{c}', CAST(`{c}` AS STRING)" for c in column_names)
    return (
        f"SELECT col_name, val, COUNT(*) AS cnt"
        f"  FROM {fq_table}"
        f"  LATERAL VIEW STACK({n}, {stack_expr}) t AS col_name, val"
        f" WHERE val IS NOT NULL"
        f" GROUP BY col_name, val"
    )


def _cardinality_filter(
    spark, candidates: list[TableCandidate], threshold: int, chunk_size: int,
) -> tuple[list[TableCandidate], list[int]]:
    """Return (filtered candidates, all observed cardinality values).

    Per table, runs one flat SELECT of approx_count_distinct per chunk of
    columns. The chunked queries re-scan the table but avoid arity limits
    on wide tables. Returns the single-row result zipped back to column
    names in Python.
    """
    kept: list[TableCandidate] = []
    all_cards: list[int] = []
    for cand in candidates:
        kept_names: list[str] = []
        kept_ids: list[str] = []
        id_by_name = dict(zip(cand.column_names, cand.column_ids))
        try:
            for chunk_cols in _chunk(cand.column_names, chunk_size):
                query = _cardinality_query(cand.fq(), chunk_cols)
                row = spark.sql(query).collect()[0]
                for i, name in enumerate(chunk_cols):
                    card = int(row[f"card_{i}"])
                    all_cards.append(card)
                    if card < threshold:
                        kept_names.append(name)
                        kept_ids.append(id_by_name[name])
        except Exception as exc:
            logger.warning(
                "[dbxcarta] cardinality probe failed for %s: %s — skipping table",
                cand.fq(), exc,
            )
            continue
        if kept_names:
            kept.append(TableCandidate(
                catalog=cand.catalog,
                schema_name=cand.schema_name,
                table_name=cand.table_name,
                column_names=kept_names,
                column_ids=kept_ids,
            ))
    return kept, all_cards


def _sample_values(
    spark, candidates: list[TableCandidate], limit: int, chunk_size: int,
) -> list[tuple[str, str, str, int]]:
    """Return list of (col_id, col_name, val, cnt), top-`limit` per column.

    SQL does not sort; we rank in Python to avoid a cross-result shuffle.
    """
    out: list[tuple[str, str, str, int]] = []
    for cand in candidates:
        id_by_name = dict(zip(cand.column_names, cand.column_ids))
        try:
            per_col: dict[str, list[tuple[str, int]]] = {}
            for chunk_cols in _chunk(cand.column_names, chunk_size):
                query = _sample_query(cand.fq(), chunk_cols)
                for r in spark.sql(query).collect():
                    per_col.setdefault(r["col_name"], []).append(
                        (r["val"], int(r["cnt"]))
                    )
            for name, pairs in per_col.items():
                top = sorted(pairs, key=lambda p: p[1], reverse=True)[:limit]
                for val, cnt in top:
                    out.append((id_by_name[name], name, val, cnt))
        except Exception as exc:
            logger.warning(
                "[dbxcarta] sampling failed for %s: %s — skipping table",
                cand.fq(), exc,
            )
            continue
    return out


def _filter_readable_schemas(
    spark, candidates: list[TableCandidate],
) -> tuple[list[TableCandidate], int]:
    """Probe one table per schema; drop schemas that raise on SELECT."""
    by_schema: dict[tuple[str, str], list[TableCandidate]] = {}
    for c in candidates:
        by_schema.setdefault((c.catalog, c.schema_name), []).append(c)

    readable: list[TableCandidate] = []
    skipped = 0
    for (cat, sch), cands in by_schema.items():
        probe = cands[0]
        try:
            spark.sql(f"SELECT 1 FROM {probe.fq()} LIMIT 1").collect()
        except Exception as exc:
            skipped += 1
            logger.warning(
                "[dbxcarta] skipping schema %s.%s (probe failed): %s", cat, sch, exc,
            )
            continue
        readable.extend(cands)
    return readable, skipped


def _record_cardinality_stats(summary: RunSummary, values: list[int]) -> None:
    if not values:
        return
    s = sorted(values)

    def _pct(p: float) -> int:
        if not s:
            return 0
        idx = min(len(s) - 1, max(0, int(round(p * (len(s) - 1)))))
        return int(s[idx])

    summary.row_counts["cardinality_min"] = int(s[0])
    summary.row_counts["cardinality_p25"] = _pct(0.25)
    summary.row_counts["cardinality_p50"] = _pct(0.50)
    summary.row_counts["cardinality_p75"] = _pct(0.75)
    summary.row_counts["cardinality_p95"] = _pct(0.95)
    summary.row_counts["cardinality_max"] = int(s[-1])


def _read_candidates(
    config: Neo4jConfig, catalog: str, schema_list: list[str],
) -> list[TableCandidate]:
    from neo4j import GraphDatabase

    query = """
    MATCH (col:Column)
    WHERE col.data_type IN ['STRING', 'BOOLEAN']
    MATCH (tbl:Table)-[:HAS_COLUMN]->(col)
    MATCH (sch:Schema)-[:HAS_TABLE]->(tbl)
    MATCH (db:Database)-[:HAS_SCHEMA]->(sch)
    WHERE db.name = $catalog
    RETURN db.name AS catalog, sch.name AS schema_name, tbl.name AS table_name,
           collect(col.name) AS column_names, collect(col.id) AS column_ids
    ORDER BY catalog, schema_name, table_name
    """
    out: list[TableCandidate] = []
    with GraphDatabase.driver(config.uri, auth=(config.username, config.password)) as driver:
        with driver.session() as session:
            for r in session.run(query, catalog=catalog):
                sch = r["schema_name"]
                if schema_list and sch not in schema_list:
                    continue
                out.append(TableCandidate(
                    catalog=r["catalog"],
                    schema_name=sch,
                    table_name=r["table_name"],
                    column_names=list(r["column_names"]),
                    column_ids=list(r["column_ids"]),
                ))
    return out


def _preflight(spark, settings: Settings, config: Neo4jConfig) -> None:
    catalog = settings.dbxcarta_catalog
    spark.sql(
        f"SELECT 1 FROM `{catalog}`.information_schema.schemata LIMIT 1"
    ).collect()

    volume_path = settings.dbxcarta_summary_volume
    parts = volume_path.lstrip("/").split("/")
    if len(parts) < 4 or parts[0] != "Volumes":
        raise RuntimeError(
            f"[dbxcarta] DBXCARTA_SUMMARY_VOLUME must be a /Volumes/<catalog>/<schema>/<volume> path,"
            f" got {volume_path!r}"
        )
    vol_catalog, vol_schema, vol_name = parts[1], parts[2], parts[3]
    spark.sql(
        f"CREATE VOLUME IF NOT EXISTS `{vol_catalog}`.`{vol_schema}`.`{vol_name}`"
    )

    table = settings.dbxcarta_summary_table
    quoted_table = ".".join(f"`{p}`" for p in table.split("."))
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {quoted_table} (
            run_id STRING NOT NULL,
            job_name STRING,
            contract_version STRING,
            catalog STRING,
            schemas ARRAY<STRING>,
            started_at TIMESTAMP,
            ended_at TIMESTAMP,
            status STRING,
            row_counts MAP<STRING, BIGINT>,
            neo4j_counts MAP<STRING, BIGINT>,
            error STRING
        ) USING DELTA
    """)

    # Phase 2-specific: require Column nodes from Phase 1.
    from neo4j import GraphDatabase
    with GraphDatabase.driver(config.uri, auth=(config.username, config.password)) as driver:
        with driver.session() as session:
            cnt = session.run("MATCH (n:Column) RETURN count(n) AS cnt").single()["cnt"]
            if cnt == 0:
                raise RuntimeError(
                    "[dbxcarta] Phase 2 preflight failed: no Column nodes in Neo4j. "
                    "Run Phase 1 (DBXCARTA_JOB=schema) before Phase 2."
                )

    logger.info(
        "[dbxcarta] preflight passed: %s.information_schema accessible, volume/table ready, columns=%d",
        catalog, cnt,
    )


def _bootstrap_constraints(config: Neo4jConfig) -> None:
    from neo4j import GraphDatabase
    from neo4j.exceptions import ClientError

    with GraphDatabase.driver(config.uri, auth=(config.username, config.password)) as driver:
        with driver.session() as session:
            try:
                session.run(
                    f"CREATE CONSTRAINT {LABEL_VALUE.lower()}_id IF NOT EXISTS "
                    f"FOR (n:{LABEL_VALUE}) REQUIRE n.id IS UNIQUE"
                )
            except ClientError as exc:
                if "ConstraintAlreadyExists" not in (exc.code or ""):
                    raise
    logger.info("[dbxcarta] neo4j Value constraint bootstrapped")


def _query_neo4j_counts(config: Neo4jConfig) -> dict[str, int]:
    from neo4j import GraphDatabase

    counts: dict[str, int] = {}
    with GraphDatabase.driver(config.uri, auth=(config.username, config.password)) as driver:
        with driver.session() as session:
            for record in session.run(
                "MATCH (n) RETURN labels(n)[0] AS label, count(*) AS cnt"
            ):
                counts[record["label"]] = record["cnt"]
            for rel_type in ("HAS_SCHEMA", "HAS_TABLE", "HAS_COLUMN", REL_HAS_VALUE):
                result = session.run(f"MATCH ()-[r:{rel_type}]->() RETURN count(r) AS cnt")
                counts[rel_type] = result.single()["cnt"]
    return counts
