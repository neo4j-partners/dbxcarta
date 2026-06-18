"""Neo4j-side I/O: connection config, bootstrap, purge, counts, graph load.

Everything that holds a neo4j `Driver` or issues Cypher lives here, plus the
`Neo4jConfig` connection details and the node/relationship writes that go
through the Neo4j Spark Connector. The orchestrator deals in typed labels and
relationship enums; this module owns the connector-facing Cypher and graph
maintenance details.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from dbxcarta.spark.contract import REFERENCES_PROPERTIES, NodeLabel, RelType

if TYPE_CHECKING:
    from dbxcarta.spark.settings import SparkIngestSettings
    from neo4j import Driver
    from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)

_FORMAT = "org.neo4j.spark.DataSource"

# Labels that get a `{label}_full_text_index`. Database is excluded — it carries
# only `id`/`name` and is never the target of keyword search. Each index covers
# `name`, `comment`, and `id`: the `id` is the readable `catalog.schema.table`
# dotted path (contract 1.6 makes it injective), and Lucene tokenizes the dots
# so the bare name still matches exactly while the catalog and schema words
# become searchable too. This is a lexical-only choice — embeddings stay on the
# `name | type | comment` text (see EMBEDDING_TEXT_EXPR) so vectors are not
# diluted with non-semantic catalog/schema words.
_FULL_TEXT_INDEX_LABELS = (
    NodeLabel.SCHEMA,
    NodeLabel.TABLE,
    NodeLabel.COLUMN,
)
_FULL_TEXT_PROPERTIES = ("name", "comment", "id")


@dataclass(frozen=True)
class Neo4jConfig:
    uri: str
    username: str
    password: str
    batch_size: int = 20000

    def _base_opts(self) -> dict[str, str]:
        return {
            "url": self.uri,
            "authentication.type": "basic",
            "authentication.basic.username": self.username,
            "authentication.basic.password": self.password,
            "batch.size": str(self.batch_size),
        }


def _single_count(result: Any) -> int:
    record = result.single()
    if record is None:
        raise RuntimeError("Neo4j count query returned no rows")
    return int(record["cnt"])


def bootstrap_constraints(driver: Driver, settings: SparkIngestSettings) -> None:
    """Create id-uniqueness constraints, lookup/full-text indexes, and per-label
    vector indexes when the matching embedding flag is enabled.
    """
    from neo4j.exceptions import ClientError

    embedding_label_flags = [
        (settings.dbxcarta_include_embeddings_tables, NodeLabel.TABLE),
        (settings.dbxcarta_include_embeddings_columns, NodeLabel.COLUMN),
        (settings.dbxcarta_include_embeddings_values, NodeLabel.VALUE),
        (settings.dbxcarta_include_embeddings_schemas, NodeLabel.SCHEMA),
        (settings.dbxcarta_include_embeddings_databases, NodeLabel.DATABASE),
    ]
    dim = settings.dbxcarta_embedding_dimension

    with driver.session() as session:
        for label in NodeLabel:
            try:
                session.run(
                    f"CREATE CONSTRAINT {label.value.lower()}_id IF NOT EXISTS "
                    f"FOR (n:{label.value}) REQUIRE n.id IS UNIQUE"
                )
            except ClientError as exc:
                if "ConstraintAlreadyExists" not in (exc.code or ""):
                    raise
                logger.info(
                    "[dbxcarta] constraint for %s already satisfied, skipping",
                    label.value,
                )

        session.run(
            f"CREATE INDEX {NodeLabel.COLUMN.value.lower()}_data_type IF NOT EXISTS "
            f"FOR (n:{NodeLabel.COLUMN.value}) ON (n.data_type)"
        )

        # RANGE index over the contract-1.3 Value run-stamp. The scoped
        # stale-Value delete keys on `last_run < datetime($run_start)`; this
        # index turns that predicate into a bounded range scan instead of a
        # full :Value label sweep at the dense-catalog target.
        session.run(
            f"CREATE INDEX {NodeLabel.VALUE.value.lower()}_last_run "
            f"IF NOT EXISTS FOR (n:{NodeLabel.VALUE.value}) ON (n.last_run)"
        )

        # Full-text indexes back the hybrid retrieval's keyword half. Without
        # them, name/comment keyword search returns nothing on the graph. One
        # `{label}_full_text_index` per Schema/Table/Column over name/comment/id;
        # `IF NOT EXISTS` makes this idempotent across reruns.
        for label in _FULL_TEXT_INDEX_LABELS:
            props = ", ".join(f"n.{p}" for p in _FULL_TEXT_PROPERTIES)
            session.run(
                f"CREATE FULLTEXT INDEX {label.value.lower()}_full_text_index "
                f"IF NOT EXISTS FOR (n:{label.value}) ON EACH [{props}]"
            )

        for enabled, label in embedding_label_flags:
            if enabled:
                session.run(
                    f"CREATE VECTOR INDEX {label.value.lower()}_embedding IF NOT EXISTS "
                    f"FOR (n:{label.value}) ON n.embedding "
                    f"OPTIONS {{indexConfig: {{`vector.dimensions`: {dim},"
                    f" `vector.similarity_function`: 'cosine'}}}}"
                )

    logger.info("[dbxcarta] neo4j constraints and indexes bootstrapped")


def delete_stale_values(
    driver: Driver,
    run_start_iso: str,
    catalogs: list[str],
    schemas: list[str],
) -> None:
    """Delete Value nodes left over from a prior run, scoped to this run.

    A single server-side Cypher delete: any :Value within the run's
    catalogs (and schemas, when schema-scoped) whose `last_run` predates
    this run's start was not refreshed by the per-chunk Value writes and is
    therefore stale. Replaces the old driver-collected `IN $col_ids` purge,
    which paged catalog-scale column ids back to the driver
    (best-practices §5). Keyed on the contract-1.3 `last_run`/`catalog`/
    `schema` Value properties; the `:Value(last_run)` RANGE index makes the
    predicate a bounded index scan rather than a label sweep.
    """
    with driver.session() as session:
        session.run(
            f"MATCH (v:{NodeLabel.VALUE.value}) "
            "WHERE v.catalog IN $catalogs "
            "AND (size($schemas) = 0 OR v.schema IN $schemas) "
            "AND v.last_run < datetime($run_start) "
            "DETACH DELETE v",
            catalogs=catalogs,
            schemas=schemas,
            run_start=run_start_iso,
        )
    logger.info(
        "[dbxcarta] deleted stale Values older than run start %s",
        run_start_iso,
    )


def query_counts(driver: Driver) -> dict[str, int]:
    """Post-load Cypher count probes. Keyed by enum .value for JSON serializability."""
    counts: dict[str, int] = {}
    with driver.session() as session:
        for label in NodeLabel:
            result = session.run(f"MATCH (n:{label.value}) RETURN count(n) AS cnt")
            counts[label.value] = _single_count(result)
        for rel_type in RelType:
            result = session.run(f"MATCH ()-[r:{rel_type.value}]->() RETURN count(r) AS cnt")
            counts[rel_type.value] = _single_count(result)
    return counts


def write_node(df: DataFrame, neo4j: Neo4jConfig, label: NodeLabel) -> None:
    """MERGE nodes on id via the Neo4j Spark Connector, updating properties on match.

    All pipeline node writes go through here. Single-label only: the connector
    MERGEs on `node.keys=id`, and adding a second label here would compile to
    `MERGE (n:Label:Extra {id})`, which can never match an existing single-label
    node and so collides with the id uniqueness constraint. Add extra labels
    server-side off a written property instead.
    """
    (
        df.write.format(_FORMAT)
        .mode("Overwrite")
        .options(**neo4j._base_opts())
        .option("labels", f":{label.value}")
        .option("node.keys", "id")
        .save()
    )


def write_rel(
    df: DataFrame,
    neo4j: Neo4jConfig,
    rel_type: RelType,
    source_label: NodeLabel,
    target_label: NodeLabel,
    *,
    source_col: str = "source_id",
    target_col: str = "target_id",
    properties: tuple[str, ...] = (),
) -> None:
    """MERGE a relationship between existing nodes matched by id.

    When `properties` is non-empty, those DataFrame columns are written as
    relationship properties. The Neo4j Spark Connector's `keys` strategy
    requires an explicit `relationship.properties` option — extra columns
    on the DataFrame are otherwise ignored. Keeping the option conditional
    lets simple relationships use only source/target ids while REFERENCES
    edges can persist provenance, confidence, and criteria.
    """
    writer = (
        df.write.format(_FORMAT)
        .mode("Overwrite")
        .options(**neo4j._base_opts())
        .option("relationship", rel_type.value)
        .option("relationship.save.strategy", "keys")
        .option("relationship.source.labels", f":{source_label.value}")
        .option("relationship.source.save.mode", "Match")
        .option("relationship.source.node.keys", f"{source_col}:id")
        .option("relationship.target.labels", f":{target_label.value}")
        .option("relationship.target.save.mode", "Match")
        .option("relationship.target.node.keys", f"{target_col}:id")
    )
    if properties:
        writer = writer.option("relationship.properties", ",".join(properties))
    writer.save()


__all__ = [
    "REFERENCES_PROPERTIES",
    "Neo4jConfig",
    "bootstrap_constraints",
    "delete_stale_values",
    "query_counts",
    "write_node",
    "write_rel",
]
