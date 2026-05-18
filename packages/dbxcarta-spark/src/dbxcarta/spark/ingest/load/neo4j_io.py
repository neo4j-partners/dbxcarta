"""Neo4j-side I/O: bootstrap, purge, per-label count queries, graph load.

Everything that holds a neo4j `Driver` or issues Cypher lives here. The
orchestrator deals in typed labels and relationship enums; this module owns
the connector-facing Cypher and graph maintenance details.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from dbxcarta.spark.contract import NodeLabel, REFERENCES_PROPERTIES, RelType
from dbxcarta.spark.ingest.load.writer import (
    write_nodes,
    write_relationship,
)

# Second label applied to key-like FK target columns (in addition to
# :Column). Not a NodeLabel enum member: it carries no separate identity
# (MERGE stays on the :Column id) and only exists to scope the dedicated
# FK-discovery vector index away from the client's all-:Column index.
KEY_COLUMN_LABEL = "KeyColumn"

if TYPE_CHECKING:
    from neo4j import Driver
    from pyspark.sql import DataFrame

    from dbxcarta.spark.settings import SparkIngestSettings
    from dbxcarta.spark.ingest.load.writer import Neo4jConfig

logger = logging.getLogger(__name__)


def _single_count(result: Any) -> int:
    record = result.single()
    if record is None:
        raise RuntimeError("Neo4j count query returned no rows")
    return int(record["cnt"])


def bootstrap_constraints(driver: "Driver", settings: "SparkIngestSettings") -> None:
    """Create id-uniqueness constraints, data_type index, and per-label vector
    indexes when the matching embedding flag is enabled."""
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

        for enabled, label in embedding_label_flags:
            if enabled:
                session.run(
                    f"CREATE VECTOR INDEX {label.value.lower()}_embedding IF NOT EXISTS "
                    f"FOR (n:{label.value}) ON n.embedding "
                    f"OPTIONS {{indexConfig: {{`vector.dimensions`: {dim},"
                    f" `vector.similarity_function`: 'cosine'}}}}"
                )

        # Dedicated FK-discovery vector index over the key-like target
        # subset (:KeyColumn). Separate from column_embedding (which the
        # client RAG retriever seeds from any :Column and must keep its
        # full scope): a nearest-neighbour query against this index can
        # only return real key targets, so nothing is post-filtered away.
        if settings.dbxcarta_include_embeddings_columns:
            session.run(
                f"CREATE VECTOR INDEX {KEY_COLUMN_LABEL.lower()}_embedding"
                f" IF NOT EXISTS FOR (n:{KEY_COLUMN_LABEL}) ON n.embedding "
                f"OPTIONS {{indexConfig: {{`vector.dimensions`: {dim},"
                f" `vector.similarity_function`: 'cosine'}}}}"
            )

    logger.info("[dbxcarta] neo4j constraints and indexes bootstrapped")


def delete_stale_values(
    driver: "Driver",
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


def apply_key_column_labels(
    driver: "Driver",
    catalogs: list[str],
    schemas: list[str],
) -> None:
    """Reconcile the :KeyColumn label from the Column `is_key_like` property.

    One scoped server-side pass over the run's catalogs/schemas: add
    :KeyColumn to every Column flagged `is_key_like`, and strip it from
    every Column that is not. The label is a per-run projection of the
    contract-1.4 `is_key_like` property, never a connector multi-label node
    write. A connector write of `(:Column:KeyColumn)` compiles to
    `MERGE (n:Column:KeyColumn {id})`, which matches the full label set:
    it can never match an existing single-label `:Column` node and instead
    creates one, colliding with the Column.id uniqueness constraint. Driving
    the label off a written property and `SET`/`REMOVE` on already-matched
    nodes avoids that entirely and heals on a re-run. Scoped exactly like
    `delete_stale_values` (bounded catalog/schema config scalars, never a
    per-column id list, best-practices §5).
    """
    scope = (
        "WHERE c.catalog IN $catalogs "
        "AND (size($schemas) = 0 OR c.schema IN $schemas) "
    )
    with driver.session() as session:
        session.run(
            f"MATCH (c:{NodeLabel.COLUMN.value}) {scope}"
            "AND c.is_key_like = true "
            f"SET c:{KEY_COLUMN_LABEL}",
            catalogs=catalogs,
            schemas=schemas,
        )
        session.run(
            f"MATCH (c:{NodeLabel.COLUMN.value}:{KEY_COLUMN_LABEL}) {scope}"
            "AND (c.is_key_like IS NULL OR c.is_key_like = false) "
            f"REMOVE c:{KEY_COLUMN_LABEL}",
            catalogs=catalogs,
            schemas=schemas,
        )
    logger.info(
        "[dbxcarta] reconciled :%s labels from is_key_like in run scope",
        KEY_COLUMN_LABEL,
    )


def query_counts(driver: "Driver") -> dict[str, int]:
    """Post-load Cypher count probes. Keyed by enum .value for JSON serializability."""
    counts: dict[str, int] = {}
    with driver.session() as session:
        for label in NodeLabel:
            result = session.run(f"MATCH (n:{label.value}) RETURN count(n) AS cnt")
            counts[label.value] = _single_count(result)
        for rel_type in RelType:
            result = session.run(
                f"MATCH ()-[r:{rel_type.value}]->() RETURN count(r) AS cnt"
            )
            counts[rel_type.value] = _single_count(result)
    return counts


def write_node(df: "DataFrame", neo4j: "Neo4jConfig", label: NodeLabel) -> None:
    """Thin enum-typed wrapper — all pipeline node writes go through here."""
    write_nodes(df, neo4j, label.value)


def write_rel(
    df: "DataFrame",
    neo4j: "Neo4jConfig",
    rel_type: RelType,
    source_label: NodeLabel,
    target_label: NodeLabel,
    *,
    properties: tuple[str, ...] = (),
) -> None:
    write_relationship(
        df, neo4j, rel_type.value, source_label.value, target_label.value,
        properties=properties,
    )


__all__ = [
    "bootstrap_constraints",
    "delete_stale_values",
    "apply_key_column_labels",
    "query_counts",
    "write_node",
    "write_rel",
    "REFERENCES_PROPERTIES",
]
