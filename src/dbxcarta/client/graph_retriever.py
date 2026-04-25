"""GraphRetriever: Neo4j vector seed + structural walk for graph_rag arm."""

from __future__ import annotations

from neo4j import GraphDatabase

from dbxcarta.client.neo4j_utils import neo4j_credentials
from dbxcarta.client.retriever import ColumnEntry, ContextBundle, Retriever
from dbxcarta.client.settings import ClientSettings

_COL_INDEX = "column_embedding"
_TABLE_INDEX = "table_embedding"
_MAX_VALUES = 30

# OPTIONAL MATCH so deployments with no REFERENCES edges don't trigger
# Neo4j's 01N51 UnknownRelationshipTypeWarning. Exported as a constant so
# the regression guard can import it.
#
# `r` is bound so we can filter on confidence. Edges with no confidence
# property are treated as 1.0 via COALESCE so they are never silently dropped.
_REFERENCES_TABLE_IDS_CYPHER = (
    "UNWIND $col_ids AS cid "
    "OPTIONAL MATCH (c:Column {id: cid})-[r:REFERENCES]-(other:Column)"
    "<-[:HAS_COLUMN]-(t:Table) "
    "WITH t, r WHERE t IS NOT NULL AND COALESCE(r.confidence, 1.0) >= $threshold "
    "RETURN DISTINCT t.id AS tid"
)

# Fetch literal join predicates for retrieved neighbour tables so they can
# be injected into the SQL-generation prompt. Gated by DBXCARTA_INJECT_CRITERIA.
_REFERENCES_CRITERIA_CYPHER = (
    "UNWIND $col_ids AS cid "
    "OPTIONAL MATCH (c:Column {id: cid})-[r:REFERENCES]-(:Column) "
    "WITH r WHERE r IS NOT NULL "
    "  AND COALESCE(r.confidence, 1.0) >= $threshold "
    "  AND r.criteria IS NOT NULL "
    "RETURN DISTINCT r.criteria AS crit"
)


class GraphRetriever(Retriever):
    def __init__(self, settings: ClientSettings) -> None:
        self._settings = settings
        uri, username, password = neo4j_credentials(settings)
        self._driver = GraphDatabase.driver(uri, auth=(username, password))

    def close(self) -> None:
        self._driver.close()

    def retrieve(self, question: str, embedding: list[float]) -> ContextBundle:
        top_k = self._settings.dbxcarta_client_top_k
        catalog = self._settings.dbxcarta_catalog
        schemas = self._settings.schemas_list
        threshold = self._settings.dbxcarta_confidence_threshold
        inject_criteria = self._settings.dbxcarta_inject_criteria

        with self._driver.session() as session:
            col_seeds = _query_vector_seeds(session, _COL_INDEX, embedding, top_k)
            tbl_seeds = _query_vector_seeds(session, _TABLE_INDEX, embedding, top_k)
            parent_tbl_ids = _parent_table_ids(session, col_seeds)
            ref_tbl_ids = _references_table_ids(session, col_seeds, threshold)

            all_tbl_ids = list(dict.fromkeys(tbl_seeds + parent_tbl_ids + ref_tbl_ids))
            columns = _fetch_columns(session, all_tbl_ids, catalog, schemas)
            values = _fetch_values(session, col_seeds)
            criteria = (
                _references_criteria(session, col_seeds, threshold)
                if inject_criteria
                else []
            )

        return ContextBundle(
            columns=columns,
            values=values,
            seed_ids=col_seeds + tbl_seeds,
            criteria=criteria,
        )


def _query_vector_seeds(session, index: str, embedding: list[float], k: int) -> list[str]:
    result = session.run(
        f"CALL db.index.vector.queryNodes('{index}', $k, $vec) "
        "YIELD node, score "
        "RETURN node.id AS id",
        k=k,
        vec=list(embedding),
    )
    return [row["id"] for row in result]


def _parent_table_ids(session, col_ids: list[str]) -> list[str]:
    if not col_ids:
        return []
    result = session.run(
        "UNWIND $col_ids AS cid "
        "MATCH (c:Column {id: cid})<-[:HAS_COLUMN]-(t:Table) "
        "RETURN DISTINCT t.id AS tid",
        col_ids=col_ids,
    )
    return [row["tid"] for row in result]


def _references_table_ids(session, col_ids: list[str], threshold: float) -> list[str]:
    """Follow REFERENCES edges in both directions to find joinable tables."""
    if not col_ids:
        return []
    result = session.run(
        _REFERENCES_TABLE_IDS_CYPHER, col_ids=col_ids, threshold=threshold,
    )
    return [row["tid"] for row in result]


def _references_criteria(session, col_ids: list[str], threshold: float) -> list[str]:
    """Pull literal join predicates off REFERENCES edges above the threshold."""
    if not col_ids:
        return []
    result = session.run(
        _REFERENCES_CRITERIA_CYPHER, col_ids=col_ids, threshold=threshold,
    )
    return [row["crit"] for row in result if row["crit"] is not None]


def _fetch_columns(
    session, table_ids: list[str], catalog: str, schemas: list[str]
) -> list[ColumnEntry]:
    if not table_ids:
        return []
    result = session.run(
        "UNWIND $tids AS tid "
        "MATCH (s:Schema)-[:HAS_TABLE]->(t:Table {id: tid}) "
        "MATCH (t)-[:HAS_COLUMN]->(c:Column) "
        "WHERE size($schemas) = 0 OR s.name IN $schemas "
        "RETURN s.name AS schema_name, t.name AS table_name, "
        "       c.name AS col_name, c.data_type AS data_type, "
        "       c.comment AS comment, c.ordinal_position AS pos "
        "ORDER BY schema_name, table_name, pos",
        tids=table_ids,
        schemas=schemas,
    )
    entries: list[ColumnEntry] = []
    for row in result:
        fqt = f"{catalog}.{row['schema_name']}.{row['table_name']}"
        entries.append(ColumnEntry(
            table_fqn=fqt,
            column_name=row["col_name"],
            data_type=row["data_type"] or "",
            comment=row["comment"] or "",
        ))
    return entries


def _fetch_values(session, col_ids: list[str]) -> list[str]:
    if not col_ids:
        return []
    result = session.run(
        "UNWIND $col_ids AS cid "
        "MATCH (c:Column {id: cid})-[:HAS_VALUE]->(v:Value) "
        f"RETURN v.value AS val LIMIT {_MAX_VALUES}",
        col_ids=col_ids,
    )
    return [row["val"] for row in result if row["val"] is not None]
