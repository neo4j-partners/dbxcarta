"""Integration tests for end-to-end semantic search through the graph.

Exercises the full client-side retrieval path described in the README: embed
a text string via the live Databricks Foundation Model API endpoint, run a
similarity search against the Neo4j vector index, then traverse graph
relationships to produce a schema subgraph.

Requires:
  - DBXCARTA_INCLUDE_EMBEDDINGS_TABLES=true in the most recent run — Table
    nodes are the only label that retains embedding_text, so they are the
    only label where re-embedding + self-rank is meaningful.
  - A Databricks workspace reachable with the current SDK auth configuration.
"""

from __future__ import annotations

import requests
import pytest

_TOP_K = 10
_SAMPLE = 5
_MIN_SELF_SIMILARITY = 0.99


def _tables_enabled(run_summary: dict) -> bool:
    flags = run_summary.get("embedding_flags") or {}
    return bool(flags.get("Table"))


def _embed_texts(ws, endpoint: str, texts: list[str]) -> list[list[float]]:
    """Call a Databricks Foundation Model API embedding endpoint.

    Uses ws.config.authenticate() so all auth schemes (PAT, OAuth, SP) work
    without assuming a token field is populated.
    """
    headers = ws.config.authenticate()
    resp = requests.post(
        f"{ws.config.host.rstrip('/')}/serving-endpoints/{endpoint}/invocations",
        headers=headers,
        json={"input": texts},
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()["data"]
    data.sort(key=lambda x: x["index"])
    return [item["embedding"] for item in data]


def test_embedding_endpoint_returns_correct_dimension(neo4j_driver, ws, run_summary) -> None:
    """The live endpoint returns a vector whose length matches the index dimension."""
    if not _tables_enabled(run_summary):
        pytest.skip("Table embeddings not enabled in the latest run")

    endpoint = run_summary.get("embedding_model", "databricks-gte-large-en")

    with neo4j_driver.session() as session:
        row = session.run(
            "SHOW VECTOR INDEXES YIELD name, options "
            "WHERE name = 'table_embedding'"
        ).single()
    assert row is not None, "Vector index 'table_embedding' not found"
    expected_dim = row["options"]["indexConfig"]["vector.dimensions"]

    vecs = _embed_texts(ws, endpoint, ["tables related to customer orders"])
    assert len(vecs) == 1
    assert len(vecs[0]) == expected_dim


def test_table_semantic_self_ranking(neo4j_driver, ws, run_summary) -> None:
    """Re-embedding a Table's own embedding_text must recover the node at cosine >= 0.99.

    databricks-gte-large-en is deterministic, so re-encoding the same string
    should return a nearly identical vector. A score below the threshold
    indicates either that the index is stale relative to the current model or
    that the wrong endpoint is configured.

    This test is graph-size-independent: it checks the cosine score directly
    rather than rank, so it passes on a 3-table fixture as well as a
    million-row catalog.
    """
    if not _tables_enabled(run_summary):
        pytest.skip("Table embeddings not enabled in the latest run")

    endpoint = run_summary.get("embedding_model", "databricks-gte-large-en")

    with neo4j_driver.session() as session:
        rows = list(session.run(
            "MATCH (n:Table) WHERE n.embedding IS NOT NULL AND n.embedding_text IS NOT NULL "
            f"RETURN n.id AS id, n.embedding_text AS text LIMIT {_SAMPLE}"
        ))

    if not rows:
        pytest.skip("No Table nodes with embedding_text found")

    texts = [r["text"] for r in rows]
    fresh_vecs = _embed_texts(ws, endpoint, texts)

    with neo4j_driver.session() as session:
        for row, vec in zip(rows, fresh_vecs):
            result = session.run(
                "CALL db.index.vector.queryNodes('table_embedding', $k, $vec) "
                "YIELD node, score WHERE node.id = $id RETURN score",
                k=_TOP_K,
                vec=vec,
                id=row["id"],
            ).single()
            assert result is not None, (
                f"Table '{row['id']}' not found in top-{_TOP_K} when its own "
                "embedding_text was re-embedded via the live endpoint"
            )
            assert result["score"] >= _MIN_SELF_SIMILARITY, (
                f"Table '{row['id']}' self-similarity score {result['score']:.4f} "
                f"< {_MIN_SELF_SIMILARITY} — index may be stale or model has drifted"
            )


def test_graph_expansion_from_vector_search(neo4j_driver, ws, run_summary) -> None:
    """A vector search result expands into a non-empty column subgraph.

    Mirrors the two-step retrieval pattern from the README: embed a query,
    find top-k Tables via similarity search, traverse HAS_COLUMN for the top
    result. Verifies that the combined pipeline returns structured schema
    context suitable for an LLM call.
    """
    if not _tables_enabled(run_summary):
        pytest.skip("Table embeddings not enabled in the latest run")

    endpoint = run_summary.get("embedding_model", "databricks-gte-large-en")

    with neo4j_driver.session() as session:
        probe = session.run(
            "MATCH (n:Table) WHERE n.embedding IS NOT NULL AND n.embedding_text IS NOT NULL "
            "RETURN n.id AS id, n.embedding_text AS text LIMIT 1"
        ).single()

    if probe is None:
        pytest.skip("No Table nodes with embedding_text found")

    (vec,) = _embed_texts(ws, endpoint, [probe["text"]])

    with neo4j_driver.session() as session:
        top_tables = list(session.run(
            "CALL db.index.vector.queryNodes('table_embedding', 5, $vec) "
            "YIELD node RETURN node.id AS id",
            vec=vec,
        ))
        assert top_tables, "Vector search returned no Table nodes"

        top_id = top_tables[0]["id"]
        columns = list(session.run(
            "MATCH (t:Table {id: $id})-[:HAS_COLUMN]->(c:Column) RETURN c.id AS id",
            id=top_id,
        ))

    assert columns, (
        f"Graph expansion from Table '{top_id}' returned no Column nodes — "
        "either the table has no columns in the graph or HAS_COLUMN relationships are missing"
    )
