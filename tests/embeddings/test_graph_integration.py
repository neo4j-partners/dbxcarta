"""Integration tests for Table embeddings in the written graph.

Requires a completed run with DBXCARTA_INCLUDE_EMBEDDINGS_TABLES=true and
the fixtures (neo4j_driver, run_summary, ws) defined in the schema_graph
sibling conftest. Skips cleanly when those preconditions aren't met.
"""

from __future__ import annotations

import hashlib

import pytest

# Reuse integration fixtures from the schema_graph sibling conftest.
# pytest collects sibling conftests automatically only for nested directories;
# for sibling dirs we re-export explicitly.
from tests.schema_graph.conftest import neo4j_driver, run_summary, ws  # noqa: F401


def _tables_enabled(run_summary: dict) -> bool:
    flags = run_summary.get("embedding_flags") or {}
    return bool(flags.get("Table"))


def _columns_enabled(run_summary: dict) -> bool:
    flags = run_summary.get("embedding_flags") or {}
    return bool(flags.get("Column"))


def test_table_nodes_carry_five_embedding_properties(neo4j_driver, run_summary) -> None:
    if not _tables_enabled(run_summary):
        pytest.skip("Table embeddings not enabled in the latest run")

    with neo4j_driver.session() as session:
        result = session.run(
            "MATCH (n:Table) "
            "RETURN count(n) AS total, "
            "  count(n.embedding_text) AS has_text, "
            "  count(n.embedding_text_hash) AS has_hash, "
            "  count(n.embedding) AS has_embedding, "
            "  count(n.embedding_model) AS has_model, "
            "  count(n.embedded_at) AS has_ts"
        ).single()
    assert result["total"] > 0, "No Table nodes in graph"
    assert result["has_text"] == result["total"]
    assert result["has_hash"] == result["total"]
    assert result["has_embedding"] == result["total"]
    assert result["has_model"] == result["total"]
    assert result["has_ts"] == result["total"]


def test_embedding_text_hash_matches_sha256(neo4j_driver, run_summary) -> None:
    if not _tables_enabled(run_summary):
        pytest.skip("Table embeddings not enabled in the latest run")

    with neo4j_driver.session() as session:
        rows = list(session.run(
            "MATCH (n:Table) "
            "RETURN n.embedding_text AS text, n.embedding_text_hash AS hash "
            "LIMIT 25"
        ))
    assert rows, "No Table nodes sampled"
    for r in rows:
        expected = hashlib.sha256(r["text"].encode()).hexdigest()
        assert r["hash"] == expected, f"hash mismatch for text={r['text']!r}"


def test_embedding_error_not_persisted_to_neo4j(neo4j_driver, run_summary) -> None:
    """embedding_error is a transform-only column; it must not appear on nodes."""
    if not _tables_enabled(run_summary):
        pytest.skip("Table embeddings not enabled in the latest run")

    with neo4j_driver.session() as session:
        count = session.run(
            "MATCH (n:Table) WHERE n.embedding_error IS NOT NULL RETURN count(n) AS c"
        ).single()["c"]
    assert count == 0


def test_vector_index_exists_with_expected_config(neo4j_driver, run_summary) -> None:
    if not _tables_enabled(run_summary):
        pytest.skip("Table embeddings not enabled in the latest run")

    with neo4j_driver.session() as session:
        rows = list(session.run(
            "SHOW VECTOR INDEXES YIELD name, labelsOrTypes, properties, options "
            "WHERE name = 'table_embedding'"
        ))
    assert rows, "Vector index 'table_embedding' not found"
    idx = rows[0]
    assert "Table" in idx["labelsOrTypes"]
    assert "embedding" in idx["properties"]
    cfg = idx["options"]["indexConfig"]
    assert cfg["vector.similarity_function"].lower() == "cosine"
    assert cfg["vector.dimensions"] > 0


def test_cosine_probe_returns_neighbors(neo4j_driver, run_summary) -> None:
    """Pick one real Table, query the vector index with its own vector,
    expect at least one neighbor (itself) back."""
    if not _tables_enabled(run_summary):
        pytest.skip("Table embeddings not enabled in the latest run")

    with neo4j_driver.session() as session:
        probe = session.run(
            "MATCH (n:Table) WHERE n.embedding IS NOT NULL "
            "RETURN n.id AS id, n.embedding AS vec LIMIT 1"
        ).single()
        assert probe is not None, "No Table with an embedding"
        neighbors = list(session.run(
            "CALL db.index.vector.queryNodes('table_embedding', 5, $vec) "
            "YIELD node, score RETURN node.id AS id, score",
            vec=probe["vec"],
        ))
    assert neighbors, "Vector index returned no neighbors for a known-good vector"


def test_staging_delta_populated_for_enabled_labels(run_summary) -> None:
    """Stage 2 materialize-once: for every embedding-enabled label, the staging
    Delta directory exists and an embedding-attempt count was recorded.
    Checked via the FUSE-mounted /Volumes path; the test is a no-op unless the
    run submitted from a Databricks environment where that path is visible."""
    import os

    flags = run_summary.get("embedding_flags") or {}
    enabled = [lbl for lbl, on in flags.items() if on]
    if not enabled:
        pytest.skip("No embedding labels enabled in the latest run")

    staging_root = os.environ.get("DBXCARTA_STAGING_PATH", "").rstrip("/")
    if not staging_root:
        summary_vol = os.environ.get("DBXCARTA_SUMMARY_VOLUME", "").rstrip("/")
        if not summary_vol:
            pytest.skip("Neither DBXCARTA_STAGING_PATH nor DBXCARTA_SUMMARY_VOLUME set")
        staging_root = "/".join(summary_vol.split("/")[:-1]) + "/staging"

    if not os.path.isdir(staging_root):
        pytest.skip(f"Staging root {staging_root} not visible from test runner")

    attempts = run_summary.get("embedding_attempts") or {}
    for label in enabled:
        path = f"{staging_root}/{label.lower()}_nodes"
        assert os.path.isdir(path), f"Staging Delta missing for {label} at {path}"
        assert attempts.get(label, 0) > 0, f"No embedding attempts recorded for {label}"


def test_column_nodes_carry_four_embedding_properties(neo4j_driver, run_summary) -> None:
    if not _columns_enabled(run_summary):
        pytest.skip("Column embeddings not enabled in the latest run")

    with neo4j_driver.session() as session:
        result = session.run(
            "MATCH (n:Column) "
            "RETURN count(n) AS total, "
            "  count(n.embedding_text_hash) AS has_hash, "
            "  count(n.embedding) AS has_embedding, "
            "  count(n.embedding_model) AS has_model, "
            "  count(n.embedded_at) AS has_ts, "
            "  count(n.embedding_text) AS has_text"
        ).single()
    assert result["total"] > 0, "No Column nodes in graph"
    assert result["has_hash"] == result["total"]
    assert result["has_embedding"] == result["total"]
    assert result["has_model"] == result["total"]
    assert result["has_ts"] == result["total"]
    assert result["has_text"] == 0, "Column nodes must not store embedding_text (hash-only)"


def test_column_embedding_error_not_persisted(neo4j_driver, run_summary) -> None:
    if not _columns_enabled(run_summary):
        pytest.skip("Column embeddings not enabled in the latest run")

    with neo4j_driver.session() as session:
        count = session.run(
            "MATCH (n:Column) WHERE n.embedding_error IS NOT NULL RETURN count(n) AS c"
        ).single()["c"]
    assert count == 0


def test_column_vector_index_exists_with_expected_config(neo4j_driver, run_summary) -> None:
    if not _columns_enabled(run_summary):
        pytest.skip("Column embeddings not enabled in the latest run")

    with neo4j_driver.session() as session:
        rows = list(session.run(
            "SHOW VECTOR INDEXES YIELD name, labelsOrTypes, properties, options "
            "WHERE name = 'column_embedding'"
        ))
    assert rows, "Vector index 'column_embedding' not found"
    idx = rows[0]
    assert "Column" in idx["labelsOrTypes"]
    assert "embedding" in idx["properties"]
    cfg = idx["options"]["indexConfig"]
    assert cfg["vector.similarity_function"].lower() == "cosine"
    assert cfg["vector.dimensions"] > 0


def test_column_cosine_probe_returns_neighbors(neo4j_driver, run_summary) -> None:
    if not _columns_enabled(run_summary):
        pytest.skip("Column embeddings not enabled in the latest run")

    with neo4j_driver.session() as session:
        probe = session.run(
            "MATCH (n:Column) WHERE n.embedding IS NOT NULL "
            "RETURN n.id AS id, n.embedding AS vec LIMIT 1"
        ).single()
        assert probe is not None, "No Column with an embedding"
        neighbors = list(session.run(
            "CALL db.index.vector.queryNodes('column_embedding', 5, $vec) "
            "YIELD node, score RETURN node.id AS id, score",
            vec=probe["vec"],
        ))
    assert neighbors, "Vector index returned no neighbors for a known-good vector"


def test_non_table_labels_carry_no_embedding_properties(neo4j_driver, run_summary) -> None:
    """With only Table embeddings enabled, other labels must not have any of
    the five embedding properties."""
    if not _tables_enabled(run_summary):
        pytest.skip("Table embeddings not enabled in the latest run")
    flags = run_summary.get("embedding_flags") or {}
    other_labels = [lbl for lbl in ("Column", "Value", "Schema", "Database") if not flags.get(lbl)]
    if not other_labels:
        pytest.skip("All labels are embedding-enabled; nothing to negative-check")

    with neo4j_driver.session() as session:
        for label in other_labels:
            row = session.run(
                f"MATCH (n:{label}) "
                f"RETURN count(n.embedding) AS e, "
                f"       count(n.embedding_text) AS t, "
                f"       count(n.embedding_text_hash) AS h, "
                f"       count(n.embedding_model) AS m, "
                f"       count(n.embedded_at) AS a"
            ).single()
            assert row["e"] == 0, f"{label} has embeddings"
            assert row["t"] == 0, f"{label} has embedding_text"
            assert row["h"] == 0, f"{label} has embedding_text_hash"
            assert row["m"] == 0, f"{label} has embedding_model"
            assert row["a"] == 0, f"{label} has embedded_at"
