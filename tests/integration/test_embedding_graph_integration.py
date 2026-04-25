"""Integration tests for Table embeddings in the written graph.

Requires a completed run with DBXCARTA_INCLUDE_EMBEDDINGS_TABLES=true and
the fixtures (neo4j_driver, run_summary, ws) defined in the schema_graph
sibling conftest. Skips cleanly when those preconditions aren't met.
"""

from __future__ import annotations

import pytest


def _tables_enabled(run_summary: dict) -> bool:
    flags = run_summary.get("embedding_flags") or {}
    return bool(flags.get("Table"))


def _columns_enabled(run_summary: dict) -> bool:
    flags = run_summary.get("embedding_flags") or {}
    return bool(flags.get("Column"))


def test_table_nodes_carry_four_embedding_properties(neo4j_driver, run_summary) -> None:
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
    assert result["has_text"] == 0
    assert result["has_hash"] == result["total"]
    assert result["has_embedding"] == result["total"]
    assert result["has_model"] == result["total"]
    assert result["has_ts"] == result["total"]


def test_table_embedding_text_not_persisted(neo4j_driver, run_summary) -> None:
    """embedding_text must not be stored on Table nodes; hash-only like all other labels."""
    if not _tables_enabled(run_summary):
        pytest.skip("Table embeddings not enabled in the latest run")

    with neo4j_driver.session() as session:
        count = session.run(
            "MATCH (n:Table) WHERE n.embedding_text IS NOT NULL RETURN count(n) AS c"
        ).single()["c"]
    assert count == 0


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


def _values_enabled(run_summary: dict) -> bool:
    flags = run_summary.get("embedding_flags") or {}
    return bool(flags.get("Value"))


def test_value_nodes_carry_four_embedding_properties(neo4j_driver, run_summary) -> None:
    if not _values_enabled(run_summary):
        pytest.skip("Value embeddings not enabled in the latest run")

    with neo4j_driver.session() as session:
        result = session.run(
            "MATCH (n:Value) "
            "RETURN count(n) AS total, "
            "  count(n.embedding_text_hash) AS has_hash, "
            "  count(n.embedding) AS has_embedding, "
            "  count(n.embedding_model) AS has_model, "
            "  count(n.embedded_at) AS has_ts, "
            "  count(n.embedding_text) AS has_text"
        ).single()
    assert result["total"] > 0, "No Value nodes in graph"
    assert result["has_hash"] == result["total"]
    assert result["has_embedding"] == result["total"]
    assert result["has_model"] == result["total"]
    assert result["has_ts"] == result["total"]
    assert result["has_text"] == 0, "Value nodes must not store embedding_text (hash-only)"


def test_value_embedding_error_not_persisted(neo4j_driver, run_summary) -> None:
    if not _values_enabled(run_summary):
        pytest.skip("Value embeddings not enabled in the latest run")

    with neo4j_driver.session() as session:
        count = session.run(
            "MATCH (n:Value) WHERE n.embedding_error IS NOT NULL RETURN count(n) AS c"
        ).single()["c"]
    assert count == 0


def test_value_vector_index_exists_with_expected_config(neo4j_driver, run_summary) -> None:
    if not _values_enabled(run_summary):
        pytest.skip("Value embeddings not enabled in the latest run")

    with neo4j_driver.session() as session:
        rows = list(session.run(
            "SHOW VECTOR INDEXES YIELD name, labelsOrTypes, properties, options "
            "WHERE name = 'value_embedding'"
        ))
    assert rows, "Vector index 'value_embedding' not found"
    idx = rows[0]
    assert "Value" in idx["labelsOrTypes"]
    assert "embedding" in idx["properties"]
    cfg = idx["options"]["indexConfig"]
    assert cfg["vector.similarity_function"].lower() == "cosine"
    assert cfg["vector.dimensions"] > 0


def test_value_cosine_probe_returns_neighbors(neo4j_driver, run_summary) -> None:
    if not _values_enabled(run_summary):
        pytest.skip("Value embeddings not enabled in the latest run")

    with neo4j_driver.session() as session:
        probe = session.run(
            "MATCH (n:Value) WHERE n.embedding IS NOT NULL "
            "RETURN n.id AS id, n.embedding AS vec LIMIT 1"
        ).single()
        assert probe is not None, "No Value with an embedding"
        neighbors = list(session.run(
            "CALL db.index.vector.queryNodes('value_embedding', 5, $vec) "
            "YIELD node, score RETURN node.id AS id, score",
            vec=probe["vec"],
        ))
    assert neighbors, "Vector index returned no neighbors for a known-good vector"


def _schemas_enabled(run_summary: dict) -> bool:
    flags = run_summary.get("embedding_flags") or {}
    return bool(flags.get("Schema"))


def _databases_enabled(run_summary: dict) -> bool:
    flags = run_summary.get("embedding_flags") or {}
    return bool(flags.get("Database"))


def test_schema_nodes_carry_four_embedding_properties(neo4j_driver, run_summary) -> None:
    if not _schemas_enabled(run_summary):
        pytest.skip("Schema embeddings not enabled in the latest run")

    with neo4j_driver.session() as session:
        result = session.run(
            "MATCH (n:Schema) "
            "RETURN count(n) AS total, "
            "  count(n.embedding_text_hash) AS has_hash, "
            "  count(n.embedding) AS has_embedding, "
            "  count(n.embedding_model) AS has_model, "
            "  count(n.embedded_at) AS has_ts, "
            "  count(n.embedding_text) AS has_text"
        ).single()
    assert result["total"] > 0, "No Schema nodes in graph"
    assert result["has_hash"] == result["total"]
    assert result["has_embedding"] == result["total"]
    assert result["has_model"] == result["total"]
    assert result["has_ts"] == result["total"]
    assert result["has_text"] == 0, "Schema nodes must not store embedding_text (hash-only)"


def test_schema_embedding_error_not_persisted(neo4j_driver, run_summary) -> None:
    if not _schemas_enabled(run_summary):
        pytest.skip("Schema embeddings not enabled in the latest run")

    with neo4j_driver.session() as session:
        count = session.run(
            "MATCH (n:Schema) WHERE n.embedding_error IS NOT NULL RETURN count(n) AS c"
        ).single()["c"]
    assert count == 0


def test_schema_vector_index_exists_with_expected_config(neo4j_driver, run_summary) -> None:
    if not _schemas_enabled(run_summary):
        pytest.skip("Schema embeddings not enabled in the latest run")

    with neo4j_driver.session() as session:
        rows = list(session.run(
            "SHOW VECTOR INDEXES YIELD name, labelsOrTypes, properties, options "
            "WHERE name = 'schema_embedding'"
        ))
    assert rows, "Vector index 'schema_embedding' not found"
    idx = rows[0]
    assert "Schema" in idx["labelsOrTypes"]
    assert "embedding" in idx["properties"]
    cfg = idx["options"]["indexConfig"]
    assert cfg["vector.similarity_function"].lower() == "cosine"
    assert cfg["vector.dimensions"] > 0


def test_schema_cosine_probe_returns_neighbors(neo4j_driver, run_summary) -> None:
    if not _schemas_enabled(run_summary):
        pytest.skip("Schema embeddings not enabled in the latest run")

    with neo4j_driver.session() as session:
        probe = session.run(
            "MATCH (n:Schema) WHERE n.embedding IS NOT NULL "
            "RETURN n.id AS id, n.embedding AS vec LIMIT 1"
        ).single()
        assert probe is not None, "No Schema with an embedding"
        neighbors = list(session.run(
            "CALL db.index.vector.queryNodes('schema_embedding', 5, $vec) "
            "YIELD node, score RETURN node.id AS id, score",
            vec=probe["vec"],
        ))
    assert neighbors, "Vector index returned no neighbors for a known-good vector"


def test_database_nodes_carry_four_embedding_properties(neo4j_driver, run_summary) -> None:
    if not _databases_enabled(run_summary):
        pytest.skip("Database embeddings not enabled in the latest run")

    with neo4j_driver.session() as session:
        result = session.run(
            "MATCH (n:Database) "
            "RETURN count(n) AS total, "
            "  count(n.embedding_text_hash) AS has_hash, "
            "  count(n.embedding) AS has_embedding, "
            "  count(n.embedding_model) AS has_model, "
            "  count(n.embedded_at) AS has_ts, "
            "  count(n.embedding_text) AS has_text"
        ).single()
    assert result["total"] > 0, "No Database nodes in graph"
    assert result["has_hash"] == result["total"]
    assert result["has_embedding"] == result["total"]
    assert result["has_model"] == result["total"]
    assert result["has_ts"] == result["total"]
    assert result["has_text"] == 0, "Database nodes must not store embedding_text (hash-only)"


def test_database_embedding_error_not_persisted(neo4j_driver, run_summary) -> None:
    if not _databases_enabled(run_summary):
        pytest.skip("Database embeddings not enabled in the latest run")

    with neo4j_driver.session() as session:
        count = session.run(
            "MATCH (n:Database) WHERE n.embedding_error IS NOT NULL RETURN count(n) AS c"
        ).single()["c"]
    assert count == 0


def test_database_vector_index_exists_with_expected_config(neo4j_driver, run_summary) -> None:
    if not _databases_enabled(run_summary):
        pytest.skip("Database embeddings not enabled in the latest run")

    with neo4j_driver.session() as session:
        rows = list(session.run(
            "SHOW VECTOR INDEXES YIELD name, labelsOrTypes, properties, options "
            "WHERE name = 'database_embedding'"
        ))
    assert rows, "Vector index 'database_embedding' not found"
    idx = rows[0]
    assert "Database" in idx["labelsOrTypes"]
    assert "embedding" in idx["properties"]
    cfg = idx["options"]["indexConfig"]
    assert cfg["vector.similarity_function"].lower() == "cosine"
    assert cfg["vector.dimensions"] > 0


def test_database_cosine_probe_returns_neighbors(neo4j_driver, run_summary) -> None:
    if not _databases_enabled(run_summary):
        pytest.skip("Database embeddings not enabled in the latest run")

    with neo4j_driver.session() as session:
        probe = session.run(
            "MATCH (n:Database) WHERE n.embedding IS NOT NULL "
            "RETURN n.id AS id, n.embedding AS vec LIMIT 1"
        ).single()
        assert probe is not None, "No Database with an embedding"
        neighbors = list(session.run(
            "CALL db.index.vector.queryNodes('database_embedding', 5, $vec) "
            "YIELD node, score RETURN node.id AS id, score",
            vec=probe["vec"],
        ))
    assert neighbors, "Vector index returned no neighbors for a known-good vector"


def test_embedding_properties_match_enabled_flags(neo4j_driver, run_summary) -> None:
    """Every label whose flag is on must carry all embedding properties on every
    node; every label whose flag is off must carry none of the four properties.

    Always asserts something regardless of which combination of flags is active,
    so this test remains meaningful as labels are enabled one at a time.
    """
    flags = run_summary.get("embedding_flags") or {}
    if not any(flags.values()):
        pytest.skip("No embedding labels enabled in the latest run")

    all_labels = ("Table", "Column", "Value", "Schema", "Database")
    with neo4j_driver.session() as session:
        for label in all_labels:
            row = session.run(
                f"MATCH (n:{label}) "
                "RETURN count(n) AS total, "
                "  count(n.embedding) AS has_embedding, "
                "  count(n.embedding_text_hash) AS has_hash, "
                "  count(n.embedding_model) AS has_model, "
                "  count(n.embedded_at) AS has_ts"
            ).single()

            if flags.get(label):
                if row["total"] == 0:
                    continue
                assert row["has_embedding"] == row["total"], f"{label}: missing embedding"
                assert row["has_hash"] == row["total"], f"{label}: missing embedding_text_hash"
                assert row["has_model"] == row["total"], f"{label}: missing embedding_model"
                assert row["has_ts"] == row["total"], f"{label}: missing embedded_at"
            else:
                assert row["has_embedding"] == 0, f"{label} has embeddings but flag is off"
                assert row["has_hash"] == 0, f"{label} has embedding_text_hash but flag is off"
                assert row["has_model"] == 0, f"{label} has embedding_model but flag is off"
                assert row["has_ts"] == 0, f"{label} has embedded_at but flag is off"
