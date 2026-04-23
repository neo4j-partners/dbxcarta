"""Assert Neo4j node counts match what the job reported writing (run summary row_counts)."""

from neo4j import Driver

from dbxcarta.contract import NodeLabel


def test_database_count(neo4j_driver: Driver, run_summary: dict) -> None:
    expected = run_summary["row_counts"].get("databases", 1)
    with neo4j_driver.session() as s:
        actual = s.run(f"MATCH (n:{NodeLabel.DATABASE}) RETURN count(n) AS cnt").single()["cnt"]
    assert actual == expected


def test_schema_count(neo4j_driver: Driver, run_summary: dict) -> None:
    expected = run_summary["row_counts"]["schemas"]
    with neo4j_driver.session() as s:
        actual = s.run(f"MATCH (n:{NodeLabel.SCHEMA}) RETURN count(n) AS cnt").single()["cnt"]
    assert actual == expected


def test_table_count(neo4j_driver: Driver, run_summary: dict) -> None:
    expected = run_summary["row_counts"]["tables"]
    with neo4j_driver.session() as s:
        actual = s.run(f"MATCH (n:{NodeLabel.TABLE}) RETURN count(n) AS cnt").single()["cnt"]
    assert actual == expected


def test_column_count(neo4j_driver: Driver, run_summary: dict) -> None:
    expected = run_summary["row_counts"]["columns"]
    with neo4j_driver.session() as s:
        actual = s.run(f"MATCH (n:{NodeLabel.COLUMN}) RETURN count(n) AS cnt").single()["cnt"]
    assert actual == expected
