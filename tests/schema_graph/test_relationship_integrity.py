"""Assert every node has the expected incoming relationship and no node is an orphan."""

from neo4j import Driver


def test_every_schema_has_one_database_parent(neo4j_driver: Driver) -> None:
    with neo4j_driver.session() as s:
        orphans = s.run(
            "MATCH (n:Schema) WHERE NOT (:Database)-[:HAS_SCHEMA]->(n)"
            " RETURN count(n) AS cnt"
        ).single()["cnt"]
    assert orphans == 0, f"{orphans} Schema node(s) have no incoming HAS_SCHEMA"


def test_every_table_has_one_schema_parent(neo4j_driver: Driver) -> None:
    with neo4j_driver.session() as s:
        orphans = s.run(
            "MATCH (n:Table) WHERE NOT (:Schema)-[:HAS_TABLE]->(n)"
            " RETURN count(n) AS cnt"
        ).single()["cnt"]
    assert orphans == 0, f"{orphans} Table node(s) have no incoming HAS_TABLE"


def test_every_column_has_one_table_parent(neo4j_driver: Driver) -> None:
    with neo4j_driver.session() as s:
        orphans = s.run(
            "MATCH (n:Column) WHERE NOT (:Table)-[:HAS_COLUMN]->(n)"
            " RETURN count(n) AS cnt"
        ).single()["cnt"]
    assert orphans == 0, f"{orphans} Column node(s) have no incoming HAS_COLUMN"


def test_no_schema_has_multiple_database_parents(neo4j_driver: Driver) -> None:
    with neo4j_driver.session() as s:
        multi = s.run(
            "MATCH (n:Schema)"
            " WITH n, size([(db:Database)-[:HAS_SCHEMA]->(n) | db]) AS parents"
            " WHERE parents > 1 RETURN count(n) AS cnt"
        ).single()["cnt"]
    assert multi == 0, f"{multi} Schema node(s) have more than one Database parent"
