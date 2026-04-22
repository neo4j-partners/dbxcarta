"""Assert Values only attach to STRING/BOOLEAN columns."""

from neo4j import Driver


def test_parent_column_type_restricted(neo4j_driver: Driver) -> None:
    with neo4j_driver.session() as s:
        bad = s.run(
            "MATCH (c:Column)-[:HAS_VALUE]->(:Value) "
            "WHERE NOT c.data_type IN ['STRING', 'BOOLEAN'] "
            "RETURN count(DISTINCT c) AS cnt"
        ).single()["cnt"]
    assert bad == 0, f"{bad} Column nodes with HAS_VALUE have data_type outside STRING/BOOLEAN"
