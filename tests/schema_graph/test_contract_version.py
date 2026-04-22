"""Assert every written node carries the current contract version.

Catches partial re-runs that mix nodes from two wheel versions.
"""

from neo4j import Driver

from dbxcarta.contract import CONTRACT_VERSION, LABEL_COLUMN, LABEL_DATABASE, LABEL_SCHEMA, LABEL_TABLE


def _count_wrong_version(session, label: str) -> int:
    return session.run(
        f"MATCH (n:{label}) WHERE n.contract_version <> $v RETURN count(n) AS cnt",
        v=CONTRACT_VERSION,
    ).single()["cnt"]


def test_database_nodes_have_current_contract_version(neo4j_driver: Driver) -> None:
    with neo4j_driver.session() as s:
        assert _count_wrong_version(s, LABEL_DATABASE) == 0


def test_schema_nodes_have_current_contract_version(neo4j_driver: Driver) -> None:
    with neo4j_driver.session() as s:
        assert _count_wrong_version(s, LABEL_SCHEMA) == 0


def test_table_nodes_have_current_contract_version(neo4j_driver: Driver) -> None:
    with neo4j_driver.session() as s:
        assert _count_wrong_version(s, LABEL_TABLE) == 0


def test_column_nodes_have_current_contract_version(neo4j_driver: Driver) -> None:
    with neo4j_driver.session() as s:
        assert _count_wrong_version(s, LABEL_COLUMN) == 0
