"""Assert Value node count matches run summary and sampled ≤ candidate columns."""

from neo4j import Driver

from dbxcarta.contract import NodeLabel


def test_value_count_matches_summary(neo4j_driver: Driver, run_summary: dict) -> None:
    expected = run_summary["row_counts"]["value_nodes"]
    with neo4j_driver.session() as s:
        actual = s.run(f"MATCH (n:{NodeLabel.VALUE}) RETURN count(n) AS cnt").single()["cnt"]
    assert actual == expected


def test_sampled_lte_candidate(run_summary: dict) -> None:
    rc = run_summary["row_counts"]
    assert rc["sampled_columns"] <= rc["candidate_columns"]


def test_sampled_plus_skipped_eq_candidate(run_summary: dict) -> None:
    rc = run_summary["row_counts"]
    assert rc["sampled_columns"] + rc["skipped_columns"] == rc["candidate_columns"]
