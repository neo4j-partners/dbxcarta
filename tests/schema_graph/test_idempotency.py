"""Assert a second job run produces identical node and relationship counts.

Marked slow — this test submits the job a second time and waits for it to finish.
Run explicitly with: pytest tests/schema_graph -m slow
"""

from pathlib import Path

import pytest
from neo4j import Driver

_PROJECT_DIR = str(Path(__file__).parent.parent.parent)


def _counts(session) -> dict[str, int]:
    counts = {}
    for record in session.run("MATCH (n) RETURN labels(n)[0] AS label, count(*) AS cnt"):
        counts[record["label"]] = record["cnt"]
    for rel in ("HAS_SCHEMA", "HAS_TABLE", "HAS_COLUMN", "REFERENCES"):
        counts[rel] = session.run(f"MATCH ()-[r:{rel}]->() RETURN count(r) AS cnt").single()["cnt"]
    return counts


@pytest.mark.slow
def test_second_run_produces_same_counts(neo4j_driver: Driver) -> None:
    from dbxcarta.cli import runner

    with neo4j_driver.session() as s:
        before = _counts(s)

    runner.submit("run_dbxcarta.py", project_dir=_PROJECT_DIR)

    with neo4j_driver.session() as s:
        after = _counts(s)

    assert after == before, f"Counts changed after second run: {set(after.items()) ^ set(before.items())}"
