"""Assert Value id = f'{column_id}.{md5(value)}' for a random sample of nodes."""

import re

from neo4j import Driver

from dbxcarta.contract import generate_value_id

_HEX32 = re.compile(r"[0-9a-f]{32}$")


def test_value_id_matches_generator(neo4j_driver: Driver) -> None:
    with neo4j_driver.session() as s:
        rows = s.run(
            "MATCH (c:Column)-[:HAS_VALUE]->(v:Value) "
            "RETURN c.id AS col_id, v.id AS val_id, v.value AS val "
            "ORDER BY rand() LIMIT 20"
        ).data()

    assert rows, "No Value nodes found — did the sample_values job run?"

    for row in rows:
        col_id, val_id, val = row["col_id"], row["val_id"], row["val"]
        assert val_id == generate_value_id(col_id, val), (
            f"id mismatch: got {val_id!r}, expected "
            f"{generate_value_id(col_id, val)!r} for col_id={col_id!r} val={val!r}"
        )
        assert val_id.startswith(col_id + "."), f"value id {val_id!r} missing column prefix"
        suffix = val_id[len(col_id) + 1:]
        assert _HEX32.match(suffix), f"MD5 suffix not 32 hex chars: {suffix!r}"
