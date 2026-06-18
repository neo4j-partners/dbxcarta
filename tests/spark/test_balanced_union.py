"""Unit tests for the balanced (logarithmic-depth) DataFrame union helper.

`balanced_union` replaces the left-folded `unionByName` chains in extract, FK
discovery, and value sampling. The contract: same rows as a left fold, by-name
column matching, and an explicit error on an empty input (no identity frame to
return).
"""

from __future__ import annotations

import pytest
from dbxcarta.spark.ingest.union import balanced_union


def _frame(spark, rows):
    return spark.createDataFrame(rows, schema=["a", "b"])


def test_empty_list_raises(local_spark) -> None:
    with pytest.raises(ValueError, match="at least one DataFrame"):
        balanced_union([])


def test_single_frame_returns_same_rows(local_spark) -> None:
    df = _frame(local_spark, [(1, "x")])
    assert balanced_union([df]).collect() == df.collect()


@pytest.mark.parametrize("count", [2, 3, 4, 7])
def test_unions_all_rows_for_even_and_odd_counts(local_spark, count) -> None:
    frames = [_frame(local_spark, [(i, f"v{i}")]) for i in range(count)]
    result = {(r["a"], r["b"]) for r in balanced_union(frames).collect()}
    assert result == {(i, f"v{i}") for i in range(count)}


def test_matches_by_name_not_position(local_spark) -> None:
    left = local_spark.createDataFrame([(1, "x")], schema=["a", "b"])
    # Same columns, reversed selection order: unionByName must align on name.
    right = local_spark.createDataFrame([("y", 2)], schema=["b", "a"])
    rows = {(r["a"], r["b"]) for r in balanced_union([left, right]).collect()}
    assert rows == {(1, "x"), (2, "y")}
