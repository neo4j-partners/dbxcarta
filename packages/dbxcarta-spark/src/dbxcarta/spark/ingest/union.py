"""Balanced (logarithmic-depth) DataFrame union.

Folding ``unionByName`` left-to-right over N frames builds an N-deep logical
plan: Catalyst then analyzes and optimizes a linear chain whose depth grows
with the input count. At the per-table value-sampling fan-out (one frame per
sampled table — thousands at the dense-catalog target) that deep plan slows
analysis and can overflow the optimizer's recursion before a single row is
read. Combining the frames as a balanced binary tree keeps the plan depth at
O(log N) for the same row result; ``unionByName`` is associative over row
multisets, so the combined frame is identical regardless of pairing order.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def balanced_union(frames: list[DataFrame]) -> DataFrame:
    """Union ``frames`` by name with O(log N) plan depth.

    All frames must share a column set (``unionByName`` matches by name). Raises
    ``ValueError`` on an empty list — there is no identity element to return.
    """
    if not frames:
        raise ValueError("balanced_union requires at least one DataFrame")
    level = frames
    while len(level) > 1:
        paired = [level[i].unionByName(level[i + 1]) for i in range(0, len(level) - 1, 2)]
        if len(level) % 2:
            paired.append(level[-1])
        level = paired
    return level[0]
