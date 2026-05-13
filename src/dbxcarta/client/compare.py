"""Result-set normalization and comparison for client SQL grading."""

from __future__ import annotations

from collections import Counter
from typing import Any


# Result-set comparison algorithm boundaries. Not runtime tunables.
COMPARE_ROW_THRESHOLD = 500
LARGE_COUNT_TOLERANCE = 0.10
LARGE_SAMPLE_MATCH_RATE = 0.80


def stringify_cell(value: Any) -> str:
    """Stringify a result-set cell with case-insensitive string comparison."""
    if value is None:
        return "NULL"
    return str(value).casefold()


def normalize_row(row: list, col_names: list[str]) -> tuple:
    """Reorder row values by sorted column name, stringifying each value."""
    if col_names and len(col_names) == len(row):
        return tuple(stringify_cell(v) for _, v in sorted(zip(col_names, row)))
    return tuple(sorted(stringify_cell(v) for v in row))


def normalize_result_set(col_names: list[str], rows: list[list]) -> list[tuple]:
    normalized = [normalize_row(row, col_names) for row in rows]
    return sorted(normalized)


def project_to_ref_columns(
    gen_cols: list[str],
    gen_rows: list[list],
    ref_cols: list[str],
) -> tuple[list[str], list[list]]:
    """Project generated rows to reference columns when ref is a subset."""
    if not ref_cols or not gen_cols or len(ref_cols) >= len(gen_cols):
        return gen_cols, gen_rows
    gen_lower = [c.casefold() for c in gen_cols]
    ref_lower = [c.casefold() for c in ref_cols]
    if not set(ref_lower).issubset(set(gen_lower)):
        return gen_cols, gen_rows
    idx = [gen_lower.index(c) for c in ref_lower]
    projected_rows = [[row[i] for i in idx] for row in gen_rows]
    return list(ref_cols), projected_rows


def is_row_superset(ref_norm: list[tuple], gen_norm: list[tuple]) -> bool:
    """True when every reference row appears in generated at least as often."""
    ref_counter = Counter(ref_norm)
    gen_counter = Counter(gen_norm)
    for row, count in ref_counter.items():
        if gen_counter[row] < count:
            return False
    return True


def compare_result_sets(
    gen_cols: list[str],
    gen_rows: list[list],
    ref_cols: list[str],
    ref_rows: list[list],
) -> tuple[bool, str | None]:
    """Compare result sets, ignoring column ordering and string case."""
    gen_cols, gen_rows = project_to_ref_columns(gen_cols, gen_rows, ref_cols)

    gen_count = len(gen_rows)
    ref_count = len(ref_rows)

    if gen_count >= COMPARE_ROW_THRESHOLD or ref_count >= COMPARE_ROW_THRESHOLD:
        if (
            gen_count < ref_count
            and ref_count > 0
            and (ref_count - gen_count) / ref_count > LARGE_COUNT_TOLERANCE
        ):
            return False, (
                "row count divergence >10% below reference: "
                f"generated={gen_count} reference={ref_count}"
            )
        gen_sorted = normalize_result_set(gen_cols, gen_rows)
        ref_sorted = normalize_result_set(ref_cols, ref_rows)
        if gen_count > ref_count and is_row_superset(ref_sorted, gen_sorted):
            return True, None
        sample_size = min(50, gen_count)
        stride = max(1, gen_count // sample_size) if sample_size else 1
        gen_sample = gen_sorted[::stride][:sample_size]
        ref_sample = ref_sorted[::stride][:sample_size]
        if not gen_sample:
            return True, None
        match_rate = sum(g == r for g, r in zip(gen_sample, ref_sample)) / len(
            gen_sample
        )
        if match_rate < LARGE_SAMPLE_MATCH_RATE:
            return False, (
                f"sampled match rate {match_rate:.1%} < "
                f"{LARGE_SAMPLE_MATCH_RATE:.0%}"
            )
        return True, None

    gen_sorted = normalize_result_set(gen_cols, gen_rows)
    ref_sorted = normalize_result_set(ref_cols, ref_rows)

    if gen_count == ref_count:
        if gen_sorted == ref_sorted:
            return True, None
        return False, "result set values differ"

    if gen_count > ref_count and is_row_superset(ref_sorted, gen_sorted):
        return True, None

    return False, f"row count mismatch: generated={gen_count} reference={ref_count}"
