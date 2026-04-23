"""Phase 3: metadata-based FK inference.

Pure Python — no Spark on the hot path. Driver-side iteration over column
metadata already collected during ingest. Scale target: ~10³ tables, tractable
on a single executor per worklog/fk-gap-v3-build.md scale notes.

Entry point: infer_fk_pairs(columns, pk_cols, unique_leftmost, declared_pairs).

Returns (rows, counters) where rows have the canonical REFERENCES schema
(source_id, target_id, confidence, source, criteria) and counters feed the
run summary.

See worklog/fk-gap-v3-build.md Phase 3.
"""

from __future__ import annotations

import math
import re

from dbxcarta.contract import generate_id


# Normalise integer-family types to a common canonical name. Any entry not in
# this dict canonicalises to itself (see _canonicalize). STRING(n) and
# DECIMAL(p,s) are special-cased because their parameters encode comparability
# in ways a plain map can't express.
_TYPE_EQUIV: dict[str, str] = {
    "BIGINT": "INTEGER",
    "INT": "INTEGER",
    "INTEGER": "INTEGER",
    "LONG": "INTEGER",
    "SMALLINT": "INTEGER",
    "TINYINT": "INTEGER",
}

# Scoring table keyed by (name_kind, pk_kind, comment_present). Numerics match
# worklog Phase 3 table. suffix+unique_or_heur+no-comment sits at 0.78 —
# intentionally below the 0.8 retriever threshold, so the candidate lands in
# the graph for audit but is filtered out at retrieval time.
_SCORE_TABLE: dict[tuple[str, str, bool], float] = {
    ("exact",  "declared_pk",    True):  0.95,
    ("exact",  "declared_pk",    False): 0.90,
    ("exact",  "unique_or_heur", True):  0.88,
    ("exact",  "unique_or_heur", False): 0.83,
    ("suffix", "declared_pk",    True):  0.88,
    ("suffix", "declared_pk",    False): 0.83,
    ("suffix", "unique_or_heur", True):  0.82,
    ("suffix", "unique_or_heur", False): 0.78,
}

_STEM_SUFFIXES = ("_id", "_fk", "_ref")

# Dropped from comment-token sets before overlap check. len>=4 filter in
# _comment_tokens makes the short stops redundant; kept for spec parity and
# so future edits to the length floor don't re-introduce bleed.
_STOPWORDS = {
    "the", "of", "and", "a", "an", "to", "for", "id", "column", "table",
}

_DECIMAL_RE = re.compile(r"^(?:DECIMAL|NUMERIC)\((\d+)(?:,(\d+))?\)$")
_STRING_PARAM_RE = re.compile(r"^(?:STRING|VARCHAR|CHAR)(?:\(\d+\))?$")
_TOKEN_SPLIT_RE = re.compile(r"[^a-zA-Z0-9]+")


def _canonicalize(data_type: str) -> tuple[str, str | None]:
    """Reduce a declared type to (family, detail) for equality comparison.

    detail holds scale for DECIMAL; None otherwise. Precision is discarded so
    DECIMAL(10,2) ↔ DECIMAL(18,2).
    """
    t = data_type.strip().upper()
    if _STRING_PARAM_RE.match(t):
        return ("STRING", None)
    m = _DECIMAL_RE.match(t)
    if m:
        scale = m.group(2) if m.group(2) is not None else "0"
        return ("DECIMAL", scale)
    return (_TYPE_EQUIV.get(t, t), None)


def _types_compatible(a: str, b: str) -> bool:
    return _canonicalize(a) == _canonicalize(b)


def _stem_matches_table(stem: str, table: str) -> bool:
    """True when table equals stem, stem+s, or stem+es (prototype plural rules)."""
    s, t = stem.lower(), table.lower()
    return t == s or t == f"{s}s" or t == f"{s}es"


def _name_match(src_col: str, tgt_col: str, tgt_table: str) -> str | None:
    """Return "exact", "suffix", or None per Phase 3 name rules.

    Suffix branch requires the source's stem to match the target table (singular
    or +s/+es plural). Without this guard, a column like `order_id` fans out to
    every `id` column in the catalog and tie-break drops legitimate matches.
    """
    src_l = src_col.lower()
    tgt_l = tgt_col.lower()
    if src_l == tgt_l:
        return "exact"
    for suf in _STEM_SUFFIXES:
        if src_l.endswith(suf) and len(src_l) > len(suf):
            stem = src_l[:-len(suf)]
            if tgt_l == "id" and _stem_matches_table(stem, tgt_table):
                return "suffix"
    return None


def _comment_tokens(comment: str | None) -> set[str]:
    if not comment:
        return set()
    return {
        t for t in _TOKEN_SPLIT_RE.split(comment.lower())
        if len(t) >= 4 and t not in _STOPWORDS
    }


def _table_key(col: dict) -> str:
    return generate_id(col["catalog"], col["schema"], col["table"])


def _col_id(col: dict) -> str:
    return generate_id(col["catalog"], col["schema"], col["table"], col["column"])


def _pk_kind(
    table_key: str,
    column: str,
    pk_cols: dict[str, set[str]],
    unique_leftmost: dict[str, set[str]],
    id_cols_by_table: dict[str, list[str]],
) -> str | None:
    """Classify target's PK-likeness: declared_pk, unique_or_heur, or None.

    Falls back to name heuristics (column == "id", or == "{table}_id" and sole
    _id-suffixed column) so the phase still fires on catalogs with no declared
    PKs. The fallback collapses into "unique_or_heur" rather than a third
    bucket, matching the worklog score table's two PK columns.
    """
    if column in pk_cols.get(table_key, set()):
        return "declared_pk"
    if column in unique_leftmost.get(table_key, set()):
        return "unique_or_heur"
    col_lower = column.lower()
    table_name = table_key.rsplit(".", 1)[-1].lower()
    if col_lower == "id":
        return "unique_or_heur"
    if col_lower == f"{table_name}_id":
        id_cols = id_cols_by_table.get(table_key, [])
        if len(id_cols) == 1 and id_cols[0].lower() == col_lower:
            return "unique_or_heur"
    return None


def _build_id_cols_index(columns: list[dict]) -> dict[str, list[str]]:
    index: dict[str, list[str]] = {}
    for c in columns:
        if c["column"].lower().endswith("_id"):
            index.setdefault(_table_key(c), []).append(c["column"])
    return index


def _empty_counters() -> dict[str, int]:
    return {
        "candidates": 0,
        "accepted": 0,
        "rejected_name": 0,
        "rejected_type": 0,
        "rejected_pk": 0,
        "rejected_tie_break": 0,
        "rejected_duplicate_declared": 0,
        "bucket_0_95": 0,
        "bucket_0_90": 0,
        "bucket_0_88": 0,
        "bucket_0_83": 0,
        "bucket_0_82": 0,
        "bucket_0_78": 0,
    }


def _bucket_key(score: float) -> str:
    # 6 rows in the scoring table that clear >=0.78; map by exact equality.
    mapping = {
        0.95: "bucket_0_95", 0.90: "bucket_0_90", 0.88: "bucket_0_88",
        0.83: "bucket_0_83", 0.82: "bucket_0_82", 0.78: "bucket_0_78",
    }
    return mapping[round(score, 2)]


def infer_fk_pairs(
    columns: list[dict],
    pk_cols: dict[str, set[str]],
    unique_leftmost: dict[str, set[str]],
    declared_pairs: set[tuple[str, str]],
    threshold: float = 0.8,
) -> tuple[list[dict], dict[str, int]]:
    """Emit metadata-inferred REFERENCES rows.

    columns: list of {catalog, schema, table, column, data_type, comment}.
    pk_cols: table_key → set of column names in its single-column declared PK.
      Composite PKs are pre-filtered upstream (worklog: non-goal).
    unique_leftmost: table_key → set of leftmost columns of UNIQUE constraints.
    declared_pairs: set of (source_id, target_id) already covered by declared
      FKs; suppressed from output to avoid duplicate edges.
    threshold: minimum attenuated score to emit. Default 0.8 matches retriever.
    """
    counters = _empty_counters()
    id_cols_by_table = _build_id_cols_index(columns)

    per_source: dict[str, list[tuple[float, str, str, str]]] = {}

    for src in columns:
        for tgt in columns:
            if src is tgt:
                continue
            counters["candidates"] += 1

            nm = _name_match(src["column"], tgt["column"], tgt["table"])
            if nm is None:
                counters["rejected_name"] += 1
                continue
            if _table_key(src) == _table_key(tgt) and src["column"].lower() == tgt["column"].lower():
                # Degenerate self-reference on same column; self-ref with
                # different column names is allowed (employee.manager_id → employee.id).
                counters["rejected_name"] += 1
                continue
            if not _types_compatible(src["data_type"], tgt["data_type"]):
                counters["rejected_type"] += 1
                continue
            pk = _pk_kind(
                _table_key(tgt), tgt["column"],
                pk_cols, unique_leftmost, id_cols_by_table,
            )
            if pk is None:
                counters["rejected_pk"] += 1
                continue

            comment_present = bool(
                _comment_tokens(src.get("comment"))
                & _comment_tokens(tgt.get("comment"))
            )
            score = _SCORE_TABLE[(nm, pk, comment_present)]
            if score < threshold:
                continue

            src_id = _col_id(src)
            per_source.setdefault(src_id, []).append(
                (score, _col_id(tgt), nm, pk)
            )

    rows: list[dict] = []
    for src_id, candidates in per_source.items():
        n = len(candidates)
        denom = max(1.0, math.sqrt(max(0, n - 1)))
        for score, tgt_id, _nm, _pk in candidates:
            attenuated = score / denom if denom > 1 else score
            if attenuated < threshold:
                counters["rejected_tie_break"] += 1
                continue
            if (src_id, tgt_id) in declared_pairs:
                counters["rejected_duplicate_declared"] += 1
                continue
            rows.append({
                "source_id": src_id,
                "target_id": tgt_id,
                "confidence": round(attenuated, 4),
                "source": "inferred_metadata",
                "criteria": None,
            })
            counters["accepted"] += 1
            counters[_bucket_key(score)] += 1

    return rows, counters
