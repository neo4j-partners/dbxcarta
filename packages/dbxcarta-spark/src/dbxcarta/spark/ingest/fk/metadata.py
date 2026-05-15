"""Metadata-based FK discovery.

Pure Python — no Spark on the hot path. Driver-side iteration over column
metadata already collected during ingest. Scale target: ~10³ tables.

Entry point: infer_fk_pairs(columns, pk_index, prior_pairs).

Returns (edges, counters) where edges are FKEdge dataclasses tagged with
EdgeSource.INFERRED_METADATA and counters is an InferenceCounters aggregate
feeding the run summary.

Schema scope: pairs are only considered when source and target columns share
the same (catalog, schema). Cross-schema candidates are skipped before the
candidate counter increments, mirroring the `src is tgt` skip. This keeps
multi-schema runs (e.g. one catalog containing many independent application
schemas) from inferring spurious FKs across unrelated applications.
"""

from __future__ import annotations

import math
import re
from dataclasses import dataclass, field
from enum import Enum

from dbxcarta.spark.contract import EdgeSource
from dbxcarta.spark.ingest.fk.common import (
    ColumnMeta,
    DeclaredPair,
    FKEdge,
    PKEvidence,
    PKIndex,
    build_id_cols_index,
    pk_kind,
    types_compatible,
)


class NameMatchKind(Enum):
    EXACT = "exact"
    SUFFIX = "suffix"


class RejectionReason(Enum):
    NAME = "rejected_name"
    TYPE = "rejected_type"
    PK = "rejected_pk"
    SUB_THRESHOLD = "rejected_sub_threshold"
    TIE_BREAK = "rejected_tie_break"
    DUPLICATE_DECLARED = "rejected_duplicate_declared"


class ScoreBucket(Enum):
    B_0_95 = 0.95
    B_0_90 = 0.90
    B_0_88 = 0.88
    B_0_83 = 0.83
    B_0_82 = 0.82
    B_0_78 = 0.78

    @property
    def counter_key(self) -> str:
        # "B_0_95" → "bucket_0_95"; used when flattening to run_summary.row_counts.
        return self.name.replace("B_", "bucket_").lower()


_SCORE_TABLE: dict[tuple[NameMatchKind, PKEvidence, bool], float] = {
    (NameMatchKind.EXACT,  PKEvidence.DECLARED_PK,    True):  0.95,
    (NameMatchKind.EXACT,  PKEvidence.DECLARED_PK,    False): 0.90,
    (NameMatchKind.EXACT,  PKEvidence.UNIQUE_OR_HEUR, True):  0.88,
    (NameMatchKind.EXACT,  PKEvidence.UNIQUE_OR_HEUR, False): 0.83,
    (NameMatchKind.SUFFIX, PKEvidence.DECLARED_PK,    True):  0.88,
    (NameMatchKind.SUFFIX, PKEvidence.DECLARED_PK,    False): 0.83,
    (NameMatchKind.SUFFIX, PKEvidence.UNIQUE_OR_HEUR, True):  0.82,
    (NameMatchKind.SUFFIX, PKEvidence.UNIQUE_OR_HEUR, False): 0.78,
}

_STEM_SUFFIXES = ("_id", "_fk", "_ref")

# Dropped from comment-token sets before overlap check. len>=4 filter in
# _comment_tokens makes the short stops redundant; kept for spec parity and
# so future edits to the length floor don't re-introduce bleed.
_STOPWORDS = frozenset({
    "the", "of", "and", "a", "an", "to", "for", "id", "column", "table",
})

_TOKEN_SPLIT_RE = re.compile(r"[^a-zA-Z0-9]+")


@dataclass
class InferenceCounters:
    """Mutable aggregate of metadata-inference bookkeeping.

    Invariant: after inference finishes,
      candidates == accepted + sum(rejections.values())
    Enforced by test_counter_invariant.

    `composite_pk_skipped` is not a candidate outcome — it's a PKIndex
    construction observation recorded here so `as_summary_dict` can emit
    the full metadata-inference summary in one call.

    Flattens to run_summary.row_counts via as_summary_dict(prefix); the prefix
    concat is the single string-building point for these counters."""

    candidates: int = 0
    accepted: int = 0
    composite_pk_skipped: int = 0
    rejections: dict[RejectionReason, int] = field(
        default_factory=lambda: {r: 0 for r in RejectionReason},
    )
    buckets: dict[ScoreBucket, int] = field(
        default_factory=lambda: {b: 0 for b in ScoreBucket},
    )

    def record_candidate(self) -> None:
        self.candidates += 1

    def record_rejected(self, reason: RejectionReason) -> None:
        self.rejections[reason] += 1

    def record_accepted(self, score: float) -> None:
        self.accepted += 1
        self.buckets[ScoreBucket(round(score, 2))] += 1

    def as_summary_dict(self, prefix: str) -> dict[str, int]:
        out: dict[str, int] = {
            f"{prefix}_candidates": self.candidates,
            f"{prefix}_accepted": self.accepted,
            f"{prefix}_composite_pk_skipped": self.composite_pk_skipped,
        }
        for reason, count in self.rejections.items():
            out[f"{prefix}_{reason.value}"] = count
        for bucket, count in self.buckets.items():
            out[f"{prefix}_{bucket.counter_key}"] = count
        return out


def _stem_matches_table(stem: str, table: str) -> bool:
    """True when table equals stem, stem+s, or stem+es (prototype plural rules)."""
    s, t = stem.lower(), table.lower()
    return t == s or t == f"{s}s" or t == f"{s}es"


def _name_match(src_col: str, tgt_col: str, tgt_table: str) -> NameMatchKind | None:
    """Return EXACT, SUFFIX, or None per column-name matching rules.

    Suffix branch requires the source's stem to match the target table (singular
    or +s/+es plural). Without this guard, a column like `order_id` fans out to
    every `id` column in the catalog and tie-break drops legitimate matches.
    """
    src_l = src_col.lower()
    tgt_l = tgt_col.lower()
    if src_l == tgt_l:
        return NameMatchKind.EXACT
    for suf in _STEM_SUFFIXES:
        if src_l.endswith(suf) and len(src_l) > len(suf):
            stem = src_l[:-len(suf)]
            if tgt_l == "id" and _stem_matches_table(stem, tgt_table):
                return NameMatchKind.SUFFIX
    return None


def _comment_tokens(comment: str | None) -> set[str]:
    if not comment:
        return set()
    return {
        t for t in _TOKEN_SPLIT_RE.split(comment.lower())
        if len(t) >= 4 and t not in _STOPWORDS
    }


def infer_fk_pairs(
    columns: list[ColumnMeta],
    pk_index: PKIndex,
    prior_pairs: frozenset[DeclaredPair],
    threshold: float = 0.8,
) -> tuple[list[FKEdge], InferenceCounters]:
    """Emit metadata-inferred REFERENCES edges tagged with EdgeSource.INFERRED_METADATA.

    columns: driver-side materialised list of ColumnMeta.
    pk_index: declared single-column PKs and UNIQUE-leftmost columns.
    prior_pairs: frozenset of DeclaredPair already covered by earlier
      strategies (declared); suppressed from output to avoid duplicate edges.
    threshold: minimum attenuated score to emit. Default 0.8 matches retriever.
    """
    counters = InferenceCounters(composite_pk_skipped=pk_index.composite_pk_count)
    id_cols_by_table = build_id_cols_index(columns)

    # per_source[src_id] = [(score, target_id, name_kind, pk_evidence), ...]
    per_source: dict[str, list[tuple[float, str, NameMatchKind, PKEvidence]]] = {}

    for src in columns:
        for tgt in columns:
            if src is tgt:
                continue
            if src.catalog != tgt.catalog or src.schema != tgt.schema:
                continue
            counters.record_candidate()

            nm = _name_match(src.column, tgt.column, tgt.table)
            if nm is None:
                counters.record_rejected(RejectionReason.NAME)
                continue
            if src.table_key == tgt.table_key and src.column.lower() == tgt.column.lower():
                counters.record_rejected(RejectionReason.NAME)
                continue
            if not types_compatible(src.data_type, tgt.data_type):
                counters.record_rejected(RejectionReason.TYPE)
                continue
            pk = pk_kind(tgt, pk_index, id_cols_by_table)
            if pk is None:
                counters.record_rejected(RejectionReason.PK)
                continue

            comment_present = bool(
                _comment_tokens(src.comment) & _comment_tokens(tgt.comment)
            )
            score = _SCORE_TABLE[(nm, pk, comment_present)]
            if score < threshold:
                counters.record_rejected(RejectionReason.SUB_THRESHOLD)
                continue

            per_source.setdefault(src.col_id, []).append((score, tgt.col_id, nm, pk))

    edges: list[FKEdge] = []
    for src_id, candidates in per_source.items():
        n = len(candidates)
        denom = max(1.0, math.sqrt(max(0, n - 1)))
        for score, tgt_id, _nm, _pk in candidates:
            attenuated = score / denom if denom > 1 else score
            if attenuated < threshold:
                counters.record_rejected(RejectionReason.TIE_BREAK)
                continue
            if DeclaredPair(source_id=src_id, target_id=tgt_id) in prior_pairs:
                counters.record_rejected(RejectionReason.DUPLICATE_DECLARED)
                continue
            edges.append(FKEdge(
                source_id=src_id,
                target_id=tgt_id,
                confidence=round(attenuated, 4),
                source=EdgeSource.INFERRED_METADATA,
                criteria=None,
            ))
            counters.record_accepted(score)

    return edges, counters
