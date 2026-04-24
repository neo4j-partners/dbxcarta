"""Phase 3: metadata-based FK inference.

Pure Python — no Spark on the hot path. Driver-side iteration over column
metadata already collected during ingest. Scale target: ~10³ tables, tractable
on a single executor per worklog/fk-gap-v3-build.md scale notes.

Entry point: infer_fk_pairs(columns, pk_index, declared_pairs).

Returns (refs, counters) where refs are InferredRef dataclasses tagged with
EdgeSource.INFERRED_METADATA and counters is an InferenceCounters aggregate
feeding the run summary.

Shared primitives (`ColumnMeta`, `ConstraintRow`, `DeclaredPair`,
`InferredRef`, `PKIndex`, `PKEvidence`, `NameMatchKind`, `types_compatible`,
`pk_kind`, `build_id_cols_index`) live in `fk_common.py` — both Phase 3 and
Phase 4 consume them. `NameMatchKind` is re-exported here for backward
compatibility with callers that import it from this module.

Phase 3-specific things (`_SCORE_TABLE`, `RejectionReason`, `ScoreBucket`,
`InferenceCounters`, `_comment_tokens`, `_name_match`) stay here.

See worklog/fk-gap-v3-build.md Phases 3, 3.5, 3.6.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING

from dbxcarta.contract import EdgeSource
from dbxcarta.fk_common import (
    ColumnMeta,
    DeclaredPair,
    InferredRef,
    NameMatchKind,  # defined in fk_common; re-exported here for compat
    PKEvidence,
    PKIndex,
    build_id_cols_index,
    is_pair_covered,
    pk_kind,
    types_compatible,
)

if TYPE_CHECKING:
    from dbxcarta.fk_config import FKInferenceConfig

# Re-export so existing callers do: from dbxcarta.fk_inference import NameMatchKind
__all__ = [
    "NameMatchKind",
    "RejectionReason",
    "ScoreBucket",
    "InferenceCounters",
    "infer_fk_pairs",
    "_SCORE_TABLE",
    "_STEM_SUFFIXES",
]


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
    """Mutable aggregate of Phase 3 inference bookkeeping.

    Invariant: after inference finishes,
      candidates == accepted + sum(rejections.values())
    Enforced by test_counter_invariant.

    `composite_pk_skipped` is not a candidate outcome — it's a PKIndex
    construction observation recorded here so `as_summary_dict` can emit
    the full metadata-phase summary in one call.

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


def _name_match(
    src_col: str,
    tgt_col: str,
    tgt_table: str,
    stem_suffixes: tuple[str, ...] = _STEM_SUFFIXES,
) -> NameMatchKind | None:
    """Return EXACT, SUFFIX, or None per Phase 3 name rules.

    Suffix branch requires the source's stem to match the target table (singular
    or +s/+es plural). Without this guard, a column like `order_id` fans out to
    every `id` column in the catalog and tie-break drops legitimate matches.

    stem_suffixes: configurable suffix list from FKInferenceConfig (defaults to
    the module-level _STEM_SUFFIXES tuple so existing call sites are unchanged).
    """
    src_l = src_col.lower()
    tgt_l = tgt_col.lower()
    if src_l == tgt_l:
        return NameMatchKind.EXACT
    for suf in stem_suffixes:
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
    declared_pairs: frozenset[DeclaredPair],
    threshold: float = 0.8,
    *,
    config: "FKInferenceConfig | None" = None,
) -> tuple[list[InferredRef], InferenceCounters]:
    """Emit metadata-inferred REFERENCES refs tagged with EdgeSource.INFERRED_METADATA.

    columns: driver-side materialised list of ColumnMeta.
    pk_index: declared single-column PKs and UNIQUE-leftmost columns.
    declared_pairs: frozenset of DeclaredPair already covered by declared
      FKs; suppressed from output to avoid duplicate edges.
    threshold: minimum attenuated score to emit. Default 0.8 matches retriever.
      When config is provided, config.metadata_threshold is used as the
      default; the explicit threshold parameter takes precedence only when
      it differs from the default (0.8).
    config: optional FKInferenceConfig providing pluggable strategies and
      tuning parameters. When None, all defaults reproduce current behavior.
    """
    from dbxcarta.fk_config import FKInferenceConfig as _FKInferenceConfig

    cfg = config if config is not None else _FKInferenceConfig()

    # When caller passes the default threshold (0.8) and config overrides it,
    # prefer config. Explicit non-default threshold values take precedence.
    _threshold = cfg.metadata_threshold if threshold == 0.8 else threshold

    score_table = cfg.score_table
    stem_suffixes = cfg.stem_suffixes
    name_match_fn = cfg.name_match_strategy or (
        lambda sc, tc, tt: _name_match(sc, tc, tt, stem_suffixes)
    )
    type_equiv = cfg.effective_type_equiv()
    pk_patterns = cfg.compiled_pk_patterns()

    counters = InferenceCounters(composite_pk_skipped=pk_index.composite_pk_count)
    id_cols_by_table = build_id_cols_index(columns)

    # --- Pass 1: score every (src, tgt) pair that clears name / type / PK gates.
    # per_source[src_id] = [(score, tgt_id, name_kind, pk_evidence), ...]
    per_source: dict[str, list[tuple[float, str, NameMatchKind, PKEvidence]]] = {}

    for src in columns:
        for tgt in columns:
            if src is tgt:
                continue
            counters.record_candidate()

            nm = name_match_fn(src.column, tgt.column, tgt.table)
            if nm is None:
                counters.record_rejected(RejectionReason.NAME)
                continue
            if src.table_key == tgt.table_key and src.column.lower() == tgt.column.lower():
                counters.record_rejected(RejectionReason.NAME)
                continue
            if not types_compatible(src.data_type, tgt.data_type, type_equiv):
                counters.record_rejected(RejectionReason.TYPE)
                continue
            pk = pk_kind(tgt, pk_index, id_cols_by_table, pk_patterns or None)
            if pk is None:
                counters.record_rejected(RejectionReason.PK)
                continue

            comment_present = bool(
                _comment_tokens(src.comment) & _comment_tokens(tgt.comment)
            )
            score = score_table[(nm, pk, comment_present)]
            if score < _threshold:
                counters.record_rejected(RejectionReason.SUB_THRESHOLD)
                continue

            per_source.setdefault(src.col_id, []).append((score, tgt.col_id, nm, pk))

    # --- Pass 2: apply tie-break attenuation then emit surviving pairs.
    # Separating the two passes makes each step independently readable and
    # testable: Pass 1 is pure scoring; Pass 2 is pure attenuation + emit.
    refs: list[InferredRef] = []
    exp = cfg.attenuation_exponent
    top_n = cfg.attenuation_top_n

    for src_id, candidates in per_source.items():
        n = len(candidates)
        # Generalization of the original sqrt(max(0, n-1)) formula:
        # denom = max(1.0, (n-1)^exp). At exp=0.5 (default) this is exactly
        # sqrt(n-1) — the same formula as before the refactoring. For n≤1
        # denom is always 1.0 (no attenuation). The exponent knob lets
        # operators tune aggressiveness without changing the anchor point.
        denom = max(1.0, (n - 1) ** exp) if n > 1 else 1.0

        # Apply optional hard top-N cap before attenuation.
        working = candidates[:top_n] if top_n is not None else candidates

        for score, tgt_id, _nm, _pk in working:
            attenuated = score / denom if denom > 1 else score
            if attenuated < _threshold:
                counters.record_rejected(RejectionReason.TIE_BREAK)
                continue
            if is_pair_covered(src_id, tgt_id, declared_pairs):
                counters.record_rejected(RejectionReason.DUPLICATE_DECLARED)
                continue
            refs.append(InferredRef(
                source_id=src_id,
                target_id=tgt_id,
                confidence=round(attenuated, 4),
                source=EdgeSource.INFERRED_METADATA,
                criteria=None,
            ))
            counters.record_accepted(score)

        # Count candidates beyond the top-N cap as tie-break rejections.
        if top_n is not None and n > top_n:
            for _ in range(n - top_n):
                counters.record_rejected(RejectionReason.TIE_BREAK)

    return refs, counters
