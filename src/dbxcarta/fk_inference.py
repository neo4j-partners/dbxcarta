"""Phase 3: metadata-based FK inference.

Pure Python — no Spark on the hot path. Driver-side iteration over column
metadata already collected during ingest. Scale target: ~10³ tables, tractable
on a single executor per worklog/fk-gap-v3-build.md scale notes.

Entry point: infer_fk_pairs(columns, pk_index, declared_pairs).

Returns (refs, counters) where refs are InferredRef dataclasses with the
canonical REFERENCES schema (source_id, target_id, confidence, source,
criteria) and counters is an InferenceCounters aggregate feeding the run
summary.

All internal types are nominal (no list[dict], no dict[str, int]): see
ColumnMeta, ConstraintRow, DeclaredPair, InferredRef, PKIndex,
InferenceCounters, and the RejectionReason / ScoreBucket / NameMatchKind /
PKEvidence enums. Spark-Row → dataclass conversion happens exactly once at
the pipeline edge via the *.from_row classmethods; dataclass → Spark tuple
happens exactly once in schema_graph.build_inferred_metadata_references_rel.

See worklog/fk-gap-v3-build.md Phases 3 and 3.5.
"""

from __future__ import annotations

import math
import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Mapping

from dbxcarta.contract import generate_id


# --- Enums (replace stringly-typed keys) ------------------------------------

class NameMatchKind(Enum):
    EXACT = "exact"
    SUFFIX = "suffix"


class PKEvidence(Enum):
    DECLARED_PK = "declared_pk"
    UNIQUE_OR_HEUR = "unique_or_heur"


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


# --- Scoring table (enum-keyed) ---------------------------------------------

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

_DECIMAL_RE = re.compile(r"^(?:DECIMAL|NUMERIC)\((\d+)(?:,(\d+))?\)$")
_STRING_PARAM_RE = re.compile(r"^(?:STRING|VARCHAR|CHAR)(?:\(\d+\))?$")
_TOKEN_SPLIT_RE = re.compile(r"[^a-zA-Z0-9]+")


# --- Nominal types ----------------------------------------------------------

@dataclass(frozen=True, slots=True)
class ColumnMeta:
    """Driver-side column metadata. Built once at the Spark boundary."""

    catalog: str
    schema: str
    table: str
    column: str
    data_type: str
    comment: str | None

    @classmethod
    def from_row(cls, row: Mapping[str, Any]) -> "ColumnMeta":
        """Spark-Row → ColumnMeta conversion; the only place that renames
        `table_catalog` → `catalog`, `column_name` → `column`, etc."""
        return cls(
            catalog=row["table_catalog"],
            schema=row["table_schema"],
            table=row["table_name"],
            column=row["column_name"],
            data_type=row["data_type"],
            comment=row["comment"],
        )

    @property
    def table_key(self) -> str:
        return generate_id(self.catalog, self.schema, self.table)

    @property
    def col_id(self) -> str:
        return generate_id(self.catalog, self.schema, self.table, self.column)


@dataclass(frozen=True, slots=True)
class ConstraintRow:
    """Raw information_schema.key_column_usage row, post-boundary conversion."""

    table_catalog: str
    table_schema: str
    table_name: str
    column_name: str
    constraint_type: str
    ordinal_position: int
    constraint_name: str

    @classmethod
    def from_row(cls, row: Mapping[str, Any]) -> "ConstraintRow":
        return cls(
            table_catalog=row["table_catalog"],
            table_schema=row["table_schema"],
            table_name=row["table_name"],
            column_name=row["column_name"],
            constraint_type=row["constraint_type"],
            ordinal_position=row["ordinal_position"],
            constraint_name=row["constraint_name"],
        )

    @property
    def table_key(self) -> str:
        return generate_id(self.table_catalog, self.table_schema, self.table_name)


@dataclass(frozen=True, slots=True)
class DeclaredPair:
    """A declared-FK endpoint pair; frozen so accidental swaps can't mutate
    the suppression set."""

    source_id: str
    target_id: str


@dataclass(frozen=True, slots=True)
class InferredRef:
    """Emitted inferred REFERENCES edge. Built in the inference layer, tuple-
    converted once in schema_graph.build_inferred_metadata_references_rel."""

    source_id: str
    target_id: str
    confidence: float
    source: str
    criteria: str | None


@dataclass(frozen=True, slots=True)
class PKIndex:
    """What the DBA declared: single-column primary keys and leftmost UNIQUE
    columns, keyed by table. Composite PKs are counted but not indexed —
    a single column that is unique only in combination with others is not
    FK-target material (worklog: composite-key non-goal).

    Deliberately does *not* include heuristic `id` / `{table}_id` fallbacks;
    those are inferred from column names inside _pk_kind via
    _build_id_cols_index. Keeping this type pure to its declared source."""

    pk_cols: dict[str, frozenset[str]]
    unique_leftmost: dict[str, frozenset[str]]
    composite_pk_count: int

    @classmethod
    def from_constraints(cls, rows: list[ConstraintRow]) -> "PKIndex":
        pk_by_constraint: dict[tuple[str, str, str], list[str]] = {}
        pk_table_by_constraint: dict[tuple[str, str, str], str] = {}
        unique_leftmost_build: dict[str, set[str]] = {}

        for r in rows:
            constraint_key = (r.table_catalog, r.table_schema, r.constraint_name)
            if r.constraint_type == "PRIMARY KEY":
                pk_by_constraint.setdefault(constraint_key, []).append(r.column_name)
                pk_table_by_constraint[constraint_key] = r.table_key
            elif r.constraint_type == "UNIQUE" and r.ordinal_position == 1:
                unique_leftmost_build.setdefault(r.table_key, set()).add(r.column_name)

        pk_cols_build: dict[str, set[str]] = {}
        composite_count = 0
        for ckey, cols in pk_by_constraint.items():
            table_key = pk_table_by_constraint[ckey]
            if len(cols) == 1:
                pk_cols_build.setdefault(table_key, set()).add(cols[0])
            else:
                composite_count += 1

        return cls(
            pk_cols={k: frozenset(v) for k, v in pk_cols_build.items()},
            unique_leftmost={k: frozenset(v) for k, v in unique_leftmost_build.items()},
            composite_pk_count=composite_count,
        )


@dataclass
class InferenceCounters:
    """Mutable aggregate of inference bookkeeping.

    Invariant: after inference finishes,
      candidates == accepted + sum(rejections.values())
    Enforced by test_counter_invariant.

    Flattens to run_summary.row_counts via as_summary_dict(prefix); the prefix
    concat is the single string-building point for these counters."""

    candidates: int = 0
    accepted: int = 0
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
        }
        for reason, count in self.rejections.items():
            out[f"{prefix}_{reason.value}"] = count
        for bucket, count in self.buckets.items():
            out[f"{prefix}_{bucket.counter_key}"] = count
        return out


# --- Helpers ----------------------------------------------------------------

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


def _name_match(src_col: str, tgt_col: str, tgt_table: str) -> NameMatchKind | None:
    """Return EXACT, SUFFIX, or None per Phase 3 name rules.

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


def _build_id_cols_index(columns: list[ColumnMeta]) -> dict[str, list[str]]:
    """Free function, not a PKIndex field — this is inferred from the column
    list, not from declared constraint metadata (advisor 2026-04-23)."""
    index: dict[str, list[str]] = {}
    for c in columns:
        if c.column.lower().endswith("_id"):
            index.setdefault(c.table_key, []).append(c.column)
    return index


def _pk_kind(
    tgt: ColumnMeta,
    pk_index: PKIndex,
    id_cols_by_table: dict[str, list[str]],
) -> PKEvidence | None:
    """Classify target's PK-likeness.

    Falls back to name heuristics (column == "id", or == "{table}_id" and sole
    _id-suffixed column) so the phase still fires on catalogs with no declared
    PKs. The fallback collapses into UNIQUE_OR_HEUR rather than a third
    bucket, matching the worklog score table's two PK columns.
    """
    if tgt.column in pk_index.pk_cols.get(tgt.table_key, frozenset()):
        return PKEvidence.DECLARED_PK
    if tgt.column in pk_index.unique_leftmost.get(tgt.table_key, frozenset()):
        return PKEvidence.UNIQUE_OR_HEUR
    col_lower = tgt.column.lower()
    if col_lower == "id":
        return PKEvidence.UNIQUE_OR_HEUR
    if col_lower == f"{tgt.table.lower()}_id":
        id_cols = id_cols_by_table.get(tgt.table_key, [])
        if len(id_cols) == 1 and id_cols[0].lower() == col_lower:
            return PKEvidence.UNIQUE_OR_HEUR
    return None


# --- Entry point ------------------------------------------------------------

def infer_fk_pairs(
    columns: list[ColumnMeta],
    pk_index: PKIndex,
    declared_pairs: frozenset[DeclaredPair],
    threshold: float = 0.8,
) -> tuple[list[InferredRef], InferenceCounters]:
    """Emit metadata-inferred REFERENCES refs.

    columns: driver-side materialised list of ColumnMeta. Pipeline layer
      converts via ColumnMeta.from_row once at the Spark boundary.
    pk_index: declared single-column PKs and UNIQUE-leftmost columns,
      pre-partitioned so composite PKs are dropped (non-goal per worklog).
    declared_pairs: frozenset of DeclaredPair already covered by declared
      FKs; suppressed from output to avoid duplicate edges.
    threshold: minimum attenuated score to emit. Default 0.8 matches retriever.
    """
    counters = InferenceCounters()
    id_cols_by_table = _build_id_cols_index(columns)

    # per_source[src_id] = [(score, target_id, name_kind, pk_evidence), ...]
    per_source: dict[str, list[tuple[float, str, NameMatchKind, PKEvidence]]] = {}

    for src in columns:
        for tgt in columns:
            if src is tgt:
                continue
            counters.record_candidate()

            nm = _name_match(src.column, tgt.column, tgt.table)
            if nm is None:
                counters.record_rejected(RejectionReason.NAME)
                continue
            if src.table_key == tgt.table_key and src.column.lower() == tgt.column.lower():
                # Degenerate self-reference on same column; self-ref with
                # different column names is allowed (employee.manager_id → employee.id).
                counters.record_rejected(RejectionReason.NAME)
                continue
            if not _types_compatible(src.data_type, tgt.data_type):
                counters.record_rejected(RejectionReason.TYPE)
                continue
            pk = _pk_kind(tgt, pk_index, id_cols_by_table)
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

    refs: list[InferredRef] = []
    for src_id, candidates in per_source.items():
        n = len(candidates)
        denom = max(1.0, math.sqrt(max(0, n - 1)))
        for score, tgt_id, _nm, _pk in candidates:
            attenuated = score / denom if denom > 1 else score
            if attenuated < threshold:
                counters.record_rejected(RejectionReason.TIE_BREAK)
                continue
            if DeclaredPair(source_id=src_id, target_id=tgt_id) in declared_pairs:
                counters.record_rejected(RejectionReason.DUPLICATE_DECLARED)
                continue
            refs.append(InferredRef(
                source_id=src_id,
                target_id=tgt_id,
                confidence=round(attenuated, 4),
                source="inferred_metadata",
                criteria=None,
            ))
            counters.record_accepted(score)

    return refs, counters
