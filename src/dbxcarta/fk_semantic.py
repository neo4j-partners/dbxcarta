"""Phase 4: semantic (embedding-based) FK inference.

Runs after Phase 3. Considers only column pairs not already covered by
declared FKs or Phase-3 metadata-inferred edges. Candidate gate reuses
`pk_kind` and `types_compatible` from `fk_common`.

Confidence model:
  - Cosine similarity ≥ `threshold` (default 0.85) to be considered.
  - Base confidence = clamp(similarity, 0.80, 0.90).
  - +0.05 bonus when value-overlap corroborates (source values ≥ 50%
    contained in target values), capped at 0.90.
  - Below 0.85 → dropped entirely; no dead storage.

Scale: driver-side cosine over candidate pairs. Candidate gates (PK-likeness,
type compat, already-covered suppression) keep the working set a small
fraction of the Cartesian product. Pure-Python cosine (no numpy) — adequate
at the 10⁴–10⁵-column scale target; see worklog scale notes. The cosine
function is injectable via the `cosine_fn` parameter for callers that want a
numpy-backed or BLAS-backed implementation on larger catalogs.

Entry point: infer_semantic_pairs(embeddings, columns, pk_index, ...).
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Callable

from dbxcarta.contract import EdgeSource
from dbxcarta.fk_common import (
    ColumnMeta,
    DeclaredPair,
    InferredRef,
    PKIndex,
    build_id_cols_index,
    is_pair_covered,
    pk_kind,
    types_compatible,
)

if TYPE_CHECKING:
    from dbxcarta.fk_config import FKInferenceConfig


class SemanticRejectionReason(Enum):
    TYPE = "rejected_type"
    PK = "rejected_pk"
    ALREADY_COVERED = "rejected_already_covered"
    SUB_THRESHOLD = "rejected_sub_threshold"


@dataclass(frozen=True, slots=True)
class ColumnEmbedding:
    """Column's 1024-dim vector + norm cached for cosine.

    frozen=True, slots=True — nominal type at the pipeline edge. Vectors
    arrive as `list[float]` (matching Spark `array<double>`); the dataclass
    caches `norm` so pairwise cosine doesn't recompute it.

    eq=False: frozen dataclasses normally auto-generate __hash__ over every
    field, which for 1024-float lists is both slow and meaningless (we never
    put ColumnEmbedding in a set). Skipping eq skips the hash."""

    col_id: str
    vector: tuple[float, ...]
    norm: float

    @classmethod
    def from_vector(cls, col_id: str, vector: list[float]) -> "ColumnEmbedding":
        v = tuple(vector)
        return cls(col_id=col_id, vector=v, norm=math.sqrt(sum(x * x for x in v)))


@dataclass(frozen=True, slots=True)
class ValueIndex:
    """Sampled distinct values per column, built at the sample-values boundary.

    Empty when DBXCARTA_INCLUDE_VALUES is off — Phase 4 treats missing as
    "no corroboration available" (bonus simply doesn't apply)."""

    values_by_col_id: dict[str, frozenset[str]]

    @classmethod
    def empty(cls) -> "ValueIndex":
        return cls(values_by_col_id={})

    def overlap_ratio(self, src_id: str, tgt_id: str) -> float | None:
        src = self.values_by_col_id.get(src_id)
        tgt = self.values_by_col_id.get(tgt_id)
        if not src or not tgt:
            return None
        return len(src & tgt) / len(src)


@dataclass
class SemanticInferenceCounters:
    """Mutable aggregate of Phase 4 bookkeeping.

    Invariant (enforced by test_semantic_counter_invariant):
      considered == accepted + sum(rejections.values())

    `value_corroborated` is an independent tally — not a rejection outcome —
    counting accepted pairs whose confidence was bumped by value overlap."""

    considered: int = 0
    accepted: int = 0
    value_corroborated: int = 0
    rejections: dict[SemanticRejectionReason, int] = field(
        default_factory=lambda: {r: 0 for r in SemanticRejectionReason},
    )

    def record_considered(self) -> None:
        self.considered += 1

    def record_rejected(self, reason: SemanticRejectionReason) -> None:
        self.rejections[reason] += 1

    def record_accepted(self, *, value_corroborated: bool) -> None:
        self.accepted += 1
        if value_corroborated:
            self.value_corroborated += 1

    def as_summary_dict(self, prefix: str) -> dict[str, int]:
        out: dict[str, int] = {
            f"{prefix}_considered": self.considered,
            f"{prefix}_accepted": self.accepted,
            f"{prefix}_value_corroborated": self.value_corroborated,
        }
        for reason, count in self.rejections.items():
            out[f"{prefix}_{reason.value}"] = count
        return out


def _cosine(a: ColumnEmbedding, b: ColumnEmbedding) -> float:
    """Pure-Python cosine. numpy is not a declared dep and isn't needed at
    the worklog's scale target."""
    if a.norm == 0.0 or b.norm == 0.0:
        return 0.0
    dot = sum(x * y for x, y in zip(a.vector, b.vector))
    return dot / (a.norm * b.norm)


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def infer_semantic_pairs(
    columns: list[ColumnMeta],
    embeddings: dict[str, ColumnEmbedding],
    pk_index: PKIndex,
    declared_pairs: frozenset[DeclaredPair],
    metadata_inferred_pairs: frozenset[DeclaredPair],
    value_index: ValueIndex | None = None,
    *,
    threshold: float = 0.85,
    floor: float = 0.80,
    cap: float = 0.90,
    value_bonus: float = 0.05,
    overlap_threshold: float = 0.5,
    config: "FKInferenceConfig | None" = None,
    cosine_fn: Callable[[ColumnEmbedding, ColumnEmbedding], float] | None = None,
) -> tuple[list[InferredRef], SemanticInferenceCounters]:
    """Emit semantic-inferred REFERENCES refs tagged with EdgeSource.SEMANTIC.

    columns: ColumnMeta for every column in the catalog-under-inference.
    embeddings: col_id → ColumnEmbedding. Columns without an embedding (e.g.,
      ai_query errors) are silently skipped from the candidate pool — no
      counter increment because they never entered the "considered" set.
    pk_index: same PKIndex Phase 3 built; reused here for the target gate.
    declared_pairs: declared FKs (already covered).
    metadata_inferred_pairs: Phase 3 output (already covered).
    value_index: optional; when present, values-overlap corroboration applies.
    config: optional FKInferenceConfig. When provided, its semantic_* fields
      supply the defaults for threshold/floor/cap/value_bonus/overlap_threshold
      (explicit keyword arguments to this function take precedence).
    cosine_fn: optional injectable cosine function. Defaults to the pure-Python
      _cosine implementation. Pass a numpy-backed function for larger catalogs.

    Returns (refs, counters). refs carry `source=EdgeSource.SEMANTIC`.
    """
    from dbxcarta.fk_config import FKInferenceConfig as _FKCfg

    cfg = config if config is not None else _FKCfg()

    # Resolve effective parameter values: explicit kwargs take precedence over
    # config defaults (which take precedence over the function default literals).
    _threshold = threshold if threshold != 0.85 else cfg.semantic_threshold
    _floor = floor if floor != 0.80 else cfg.semantic_floor
    _cap = cap if cap != 0.90 else cfg.semantic_cap
    _value_bonus = value_bonus if value_bonus != 0.05 else cfg.semantic_value_bonus
    _overlap_threshold = (
        overlap_threshold if overlap_threshold != 0.5 else cfg.semantic_overlap_threshold
    )
    _cosine_fn = cosine_fn if cosine_fn is not None else _cosine
    type_equiv = cfg.effective_type_equiv()
    pk_patterns = cfg.compiled_pk_patterns()

    counters = SemanticInferenceCounters()
    id_cols_by_table = build_id_cols_index(columns)
    covered = declared_pairs | metadata_inferred_pairs

    refs: list[InferredRef] = []
    for src in columns:
        src_emb = embeddings.get(src.col_id)
        if src_emb is None:
            continue
        for tgt in columns:
            if src is tgt:
                continue
            tgt_emb = embeddings.get(tgt.col_id)
            if tgt_emb is None:
                continue

            counters.record_considered()

            if is_pair_covered(src.col_id, tgt.col_id, covered):
                counters.record_rejected(SemanticRejectionReason.ALREADY_COVERED)
                continue
            if not types_compatible(src.data_type, tgt.data_type, type_equiv):
                counters.record_rejected(SemanticRejectionReason.TYPE)
                continue
            if pk_kind(tgt, pk_index, id_cols_by_table, pk_patterns or None) is None:
                counters.record_rejected(SemanticRejectionReason.PK)
                continue

            sim = _cosine_fn(src_emb, tgt_emb)
            if sim < _threshold:
                counters.record_rejected(SemanticRejectionReason.SUB_THRESHOLD)
                continue

            confidence = _clamp(sim, _floor, _cap)
            corroborated = False
            if value_index is not None:
                ratio = value_index.overlap_ratio(src.col_id, tgt.col_id)
                if ratio is not None and ratio >= _overlap_threshold:
                    confidence = _clamp(confidence + _value_bonus, _floor, _cap)
                    corroborated = True

            refs.append(InferredRef(
                source_id=src.col_id,
                target_id=tgt.col_id,
                confidence=round(confidence, 4),
                source=EdgeSource.SEMANTIC,
                criteria=None,
            ))
            counters.record_accepted(value_corroborated=corroborated)

    return refs, counters
