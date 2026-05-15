"""Semantic (embedding-based) FK discovery.

Runs after metadata. Considers only column pairs not already covered by
earlier strategies (declared + metadata). Candidate gate reuses
`pk_kind` and `types_compatible` from `dbxcarta.spark.ingest.fk.common`.

Schema scope: pairs are only considered when source and target columns share
the same (catalog, schema). Cross-schema candidates are skipped before the
`considered` counter increments. This matches the metadata strategy and
prevents cosine matches from connecting unrelated application schemas under
the same catalog.

Confidence model:
  - Cosine similarity ≥ `threshold` (default 0.85) to be considered.
  - Base confidence = clamp(similarity, 0.80, 0.90).
  - +0.05 bonus when value-overlap corroborates (source values ≥ 50%
    contained in target values), capped at 0.90.
  - Below 0.85 → dropped entirely; no dead storage.

Scale: driver-side cosine over candidate pairs. Candidate gates (PK-likeness,
type compat, already-covered suppression) keep the working set a small
fraction of the Cartesian product. Pure-Python cosine (no numpy) — adequate
for the expected catalog sizes because vector math only runs after the cheap
candidate gates.

Entry point: infer_semantic_pairs(embeddings, columns, pk_index, ...).
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from enum import Enum

from dbxcarta.spark.contract import EdgeSource
from dbxcarta.spark.ingest.fk.common import (
    ColumnMeta,
    DeclaredPair,
    FKEdge,
    PKIndex,
    build_id_cols_index,
    pk_kind,
    types_compatible,
)


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

    Empty when DBXCARTA_INCLUDE_VALUES is off. Missing values mean "no
    corroboration available"; the semantic confidence bonus simply does not
    apply."""

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
    """Mutable aggregate of semantic-inference bookkeeping.

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
    """Pure-Python cosine.

    numpy is not a declared dependency, and this function only runs after
    target PK, type-compatibility, and prior-pair filters have reduced the
    candidate set.
    """
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
    prior_pairs: frozenset[DeclaredPair],
    value_index: ValueIndex | None = None,
    *,
    threshold: float = 0.85,
    floor: float = 0.80,
    cap: float = 0.90,
    value_bonus: float = 0.05,
    overlap_threshold: float = 0.5,
) -> tuple[list[FKEdge], SemanticInferenceCounters]:
    """Emit semantic-inferred REFERENCES edges tagged with EdgeSource.SEMANTIC.

    columns: ColumnMeta for every column in the catalog-under-inference.
    embeddings: col_id → ColumnEmbedding. Columns without an embedding (e.g.,
      ai_query errors) are silently skipped from the candidate pool — no
      counter increment because they never entered the "considered" set.
    pk_index: shared PKIndex; reused here for the target gate.
    prior_pairs: pairs already covered by declared + metadata strategies.
    value_index: optional; when present, values-overlap corroboration applies.

    Returns (edges, counters). edges carry `source=EdgeSource.SEMANTIC`.
    """
    counters = SemanticInferenceCounters()
    id_cols_by_table = build_id_cols_index(columns)

    edges: list[FKEdge] = []
    for src in columns:
        src_emb = embeddings.get(src.col_id)
        if src_emb is None:
            continue
        for tgt in columns:
            if src is tgt:
                continue
            if src.catalog != tgt.catalog or src.schema != tgt.schema:
                continue
            tgt_emb = embeddings.get(tgt.col_id)
            if tgt_emb is None:
                continue

            counters.record_considered()

            if DeclaredPair(source_id=src.col_id, target_id=tgt.col_id) in prior_pairs:
                counters.record_rejected(SemanticRejectionReason.ALREADY_COVERED)
                continue
            if not types_compatible(src.data_type, tgt.data_type):
                counters.record_rejected(SemanticRejectionReason.TYPE)
                continue
            if pk_kind(tgt, pk_index, id_cols_by_table) is None:
                counters.record_rejected(SemanticRejectionReason.PK)
                continue

            sim = _cosine(src_emb, tgt_emb)
            if sim < threshold:
                counters.record_rejected(SemanticRejectionReason.SUB_THRESHOLD)
                continue

            confidence = _clamp(sim, floor, cap)
            corroborated = False
            if value_index is not None:
                ratio = value_index.overlap_ratio(src.col_id, tgt.col_id)
                if ratio is not None and ratio >= overlap_threshold:
                    confidence = _clamp(confidence + value_bonus, floor, cap)
                    corroborated = True

            edges.append(FKEdge(
                source_id=src.col_id,
                target_id=tgt.col_id,
                confidence=round(confidence, 4),
                source=EdgeSource.SEMANTIC,
                criteria=None,
            ))
            counters.record_accepted(value_corroborated=corroborated)

    return edges, counters
