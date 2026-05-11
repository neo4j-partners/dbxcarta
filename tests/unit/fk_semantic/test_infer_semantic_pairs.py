"""Semantic FK inference over hand-crafted 1024-dim vectors.

Coverage:

1. On a synthetic `buyer_ref → customers.id` / `purchase_ref → orders.id`
   rename fixture plus hand-crafted vectors satisfying exact cosine
   constraints, `infer_semantic_pairs` recovers both edges at confidence ≥ 0.80.
2. Min-catalog-size gate is exercised at the pipeline boundary; here we
   exercise the pure function semantics (gate lives in `ingest.fk.discovery`).

Additional invariants:
- Frozen-dataclass immutability on ColumnEmbedding and ValueIndex.
- Counter invariant: considered == accepted + Σ rejections.
- EdgeSource.SEMANTIC tagging.
- Value-overlap corroboration bumps confidence but never exceeds cap.
- Already-covered suppression against metadata-inferred pairs.
"""

from __future__ import annotations

import pickle
from dataclasses import FrozenInstanceError
from pathlib import Path

import pytest

from dbxcarta.contract import EdgeSource
from dbxcarta.ingest.fk.common import ColumnMeta, ConstraintRow, DeclaredPair, PKIndex
from dbxcarta.ingest.fk.semantic import (
    ColumnEmbedding,
    SemanticRejectionReason,
    ValueIndex,
    infer_semantic_pairs,
)


_CAT = "main"
_SCHEMA = "shop"


def _col(table: str, column: str, data_type: str = "BIGINT") -> ColumnMeta:
    return ColumnMeta(
        catalog=_CAT, schema=_SCHEMA, table=table, column=column,
        data_type=data_type, comment=None,
    )


def _embeddings_from_pkl(path: Path) -> dict[str, ColumnEmbedding]:
    with path.open("rb") as f:
        raw: dict[str, list[float]] = pickle.load(f)
    return {col_id: ColumnEmbedding.from_vector(col_id, vec) for col_id, vec in raw.items()}


def _fixture_columns() -> list[ColumnMeta]:
    return [
        _col("customers", "id"),
        _col("orders", "id"),
        _col("orders", "buyer_ref"),
        _col("order_items", "purchase_ref"),
        _col("unrelated", "payload"),
    ]


def _fixture_pk_index() -> PKIndex:
    rows = [
        ConstraintRow(
            table_catalog=_CAT, table_schema=_SCHEMA, table_name=t,
            column_name="id", constraint_type="PRIMARY KEY",
            ordinal_position=1, constraint_name=f"{t}_pk",
        )
        for t in ("customers", "orders")
    ]
    return PKIndex.from_constraints(rows)


# --- Deliverable 1: renamed FKs recovered -----------------------------------

def test_recovers_renamed_edges_above_threshold(phase4_embeddings_pkl: Path) -> None:
    """buyer_ref → customers.id and purchase_ref → orders.id both emit.

    Hand-crafted cosines are 0.88 and 0.87. Base confidence = clamp(sim, 0.80,
    0.90). No value-overlap index in this test so no bonus is applied.
    """
    embeddings = _embeddings_from_pkl(phase4_embeddings_pkl)
    refs, counters = infer_semantic_pairs(
        columns=_fixture_columns(),
        embeddings=embeddings,
        pk_index=_fixture_pk_index(),
        prior_pairs=frozenset(),
    )

    by_pair = {(r.source_id, r.target_id): r for r in refs}
    buyer_edge = (f"{_CAT}.{_SCHEMA}.orders.buyer_ref",
                  f"{_CAT}.{_SCHEMA}.customers.id")
    purch_edge = (f"{_CAT}.{_SCHEMA}.order_items.purchase_ref",
                  f"{_CAT}.{_SCHEMA}.orders.id")

    assert buyer_edge in by_pair
    assert purch_edge in by_pair
    assert by_pair[buyer_edge].confidence == pytest.approx(0.88, abs=1e-3)
    assert by_pair[purch_edge].confidence == pytest.approx(0.87, abs=1e-3)
    for r in refs:
        assert r.source is EdgeSource.SEMANTIC
        assert r.criteria is None
    assert counters.accepted >= 2


def test_unrelated_pair_rejected_sub_threshold(phase4_embeddings_pkl: Path) -> None:
    """The unrelated.payload column is orthogonal to customers.id; cosine=0
    < 0.85 threshold → SUB_THRESHOLD rejection, no edge emitted."""
    embeddings = _embeddings_from_pkl(phase4_embeddings_pkl)
    refs, counters = infer_semantic_pairs(
        columns=_fixture_columns(),
        embeddings=embeddings,
        pk_index=_fixture_pk_index(),
        prior_pairs=frozenset(),
    )
    emitted_pairs = {(r.source_id, r.target_id) for r in refs}
    unrelated_edge = (
        f"{_CAT}.{_SCHEMA}.unrelated.payload",
        f"{_CAT}.{_SCHEMA}.customers.id",
    )
    assert unrelated_edge not in emitted_pairs
    # The unrelated column is on a table with no PK and is not PK-like itself,
    # so either SUB_THRESHOLD or PK rejection accounts for it — both are
    # legitimate depending on pair direction. Invariant still holds below.
    assert counters.rejections[SemanticRejectionReason.SUB_THRESHOLD] > 0


# --- Covered-pair suppression -----------------------------------------------

def test_suppresses_edges_already_covered_by_declared(
    phase4_embeddings_pkl: Path,
) -> None:
    """A declared-FK pair never re-emits as SEMANTIC, even if cosine qualifies."""
    embeddings = _embeddings_from_pkl(phase4_embeddings_pkl)
    declared = frozenset({
        DeclaredPair(
            source_id=f"{_CAT}.{_SCHEMA}.orders.buyer_ref",
            target_id=f"{_CAT}.{_SCHEMA}.customers.id",
        ),
    })
    refs, counters = infer_semantic_pairs(
        columns=_fixture_columns(),
        embeddings=embeddings,
        pk_index=_fixture_pk_index(),
        prior_pairs=declared,
    )
    emitted = {DeclaredPair(source_id=r.source_id, target_id=r.target_id) for r in refs}
    assert declared.isdisjoint(emitted)
    assert counters.rejections[SemanticRejectionReason.ALREADY_COVERED] >= 1


def test_suppresses_edges_already_covered_by_metadata(
    phase4_embeddings_pkl: Path,
) -> None:
    """Same rule for metadata-inferred pairs: semantic doesn't re-emit them."""
    embeddings = _embeddings_from_pkl(phase4_embeddings_pkl)
    metadata_pairs = frozenset({
        DeclaredPair(
            source_id=f"{_CAT}.{_SCHEMA}.order_items.purchase_ref",
            target_id=f"{_CAT}.{_SCHEMA}.orders.id",
        ),
    })
    refs, _ = infer_semantic_pairs(
        columns=_fixture_columns(),
        embeddings=embeddings,
        pk_index=_fixture_pk_index(),
        prior_pairs=metadata_pairs,
    )
    emitted = {DeclaredPair(source_id=r.source_id, target_id=r.target_id) for r in refs}
    assert metadata_pairs.isdisjoint(emitted)


# --- Value-overlap corroboration --------------------------------------------

def test_value_overlap_adds_bonus_up_to_cap(phase4_embeddings_pkl: Path) -> None:
    """When ≥ 50% of source values appear in target values, confidence bumps
    by +0.05 — but caps at 0.90. The 0.87 pair becomes 0.90 (not 0.92)."""
    embeddings = _embeddings_from_pkl(phase4_embeddings_pkl)
    value_index = ValueIndex(values_by_col_id={
        f"{_CAT}.{_SCHEMA}.orders.buyer_ref":
            frozenset({"v1", "v2", "v3", "v4"}),
        f"{_CAT}.{_SCHEMA}.customers.id":
            frozenset({"v1", "v2", "v3", "v9", "v10"}),  # 3/4 = 75% overlap
    })
    refs, counters = infer_semantic_pairs(
        columns=_fixture_columns(),
        embeddings=embeddings,
        pk_index=_fixture_pk_index(),
        prior_pairs=frozenset(),
        value_index=value_index,
    )
    buyer_edge = (
        f"{_CAT}.{_SCHEMA}.orders.buyer_ref",
        f"{_CAT}.{_SCHEMA}.customers.id",
    )
    by_pair = {(r.source_id, r.target_id): r for r in refs}
    assert by_pair[buyer_edge].confidence == pytest.approx(0.90, abs=1e-3)
    assert counters.value_corroborated >= 1


def test_value_overlap_below_threshold_no_bonus(phase4_embeddings_pkl: Path) -> None:
    """25% overlap is below the 50% threshold — no bonus applied."""
    embeddings = _embeddings_from_pkl(phase4_embeddings_pkl)
    value_index = ValueIndex(values_by_col_id={
        f"{_CAT}.{_SCHEMA}.orders.buyer_ref":
            frozenset({"v1", "v2", "v3", "v4"}),
        f"{_CAT}.{_SCHEMA}.customers.id":
            frozenset({"v1", "v9", "v10", "v11"}),  # 1/4 = 25% overlap
    })
    refs, counters = infer_semantic_pairs(
        columns=_fixture_columns(),
        embeddings=embeddings,
        pk_index=_fixture_pk_index(),
        prior_pairs=frozenset(),
        value_index=value_index,
    )
    buyer_edge = (
        f"{_CAT}.{_SCHEMA}.orders.buyer_ref",
        f"{_CAT}.{_SCHEMA}.customers.id",
    )
    by_pair = {(r.source_id, r.target_id): r for r in refs}
    assert by_pair[buyer_edge].confidence == pytest.approx(0.88, abs=1e-3)
    assert counters.value_corroborated == 0


# --- Missing embeddings are skipped, not errored ----------------------------

def test_columns_without_embeddings_are_silently_skipped(
    phase4_embeddings_pkl: Path,
) -> None:
    """ai_query failures leave some col_ids without vectors. Those columns
    must not appear as src or tgt in any candidate — they're pre-filtered
    before `considered` is incremented."""
    embeddings = _embeddings_from_pkl(phase4_embeddings_pkl)
    # Add a column with no embedding.
    cols = _fixture_columns() + [_col("ghost", "id")]
    refs, counters = infer_semantic_pairs(
        columns=cols,
        embeddings=embeddings,
        pk_index=_fixture_pk_index(),
        prior_pairs=frozenset(),
    )
    for r in refs:
        assert "ghost" not in r.source_id
        assert "ghost" not in r.target_id
    # Invariant must still hold against what WAS considered.
    total_rejections = sum(counters.rejections.values())
    assert counters.considered == counters.accepted + total_rejections


# --- Counter invariant ------------------------------------------------------

def test_counter_invariant(phase4_embeddings_pkl: Path) -> None:
    """considered == accepted + Σ rejections. No silent drops."""
    embeddings = _embeddings_from_pkl(phase4_embeddings_pkl)
    _, counters = infer_semantic_pairs(
        columns=_fixture_columns(),
        embeddings=embeddings,
        pk_index=_fixture_pk_index(),
        prior_pairs=frozenset(),
    )
    total_rejections = sum(counters.rejections.values())
    assert counters.considered == counters.accepted + total_rejections
    assert set(counters.rejections.keys()) == set(SemanticRejectionReason)


def test_counters_flatten_to_summary_dict(phase4_embeddings_pkl: Path) -> None:
    """as_summary_dict produces the flat prefix_{field} mapping expected by
    RunSummary.row_counts, covering every rejection reason."""
    embeddings = _embeddings_from_pkl(phase4_embeddings_pkl)
    _, counters = infer_semantic_pairs(
        columns=_fixture_columns(),
        embeddings=embeddings,
        pk_index=_fixture_pk_index(),
        prior_pairs=frozenset(),
    )
    flat = counters.as_summary_dict("fk_inferred_semantic")
    assert "fk_inferred_semantic_considered" in flat
    assert "fk_inferred_semantic_accepted" in flat
    assert "fk_inferred_semantic_value_corroborated" in flat
    for reason in SemanticRejectionReason:
        assert f"fk_inferred_semantic_{reason.value}" in flat


# --- Immutability -----------------------------------------------------------

def test_column_embedding_is_frozen() -> None:
    emb = ColumnEmbedding.from_vector("c1", [1.0, 0.0, 0.0])
    with pytest.raises(FrozenInstanceError):
        emb.col_id = "c2"  # type: ignore[misc]


def test_value_index_is_frozen() -> None:
    idx = ValueIndex.empty()
    with pytest.raises(FrozenInstanceError):
        idx.values_by_col_id = {}  # type: ignore[misc]


def test_column_embedding_norm_matches_euclidean() -> None:
    """from_vector caches sqrt(Σ x²). Round-trip via fixture pkl vectors."""
    vec = [3.0, 4.0, 0.0]
    emb = ColumnEmbedding.from_vector("c", vec)
    assert emb.norm == pytest.approx(5.0)
    assert emb.vector == (3.0, 4.0, 0.0)
