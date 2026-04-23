"""Phase 3 + 3.5 deliverables: metadata-based FK inference with nominal types.

Three deliverables from worklog/fk-gap-v3-build.md Phase 3:

1. On the v5-fk fixture, infer_fk_pairs rediscovers the 3 declared FKs at
   confidence ≥ 0.83. (Worklog originally read ≥ 0.90; relaxed after sign-off
   because all three v5-fk FKs are suffix matches, capped at 0.83 w/o comments
   per the approved scoring table.)
2. On a synthetic no-FK fixture (same tables, constraints stripped), the
   name-based PK-likeness fallback recovers equivalent edges.
3. Tie-break attenuation drops a 9-way fan-out and preserves a 2-way
   polymorphic pair.

Plus column-parity guard: the inferred DataFrame's columns match
REFERENCES_PROPERTIES, so the Neo4j Spark Connector doesn't silently drop
property writes.

Phase 3.5 additions:
- All fixture construction uses ColumnMeta / PKIndex / DeclaredPair (no dicts).
- Score assertions pull from _SCORE_TABLE, not magic-number literals.
- New invariant test: candidates == accepted + sum(rejections).
- New immutability test: frozen dataclasses refuse mutation.
"""

from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest

from dbxcarta.contract import REFERENCES_PROPERTIES
from dbxcarta.fk_inference import (
    _SCORE_TABLE,
    ColumnMeta,
    ConstraintRow,
    DeclaredPair,
    InferredRef,
    NameMatchKind,
    PKEvidence,
    PKIndex,
    RejectionReason,
    ScoreBucket,
    infer_fk_pairs,
)
from dbxcarta.schema_graph import build_inferred_metadata_references_rel


_CAT = "main"
_SA = "dbxcarta_fk_test"
_SB = "dbxcarta_fk_test_b"

# Score constants resolved once from the scoring table so tests move with it.
_S_SUFFIX_DPK_NO_COMMENT = _SCORE_TABLE[
    (NameMatchKind.SUFFIX, PKEvidence.DECLARED_PK, False)
]  # 0.83
_S_SUFFIX_UOH_COMMENT = _SCORE_TABLE[
    (NameMatchKind.SUFFIX, PKEvidence.UNIQUE_OR_HEUR, True)
]  # 0.82
_S_EXACT_UOH_COMMENT = _SCORE_TABLE[
    (NameMatchKind.EXACT, PKEvidence.UNIQUE_OR_HEUR, True)
]  # 0.88


def _col(
    schema: str, table: str, column: str,
    data_type: str = "BIGINT", comment: str | None = None,
) -> ColumnMeta:
    return ColumnMeta(
        catalog=_CAT, schema=schema, table=table, column=column,
        data_type=data_type, comment=comment,
    )


def _v5fk_columns() -> list[ColumnMeta]:
    return [
        _col(_SA, "customers", "id"),
        _col(_SA, "customers", "name", data_type="STRING"),
        _col(_SA, "orders", "id"),
        _col(_SA, "orders", "customer_id"),
        _col(_SA, "order_items", "id"),
        _col(_SA, "order_items", "order_id"),
        _col(_SB, "shipments", "id"),
        _col(_SB, "shipments", "order_id"),
    ]


def _v5fk_pk_index() -> PKIndex:
    """Declared PKs for the v5-fk fixture, built via ConstraintRow."""
    rows = [
        ConstraintRow(
            table_catalog=_CAT, table_schema=schema, table_name=table,
            column_name="id", constraint_type="PRIMARY KEY",
            ordinal_position=1, constraint_name=f"{schema}_{table}_pk",
        )
        for schema, table in [
            (_SA, "customers"), (_SA, "orders"),
            (_SA, "order_items"), (_SB, "shipments"),
        ]
    ]
    return PKIndex.from_constraints(rows)


# --- Deliverable 1: v5-fk rediscovery ---------------------------------------

def test_v5fk_rediscovers_three_declared_fks_at_0_83() -> None:
    refs, counters = infer_fk_pairs(
        _v5fk_columns(), _v5fk_pk_index(), declared_pairs=frozenset(),
    )
    edges = {(r.source_id, r.target_id) for r in refs}
    expected = {
        (f"{_CAT}.{_SA}.orders.customer_id",     f"{_CAT}.{_SA}.customers.id"),
        (f"{_CAT}.{_SA}.order_items.order_id",   f"{_CAT}.{_SA}.orders.id"),
        (f"{_CAT}.{_SB}.shipments.order_id",     f"{_CAT}.{_SA}.orders.id"),
    }
    assert expected.issubset(edges), f"missing: {expected - edges}"
    for r in refs:
        assert r.confidence >= _S_SUFFIX_DPK_NO_COMMENT
        assert r.source == "inferred_metadata"
        assert r.criteria is None
    assert counters.accepted == len(refs)


def test_v5fk_declared_duplicate_suppression() -> None:
    """When declared_pairs is populated, inference skips those exact edges."""
    declared = frozenset({
        DeclaredPair(
            source_id=f"{_CAT}.{_SA}.orders.customer_id",
            target_id=f"{_CAT}.{_SA}.customers.id",
        ),
    })
    refs, counters = infer_fk_pairs(
        _v5fk_columns(), _v5fk_pk_index(), declared_pairs=declared,
    )
    emitted_pairs = {
        DeclaredPair(source_id=r.source_id, target_id=r.target_id) for r in refs
    }
    assert declared.isdisjoint(emitted_pairs)
    assert counters.rejections[RejectionReason.DUPLICATE_DECLARED] == 1


# --- Deliverable 2: no-FK synthetic fallback --------------------------------

def test_no_fk_synthetic_recovers_via_name_heuristic() -> None:
    """With zero declared PKs, the `id`/`{table}_id` heuristic takes over.

    All three v5-fk FKs are still recovered; confidences downgrade from 0.83
    (suffix + declared_pk) to 0.78 (suffix + heuristic). 0.78 < 0.8 threshold,
    so the worklog's "borderline audit-only" bucket is exercised — nothing is
    emitted, and that's the correct behaviour.
    """
    empty_index = PKIndex.from_constraints([])
    refs, counters = infer_fk_pairs(
        _v5fk_columns(), empty_index, declared_pairs=frozenset(),
    )
    assert refs == []
    assert counters.accepted == 0


def test_no_fk_synthetic_with_comment_overlap_clears_threshold() -> None:
    """Raising comment-overlap brings suffix+heuristic to 0.82, above 0.8.

    This shows the fallback recovers edges when comments disambiguate — the
    realistic no-declared-PK case on production catalogs with curated comments.
    Exact id↔id cross-matches between customers.id and orders.id also land at
    0.88 (exact + unique_or_heur + comment "identifier"); assert the suffix
    recovery as a subset on the edge of interest.
    """
    columns = [
        ColumnMeta(
            catalog=_CAT, schema=_SA, table="customers", column="id",
            data_type="BIGINT", comment="customer identifier",
        ),
        ColumnMeta(
            catalog=_CAT, schema=_SA, table="customers", column="name",
            data_type="BIGINT", comment=None,
        ),
        ColumnMeta(
            catalog=_CAT, schema=_SA, table="orders", column="id",
            data_type="BIGINT", comment="order identifier",
        ),
        ColumnMeta(
            catalog=_CAT, schema=_SA, table="orders", column="customer_id",
            data_type="BIGINT", comment="customer identifier",
        ),
    ]
    empty_index = PKIndex.from_constraints([])
    refs, _ = infer_fk_pairs(columns, empty_index, declared_pairs=frozenset())
    edges_and_conf = {(r.source_id, r.target_id): r.confidence for r in refs}
    suffix_edge = (
        f"{_CAT}.{_SA}.orders.customer_id",
        f"{_CAT}.{_SA}.customers.id",
    )
    assert suffix_edge in edges_and_conf
    assert edges_and_conf[suffix_edge] == _S_SUFFIX_UOH_COMMENT


# --- Deliverable 3: tie-break attenuation -----------------------------------

def test_tie_break_drops_nine_way_user_id_fanout() -> None:
    """9 tables named user across 9 schemas; every one passes stem match.

    With n=9 (suffix source) and n=8 (each exact-match id source), every
    candidate attenuates below the 0.8 threshold. Entire fan-out drops.
    The 9 user_id → 9 user.id attenuations contribute at least 9 tie-break
    rejections; the additional 72 from id↔id cross-matches take the total to
    81. We assert the lower bound and the "no emitted edge" outcome.
    """
    columns: list[ColumnMeta] = [
        ColumnMeta(
            catalog=_CAT, schema="src_schema", table="accounts",
            column="user_id", data_type="BIGINT", comment=None,
        ),
    ]
    constraint_rows: list[ConstraintRow] = []
    for i in range(9):
        schema = f"tenant_{i}"
        columns.append(ColumnMeta(
            catalog=_CAT, schema=schema, table="user", column="id",
            data_type="BIGINT", comment=None,
        ))
        constraint_rows.append(ConstraintRow(
            table_catalog=_CAT, table_schema=schema, table_name="user",
            column_name="id", constraint_type="PRIMARY KEY",
            ordinal_position=1, constraint_name=f"{schema}_user_pk",
        ))
    pk_index = PKIndex.from_constraints(constraint_rows)

    refs, counters = infer_fk_pairs(
        columns, pk_index, declared_pairs=frozenset(),
    )
    assert refs == []
    assert counters.rejections[RejectionReason.TIE_BREAK] >= 9


def test_tie_break_preserves_two_way_polymorphic_pair() -> None:
    """Source column `user_id` with targets in two `user`-stemmed schemas.

    The two suffix candidates (user_id → primary.user.id / replica.users.id)
    land at n=2, denom=1, full 0.83 score preserved. The two id↔id exact
    cross-matches between primary.user.id and replica.users.id also pass
    (n=1, 0.90 each) — expected side effect of exact-match symmetry. Assert
    the polymorphic-suffix invariant as a subset rather than a total count.
    """
    columns = [
        ColumnMeta(
            catalog=_CAT, schema="primary", table="accounts",
            column="user_id", data_type="BIGINT", comment=None,
        ),
        ColumnMeta(
            catalog=_CAT, schema="primary", table="user",
            column="id", data_type="BIGINT", comment=None,
        ),
        ColumnMeta(
            catalog=_CAT, schema="replica", table="users",
            column="id", data_type="BIGINT", comment=None,
        ),
    ]
    constraint_rows = [
        ConstraintRow(
            table_catalog=_CAT, table_schema="primary", table_name="user",
            column_name="id", constraint_type="PRIMARY KEY",
            ordinal_position=1, constraint_name="primary_user_pk",
        ),
        ConstraintRow(
            table_catalog=_CAT, table_schema="replica", table_name="users",
            column_name="id", constraint_type="PRIMARY KEY",
            ordinal_position=1, constraint_name="replica_users_pk",
        ),
    ]
    pk_index = PKIndex.from_constraints(constraint_rows)
    refs, counters = infer_fk_pairs(
        columns, pk_index, declared_pairs=frozenset(),
    )
    suffix_targets = {
        (f"{_CAT}.primary.accounts.user_id", f"{_CAT}.primary.user.id"),
        (f"{_CAT}.primary.accounts.user_id", f"{_CAT}.replica.users.id"),
    }
    edges_and_conf = {(r.source_id, r.target_id): r.confidence for r in refs}
    assert suffix_targets.issubset(edges_and_conf.keys())
    for pair in suffix_targets:
        assert edges_and_conf[pair] == _S_SUFFIX_DPK_NO_COMMENT
    assert counters.accepted >= 2


# --- Column-parity guard ----------------------------------------------------

_EXPECTED_COLUMNS = ("source_id", "target_id", "confidence", "source", "criteria")


def test_build_inferred_dataframe_columns_match_references_properties(
    local_spark,
) -> None:
    refs = [InferredRef(
        source_id="cat.s.t.a", target_id="cat.s.t.b",
        confidence=_S_SUFFIX_DPK_NO_COMMENT,
        source="inferred_metadata", criteria=None,
    )]
    df = build_inferred_metadata_references_rel(local_spark, refs)
    assert tuple(df.columns) == _EXPECTED_COLUMNS
    assert set(REFERENCES_PROPERTIES).issubset(set(df.columns))


def test_build_inferred_dataframe_empty_rows(local_spark) -> None:
    """Empty input still produces a schema-shaped DataFrame; _load guards on
    the emit count so this path is never written to Neo4j, but the builder
    must not crash on the edge case."""
    df = build_inferred_metadata_references_rel(local_spark, [])
    assert tuple(df.columns) == _EXPECTED_COLUMNS
    assert df.count() == 0


# --- Type-compatibility sanity ----------------------------------------------

def test_type_equiv_accepts_int_and_bigint() -> None:
    """BIGINT ↔ INT ↔ LONG must match under _TYPE_EQUIV."""
    columns = [
        ColumnMeta(
            catalog=_CAT, schema="s", table="orders", column="customer_id",
            data_type="INT", comment=None,
        ),
        ColumnMeta(
            catalog=_CAT, schema="s", table="customer", column="id",
            data_type="BIGINT", comment=None,
        ),
    ]
    pk_index = PKIndex.from_constraints([ConstraintRow(
        table_catalog=_CAT, table_schema="s", table_name="customer",
        column_name="id", constraint_type="PRIMARY KEY",
        ordinal_position=1, constraint_name="s_customer_pk",
    )])
    refs, _ = infer_fk_pairs(columns, pk_index, declared_pairs=frozenset())
    assert len(refs) == 1


def test_type_mismatch_blocks_match() -> None:
    columns = [
        ColumnMeta(
            catalog=_CAT, schema="s", table="orders", column="customer_id",
            data_type="STRING", comment=None,
        ),
        ColumnMeta(
            catalog=_CAT, schema="s", table="customer", column="id",
            data_type="BIGINT", comment=None,
        ),
    ]
    pk_index = PKIndex.from_constraints([ConstraintRow(
        table_catalog=_CAT, table_schema="s", table_name="customer",
        column_name="id", constraint_type="PRIMARY KEY",
        ordinal_position=1, constraint_name="s_customer_pk",
    )])
    refs, counters = infer_fk_pairs(
        columns, pk_index, declared_pairs=frozenset(),
    )
    assert refs == []
    assert counters.rejections[RejectionReason.TYPE] == 1


# --- Phase 3.5: counter invariant and immutability --------------------------

def test_counter_invariant_on_v5fk() -> None:
    """candidates == accepted + Σ rejections. No silent drops."""
    _, counters = infer_fk_pairs(
        _v5fk_columns(), _v5fk_pk_index(), declared_pairs=frozenset(),
    )
    rejected_total = sum(counters.rejections.values())
    assert counters.candidates == counters.accepted + rejected_total
    assert set(counters.rejections.keys()) == set(RejectionReason)


def test_counter_invariant_on_sub_threshold_fixture() -> None:
    """The 0.78 (suffix + heuristic + no-comment) path exercises SUB_THRESHOLD.

    With zero declared PKs, every v5-fk suffix match scores 0.78, which drops
    pre-attenuation. This ensures the SUB_THRESHOLD counter is the one that
    accounts for those drops — previously they were silent `continue`s.
    """
    empty_index = PKIndex.from_constraints([])
    _, counters = infer_fk_pairs(
        _v5fk_columns(), empty_index, declared_pairs=frozenset(),
    )
    rejected_total = sum(counters.rejections.values())
    assert counters.candidates == counters.accepted + rejected_total
    assert counters.rejections[RejectionReason.SUB_THRESHOLD] > 0


def test_counters_flatten_to_summary_dict() -> None:
    """as_summary_dict produces the flat prefix_{field} mapping expected by
    RunSummary.row_counts, with no missing enum members."""
    _, counters = infer_fk_pairs(
        _v5fk_columns(), _v5fk_pk_index(), declared_pairs=frozenset(),
    )
    flat = counters.as_summary_dict("fk_inferred_metadata")
    assert "fk_inferred_metadata_candidates" in flat
    assert "fk_inferred_metadata_accepted" in flat
    for reason in RejectionReason:
        assert f"fk_inferred_metadata_{reason.value}" in flat
    for bucket in ScoreBucket:
        assert f"fk_inferred_metadata_{bucket.counter_key}" in flat


def test_declared_pair_is_frozen() -> None:
    p = DeclaredPair(source_id="a", target_id="b")
    with pytest.raises(FrozenInstanceError):
        p.source_id = "c"  # type: ignore[misc]


def test_inferred_ref_is_frozen() -> None:
    r = InferredRef(
        source_id="a", target_id="b", confidence=0.9,
        source="inferred_metadata", criteria=None,
    )
    with pytest.raises(FrozenInstanceError):
        r.confidence = 0.5  # type: ignore[misc]


def test_column_meta_is_frozen() -> None:
    c = ColumnMeta(
        catalog="x", schema="y", table="z", column="c",
        data_type="BIGINT", comment=None,
    )
    with pytest.raises(FrozenInstanceError):
        c.column = "other"  # type: ignore[misc]


def test_constraint_row_is_frozen() -> None:
    r = ConstraintRow(
        table_catalog="x", table_schema="y", table_name="z",
        column_name="c", constraint_type="PRIMARY KEY",
        ordinal_position=1, constraint_name="x_y_z_pk",
    )
    with pytest.raises(FrozenInstanceError):
        r.column_name = "other"  # type: ignore[misc]


def test_pk_index_is_frozen() -> None:
    idx = PKIndex.from_constraints([])
    with pytest.raises(FrozenInstanceError):
        idx.composite_pk_count = 5  # type: ignore[misc]


def test_declared_pair_equality_and_hash() -> None:
    """frozenset[DeclaredPair] suppression depends on hash+eq, not frozen-ness."""
    a = DeclaredPair(source_id="s", target_id="t")
    b = DeclaredPair(source_id="s", target_id="t")
    c = DeclaredPair(source_id="s", target_id="other")
    assert a == b
    assert hash(a) == hash(b)
    assert a != c
    assert hash(a) != hash(c)
    assert {a, b} == {a}          # dedupes on equality
    assert a in frozenset({b})    # membership via hash+eq


def test_inferred_ref_equality_and_hash() -> None:
    r1 = InferredRef(
        source_id="s", target_id="t", confidence=0.83,
        source="inferred_metadata", criteria=None,
    )
    r2 = InferredRef(
        source_id="s", target_id="t", confidence=0.83,
        source="inferred_metadata", criteria=None,
    )
    r3 = InferredRef(
        source_id="s", target_id="t", confidence=0.90,
        source="inferred_metadata", criteria=None,
    )
    assert r1 == r2
    assert hash(r1) == hash(r2)
    assert r1 != r3


# --- Phase 3.5: PKIndex behaviour -------------------------------------------

def test_pk_index_excludes_composite_keys_and_counts_them() -> None:
    """A two-column declared PK does not populate pk_cols but is counted."""
    rows = [
        ConstraintRow(
            table_catalog="c", table_schema="s", table_name="junction",
            column_name="left_id", constraint_type="PRIMARY KEY",
            ordinal_position=1, constraint_name="junction_pk",
        ),
        ConstraintRow(
            table_catalog="c", table_schema="s", table_name="junction",
            column_name="right_id", constraint_type="PRIMARY KEY",
            ordinal_position=2, constraint_name="junction_pk",
        ),
    ]
    idx = PKIndex.from_constraints(rows)
    assert idx.pk_cols == {}
    assert idx.composite_pk_count == 1


def test_pk_index_unique_leftmost_captures_ordinal_one_only() -> None:
    """A two-column UNIQUE keeps only the leftmost in unique_leftmost."""
    rows = [
        ConstraintRow(
            table_catalog="c", table_schema="s", table_name="t",
            column_name="left", constraint_type="UNIQUE",
            ordinal_position=1, constraint_name="t_uniq",
        ),
        ConstraintRow(
            table_catalog="c", table_schema="s", table_name="t",
            column_name="right", constraint_type="UNIQUE",
            ordinal_position=2, constraint_name="t_uniq",
        ),
    ]
    idx = PKIndex.from_constraints(rows)
    assert idx.unique_leftmost == {"c.s.t": frozenset({"left"})}
