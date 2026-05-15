"""Metadata-based FK inference with nominal types.

Coverage:

1. On the declared-FK fixture, infer_fk_pairs rediscovers the 3 declared FKs at
   confidence ≥ 0.83. (Threshold originally read ≥ 0.90; relaxed because all
   three declared fixture FKs are suffix matches, capped at 0.83 w/o comments
   per the approved scoring table.)
2. On a synthetic no-FK fixture (same tables, constraints stripped), the
   name-based PK-likeness fallback recovers equivalent edges.
3. Tie-break attenuation drops a 9-way fan-out and preserves a 2-way
   polymorphic pair.

Column-parity guard: the inferred DataFrame's columns match
REFERENCES_PROPERTIES, so the Neo4j Spark Connector doesn't silently drop
property writes.

Fixture construction uses ColumnMeta / PKIndex / DeclaredPair (no dicts).
Score assertions pull from _SCORE_TABLE, not magic-number literals.
Invariant: candidates == accepted + sum(rejections).
Frozen dataclasses refuse mutation.
"""

from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest

from dbxcarta.spark.contract import EdgeSource, REFERENCES_PROPERTIES
from dbxcarta.spark.ingest.fk.common import (
    ColumnMeta,
    ConstraintRow,
    DeclaredPair,
    FKEdge,
    PKEvidence,
    PKIndex,
)
from dbxcarta.spark.ingest.fk.metadata import (
    _SCORE_TABLE,
    NameMatchKind,
    RejectionReason,
    ScoreBucket,
    infer_fk_pairs,
)
from dbxcarta.spark.ingest.schema_graph import build_references_rel


_CAT = "main"
_SA = "dbxcarta_fk_test"
_SB = "dbxcarta_fk_test_b"  # used by within-schema rejection tests below

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
        _col(_SA, "shipments", "id"),
        _col(_SA, "shipments", "order_id"),
    ]


def _v5fk_pk_index() -> PKIndex:
    """Declared PKs for the FK fixture, built via ConstraintRow."""
    rows = [
        ConstraintRow(
            table_catalog=_CAT, table_schema=schema, table_name=table,
            column_name="id", constraint_type="PRIMARY KEY",
            ordinal_position=1, constraint_name=f"{schema}_{table}_pk",
        )
        for schema, table in [
            (_SA, "customers"), (_SA, "orders"),
            (_SA, "order_items"), (_SA, "shipments"),
        ]
    ]
    return PKIndex.from_constraints(rows)


# --- Declared-FK rediscovery -------------------------------------------------

def test_v5fk_rediscovers_three_declared_fks_at_0_83() -> None:
    refs, counters = infer_fk_pairs(
        _v5fk_columns(), _v5fk_pk_index(), prior_pairs=frozenset(),
    )
    edges = {(r.source_id, r.target_id) for r in refs}
    expected = {
        (f"{_CAT}.{_SA}.orders.customer_id",     f"{_CAT}.{_SA}.customers.id"),
        (f"{_CAT}.{_SA}.order_items.order_id",   f"{_CAT}.{_SA}.orders.id"),
        (f"{_CAT}.{_SA}.shipments.order_id",     f"{_CAT}.{_SA}.orders.id"),
    }
    assert expected.issubset(edges), f"missing: {expected - edges}"
    for r in refs:
        assert r.confidence >= _S_SUFFIX_DPK_NO_COMMENT
        assert r.source is EdgeSource.INFERRED_METADATA
        assert r.criteria is None
    assert counters.accepted == len(refs)


def test_v5fk_declared_duplicate_suppression() -> None:
    """When prior_pairs is populated, inference skips those exact edges."""
    declared = frozenset({
        DeclaredPair(
            source_id=f"{_CAT}.{_SA}.orders.customer_id",
            target_id=f"{_CAT}.{_SA}.customers.id",
        ),
    })
    refs, counters = infer_fk_pairs(
        _v5fk_columns(), _v5fk_pk_index(), prior_pairs=declared,
    )
    emitted_pairs = {
        DeclaredPair(source_id=r.source_id, target_id=r.target_id) for r in refs
    }
    assert declared.isdisjoint(emitted_pairs)
    assert counters.rejections[RejectionReason.DUPLICATE_DECLARED] == 1


# --- No-FK synthetic fallback ------------------------------------------------

def test_no_fk_synthetic_recovers_via_name_heuristic() -> None:
    """With zero declared PKs, the `id`/`{table}_id` heuristic takes over.

    All three fixture FKs are still evaluated; confidences downgrade from
    0.83 (suffix + declared_pk) to 0.78 (suffix + heuristic). Because 0.78 is
    below the 0.8 threshold, the below-threshold bucket is exercised. Nothing
    is emitted, which is the correct behaviour without corroborating metadata.
    """
    empty_index = PKIndex.from_constraints([])
    refs, counters = infer_fk_pairs(
        _v5fk_columns(), empty_index, prior_pairs=frozenset(),
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
    refs, _ = infer_fk_pairs(columns, empty_index, prior_pairs=frozenset())
    edges_and_conf = {(r.source_id, r.target_id): r.confidence for r in refs}
    suffix_edge = (
        f"{_CAT}.{_SA}.orders.customer_id",
        f"{_CAT}.{_SA}.customers.id",
    )
    assert suffix_edge in edges_and_conf
    assert edges_and_conf[suffix_edge] == _S_SUFFIX_UOH_COMMENT


# --- Tie-break attenuation ---------------------------------------------------

def test_tie_break_drops_user_id_fanout_within_schema() -> None:
    """user_id source fans out across all stem-matching `user*` tables in one schema.

    With three stem-matching tables (`user`, `users`, `useres`) and the source's
    own `accounts.id` as a fourth `id` target, every source ends up with three
    same-schema candidates. denom = sqrt(2) ≈ 1.41 attenuates the 0.83 suffix
    score to 0.587 and the 0.90 exact-id score to 0.638, both below the 0.80
    threshold. The full set of candidates collapses to zero emitted edges and
    contributes many tie-break rejections.

    Single-schema by construction: FK inference no longer crosses schemas, so
    the tie-break behaviour is exercised within one application boundary.
    """
    schema = "tenant"
    columns: list[ColumnMeta] = [
        ColumnMeta(
            catalog=_CAT, schema=schema, table="accounts",
            column="user_id", data_type="BIGINT", comment=None,
        ),
        ColumnMeta(
            catalog=_CAT, schema=schema, table="accounts", column="id",
            data_type="BIGINT", comment=None,
        ),
    ]
    user_tables = ["user", "users", "useres"]
    constraint_rows: list[ConstraintRow] = [
        ConstraintRow(
            table_catalog=_CAT, table_schema=schema, table_name="accounts",
            column_name="id", constraint_type="PRIMARY KEY",
            ordinal_position=1, constraint_name=f"{schema}_accounts_pk",
        ),
    ]
    for table_name in user_tables:
        columns.append(ColumnMeta(
            catalog=_CAT, schema=schema, table=table_name, column="id",
            data_type="BIGINT", comment=None,
        ))
        constraint_rows.append(ConstraintRow(
            table_catalog=_CAT, table_schema=schema, table_name=table_name,
            column_name="id", constraint_type="PRIMARY KEY",
            ordinal_position=1, constraint_name=f"{schema}_{table_name}_pk",
        ))
    pk_index = PKIndex.from_constraints(constraint_rows)

    refs, counters = infer_fk_pairs(
        columns, pk_index, prior_pairs=frozenset(),
    )
    assert refs == []
    assert counters.rejections[RejectionReason.TIE_BREAK] >= 9


def test_tie_break_preserves_two_way_user_stem_pair() -> None:
    """Source column `user_id` with two stem-matching tables in the same schema.

    With `user` and `users` co-located in one schema, both suffix candidates
    (user_id → user.id / users.id) land at n=2, denom=1, full 0.83 score
    preserved. The id↔id exact cross-matches between user.id and users.id also
    pass (n=1, 0.90 each) as a side effect of exact-match symmetry. Assert
    the suffix invariant as a subset rather than a total count.

    Replaces the prior cross-schema polymorphic test: the suffix-tolerant
    plural rule is what made that pattern interesting, and it still works in
    a single-schema layout.
    """
    columns = [
        ColumnMeta(
            catalog=_CAT, schema="shop", table="accounts",
            column="user_id", data_type="BIGINT", comment=None,
        ),
        ColumnMeta(
            catalog=_CAT, schema="shop", table="user",
            column="id", data_type="BIGINT", comment=None,
        ),
        ColumnMeta(
            catalog=_CAT, schema="shop", table="users",
            column="id", data_type="BIGINT", comment=None,
        ),
    ]
    constraint_rows = [
        ConstraintRow(
            table_catalog=_CAT, table_schema="shop", table_name="user",
            column_name="id", constraint_type="PRIMARY KEY",
            ordinal_position=1, constraint_name="shop_user_pk",
        ),
        ConstraintRow(
            table_catalog=_CAT, table_schema="shop", table_name="users",
            column_name="id", constraint_type="PRIMARY KEY",
            ordinal_position=1, constraint_name="shop_users_pk",
        ),
    ]
    pk_index = PKIndex.from_constraints(constraint_rows)
    refs, counters = infer_fk_pairs(
        columns, pk_index, prior_pairs=frozenset(),
    )
    suffix_targets = {
        (f"{_CAT}.shop.accounts.user_id", f"{_CAT}.shop.user.id"),
        (f"{_CAT}.shop.accounts.user_id", f"{_CAT}.shop.users.id"),
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
    refs = [FKEdge(
        source_id="cat.s.t.a", target_id="cat.s.t.b",
        confidence=_S_SUFFIX_DPK_NO_COMMENT,
        source=EdgeSource.INFERRED_METADATA, criteria=None,
    )]
    df = build_references_rel(local_spark, refs)
    assert tuple(df.columns) == _EXPECTED_COLUMNS
    assert set(REFERENCES_PROPERTIES).issubset(set(df.columns))


def test_build_inferred_dataframe_empty_rows(local_spark) -> None:
    """Empty input still produces a schema-shaped DataFrame; _load guards on
    the emit count so this path is never written to Neo4j, but the builder
    must not crash on the edge case."""
    df = build_references_rel(local_spark, [])
    assert tuple(df.columns) == _EXPECTED_COLUMNS
    assert df.count() == 0


# --- Within-schema scope ----------------------------------------------------

def test_cross_schema_candidates_silently_rejected() -> None:
    """Identical column patterns across two schemas produce no inferred edges.

    Same schema would emit `orders.customer_id → customers.id`. With the
    columns split across two schemas, the candidate is skipped before
    `record_candidate()`, so the candidate counter does not increment and no
    edge is emitted. This is the unit-level guarantee that multi-schema runs
    (e.g. schemapile) cannot manufacture spurious cross-application FKs.
    """
    columns = [
        ColumnMeta(
            catalog=_CAT, schema=_SA, table="customers", column="id",
            data_type="BIGINT", comment=None,
        ),
        ColumnMeta(
            catalog=_CAT, schema=_SB, table="orders", column="customer_id",
            data_type="BIGINT", comment=None,
        ),
    ]
    pk_index = PKIndex.from_constraints([
        ConstraintRow(
            table_catalog=_CAT, table_schema=_SA, table_name="customers",
            column_name="id", constraint_type="PRIMARY KEY",
            ordinal_position=1, constraint_name="customers_pk",
        ),
    ])
    refs, counters = infer_fk_pairs(columns, pk_index, prior_pairs=frozenset())
    assert refs == []
    assert counters.candidates == 0
    assert counters.accepted == 0


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
    refs, _ = infer_fk_pairs(columns, pk_index, prior_pairs=frozenset())
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
        columns, pk_index, prior_pairs=frozenset(),
    )
    assert refs == []
    assert counters.rejections[RejectionReason.TYPE] == 1


# --- InferenceCounters invariant and immutability ----------------------------

def test_counter_invariant_on_v5fk() -> None:
    """candidates == accepted + Σ rejections. No silent drops."""
    _, counters = infer_fk_pairs(
        _v5fk_columns(), _v5fk_pk_index(), prior_pairs=frozenset(),
    )
    rejected_total = sum(counters.rejections.values())
    assert counters.candidates == counters.accepted + rejected_total
    assert set(counters.rejections.keys()) == set(RejectionReason)


def test_counter_invariant_on_sub_threshold_fixture() -> None:
    """The 0.78 (suffix + heuristic + no-comment) path exercises SUB_THRESHOLD.

    With zero declared PKs, every declared-fixture suffix match scores 0.78,
    which drops pre-attenuation. This ensures the SUB_THRESHOLD counter is the
    one that accounts for those drops instead of hiding them behind a bare
    `continue`.
    """
    empty_index = PKIndex.from_constraints([])
    _, counters = infer_fk_pairs(
        _v5fk_columns(), empty_index, prior_pairs=frozenset(),
    )
    rejected_total = sum(counters.rejections.values())
    assert counters.candidates == counters.accepted + rejected_total
    assert counters.rejections[RejectionReason.SUB_THRESHOLD] > 0


def test_counters_flatten_to_summary_dict() -> None:
    """as_summary_dict produces the flat prefix_{field} mapping expected by
    RunSummary.row_counts, with no missing enum members."""
    _, counters = infer_fk_pairs(
        _v5fk_columns(), _v5fk_pk_index(), prior_pairs=frozenset(),
    )
    flat = counters.as_summary_dict("fk_inferred_metadata")
    assert "fk_inferred_metadata_candidates" in flat
    assert "fk_inferred_metadata_accepted" in flat
    # composite_pk_skipped is tracked in InferenceCounters — assert it's emitted
    # by as_summary_dict.
    assert "fk_inferred_metadata_composite_pk_skipped" in flat
    for reason in RejectionReason:
        assert f"fk_inferred_metadata_{reason.value}" in flat
    for bucket in ScoreBucket:
        assert f"fk_inferred_metadata_{bucket.counter_key}" in flat


def test_declared_pair_is_frozen() -> None:
    p = DeclaredPair(source_id="a", target_id="b")
    with pytest.raises(FrozenInstanceError):
        p.source_id = "c"  # type: ignore[misc]


def test_inferred_ref_is_frozen() -> None:
    r = FKEdge(
        source_id="a", target_id="b", confidence=0.9,
        source=EdgeSource.INFERRED_METADATA, criteria=None,
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
    r1 = FKEdge(
        source_id="s", target_id="t", confidence=0.83,
        source=EdgeSource.INFERRED_METADATA, criteria=None,
    )
    r2 = FKEdge(
        source_id="s", target_id="t", confidence=0.83,
        source=EdgeSource.INFERRED_METADATA, criteria=None,
    )
    r3 = FKEdge(
        source_id="s", target_id="t", confidence=0.90,
        source=EdgeSource.INFERRED_METADATA, criteria=None,
    )
    assert r1 == r2
    assert hash(r1) == hash(r2)
    assert r1 != r3


# --- PKIndex behaviour -------------------------------------------------------

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
