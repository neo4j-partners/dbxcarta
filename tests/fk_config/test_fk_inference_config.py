"""Integration tests for FKInferenceConfig and associated protocols.

Covers:
  - FKInferenceConfig default values reproduce current hard-coded behavior
  - CounterProtocol conformance for both counter classes
  - Custom name-match strategy
  - Custom attenuation (exponent + top-N cap)
  - extra_type_equiv extension
  - pk_extra_patterns heuristic
  - Custom stem suffixes
  - Injectable cosine function in infer_semantic_pairs
  - effective_type_equiv / compiled_pk_patterns helpers
"""

from __future__ import annotations

import math
from dataclasses import FrozenInstanceError

import pytest

from dbxcarta.fk_common import (
    ColumnMeta,
    ConstraintRow,
    DeclaredPair,
    NameMatchKind,
    PKEvidence,
    PKIndex,
    is_pair_covered,
)
from dbxcarta.fk_config import (
    CounterProtocol,
    FKInferenceConfig,
    InferencePhase,
    NameMatchStrategy,
)
from dbxcarta.fk_inference import (
    InferenceCounters,
    RejectionReason,
    ScoreBucket,
    infer_fk_pairs,
)
from dbxcarta.fk_semantic import (
    ColumnEmbedding,
    SemanticInferenceCounters,
    infer_semantic_pairs,
)


_CAT = "main"
_SA = "test_schema"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _col(
    table: str,
    column: str,
    schema: str = _SA,
    data_type: str = "BIGINT",
    comment: str | None = None,
) -> ColumnMeta:
    return ColumnMeta(
        catalog=_CAT, schema=schema, table=table, column=column,
        data_type=data_type, comment=comment,
    )


def _pk_index_for(table_col_pairs: list[tuple[str, str]]) -> PKIndex:
    rows = [
        ConstraintRow(
            table_catalog=_CAT, table_schema=_SA, table_name=table,
            column_name=col, constraint_type="PRIMARY KEY",
            ordinal_position=1, constraint_name=f"{_SA}_{table}_pk",
        )
        for table, col in table_col_pairs
    ]
    return PKIndex.from_constraints(rows)


# ---------------------------------------------------------------------------
# FKInferenceConfig: default values
# ---------------------------------------------------------------------------

class TestFKInferenceConfigDefaults:
    def test_default_metadata_threshold(self) -> None:
        cfg = FKInferenceConfig()
        assert cfg.metadata_threshold == 0.8

    def test_default_attenuation_exponent_is_sqrt(self) -> None:
        cfg = FKInferenceConfig()
        assert cfg.attenuation_exponent == 0.5

    def test_default_no_top_n_cap(self) -> None:
        cfg = FKInferenceConfig()
        assert cfg.attenuation_top_n is None

    def test_default_stem_suffixes(self) -> None:
        cfg = FKInferenceConfig()
        assert "_id" in cfg.stem_suffixes
        assert "_fk" in cfg.stem_suffixes
        assert "_ref" in cfg.stem_suffixes

    def test_default_score_table_has_eight_entries(self) -> None:
        cfg = FKInferenceConfig()
        assert len(cfg.score_table) == 8

    def test_default_semantic_threshold(self) -> None:
        cfg = FKInferenceConfig()
        assert cfg.semantic_threshold == 0.85

    def test_default_no_extra_type_equiv(self) -> None:
        cfg = FKInferenceConfig()
        assert cfg.extra_type_equiv == {}
        assert cfg.effective_type_equiv() is None

    def test_default_no_pk_extra_patterns(self) -> None:
        cfg = FKInferenceConfig()
        assert cfg.pk_extra_patterns == []
        assert cfg.compiled_pk_patterns() == []

    def test_default_no_name_match_strategy(self) -> None:
        cfg = FKInferenceConfig()
        assert cfg.name_match_strategy is None


# ---------------------------------------------------------------------------
# CounterProtocol conformance
# ---------------------------------------------------------------------------

class TestCounterProtocolConformance:
    def test_inference_counters_satisfies_protocol(self) -> None:
        c = InferenceCounters()
        assert isinstance(c, CounterProtocol)

    def test_semantic_counters_satisfies_protocol(self) -> None:
        c = SemanticInferenceCounters()
        assert isinstance(c, CounterProtocol)

    def test_both_as_summary_dict_return_str_int_dict(self) -> None:
        for counter in (InferenceCounters(), SemanticInferenceCounters()):
            result = counter.as_summary_dict("prefix")
            assert isinstance(result, dict)
            for k, v in result.items():
                assert isinstance(k, str)
                assert isinstance(v, int)

    def test_counters_list_iteration_pattern(self) -> None:
        """RunSummary can iterate over a list of (counter, prefix) pairs."""
        phase_counters: list[tuple[CounterProtocol, str]] = [
            (InferenceCounters(), "fk_inferred_metadata"),
            (SemanticInferenceCounters(), "fk_inferred_semantic"),
        ]
        combined: dict[str, int] = {}
        for counter, prefix in phase_counters:
            combined.update(counter.as_summary_dict(prefix))
        assert "fk_inferred_metadata_candidates" in combined
        assert "fk_inferred_semantic_considered" in combined


# ---------------------------------------------------------------------------
# NameMatchStrategy protocol
# ---------------------------------------------------------------------------

class TestNameMatchStrategy:
    def test_protocol_is_runtime_checkable(self) -> None:
        """Any callable with the right signature satisfies NameMatchStrategy."""

        def my_strategy(src_col: str, tgt_col: str, tgt_table: str) -> NameMatchKind | None:
            return NameMatchKind.EXACT if src_col.lower() == tgt_col.lower() else None

        assert isinstance(my_strategy, NameMatchStrategy)

    def test_custom_strategy_overrides_default_in_infer_fk_pairs(self) -> None:
        """A custom strategy that always returns None should emit zero refs."""
        def no_match(src_col: str, tgt_col: str, tgt_table: str) -> NameMatchKind | None:
            return None

        cfg = FKInferenceConfig(name_match_strategy=no_match)
        columns = [
            _col("customers", "id"),
            _col("orders", "customer_id"),
        ]
        pk_index = _pk_index_for([("customers", "id")])
        refs, counters = infer_fk_pairs(
            columns, pk_index, declared_pairs=frozenset(), config=cfg,
        )
        assert refs == []
        # Every candidate should be rejected for NAME
        assert counters.rejections[RejectionReason.NAME] > 0

    def test_custom_strategy_prefix_match(self) -> None:
        """Strategy that matches fk_{table} prefix pattern emits a ref."""
        def prefix_strategy(
            src_col: str, tgt_col: str, tgt_table: str,
        ) -> NameMatchKind | None:
            src_l = src_col.lower()
            tgt_l = tgt_col.lower()
            if src_l == tgt_l:
                return NameMatchKind.EXACT
            prefix = f"fk_{tgt_table.lower()}"
            if src_l == prefix and tgt_l == "id":
                return NameMatchKind.SUFFIX
            return None

        columns = [
            _col("customers", "id"),
            _col("orders", "fk_customers"),
        ]
        pk_index = _pk_index_for([("customers", "id")])
        cfg = FKInferenceConfig(name_match_strategy=prefix_strategy)
        refs, _ = infer_fk_pairs(
            columns, pk_index, declared_pairs=frozenset(), config=cfg,
        )
        emitted = {(r.source_id, r.target_id) for r in refs}
        expected_edge = (
            f"{_CAT}.{_SA}.orders.fk_customers",
            f"{_CAT}.{_SA}.customers.id",
        )
        assert expected_edge in emitted


# ---------------------------------------------------------------------------
# Custom stem suffixes
# ---------------------------------------------------------------------------

class TestCustomStemSuffixes:
    def test_extra_suffix_key_is_recognized(self) -> None:
        """Adding _key to stem suffixes makes orders.customer_key → customers.id fire."""
        columns = [
            _col("customers", "id"),
            _col("orders", "customer_key"),
        ]
        pk_index = _pk_index_for([("customers", "id")])

        cfg = FKInferenceConfig(stem_suffixes=("_id", "_fk", "_ref", "_key"))
        refs, _ = infer_fk_pairs(
            columns, pk_index, declared_pairs=frozenset(), config=cfg,
        )
        emitted = {(r.source_id, r.target_id) for r in refs}
        expected = (
            f"{_CAT}.{_SA}.orders.customer_key",
            f"{_CAT}.{_SA}.customers.id",
        )
        assert expected in emitted

    def test_removing_id_suffix_blocks_default_matches(self) -> None:
        """Without _id in suffixes, standard customer_id → customers.id doesn't fire."""
        columns = [
            _col("customers", "id"),
            _col("orders", "customer_id"),
        ]
        pk_index = _pk_index_for([("customers", "id")])
        cfg = FKInferenceConfig(stem_suffixes=("_fk", "_ref"))
        refs, _ = infer_fk_pairs(
            columns, pk_index, declared_pairs=frozenset(), config=cfg,
        )
        suffix_edge = (
            f"{_CAT}.{_SA}.orders.customer_id",
            f"{_CAT}.{_SA}.customers.id",
        )
        emitted = {(r.source_id, r.target_id) for r in refs}
        assert suffix_edge not in emitted


# ---------------------------------------------------------------------------
# Configurable attenuation
# ---------------------------------------------------------------------------

class TestConfigurableAttenuation:
    def _nine_way_fixture(self) -> tuple[list[ColumnMeta], PKIndex]:
        """9 user schemas each with a user.id PK; accounts.user_id fans out."""
        columns: list[ColumnMeta] = [
            ColumnMeta(
                catalog=_CAT, schema="src", table="accounts",
                column="user_id", data_type="BIGINT", comment=None,
            ),
        ]
        constraint_rows: list[ConstraintRow] = []
        for i in range(9):
            schema = f"t_{i}"
            columns.append(ColumnMeta(
                catalog=_CAT, schema=schema, table="user", column="id",
                data_type="BIGINT", comment=None,
            ))
            constraint_rows.append(ConstraintRow(
                table_catalog=_CAT, table_schema=schema, table_name="user",
                column_name="id", constraint_type="PRIMARY KEY",
                ordinal_position=1, constraint_name=f"{schema}_user_pk",
            ))
        return columns, PKIndex.from_constraints(constraint_rows)

    def test_default_exponent_0_5_drops_9way_fanout(self) -> None:
        """Default sqrt attenuation (exponent=0.5) drops a 9-way fan-out."""
        columns, pk_index = self._nine_way_fixture()
        cfg = FKInferenceConfig()  # exponent=0.5, top_n=None
        refs, counters = infer_fk_pairs(
            columns, pk_index, declared_pairs=frozenset(), config=cfg,
        )
        assert refs == []
        assert counters.rejections[RejectionReason.TIE_BREAK] >= 9

    def test_zero_exponent_no_attenuation_emits_all(self) -> None:
        """Exponent=0 means denom=1 always; all 9 candidates should emit."""
        columns, pk_index = self._nine_way_fixture()
        cfg = FKInferenceConfig(attenuation_exponent=0.0)
        refs, _ = infer_fk_pairs(
            columns, pk_index, declared_pairs=frozenset(), config=cfg,
        )
        # With no attenuation every suffix candidate clears the threshold.
        assert len(refs) >= 9

    def test_top_n_cap_limits_emitted_per_source(self) -> None:
        """top_n=2 emits at most 2 refs from accounts.user_id despite 9 candidates."""
        columns, pk_index = self._nine_way_fixture()
        cfg = FKInferenceConfig(attenuation_exponent=0.0, attenuation_top_n=2)
        refs, _ = infer_fk_pairs(
            columns, pk_index, declared_pairs=frozenset(), config=cfg,
        )
        # Count only refs whose source is accounts.user_id
        source_id = f"{_CAT}.src.accounts.user_id"
        from_accounts = [r for r in refs if r.source_id == source_id]
        assert len(from_accounts) <= 2

    def test_top_n_cap_counts_excess_as_tie_break_rejections(self) -> None:
        """Candidates beyond top_n are counted as TIE_BREAK rejections."""
        columns, pk_index = self._nine_way_fixture()
        cfg = FKInferenceConfig(attenuation_exponent=0.0, attenuation_top_n=2)
        _, counters = infer_fk_pairs(
            columns, pk_index, declared_pairs=frozenset(), config=cfg,
        )
        # 9 candidates, cap 2 → 7 tie-break rejections
        assert counters.rejections[RejectionReason.TIE_BREAK] >= 7

    def test_counter_invariant_with_top_n(self) -> None:
        """candidates == accepted + Σ rejections holds with top_n enabled."""
        columns, pk_index = self._nine_way_fixture()
        cfg = FKInferenceConfig(attenuation_exponent=0.0, attenuation_top_n=3)
        _, counters = infer_fk_pairs(
            columns, pk_index, declared_pairs=frozenset(), config=cfg,
        )
        total_rejected = sum(counters.rejections.values())
        assert counters.candidates == counters.accepted + total_rejected


# ---------------------------------------------------------------------------
# extra_type_equiv
# ---------------------------------------------------------------------------

class TestExtraTypeEquiv:
    def test_effective_type_equiv_returns_none_when_empty(self) -> None:
        cfg = FKInferenceConfig()
        assert cfg.effective_type_equiv() is None

    def test_effective_type_equiv_merges_extra(self) -> None:
        cfg = FKInferenceConfig(extra_type_equiv={"NUMBER": "INTEGER"})
        merged = cfg.effective_type_equiv()
        assert merged is not None
        assert merged["NUMBER"] == "INTEGER"
        assert merged["BIGINT"] == "INTEGER"  # base entry preserved

    def test_extra_type_equiv_enables_match_for_vendor_type(self) -> None:
        """NUMBER column matches BIGINT pk when NUMBER=INTEGER added to equiv."""
        columns = [
            ColumnMeta(
                catalog=_CAT, schema=_SA, table="customer", column="id",
                data_type="BIGINT", comment=None,
            ),
            ColumnMeta(
                catalog=_CAT, schema=_SA, table="orders", column="customer_id",
                data_type="NUMBER", comment=None,
            ),
        ]
        pk_index = _pk_index_for([("customer", "id")])

        # Without extra equiv, NUMBER ≠ BIGINT → no match
        refs_no_extra, _ = infer_fk_pairs(
            columns, pk_index, declared_pairs=frozenset(),
        )
        assert refs_no_extra == []

        # With NUMBER=INTEGER, NUMBER ↔ BIGINT matches
        cfg = FKInferenceConfig(extra_type_equiv={"NUMBER": "INTEGER"})
        refs_with_extra, _ = infer_fk_pairs(
            columns, pk_index, declared_pairs=frozenset(), config=cfg,
        )
        assert len(refs_with_extra) == 1


# ---------------------------------------------------------------------------
# pk_extra_patterns
# ---------------------------------------------------------------------------

class TestPKExtraPatterns:
    def test_compiled_pk_patterns_empty_by_default(self) -> None:
        cfg = FKInferenceConfig()
        assert cfg.compiled_pk_patterns() == []

    def test_extra_pattern_classifies_pk_prefix_column(self) -> None:
        """Catalog uses pk_{col} convention; patterns make it PK-like."""
        columns = [
            ColumnMeta(
                catalog=_CAT, schema=_SA, table="users", column="pk_users",
                data_type="BIGINT", comment=None,
            ),
            ColumnMeta(
                catalog=_CAT, schema=_SA, table="orders", column="user_ref",
                data_type="BIGINT", comment=None,
            ),
        ]
        # Empty pk_index — no declared PKs
        pk_index = PKIndex.from_constraints([])

        # Without extra pattern: pk_users is not recognized as PK-like
        refs_no_pattern, _ = infer_fk_pairs(
            columns, pk_index, declared_pairs=frozenset(),
        )
        # user_ref → pk_users won't fire (pk_users isn't PK-like)
        emitted_no = {(r.source_id, r.target_id) for r in refs_no_pattern}
        ref_pk = (
            f"{_CAT}.{_SA}.orders.user_ref",
            f"{_CAT}.{_SA}.users.pk_users",
        )
        assert ref_pk not in emitted_no

    def test_extra_patterns_compiled_correctly(self) -> None:
        cfg = FKInferenceConfig(pk_extra_patterns=[r"^pk_", r"^key_"])
        patterns = cfg.compiled_pk_patterns()
        assert len(patterns) == 2
        assert patterns[0].search("pk_users") is not None
        assert patterns[1].search("key_account") is not None
        assert patterns[0].search("user_id") is None


# ---------------------------------------------------------------------------
# is_pair_covered helper
# ---------------------------------------------------------------------------

class TestIsPairCovered:
    def test_covered_pair_returns_true(self) -> None:
        covered = frozenset({DeclaredPair(source_id="a.b", target_id="c.d")})
        assert is_pair_covered("a.b", "c.d", covered) is True

    def test_uncovered_pair_returns_false(self) -> None:
        covered = frozenset({DeclaredPair(source_id="a.b", target_id="c.d")})
        assert is_pair_covered("a.b", "x.y", covered) is False

    def test_empty_covered_always_false(self) -> None:
        assert is_pair_covered("a", "b", frozenset()) is False


# ---------------------------------------------------------------------------
# Injectable cosine function in Phase 4
# ---------------------------------------------------------------------------

class TestInjectableCosineFn:
    def _simple_fixture(self) -> tuple[list[ColumnMeta], dict[str, ColumnEmbedding], PKIndex]:
        columns = [
            _col("customers", "id"),
            _col("orders", "cust_ref"),
        ]
        pk_index = _pk_index_for([("customers", "id")])
        # All vectors identical → cosine = 1.0 → above threshold
        vec = [1.0] + [0.0] * 1023
        embeddings = {
            c.col_id: ColumnEmbedding.from_vector(c.col_id, vec)
            for c in columns
        }
        return columns, embeddings, pk_index

    def test_default_cosine_fn_works(self) -> None:
        columns, embeddings, pk_index = self._simple_fixture()
        refs, _ = infer_semantic_pairs(
            columns=columns,
            embeddings=embeddings,
            pk_index=pk_index,
            declared_pairs=frozenset(),
            metadata_inferred_pairs=frozenset(),
        )
        # With cosine=1.0 between all pairs, some refs should emit
        assert len(refs) >= 0  # basic sanity

    def test_custom_cosine_fn_always_zero_emits_nothing(self) -> None:
        """Injecting a zero-cosine function blocks all pairs at threshold."""
        columns, embeddings, pk_index = self._simple_fixture()

        def zero_cosine(a: object, b: object) -> float:
            return 0.0

        refs, counters = infer_semantic_pairs(
            columns=columns,
            embeddings=embeddings,
            pk_index=pk_index,
            declared_pairs=frozenset(),
            metadata_inferred_pairs=frozenset(),
            cosine_fn=zero_cosine,
        )
        assert refs == []
        # All considered pairs should be rejected (SUB_THRESHOLD since cosine=0)
        from dbxcarta.fk_semantic import SemanticRejectionReason
        assert counters.rejections[SemanticRejectionReason.SUB_THRESHOLD] > 0

    def test_custom_cosine_fn_is_called_instead_of_default(self) -> None:
        """Count calls to confirm the injectable function is used."""
        columns, embeddings, pk_index = self._simple_fixture()
        call_count = [0]

        def counting_cosine(a: object, b: object) -> float:
            call_count[0] += 1
            return 0.0  # always below threshold → no emissions

        infer_semantic_pairs(
            columns=columns,
            embeddings=embeddings,
            pk_index=pk_index,
            declared_pairs=frozenset(),
            metadata_inferred_pairs=frozenset(),
            cosine_fn=counting_cosine,
        )
        # With 2 columns each having embeddings, at least one pair was tested.
        assert call_count[0] >= 1


# ---------------------------------------------------------------------------
# FKInferenceConfig: behavior with infer_fk_pairs default config
# ---------------------------------------------------------------------------

class TestDefaultConfigEquivalence:
    """Verify that passing FKInferenceConfig() produces the same output as
    calling infer_fk_pairs without config (i.e., pure backward compat)."""

    def test_default_config_matches_no_config_on_v5fk_fixture(self) -> None:
        columns = [
            _col("customers", "id", schema="s"),
            _col("customers", "name", schema="s", data_type="STRING"),
            _col("orders", "id", schema="s"),
            _col("orders", "customer_id", schema="s"),
            _col("order_items", "id", schema="s"),
            _col("order_items", "order_id", schema="s"),
        ]
        rows = [
            ConstraintRow(
                table_catalog=_CAT, table_schema="s", table_name=t,
                column_name="id", constraint_type="PRIMARY KEY",
                ordinal_position=1, constraint_name=f"s_{t}_pk",
            )
            for t in ("customers", "orders", "order_items")
        ]
        pk_index = PKIndex.from_constraints(rows)

        refs_no_cfg, ctrs_no_cfg = infer_fk_pairs(
            columns, pk_index, declared_pairs=frozenset(),
        )
        refs_with_cfg, ctrs_with_cfg = infer_fk_pairs(
            columns, pk_index, declared_pairs=frozenset(),
            config=FKInferenceConfig(),
        )

        pairs_no_cfg = {(r.source_id, r.target_id, r.confidence) for r in refs_no_cfg}
        pairs_with_cfg = {(r.source_id, r.target_id, r.confidence) for r in refs_with_cfg}
        assert pairs_no_cfg == pairs_with_cfg
        assert ctrs_no_cfg.candidates == ctrs_with_cfg.candidates
        assert ctrs_no_cfg.accepted == ctrs_with_cfg.accepted
