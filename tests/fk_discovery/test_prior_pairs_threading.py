"""Uniform `prior_pairs` contract across the three strategies.

Metadata skips pairs covered by declared; semantic skips pairs covered by
declared + metadata. All three strategies accept `prior_pairs` with the
same meaning — "already emitted by an earlier strategy in this run."
"""

from __future__ import annotations

from dbxcarta.contract import EdgeSource
from dbxcarta.fk_common import ColumnMeta, ConstraintRow, DeclaredPair, PKIndex
from dbxcarta.fk_metadata import infer_fk_pairs
from dbxcarta.fk_semantic import ColumnEmbedding, infer_semantic_pairs


_CAT = "main"
_SCHEMA = "shop"


def _col(table: str, column: str) -> ColumnMeta:
    return ColumnMeta(
        catalog=_CAT, schema=_SCHEMA, table=table, column=column,
        data_type="BIGINT", comment=None,
    )


def _pk_index() -> PKIndex:
    rows = [
        ConstraintRow(
            table_catalog=_CAT, table_schema=_SCHEMA, table_name="customers",
            column_name="id", constraint_type="PRIMARY KEY",
            ordinal_position=1, constraint_name="customers_pk",
        ),
    ]
    return PKIndex.from_constraints(rows)


def test_metadata_skips_declared_prior_pair() -> None:
    """Metadata would infer orders.customer_id → customers.id; declared
    covered it first, so prior_pairs suppresses it."""
    columns = [
        _col("customers", "id"),
        _col("orders", "customer_id"),
    ]
    prior = frozenset({
        DeclaredPair(
            source_id=f"{_CAT}.{_SCHEMA}.orders.customer_id",
            target_id=f"{_CAT}.{_SCHEMA}.customers.id",
        ),
    })
    edges, _ = infer_fk_pairs(columns, _pk_index(), prior_pairs=prior)
    emitted = {DeclaredPair(source_id=e.source_id, target_id=e.target_id) for e in edges}
    assert prior.isdisjoint(emitted)


def test_semantic_skips_declared_and_metadata_prior_pairs() -> None:
    """Semantic receives the accumulated declared | metadata set and
    suppresses any pair in that union."""
    columns = [
        _col("customers", "id"),
        _col("orders", "customer_id"),
    ]
    # Identical unit vectors → cosine 1.0, easily above the 0.85 threshold.
    vec = [1.0] + [0.0] * 1023
    embeddings = {
        f"{_CAT}.{_SCHEMA}.customers.id": ColumnEmbedding.from_vector(
            f"{_CAT}.{_SCHEMA}.customers.id", vec,
        ),
        f"{_CAT}.{_SCHEMA}.orders.customer_id": ColumnEmbedding.from_vector(
            f"{_CAT}.{_SCHEMA}.orders.customer_id", vec,
        ),
    }
    prior = frozenset({
        DeclaredPair(
            source_id=f"{_CAT}.{_SCHEMA}.orders.customer_id",
            target_id=f"{_CAT}.{_SCHEMA}.customers.id",
        ),
    })
    edges, _ = infer_semantic_pairs(
        columns=columns,
        embeddings=embeddings,
        pk_index=_pk_index(),
        prior_pairs=prior,
    )
    emitted = {DeclaredPair(source_id=e.source_id, target_id=e.target_id) for e in edges}
    assert prior.isdisjoint(emitted)
    # Semantic should not re-emit any pair from the union.
    for e in edges:
        assert e.source is EdgeSource.SEMANTIC
