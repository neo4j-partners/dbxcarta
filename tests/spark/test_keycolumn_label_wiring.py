"""Wiring guard: the key-like Column subset reaches Neo4j as the
contract-1.4 `is_key_like` property, and the `:KeyColumn` label is a
single scoped server-side projection of that property.

Bug 1 was a connector multi-label write (`MERGE (n:Column:KeyColumn
{id})`) colliding with the Column.id uniqueness constraint. The fix drops
the split write entirely: the Column frame carries `is_key_like` (true for
ids in the key-like target set, false otherwise) through the one node
write, and `apply_key_column_labels` does a scoped `SET`/`REMOVE` off that
written property after the chunk loop. This test pins both halves and that
`bootstrap_constraints` still creates the dedicated `keycolumn_embedding`
vector index iff column embeddings are enabled.
"""

from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace

import pytest

import dbxcarta.spark.run as pipeline
from dbxcarta.spark.contract import NodeLabel
from dbxcarta.spark.ingest.load import neo4j_io
from dbxcarta.spark.ingest.load.neo4j_io import KEY_COLUMN_LABEL
from dbxcarta.spark.settings import SparkIngestSettings

_BASE = {
    "dbxcarta_catalog": "main",
    "dbxcarta_summary_volume": "/Volumes/cat/schema/vol/dbxcarta",
    "dbxcarta_summary_table": "cat.schema.table",
}


def test_augment_is_key_like_defaults_false_when_no_keylike(
    local_spark,
) -> None:
    df = local_spark.createDataFrame(
        [("a", False), ("b", False)], ["id", "is_key_like"],
    )
    out = {
        r["id"]: r["is_key_like"]
        for r in pipeline._augment_is_key_like(df, None).collect()
    }
    assert out == {"a": False, "b": False}


def test_augment_is_key_like_marks_only_targets(local_spark) -> None:
    df = local_spark.createDataFrame(
        [("a", False), ("b", False), ("c", False)], ["id", "is_key_like"],
    )
    keylike = local_spark.createDataFrame(
        [("a",), ("c",), ("c",)], ["col_id"],
    )
    out = {
        r["id"]: r["is_key_like"]
        for r in pipeline._augment_is_key_like(df, keylike).collect()
    }
    # Only the key-like ids flip true; duplicate keylike rows do not fan out
    # the left join (distinct on col_id), and the builder default is
    # overwritten, not appended.
    assert out == {"a": True, "b": False, "c": True}


def _run_write(monkeypatch, *, embed_enabled, keylike_ids):
    settings = SparkIngestSettings(**_BASE)

    @contextmanager
    def fake_embedded_batch(df, label, *a, **k):  # noqa: ANN001
        yield f"staged({df})"

    node_writes: list[tuple[object, NodeLabel]] = []
    augment_calls: list[object] = []

    def fake_augment(df, kl):  # noqa: ANN001
        augment_calls.append(kl)
        return f"augmented({df})"

    monkeypatch.setattr(pipeline, "embedded_batch", fake_embedded_batch)
    monkeypatch.setattr(pipeline, "_augment_is_key_like", fake_augment)
    monkeypatch.setattr(pipeline, "_project", lambda df, label: df)
    monkeypatch.setattr(
        pipeline, "write_node",
        lambda df, neo4j, label: node_writes.append((df, label)),
    )

    pipeline._write_label_nodes(
        "column_node_df", NodeLabel.COLUMN, neo4j=object(),
        settings=settings, ledger_path="/l", transient_root="/t",
        batch_tag="b0", summary=object(), embed_enabled=embed_enabled,
        keylike_ids=keylike_ids,
    )
    return node_writes, augment_calls


def test_column_write_augments_is_key_like_then_single_write(
    monkeypatch,
) -> None:
    keylike = object()
    node_writes, augment_calls = _run_write(
        monkeypatch, embed_enabled=True, keylike_ids=keylike,
    )
    # Exactly one Column node write (no second :KeyColumn write that would
    # collide with the id uniqueness constraint), and is_key_like was
    # derived from the threaded keylike frame before the write.
    assert augment_calls == [keylike]
    assert len(node_writes) == 1
    assert node_writes[0][1] is NodeLabel.COLUMN
    assert "augmented(" in node_writes[0][0]


def test_column_write_augments_with_none_when_keylike_absent(
    monkeypatch,
) -> None:
    node_writes, augment_calls = _run_write(
        monkeypatch, embed_enabled=False, keylike_ids=None,
    )
    # Non-embed arm still augments (so is_key_like is always written) and
    # still writes exactly once.
    assert augment_calls == [None]
    assert len(node_writes) == 1


def test_no_write_key_columns_symbol() -> None:
    # The split multi-label write is gone for good; its reintroduction
    # would resurrect Bug 1.
    assert not hasattr(pipeline, "write_key_columns")
    assert not hasattr(neo4j_io, "write_key_columns")


def test_apply_key_column_labels_scoped_set_and_remove() -> None:
    cyphers: list[tuple[str, dict]] = []

    class _Session:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def run(self, cypher, **kw):  # noqa: ANN001
            cyphers.append((cypher, kw))

    driver = SimpleNamespace(session=lambda: _Session())
    neo4j_io.apply_key_column_labels(
        driver, ["bronze", "silver"], ["sales"],
    )

    assert len(cyphers) == 2
    set_cy, set_kw = cyphers[0]
    rm_cy, rm_kw = cyphers[1]

    # Statement 1 adds the label to currently-key-like columns; statement 2
    # strips it from those no longer key-like. Both are scoped to the run's
    # catalogs/schemas (bounded scalars, never a per-column id list) and
    # keyed on the written is_key_like property, never a connector write.
    assert f"SET c:{KEY_COLUMN_LABEL}" in set_cy
    assert "c.is_key_like = true" in set_cy
    assert f"REMOVE c:{KEY_COLUMN_LABEL}" in rm_cy
    assert "c.is_key_like IS NULL OR c.is_key_like = false" in rm_cy
    for cy in (set_cy, rm_cy):
        assert "c.catalog IN $catalogs" in cy
        assert "c.schema IN $schemas" in cy
    assert set_kw == {"catalogs": ["bronze", "silver"], "schemas": ["sales"]}
    assert rm_kw == {"catalogs": ["bronze", "silver"], "schemas": ["sales"]}


@pytest.mark.parametrize("columns_enabled", [True, False])
def test_keycolumn_embedding_index_gated_on_column_embeddings(
    monkeypatch, columns_enabled: bool,
) -> None:
    settings = SparkIngestSettings(
        dbxcarta_include_embeddings_columns=columns_enabled, **_BASE,
    )

    cyphers: list[str] = []

    class _Session:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def run(self, cypher, **kw):  # noqa: ANN001
            cyphers.append(cypher)
            return SimpleNamespace(single=lambda: {"cnt": 0})

    driver = SimpleNamespace(session=lambda: _Session())
    neo4j_io.bootstrap_constraints(driver, settings)

    created = any(
        "VECTOR INDEX keycolumn_embedding" in c for c in cyphers
    )
    assert created is columns_enabled
