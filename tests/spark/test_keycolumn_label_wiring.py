"""Phase 2 wiring guard: the key-like Column subset is re-written with the
extra `:KeyColumn` label, and only when it should be.

`_write_label_nodes` is the single node-write seam. This test proves the
`:KeyColumn` second-label pass fires exactly when (a) embedding is enabled
*and* (b) a `keylike_ids` frame was threaded in (Column label only),
reusing the one `embedded_batch` ai_query pass — and that it stays silent
otherwise (no keylike frame, or embedding off). It also proves
`bootstrap_constraints` creates the dedicated `keycolumn_embedding` vector
index iff column embeddings are enabled.
"""

from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace

import pytest

import dbxcarta.spark.run as pipeline
from dbxcarta.spark.contract import NodeLabel
from dbxcarta.spark.ingest.load import neo4j_io
from dbxcarta.spark.settings import SparkIngestSettings

_BASE = {
    "dbxcarta_catalog": "main",
    "dbxcarta_summary_volume": "/Volumes/cat/schema/vol/dbxcarta",
    "dbxcarta_summary_table": "cat.schema.table",
}


class _FakeStaged:
    """Stands in for the embedded/frozen batch frame.

    `join(..., "left_semi")` is the key-like subset selection; it returns a
    distinct sentinel so the test can prove the subset (not the full frame)
    is what reaches `write_key_columns`.
    """

    def __getitem__(self, key):  # noqa: ANN001
        return f"staged[{key}]"

    def join(self, other, cond, how):  # noqa: ANN001
        assert how == "left_semi"
        return "key_subset"


class _FakeKeylike:
    def __getitem__(self, key):  # noqa: ANN001
        return f"keylike[{key}]"


def _run_write(monkeypatch, *, embed_enabled, keylike_ids):
    settings = SparkIngestSettings(**_BASE)

    @contextmanager
    def fake_embedded_batch(df, label, *a, **k):  # noqa: ANN001
        yield _FakeStaged()

    node_writes: list[tuple[object, NodeLabel]] = []
    key_writes: list[object] = []

    monkeypatch.setattr(pipeline, "embedded_batch", fake_embedded_batch)
    monkeypatch.setattr(pipeline, "_project", lambda df, label: df)
    monkeypatch.setattr(
        pipeline, "write_node",
        lambda df, neo4j, label: node_writes.append((df, label)),
    )
    monkeypatch.setattr(
        pipeline, "write_key_columns",
        lambda df, neo4j: key_writes.append(df),
    )
    monkeypatch.setattr(
        "pyspark.sql.functions.broadcast", lambda df: df,
    )

    pipeline._write_label_nodes(
        "column_node_df", NodeLabel.COLUMN, neo4j=object(),
        settings=settings, ledger_path="/l", transient_root="/t",
        batch_tag="b0", summary=object(), embed_enabled=embed_enabled,
        keylike_ids=keylike_ids,
    )
    return node_writes, key_writes


def test_keycolumn_subset_rewritten_when_embedded_and_keylike_present(
    monkeypatch,
) -> None:
    node_writes, key_writes = _run_write(
        monkeypatch, embed_enabled=True, keylike_ids=_FakeKeylike(),
    )
    # Primary :Column write of the full embedded frame still happens once.
    assert len(node_writes) == 1
    assert node_writes[0][1] is NodeLabel.COLUMN
    # The :KeyColumn re-write receives the left-semi subset, not the full
    # frame — proving it is the key-like targets only.
    assert key_writes == ["key_subset"]


def test_no_keycolumn_write_when_keylike_ids_absent(monkeypatch) -> None:
    node_writes, key_writes = _run_write(
        monkeypatch, embed_enabled=True, keylike_ids=None,
    )
    assert len(node_writes) == 1
    assert key_writes == []


def test_no_keycolumn_write_when_embedding_disabled(monkeypatch) -> None:
    # keylike_ids present but embedding off: no embedded pass exists to
    # reuse, so the :KeyColumn write must not fire.
    node_writes, key_writes = _run_write(
        monkeypatch, embed_enabled=False, keylike_ids=_FakeKeylike(),
    )
    assert len(node_writes) == 1
    assert key_writes == []


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
