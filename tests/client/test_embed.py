"""Tests for batch-chunked embedding and graph_rag fail-fast behaviour."""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from databricks.sdk.errors import DatabricksError

from dbxcarta.client.embed import EMBED_MAX_BATCH, embed_questions
from dbxcarta.client.eval import arms
from dbxcarta.client.questions import Question


class _RecordingApiClient:
    """Returns one float per input; index is per-request, shuffled on return."""

    def __init__(self) -> None:
        self.batch_sizes: list[int] = []

    def do(self, method: str, path: str, body: dict) -> dict:
        inputs = body["input"]
        self.batch_sizes.append(len(inputs))
        items = [
            {"index": i, "embedding": [float(hash(text) % 1000)]}
            for i, text in enumerate(inputs)
        ]
        # Endpoint does not guarantee response order; force callers to sort.
        items.reverse()
        return {"data": items}


class _Workspace:
    def __init__(self, api_client) -> None:
        self.api_client = api_client


def test_empty_input_returns_empty_no_call() -> None:
    api = _RecordingApiClient()
    embeddings, error = embed_questions(_Workspace(api), "ep", [])
    assert embeddings == []
    assert error is None
    assert api.batch_sizes == []


def test_chunks_respect_max_batch() -> None:
    api = _RecordingApiClient()
    texts = [f"q{i}" for i in range(EMBED_MAX_BATCH * 2 + 5)]

    embeddings, error = embed_questions(_Workspace(api), "ep", texts)

    assert error is None
    assert embeddings is not None
    assert len(embeddings) == len(texts)
    assert all(size <= EMBED_MAX_BATCH for size in api.batch_sizes)
    assert sum(api.batch_sizes) == len(texts)
    assert api.batch_sizes == [EMBED_MAX_BATCH, EMBED_MAX_BATCH, 5]


def test_order_preserved_across_chunk_boundaries() -> None:
    api = _RecordingApiClient()
    texts = [f"unique-{i}" for i in range(EMBED_MAX_BATCH + 3)]

    embeddings, error = embed_questions(_Workspace(api), "ep", texts)

    assert error is None
    assert embeddings is not None
    expected = [[float(hash(t) % 1000)] for t in texts]
    assert embeddings == expected


def test_batch_over_endpoint_limit_returns_error() -> None:
    class _RejectingApiClient:
        def do(self, *_args, **_kwargs):
            raise DatabricksError(
                "Input embeddings size is too large, exceeding 150 limit "
                "for databricks-gte-large-en"
            )

    embeddings, error = embed_questions(
        _Workspace(_RejectingApiClient()), "ep", ["a", "b"]
    )

    assert embeddings is None
    assert error is not None
    assert "exceeding 150 limit" in error


def _settings() -> SimpleNamespace:
    return SimpleNamespace(
        dbxcarta_catalog="cat",
        schemas_list=["s"],
        dbxcarta_embed_endpoint="ep",
    )


def _questions(n: int) -> list[Question]:
    return [Question(question_id=f"q{i}", question=f"text {i}") for i in range(n)]


def test_graph_rag_raises_when_embedding_fails(monkeypatch) -> None:
    monkeypatch.setattr(
        arms, "_embed_questions", lambda *_a, **_k: (None, "boom")
    )
    with pytest.raises(RuntimeError, match="graph_rag embedding failed"):
        arms._run_graph_rag_arm(
            None, None, _settings(), _questions(3), None, None, "staging"
        )


def test_graph_rag_raises_on_embedding_count_mismatch(monkeypatch) -> None:
    monkeypatch.setattr(
        arms, "_embed_questions", lambda *_a, **_k: ([[0.1]], None)
    )
    with pytest.raises(RuntimeError, match="misaligned embeddings"):
        arms._run_graph_rag_arm(
            None, None, _settings(), _questions(3), None, None, "staging"
        )
