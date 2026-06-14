"""Unit tests for local SQL generation and the on-disk response cache."""

from __future__ import annotations

from dbxcarta.client import generation
from dbxcarta.client.local_generation import LocalGenerationError

_PROMPTS = [
    {"question_id": "q1", "prompt": "count accounts"},
    {"question_id": "q2", "prompt": "list customers"},
]


def _fake_endpoint(replies, calls):
    def _generate(_ws, _endpoint, prompt):
        calls.append(prompt)
        return replies[prompt]

    return _generate


def test_generate_writes_cache_and_returns_results(monkeypatch, tmp_path) -> None:
    calls: list[str] = []
    replies = {"count accounts": "SELECT 1", "list customers": "SELECT 2"}
    monkeypatch.setattr(generation, "generate_sql_local", _fake_endpoint(replies, calls))

    results = generation.generate_sql_batch(
        object(), "chat", _PROMPTS, str(tmp_path), "no_context"
    )

    assert results == {"q1": ("SELECT 1", None), "q2": ("SELECT 2", None)}
    assert calls == ["count accounts", "list customers"]
    assert (tmp_path / "no_context.json").exists()


def test_identical_rerun_hits_cache_without_calling_endpoint(monkeypatch, tmp_path) -> None:
    calls: list[str] = []
    replies = {"count accounts": "SELECT 1", "list customers": "SELECT 2"}
    monkeypatch.setattr(generation, "generate_sql_local", _fake_endpoint(replies, calls))

    first = generation.generate_sql_batch(object(), "chat", _PROMPTS, str(tmp_path), "no_context")
    calls.clear()
    second = generation.generate_sql_batch(object(), "chat", _PROMPTS, str(tmp_path), "no_context")

    assert second == first
    assert calls == []


def test_refresh_forces_reinference(monkeypatch, tmp_path) -> None:
    calls: list[str] = []
    replies = {"count accounts": "SELECT 1", "list customers": "SELECT 2"}
    monkeypatch.setattr(generation, "generate_sql_local", _fake_endpoint(replies, calls))

    generation.generate_sql_batch(object(), "chat", _PROMPTS, str(tmp_path), "no_context")
    calls.clear()
    generation.generate_sql_batch(
        object(), "chat", _PROMPTS, str(tmp_path), "no_context", refresh=True
    )

    assert calls == ["count accounts", "list customers"]


def test_changed_prompt_invalidates_cache(monkeypatch, tmp_path) -> None:
    calls: list[str] = []
    replies = {"count accounts": "SELECT 1", "list customers": "SELECT 2", "changed": "SELECT 3"}
    monkeypatch.setattr(generation, "generate_sql_local", _fake_endpoint(replies, calls))

    generation.generate_sql_batch(object(), "chat", _PROMPTS, str(tmp_path), "no_context")
    calls.clear()
    changed = [_PROMPTS[0], {"question_id": "q2", "prompt": "changed"}]
    generation.generate_sql_batch(object(), "chat", changed, str(tmp_path), "no_context")

    assert calls == ["count accounts", "changed"]


def test_failed_call_is_recorded_as_error(monkeypatch, tmp_path) -> None:
    def _boom(_ws, _endpoint, prompt):
        raise LocalGenerationError(f"endpoint down for {prompt!r}")

    monkeypatch.setattr(generation, "generate_sql_local", _boom)

    results = generation.generate_sql_batch(
        object(), "chat", _PROMPTS[:1], str(tmp_path), "no_context"
    )

    sql, error = results["q1"]
    assert sql is None
    assert "endpoint down" in error
