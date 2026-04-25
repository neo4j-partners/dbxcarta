"""Phase 2 deliverable: criteria injection into the SQL-gen prompt.

The graph_rag_prompt renders the ContextBundle via bundle.to_text(), so a
criteria list on the bundle flows directly into the prompt text. The tests
in this module cover two orthogonal halves of the feature:

1. Rendering (the bundle → text → prompt plumbing). Exercised via direct
   ContextBundle construction, since Phase 2 itself never emits a non-null
   `criteria` — declared FKs get `criteria=null`, and the Phase 6
   query-log miner is the first source that populates the field. A
   "declared FK with a known criteria string" fixture therefore doesn't
   exist yet; constructing the bundle directly is how this phase's
   deliverable gets satisfied without fabricating post-hoc source data.

2. Flag gating (DBXCARTA_INJECT_CRITERIA → GraphRetriever branching).
   Exercised via a stub Neo4j session so the flag's effect on which
   Cypher statements run is observable without a live driver.

See worklog/fk-gap-v3-build.md Phase 2.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

from dbxcarta.client.graph_retriever import (
    GraphRetriever,
    _REFERENCES_CRITERIA_CYPHER,
)
from dbxcarta.client.prompt import graph_rag_prompt
from dbxcarta.client.retriever import ColumnEntry, ContextBundle


_PREDICATE = "orders.customer_id = customers.id"


def _bundle(criteria: list[str]) -> ContextBundle:
    return ContextBundle(
        columns=[
            ColumnEntry("cat.s.orders", "customer_id", "BIGINT"),
            ColumnEntry("cat.s.customers", "id", "BIGINT", "Primary key"),
        ],
        criteria=criteria,
    )


def test_bundle_to_text_renders_criteria_section() -> None:
    text = _bundle([_PREDICATE]).to_text()
    assert "Join predicates:" in text
    assert _PREDICATE in text


def test_bundle_to_text_omits_section_when_empty() -> None:
    text = _bundle([]).to_text()
    assert "Join predicates:" not in text
    assert _PREDICATE not in text


def test_graph_rag_prompt_contains_predicate_when_bundle_has_criteria() -> None:
    bundle = _bundle([_PREDICATE])
    prompt = graph_rag_prompt("q", "cat", ["s"], bundle.to_text())
    assert _PREDICATE in prompt


def test_graph_rag_prompt_omits_predicate_when_bundle_empty() -> None:
    bundle = _bundle([])
    prompt = graph_rag_prompt("q", "cat", ["s"], bundle.to_text())
    assert _PREDICATE not in prompt
    assert "Join predicates:" not in prompt


def test_criteria_default_empty_back_compat() -> None:
    """Callers predating Phase 2 constructed ContextBundle without criteria;
    the field must default to an empty list so to_text() stays well-defined."""
    bundle = ContextBundle(columns=[ColumnEntry("c.s.t", "col", "INT")])
    assert bundle.criteria == []
    assert "Join predicates:" not in bundle.to_text()


# --- Flag-gating tests via stub session -------------------------------------

class _StubRow:
    """Accepts any key access and returns a sentinel string."""

    def __getitem__(self, _key: str) -> str:
        return "stub"


class _StubSession:
    """Minimal Neo4j session stand-in that records Cypher statements.

    Every .run() returns a single row that responds to any key, so each
    retrieve() helper produces non-empty output and the downstream
    criteria-fetch path is reached when the flag is on. The important
    thing is which statements were dispatched, recorded in .statements.
    """

    def __init__(self) -> None:
        self.statements: list[str] = []

    def run(self, statement: str, **_: Any) -> list[_StubRow]:
        self.statements.append(statement)
        return [_StubRow()]

    def __enter__(self) -> "_StubSession":
        return self

    def __exit__(self, *_: Any) -> None:
        return None


def _retriever_with_stub(inject_criteria: bool, stub: _StubSession) -> GraphRetriever:
    """Build a GraphRetriever whose driver.session() yields the stub.

    The constructor opens a real Neo4j driver, which we bypass by
    instantiating the class without __init__ and wiring the fields used by
    retrieve() directly.
    """
    settings = MagicMock()
    settings.dbxcarta_client_top_k = 1
    settings.dbxcarta_catalog = "cat"
    settings.schemas_list = ["s"]
    settings.dbxcarta_confidence_threshold = 0.8
    settings.dbxcarta_inject_criteria = inject_criteria

    driver = MagicMock()
    driver.session.return_value = stub

    retriever = GraphRetriever.__new__(GraphRetriever)
    retriever._settings = settings
    retriever._driver = driver
    return retriever


def test_retriever_runs_criteria_cypher_when_flag_on() -> None:
    stub = _StubSession()
    retriever = _retriever_with_stub(inject_criteria=True, stub=stub)
    retriever.retrieve("q", [0.0] * 4)
    assert any(_REFERENCES_CRITERIA_CYPHER in s for s in stub.statements)


def test_retriever_skips_criteria_cypher_when_flag_off() -> None:
    stub = _StubSession()
    retriever = _retriever_with_stub(inject_criteria=False, stub=stub)
    bundle = retriever.retrieve("q", [0.0] * 4)
    assert all(_REFERENCES_CRITERIA_CYPHER not in s for s in stub.statements)
    assert bundle.criteria == []
