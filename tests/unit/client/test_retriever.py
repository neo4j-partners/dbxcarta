"""Unit tests for the Retriever ABC, ContextBundle, and MockRetriever."""

from __future__ import annotations

from dbxcarta.client.retriever import ColumnEntry, ContextBundle, Retriever


class MockRetriever(Retriever):
    """Deterministic retriever for unit tests — returns a canned ContextBundle."""

    def __init__(self, bundle: ContextBundle) -> None:
        self._bundle = bundle

    def retrieve(self, question: str, embedding: list[float]) -> ContextBundle:
        return self._bundle


def _make_bundle() -> ContextBundle:
    return ContextBundle(
        columns=[
            ColumnEntry(
                table_fqn="cat.schema.accounts",
                column_name="account_id",
                data_type="BIGINT",
                comment="Primary key",
                column_id="cat.schema.accounts.account_id",
            ),
            ColumnEntry(
                table_fqn="cat.schema.accounts",
                column_name="status",
                data_type="STRING",
                comment="",
                column_id="cat.schema.accounts.status",
            ),
            ColumnEntry(
                table_fqn="cat.schema.transactions",
                column_name="amount",
                data_type="DOUBLE",
                comment="Transaction amount in USD",
                column_id="cat.schema.transactions.amount",
            ),
        ],
        values={"cat.schema.accounts.status": ["active", "closed", "pending"]},
        seed_ids=["cat.schema.accounts.status", "cat.schema.transactions"],
    )


def test_mock_retriever_implements_abc() -> None:
    bundle = _make_bundle()
    retriever: Retriever = MockRetriever(bundle)
    result = retriever.retrieve("how many accounts are active?", [0.1, 0.2, 0.3])
    assert result is bundle


def test_context_bundle_to_text_groups_by_table() -> None:
    bundle = _make_bundle()
    text = bundle.to_text()
    lines = text.splitlines()

    table_lines = [l for l in lines if l.startswith("Table:")]
    assert len(table_lines) == 2
    assert "cat.schema.accounts" in table_lines[0]
    assert "cat.schema.transactions" in table_lines[1]


def test_context_bundle_to_text_includes_columns() -> None:
    bundle = _make_bundle()
    text = bundle.to_text()
    assert "account_id" in text
    assert "BIGINT" in text
    assert "Primary key" in text
    assert "status" in text


def test_context_bundle_to_text_includes_values() -> None:
    bundle = _make_bundle()
    text = bundle.to_text()
    assert "Sample values:" in text
    assert "active" in text


def test_context_bundle_values_attach_to_owning_column() -> None:
    """Sample values render on the line of the column they belong to."""
    bundle = _make_bundle()
    lines = bundle.to_text().splitlines()
    status_line = next(line for line in lines if "status" in line)
    account_id_line = next(line for line in lines if "account_id" in line)
    assert "Sample values: active, closed, pending" in status_line
    assert "Sample values" not in account_id_line


def test_context_bundle_to_text_empty() -> None:
    assert ContextBundle().to_text() == ""


def test_context_bundle_no_values_omits_sample_line() -> None:
    bundle = ContextBundle(
        columns=[ColumnEntry("c.s.t", "col", "STRING", column_id="c.s.t.col")],
        values={},
    )
    text = bundle.to_text()
    assert "Sample values" not in text


def test_column_entry_defaults() -> None:
    col = ColumnEntry(table_fqn="c.s.t", column_name="col", data_type="INT")
    assert col.comment == ""
    assert col.column_id == ""
