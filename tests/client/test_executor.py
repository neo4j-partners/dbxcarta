from __future__ import annotations

from types import SimpleNamespace

from databricks.sdk.service.sql import StatementState

from dbxcarta.client.executor import fetch_rows


def _response(
    *,
    statement_id: str | None = "stmt-1",
    first_rows: list[list[object]] | None = None,
    next_chunk_index: int | None = None,
    column_names: list[str | None] | None = None,
):
    names = column_names if column_names is not None else ["id", "name"]
    return SimpleNamespace(
        statement_id=statement_id,
        status=SimpleNamespace(state=StatementState.SUCCEEDED),
        manifest=SimpleNamespace(
            schema=SimpleNamespace(
                columns=[SimpleNamespace(name=name) for name in names]
            )
        ),
        result=SimpleNamespace(
            data_array=first_rows or [],
            next_chunk_index=next_chunk_index,
        ),
    )


class _StatementExecution:
    def __init__(self, response, chunks):
        self._response = response
        self._chunks = chunks
        self.chunk_calls: list[tuple[str, int]] = []

    def execute_statement(self, **_kwargs):
        return self._response

    def get_statement_result_chunk_n(self, statement_id: str, chunk_index: int):
        self.chunk_calls.append((statement_id, chunk_index))
        return self._chunks[chunk_index]


class _Workspace:
    def __init__(self, statement_execution):
        self.statement_execution = statement_execution


def test_fetch_rows_reads_all_result_chunks() -> None:
    statement_execution = _StatementExecution(
        _response(first_rows=[[1, "a"]], next_chunk_index=1),
        {
            1: SimpleNamespace(data_array=[[2, "b"]], next_chunk_index=2),
            2: SimpleNamespace(data_array=[[3, "c"]], next_chunk_index=None),
        },
    )
    ws = _Workspace(statement_execution)

    columns, rows, error = fetch_rows(ws, "warehouse", "SELECT 1")

    assert error is None
    assert columns == ["id", "name"]
    assert rows == [[1, "a"], [2, "b"], [3, "c"]]
    assert statement_execution.chunk_calls == [("stmt-1", 1), ("stmt-1", 2)]


def test_fetch_rows_requires_statement_id_for_paginated_result() -> None:
    statement_execution = _StatementExecution(
        _response(statement_id=None, first_rows=[[1, "a"]], next_chunk_index=1),
        {},
    )
    ws = _Workspace(statement_execution)

    columns, rows, error = fetch_rows(ws, "warehouse", "SELECT 1")

    assert columns is None
    assert rows is None
    assert error == "statement id missing for paginated result"
    assert statement_execution.chunk_calls == []


def test_fetch_rows_uses_fallback_name_for_unnamed_columns() -> None:
    statement_execution = _StatementExecution(
        _response(first_rows=[[1, "a"]], column_names=["id", None]),
        {},
    )
    ws = _Workspace(statement_execution)

    columns, rows, error = fetch_rows(ws, "warehouse", "SELECT 1")

    assert error is None
    assert columns == ["id", "col_2"]
    assert rows == [[1, "a"]]
