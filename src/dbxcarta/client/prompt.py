"""Prompt templates for each retrieval arm."""

from __future__ import annotations

_SQL_INSTRUCTION = (
    "Return ONLY the SQL query — no explanation, no markdown code fences, no preamble.\n"
    "Output format rules:\n"
    "- SELECT only the columns and aggregations the question explicitly asks for.\n"
    "- Do not rename plain column references with AS aliases "
    "(e.g. write `name`, not `name AS project_name`).\n"
    "- Do not add ID columns to SELECT or GROUP BY unless the question asks for IDs.\n"
    "- GROUP BY the name or text column only — do not include the primary key "
    "alongside the name (e.g. GROUP BY name, not GROUP BY id, name).\n"
    "- Do not add ORDER BY unless the question asks for ordering."
)


def no_context_prompt(question: str, catalog: str, schemas: list[str]) -> str:
    schema_hint = (
        f"schema '{schemas[0]}'"
        if len(schemas) == 1
        else "schemas " + ", ".join(f"'{s}'" for s in schemas)
    )
    return (
        f"You are a SQL expert. Write a single SQL SELECT query to answer the following question.\n"
        f"The data lives in Databricks Unity Catalog: catalog '{catalog}', {schema_hint}.\n"
        f"{_SQL_INSTRUCTION}\n\n"
        f"Question: {question}"
    )


def schema_dump_prompt(
    question: str, catalog: str, schemas: list[str], schema_text: str
) -> str:
    schema_hint = (
        f"schema '{schemas[0]}'"
        if len(schemas) == 1
        else "schemas " + ", ".join(f"'{s}'" for s in schemas)
    )
    return (
        f"You are a SQL expert. Write a single SQL SELECT query to answer the following question.\n"
        f"The data lives in Databricks Unity Catalog: catalog '{catalog}', {schema_hint}.\n\n"
        f"Available tables and columns:\n{schema_text}\n\n"
        f"{_SQL_INSTRUCTION}\n\n"
        f"Question: {question}"
    )


def graph_rag_prompt(
    question: str, catalog: str, schemas: list[str], context_text: str
) -> str:
    schema_hint = (
        f"schema '{schemas[0]}'"
        if len(schemas) == 1
        else "schemas " + ", ".join(f"'{s}'" for s in schemas)
    )
    return (
        f"You are a SQL expert. Write a single SQL SELECT query to answer the following question.\n"
        f"The data lives in Databricks Unity Catalog: catalog '{catalog}', {schema_hint}.\n"
        f"Use only tables from the target schema shown in the context. Do not join across unrelated schemas.\n\n"
        f"Relevant schema context retrieved from the knowledge graph:\n{context_text}\n\n"
        f"{_SQL_INSTRUCTION}\n\n"
        f"Question: {question}"
    )
