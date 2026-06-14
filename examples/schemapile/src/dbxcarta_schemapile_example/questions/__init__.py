"""Databricks-connected question stage: LLM generation + SQL validation.

This stage needs a workspace. It prompts a foundation-model endpoint for
question/SQL pairs and validates each SQL against the materialized tables on a
SQL warehouse, writing the surviving pairs to ``questions.json``.
"""
