"""Configuration for the Databricks-connected question stage.

Question generation prompts a foundation-model endpoint and validates the
resulting SQL against the materialized tables in the data catalog, so this
config reads the data catalog, the LLM parameters, and the candidate-blueprint
path it generates questions from.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import TYPE_CHECKING

from dbxcarta_schemapile_example.config import candidate_cache_path, require_env

if TYPE_CHECKING:
    from collections.abc import Mapping
    from pathlib import Path

# Catalogs owned by the project. The example must generate and validate SQL
# against a dedicated catalog, never one of these, so a stray DBXCARTA_CATALOG
# can't aim the generated SQL at shared project data.
_PROJECT_CATALOGS_BLOCKLIST: frozenset[str] = frozenset(
    {
        "graph-enriched-lakehouse",
        "dbxcarta-catalog",
        "main",
        "hive_metastore",
        "samples",
        "system",
    }
)


@dataclass(frozen=True)
class QuestionConfig:
    """Parsed configuration for generating and validating the question set."""

    catalog: str
    candidate_cache: Path
    question_model: str
    questions_per_schema: int
    question_temperature: float
    seed: int


def load_question_config(env: Mapping[str, str] | None = None) -> QuestionConfig:
    """Build a QuestionConfig from environment variables. Defaults match .env.sample."""
    e = env if env is not None else os.environ

    catalog = require_env(e, "DBXCARTA_CATALOG")
    if catalog.casefold() in _PROJECT_CATALOGS_BLOCKLIST:
        raise ValueError(
            f"DBXCARTA_CATALOG={catalog!r} collides with a known project catalog;"
            " choose a dedicated catalog for the schemapile example"
        )

    return QuestionConfig(
        catalog=catalog,
        candidate_cache=candidate_cache_path(e),
        question_model=e.get(
            "SCHEMAPILE_QUESTION_MODEL",
            "databricks-meta-llama-3-3-70b-instruct",
        ),
        questions_per_schema=int(e.get("SCHEMAPILE_QUESTIONS_PER_SCHEMA", "6")),
        question_temperature=float(e.get("SCHEMAPILE_QUESTION_TEMPERATURE", "0.2")),
        seed=int(e.get("SCHEMAPILE_SEED", "42")),
    )
