"""Load DenseSchema example configuration from environment."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path


_PROJECT_CATALOGS_BLOCKLIST: frozenset[str] = frozenset({
    "graph-enriched-lakehouse",
    "dbxcarta-catalog",
    "main",
    "hive_metastore",
    "samples",
    "system",
})


@dataclass(frozen=True)
class DenseSchemaConfig:
    catalog: str
    meta_schema: str
    volume: str
    table_count: int
    uc_schema: str
    seed: int
    candidate_cache: Path
    # Ops volume path, sourced from DATABRICKS_VOLUME_PATH. Deliberately not
    # derived from `catalog`: the ops plane lives in its own catalog
    # (dbxcarta-catalog.dense_ops), separate from the data catalog this config
    # materializes tables into, so data discovery never sweeps it in.
    volume_path: str
    questions_path: str
    question_model: str
    questions_target: int
    questions_per_batch: int
    question_temperature: float


def load_config(env: Mapping[str, str] | None = None) -> DenseSchemaConfig:
    e = env if env is not None else os.environ
    catalog = _required(e, "DBXCARTA_CATALOG")
    if catalog.casefold() in _PROJECT_CATALOGS_BLOCKLIST:
        raise ValueError(
            f"DBXCARTA_CATALOG={catalog!r} collides with a known project catalog"
        )
    table_count = int(e.get("DENSE_TABLE_COUNT", "500"))
    uc_schema = e.get("DENSE_SCHEMA_NAME", f"dense_{table_count}")
    meta_schema = e.get("SCHEMAPILE_META_SCHEMA", "_meta")
    volume = e.get("SCHEMAPILE_VOLUME", "schemapile_volume")
    # The ops plane is separate from the data catalog. Prefer the explicit
    # DATABRICKS_VOLUME_PATH (the overlay/.env points it at the ops catalog);
    # fall back to the legacy in-catalog derivation only when it is unset.
    volume_path = e.get(
        "DATABRICKS_VOLUME_PATH",
        f"/Volumes/{catalog}/{meta_schema}/{volume}",
    )
    return DenseSchemaConfig(
        catalog=catalog,
        meta_schema=meta_schema,
        volume=volume,
        table_count=table_count,
        uc_schema=uc_schema,
        seed=int(e.get("DENSE_SEED", "42")),
        candidate_cache=Path(
            e.get("DENSE_CANDIDATE_CACHE", f".cache/candidates_{table_count}.json")
        ),
        volume_path=volume_path,
        questions_path=e.get(
            "DBXCARTA_CLIENT_QUESTIONS",
            f"{volume_path}/dbxcarta/dense_questions.json",
        ),
        question_model=e.get(
            "DENSE_QUESTION_MODEL",
            "databricks-meta-llama-3-3-70b-instruct",
        ),
        questions_target=int(e.get("DENSE_QUESTIONS_TARGET", "60")),
        questions_per_batch=int(e.get("DENSE_QUESTIONS_PER_BATCH", "3")),
        question_temperature=float(e.get("DENSE_QUESTION_TEMPERATURE", "0.2")),
    )


def _required(env: Mapping[str, str], key: str) -> str:
    val = env.get(key, "").strip()
    if not val:
        raise ValueError(
            f"{key} is not set; check examples/dense-schema/.env"
        )
    return val
