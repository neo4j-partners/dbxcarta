"""Shared configuration primitives for the SchemaPile example stages.

The example builds its evaluation fixture in two stages, each reading a disjoint
slice of the example's `.env`:

- ``dataset`` (host-only): the slice runner and candidate selector. They run on
  a laptop with no Databricks credentials and own ``SliceConfig`` /
  ``CandidateConfig`` in ``dataset/config.py``.
- ``questions`` (Databricks-connected): generation + SQL validation. It needs a
  workspace and owns ``QuestionConfig`` in ``questions/config.py``.

The helpers here are the `.env` parsing primitives those per-stage loaders build
on, plus the cache-path defaults that resolve against the committed blueprint
directory regardless of the working directory.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Mapping

# The committed candidate blueprint lives under the example directory, not in
# .cache, so cache-path defaults resolve against the package location rather
# than the current working directory.
_EXAMPLE_DIR = Path(__file__).resolve().parents[2]
_BLUEPRINT_DIR = _EXAMPLE_DIR / "blueprint"

# Default `.env` an entrypoint loads before reading variables.
DEFAULT_DOTENV = _EXAMPLE_DIR / ".env"


def require_env(env: Mapping[str, str], key: str) -> str:
    """Return a non-empty env value, or raise naming the variable to fix."""
    value = env.get(key, "").strip()
    if not value:
        raise ValueError(f"{key} is not set; check examples/schemapile/.env")
    return value


def truthy(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "on"}


def slice_cache_path(env: Mapping[str, str]) -> Path:
    """Path to the produced slice JSON (shared by the slice and select steps)."""
    return Path(env.get("SCHEMAPILE_SLICE_CACHE", ".cache/slice_random_1000.json"))


def candidate_cache_path(env: Mapping[str, str]) -> Path:
    """Path to the committed candidate blueprint (read by select and questions)."""
    return Path(
        env.get("SCHEMAPILE_CANDIDATE_CACHE") or _BLUEPRINT_DIR / "candidates_random_1000.json"
    )
