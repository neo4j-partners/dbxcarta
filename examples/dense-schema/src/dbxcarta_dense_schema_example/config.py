"""Shared configuration primitives for the dense-schema example stages.

The example builds its evaluation fixture in two stages:

- ``dataset`` (host-only): the synthetic blueprint generator. It runs on a
  laptop with no Databricks credentials and reads no ``.env`` — its parameters
  come from CLI flags, so it owns no config object.
- ``questions`` (Databricks-connected): generation + SQL validation. It needs a
  workspace and owns ``QuestionConfig`` in ``questions/config.py``.

The helpers here are the `.env` parsing primitives the question loader builds
on, plus the cache-path default that resolves against the committed blueprint
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
        raise ValueError(f"{key} is not set; check examples/dense-schema/.env")
    return value


def candidate_cache_path(env: Mapping[str, str], table_count: int) -> Path:
    """Path to the committed candidate blueprint (read by question generation)."""
    return Path(
        env.get("DENSE_CANDIDATE_CACHE") or _BLUEPRINT_DIR / f"candidates_{table_count}.json"
    )
