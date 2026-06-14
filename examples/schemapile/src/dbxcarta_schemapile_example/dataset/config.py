"""Configuration for the host-only dataset stage.

The slice runner and candidate selector run with no Databricks credentials, so
this config reads only the ``SCHEMAPILE_*`` slice and candidate-selection
parameters. Read once at the entrypoint boundary; downstream functions accept
the parsed object rather than re-reading ``os.environ``.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from dbxcarta_schemapile_example.config import (
    candidate_cache_path,
    require_env,
    slice_cache_path,
    truthy,
)

if TYPE_CHECKING:
    from collections.abc import Mapping


@dataclass(frozen=True)
class SliceConfig:
    """Parameters passed verbatim to the upstream ``slice.py``."""

    repo: Path
    input_filename: str
    target_tables: int
    strategy: str
    seed: int
    min_tables: int
    max_tables: int
    min_fk_edges: int
    require_self_contained: bool
    require_data: bool
    slice_cache: Path

    @property
    def input_path(self) -> Path:
        return self.repo / self.input_filename

    @property
    def upstream_slice_script(self) -> Path:
        return self.repo / "slice.py"


@dataclass(frozen=True)
class CandidateConfig:
    """Filter, rank, and cap parameters for selecting candidate schemas."""

    slice_cache: Path
    candidate_cache: Path
    candidate_min_tables: int
    candidate_max_tables: int
    candidate_min_fk_edges: int
    candidate_require_data: bool
    candidate_limit: int


def load_slice_config(env: Mapping[str, str] | None = None) -> SliceConfig:
    """Build a SliceConfig from environment variables. Defaults match .env.sample."""
    e = env if env is not None else os.environ
    repo = Path(require_env(e, "SCHEMAPILE_REPO")).expanduser().resolve()
    return SliceConfig(
        repo=repo,
        input_filename=e.get("SCHEMAPILE_INPUT", "schemapile-perm.json"),
        target_tables=int(e.get("SCHEMAPILE_TARGET_TABLES", "1000")),
        strategy=e.get("SCHEMAPILE_STRATEGY", "random"),
        seed=int(e.get("SCHEMAPILE_SEED", "42")),
        min_tables=int(e.get("SCHEMAPILE_MIN_TABLES", "2")),
        max_tables=int(e.get("SCHEMAPILE_MAX_TABLES", "100")),
        min_fk_edges=int(e.get("SCHEMAPILE_MIN_FK_EDGES", "1")),
        require_self_contained=truthy(e.get("SCHEMAPILE_REQUIRE_SELF_CONTAINED", "true")),
        require_data=truthy(e.get("SCHEMAPILE_REQUIRE_DATA", "false")),
        slice_cache=slice_cache_path(e),
    )


def load_candidate_config(env: Mapping[str, str] | None = None) -> CandidateConfig:
    """Build a CandidateConfig from environment variables. Defaults match .env.sample."""
    e = env if env is not None else os.environ
    return CandidateConfig(
        slice_cache=slice_cache_path(e),
        candidate_cache=candidate_cache_path(e),
        candidate_min_tables=int(e.get("SCHEMAPILE_CANDIDATE_MIN_TABLES", "3")),
        candidate_max_tables=int(e.get("SCHEMAPILE_CANDIDATE_MAX_TABLES", "20")),
        candidate_min_fk_edges=int(e.get("SCHEMAPILE_CANDIDATE_MIN_FK_EDGES", "2")),
        candidate_require_data=truthy(e.get("SCHEMAPILE_CANDIDATE_REQUIRE_DATA", "false")),
        candidate_limit=int(e.get("SCHEMAPILE_CANDIDATE_LIMIT", "20")),
    )
