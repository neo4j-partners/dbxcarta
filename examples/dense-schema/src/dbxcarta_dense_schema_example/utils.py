"""Shared entrypoint helpers for the dense-schema example."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path


def load_dotenv_file(path: Path) -> None:
    """Load a dotenv file when python-dotenv is installed and the file exists."""
    try:
        from dotenv import load_dotenv
    except ImportError:
        logging.getLogger(__name__).warning(
            "python-dotenv is not installed; %s was not loaded and "
            "configuration will come from the process environment only",
            path,
        )
        return
    if path.is_file():
        load_dotenv(path, override=False)


def load_layered_env(example_dir: Path, dotenv: Path | None = None) -> None:
    """Load a connected tool's three config layers, anchored to example_dir.

    The Databricks-connected stage (question generation) reads config split
    across three files by the project's one-key-one-file rule: the committed
    per-integration overlay (``dbxcarta-overlay.env``: catalog, ops volume,
    secret scope), the repo-root base ``.env`` (shared profile, warehouse,
    endpoints), and the example's own gitignored ``.env`` (``NEO4J_*`` secrets
    plus the ``DENSE_*`` knobs). All three load through ``load_env_files``
    (``override=False``, first writer wins), so the overlay beats the base, a
    real exported env var beats every file, and a stale stripped key left in the
    example ``.env`` cannot shadow the overlay. Anchoring to *example_dir* makes
    resolution independent of the working directory. *dotenv* overrides only the
    example ``.env`` layer; the overlay and base stay anchored to *example_dir*.
    """
    from dbxcarta.core.env import load_env_files

    secrets = dotenv if dotenv is not None else example_dir / ".env"
    load_env_files(
        [
            example_dir / "dbxcarta-overlay.env",
            example_dir.parents[1] / ".env",
            secrets,
        ]
    )
