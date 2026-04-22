"""Cluster bootstrap helpers — imported by job scripts running on Databricks.

The databricks_job_runner package is local-only and not installed on the
cluster. Uploading this module alongside the job scripts lets each job import
inject_params here instead of depending on databricks_job_runner at runtime.
"""
from __future__ import annotations

import os
import sys


def inject_params() -> None:
    """Parse KEY=VALUE argv parameters into os.environ.

    The CLI runner forwards .env variables as positional KEY=VALUE args.
    Uses setdefault so pre-existing environment variables take precedence,
    matching standard 12-factor semantics.
    """
    remaining: list[str] = []
    for arg in sys.argv[1:]:
        if "=" in arg and not arg.startswith("-"):
            key, _, value = arg.partition("=")
            os.environ.setdefault(key, value)
        else:
            remaining.append(arg)
    sys.argv[1:] = remaining
