"""`python -m dbxcarta.entrypoints.ingest` — Databricks job entrypoint.

Parses leading KEY=VALUE positional arguments into the environment, then runs
the ingest pipeline.
"""
from __future__ import annotations

from dbxcarta.entrypoints._bootstrap import inject_params
from dbxcarta.ingest import run_dbxcarta


def main() -> None:
    inject_params()
    run_dbxcarta()


if __name__ == "__main__":
    main()
