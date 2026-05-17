"""`python -m dbxcarta.client.eval.entrypoint` — Databricks client entrypoint.

Parses leading KEY=VALUE positional arguments into the environment, then runs
the Text2SQL evaluation harness.
"""

from __future__ import annotations

from dbxcarta.client.env import inject_params
from dbxcarta.client.eval import run_client


def main() -> None:
    inject_params()
    run_client()


if __name__ == "__main__":
    main()
