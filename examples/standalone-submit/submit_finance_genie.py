# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "dbxcarta-submit>=1.1.0",
# ]
# ///
r"""Submit the finance-genie ingest job to Databricks with dbxcarta-submit.

This is the standalone library-call counterpart to
``dbxcarta submit-entrypoint ingest``: it uses ``dbxcarta-submit`` to stage a
prebuilt neocarta connector wheel on a UC Volume and submit the ingest job. The
job runs on the cluster and writes the finance-genie semantic graph into Neo4j.

Run it from the dbxcarta repo root so the relative base ``.env`` resolves::

    uv run python examples/standalone-submit/submit_finance_genie.py

The neocarta connector wheel is built in the neocarta project, not here. This
script resolves it the same way the operator CLI does: an explicit wheel path
as the first argument wins, otherwise the newest ``neocarta-*.whl`` in the
directory named by ``NEOCARTA_WHEEL_SOURCE`` (set in the repo-root ``.env``).

    uv run python examples/standalone-submit/submit_finance_genie.py \
        /path/to/neocarta-<version>-py3-none-any.whl

Configuration comes from ``submit_finance_genie.env`` beside this script (copy
the ``.sample`` and fill in the infra values). It is secret-free; the cluster
reads Neo4j credentials from the Databricks secret scope it names.

Prerequisites:
- A classic (non-serverless) cluster with the Neo4j Spark Connector attached.
- A reachable Neo4j instance and a provisioned Databricks secret scope holding
  its credentials (the scope named by ``NEOCARTA_DATABRICKS_SECRET_SCOPE``).
- Databricks auth for the profile named in the config.
- A built neocarta wheel, reachable via ``NEOCARTA_WHEEL_SOURCE`` or an explicit
  path argument.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

from databricks_job_runner.upload import find_latest_wheel
from dbxcarta.core.env import load_env_files, resolve_env_files
from dbxcarta.submit import submit_neocarta_ingest

_HERE = Path(__file__).resolve().parent
_ENV_FILE = _HERE / "submit_finance_genie.env"
# The ingest wheel is the neocarta connector, whose distribution name (and so
# its wheel glob prefix) is ``neocarta``. Matches _ENTRYPOINT_WHEEL_PACKAGE["ingest"].
_NEOCARTA_PACKAGE = "neocarta"


def _resolve_neocarta_wheel(argv: list[str]) -> Path:
    """Return the neocarta wheel to submit.

    An explicit path argument wins. Otherwise resolve the newest
    ``neocarta-*.whl`` in ``NEOCARTA_WHEEL_SOURCE``, the same local operator
    setting ``dbxcarta publish-wheels`` reads from the repo-root ``.env``.
    """
    if argv:
        wheel = Path(argv[0]).expanduser()
        if not wheel.is_file():
            raise SystemExit(f"neocarta wheel not found: {wheel}")
        return wheel

    source_value = os.environ.get("NEOCARTA_WHEEL_SOURCE", "").strip()
    if not source_value:
        raise SystemExit(
            "no neocarta wheel given and NEOCARTA_WHEEL_SOURCE is not set. Pass a "
            "wheel path as the first argument, or set NEOCARTA_WHEEL_SOURCE to the "
            "neocarta project's dist/ folder (it is set in the repo-root .env)."
        )
    source_dir = Path(source_value).expanduser()
    if not source_dir.is_dir():
        raise SystemExit(f"NEOCARTA_WHEEL_SOURCE {source_dir} is not a directory.")

    wheel = find_latest_wheel(source_dir, _NEOCARTA_PACKAGE)
    if wheel is None:
        raise SystemExit(
            f"no {_NEOCARTA_PACKAGE} wheel found in {source_dir}. Build it in the "
            "neocarta project (uv build --wheel), point NEOCARTA_WHEEL_SOURCE at "
            "its dist/, or pass a wheel path as the first argument."
        )
    return wheel


def main() -> int:
    """Stage the prebuilt connector wheel and submit the ingest job."""
    if not _ENV_FILE.is_file():
        raise SystemExit(
            f"config not found: {_ENV_FILE}\n"
            "Copy submit_finance_genie.env.sample to submit_finance_genie.env "
            "and fill in the infra values."
        )

    # dbxcarta-submit resolves the config from DBXCARTA_ENV_FILE, exactly as the
    # `dbxcarta` CLI does. setdefault lets an already-exported value win.
    os.environ.setdefault("DBXCARTA_ENV_FILE", str(_ENV_FILE))

    # Load the selected overlay then the repo-root base .env (override=False, so
    # overlay wins), mirroring `dbxcarta publish-wheels`. This is what makes
    # NEOCARTA_WHEEL_SOURCE resolve here: it is a local operator setting kept in
    # the base .env and never forwarded to the cluster.
    files, _ = resolve_env_files([])
    load_env_files(files)

    wheel = _resolve_neocarta_wheel(sys.argv[1:])
    print(f"submitting neocarta wheel: {wheel}")
    # Stages the wheel and submits the ingest job on classic compute (its default).
    submit_neocarta_ingest(wheel)
    return 0


if __name__ == "__main__":
    sys.exit(main())
