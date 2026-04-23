from __future__ import annotations

import sys
from pathlib import Path

from databricks_job_runner import Runner

runner = Runner(
    run_name_prefix="dbxcarta",
    wheel_package="dbxcarta",
    scripts_dir="scripts",
    cli_command="uv run dbxcarta",
)

_CLIENT_SCRIPT = "run_dbxcarta_client.py"


def _read_client_serverless(env_file: Path = Path(".env")) -> bool:
    import os
    env_val = os.environ.get("DBXCARTA_CLIENT_SERVERLESS")
    if env_val is not None:
        return env_val.strip().lower() in ("1", "true", "yes")
    if not env_file.exists():
        return False
    for line in env_file.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        key, _, value = line.partition("=")
        if key.strip() == "DBXCARTA_CLIENT_SERVERLESS":
            return value.strip().strip("'\"").lower() in ("1", "true", "yes")
    return False


def main() -> None:
    """Entry point. Injects --compute serverless for the client script when
    DBXCARTA_CLIENT_SERVERLESS=true and no explicit --compute flag is present."""
    is_submit = "submit" in sys.argv[1:]
    is_client = _CLIENT_SCRIPT in sys.argv[1:]
    has_compute = "--compute" in sys.argv[1:]

    if is_submit and is_client and not has_compute and _read_client_serverless():
        idx = sys.argv.index("submit")
        sys.argv.insert(idx + 1, "--compute")
        sys.argv.insert(idx + 2, "serverless")

    runner.main()
