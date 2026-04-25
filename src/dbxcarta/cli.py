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
    """Entry point.

    - `dbxcarta verify [--run-id RUN_ID]` runs Phase 2 verify against the
      most recent status='success' run summary in `dbxcarta_summary_volume`
      (or the explicit run-id) and exits non-zero on any violation.
    - All other invocations dispatch to the databricks_job_runner.Runner that
      backs `submit`/`status`/etc., with the legacy `--compute serverless`
      injection for the client script.
    """
    if sys.argv[1:2] == ["verify"]:
        sys.exit(_handle_verify(sys.argv[2:]))

    is_submit = "submit" in sys.argv[1:]
    is_client = _CLIENT_SCRIPT in sys.argv[1:]
    has_compute = "--compute" in sys.argv[1:]

    if is_submit and is_client and not has_compute and _read_client_serverless():
        idx = sys.argv.index("submit")
        sys.argv.insert(idx + 1, "--compute")
        sys.argv.insert(idx + 2, "serverless")

    runner.main()


def _handle_verify(argv: list[str]) -> int:
    import argparse

    from dotenv import load_dotenv

    load_dotenv(Path(".env"))

    parser = argparse.ArgumentParser(prog="dbxcarta verify")
    parser.add_argument(
        "--run-id",
        help="Run id to verify. Defaults to the most recent status='success' summary in dbxcarta_summary_volume.",
    )
    args = parser.parse_args(argv)

    from dbxcarta.settings import Settings
    from dbxcarta.verify import verify_run

    settings = Settings()  # type: ignore[call-arg]

    ws = _build_workspace_client()
    try:
        summary = _load_run_summary(ws, settings, args.run_id)
    except _LoadSummaryError as e:
        print(f"error: {e}", file=sys.stderr)
        return 2
    if summary is None:
        scope_msg = f"run_id={args.run_id!r}" if args.run_id else "most recent status='success' run"
        print(f"error: no run summary found in {settings.dbxcarta_summary_volume} for {scope_msg}.", file=sys.stderr)
        return 2

    driver = _build_neo4j_driver(ws, settings)
    try:
        report = verify_run(
            summary=summary,
            neo4j_driver=driver,
            ws=ws,
            warehouse_id=settings.databricks_warehouse_id,
            catalog=settings.dbxcarta_catalog,
            sample_limit=settings.dbxcarta_sample_limit,
        )
    finally:
        driver.close()

    print(report.format())
    return 0 if report.ok else 1


def _build_workspace_client():
    import os

    from databricks.sdk import WorkspaceClient

    return WorkspaceClient(profile=os.environ.get("DATABRICKS_PROFILE"))


def _build_neo4j_driver(ws, settings):
    import base64

    from neo4j import GraphDatabase

    scope = settings.databricks_secret_scope

    def _secret(key: str) -> str:
        return base64.b64decode(ws.secrets.get_secret(scope=scope, key=key).value).decode()

    return GraphDatabase.driver(
        _secret("uri"),
        auth=(_secret("username"), _secret("password")),
    )


class _LoadSummaryError(Exception):
    """Raised on ambiguous --run-id matches or other unrecoverable load errors."""


def _load_run_summary(ws, settings, run_id: str | None) -> dict | None:
    """Load a run summary from the UC Volume JSON files; return its dict, or None.

    `summary.py:emit_json` writes `{job_name}_{run_id}_{ts}.json` files. With
    `--run-id` set, we filter for that token; otherwise we scan newest-first
    and return the first summary with status='success'.
    """
    import json

    volume_path = settings.dbxcarta_summary_volume
    entries = list(ws.files.list_directory_contents(directory_path=volume_path))
    candidates = [
        e for e in entries
        if e.name.startswith("dbxcarta_") and e.name.endswith(".json")
    ]
    if not candidates:
        return None

    if run_id:
        matched = [e for e in candidates if e.name.startswith(f"dbxcarta_{run_id}_")]
        if not matched:
            return None
        if len(matched) > 1:
            names = sorted(e.name for e in matched)
            raise _LoadSummaryError(
                f"--run-id={run_id!r} matched {len(matched)} files in {volume_path}: "
                f"{names}. Disambiguate by removing duplicates or pass a more specific value."
            )
        target = matched[0]
        content = ws.files.download(file_path=f"{volume_path}/{target.name}").contents.read()
        return json.loads(content)

    # No run_id: scan newest-first, return the first success.
    for entry in sorted(candidates, key=lambda e: e.name, reverse=True):
        content = ws.files.download(file_path=f"{volume_path}/{entry.name}").contents.read()
        loaded = json.loads(content)
        if loaded.get("status") == "success":
            return loaded
    return None
