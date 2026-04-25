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
      catalog at `dbxcarta_summary_table`'s most recent successful run (or the
      explicit run-id) and exits non-zero on any violation.
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
        help="Run id to verify. Defaults to the most recent status='success' row in dbxcarta_summary_table.",
    )
    args = parser.parse_args(argv)

    from dbxcarta.settings import Settings
    from dbxcarta.verify import verify_run

    settings = Settings()  # type: ignore[call-arg]
    if not settings.databricks_warehouse_id:
        print("error: DATABRICKS_WAREHOUSE_ID is required (verify CLI loads the run summary from the Delta summary table).", file=sys.stderr)
        return 2

    ws = _build_workspace_client()
    summary = _load_run_summary(ws, settings, args.run_id)
    if summary is None:
        scope_msg = f"run_id={args.run_id!r}" if args.run_id else "most recent status='success' run"
        print(f"error: no row found in {settings.dbxcarta_summary_table} for {scope_msg}.", file=sys.stderr)
        return 2

    driver = _build_neo4j_driver(ws, settings)
    try:
        report = verify_run(
            summary=summary,
            neo4j_driver=driver,
            ws=ws,
            warehouse_id=settings.databricks_warehouse_id,
            catalog=settings.dbxcarta_catalog,
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


def _load_run_summary(ws, settings, run_id: str | None) -> dict | None:
    """Load one row from the Delta summary table; return its column dict, or None."""
    from databricks.sdk.service.sql import StatementParameterListItem

    table = settings.dbxcarta_summary_table
    quoted = ".".join(f"`{p}`" for p in table.split("."))

    if run_id:
        statement = (
            f"SELECT * FROM {quoted} WHERE run_id = :run_id ORDER BY started_at DESC LIMIT 1"
        )
        params = [StatementParameterListItem(name="run_id", value=run_id)]
    else:
        statement = (
            f"SELECT * FROM {quoted} WHERE status = 'success' ORDER BY started_at DESC LIMIT 1"
        )
        params = []

    result = ws.statement_execution.execute_statement(
        warehouse_id=settings.databricks_warehouse_id,
        statement=statement,
        parameters=params or None,
        wait_timeout="30s",
    )
    if result.result is None or not result.result.data_array:
        return None
    schema = result.manifest.schema.columns
    row = result.result.data_array[0]
    return {col.name: row[i] for i, col in enumerate(schema)}
