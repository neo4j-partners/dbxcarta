from __future__ import annotations

import sys
from pathlib import Path

from databricks_job_runner import Runner

runner = Runner(
    run_name_prefix="dbxcarta",
    wheel_package="dbxcarta",
    scripts_dir="scripts",
    cli_command="uv run dbxcarta",
    # Excluded from cleartext env-param forwarding to job parameters. The
    # runner instead emits DATABRICKS_SECRET_KEYS=NEO4J_URI,... and
    # inject_params() pulls these from databricks_secret_scope on the
    # cluster. Required to keep credentials out of the job-run record
    # (visible via `databricks jobs get-run <id>`).
    secret_keys=["NEO4J_URI", "NEO4J_USERNAME", "NEO4J_PASSWORD"],
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

    - `dbxcarta verify [--run-id RUN_ID]` runs graph and catalog verification
      against the most recent status='success' run summary in
      `dbxcarta_summary_volume` (or the explicit run-id) and exits non-zero on
      any violation.
    - `dbxcarta preset finance-genie ...` prints or checks the Finance Genie
      companion configuration without dispatching a Databricks job.
    - `dbxcarta demo finance-genie ...` runs the local read-only Finance Genie
      semantic-layer demo.
    - All other invocations dispatch to the databricks_job_runner.Runner that
      backs `submit`/`status`/etc., with the legacy `--compute serverless`
      injection for the client script.
    """
    if sys.argv[1:2] == ["verify"]:
        sys.exit(_handle_verify(sys.argv[2:]))
    if sys.argv[1:3] == ["preset", "finance-genie"]:
        sys.exit(_handle_finance_genie_preset(sys.argv[3:]))
    if sys.argv[1:3] == ["demo", "finance-genie"]:
        sys.exit(_handle_finance_genie_demo(sys.argv[3:]))

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
    from dbxcarta.ingest.summary import LoadSummaryError, load_summary_from_volume
    from dbxcarta.verify import verify_run

    settings = Settings()  # type: ignore[call-arg]

    ws = _build_workspace_client()
    try:
        summary = load_summary_from_volume(ws, settings.dbxcarta_summary_volume, run_id=args.run_id)
    except LoadSummaryError as e:
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


def _handle_finance_genie_preset(argv: list[str]) -> int:
    import argparse
    import os

    from dotenv import load_dotenv

    from dbxcarta.databricks import validate_identifier
    from dbxcarta.presets.finance_genie import (
        FINANCE_GENIE_CATALOG,
        FINANCE_GENIE_SCHEMA,
        FINANCE_GENIE_VOLUME,
        finance_genie_env,
        format_env,
        readiness_from_table_names,
    )

    load_dotenv(Path(".env"), override=False)

    parser = argparse.ArgumentParser(prog="dbxcarta preset finance-genie")
    actions = parser.add_mutually_exclusive_group(required=True)
    actions.add_argument(
        "--print-env",
        action="store_true",
        help="Print the recommended dbxcarta env overlay for Finance Genie.",
    )
    actions.add_argument(
        "--check-ready",
        action="store_true",
        help="Check whether the expected Finance Genie UC tables exist.",
    )
    parser.add_argument("--catalog", default="")
    parser.add_argument("--schema", default="")
    parser.add_argument("--volume", default="")
    parser.add_argument("--warehouse-id", default="")
    parser.add_argument(
        "--strict-gold",
        action="store_true",
        help="Fail readiness if Gold enrichment tables are missing.",
    )
    args = parser.parse_args(argv)

    if args.print_env:
        catalog = validate_identifier(
            args.catalog or FINANCE_GENIE_CATALOG,
            label="catalog",
        )
        schema = validate_identifier(
            args.schema or FINANCE_GENIE_SCHEMA,
            label="schema",
        )
        volume = validate_identifier(
            args.volume or FINANCE_GENIE_VOLUME,
            label="volume",
        )
    else:
        catalog = validate_identifier(
            args.catalog
            or os.environ.get("DBXCARTA_CATALOG")
            or os.environ.get("DATABRICKS_CATALOG")
            or FINANCE_GENIE_CATALOG,
            label="catalog",
        )
        schema = validate_identifier(
            args.schema
            or _single_schema(os.environ.get("DBXCARTA_SCHEMAS", ""))
            or os.environ.get("DATABRICKS_SCHEMA")
            or FINANCE_GENIE_SCHEMA,
            label="schema",
        )
        volume = validate_identifier(
            args.volume
            or os.environ.get("DATABRICKS_VOLUME")
            or FINANCE_GENIE_VOLUME,
            label="volume",
    )

    if args.print_env:
        print(
            format_env(
                finance_genie_env(catalog=catalog, schema=schema, volume=volume)
            ),
            end="",
        )
        return 0

    warehouse_id = args.warehouse_id or os.environ.get("DATABRICKS_WAREHOUSE_ID", "")
    if not warehouse_id:
        print(
            "error: DATABRICKS_WAREHOUSE_ID is required for --check-ready",
            file=sys.stderr,
        )
        return 2

    ws = _build_workspace_client()
    table_names = _fetch_table_names(ws, warehouse_id, catalog, schema)
    report = readiness_from_table_names(table_names, catalog=catalog, schema=schema)
    print(report.format(strict_gold=args.strict_gold))
    return 0 if report.ok(strict_gold=args.strict_gold) else 1


def _handle_finance_genie_demo(argv: list[str]) -> int:
    from dotenv import load_dotenv

    from dbxcarta.client.local_demo import main as demo_main

    load_dotenv(Path(".env"), override=False)
    return demo_main(argv)


def _single_schema(value: str) -> str:
    schemas = [part.strip() for part in value.split(",") if part.strip()]
    return schemas[0] if len(schemas) == 1 else ""


def _fetch_table_names(ws, warehouse_id: str, catalog: str, schema: str) -> list[str]:
    from databricks.sdk.service.sql import ExecuteStatementRequestOnWaitTimeout

    from dbxcarta.databricks import quote_identifier

    statement = (
        "SELECT table_name "
        f"FROM {quote_identifier(catalog)}.information_schema.tables "
        f"WHERE table_schema = '{schema}'"
    )
    response = ws.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=statement,
        wait_timeout="50s",
        on_wait_timeout=ExecuteStatementRequestOnWaitTimeout.CANCEL,
    )
    rows = getattr(getattr(response, "result", None), "data_array", None) or []
    return [row[0] for row in rows if row]


def _build_workspace_client():
    from dbxcarta.databricks import build_workspace_client

    return build_workspace_client()


def _build_neo4j_driver(ws, settings):
    import base64

    from neo4j import GraphDatabase

    scope = settings.databricks_secret_scope

    def _secret(key: str) -> str:
        return base64.b64decode(ws.secrets.get_secret(scope=scope, key=key).value).decode()

    return GraphDatabase.driver(
        _secret("NEO4J_URI"),
        auth=(_secret("NEO4J_USERNAME"), _secret("NEO4J_PASSWORD")),
    )
