from __future__ import annotations

import sys
from typing import TYPE_CHECKING

from dbxcarta.core.env import select_overlay_path

if TYPE_CHECKING:
    from pathlib import Path

    from databricks.sdk import WorkspaceClient
    from dbxcarta.spark.settings import SparkIngestSettings
    from neo4j import Driver


def main() -> None:
    """Entry point for the dbxcarta domain commands.

    - `dbxcarta verify [--run-id RUN_ID]` runs graph and catalog verification.
    - `dbxcarta ready` reports whether each ingested catalog holds a data schema.
    - `dbxcarta upload-questions [--questions PATH]` uploads the example's
      questions file (default: `questions.json` beside the `--env-file` overlay).

    Job submission and wheel upload moved to the separate `dbxcarta-submit`
    command. This package no longer depends on databricks-job-runner.
    """
    overlay = select_overlay_path()
    if overlay is not None:
        # Path only, never resolved values, so no secret reaches logs.
        print(f"dbxcarta: active env overlay: {overlay}", file=sys.stderr)

    command = sys.argv[1:2]
    if command == ["verify"]:
        sys.exit(_handle_verify(sys.argv[2:]))
    if command == ["ready"]:
        sys.exit(_handle_ready(sys.argv[2:]))
    if command == ["upload-questions"]:
        sys.exit(_handle_upload_questions(sys.argv[2:]))

    if command:
        print(f"error: unknown command {command[0]!r}", file=sys.stderr)
    print(
        "usage: dbxcarta {verify|ready|upload-questions} ...\n"
        "  Job submission and upload moved to the `dbxcarta-submit` command.",
        file=sys.stderr,
    )
    sys.exit(2)


def _load_env(argv: list[str]) -> tuple[list[str] | None, int]:
    """Resolve and load the overlay/base env files, stripping the option.

    Returns ``(cleaned_argv, 0)`` on success, or ``(None, 2)`` after
    printing an :class:`EnvFileError` so the caller returns the code.
    """
    from dbxcarta.core.env import EnvFileError, load_env_files, resolve_env_files

    try:
        env_files, cleaned = resolve_env_files(argv)
    except EnvFileError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return None, 2
    load_env_files(env_files)
    return cleaned, 0


def _handle_verify(argv: list[str]) -> int:
    import argparse

    cleaned, code = _load_env(argv)
    if cleaned is None:
        return code
    argv = cleaned

    parser = argparse.ArgumentParser(prog="dbxcarta verify")
    parser.add_argument(
        "--run-id",
        help="Run id to verify. Defaults to the most recent status='success' summary.",
    )
    args = parser.parse_args(argv)

    from dbxcarta.spark.ingest.summary_io import LoadSummaryError, load_summary_from_volume
    from dbxcarta.spark.settings import SparkIngestSettings
    from dbxcarta.spark.verify import verify_run

    settings = SparkIngestSettings()

    ws = _build_workspace_client()
    try:
        summary = load_summary_from_volume(ws, settings.dbxcarta_summary_volume, run_id=args.run_id)
    except LoadSummaryError as e:
        print(f"error: {e}", file=sys.stderr)
        return 2
    if summary is None:
        scope_msg = f"run_id={args.run_id!r}" if args.run_id else "most recent status='success' run"
        print(
            f"error: no run summary found in {settings.dbxcarta_summary_volume} for {scope_msg}.",
            file=sys.stderr,
        )
        return 2

    driver = _build_neo4j_driver(ws, settings)
    try:
        report = verify_run(
            summary=summary,
            neo4j_driver=driver,
            ws=ws,
            warehouse_id=settings.databricks_warehouse_id,
            catalog=settings.dbxcarta_catalog,
            catalogs=settings.resolved_catalogs(),
            sample_limit=settings.dbxcarta_sample_limit,
        )
    finally:
        driver.close()

    print(report.format())
    return 0 if report.ok else 1


def _handle_ready(argv: list[str]) -> int:
    import argparse
    import os

    cleaned, code = _load_env(argv)
    if cleaned is None:
        return code

    parser = argparse.ArgumentParser(prog="dbxcarta ready")
    parser.add_argument("--warehouse-id", default="")
    parser.add_argument("--strict-optional", action="store_true")
    args = parser.parse_args(cleaned)

    from dbxcarta.core.readiness import check_readiness

    warehouse_id = args.warehouse_id or os.environ.get("DATABRICKS_WAREHOUSE_ID", "")
    if not warehouse_id:
        print("error: DATABRICKS_WAREHOUSE_ID is required for ready", file=sys.stderr)
        return 2
    report = check_readiness(_build_workspace_client(), warehouse_id)
    print(report.format(strict_optional=args.strict_optional))
    return 0 if report.ok(strict_optional=args.strict_optional) else 1


def _handle_upload_questions(argv: list[str]) -> int:
    import argparse

    cleaned, code = _load_env(argv)
    if cleaned is None:
        return code

    parser = argparse.ArgumentParser(prog="dbxcarta upload-questions")
    parser.add_argument(
        "--questions",
        default="",
        help="Path to questions.json (default: beside the --env-file overlay).",
    )
    args = parser.parse_args(cleaned)

    from dbxcarta.core.readiness import upload_questions

    questions_file = _resolve_questions_file(args.questions)
    if questions_file is None:
        return 2
    upload_questions(_build_workspace_client(), questions_file)
    return 0


def _resolve_questions_file(override: str) -> Path | None:
    """Resolve the questions file from ``--questions`` or beside the overlay.

    Without ``--questions``, the file is ``questions.json`` in the same
    directory as the selected ``--env-file`` overlay. Returns ``None`` after
    printing an error when no path can be resolved or the file is missing.
    """
    from pathlib import Path

    from dbxcarta.core.env import select_overlay_path

    if override:
        path = Path(override)
    else:
        overlay = select_overlay_path()
        if overlay is None:
            print(
                "error: no --questions given and no --env-file overlay selected"
                " to locate questions.json",
                file=sys.stderr,
            )
            return None
        path = overlay.parent / "questions.json"
    if not path.is_file():
        print(f"error: questions file not found at {path}", file=sys.stderr)
        return None
    return path


def _build_workspace_client() -> WorkspaceClient:
    from dbxcarta.core.workspace import build_workspace_client

    return build_workspace_client()


def _build_neo4j_driver(ws: WorkspaceClient, settings: SparkIngestSettings) -> Driver:
    from dbxcarta.core.workspace import read_workspace_secret
    from neo4j import GraphDatabase

    scope = settings.databricks_secret_scope
    return GraphDatabase.driver(
        read_workspace_secret(ws, scope, "NEO4J_URI"),
        auth=(
            read_workspace_secret(ws, scope, "NEO4J_USERNAME"),
            read_workspace_secret(ws, scope, "NEO4J_PASSWORD"),
        ),
    )
