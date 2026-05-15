from __future__ import annotations

import sys
from pathlib import Path

from databricks_job_runner import Runner

_ENTRYPOINT_WHEEL_PACKAGE: dict[str, str] = {
    "ingest": "dbxcarta-spark",
    "client": "dbxcarta-client",
}

runner = Runner(
    run_name_prefix="dbxcarta",
    wheel_package="dbxcarta-spark",
    scripts_dir="scripts",
    cli_command="uv run dbxcarta",
    secret_keys=["NEO4J_URI", "NEO4J_USERNAME", "NEO4J_PASSWORD"],
)


def main() -> None:
    """Entry point.

    - `dbxcarta verify [--run-id RUN_ID]` runs graph and catalog verification.
    - `dbxcarta preset <import-path> {--print-env|--check-ready|--upload-questions|--run}`
      resolves the given preset and runs the requested action.
    - `dbxcarta submit-entrypoint {ingest|client}` submits the wheel entrypoint.
    - All other invocations dispatch to databricks_job_runner.Runner.
    """
    if sys.argv[1:2] == ["verify"]:
        sys.exit(_handle_verify(sys.argv[2:]))
    if sys.argv[1:2] == ["preset"]:
        sys.exit(_handle_preset(sys.argv[2:]))
    if sys.argv[1:2] == ["submit-entrypoint"]:
        sys.exit(_handle_submit_entrypoint(sys.argv[2:]))

    runner.main()


def _handle_verify(argv: list[str]) -> int:
    import argparse

    from dotenv import load_dotenv

    load_dotenv(Path(".env"))

    parser = argparse.ArgumentParser(prog="dbxcarta verify")
    parser.add_argument(
        "--run-id",
        help="Run id to verify. Defaults to the most recent status='success' summary.",
    )
    args = parser.parse_args(argv)

    from dbxcarta.spark.settings import SparkIngestSettings
    from dbxcarta.spark.ingest.summary_io import LoadSummaryError, load_summary_from_volume
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


def _handle_preset(argv: list[str]) -> int:
    import argparse
    import os

    from dotenv import load_dotenv

    load_dotenv(Path(".env"), override=False)

    parser = argparse.ArgumentParser(prog="dbxcarta preset")
    parser.add_argument("spec", help="Preset import spec in 'package.module:attr' form.")
    actions = parser.add_mutually_exclusive_group(required=True)
    actions.add_argument("--print-env", action="store_true")
    actions.add_argument("--check-ready", action="store_true")
    actions.add_argument("--upload-questions", action="store_true")
    actions.add_argument("--run", action="store_true")
    parser.add_argument("--warehouse-id", default="")
    parser.add_argument("--strict-optional", action="store_true")
    args = parser.parse_args(argv)

    from dbxcarta.spark.loader import load_preset
    from dbxcarta.spark.presets import (
        QuestionsUploadable,
        ReadinessCheckable,
        format_env,
    )

    try:
        preset = load_preset(args.spec)
    except (ValueError, ImportError, AttributeError, TypeError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    if args.print_env:
        print(format_env(preset.env()), end="")
        return 0

    if args.check_ready:
        if not isinstance(preset, ReadinessCheckable):
            print(f"error: preset {args.spec!r} does not implement readiness()", file=sys.stderr)
            return 2
        warehouse_id = args.warehouse_id or os.environ.get("DATABRICKS_WAREHOUSE_ID", "")
        if not warehouse_id:
            print("error: DATABRICKS_WAREHOUSE_ID is required for --check-ready", file=sys.stderr)
            return 2
        ws = _build_workspace_client()
        report = preset.readiness(ws, warehouse_id)
        print(report.format(strict_optional=args.strict_optional))
        return 0 if report.ok(strict_optional=args.strict_optional) else 1

    if args.upload_questions:
        if not isinstance(preset, QuestionsUploadable):
            print(f"error: preset {args.spec!r} does not implement upload_questions()", file=sys.stderr)
            return 2
        ws = _build_workspace_client()
        preset.upload_questions(ws)
        return 0

    if args.run:
        from dbxcarta.spark.env import apply_env_overlay
        from dbxcarta.spark.run import run_dbxcarta

        apply_env_overlay(preset)
        run_dbxcarta()
        return 0

    return 2


def _handle_submit_entrypoint(argv: list[str]) -> int:
    import argparse

    from databricks_job_runner.errors import RunnerError

    parser = argparse.ArgumentParser(prog="dbxcarta submit-entrypoint")
    parser.add_argument("entrypoint", choices=("ingest", "client"))
    parser.add_argument("--compute", choices=("cluster", "serverless"), default=None)
    parser.add_argument("--no-wait", action="store_true")
    args = parser.parse_args(argv)

    console_entrypoints = {
        "ingest": "dbxcarta-ingest",
        "client": "dbxcarta-client",
    }
    try:
        _submit_wheel_entrypoint(
            args.entrypoint,
            console_entrypoints[args.entrypoint],
            compute_mode=args.compute,
            no_wait=args.no_wait,
        )
    except RunnerError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    return 0


def _submit_wheel_entrypoint(
    name: str,
    console_entrypoint: str,
    *,
    compute_mode: str | None,
    no_wait: bool,
) -> None:
    from databricks.sdk.service.jobs import (
        PythonWheelTask,
        RunResultState,
        SubmitTask,
    )
    from databricks_job_runner.errors import RunnerError
    from databricks_job_runner.upload import find_latest_wheel

    wheel_package = _ENTRYPOINT_WHEEL_PACKAGE.get(name)
    if wheel_package is None:
        raise RunnerError(f"unknown wheel entrypoint {name!r}")

    wheel = find_latest_wheel(runner.project_dir / "dist", wheel_package)
    if wheel is None:
        raise RunnerError(
            f"no {wheel_package} wheel found in dist/."
            f" Run `uv build --package {wheel_package}` first."
        )
    wheel_path = f"{runner.wheel_volume_dir}/{wheel.name}"

    params = runner.config.env_params(secret_keys=runner.secret_keys)
    run_name = f"{runner.run_name_prefix}: {name}"
    compute = runner._compute(compute_mode)
    if name == "ingest" and _is_serverless_compute(compute):
        raise RunnerError(
            "dbxcarta ingest uses the Neo4j Spark Connector, which is not "
            "supported on Databricks serverless jobs compute. Use classic "
            "compute with `--compute cluster`."
        )

    print("Submitting wheel entrypoint")
    print(f"  Entrypoint: {console_entrypoint}")
    print(f"  Wheel:      {wheel_path}")
    print(f"  Run name:   {run_name}")
    if params:
        print(f"  Params:     {len(params)} env values from .env")
    print("---")

    compute.validate(runner.ws)
    task = SubmitTask(
        task_key=f"run_{name}",
        python_wheel_task=PythonWheelTask(
            package_name=wheel_package,
            entry_point=console_entrypoint,
            parameters=params if params else None,
        ),
    )
    task = compute.decorate_task(task, wheel_path)

    waiter = runner.ws.jobs.submit(
        run_name=run_name,
        tasks=[task],
        environments=compute.environments(wheel_path),
    )

    run_id = waiter.run_id
    if run_id is None:
        raise RunnerError("Databricks did not return a run_id for the submitted job.")
    print(f"  Run ID:     {run_id}")

    if no_wait:
        print("\nJob submitted (--no-wait). Check status in the Databricks UI.")
    else:
        print("  Waiting for completion...")
        run = waiter.result()
        result_state = run.state.result_state if run.state else None
        state_name = result_state.value if result_state else "UNKNOWN"
        page_url = run.run_page_url or ""

        print(f"\n  Result:     {state_name}")
        if page_url:
            print(f"  URL:        {page_url}")

        if result_state != RunResultState.SUCCESS:
            raise RunnerError(f"Job finished with non-success state: {state_name}")
        print("\nJob complete.")

    print()
    print("Next steps:")
    print(f"  View logs:          {runner.cli_command} logs {run_id}")
    if runner.config.databricks_volume_path:
        print(f"  List results:       {runner.cli_command} download --list results")
        print(f"  Download results:   {runner.cli_command} download results/<filename>")


def _is_serverless_compute(compute: object) -> bool:
    return compute.__class__.__name__ == "Serverless"


def _build_workspace_client():
    from dbxcarta.spark.databricks import build_workspace_client

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
