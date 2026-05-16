from __future__ import annotations

import sys
from pathlib import Path
from typing import TYPE_CHECKING, TypedDict

from databricks_job_runner import (
    Compute,
    DesiredLibrary,
    Runner,
    Serverless,
    maven_libraries_preflight,
)

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient
    from neo4j import Driver

    from dbxcarta.spark.settings import SparkIngestSettings

_ENTRYPOINT_WHEEL_PACKAGE: dict[str, str] = {
    "ingest": "dbxcarta-spark",
    "client": "dbxcarta-client",
}

_ENTRYPOINT_CONSOLE_SCRIPT: dict[str, str] = {
    "ingest": "dbxcarta-ingest",
    "client": "dbxcarta-client",
}

# Packages the Databricks Runtime provides on the cluster and that the
# curated closures below must never reinstall. ``pyspark``/``py4j`` are
# the cluster runtime itself; ``databricks-sdk`` is DBR-managed and
# reinstalling it would shadow the platform SDK and drag in a heavy auth
# subtree. ``pydantic``/``pydantic-core`` are deliberately NOT excluded:
# the ingest and client code require pydantic v2 and the DBR-bundled
# version is not guaranteed to match.
_DBR_PROVIDED_PACKAGES: frozenset[str] = frozenset(
    {"pyspark", "py4j", "databricks-sdk"}
)

# Fully pinned dependency closures installed by the runner bootstrap into
# the shared driver environment with ``--no-deps``. These are curated
# transitive closures, not top-level lists: pip performs no resolution,
# so a steady-state run cannot pull a newer transitive that conflicts
# with a DBR-managed package. Each excludes the DBR-provided packages
# above. The cluster Python/platform must have matching binary wheels
# (notably ``pydantic-core``); that is validated on the warm cluster in
# Phase V3.
_INGEST_PINNED_CLOSURE: tuple[str, ...] = (
    "databricks-job-runner==0.6.1",
    "neo4j==6.1.0",
    "pytz==2026.1.post1",
    "pydantic==2.13.3",
    "pydantic-core==2.46.3",
    "pydantic-settings==2.14.0",
    "python-dotenv==1.2.2",
    "annotated-types==0.7.0",
    "typing-extensions==4.15.0",
    "typing-inspection==0.4.2",
)

_CLIENT_PINNED_CLOSURE: tuple[str, ...] = (
    "neo4j==6.1.0",
    "pytz==2026.1.post1",
    "pydantic==2.13.3",
    "pydantic-core==2.46.3",
    "pydantic-settings==2.14.0",
    "python-dotenv==1.2.2",
    "annotated-types==0.7.0",
    "typing-extensions==4.15.0",
    "typing-inspection==0.4.2",
)

_ENTRYPOINT_PINNED_CLOSURE: dict[str, tuple[str, ...]] = {
    "ingest": _INGEST_PINNED_CLOSURE,
    "client": _CLIENT_PINNED_CLOSURE,
}

# Top-level import names checked by the bootstrap post-install smoke
# check. Import names cannot be derived from version pins, so they are
# supplied explicitly.
_ENTRYPOINT_SMOKE_IMPORTS: dict[str, tuple[str, ...]] = {
    "ingest": (
        "databricks_job_runner",
        "neo4j",
        "pydantic",
        "pydantic_core",
        "pydantic_settings",
        "dotenv",
    ),
    "client": (
        "neo4j",
        "pydantic",
        "pydantic_core",
        "pydantic_settings",
    ),
}

# The Neo4j Spark Connector stays a pinned JVM cluster library (pip
# cannot install it). Only the ingest path probes and asserts it.
_NEO4J_MAVEN_COORDINATES = (
    "org.neo4j:neo4j-connector-apache-spark_2.13:5.3.10_for_spark_3"
)
_INGEST_JVM_PROBE_CLASS = "org.neo4j.spark.DataSource"

_ENTRYPOINT_JVM_PROBE_CLASS: dict[str, str | None] = {
    "ingest": _INGEST_JVM_PROBE_CLASS,
    "client": None,
}

class _RunnerKwargs(TypedDict):
    """Per-key types for the shared Runner kwargs.

    A plain ``dict(...)`` literal makes mypy unify the str and list[str]
    values to ``Sequence[str]``, which then fails to match Runner's distinct
    parameter types when splatted. The TypedDict preserves each key's type
    through ``Runner(**_RUNNER_KWARGS)`` while keeping the single source of
    truth shared by both call sites.
    """

    run_name_prefix: str
    wheel_package: str
    scripts_dir: str
    cli_command: str
    secret_keys: list[str]


_RUNNER_KWARGS: _RunnerKwargs = {
    "run_name_prefix": "dbxcarta",
    "wheel_package": "dbxcarta-spark",
    "scripts_dir": "scripts",
    "cli_command": "uv run dbxcarta",
    "secret_keys": ["NEO4J_URI", "NEO4J_USERNAME", "NEO4J_PASSWORD"],
}

# Shared runner for generic pass-through commands, the client path, and
# uploads. No preflights: client and generic commands do not need the
# Neo4j connector.
runner = Runner(**_RUNNER_KWARGS)


def _ingest_runner() -> Runner:
    """Runner scoped to the ingest submission with the connector preflight.

    The maven preflight is wired here rather than on the shared runner so
    submit/validate of the client and generic pass-through paths, which
    do not use the Neo4j connector, are not gated on it. The check is
    assert-only: it never installs or uninstalls a library.
    """
    return Runner(
        **_RUNNER_KWARGS,
        preflights=[
            maven_libraries_preflight(
                [DesiredLibrary.maven(_NEO4J_MAVEN_COORDINATES)],
                name="neo4j connector",
            )
        ],
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
    if sys.argv[1:2] == ["upload"] and "--wheel" in sys.argv[2:]:
        sys.exit(_handle_upload())

    runner.main()


def _handle_upload() -> int:
    from databricks_job_runner.errors import RunnerError
    from databricks_job_runner.upload import publish_wheel_stable

    # Publish a stable wheel for every submit-entrypoint package so both
    # `submit-entrypoint ingest` and `submit-entrypoint client` resolve
    # their wheel from the fixed Volume path. `upload_all` then ships the
    # runner bootstrap script the SparkPythonTask runs.
    try:
        for wheel_package in dict.fromkeys(_ENTRYPOINT_WHEEL_PACKAGE.values()):
            publish_wheel_stable(
                runner.ws,
                runner.project_dir,
                runner.wheel_volume_dir,
                wheel_package,
            )
        runner.upload_all()
    except RunnerError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    return 0


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

    # Unreachable: the actions group is mutually exclusive and required,
    # so exactly one branch above returns. Kept to satisfy the int return
    # contract, since mypy cannot prove the argparse group is exhaustive.
    return 2


def _handle_submit_entrypoint(argv: list[str]) -> int:
    import argparse

    from databricks_job_runner.errors import RunnerError

    parser = argparse.ArgumentParser(prog="dbxcarta submit-entrypoint")
    parser.add_argument("entrypoint", choices=("ingest", "client"))
    parser.add_argument("--compute", choices=("cluster", "serverless"), default=None)
    parser.add_argument("--no-wait", action="store_true")
    args = parser.parse_args(argv)

    try:
        _submit_bootstrap_entrypoint(
            args.entrypoint,
            compute_mode=args.compute,
            no_wait=args.no_wait,
        )
    except RunnerError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    return 0


def _submit_bootstrap_entrypoint(
    name: str,
    *,
    compute_mode: str | None,
    no_wait: bool,
) -> None:
    from databricks_job_runner import BootstrapConfig
    from databricks_job_runner.errors import RunnerError
    from databricks_job_runner.upload import stable_wheel_name

    wheel_package = _ENTRYPOINT_WHEEL_PACKAGE.get(name)
    if wheel_package is None:
        raise RunnerError(f"unknown wheel entrypoint {name!r}")

    # Ingest is gated on the Neo4j connector via the dedicated runner's
    # maven preflight; the client and generic paths do not need it.
    submit_runner = _ingest_runner() if name == "ingest" else runner

    # Runner._compute is private but is the only accessor for the resolved
    # Compute strategy; there is no public equivalent. Pinned to
    # databricks-job-runner==0.6.1 in the closures above, so the surface is
    # stable for this code's lifetime.
    if name == "ingest" and _is_serverless_compute(
        submit_runner._compute(compute_mode)
    ):
        raise RunnerError(
            "dbxcarta ingest uses the Neo4j Spark Connector, which is not "
            "supported on Databricks serverless jobs compute. Use classic "
            "compute with `--compute cluster`."
        )

    wheel_volume_path = (
        f"{submit_runner.wheel_volume_dir}/{stable_wheel_name(wheel_package)}"
    )

    bootstrap = BootstrapConfig(
        wheel_volume_path=wheel_volume_path,
        pinned_closure=list(_ENTRYPOINT_PINNED_CLOSURE[name]),
        wheel_package=wheel_package,
        console_script=_ENTRYPOINT_CONSOLE_SCRIPT[name],
        jvm_probe_class=_ENTRYPOINT_JVM_PROBE_CLASS[name],
        smoke_imports=list(_ENTRYPOINT_SMOKE_IMPORTS[name]),
    )

    submit_runner.submit_bootstrap(
        bootstrap,
        run_name_suffix=name,
        no_wait=no_wait,
        compute_mode=compute_mode,
    )


def _is_serverless_compute(compute: Compute) -> bool:
    return isinstance(compute, Serverless)


def _build_workspace_client() -> WorkspaceClient:
    from dbxcarta.spark.databricks import build_workspace_client

    return build_workspace_client()


def _build_neo4j_driver(
    ws: WorkspaceClient, settings: SparkIngestSettings
) -> Driver:
    from neo4j import GraphDatabase

    from dbxcarta.spark.databricks import read_workspace_secret

    scope = settings.databricks_secret_scope
    return GraphDatabase.driver(
        read_workspace_secret(ws, scope, "NEO4J_URI"),
        auth=(
            read_workspace_secret(ws, scope, "NEO4J_USERNAME"),
            read_workspace_secret(ws, scope, "NEO4J_PASSWORD"),
        ),
    )
