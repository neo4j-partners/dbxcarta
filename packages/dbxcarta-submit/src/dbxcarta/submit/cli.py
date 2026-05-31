from __future__ import annotations

import sys
from typing import TypedDict

from dbxcarta.spark.env import select_overlay_path

from databricks_job_runner import (
    Compute,
    DesiredLibrary,
    Runner,
    Serverless,
    maven_libraries_preflight,
)

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
    "databricks-job-runner==0.6.2",
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
    "cli_command": "uv run dbxcarta-submit",
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
        env_file=runner.env_file,
    )


def main() -> None:
    """Entry point for the operator submission CLI.

    - `dbxcarta-submit submit-entrypoint {ingest|client}` submits the wheel
      entrypoint.
    - `dbxcarta-submit upload --wheel` publishes the stable per-package
      wheels and ships the bootstrap script.
    - All other invocations dispatch to databricks_job_runner.Runner
      (submit, validate, logs, clean).
    """
    overlay = select_overlay_path()
    runner.env_file = overlay
    if overlay is not None:
        # Path only, never resolved values, so no secret reaches logs.
        print(f"dbxcarta-submit: active env overlay: {overlay}", file=sys.stderr)

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


def _handle_submit_entrypoint(argv: list[str]) -> int:
    import argparse

    from databricks_job_runner.errors import RunnerError

    parser = argparse.ArgumentParser(prog="dbxcarta-submit submit-entrypoint")
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
    # databricks-job-runner==0.6.2 in the closures above, so the surface is
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
