"""Public library surface for submitting a neocarta ingest job.

neocarta installs ``dbxcarta-submit`` and calls :func:`submit_neocarta_ingest`
to stage its prebuilt connector wheel and submit the ingest job, without driving
the full operator CLI and without the materialize build that ``dbxcarta
publish-wheels`` performs. That build needs the dbxcarta source tree, which a
pip-installed package does not have; this path never touches it.

Only the per-call inputs are parameters. The catalog, Volume, profile, and Neo4j
secrets come from the selected overlay exactly as the CLI resolves them, so this
helper adds no ``os.environ`` reads of its own.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from dbxcarta.core.env import select_overlay_path
from dbxcarta.submit.cli import (
    _ENTRYPOINT_WHEEL_PACKAGE,
    _publish_prebuilt_wheel,
    _submit_bootstrap_entrypoint,
    runner,
)

if TYPE_CHECKING:
    from pathlib import Path


def submit_neocarta_ingest(
    neocarta_wheel: Path,
    *,
    compute_mode: str = "cluster",
    no_wait: bool = False,
) -> None:
    """Stage a prebuilt neocarta connector wheel and submit the ingest job.

    Mirrors ``dbxcarta submit-entrypoint ingest`` as a library call: it stages
    the neocarta wheel onto the ops Volume, ships the runner bootstrap script,
    then submits the ingest entry point. It does not build or stage the
    materialize wheel.

    The ingest job uses the Neo4j Spark Connector, a JVM cluster library, so it
    must run on classic compute. ``compute_mode`` defaults to ``"cluster"`` for
    that reason; ``_submit_bootstrap_entrypoint`` rejects serverless for ingest.

    Args:
        neocarta_wheel: Path to the prebuilt neocarta connector wheel. The newest
            ``neocarta`` wheel in its parent directory is staged, matching the
            operator ``NEOCARTA_WHEEL_SOURCE`` flow.
        compute_mode: ``"cluster"`` (the default and only supported mode for
            ingest) or ``"serverless"``, which is rejected for ingest.
        no_wait: Submit and return without waiting for the run to finish.

    Raises:
        FileNotFoundError: If ``neocarta_wheel`` is not an existing file.
        databricks_job_runner.errors.RunnerError: If staging or submission fails,
            including a serverless ``compute_mode`` for the ingest connector.
    """
    if not neocarta_wheel.is_file():
        raise FileNotFoundError(f"neocarta wheel not found: {neocarta_wheel}")

    # Resolve and apply the selected overlay exactly as the CLI's main() does, so
    # the runner reads the same profile, catalog, and Volume the operator path
    # uses. This is the only overlay touch; no new os.environ reads.
    runner.env_file = select_overlay_path()

    neocarta_package = _ENTRYPOINT_WHEEL_PACKAGE["ingest"]
    _publish_prebuilt_wheel(
        runner.ws,
        runner.wheel_volume_dir,
        neocarta_wheel.parent,
        neocarta_package,
    )
    # Ship the runner bootstrap script the SparkPythonTask runs. publish-wheels
    # does this via upload_all(); the library path must do it too.
    runner.upload_all()
    _submit_bootstrap_entrypoint("ingest", compute_mode=compute_mode, no_wait=no_wait)
