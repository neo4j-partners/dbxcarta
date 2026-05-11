"""UC Volume path resolution and Delta staging-root lifecycle.

Everything in this module is pure path arithmetic or Delta/dbutils I/O —
no Neo4j driver state and no Unity Catalog metadata query. The staging root
holds materialized embedding results for the current run only; the ledger root
is the durable cross-run cache.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from dbxcarta.contract import NodeLabel
from dbxcarta.databricks import uc_volume_parent, uc_volume_parts

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

    from dbxcarta.settings import Settings

logger = logging.getLogger(__name__)


def parse_volume_path(path: str) -> list[str]:
    """Validate a /Volumes path and return its parts (after lstrip).

    Requires at least /Volumes/<cat>/<schema>/<vol>/<subdir> (5 segments).
    Raises RuntimeError for bare volume roots so neither preflight nor
    resolve_staging_path silently accepts a path that will fail at runtime.
    """
    try:
        return uc_volume_parts(path)
    except ValueError as exc:
        raise RuntimeError(
            f"[dbxcarta] volume path must be at least"
            f" /Volumes/<cat>/<schema>/<vol>/<subdir>, got {path!r}."
            f" Set DBXCARTA_SUMMARY_VOLUME to a path with a subdirectory"
            f" under the volume root (e.g. /Volumes/cat/schema/vol/dbxcarta)."
        ) from exc


def resolve_staging_path(settings: "Settings") -> str:
    """Return the Delta staging root.

    If DBXCARTA_STAGING_PATH is set, use it verbatim. Otherwise derive a sibling
    "staging" directory under the same UC volume as DBXCARTA_SUMMARY_VOLUME by
    stripping the summary subdir.
    """
    configured = settings.dbxcarta_staging_path.strip()
    if configured:
        return configured.rstrip("/")
    summary_root = settings.dbxcarta_summary_volume.rstrip("/")
    return f"{uc_volume_parent(summary_root)}/staging"


def resolve_ledger_path(settings: "Settings") -> str:
    """Return the Delta ledger root.

    Sibling "ledger" directory under the same UC volume as the summary volume
    when unset. The ledger is never truncated between runs — it persists as a
    durable cache.
    """
    configured = settings.dbxcarta_ledger_path.strip()
    if configured:
        return configured.rstrip("/")
    summary_root = settings.dbxcarta_summary_volume.rstrip("/")
    return f"{uc_volume_parent(summary_root)}/ledger"


def truncate_staging_root(staging_root: str) -> None:
    """Remove the staging root so each run starts clean.

    Overwrite-per-label would leave orphan subdirs when a label's embedding
    flag flips off between runs; truncating the whole root once per run keeps
    the staging volume faithful to the current config.

    Probe-first design: check existence via `dbutils.fs.ls` and narrow-catch
    only on the probe. Any failure from the destructive `rm` call propagates
    as signal. Different Databricks runtimes wrap a missing path differently,
    so the probe treats only known missing-path messages as non-existence.
    """
    from databricks.sdk.runtime import dbutils
    from py4j.protocol import Py4JJavaError

    try:
        dbutils.fs.ls(staging_root)
        exists = True
    except Py4JJavaError as exc:
        if not _is_missing_path_error(exc):
            raise
        exists = False
    except Exception as exc:
        if not _is_missing_path_error(exc):
            raise
        exists = False

    if exists:
        dbutils.fs.rm(staging_root, recurse=True)
        logger.info("[dbxcarta] truncated staging root %s", staging_root)
    else:
        logger.info("[dbxcarta] staging root %s not present", staging_root)


def _is_missing_path_error(exc: BaseException) -> bool:
    """Return True when dbutils.fs.ls failed because the path is absent."""
    text = str(exc)
    return (
        "FileNotFoundException" in text
        or "No such file or directory" in text
        or "java.io.FileNotFoundException" in text
    )


def stage_embedded_nodes(df: "DataFrame", staging_root: str, label: NodeLabel) -> "DataFrame":
    """Write df to <staging_root>/<label>_nodes as Delta and read back.

    overwriteSchema is enabled so future column additions don't require a
    manual drop of the staging table. The returned DataFrame is a fresh read
    off the staging table, so the failure-rate aggregation and the Neo4j write
    do not re-invoke ai_query.
    """
    out = f"{staging_root.rstrip('/')}/{label.value.lower()}_nodes"
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(out)
    )
    logger.info("[dbxcarta] staged %s nodes at %s", label.value, out)
    return df.sparkSession.read.format("delta").load(out)
