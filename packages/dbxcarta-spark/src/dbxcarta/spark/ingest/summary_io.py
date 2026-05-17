"""Run-summary I/O: stdout, JSON volume file, Delta table, and volume reader.

The wire shape itself lives on the `RunSummary` DTO (`to_dict` /
`to_json_dict`); this module only moves that shape across process boundaries
(emitters) and reads previously-emitted JSON summaries back from a UC Volume
(`load_summary_from_volume`).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

from dbxcarta.spark.databricks import quote_qualified_name
from dbxcarta.spark.ingest.summary import RunSummary

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient
    from pyspark.sql import SparkSession


class LoadSummaryError(Exception):
    """Raised when a `--run-id` selector matches multiple summary files in the
    volume. Caller is expected to disambiguate (typically by deleting duplicates
    or passing a more specific id).
    """


def load_summary_from_volume(
    ws: "WorkspaceClient",
    volume_path: str,
    *,
    run_id: str | None = None,
) -> dict | None:
    """Load a run summary JSON from a UC Volume (the file emitted by `emit_json`).

    `emit_json` writes `{job_name}_{run_id}_{ts}.json` files. With `run_id`
    set, filter for that token; otherwise scan newest-first by filename and
    return the first summary with `status == 'success'`.

    Returns the parsed dict, or `None` if no matching file exists. Raises
    `LoadSummaryError` if `run_id` matches multiple files (ambiguity).
    """
    entries = list(ws.files.list_directory_contents(directory_path=volume_path))
    candidates = [
        e for e in entries
        if e.name is not None
        and e.name.startswith("dbxcarta_")
        and e.name.endswith(".json")
    ]
    if not candidates:
        return None

    if run_id:
        matched = [
            e for e in candidates
            if e.name is not None
            and e.name.startswith(f"dbxcarta_{run_id}_")
        ]
        if not matched:
            return None
        if len(matched) > 1:
            names = sorted(e.name for e in matched if e.name is not None)
            raise LoadSummaryError(
                f"--run-id={run_id!r} matched {len(matched)} files in {volume_path}: "
                f"{names}. Disambiguate by removing duplicates or pass a more specific value."
            )
        target = matched[0]
        downloaded = ws.files.download(file_path=f"{volume_path}/{target.name}")
        if downloaded.contents is None:
            raise LoadSummaryError(f"summary file has no contents: {target.name}")
        content = downloaded.contents.read()
        return cast(dict[str, Any], json.loads(content))

    for entry in sorted(candidates, key=lambda e: e.name or "", reverse=True):
        downloaded = ws.files.download(file_path=f"{volume_path}/{entry.name}")
        if downloaded.contents is None:
            raise LoadSummaryError(f"summary file has no contents: {entry.name}")
        content = downloaded.contents.read()
        loaded = cast(dict[str, Any], json.loads(content))
        if loaded.get("status") == "success":
            return loaded
    return None


def _mkdirs(dirpath: Path) -> None:
    """Create local or UC Volume subdirectories needed for JSON summaries.

    UC Volumes expose a FUSE path whose root must be provisioned by Databricks,
    so this helper only creates subdirectories below the volume root.
    """
    parts = dirpath.parts
    if len(parts) > 1 and parts[1] == "Volumes":
        # UC Volume FUSE mount: creating the volume root (/Volumes/cat/schema/vol)
        # is handled by CREATE VOLUME in preflight and returns errno 95 if
        # attempted via os.mkdir. Only create subdirectories beyond the volume
        # root (depth > 5).
        for depth in range(6, len(parts) + 1):
            Path(*parts[:depth]).mkdir(exist_ok=True)
    else:
        dirpath.mkdir(parents=True, exist_ok=True)


def emit_stdout(summary: RunSummary) -> None:
    """Print a compact human-readable summary to driver stdout."""
    print(
        f"[dbxcarta] run_id={summary.run_id} job={summary.job_name} "
        f"status={summary.status} catalog={summary.catalog}"
    )
    for stage, count in summary.to_dict()["row_counts"].items():
        print(f"  {stage}: {count}")
    for label, count in summary.neo4j_counts.items():
        print(f"  neo4j/{label}: {count}")
    if summary.verify is not None:
        n = len(summary.verify.violations)
        print(f"  verify: ok={summary.verify.ok} violations={n}")
        for v in summary.verify.violations:
            print(f"    [{v['code']}] {v['message']}")
    if summary.error:
        print(f"  error: {summary.error}")


def emit_json(summary: RunSummary, volume_path: str) -> None:
    """Write the JSON summary file under the configured UC Volume path."""
    ts = (summary.ended_at or summary.started_at).strftime("%Y%m%dT%H%M%SZ")
    path = Path(volume_path) / f"{summary.job_name}_{summary.run_id}_{ts}.json"
    _mkdirs(path.parent)
    path.write_text(json.dumps(summary.to_json_dict(), indent=2))


def emit_delta(summary: RunSummary, spark: "SparkSession", table_name: str) -> None:
    """Append one summary row to the configured Delta history table.

    The explicit schema keeps Spark from inferring unstable map and
    timestamp types from a single Python Row.
    """
    from pyspark.sql import Row
    from pyspark.sql.types import (
        ArrayType,
        BooleanType,
        DoubleType,
        LongType,
        MapType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    schema = StructType([
        StructField("run_id", StringType(), nullable=False),
        StructField("job_name", StringType()),
        StructField("contract_version", StringType()),
        StructField("catalog", StringType()),
        StructField("schemas", ArrayType(StringType())),
        StructField("started_at", TimestampType()),
        StructField("ended_at", TimestampType()),
        StructField("status", StringType()),
        StructField("row_counts", MapType(StringType(), LongType())),
        StructField("neo4j_counts", MapType(StringType(), LongType())),
        StructField("error", StringType()),
        StructField("embedding_model", StringType()),
        StructField("embedding_flags", MapType(StringType(), BooleanType())),
        StructField("embedding_attempts", MapType(StringType(), LongType())),
        StructField("embedding_successes", MapType(StringType(), LongType())),
        StructField("embedding_failure_rate_per_label", MapType(StringType(), DoubleType())),
        StructField("embedding_failure_rate", DoubleType()),
        StructField("embedding_failure_threshold", DoubleType()),
        StructField("embedding_ledger_hits", MapType(StringType(), LongType())),
        StructField("verify_ok", BooleanType()),
        StructField("verify_violation_count", LongType()),
    ])

    quoted_table = quote_qualified_name(table_name, expected_parts=3)
    row = Row(**summary.to_dict())
    (
        spark.createDataFrame([row], schema=schema)
        .write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(quoted_table)
    )


def emit(
    summary: RunSummary, spark: "SparkSession", volume_path: str, table_name: str
) -> None:
    """Emit the run summary through all configured sinks."""
    emit_stdout(summary)
    emit_json(summary, volume_path)
    emit_delta(summary, spark, table_name)
