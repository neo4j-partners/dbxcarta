"""Run-summary emitter: stdout, JSON volume file, and Delta table."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


def _mkdirs(dirpath: Path) -> None:
    parts = dirpath.parts
    if len(parts) > 1 and parts[1] == "Volumes":
        # UC Volume FUSE mount: creating the volume root (/Volumes/cat/schema/vol)
        # is handled by CREATE VOLUME in _preflight and returns errno 95 if attempted
        # via os.mkdir.  Only create subdirectories beyond the volume root (depth > 5).
        for depth in range(6, len(parts) + 1):
            Path(*parts[:depth]).mkdir(exist_ok=True)
    else:
        dirpath.mkdir(parents=True, exist_ok=True)


@dataclass
class RunSummary:
    run_id: str
    job_name: str
    contract_version: str
    catalog: str
    schemas: list[str]
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    ended_at: datetime | None = None
    status: str = "running"
    row_counts: dict[str, int] = field(default_factory=dict)
    neo4j_counts: dict[str, int] = field(default_factory=dict)
    error: str | None = None
    # Embedding fields — None/empty when no embedding flags are enabled.
    embedding_model: str | None = None
    embedding_flags: dict[str, bool] = field(default_factory=dict)
    embedding_attempts: dict[str, int] = field(default_factory=dict)
    embedding_successes: dict[str, int] = field(default_factory=dict)
    embedding_failure_rate_per_label: dict[str, float] = field(default_factory=dict)
    embedding_failure_rate: float | None = None
    embedding_failure_threshold: float | None = None

    def finish(self, *, status: str, error: str | None = None) -> None:
        self.status = status
        self.error = error
        self.ended_at = datetime.now(timezone.utc)

    def to_dict(self) -> dict:
        """Returns dict with datetime objects — suitable for Spark Row."""
        return asdict(self)

    def to_json_dict(self) -> dict:
        """Returns dict with ISO string timestamps — suitable for JSON serialization."""
        d = asdict(self)
        d["started_at"] = self.started_at.isoformat()
        d["ended_at"] = self.ended_at.isoformat() if self.ended_at else None
        return d

    def emit_stdout(self) -> None:
        print(
            f"[dbxcarta] run_id={self.run_id} job={self.job_name} "
            f"status={self.status} catalog={self.catalog}"
        )
        for stage, count in self.row_counts.items():
            print(f"  {stage}: {count}")
        for label, count in self.neo4j_counts.items():
            print(f"  neo4j/{label}: {count}")
        if self.error:
            print(f"  error: {self.error}")

    def emit_json(self, volume_path: str) -> None:
        ts = (self.ended_at or self.started_at).strftime("%Y%m%dT%H%M%SZ")
        path = Path(volume_path) / f"{self.job_name}_{self.run_id}_{ts}.json"
        _mkdirs(path.parent)
        path.write_text(json.dumps(self.to_json_dict(), indent=2))

    def emit_delta(self, spark: "SparkSession", table_name: str) -> None:
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
        ])

        quoted_table = ".".join(f"`{p}`" for p in table_name.split("."))
        row = Row(**self.to_dict())
        (
            spark.createDataFrame([row], schema=schema)
            .write.format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .saveAsTable(quoted_table)
        )

    def emit(self, spark: "SparkSession", volume_path: str, table_name: str) -> None:
        self.emit_stdout()
        self.emit_json(volume_path)
        self.emit_delta(spark, table_name)
