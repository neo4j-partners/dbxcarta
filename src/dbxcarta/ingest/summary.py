"""Run-summary emitter: stdout, JSON volume file, and Delta table.

The in-memory shape is nominal — `ExtractCounts`, `EmbeddingCounts`,
`SampleValueCounts`, `InferenceCounters`, `SemanticInferenceCounters` — so
that no pipeline site pokes stringly-typed keys into a dict-bag. The flat
`row_counts: dict[str, int]` and `embedding_*: dict[str, int/float]` shape
still appears on the wire (JSON + Delta), but only at the `to_dict()`
serialization boundary. Delta DDL is unchanged; JSON contract is unchanged.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

from dbxcarta.contract import NodeLabel
from dbxcarta.databricks import quote_qualified_name

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient
    from pyspark.sql import SparkSession

    from dbxcarta.ingest.fk.declared import DeclaredCounters
    from dbxcarta.ingest.fk.metadata import InferenceCounters
    from dbxcarta.ingest.fk.semantic import SemanticInferenceCounters
    from dbxcarta.ingest.transform.sample_values import SampleStats


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
        if e.name.startswith("dbxcarta_") and e.name.endswith(".json")
    ]
    if not candidates:
        return None

    if run_id:
        matched = [e for e in candidates if e.name.startswith(f"dbxcarta_{run_id}_")]
        if not matched:
            return None
        if len(matched) > 1:
            names = sorted(e.name for e in matched)
            raise LoadSummaryError(
                f"--run-id={run_id!r} matched {len(matched)} files in {volume_path}: "
                f"{names}. Disambiguate by removing duplicates or pass a more specific value."
            )
        target = matched[0]
        content = ws.files.download(file_path=f"{volume_path}/{target.name}").contents.read()
        return json.loads(content)

    for entry in sorted(candidates, key=lambda e: e.name, reverse=True):
        content = ws.files.download(file_path=f"{volume_path}/{entry.name}").contents.read()
        loaded = json.loads(content)
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


@dataclass
class ExtractCounts:
    """Counts captured immediately after Unity Catalog extraction."""

    schemas: int = 0
    tables: int = 0
    columns: int = 0

    def as_row_counts(self) -> dict[str, int]:
        """Flatten extraction counters into the shared row_counts map."""
        return {
            "schemas": self.schemas,
            "tables": self.tables,
            "columns": self.columns,
        }


@dataclass
class SampleValueCounts:
    """Mirrors sample_values.SampleStats but lives on RunSummary.

    Captured from SampleStats via `from_sample_stats`; flat serialization to
    `row_counts` happens in `as_row_counts`. Optional fields (percentiles)
    are emitted only when non-None — matches the legacy pipeline behaviour
    where the percentile keys were skipped if unset."""

    candidate_columns: int = 0
    sampled_columns: int = 0
    skipped_columns: int = 0
    skipped_schemas: int = 0
    cardinality_failed_tables: int = 0
    cardinality_wall_clock_ms: int = 0
    sample_wall_clock_ms: int = 0
    value_nodes: int = 0
    has_value_edges: int = 0
    cardinality_min: int | None = None
    cardinality_p25: int | None = None
    cardinality_p50: int | None = None
    cardinality_p75: int | None = None
    cardinality_p95: int | None = None
    cardinality_max: int | None = None

    @classmethod
    def from_sample_stats(cls, stats: "SampleStats") -> "SampleValueCounts":
        """Copy Spark sample statistics into the run-summary DTO.

        Keeping this conversion here prevents the pipeline from depending on
        the summary's flattened wire keys.
        """
        return cls(
            candidate_columns=stats.candidate_columns,
            sampled_columns=stats.sampled_columns,
            skipped_columns=stats.skipped_columns,
            skipped_schemas=stats.skipped_schemas,
            cardinality_failed_tables=stats.cardinality_failed_tables,
            cardinality_wall_clock_ms=stats.cardinality_wall_clock_ms,
            sample_wall_clock_ms=stats.sample_wall_clock_ms,
            value_nodes=stats.value_nodes,
            has_value_edges=stats.has_value_edges,
            cardinality_min=stats.cardinality_min,
            cardinality_p25=stats.cardinality_p25,
            cardinality_p50=stats.cardinality_p50,
            cardinality_p75=stats.cardinality_p75,
            cardinality_p95=stats.cardinality_p95,
            cardinality_max=stats.cardinality_max,
        )

    def as_row_counts(self) -> dict[str, int]:
        """Flatten sample-value counters into the shared row_counts map.

        Percentile keys are included only when the sampler computed them, which
        keeps empty or disabled sampling runs from emitting misleading zeroes.
        """
        out: dict[str, int] = {
            "candidate_columns": self.candidate_columns,
            "sampled_columns": self.sampled_columns,
            "skipped_columns": self.skipped_columns,
            "skipped_schemas": self.skipped_schemas,
            "cardinality_failed_tables": self.cardinality_failed_tables,
            "cardinality_wall_clock_ms": self.cardinality_wall_clock_ms,
            "sample_wall_clock_ms": self.sample_wall_clock_ms,
            "value_nodes": self.value_nodes,
            "has_value_edges": self.has_value_edges,
        }
        for name in ("cardinality_min", "cardinality_p25", "cardinality_p50",
                     "cardinality_p75", "cardinality_p95", "cardinality_max"):
            val = getattr(self, name)
            if val is not None:
                out[name] = val
        return out


@dataclass
class EmbeddingCounts:
    """Per-label embedding bookkeeping.

    Keyed by `NodeLabel` enum in memory; serialization to the Delta/JSON
    wire flattens to `dict[str, ...]` using `label.value` via `as_*_map`."""

    model: str | None = None
    failure_threshold: float | None = None
    flags: dict[NodeLabel, bool] = field(default_factory=dict)
    attempts: dict[NodeLabel, int] = field(default_factory=dict)
    successes: dict[NodeLabel, int] = field(default_factory=dict)
    failure_rate_per_label: dict[NodeLabel, float] = field(default_factory=dict)
    ledger_hits: dict[NodeLabel, int] = field(default_factory=dict)
    aggregate_failure_rate: float | None = None

    def as_flags_map(self) -> dict[str, bool]:
        """Return embedding enablement keyed by public node-label strings."""
        return {k.value: v for k, v in self.flags.items()}

    def as_attempts_map(self) -> dict[str, int]:
        """Return embedding attempt counts keyed by public node-label strings."""
        return {k.value: v for k, v in self.attempts.items()}

    def as_successes_map(self) -> dict[str, int]:
        """Return embedding success counts keyed by public node-label strings."""
        return {k.value: v for k, v in self.successes.items()}

    def as_failure_rate_map(self) -> dict[str, float]:
        """Return per-label embedding failure rates keyed by public labels."""
        return {k.value: v for k, v in self.failure_rate_per_label.items()}

    def as_ledger_hits_map(self) -> dict[str, int]:
        """Return per-label ledger hit counts keyed by public labels."""
        return {k.value: v for k, v in self.ledger_hits.items()}


@dataclass
class VerifyResult:
    """Verify outcome captured into the RunSummary.

    `ok=False` means at least one violation. `violations` is the full list
    surfaced through the JSON output for auditability without driver logs.
    Delta only retains the flat `verify_ok`/`verify_violation_count` summary
    columns to keep the table schema simple.
    """

    ok: bool
    violations: list[dict[str, str]]  # [{"code": str, "message": str}, ...]


@dataclass
class RunSummary:
    """Run-level aggregate.

    In-memory: nominal sub-dataclasses for each counter group.
    On the wire: flat `row_counts` dict + flat `embedding_*` maps. The
    `to_dict` / `to_json_dict` methods are the sole place where enum keys
    become strings and sub-dataclasses flatten into the legacy wire shape.
    """

    run_id: str
    job_name: str
    contract_version: str
    catalog: str
    schemas: list[str]
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    ended_at: datetime | None = None
    status: str = "running"
    error: str | None = None
    extract: ExtractCounts = field(default_factory=ExtractCounts)
    fk_declared: "DeclaredCounters | None" = None
    fk_metadata: "InferenceCounters | None" = None
    fk_semantic: "SemanticInferenceCounters | None" = None
    sample_values: SampleValueCounts | None = None
    embeddings: EmbeddingCounts = field(default_factory=EmbeddingCounts)
    neo4j_counts: dict[str, int] = field(default_factory=dict)
    verify: VerifyResult | None = None

    def finish(self, *, status: str, error: str | None = None) -> None:
        """Mark the run terminal and stamp its end time.

        The pipeline calls this exactly once on the success or failure path
        before emitters serialize the summary.
        """
        self.status = status
        self.error = error
        self.ended_at = datetime.now(timezone.utc)

    def _build_row_counts(self) -> dict[str, int]:
        """Flatten all nominal counter groups into the legacy `row_counts` shape."""
        out: dict[str, int] = {}
        out.update(self.extract.as_row_counts())
        if self.fk_declared is not None:
            out.update(self.fk_declared.as_row_counts())
        if self.fk_metadata is not None:
            out.update(self.fk_metadata.as_summary_dict("fk_inferred_metadata"))
        if self.fk_semantic is not None:
            out.update(self.fk_semantic.as_summary_dict("fk_inferred_semantic"))
        if self.sample_values is not None:
            out.update(self.sample_values.as_row_counts())
        return out

    def to_dict(self) -> dict[str, Any]:
        """Returns dict with datetime objects — suitable for Spark Row.

        Serialization shape is the legacy flat contract: `row_counts` is a
        single dict[str, int], embedding fields are flat `dict[str, ...]`.
        """
        return {
            "run_id": self.run_id,
            "job_name": self.job_name,
            "contract_version": self.contract_version,
            "catalog": self.catalog,
            "schemas": self.schemas,
            "started_at": self.started_at,
            "ended_at": self.ended_at,
            "status": self.status,
            "row_counts": self._build_row_counts(),
            "neo4j_counts": dict(self.neo4j_counts),
            "error": self.error,
            "embedding_model": self.embeddings.model,
            "embedding_flags": self.embeddings.as_flags_map(),
            "embedding_attempts": self.embeddings.as_attempts_map(),
            "embedding_successes": self.embeddings.as_successes_map(),
            "embedding_failure_rate_per_label": self.embeddings.as_failure_rate_map(),
            "embedding_failure_rate": self.embeddings.aggregate_failure_rate,
            "embedding_failure_threshold": self.embeddings.failure_threshold,
            "embedding_ledger_hits": self.embeddings.as_ledger_hits_map(),
            # Flat verify summary — kept flat so the Delta schema doesn't need
            # a nested struct. The JSON output replaces these with a richer
            # nested `verify` object that includes the violation list.
            "verify_ok": None if self.verify is None else self.verify.ok,
            "verify_violation_count": (
                0 if self.verify is None else len(self.verify.violations)
            ),
        }

    def to_json_dict(self) -> dict[str, Any]:
        """Returns dict with ISO string timestamps — suitable for JSON serialization."""
        d = self.to_dict()
        d["started_at"] = self.started_at.isoformat()
        d["ended_at"] = self.ended_at.isoformat() if self.ended_at else None
        if self.verify is not None:
            d.pop("verify_ok", None)
            d.pop("verify_violation_count", None)
            d["verify"] = {
                "ok": self.verify.ok,
                "violation_count": len(self.verify.violations),
                "violations": self.verify.violations,
            }
        return d

    def emit_stdout(self) -> None:
        """Print a compact human-readable summary to driver stdout."""
        print(
            f"[dbxcarta] run_id={self.run_id} job={self.job_name} "
            f"status={self.status} catalog={self.catalog}"
        )
        for stage, count in self._build_row_counts().items():
            print(f"  {stage}: {count}")
        for label, count in self.neo4j_counts.items():
            print(f"  neo4j/{label}: {count}")
        if self.verify is not None:
            n = len(self.verify.violations)
            print(f"  verify: ok={self.verify.ok} violations={n}")
            for v in self.verify.violations:
                print(f"    [{v['code']}] {v['message']}")
        if self.error:
            print(f"  error: {self.error}")

    def emit_json(self, volume_path: str) -> None:
        """Write the JSON summary file under the configured UC Volume path."""
        ts = (self.ended_at or self.started_at).strftime("%Y%m%dT%H%M%SZ")
        path = Path(volume_path) / f"{self.job_name}_{self.run_id}_{ts}.json"
        _mkdirs(path.parent)
        path.write_text(json.dumps(self.to_json_dict(), indent=2))

    def emit_delta(self, spark: "SparkSession", table_name: str) -> None:
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
        row = Row(**self.to_dict())
        (
            spark.createDataFrame([row], schema=schema)
            .write.format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .saveAsTable(quoted_table)
        )

    def emit(self, spark: "SparkSession", volume_path: str, table_name: str) -> None:
        """Emit the run summary through all configured sinks."""
        self.emit_stdout()
        self.emit_json(volume_path)
        self.emit_delta(spark, table_name)
