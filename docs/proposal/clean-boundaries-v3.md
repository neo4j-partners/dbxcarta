# Clean Boundaries v3: Schema-Carrying Ports

This proposal supersedes `clean-boundaries-v2.md`. v2 defined a strict
`SemanticLayerInput(source_ref, scope)` and proposed that the builder return
a `SemanticLayerResult` value. That shape collapsed the contract to a stringly
typed source identifier and forced the writer to consume a materialized
Python value. Both choices are wrong for the real use cases.

v3 promotes the **graph schema itself** to a first-class core artifact, defines
carrier-neutral schema descriptors (kinds) in core, and resolves the
distributed-pipeline performance constraint with twin writer ports.

## Goals

1. The shape of the semantic layer in Neo4j is a versioned contract owned by
   `dbxcarta-core`. Adapters cannot deviate.
2. Adapters keep their native data carriers. The Spark pipeline writes
   DataFrames through the Neo4j Spark Connector with no driver-side
   materialization. A future bolt-driver pipeline writes row batches via
   `UNWIND`. Both produce the same graph shape.
3. New backends are purely additive. A BigQuery + bolt-driver pipeline adds
   one reader adapter and one writer adapter, with zero changes to core, to
   the Spark adapter, or to the client.
4. `dbxcarta-client` queries Neo4j directly, knows the universal graph shape
   from core, and is independent of which pipeline produced the data.
5. Enforcement is structural: kinds are imported from core, mapping outputs
   are validated against kinds before any external write, and a contract
   version sentinel detects cross-pipeline drift at query time.

## Overview

Three things move into `dbxcarta-core`:

- **Kinds**: `NodeKind`, `EdgeKind`, `RelationalEntityKind`, and the
  `FieldSpec` they compose from. These describe shape without committing to a
  carrier.
- **Twin writer ports**: `SparkGraphWriter` accepts `(NodeKind, DataFrame)`
  and `(EdgeKind, DataFrame)`. `RowGraphWriter` accepts `(NodeKind, Iterable[Mapping])`
  and `(EdgeKind, Iterable[Mapping])`. Both protocols live in core; pyspark
  is referenced only under `TYPE_CHECKING`.
- **Application service**: `SemanticLayerService.build(reader, writer, spec)`
  orchestrates read, transform, validate, write. The service is generic over
  the carrier; the reader and writer agree on it.

Everything else stays in adapters: UC reader, sample-value sampler, AI Query
embedder, Neo4j Spark writer, and the BigQuery + bolt-driver pair when it
lands.

## Performance constraint: DataFrame-native, end to end

The Spark pipeline today reads tens of thousands of columns from
`information_schema` on the largest customer catalogs. Per column it samples
values, generates a 1024-dimension embedding via `ai_query`, and writes
everything to Neo4j via the Spark Connector. A single run can produce tens
of millions of `Value` rows, each carrying an `array<double>` of 1024
floats. Total data volume on large catalogs is in the tens of gigabytes.

Every step of that pipeline must stay distributed. Once Spark has the data
laid out across executors, the work happens in parallel on the cluster:
`information_schema` joins, `df.sample()` calls, `ai_query` UDF calls, and
the Neo4j Spark Connector's batched writes all execute on workers. The
driver coordinates the plan but never holds row data. Cluster size, not
driver memory, sets throughput.

A single `collect()` anywhere in this chain breaks the pipeline. Pulling
ten million `Value` rows with 1024-float embeddings to the driver costs
roughly 80 GB of heap to materialize as Python objects, serializes the
cluster's parallel work onto one node, forces a second pass when the
writer fans the rows back out to executors, and stretches run time from
minutes to hours. On the largest catalogs the job stops fitting on any
driver size and simply fails.

This is the principal reason v2 was wrong. A port that accepts a
`GraphSchema` Python value cannot avoid materializing every node and
every edge on the driver before the writer sees it. The contract becomes
incompatible with the only production deployment that exists today.

v3 makes the no-collect property a structural guarantee:

- The Spark writer's input is a `DataFrame`, a lazy plan that has not
  been evaluated. It is not a list, a tuple, or a dataclass.
- `assert_df_matches_kind(df, TABLE)` reads `df.schema`, which is
  planner metadata. It triggers no executors and moves no records.
- `df.select(*[col(f.name) for f in TABLE.fields])` is a planner-only
  projection. No execution.
- `df.write.format("org.neo4j.spark.DataSource").save()` is the only
  action in the entire pipeline. It runs distributed across executors,
  with each partition serializing Cypher batches on the executor that
  owns the partition.

The pipeline executes once, top to bottom, with `save()` as the trigger.
No second pass, no driver bottleneck, no perf regression versus the
pre-v3 baseline. Catalogs ten times larger than today's continue to
scale linearly with cluster size.

This constraint is what motivates every other choice in v3:

- Carrier-neutral kinds in core, never node-value classes. Value classes
  would force materialization at the port boundary.
- Twin writer ports instead of a single port. A single port forces the
  carrier to be either a Python value (breaks Spark) or a
  `pyspark.sql.DataFrame` (breaks the bolt pipeline and pollutes core).
- Schema enforcement via `df.schema` comparison, not via row-by-row
  validation. Row-by-row would trigger execution.
- Mapping execution in the adapter, not in core. Core cannot import
  `pyspark`, so a core-side mapping over DataFrames is impossible by
  construction. The adapter stays responsible for the carrier.

Any future architecture change that would require a `collect()`,
`toPandas()`, or per-row Python iteration on the main data path is
rejected. Such needs are instead expressed as a new kind, a new mapping
variant, or a new auxiliary port. None of those materialize data.

## What v2 got wrong

Three concrete problems with v2 surfaced once the real use cases were
articulated:

1. **`source_ref: str` is not a port.** A bare string with "trust me, the
   builder knows what this means" carries no domain information. There is no
   type-level guarantee that the adapter and the caller agree on what the
   string means. Ports should speak the domain.
2. **A materialized `SemanticLayerResult` value forces driver collect.**
   Wrapping every Table, Column, Value, and embedding row in a Python
   dataclass kills the Spark pipeline. The current pipeline keeps data
   lazy from `information_schema` query through `ai_query` enrichment to
   Neo4j Spark Connector write. The proposed value type would require a
   `collect()` somewhere in the middle.
3. **Behavioral flags vs. shape variants.** v2 hesitated on whether
   `include_sample_values` and `infer_foreign_keys` should be in the input.
   The honest answer is neither: they are choices about *which mapping
   produces the GraphSchema*, not flags on a uniform input.

## What to avoid

- Backend-shaped vocabulary in core. No `pyspark.sql.DataFrame` at runtime,
  no `neo4j.GraphDatabase`, no `google.cloud.bigquery`. `TYPE_CHECKING` imports
  are allowed because they do not load anything.
- Single-port purity that forces materialization. Twin ports for two carriers
  are not a code smell; they are an honest reflection of two transports.
- Stringly typed sources. The reader port accepts a typed `BuildSpec`, not a
  free-form identifier.
- Cross-adapter imports. Spark adapter never imports BigQuery adapter, never
  imports client. Client never imports Spark.
- Boolean flags whose effect is "what gets written." Those are *mappings*, not
  flags. New shape variants are new mapping objects in core.
- A `dbxcarta-neo4j` distribution. The Spark writer ships with
  `dbxcarta-spark`; a bolt writer ships with the pipeline that needs it.
- Schemas validated after the write. Validation runs on planner metadata
  before `.save()` so failures are cheap.

## Defined interfaces

### Field and schema descriptors

```python
# dbxcarta-core/src/dbxcarta/core/schema/kinds.py
from dataclasses import dataclass
from typing import Literal

DType = Literal[
    "string", "long", "double", "boolean",
    "array<double>", "array<string>", "timestamp",
]

@dataclass(frozen=True)
class FieldSpec:
    name: str
    dtype: DType
    nullable: bool = False
    description: str = ""

@dataclass(frozen=True)
class NodeKind:
    label: str
    key_field: str
    fields: tuple[FieldSpec, ...]

    def __post_init__(self) -> None:
        names = {f.name for f in self.fields}
        if self.key_field not in names:
            raise ValueError(
                f"NodeKind '{self.label}' key_field "
                f"'{self.key_field}' missing from fields"
            )

@dataclass(frozen=True)
class EdgeKind:
    rel_type: str
    source_label: str
    target_label: str
    source_key: str
    target_key: str
    fields: tuple[FieldSpec, ...] = ()

@dataclass(frozen=True)
class RelationalEntityKind:
    name: str
    key_field: str
    fields: tuple[FieldSpec, ...]
```

The dtype vocabulary is a small closed set; adapters translate each token
into their native type. Adding a dtype is a core change; adapters then
extend their translation table.

### Universal graph contract

```python
# dbxcarta-core/src/dbxcarta/core/schema/graph_contract.py
from dbxcarta.core.schema.kinds import NodeKind, EdgeKind, FieldSpec

DATABASE = NodeKind("Database", "name", (
    FieldSpec("name", "string"),
    FieldSpec("comment", "string", nullable=True),
))
SCHEMA = NodeKind("Schema", "fqn", (
    FieldSpec("fqn", "string"),
    FieldSpec("name", "string"),
    FieldSpec("database_name", "string"),
    FieldSpec("comment", "string", nullable=True),
))
TABLE = NodeKind("Table", "fqn", (
    FieldSpec("fqn", "string"),
    FieldSpec("name", "string"),
    FieldSpec("schema_fqn", "string"),
    FieldSpec("comment", "string", nullable=True),
    FieldSpec("embedding", "array<double>", nullable=True),
))
COLUMN = NodeKind("Column", "fqn", (
    FieldSpec("fqn", "string"),
    FieldSpec("name", "string"),
    FieldSpec("table_fqn", "string"),
    FieldSpec("dtype", "string"),
    FieldSpec("nullable", "boolean"),
    FieldSpec("comment", "string", nullable=True),
    FieldSpec("embedding", "array<double>", nullable=True),
))
VALUE = NodeKind("Value", "fqn", (
    FieldSpec("fqn", "string"),
    FieldSpec("column_fqn", "string"),
    FieldSpec("value", "string"),
    FieldSpec("embedding", "array<double>", nullable=True),
))

HAS_SCHEMA = EdgeKind("HAS_SCHEMA",
    source_label="Database", target_label="Schema",
    source_key="name", target_key="fqn")

HAS_TABLE = EdgeKind("HAS_TABLE",
    source_label="Schema", target_label="Table",
    source_key="fqn", target_key="fqn")

HAS_COLUMN = EdgeKind("HAS_COLUMN",
    source_label="Table", target_label="Column",
    source_key="fqn", target_key="fqn")

HAS_VALUE = EdgeKind("HAS_VALUE",
    source_label="Column", target_label="Value",
    source_key="fqn", target_key="fqn")

REFERENCES = EdgeKind("REFERENCES",
    source_label="Column", target_label="Column",
    source_key="fqn", target_key="fqn",
    fields=(
        FieldSpec("confidence", "double"),
        FieldSpec("source", "string"),  # "declared" | "metadata" | "semantic"
    ))

@dataclass(frozen=True)
class GraphContract:
    version: str
    nodes: tuple[NodeKind, ...]
    edges: tuple[EdgeKind, ...]

GRAPH_CONTRACT = GraphContract(
    version="1.0",
    nodes=(DATABASE, SCHEMA, TABLE, COLUMN, VALUE),
    edges=(HAS_SCHEMA, HAS_TABLE, HAS_COLUMN, HAS_VALUE, REFERENCES),
)
```

`GRAPH_CONTRACT.version` is stamped on every run and checked by the client.
Bumps to the version require coordinated client and adapter updates.

### Universal relational contract

```python
# dbxcarta-core/src/dbxcarta/core/schema/relational_contract.py

DATABASE_ENTITY = RelationalEntityKind("databases", "name", (
    FieldSpec("name", "string"),
    FieldSpec("comment", "string", nullable=True),
))
SCHEMA_ENTITY = RelationalEntityKind("schemas", "fqn", (
    FieldSpec("fqn", "string"),
    FieldSpec("name", "string"),
    FieldSpec("database_name", "string"),
    FieldSpec("comment", "string", nullable=True),
))
TABLE_ENTITY = RelationalEntityKind("tables", "fqn", (
    FieldSpec("fqn", "string"),
    FieldSpec("name", "string"),
    FieldSpec("schema_fqn", "string"),
    FieldSpec("comment", "string", nullable=True),
))
COLUMN_ENTITY = RelationalEntityKind("columns", "fqn", (
    FieldSpec("fqn", "string"),
    FieldSpec("name", "string"),
    FieldSpec("table_fqn", "string"),
    FieldSpec("dtype", "string"),
    FieldSpec("nullable", "boolean"),
    FieldSpec("comment", "string", nullable=True),
))
FOREIGN_KEY_ENTITY = RelationalEntityKind("foreign_keys", "fqn", (
    FieldSpec("fqn", "string"),
    FieldSpec("from_column_fqn", "string"),
    FieldSpec("to_column_fqn", "string"),
    FieldSpec("source", "string"),  # "declared" | "metadata" | "semantic"
))
SAMPLED_VALUE_ENTITY = RelationalEntityKind("sampled_values", "fqn", (
    FieldSpec("fqn", "string"),
    FieldSpec("column_fqn", "string"),
    FieldSpec("value", "string"),
))
```

Both UC reader and BigQuery reader produce data conforming to these
entities. The shape is identical; the query that fills them differs.

### Twin writer ports

```python
# dbxcarta-core/src/dbxcarta/core/ports/writers.py
from __future__ import annotations

from typing import TYPE_CHECKING, Iterable, Mapping, Protocol, runtime_checkable

from dbxcarta.core.schema.kinds import EdgeKind, NodeKind

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

@runtime_checkable
class SparkGraphWriter(Protocol):
    def write_nodes(
        self, kind: NodeKind, df: "DataFrame", *, run_id: str,
    ) -> int: ...
    def write_edges(
        self, kind: EdgeKind, df: "DataFrame", *, run_id: str,
    ) -> int: ...
    def write_contract_sentinel(
        self, version: str, run_id: str,
    ) -> None: ...

@runtime_checkable
class RowGraphWriter(Protocol):
    def write_nodes(
        self, kind: NodeKind,
        rows: Iterable[Mapping[str, object]],
        *, run_id: str,
    ) -> int: ...
    def write_edges(
        self, kind: EdgeKind,
        rows: Iterable[Mapping[str, object]],
        *, run_id: str,
    ) -> int: ...
    def write_contract_sentinel(
        self, version: str, run_id: str,
    ) -> None: ...
```

The `DataFrame` reference is only evaluated by static type checkers. At
runtime, `pyspark` is never imported by core. Boundary tests confirm this.

### Twin reader ports

```python
# dbxcarta-core/src/dbxcarta/core/ports/readers.py
from typing import TYPE_CHECKING, Iterable, Mapping, Protocol, runtime_checkable

from dbxcarta.core.schema.kinds import RelationalEntityKind

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

@dataclass(frozen=True)
class BuildSpec:
    source: SourceRef
    scope: tuple[str, ...] = ()         # schema names; empty = all
    options: Mapping[str, str] = field(default_factory=dict)

@runtime_checkable
class SparkRelationalReader(Protocol):
    def read(
        self, entity: RelationalEntityKind, spec: BuildSpec,
    ) -> "DataFrame": ...

@runtime_checkable
class RowRelationalReader(Protocol):
    def read(
        self, entity: RelationalEntityKind, spec: BuildSpec,
    ) -> Iterable[Mapping[str, object]]: ...
```

`SourceRef` is a discriminated union covering the source types each adapter
knows. The UC adapter narrows to `UCSourceRef(catalog: str)`. The BigQuery
adapter narrows to `BigQuerySourceRef(project: str, dataset: str)`. Core
defines the base protocol; adapters extend.

### Auxiliary ports

Sampling and embedding are separate ports because both Spark and bolt
pipelines may want to swap providers independently of read/write:

```python
# dbxcarta-core/src/dbxcarta/core/ports/enrichment.py

@runtime_checkable
class SparkValueSampler(Protocol):
    def sample(
        self, columns_df: "DataFrame", spec: BuildSpec,
    ) -> "DataFrame": ...

@runtime_checkable
class SparkEmbedder(Protocol):
    def embed(
        self, df: "DataFrame", text_field: str, output_field: str,
    ) -> "DataFrame": ...

@runtime_checkable
class RowEmbedder(Protocol):
    def embed(
        self, rows: Iterable[Mapping[str, object]],
        text_field: str, output_field: str,
    ) -> Iterable[Mapping[str, object]]: ...
```

Adapters that do not need an embedder pass a no-op implementation.

### Application service

```python
# dbxcarta-core/src/dbxcarta/core/service.py

@dataclass(frozen=True)
class BuildResult:
    contract_version: str
    run_id: str
    spec: BuildSpec
    node_counts: Mapping[str, int]
    edge_counts: Mapping[str, int]
    started_at: datetime
    ended_at: datetime

class SparkSemanticLayerService:
    """Orchestrates a DataFrame-native build."""

    def __init__(
        self,
        *,
        mapping: SparkRelationalToGraphMapping,
        contract: GraphContract = GRAPH_CONTRACT,
    ) -> None:
        self._mapping = mapping
        self._contract = contract

    def build(
        self,
        *,
        reader: SparkRelationalReader,
        writer: SparkGraphWriter,
        sampler: SparkValueSampler | None = None,
        embedder: SparkEmbedder | None = None,
        spec: BuildSpec,
    ) -> BuildResult:
        run_id = _make_run_id()
        started = utcnow()

        rel_dfs = {
            entity.name: reader.read(entity, spec)
            for entity in self._mapping.required_entities()
        }
        if sampler is not None:
            rel_dfs["sampled_values"] = sampler.sample(rel_dfs["columns"], spec)
        if embedder is not None:
            rel_dfs = self._mapping.with_embeddings(rel_dfs, embedder)

        graph_dfs = self._mapping.apply(rel_dfs)
        node_counts = {
            kind.label: writer.write_nodes(kind, graph_dfs[kind.label], run_id=run_id)
            for kind in self._contract.nodes
            if kind.label in graph_dfs
        }
        edge_counts = {
            kind.rel_type: writer.write_edges(kind, graph_dfs[kind.rel_type], run_id=run_id)
            for kind in self._contract.edges
            if kind.rel_type in graph_dfs
        }
        writer.write_contract_sentinel(self._contract.version, run_id)
        return BuildResult(
            contract_version=self._contract.version,
            run_id=run_id,
            spec=spec,
            node_counts=node_counts,
            edge_counts=edge_counts,
            started_at=started,
            ended_at=utcnow(),
        )
```

`RowSemanticLayerService` is the symmetric class for row-based pipelines.
Both services have identical shape; only the typed carriers differ.

### Mapping placement

Mappings split into two halves:

- **Declarative half (core)**: a mapping declares which `RelationalEntityKind`
  inputs it consumes and which `NodeKind` / `EdgeKind` outputs it produces.
  Core can read this declaration to know what to write and in what order.
- **Executable half (adapter)**: the actual DataFrame joins or row
  comprehensions live in the adapter. Two adapters with the same target
  contract may share zero code; what they share is the *output schema*.

```python
# dbxcarta-core/src/dbxcarta/core/mapping/base.py

@runtime_checkable
class SparkRelationalToGraphMapping(Protocol):
    def required_entities(self) -> tuple[RelationalEntityKind, ...]: ...
    def with_embeddings(
        self,
        rel_dfs: Mapping[str, "DataFrame"],
        embedder: SparkEmbedder,
    ) -> Mapping[str, "DataFrame"]: ...
    def apply(
        self, rel_dfs: Mapping[str, "DataFrame"],
    ) -> Mapping[str, "DataFrame"]: ...
```

The adapter implements `apply` such that each returned DataFrame has a
schema conformant with its target kind. The service validates that
contract before writing.

## Distribution layout

```
dbxcarta-core
    src/dbxcarta/core/
        schema/                kinds, graph_contract, relational_contract
        ports/                 readers, writers, enrichment
        mapping/               protocols + declaration types
        service.py             Spark + Row services
        contract.py            run id, types, GraphContract
        databricks.py          identifier validation (carrier-neutral text)
    Imports: stdlib, pydantic, dataclasses. No pyspark, no neo4j,
    no google-cloud-bigquery at runtime.

dbxcarta-spark
    src/dbxcarta/spark/
        readers/uc.py          UCSchemaReader: SparkRelationalReader
        sampler.py             SparkValueSampler implementation
        embedder.py            AIQueryEmbedder: SparkEmbedder
        mapping.py             SparkDefaultMapping: SparkRelationalToGraphMapping
        writers/neo4j_spark.py Neo4jSparkWriter: SparkGraphWriter
        _schema_compat.py      kind to StructType translation, df schema assert
        run.py                 thin entry point used by the wheel job
    Imports: pyspark, neo4j-spark-connector (JVM), databricks-sdk.
    Forbidden: dbxcarta.client, dbxcarta.presets, google-cloud-bigquery.

dbxcarta-client
    src/dbxcarta/client/
        ...                    existing query API
        contract_gate.py       reads (:_DbxCartaContract) sentinel on startup
    Imports: neo4j (bolt driver), dbxcarta.core.
    Forbidden: pyspark, neo4j-spark-connector, dbxcarta.spark, dbxcarta.presets,
               google-cloud-bigquery.

dbxcarta-presets
    src/dbxcarta/presets/
        cli.py                 preset --run wires reader + writer + service
        finance_genie.py       canonical compositions
    Imports: dbxcarta.core, dbxcarta.spark, dbxcarta.client.
    This distribution is the composition root and is allowed to depend on
    multiple adapters by Decision 2 of clean-boundaries.md.

dbxcarta-bigquery        (future, not built in this proposal)
    src/dbxcarta/bigquery/
        readers/bq.py          BigQuerySchemaReader: RowRelationalReader
        embedder.py            VertexEmbedder: RowEmbedder
        mapping.py             RowDefaultMapping
        writers/neo4j_bolt.py  Neo4jBoltWriter: RowGraphWriter
    Imports: google-cloud-bigquery, neo4j (bolt driver), dbxcarta.core.
    Forbidden: pyspark, neo4j-spark-connector, dbxcarta.spark.
```

No `dbxcarta-neo4j` distribution. The Spark writer ships in `dbxcarta-spark`
because it relies on the Neo4j Spark Connector, which already requires a
Spark session. The bolt writer ships in whichever distribution first needs
it, which is `dbxcarta-bigquery` when that lands.

## Enforcement model

Enforcement happens at four layers, in this order:

### 1. Kind import is the only API for shape

Adapters cannot define their own node label "Table" or edge "REFERENCES".
The label string and field list both come from `dbxcarta.core.schema.graph_contract`.
A new label requires a core change. The static import graph makes this
unambiguous.

### 2. Constructive selection from the kind

The Spark mapping projects every output DataFrame using the kind's field
list, not free-form column names:

```python
# dbxcarta-spark/src/dbxcarta/spark/mapping.py
from dbxcarta.core.schema.graph_contract import TABLE, COLUMN, REFERENCES

def _project(df: DataFrame, kind: NodeKind | EdgeKind) -> DataFrame:
    return df.select(*[col(f.name) for f in kind.fields])
```

The DataFrame schema produced by `_project` is correct by construction.

### 3. Defensive validation before write

The writer asserts the carrier schema matches the kind on planner metadata,
before any action triggers execution:

```python
# dbxcarta-spark/src/dbxcarta/spark/_schema_compat.py
from pyspark.sql.types import (ArrayType, BooleanType, DoubleType, LongType,
                               StringType, StructField, StructType, TimestampType)

_DTYPE = {
    "string":        StringType(),
    "long":          LongType(),
    "double":        DoubleType(),
    "boolean":       BooleanType(),
    "array<double>": ArrayType(DoubleType()),
    "array<string>": ArrayType(StringType()),
    "timestamp":     TimestampType(),
}

def to_struct_type(kind: NodeKind | EdgeKind) -> StructType:
    return StructType([
        StructField(f.name, _DTYPE[f.dtype], f.nullable)
        for f in kind.fields
    ])

def assert_df_matches_kind(df: DataFrame, kind: NodeKind | EdgeKind) -> None:
    expected = to_struct_type(kind)
    if df.schema != expected:
        raise GraphContractViolation(
            f"{type(kind).__name__} '{_label(kind)}' schema mismatch.\n"
            f"  expected: {expected.simpleString()}\n"
            f"  actual:   {df.schema.simpleString()}"
        )
```

Calling `df.schema` is planner-only; no records move and no executor
work is triggered. If the mismatch is caught, no external write occurs
and the run fails fast. The Spark pipeline stays fully lazy until
`writer.write_nodes(...)` triggers `.save()`. This is the property that
keeps enforcement free: a row-by-row validator would force the same
materialization that v3 is designed to avoid. The schema check is the
strongest validation that can run without breaking the no-collect
guarantee.

### 4. Runtime sentinel for cross-pipeline drift

The last write of every run upserts a singleton:

```cypher
MERGE (c:_DbxCartaContract)
SET c.version = $version,
    c.run_id = $run_id,
    c.written_at = datetime()
```

The client reads this node on first query. If the version is not in the set
the client recognizes, it raises before issuing any user query. This catches
the case where a Spark pipeline and a bolt pipeline write to the same Neo4j
instance with different contract versions.

### 5. Per-adapter conformance tests in core

Core ships a reusable pytest harness:

```python
# dbxcarta-core/src/dbxcarta/core/testing/writer_conformance.py

def assert_writer_round_trips(
    writer_factory, reader_factory, *, sample: GraphSample,
) -> None:
    """Write a known small graph and read it back via the same store."""
```

Each adapter calls `assert_writer_round_trips` in its test suite with its
own writer factory. The harness drives a tiny canonical graph (one Database,
one Schema, two Tables, three Columns, one REFERENCES edge) and asserts
shape equality on the readback. Adapters cannot merge until this passes.

## Summary of v1 and v2

v1 (already merged) established:

- Four-distribution packaging under PEP 420 namespace `dbxcarta`.
- Boundary tests for both import-time loading and source-level imports.
- Console scripts owned by the implementing distribution.
- `EnvOverlay` for `os.environ.setdefault`-style configuration composition.
- `RunSummary` from the Spark pipeline carrying neo4j counts and timing.
- `dbxcarta.core` free of pyspark, neo4j, and databricks-sdk imports.

v1 work that v3 preserves:

- Packaging and distribution layout.
- Boundary tests (extended in Phase 6).
- Console script ownership.
- `EnvOverlay`.
- Identifier validation helpers in `dbxcarta.core.databricks`.

v2 work that v3 revises:

- `SemanticLayerConfig` (the partial-config shim): still deleted.
- `SparkIngestSettings.from_semantic_config`: still deleted.
- `run_dbxcarta` XOR branch on `semantic_config`: still collapsed.
- `SemanticLayerInput` and `SemanticLayerResult` as proposed in v2: replaced
  by `BuildSpec`, kinds, and `BuildResult`. v2's `SemanticLayerBuilder`
  protocol is replaced by the twin `SparkSemanticLayerService` and
  `RowSemanticLayerService` orchestrators.

## Implementation plan

Six phases. Each phase is mergeable on its own; the final cutover is at
the end of Phase 4.

### Phase 1: Core kinds, ports, services

Files added:

- `packages/dbxcarta-core/src/dbxcarta/core/schema/kinds.py`
- `packages/dbxcarta-core/src/dbxcarta/core/schema/graph_contract.py`
- `packages/dbxcarta-core/src/dbxcarta/core/schema/relational_contract.py`
- `packages/dbxcarta-core/src/dbxcarta/core/ports/readers.py`
- `packages/dbxcarta-core/src/dbxcarta/core/ports/writers.py`
- `packages/dbxcarta-core/src/dbxcarta/core/ports/enrichment.py`
- `packages/dbxcarta-core/src/dbxcarta/core/mapping/base.py`
- `packages/dbxcarta-core/src/dbxcarta/core/service.py`
- `packages/dbxcarta-core/src/dbxcarta/core/testing/writer_conformance.py`

Files updated:

- `packages/dbxcarta-core/src/dbxcarta/core/__init__.py` adds the new exports.

Files deleted:

- `packages/dbxcarta-core/src/dbxcarta/core/builder.py` (the v2
  `SemanticLayerBuilder` protocol and `SemanticLayerResult` shim).
- `SemanticLayerConfig` and related fields from
  `packages/dbxcarta-core/src/dbxcarta/core/settings.py`.

Tests added:

- `tests/core/test_kinds.py`: validation, dtype set, key-field invariant.
- `tests/core/test_graph_contract.py`: every edge's source/target labels
  reference a defined node kind; every node's key_field is among its fields.
- `tests/core/test_relational_contract.py`: same invariants for entities.
- `tests/core/test_service.py`: orchestration test with fake reader, fake
  writer, fake mapping; asserts call order and contract sentinel call.

Boundary tests in this phase: confirm core still loads with no pyspark,
neo4j, or databricks-sdk modules.

### Phase 2: Spark adapter

Files added:

- `packages/dbxcarta-spark/src/dbxcarta/spark/readers/uc.py`
- `packages/dbxcarta-spark/src/dbxcarta/spark/sampler.py`
- `packages/dbxcarta-spark/src/dbxcarta/spark/embedder.py`
- `packages/dbxcarta-spark/src/dbxcarta/spark/mapping.py`
- `packages/dbxcarta-spark/src/dbxcarta/spark/writers/neo4j_spark.py`
- `packages/dbxcarta-spark/src/dbxcarta/spark/_schema_compat.py`

Files updated:

- `packages/dbxcarta-spark/src/dbxcarta/spark/run.py`: thin entry point that
  composes `UCSchemaReader`, `SparkDefaultMapping`, `SparkValueSampler`,
  `AIQueryEmbedder`, `Neo4jSparkWriter`, and `SparkSemanticLayerService`.
  The signature collapses to a single form, removing the v2 XOR branch.
- `packages/dbxcarta-spark/src/dbxcarta/spark/settings.py`: drop
  `from_semantic_config`. `SparkIngestSettings` now configures only the
  Spark adapter (Neo4j secret scope, summary sinks, feature flags). Source
  scope flows through `BuildSpec`.

Files deleted or moved:

- The existing `packages/dbxcarta-spark/src/dbxcarta/spark/builder.py`
  (the v2-era builder) is deleted.
- The existing `packages/dbxcarta-spark/src/dbxcarta/spark/ingest/`
  transform pipeline is reorganized into `readers/`, `sampler.py`,
  `embedder.py`, and `mapping.py`. Naming changes only where the new
  organization requires it; logic ports directly.

Tests added or updated:

- `tests/spark/test_uc_reader.py`: schema conformance with synthetic
  `information_schema` views.
- `tests/spark/test_mapping.py`: every produced DataFrame matches its
  target kind schema.
- `tests/spark/test_neo4j_spark_writer.py`: round-trip via the core
  conformance harness with an embedded Neo4j test container.
- `tests/spark/test_builder.py`: removed (replaced by the above).

### Phase 3: Client contract gate

Files added:

- `packages/dbxcarta-client/src/dbxcarta/client/contract_gate.py`: opens
  a bolt session, reads `(:_DbxCartaContract)`, asserts the version is in
  the recognized set, caches the result for the session.

Files updated:

- `packages/dbxcarta-client/src/dbxcarta/client/settings.py`: add the set
  of recognized contract versions (default: `{"1.0"}`).
- The client's entry point invokes the gate before serving the first query.

Tests added:

- `tests/client/test_contract_gate.py`: behavior on missing sentinel,
  on unknown version, on recognized version.

No other client code changes. Per the existing audit, the client does not
build semantic layers and does not consume `SemanticLayerResult`.

### Phase 4: Presets and CLI cutover

Files updated:

- `packages/dbxcarta-presets/src/dbxcarta/presets/cli.py`: `preset --run`
  composes the same set of adapter objects as `dbxcarta.spark.run.run_dbxcarta`
  and invokes the service. Output formatting uses `BuildResult`, not
  `RunSummary`.
- Spark wheel entrypoint declared in `pyproject.toml` continues to call
  `dbxcarta.spark.run.run_dbxcarta`, now collapsed to a single signature.

This phase is the cutover: at merge time, every caller produces a
`BuildResult` from a single service path. The legacy `RunSummary` is
retained internally only as long as the Spark pipeline emits it for its
own summary table; it is not exposed beyond the Spark adapter.

### Phase 5: Examples and sql-semantics

Files updated:

- `examples/finance-genie/README.md`: replace the v2 code snippet with the
  v3 service composition.
- `examples/*/...`: any other example showing programmatic use.
- `/Users/ryanknight/projects/databricks/graph-on-databricks/sql-semantics`
  programmatic demo: update to consume `BuildResult` and the v3 composition.

The `sql-semantics` repo only uses `dbxcarta.client.*` and presets, not the
service directly. Its updates are limited to documentation and CLI examples.

### Phase 6: Boundary tests, conformance harness, docs

Files updated:

- `tests/boundary/test_import_boundaries.py`: add a `dbxcarta.core` check
  that `pyspark`, `neo4j`, and `google.cloud` are absent from the runtime
  module table after `import dbxcarta.core`. Add the symmetric check for
  the bolt write path forbidden in core source files. Tighten the
  per-layer forbidden sets to include any new adapter prefixes.
- `docs/proposal/clean-boundaries.md`: cross-reference v3 as the
  successor; mark v1 sections that remain authoritative.
- `docs/proposal/clean-boundaries-v2.md`: mark superseded by v3, with a
  one-paragraph note explaining what changed and why.
- `README.md` and per-package READMEs: refresh the architecture diagram
  and the "what each distribution does" tables.

## Merge gate

A phase is mergeable when all the following hold:

1. `uv run pytest` passes with the full matrix.
2. Boundary tests pass: `dbxcarta.core` loads without pyspark, neo4j, or
   google-cloud-bigquery modules; each adapter forbids the others; client
   forbids both Spark and BigQuery adapters and the Spark Connector.
3. The per-adapter conformance test runs the core round-trip harness
   against the adapter's writer and passes.
4. The Spark pipeline's end-to-end integration test on a small synthetic
   catalog produces a graph whose node and edge counts match the v2-era
   baseline, and whose contract sentinel reads `version=1.0`.
5. No `# type: ignore` is added to satisfy `TYPE_CHECKING` imports; if a
   type checker complains, the type hint is wrong, not the suppression.

## Risks

**Risk 1: Twin services duplicate orchestration logic.**
`SparkSemanticLayerService` and `RowSemanticLayerService` share the same
shape but operate on different carriers. As long as both are small (under
100 lines each), duplication is preferable to a higher-kinded abstraction
over carriers. If a third carrier appears, revisit.

**Risk 2: Kind dtype vocabulary is too narrow.**
The closed set of `DType` values is intentional. Adding a dtype requires a
core change so that adapters can synchronize their translation tables.
Risk: a future need for a dtype core has not anticipated, for example
geospatial or struct. Mitigation: the dtype set is internal and easy to
extend; the test in Phase 1 enforces parity across the translation table.

**Risk 3: Mapping declaration drifts from execution.**
A mapping declares its required entities and target kinds. If the executable
half does not actually produce all declared outputs (or produces extras),
the service's loop over `contract.nodes` will read missing keys. Mitigation:
the conformance test in core feeds a canonical input and asserts the output
key set matches the declaration exactly.

**Risk 4: Contract version sentinel is missed on a partial write.**
If the Spark job fails after writing nodes but before writing the sentinel,
the graph is in a half-written state without a version stamp. Mitigation:
existing behavior already requires idempotency on rerun (MERGE-style
writes); the sentinel is the last write so the next successful run repairs
it. The client treats a missing sentinel as an error rather than as
permissive, so half-written state cannot be queried.

**Risk 5: Spark adapter still owns the bulk of the code.**
v3 does not reduce the size of `dbxcarta-spark`. It moves shape decisions
out and keeps execution in. The win is on the next adapter: the BigQuery
adapter starts from a contract, not from an inherited mass.

## Deferred questions

- **Bolt writer location.** Defer the `Neo4jBoltWriter` implementation
  until the BigQuery pipeline lands. The protocol (`RowGraphWriter`) is
  defined in core in Phase 1 so it is available without a placeholder
  implementation.
- **Verify rules in core or adapter.** Today's `dbxcarta.core.verify`
  module performs cross-checks between the source catalog and the written
  graph. Under v3 the verify rules naturally split: catalog-side checks
  belong to the reader adapter; graph-side checks belong to a
  `GraphSchemaReader` port. This split is deferred to a follow-up;
  Phase 2 keeps the existing verify path working as-is in the Spark
  adapter.
- **Multiple mapping variants.** Variants such as "schema-only, no values"
  or "no embeddings" should be expressed as additional mapping objects,
  not boolean flags. Defining the second variant is deferred until a real
  caller asks for it. The architecture allows it without further changes.
- **Schema evolution across `contract_version`.** Bumping from 1.0 to 1.1
  is a coordinated change. The exact policy on backward-compatible
  additions vs breaking changes is deferred to the first such bump.
