"""RetrievalTrace: per-question diagnostic record for the graph_rag arm."""

from __future__ import annotations

from dataclasses import dataclass

from dbxcarta.client.ids import schema_from_node_id


def schema_scores_from_seeds(
    col_seed_ids: list[str],
    col_seed_scores: list[float],
    tbl_seed_ids: list[str],
    tbl_seed_scores: list[float],
) -> dict[str, float]:
    """Aggregate seed scores onto schemas, normalized per vector index first."""
    raw: dict[str, float] = {}
    for scores in (
        _normalized_schema_scores(col_seed_ids, col_seed_scores),
        _normalized_schema_scores(tbl_seed_ids, tbl_seed_scores),
    ):
        for schema, score in scores.items():
            raw[schema] = raw.get(schema, 0.0) + score
    total = sum(raw.values()) or 1.0
    return {k: v / total for k, v in raw.items()}


def _normalized_schema_scores(
    seed_ids: list[str],
    seed_scores: list[float],
) -> dict[str, float]:
    raw: dict[str, float] = {}
    for node_id, score in zip(seed_ids, seed_scores, strict=False):
        schema = schema_from_node_id(node_id)
        if schema:
            raw[schema] = raw.get(schema, 0.0) + score
    total = sum(raw.values()) or 1.0
    return {schema: score / total for schema, score in raw.items()}


def chosen_schemas_from_columns(
    columns: list,
) -> list[str]:
    """Return deduplicated schemas present in the final column set, in order."""
    seen: set[str] = set()
    result: list[str] = []
    for col in columns:
        schema = schema_from_node_id(col.table_fqn)
        if schema and schema not in seen:
            seen.add(schema)
            result.append(schema)
    return result


@dataclass
class RetrievalTrace:
    """Per-question retrieval-metrics record for the graph_rag arm.

    Carries only what the arm consumes: ``target_schema``, ``schema_scores``,
    ``chosen_schemas``, and ``final_col_ids`` are the inputs
    ``compute_retrieval_metrics`` reads; ``col_seed_ids``/``tbl_seed_ids`` are
    the seed lists the arm reports as ``context_ids``; and the three metric
    fields are the computed outputs. The earlier per-question generation and
    execution fields were dropped with the ``client_retrieval`` Delta sink that
    once consumed them.
    """

    target_schema: str | None
    col_seed_ids: list[str]
    tbl_seed_ids: list[str]
    schema_scores: dict[str, float]
    chosen_schemas: list[str]
    final_col_ids: list[str]
    top1_schema_match: bool | None = None
    schema_in_context: bool | None = None
    context_purity: float | None = None


def compute_retrieval_metrics(trace: RetrievalTrace) -> None:
    """Compute top1_schema_match, schema_in_context, context_purity in-place."""
    if not trace.target_schema:
        return
    if trace.schema_scores:
        top1 = max(trace.schema_scores, key=trace.schema_scores.__getitem__)
        trace.top1_schema_match = top1 == trace.target_schema
    trace.schema_in_context = trace.target_schema in trace.chosen_schemas
    if trace.final_col_ids:
        from_target = sum(
            1 for cid in trace.final_col_ids if schema_from_node_id(cid) == trace.target_schema
        )
        trace.context_purity = from_target / len(trace.final_col_ids)
