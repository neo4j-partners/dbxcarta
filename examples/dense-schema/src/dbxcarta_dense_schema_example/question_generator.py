"""LLM question generation for the dense-schema fixture.

Generates (question, SQL) pairs by sampling connected subgraphs of tables
from the single large schema. For each batch, a seed table is picked at
random; FK edges expand it to a neighborhood of 3-8 related tables. The
LLM is asked to produce `questions_per_batch` questions from that subgraph.
This distributes questions across the schema rather than concentrating them
on a small number of tables.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import re
import sys
import textwrap
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Any, TYPE_CHECKING

from dbxcarta.databricks import build_workspace_client
from dbxcarta_dense_schema_example.config import DenseSchemaConfig, load_config
from dbxcarta_dense_schema_example.materialize import _sanitize_name
from dbxcarta_dense_schema_example.utils import (
    load_dotenv_file,
    read_required_warehouse_id,
)

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient


logger = logging.getLogger(__name__)

_SHAPES = ("single_table_filter", "two_table_join", "aggregation")
_MAX_SUBGRAPH_TABLES = 8
_MIN_SUBGRAPH_TABLES = 3
_MAX_BFS_DEPTH = 2
_MAX_ATTEMPTS_PER_BATCH = 5


@dataclass(frozen=True)
class GeneratedPair:
    uc_schema: str
    source_id: str
    shape: str
    question: str
    sql: str


@dataclass
class ValidationOutcome:
    accepted: list[GeneratedPair]
    errored: int = 0
    empty: int = 0
    trivial: int = 0


def main() -> int:
    parser = argparse.ArgumentParser(prog="dbxcarta-dense-generate-questions")
    parser.add_argument("--dotenv", type=Path, default=Path(".env"))
    parser.add_argument("--output", type=Path, default=Path("questions.json"))
    parser.add_argument(
        "--cache-dir", type=Path, default=Path(".cache/questions")
    )
    parser.add_argument("--warehouse-id", type=str, default=None)
    parser.add_argument(
        "--skip-validate",
        action="store_true",
        help="Skip SQL execution validation. Useful for offline iteration.",
    )
    args = parser.parse_args()

    load_dotenv_file(args.dotenv)
    config = load_config()

    if not config.candidate_cache.is_file():
        raise FileNotFoundError(
            f"Candidate JSON not found at {config.candidate_cache}."
            " Run dbxcarta-dense-generate first."
        )

    warehouse_id = ""
    if not args.skip_validate:
        warehouse_id = read_required_warehouse_id(
            args.warehouse_id,
            operation="SQL validation",
            extra_hint="or use --skip-validate",
        )

    payload = json.loads(config.candidate_cache.read_text())
    schema_entry = (payload.get("schemas") or [{}])[0]

    ws = build_workspace_client()
    pairs = _generate_all(ws, config, schema_entry, args.cache_dir)
    if args.skip_validate:
        outcome = ValidationOutcome(accepted=pairs)
    else:
        outcome = _validate_all(ws, warehouse_id, config.catalog, pairs)

    args.output.write_text(json.dumps(_format_questions(outcome.accepted), indent=2))
    print(
        f"[dense] generated={len(pairs)}"
        f" accepted={len(outcome.accepted)}"
        f" errored={outcome.errored} empty={outcome.empty} trivial={outcome.trivial}",
        file=sys.stderr,
    )
    print(f"[dense] wrote {args.output}", file=sys.stderr)
    return 0


def _generate_all(
    ws: "WorkspaceClient",
    config: DenseSchemaConfig,
    schema_entry: dict[str, Any],
    cache_dir: Path,
) -> list[GeneratedPair]:
    cache_dir.mkdir(parents=True, exist_ok=True)
    tables = schema_entry.get("tables") or []
    if not tables:
        return []

    fk_graph = _build_fk_graph(tables)
    table_names = [_sanitize_name(t.get("name", "")) for t in tables]
    table_names = [n for n in table_names if n]

    pairs: list[GeneratedPair] = []
    import random

    rng = random.Random(config.seed)
    batch_idx = 0
    max_batches = config.questions_target * 3

    while len(pairs) < config.questions_target and batch_idx < max_batches:
        seed_table = rng.choice(table_names)
        subgraph_names = _expand_subgraph(seed_table, fk_graph, rng)
        if len(subgraph_names) < _MIN_SUBGRAPH_TABLES:
            batch_idx += 1
            continue

        subgraph_tables = [
            t for t in tables
            if _sanitize_name(t.get("name", "")) in set(subgraph_names)
        ]

        cache_key = _cache_key(subgraph_names, config, batch_idx)
        cached = cache_dir / f"batch_{cache_key}.json"
        if cached.is_file():
            raw = json.loads(cached.read_text())
        else:
            raw = _call_model(ws, config, schema_entry, subgraph_tables)
            cached.write_text(json.dumps(raw, indent=2))

        for item in raw:
            shape = str(item.get("shape", "")).strip()
            question = str(item.get("question", "")).strip()
            sql = str(item.get("sql", "")).strip()
            if shape not in _SHAPES or not question or not sql:
                continue
            pairs.append(GeneratedPair(
                uc_schema=schema_entry.get("uc_schema", ""),
                source_id=schema_entry.get("source_id", ""),
                shape=shape,
                question=question,
                sql=sql,
            ))

        batch_idx += 1

    return pairs[: config.questions_target * 2]


def _build_fk_graph(tables: list[dict[str, Any]]) -> dict[str, set[str]]:
    graph: dict[str, set[str]] = {}
    for table in tables:
        name = _sanitize_name(table.get("name", ""))
        if not name:
            continue
        graph.setdefault(name, set())
        for fk in table.get("foreign_keys") or []:
            foreign = _sanitize_name(str(fk.get("foreign_table", "")))
            if foreign and foreign != name:
                graph[name].add(foreign)
                graph.setdefault(foreign, set()).add(name)
    return graph


def _expand_subgraph(
    seed: str,
    graph: dict[str, set[str]],
    rng,
) -> list[str]:
    visited: set[str] = set()
    queue: deque[tuple[str, int]] = deque([(seed, 0)])
    while queue and len(visited) < _MAX_SUBGRAPH_TABLES:
        node, depth = queue.popleft()
        if node in visited:
            continue
        visited.add(node)
        if depth < _MAX_BFS_DEPTH:
            neighbors = list(graph.get(node, set()))
            rng.shuffle(neighbors)
            for neighbor in neighbors:
                if neighbor not in visited:
                    queue.append((neighbor, depth + 1))
    return list(visited)


def _cache_key(
    subgraph_names: list[str], config: DenseSchemaConfig, batch_idx: int
) -> str:
    sig = json.dumps(
        {
            "tables": sorted(subgraph_names),
            "model": config.question_model,
            "per_batch": config.questions_per_batch,
            "temperature": config.question_temperature,
            "batch": batch_idx,
        },
        sort_keys=True,
    ).encode()
    return hashlib.sha256(sig).hexdigest()[:16]


def _call_model(
    ws: "WorkspaceClient",
    config: DenseSchemaConfig,
    schema_entry: dict[str, Any],
    subgraph_tables: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    from databricks.sdk.service.serving import ChatMessage, ChatMessageRole

    prompt = _build_prompt(schema_entry, subgraph_tables, config)
    response = ws.serving_endpoints.query(
        name=config.question_model,
        messages=[
            ChatMessage(role=ChatMessageRole.SYSTEM, content=_SYSTEM_PROMPT),
            ChatMessage(role=ChatMessageRole.USER, content=prompt),
        ],
        temperature=config.question_temperature,
        max_tokens=2000,
    )
    text = _first_message_text(response)
    return _parse_json_block(text)


_SYSTEM_PROMPT = textwrap.dedent("""\
    You write evaluation questions for natural-language-to-SQL benchmarks.
    Output strict JSON only: a list of objects each with the keys
    "shape", "question", "sql". Valid "shape" values are
    "single_table_filter", "two_table_join", "aggregation". Every "sql"
    must be a syntactically valid Databricks SQL SELECT referencing only
    tables shown in the user prompt. Use fully qualified table names with
    backticks. Do not emit any prose outside the JSON array.
""")


def _build_prompt(
    schema_entry: dict[str, Any],
    tables: list[dict[str, Any]],
    config: DenseSchemaConfig,
) -> str:
    catalog = config.catalog
    uc_schema = schema_entry.get("uc_schema", "")
    ddl_lines: list[str] = []
    for table in tables:
        table_name = _sanitize_name(str(table.get("name", "")))
        if not table_name:
            continue
        safe_cols = [
            (_sanitize_name(str(c.get("name", ""))), c)
            for c in (table.get("columns") or [])
        ]
        safe_cols = [(n, c) for n, c in safe_cols if n]
        cols = ", ".join(
            f"{name} {c.get('type', '')}" for name, c in safe_cols
        )
        pk = [
            _sanitize_name(str(k)) for k in (table.get("primary_keys") or [])
        ]
        pk_clause = f" PK({', '.join(pk)})" if pk else ""
        fks = table.get("foreign_keys") or []
        fk_parts = []
        for fk in fks:
            fk_cols = [_sanitize_name(str(c)) for c in (fk.get("columns") or [])]
            ref_table = _sanitize_name(str(fk.get("foreign_table") or ""))
            ref_cols = [
                _sanitize_name(str(c))
                for c in (fk.get("referred_columns") or [])
            ]
            if fk_cols and ref_table and ref_cols:
                fk_parts.append(
                    f"{', '.join(fk_cols)} -> {ref_table}({', '.join(ref_cols)})"
                )
        fk_clause = f" FKs: {'; '.join(fk_parts)}" if fk_parts else ""
        ddl_lines.append(f"- {table_name}({cols}){pk_clause}{fk_clause}")

    schema_block = "\n".join(ddl_lines) if ddl_lines else "(no tables)"
    n = config.questions_per_batch
    n_per_shape = max(1, n // 3)
    return textwrap.dedent(f"""\
        Catalog: `{catalog}`
        Schema: `{uc_schema}`

        Tables:
        {schema_block}

        Produce exactly {n} question/SQL pairs as a JSON array. Distribute
        across the three shapes with at least {n_per_shape} of each shape.
        SQL must reference tables as `{catalog}`.`{uc_schema}`.`<table>`.
        Prefer multi-table JOINs using the FK relationships shown above.
    """)


_JSON_BLOCK_RE = re.compile(r"\[\s*\{.*?\}\s*\]", re.DOTALL)


def _parse_json_block(text: str) -> list[dict[str, Any]]:
    match = _JSON_BLOCK_RE.search(text)
    if not match:
        return []
    try:
        parsed = json.loads(match.group(0))
    except json.JSONDecodeError:
        return []
    return parsed if isinstance(parsed, list) else []


def _first_message_text(response: Any) -> str:
    if isinstance(response, dict):
        choices = response.get("choices") or []
    else:
        choices = getattr(response, "choices", None) or []
    if not choices:
        return ""
    first = choices[0]
    if isinstance(first, dict):
        message = first.get("message")
    else:
        message = getattr(first, "message", None)
    if message is None:
        return ""
    if isinstance(message, dict):
        return message.get("content") or ""
    return getattr(message, "content", None) or ""


def _validate_all(
    ws: "WorkspaceClient",
    warehouse_id: str,
    catalog: str,
    pairs: list[GeneratedPair],
) -> ValidationOutcome:
    from databricks.sdk.service.sql import (
        ExecuteStatementRequestOnWaitTimeout,
        StatementState,
    )

    accepted: list[GeneratedPair] = []
    errored = 0
    empty = 0
    trivial = 0
    for pair in pairs:
        if not _sql_targets_only_catalog(pair.sql, catalog):
            errored += 1
            continue
        try:
            response = ws.statement_execution.execute_statement(
                warehouse_id=warehouse_id,
                statement=pair.sql,
                wait_timeout="30s",
                on_wait_timeout=ExecuteStatementRequestOnWaitTimeout.CANCEL,
            )
        except Exception as exc:
            logger.info("[dense] sql errored: %s", exc)
            errored += 1
            continue
        status = getattr(getattr(response, "status", None), "state", None)
        if status != StatementState.SUCCEEDED:
            errored += 1
            continue
        result = getattr(response, "result", None)
        rows = getattr(result, "data_array", None) or []
        if not rows:
            empty += 1
            continue
        if len(rows) == 1 and len(rows[0]) == 1 and rows[0][0] in (None, "", 0, "0"):
            trivial += 1
            continue
        accepted.append(pair)
    return ValidationOutcome(accepted=accepted, errored=errored, empty=empty, trivial=trivial)


_FORBIDDEN_SQL_RE = re.compile(
    r"\b("
    r"alter|call|copy|create|delete|drop|execute|grant|insert|merge|msck|"
    r"optimize|refresh|repair|replace|revoke|truncate|update|use|vacuum"
    r")\b",
    re.IGNORECASE,
)
_TABLE_REF_RE = re.compile(r"\b(?:from|join)\s+([`A-Za-z0-9_.-]+)", re.IGNORECASE)


def _sql_targets_only_catalog(sql: str, catalog: str) -> bool:
    normalized = sql.strip()
    lowered = normalized.lower()
    if not lowered.startswith("select"):
        return False
    if ";" in normalized.rstrip(";"):
        return False
    if _FORBIDDEN_SQL_RE.search(normalized):
        return False
    if "information_schema" in lowered or re.search(r"\bsystem\s*\.", lowered):
        return False
    target = f"`{catalog.lower()}`"
    if target not in lowered:
        return False
    for match in _TABLE_REF_RE.finditer(sql):
        ref = match.group(1).strip()
        if ref.startswith("`"):
            if not ref.lower().startswith(target):
                return False
        elif "." in ref:
            if not ref.lower().startswith(f"{catalog.lower()}."):
                return False
        else:
            return False
    return True


def _format_questions(pairs: list[GeneratedPair]) -> list[dict[str, Any]]:
    return [
        {
            "question_id": f"ds_{i:04d}",
            "question": pair.question,
            "reference_sql": pair.sql,
            "schema": pair.uc_schema,
            "source_id": pair.source_id,
            "shape": pair.shape,
        }
        for i, pair in enumerate(pairs)
    ]


if __name__ == "__main__":
    raise SystemExit(main())
