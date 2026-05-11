"""Finance Genie companion preset.

The preset keeps dbxcarta generic while documenting the known Finance Genie
Lakehouse contract in one place. Runtime commands can print an env overlay or
check whether the expected Unity Catalog tables are present.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable


FINANCE_GENIE_CATALOG = "graph-enriched-lakehouse"
FINANCE_GENIE_SCHEMA = "graph-enriched-schema"
FINANCE_GENIE_VOLUME = "graph-enriched-volume"

FINANCE_GENIE_BASE_TABLES = (
    "accounts",
    "merchants",
    "transactions",
    "account_links",
    "account_labels",
)

FINANCE_GENIE_GOLD_TABLES = (
    "gold_accounts",
    "gold_account_similarity_pairs",
    "gold_fraud_ring_communities",
)

FINANCE_GENIE_TABLES = FINANCE_GENIE_BASE_TABLES + FINANCE_GENIE_GOLD_TABLES


def finance_genie_volume_path(
    *,
    catalog: str = FINANCE_GENIE_CATALOG,
    schema: str = FINANCE_GENIE_SCHEMA,
    volume: str = FINANCE_GENIE_VOLUME,
) -> str:
    return f"/Volumes/{catalog}/{schema}/{volume}"


def finance_genie_env(
    *,
    catalog: str = FINANCE_GENIE_CATALOG,
    schema: str = FINANCE_GENIE_SCHEMA,
    volume: str = FINANCE_GENIE_VOLUME,
) -> dict[str, str]:
    """Return the recommended dbxcarta env overlay for Finance Genie."""
    volume_path = finance_genie_volume_path(
        catalog=catalog,
        schema=schema,
        volume=volume,
    )
    return {
        "DBXCARTA_CATALOG": catalog,
        "DBXCARTA_SCHEMAS": schema,
        "DATABRICKS_VOLUME_PATH": volume_path,
        "DBXCARTA_SUMMARY_VOLUME": f"{volume_path}/dbxcarta/runs",
        "DBXCARTA_SUMMARY_TABLE": f"{catalog}.{schema}.dbxcarta_run_summary",
        "DBXCARTA_INCLUDE_VALUES": "true",
        "DBXCARTA_SAMPLE_LIMIT": "10",
        "DBXCARTA_SAMPLE_CARDINALITY_THRESHOLD": "50",
        "DBXCARTA_INCLUDE_EMBEDDINGS_TABLES": "true",
        "DBXCARTA_INCLUDE_EMBEDDINGS_COLUMNS": "true",
        "DBXCARTA_INCLUDE_EMBEDDINGS_VALUES": "true",
        "DBXCARTA_INCLUDE_EMBEDDINGS_SCHEMAS": "true",
        "DBXCARTA_INCLUDE_EMBEDDINGS_DATABASES": "true",
        "DBXCARTA_INFER_SEMANTIC": "true",
        "DBXCARTA_EMBEDDING_ENDPOINT": "databricks-gte-large-en",
        "DBXCARTA_EMBEDDING_DIMENSION": "1024",
        "DBXCARTA_EMBEDDING_FAILURE_THRESHOLD": "0.10",
        "DBXCARTA_CLIENT_QUESTIONS": f"{volume_path}/dbxcarta/questions.json",
        "DBXCARTA_CLIENT_ARMS": "no_context,schema_dump,graph_rag",
        "DBXCARTA_INJECT_CRITERIA": "false",
    }


def format_env(values: dict[str, str]) -> str:
    """Format an env overlay in stable shell-compatible order."""
    return "\n".join(f"{key}={values[key]}" for key in values) + "\n"


@dataclass(frozen=True)
class FinanceGenieReadiness:
    catalog: str
    schema: str
    present_tables: tuple[str, ...]
    missing_base_tables: tuple[str, ...]
    missing_gold_tables: tuple[str, ...]

    @property
    def base_ready(self) -> bool:
        return not self.missing_base_tables

    @property
    def gold_ready(self) -> bool:
        return not self.missing_gold_tables

    def ok(self, *, strict_gold: bool = False) -> bool:
        return self.base_ready and (self.gold_ready or not strict_gold)

    def format(self, *, strict_gold: bool = False) -> str:
        lines = [
            f"Finance Genie UC scope: {self.catalog}.{self.schema}",
            f"Present expected tables: {len(self.present_tables)}/{len(FINANCE_GENIE_TABLES)}",
        ]
        if self.missing_base_tables:
            lines.append(
                "Missing base tables: " + ", ".join(self.missing_base_tables)
            )
        else:
            lines.append("Base tables: ready")
        if self.missing_gold_tables:
            label = "Missing Gold tables"
            if not strict_gold:
                label += " (warning)"
            lines.append(f"{label}: " + ", ".join(self.missing_gold_tables))
        else:
            lines.append("Gold tables: ready")
        lines.append(
            "Status: ready" if self.ok(strict_gold=strict_gold) else "Status: not ready"
        )
        return "\n".join(lines)


def readiness_from_table_names(
    table_names: Iterable[str],
    *,
    catalog: str = FINANCE_GENIE_CATALOG,
    schema: str = FINANCE_GENIE_SCHEMA,
) -> FinanceGenieReadiness:
    present = {name.strip() for name in table_names if name and name.strip()}
    expected_present = tuple(name for name in FINANCE_GENIE_TABLES if name in present)
    return FinanceGenieReadiness(
        catalog=catalog,
        schema=schema,
        present_tables=expected_present,
        missing_base_tables=tuple(
            name for name in FINANCE_GENIE_BASE_TABLES if name not in present
        ),
        missing_gold_tables=tuple(
            name for name in FINANCE_GENIE_GOLD_TABLES if name not in present
        ),
    )
