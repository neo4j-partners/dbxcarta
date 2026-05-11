"""Built-in example presets for dbxcarta."""

from dbxcarta.presets.finance_genie import (
    FINANCE_GENIE_BASE_TABLES,
    FINANCE_GENIE_GOLD_TABLES,
    FINANCE_GENIE_TABLES,
    FinanceGenieReadiness,
    finance_genie_env,
    format_env,
    readiness_from_table_names,
)

__all__ = [
    "FINANCE_GENIE_BASE_TABLES",
    "FINANCE_GENIE_GOLD_TABLES",
    "FINANCE_GENIE_TABLES",
    "FinanceGenieReadiness",
    "finance_genie_env",
    "format_env",
    "readiness_from_table_names",
]
