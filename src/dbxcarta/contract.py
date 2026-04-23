"""Graph contract: labels, relationship types, and identifier generation.

All identifier production goes through generate_id or generate_value_id.
No call site builds an ID inline.
"""

import hashlib

CONTRACT_VERSION = "1.0"

# Node labels
LABEL_DATABASE = "Database"
LABEL_SCHEMA = "Schema"
LABEL_TABLE = "Table"
LABEL_COLUMN = "Column"
LABEL_VALUE = "Value"

# Relationship types
REL_HAS_SCHEMA = "HAS_SCHEMA"
REL_HAS_TABLE = "HAS_TABLE"
REL_HAS_COLUMN = "HAS_COLUMN"
REL_HAS_VALUE = "HAS_VALUE"
REL_REFERENCES = "REFERENCES"

# REFERENCES edge properties (additive in contract v1.0). All three are
# nullable. Legacy edges written before Phase 2 have none of these set;
# readers treat absence as (1.0, "declared", null) via COALESCE in Cypher.
# See worklog/fk-gap-v3-build.md Phase 2.
REL_PROP_CONFIDENCE = "confidence"
REL_PROP_SOURCE = "source"
REL_PROP_CRITERIA = "criteria"
REFERENCES_PROPERTIES = (REL_PROP_CONFIDENCE, REL_PROP_SOURCE, REL_PROP_CRITERIA)

# Characters translated in normalization: space→_, hyphen→_
# These constants are shared with the Spark SQL expression to prevent drift.
_TRANSLATE_FROM = " -"
_TRANSLATE_TO = "__"


def generate_id(*parts: str) -> str:
    """Return a normalized dot-separated identifier.

    Lowercases each part and replaces spaces and hyphens with underscores,
    then joins with dots. Must produce byte-identical output to id_expr().
    """
    return ".".join(p.lower().replace(" ", "_").replace("-", "_") for p in parts)


def generate_value_id(column_id: str, value: object) -> str:
    """Return the Value node id for a sampled distinct value."""
    digest = hashlib.md5(str(value).encode()).hexdigest()
    return f"{column_id}.{digest}"


def id_expr(*column_names: str):
    """Return a PySpark Column expression equivalent to generate_id().

    Uses lower(translate(concat_ws('.', ...), ' -', '__')) so the Spark
    computation is byte-identical to the Python function above.
    """
    from pyspark.sql import functions as F

    parts = [F.col(c) for c in column_names]
    return F.lower(F.translate(F.concat_ws(".", *parts), _TRANSLATE_FROM, _TRANSLATE_TO))


def value_id_expr():
    """Return a PySpark Column expression equivalent to generate_value_id().

    Computes concat(col_id, ".", md5(val)) where col_id and val are column
    names in the calling DataFrame. Byte-identical to generate_value_id() for
    UTF-8 string values (Spark md5() returns lowercase hex, as does hashlib).
    """
    from pyspark.sql import functions as F

    return F.concat(F.col("col_id"), F.lit("."), F.md5(F.col("val")))
