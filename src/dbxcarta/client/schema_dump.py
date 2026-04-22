"""Neo4j schema dump: read all columns for the configured catalog/schemas."""

from __future__ import annotations

from neo4j import GraphDatabase

from dbxcarta.client.neo4j_utils import neo4j_credentials
from dbxcarta.client.settings import ClientSettings

_CYPHER = """
MATCH (db:Database {name: $catalog})-[:HAS_SCHEMA]->(s:Schema)
      -[:HAS_TABLE]->(t:Table)-[:HAS_COLUMN]->(c:Column)
WHERE size($schemas) = 0 OR s.name IN $schemas
RETURN s.name AS schema_name,
       t.name  AS table_name,
       c.name  AS column_name,
       c.data_type AS data_type,
       c.comment   AS comment,
       c.ordinal_position AS pos
ORDER BY s.name, t.name, c.ordinal_position
"""


def fetch_schema_dump(settings: ClientSettings) -> str:
    """Query Neo4j and return a formatted schema string for the prompt."""
    uri, username, password = neo4j_credentials(settings)
    driver = GraphDatabase.driver(uri, auth=(username, password))

    rows: list[dict] = []
    try:
        with driver.session() as session:
            result = session.run(
                _CYPHER,
                catalog=settings.dbxcarta_catalog,
                schemas=settings.schemas_list,
            )
            rows = result.data()
    finally:
        driver.close()

    if not rows:
        raise RuntimeError(
            "Neo4j returned no columns for "
            f"catalog={settings.dbxcarta_catalog!r} schemas={settings.schemas_list!r}. "
            "Run the server pipeline first to populate the graph."
        )

    return _format_schema(rows, settings.dbxcarta_catalog)


def _format_schema(rows: list[dict], catalog: str) -> str:
    """Render column rows as a structured schema block for prompt insertion."""
    lines: list[str] = []
    current_table: str | None = None

    for row in rows:
        fqt = f"{catalog}.{row['schema_name']}.{row['table_name']}"
        if fqt != current_table:
            if current_table is not None:
                lines.append("")
            lines.append(f"Table: {fqt}")
            current_table = fqt

        comment = row.get("comment") or ""
        suffix = f" — {comment}" if comment else ""
        lines.append(f"  {row['column_name']} ({row['data_type']}){suffix}")

    return "\n".join(lines)
