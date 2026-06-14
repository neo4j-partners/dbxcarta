"""Shared, Spark-free foundation for the dbxcarta layers.

``dbxcarta-core`` sits underneath ``dbxcarta-client``, ``dbxcarta-materialize``,
and ``dbxcarta-submit``. It holds the helpers they need and pulls in only the
Databricks SDK, never Spark: identifier and path helpers (:mod:`identifiers`),
the catalog-list rule (:mod:`catalogs`), workspace/secret access
(:mod:`workspace`), the SQL warehouse runner (:mod:`executor`), the readiness
check and question upload (:mod:`readiness`), and the env-file overlay loader
(:mod:`env`).

Submodules are imported directly (``from dbxcarta.core.identifiers import
quote_identifier``); this package root deliberately re-exports nothing so that
importing it pulls in no Spark, client, or Neo4j code.
"""
