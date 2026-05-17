# Proposal: Verify the Finance Genie pipeline end to end

## Goal

Run the full Finance Genie dbxcarta pipeline from artifact build through
client evaluation, on the committed overlay
`examples/integration/finance-genie/dbxcarta-overlay.env`, while the
schemapile and dense-schema demos may also be running.

## Isolation assessment

The overlay model isolates Finance Genie from the other demos at the
Unity Catalog layer:

| Resource | Finance Genie | schemapile |
|---|---|---|
| Catalogs | `graph-enriched-finance-bronze/silver/gold` | `schemapile_lakehouse` |
| Ops volume | `/Volumes/dbxcarta-catalog/finance_genie_ops/dbxcarta-ops` | `/Volumes/schemapile_lakehouse/_meta/schemapile_volume` |
| Summary table | `dbxcarta-catalog.finance_genie_ops.dbxcarta_run_summary` | `schemapile_lakehouse._meta.dbxcarta_run_summary` |
| Questions path | `.../finance_genie_ops/dbxcarta-ops/dbxcarta/questions.json` | `.../schemapile_volume/dbxcarta/questions.json` |

These sets are disjoint, so UC reads, the summary table, and the
question fixture do not collide across demos.

Neo4j writes are also safe to run concurrently. `write_nodes` uses
`.mode("Overwrite")` with `node.keys=id`, which is a MERGE on `id`, and
node ids are catalog-qualified (`contract.py` concatenates
catalog.schema.table.column). Distinct catalog prefixes produce distinct
ids, so a finance-genie ingest and a schemapile ingest upsert disjoint
node sets. There is no global `DETACH DELETE`; the only delete is
`purge_stale_values`, scoped by the current run's column ids.

## Issue status (all resolved for this run)

1. **Shared Neo4j database, whole-graph metrics — moot for this run.**
   `query_counts` and the verify step count every node and relationship
   of a label across the whole graph, not by catalog, and all demos
   share one Neo4j instance from the base `.env`. A read-only scope
   check on 2026-05-16 found the `neo4j` database empty: 0 nodes total,
   no schemapile, dense-schema, or Finance Genie data. With nothing
   resident, post-ingest counts and the
   `DBXCARTA_EMBEDDING_FAILURE_THRESHOLD=0.10` evaluation are
   Finance-Genie-only by construction. `DBXCARTA_VERIFY_GATE=false` in
   the base `.env`, so the threshold does not hard-fail regardless.

2. **GraphRAG retrieval over the union graph — moot for this run.**
   Vector indexes are global per label, but with an empty starting graph
   the only nodes present after ingest are Finance Genie's, so the
   `graph_rag` arm retrieves only Finance Genie nodes. The `no_context`
   and `schema_dump` arms never read Neo4j.

3. **Catalog scope — confirmed.** The upstream Finance Genie data lives
   in the medallion catalogs the overlay names
   (`graph-enriched-finance-bronze/silver/gold`), confirmed by the
   operator. The readiness check (step 2 below) re-verifies this.

4. **Embedding endpoint — already satisfied.** The base `.env` supplies
   `DBXCARTA_EMBEDDING_ENDPOINT=databricks-gte-large-en` and
   `DBXCARTA_EMBEDDING_DIMENSION=1024`. No change needed.

## Residual condition

The isolation of issues 1 and 2 depends on no other demo ingesting into
Neo4j during this run window. schemapile and dense-schema share the
catalog `schemapile_lakehouse`; their node ids start with
`schemapile_lakehouse`, disjoint from the Finance Genie
`graph_enriched_finance_*` prefixes. If a foreign graph appears later
and a scoped cleanup is wanted, the precise, non-destructive predicate
is:

```cypher
// preview count first, then DETACH DELETE the same match
MATCH (n)
WHERE n.id = 'schemapile_lakehouse'
   OR n.id STARTS WITH 'schemapile_lakehouse.'
RETURN labels(n) AS label, count(n) AS c
```

This never touches the Finance Genie prefixes. Run the count form and
confirm before converting it to `DETACH DELETE n`.

## Execution plan

All commands run from the repo root. Each `dbxcarta` command takes the
overlay; export it once to drop the repeated flag:

```bash
export DBXCARTA_ENV_FILE=examples/integration/finance-genie/dbxcarta-overlay.env
```

1. **Sync and install the preset**
   ```bash
   uv sync
   uv pip install -e examples/integration/finance-genie/
   ```

2. **Check readiness** (resolves issue 3 before any submit)
   ```bash
   uv run dbxcarta preset dbxcarta_finance_genie_example:preset --check-ready --strict-optional
   ```
   Stop and reconcile catalogs if base tables are reported missing.

3. **Refresh Neo4j secrets**
   ```bash
   ./setup_secrets.sh --profile azure-rk-knight
   ```

4. **Upload the question set**
   ```bash
   uv run dbxcarta preset dbxcarta_finance_genie_example:preset --upload-questions
   ```

5. **Build and upload artifacts**
   ```bash
   uv run dbxcarta upload --wheel
   uv run dbxcarta upload --all
   ```

6. **Build the semantic layer**
   ```bash
   uv run dbxcarta submit-entrypoint ingest
   ```

7. **Verify the result**
   ```bash
   uv run dbxcarta verify
   ```
   Interpret counts and the embedding-failure gate per issues 1 and 2.

8. **Run the client evaluation**
   ```bash
   uv run dbxcarta submit-entrypoint client
   ```
   Arms: `no_context,schema_dump,graph_rag`. The `graph_rag` arm reflects
   the union graph if other demos are resident.

9. **Optional: local read-only demo**
   ```bash
   cp examples/integration/finance-genie/.env.sample examples/integration/finance-genie/.env
   # edit warehouse, chat endpoint, Neo4j creds
   uv run --directory examples/integration/finance-genie \
     python -m dbxcarta_finance_genie_example.local_demo preflight
   ```

## Success criteria

- Steps 1 through 8 exit zero.
- Readiness reports the five base tables present.
- Ingest writes the expected node and relationship labels; counts are
  consistent with Finance Genie magnitudes after accounting for any
  co-resident graphs.
- Client eval produces scored results for all three arms.
