# docker-tests — current-version Neo4j for the semantic-FK spike

The deployed Neo4j is **5.27-aura**, which predates both the `SEARCH` clause
(2025.06) and the in-index filtered vector index (GA **2026.02**). This
folder brings up a throwaway **Community** Neo4j on a current version so
`scripts/spikes/neo4j_query_param_spike.py` can answer the one open
question: does the `search_in_index_filtered` form work — specifically a
**correlated outer reference** (`t.catalog = s.catalog`) inside the in-index
`WHERE`, which every published 2026.01 example avoids (literals/params only).

This proves syntax and behavior on a supported version. It does **not**
prove the dense-scale transaction-memory fix from `fix-zombines-v4.md`
Phase 6b — that still needs a scale run against the real instance.

## Run it

```bash
# 1. local creds for the throwaway container
cp docker-tests/.env.example docker-tests/.env

# 2. bring up Neo4j + the Cypher-25 default-language init (one-shot)
docker compose -f docker-tests/docker-compose.yml up -d
#    wait for health (init exits 0 when the ALTER DATABASE has run):
docker compose -f docker-tests/docker-compose.yml ps

# 3. point the spike at the local container (not the shared repo .env)
export NEO4J_SPIKE_ENV_FILE="$(pwd)/docker-tests/.env"
uv run scripts/spikes/neo4j_query_param_spike.py

# 4. clean slate (no volumes — nothing persists)
docker compose -f docker-tests/docker-compose.yml down
```

The verdict line to read is **`In-index filter (Neo4j 2026.01): ...`**. On a
2026.02+ instance the `index create` parse failure seen on 5.27 must be
gone; then either it returns rows (correlated outer ref works) or it fails
with a precise WHERE-subset error (correlated ref rejected — the query would
need restructuring, not just a version bump).

## Notes

- **Tag**: pinned to `neo4j:2026.04.0` (Community; in-index filtering is GA
  from 2026.02). Bump to a newer Community tag from
  <https://hub.docker.com/_/neo4j/tags> if this one is ever pulled.
- **Cypher 25**: the `neo4j-init` service runs
  `ALTER DATABASE neo4j SET DEFAULT LANGUAGE CYPHER 25` so the spike needs
  no `CYPHER 25` prefix. If that statement ever fails on the chosen
  version, the fallback is to prefix the spike's queries with `CYPHER 25`.
- **Why `NEO4J_SPIKE_ENV_FILE`**: set-but-missing is a hard error in the
  spike (never a silent fall back to the real repo `.env`), matching the
  project's env-layering rule.
- `docker-tests/.env` is gitignored. `.env.example` is the committed
  template; its localhost values are not secrets.
