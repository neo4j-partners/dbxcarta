# Virtual Graph — Serverless Warehouse Findings

## Summary

The **Serverless Starter Warehouse** (`c37d0438c79ad6c5`) would not stay stopped because a
**virtual graph** holding an open JDBC connection pool against Databricks was continuously
probing it. Each probe reset the 10-minute auto-stop timer, so every stop (manual or
automatic) was undone within seconds by the pool's next keepalive query.

Killing the virtual graph closed the JDBC pool, the probes stopped, and the warehouse
finally stayed `STOPPED`. The last probe landed at `2026-06-04T01:25:59Z`, the same moment
the stop took effect. Nothing has hit the warehouse since.

If the virtual graph is restarted, expect the warehouse to come back online with it — that
is by design, since the federation needs live compute to serve queries.

## What the history shows (last ~2 hours on `c37d0438c79ad6c5`)

| Source | Query | Count | First seen | Last seen |
|---|---|---|---|---|
| Databricks JDBC Driver (OSS) | `SELECT current_timezone()` | 193,575 | 2026-06-03T23:37:48Z | 2026-06-04T01:25:59Z |
| Databricks JDBC Driver (OSS) | `select 1` | 445 | 2026-06-03T23:09:21Z | 2026-06-04T01:25:55Z |
| (other client) | `SELECT current_timezone()` | 2,507 | 2026-06-03T23:40:15Z | 2026-06-04T01:25:53Z |
| (other client) | `select 1` | 6 | 2026-06-04T00:24:25Z | 2026-06-04T01:20:25Z |
| Databricks JDBC Driver (OSS) | `WITH relationship_counts AS (SELECT count(*) ...)` | 86 | 2026-06-03T23:13:23Z | 2026-06-04T00:04:44Z |
| Databricks JDBC Driver (OSS) | `WITH node_counts AS (SELECT count(*) ...)` | 86 | 2026-06-03T23:13:22Z | 2026-06-04T00:04:44Z |
| Databricks JDBC Driver (OSS) | graph projection joins (`src_account_id`, `mule`, `account_id`, `merchant_id`) over `graph-enriched-schema.*` | dozens | 2026-06-03T23:37:45Z | 2026-06-04T00:02:10Z |

## Interpretation

- The ~194K `SELECT current_timezone()` calls and the `select 1` pings are JDBC
  **connection-pool keepalive / validation probes** — one per connection acquisition or
  health check. A virtual graph holding an open pool fires these constantly.
- The real workload is unmistakably a graph federation: queries against
  `` `graph-enriched-schema` `` projecting nodes and relationships over accounts,
  transactions, merchants, and a `mule` label. That is the virtual graph federating over
  the Databricks JDBC connection.
- Mechanism of the "can't stop it" behavior: each probe resets the warehouse's 10-minute
  auto-stop timer, so the warehouse never goes idle long enough to stop while the pool is
  alive.

---

# `high-cost-high-performance` (`5ecbef8cd7d6d523`) — Performance Diagnosis

## Config

PRO, Small, Photon on, autoscaling 1–3 clusters, 10-min auto-stop. At the time of
investigation: `RUNNING`, 1 cluster, 0 active sessions, health `HEALTHY`. This warehouse is
**also** serving a virtual graph: ~191K queries in 6 hours, almost all keepalive probes,
with the most recent query at `2026-06-04T04:00:59Z` (still active during the investigation).

## Is it blocked on CPU or memory? Neither.

The diagnostic columns in `system.query.history` for the slowest real queries are all flat:

| Constraint | Signal | Reading |
|---|---|---|
| Memory | `spilled_local_bytes` | **0 on every query** — zero spill, no memory pressure |
| I/O | `read_bytes` / cache % | **0.0–0.01 GB**, 51–100% cache hits — data is tiny and cached |
| Concurrency / queueing | `waiting_at_capacity_ms` | **0 everywhere** — never queued, autoscaling (1→3) never engaged |
| Cluster startup | `waiting_for_compute_ms` | **0** — cluster already warm |
| CPU (warehouse) | `total_task_duration_ms` | **~0 for most queries** — hardware essentially idle |

The compute is barely working. Upsizing the warehouse would not help.

## Actual bottleneck: per-query overhead from the federation pattern

The slowest queries are dominated by **compilation/planning**, not execution:

| Query | Total | Compile | Exec | Task | Note |
|---|---|---|---|---|---|
| `SELECT 1 FROM accounts GROUP BY account_id HAVING COUNT(*)>1 LIMIT 1` | 11.3s | 10.3s | 0.9s | 0 | uniqueness check — 91% planning |
| `WITH node_counts AS (SELECT count(*) ...)` | 6.2s | 5.7s | 0.5s | 0.3s | count — 92% planning |
| `SELECT count(account_id) ...` | 1.9s | 1.8s | 0.1s | 0 | count — 94% planning |
| `Listing databases` (JDBC metadata) | 4.8s | — | — | — | driver round-trip |
| `Listing catalogs` (JDBC metadata) | 1.4s | — | — | — | driver round-trip |

Plus two genuinely execution-heavy federation queries with no parallelism
(`task_ms ≈ exec_ms`, a single dominating task):

- `accounts ⋈ account_links WHERE balance > ?` — 7.5s exec / 7.2s task
- `max(transfer_timestamp)` — 4.2s exec / 2.9s task

## Slow-query EXPLAIN — the plan is optimal

`EXPLAIN FORMATTED` on the slowest execution query
(`accounts ⋈ account_links WHERE balance > ?`):

```
PhotonBroadcastHashJoin Inner
  :- PhotonScan accounts        (balance > 0 pushed down as DictionaryFilter + RequiredDataFilter)
  +- PhotonScan account_links   (hashedrelationcontains(src_account_id) pushed down)
== Photon Explanation == The query is fully supported by Photon.
== Optimizer Statistics == full = account_links, accounts
```

Best-case plan: broadcast hash join (small `accounts` side broadcast), full optimizer
statistics on both tables, join key and `balance` filter pushed into the scans, 100% Photon.
The slowness is **not** from a bad plan or missing stats. The 7s of task time on 0.01 GB read
is the federation engine materializing/returning a large joined result set serially.

## Bottom line

`high-cost-high-performance` is **not CPU- or memory-constrained — it is overhead-constrained.**
The virtual-graph federation issues a flood of tiny queries, each paying a fixed compilation
tax (often multiple seconds for a trivial count) plus repeated slow JDBC metadata listings.
The compute is idle; the cost is per-query fixed overhead and round-trip count. Levers that
would actually help: reducing query/probe volume, reusing prepared statements, and caching
catalog metadata in the virtual-graph driver — not a bigger warehouse.
