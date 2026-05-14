# Phase A: graph_rag Failure Classification

**Run:** 52079308984308 (Databricks job) / `local` (Delta table run_id)
**Date:** 2026-05-14
**Source table:** `schemapile_lakehouse._meta.client_retrieval WHERE run_id = 'local' AND question_id LIKE 'ds_%'`

---

## Summary

| Bucket | Count | % | Action |
|---|---|---|---|
| **B1: Missing table in context** | 7 | 27% | Phase C (retrieval) partially addresses |
| **B2: Table present, SQL wrong** | 19 | 73% | Phase D (prompt fix) required |
| **B3: Suspect reference** | 0 | 0% | None |
| **Total failures** | 26 | 100% | |

**Key finding:** Bucket 2 dominates. Per the Phase A acceptance criterion, Phase C (re-ranking) is low-value for the majority of failures. Phase D (prompt fix) is the highest-leverage intervention. Phase C remains worth a targeted implementation for the 7 Bucket 1 failures.

---

## Bucket 1: Missing Table in Context (7 failures)

All 7 are cross-domain FK traversals the expansion did not reach.

| Question | Missing table | Domain | Pattern |
|---|---|---|---|
| ds_0009 | `sys_users` | crm | "names of users who created crm_renewal_proposals" |
| ds_0016 | `sys_permissions` | sys | "total amount of api_calls_lines for each permission" |
| ds_0024 | `sys_users` | proj | "names of users who created active project types" |
| ds_0030 | `sys_users` | log | "names of users who created active shipping methods" |
| ds_0039 | `sys_users` | log | "names of users who created log_carrier_bookings" |
| ds_0043 | `fin_accounts` | fin | "total amount of invoices for each account" (only 4 expansion tables) |
| ds_0051 | `sys_users` | log | "names of users who created active routes" |

**Pattern:** 5 of 7 are "names of users who created X" questions requiring a JOIN to `sys_users`. The expansion seeds were FK columns (`created_by_id`) on domain tables, but the expansion did not traverse to `sys_users` because:

1. `sys_users` is in the `sys` domain while the seeded table is in a different domain.
2. The FK walk follows highest-confidence edges first. The `created_by_id` column on domain tables likely has low FK confidence against `sys_users` (the column name is generic and the domain crossing reduces confidence).
3. The 15-table cap is exhausted by intra-domain neighbors before the cross-domain join target is reached.

**ds_0043** is a different case: `fin_accounts` was not reached and the expansion only produced 4 tables total. The seeds (`fin_invoices_lines`, `fin_invoices`) did not have a strong FK path to `fin_accounts`.

---

## Bucket 2: Table Present, SQL Wrong (19 failures)

All required tables were in `expansion_tbl_ids`. The model generated semantically incorrect or format-mismatched SQL.

### Sub-pattern A: Extra ID column in SELECT and GROUP BY (12 failures)

The model groups by `(id, name)` instead of just `(name)`, and adds the ID column to the SELECT list. Since the grader compares result schemas, the extra column causes a mismatch.

| Question | Extra column | Domain |
|---|---|---|
| ds_0002 | adds `p.id` to GROUP BY/SELECT | proj |
| ds_0004 | adds `customer_id` to SELECT, groups by `(id, name)` | crm |
| ds_0022 | adds `employee_id` to SELECT, groups by `(id, name)` | hr |
| ds_0025 | adds `pt.code` to SELECT/GROUP BY | proj |
| ds_0031 | adds `sm.code` to SELECT/GROUP BY | log |
| ds_0034 | adds `product_id` to SELECT, groups by `(id, name)` | inv |
| ds_0046 | adds `service_id` to SELECT, groups by `(id, name)` | svc |
| ds_0052 | adds `route_id` to SELECT, groups by `(id, name)` | log |
| ds_0055 | adds `service_name` to SELECT (reference only had `id`) | svc |
| ds_0058 | adds `production_line_id` to SELECT, groups by `(id, name)` | mfg |
| ds_0010 | adds `rt.code` to SELECT/GROUP BY | crm |
| ds_0037 | adds `p.id` to SELECT/GROUP BY + uses LEFT JOIN | crm |

**Root cause:** The model follows "good practice" SQL conventions (always include the primary key in output) rather than the reference SQL's minimal-output style. This is a prompt instruction gap.

### Sub-pattern B: Extra SELECT columns (4 failures)

The model returns many more columns than the question requires.

| Question | Issue | Domain |
|---|---|---|
| ds_0018 | Adds `id, reference_number, status, amount, currency, notes` | mfg |
| ds_0045 | Adds `activity_id, reference_number, event_date, status, amount, currency, notes` | svc |
| ds_0057 | Adds `work_order_id, status, event_date, amount` | mfg |
| ds_0027 | Uses SELECT specific columns instead of `SELECT *` (reference used `hr_monthly_kpis.*`) | hr |

### Sub-pattern C: Semantic errors (2 failures)

| Question | Error | Domain |
|---|---|---|
| ds_0019 | Groups by `(pl.id, pl.name, md.currency)` — splits per-currency, so multi-currency lines are not summed together | mfg |
| ds_0037 | Uses LEFT JOIN instead of INNER JOIN — includes personas with no referral submissions (nulls in total) | crm |

### Sub-pattern D: DISTINCT mismatch (2 failures, borderline Bucket 3)

| Question | Issue | Domain |
|---|---|---|
| ds_0015 | Generated uses DISTINCT, reference does not — result differs if user created multiple permissions | sys |
| ds_0048 | Generated uses DISTINCT, reference does not — result differs if user created multiple invoices | fin+sys |

---

## Domain Distribution of Failures

| Domain | Failures | Notes |
|---|---|---|
| crm | 4 | ds_0004, ds_0009, ds_0010, ds_0037 |
| fin | 2 | ds_0043, ds_0048 |
| hr | 2 | ds_0022, ds_0027 |
| inv | 1 | ds_0034 |
| log | 5 | ds_0030, ds_0031, ds_0039, ds_0051, ds_0052 |
| mfg | 4 | ds_0018, ds_0019, ds_0057, ds_0058 |
| proj | 3 | ds_0002, ds_0024, ds_0025 |
| sales | 0 | — |
| svc | 3 | ds_0045, ds_0046, ds_0055 |
| sys | 2 | ds_0015, ds_0016 |

Failures are spread across all 10 domains with no strong clustering. The first 5 alphabetical domains (crm, fin, hr, inv, log) account for 14/26 = 54%, the last 5 (mfg, proj, sales, svc, sys) account for 12/26 = 46%. This is close to uniform.

---

## Recommendations

### 1. Phase D first: Fix the prompt (addresses 73% of failures)

The dominant failure is the model generating SQL with extra output columns. Add an explicit instruction to the client prompt:

> Return only the columns and grouping dimensions that the question explicitly asks for. Do not add primary key columns, ORDER BY clauses, or extra grouping dimensions unless the question requires them. If the question asks for "total amount for each X", GROUP BY the name column of X only.

This single instruction would likely fix 12-15 of the 19 Bucket 2 failures immediately.

### 2. Phase C (limited scope): Fix cross-domain FK traversal for `sys_users`

All 5 "created_by" missing-table failures point to the same root cause: the FK walk does not reliably reach `sys_users` from other domains. Two targeted fixes:

**Option A (simpler):** Add `sys_users` as a forced inclusion whenever `created_by_id` or `approved_by_id` appears in any seed column. This is a special-case rule in the expansion logic, not a full re-ranking.

**Option B (generalizes):** Combined FK+cosine re-ranking as described in Phase C. The question "names of users who created X" has high cosine similarity to `sys_users` (the word "users" in the question maps directly to the table). Re-ranking by combined score would surface `sys_users` over intra-domain neighbors.

Option A is two lines of code. Option B is the full Phase C implementation. Start with Option A.

### 3. Grader tolerance (addresses 2 borderline failures)

Consider updating the grader to ignore column-order differences and accept result sets that are supersets of the reference (i.e., extra columns are OK, wrong values are not). This would recover ds_0015 and ds_0048 (DISTINCT) and several sub-pattern A cases where `(id, name)` grouping is semantically equivalent to `(name)` grouping.
