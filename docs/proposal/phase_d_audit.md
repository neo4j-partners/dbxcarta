# Phase D: Question Audit

**Date:** 2026-05-14
**Status:** Complete — question set has critical quality problems requiring regeneration.

---

## Summary

| Check | Threshold | Actual | Status |
|---|---|---|---|
| Reference SQL executes and returns results | >=50/59 | 59/59 | PASS |
| Multi-table questions (2+ tables) | >=35/59 (59%) | 34/59 (58%) | barely PASS |
| 3+ table questions | >0 | 0/59 (0%) | FAIL |
| Single-table questions | <20% | 25/59 (42%) | FAIL |
| Duplicate/near-duplicate questions | 0 | 12+ near-exact duplicates | FAIL |
| Domain balance (no domain <3, no domain >10) | met | hr=14 (24%), sales=1 (2%) | FAIL |

---

## Finding 1: 42% Single-Table Questions

25 of 59 reference SQLs query exactly one table. These test basic column lookup or filtering, not multi-table retrieval.

| Question ID | Question | Table |
|---|---|---|
| ds_0000 | Active employees (name) | hr_employees |
| ds_0003 | Active crm customers (name) | crm_customers |
| ds_0005 | Active employees (name) | hr_employees |
| ds_0007 | Total invoice amount by currency | fin_invoices |
| ds_0008 | Active crm relationship types | crm_relationship_types |
| ds_0011 | Active employees (name) | hr_employees |
| ds_0013 | Total amount of service cases | svc_service_cases |
| ds_0014 | Active sys users | sys_users |
| ds_0017 | Active employees (name) | hr_employees |
| ds_0020 | Active employees (name) | hr_employees |
| ds_0023 | Active project types | proj_project_types |
| ds_0026 | Active employees (SELECT *) | hr_employees |
| ds_0028 | Total approved sys_data_exports | sys_data_exports |
| ds_0029 | Active shipping methods | log_shipping_methods |
| ds_0032 | Active products | inv_products |
| ds_0035 | Active crm customers (name) | crm_customers |
| ds_0038 | Active commission plans | sales_commission_plans |
| ds_0040 | Total amount of svc case resolutions | svc_case_resolutions |
| ds_0041 | Active employees (name) | hr_employees |
| ds_0044 | Active employees (name) | hr_employees |
| ds_0047 | Active financial accounts | fin_accounts |
| ds_0049 | Total invoice amount by currency | fin_invoices |
| ds_0050 | Active routes | log_routes |
| ds_0053 | Active customers (name) | crm_customers |
| ds_0056 | Active employees (name) | hr_employees |

---

## Finding 2: Severe Near-Duplicate Questions

Multiple questions are semantically or textually identical.

**Group A: "Active employees" — 9 questions (ds_0000, ds_0005, ds_0011, ds_0017, ds_0020, ds_0026, ds_0041, ds_0044, ds_0056)**

Seven are word-for-word identical: "What are the names of active employees in the hr_employees table?" with `SELECT name FROM hr_employees WHERE is_active = TRUE`. ds_0026 uses `SELECT *` and ds_0011/ds_0020 use minor phrasing variants. All 9 produce the same result.

**Group B: "Active crm customers" — 3 questions (ds_0003, ds_0035, ds_0053)**

All return names of active customers from crm_customers.

**Group C: "Total invoices by currency" — 2 questions (ds_0007, ds_0049)**

Both are `SELECT currency, SUM(amount) FROM fin_invoices GROUP BY currency`. Identical result.

**Effect:** After deduplication these groups collapse from 14 questions to 3, recovering 11 question slots. The hr domain's apparent strength (14 questions, 12 correct) is largely an artifact of this duplication — 8 of those 14 hr questions are the "active employees" variants.

---

## Finding 3: No 3+ Table Questions

All 59 questions join at most 2 tables. graph_rag's FK walk advantage over schema_dump grows with join depth. A benchmark with zero 3+ table questions underestimates graph_rag's potential advantage and over-weights schema_dump's broad-but-shallow coverage.

---

## Finding 4: Domain Imbalance

| Domain | Questions | (%) |
|---|---|---|
| hr | 14 | 24% |
| crm | 10 | 17% |
| sys | 6 | 10% |
| log | 6 | 10% |
| fin | 5 | 8% |
| proj | 5 | 8% |
| svc | 5 | 8% |
| mfg | 4 | 7% |
| inv | 3 | 5% |
| sales | 1 | 2% |

Target: 5–7 per domain for 60 questions total. hr is 3x over-represented; sales, inv, mfg are under-represented.

---

## Finding 5: Reference SQL Quality Issues

Three questions have reference SQLs that are technically valid but produce odd outputs:

- **ds_0026:** `SELECT * FROM hr_employees WHERE is_active = TRUE` — returns all columns. Any generated SQL that returns just names is graded wrong because the result schemas differ.
- **ds_0027:** `SELECT hr_employees.name, hr_monthly_kpis.* FROM ... JOIN ...` — uses `SELECT *` on the right table, making the result schema unstable if column ordering changes.
- **ds_0055:** Groups by `s.id` but the question says "for each service." Returning the service ID instead of the service name is poor reference quality — any model that returns `s.name` is marked wrong.

---

## Root Cause: Question Generator Does Not Deduplicate or Validate

The generator samples BFS subgraphs from random seed tables. `hr_employees.is_active` and `crm_customers.is_active` are FK-adjacent to many subgraphs, so the same trivial question gets re-generated repeatedly. There is no post-generation deduplication or single-table rejection.

---

## Recommended Regeneration Rules

Apply these rules in `question_generator.py` before accepting a generated question:

1. **Reject single-table questions.** The reference SQL must reference at least 2 distinct tables.
2. **Require at least one JOIN.** A `WHERE` predicate on one table is not a retrieval challenge.
3. **Domain-stratify:** Generate exactly 6 questions per domain (60 total). Reject any question from a domain that already has 6 accepted questions.
4. **Deduplicate reference SQL.** Before accepting, compare the normalized reference SQL (strip whitespace, lowercase, remove backticks) to the accepted set. Reject if similarity > 0.9.
5. **Validate reference SQL:** Run the reference SQL against the warehouse. Reject if it errors or returns 0 rows.
6. **Include at least 2 three-table questions per domain** (the BFS subgraph sampler already captures multi-hop subgraphs; use them).

---

## Deduplication of Existing Set (Quick Fix)

Rather than regenerating all 59 questions, deduplicate the existing set first:

1. Drop 8 duplicate "active employees" questions (keep ds_0000, remove ds_0005, ds_0011, ds_0017, ds_0020, ds_0026, ds_0041, ds_0044, ds_0056).
2. Drop 2 duplicate "active customers" questions (keep ds_0003, remove ds_0035, ds_0053).
3. Drop 1 duplicate "invoices by currency" question (keep ds_0007, remove ds_0049).
4. Drop ds_0026 (SELECT * reference) and ds_0027 (SELECT * on join right side).
5. Fix ds_0055: change reference SQL to return `s.name` not `s.id`.

After dedup: **47 questions remain.** Add 13 new questions targeting: mfg (3), inv (3), sales (4), and 3-table joins across any domains.

---

## Impact on Benchmark Validity

With the current question set:
- graph_rag's 33/59 includes 8 trivially-correct "active employees" answers and 2 "active customers" answers — **10 trivial correct answers that inflate the score**.
- If we strip trivially-duplicated questions (collapse 9 "active employees" to 1, 3 "active customers" to 1, 2 "invoices by currency" to 1), the effective question count drops to ~47.
- At 47 questions, graph_rag's **adjusted score** = ~25/47 = **53%** (removing the duplicate bonus) vs schema_dump's adjusted score which also changes.

The benchmark as-is does not reliably differentiate graph_rag vs schema_dump for retrieval quality. Regeneration is required before Phase E (dense_1000) results would be interpretable.
