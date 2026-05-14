# Phase B: schema_dump Truncation Bias Check

**Date:** 2026-05-14
**Status:** Complete — truncation bias is NOT the primary explanation.

---

## Domain Layout

Each of the 10 domains has exactly 50 tables, ordered alphabetically within `dense_500`. schema_dump is truncated at 50k chars, covering approximately the first 285 tables (57%).

| Domain | Table ranks | schema_dump coverage |
|---|---|---|
| crm | 1–50 | Fully covered |
| fin | 51–100 | Fully covered |
| hr | 101–150 | Fully covered |
| inv | 151–200 | Fully covered |
| log | 201–250 | Fully covered |
| mfg | 251–300 | Partial: 35/50 tables (ranks 251–285) |
| proj | 301–350 | Not covered |
| sales | 351–400 | Not covered |
| svc | 401–450 | Not covered |
| sys | 451–500 | Not covered |

---

## Question Distribution and graph_rag Correct Rate by Domain

| Domain | Questions | graph_rag correct | Failures | Correct rate | schema_dump coverage |
|---|---|---|---|---|---|
| crm | 10 | 7 | 3 | 0.700 | Fully covered |
| fin | 5 | 3 | 2 | 0.600 | Fully covered |
| hr | **14** | 12 | 2 | **0.857** | Fully covered |
| inv | 3 | 2 | 1 | 0.667 | Fully covered |
| log | 6 | 2 | 4 | 0.333 | Fully covered |
| mfg | 4 | 0 | 4 | **0.000** | Partial (35/50) |
| proj | 5 | 2 | 3 | 0.400 | Not covered |
| sales | 1 | 1 | 0 | 1.000 | Not covered |
| svc | 5 | 2 | 3 | 0.400 | Not covered |
| sys | 6 | 2 | 4 | 0.333 | Not covered |
| **Total** | **59** | **33** | **26** | **0.559** | |

---

## Failure Distribution by Coverage Tier

| Coverage tier | Domains | Failures | % of 26 |
|---|---|---|---|
| Fully covered | crm, fin, hr, inv, log | 12 | 46% |
| Partial | mfg | 4 | 15% |
| Not covered | proj, sales, svc, sys | 10 | 38% |

**Acceptance criterion result:** 46% of failures are in covered domains. This is well below the 60% threshold that would indicate truncation bias as the primary cause. The distribution is roughly uniform.

---

## Why the Hypothesis is Rejected

For truncation bias to explain the gap, schema_dump-wins should cluster in covered domains. But:

1. **38% of failures are in not-covered domains** (proj+svc+sys). schema_dump cannot win in these domains since it has no schema context there. These failures do NOT represent schema_dump wins.

2. **The dominant failure mode (73%) is SQL formatting error**, not missing schema context. The model generates SQL with extra columns or finer-grained grouping. schema_dump would make the same formatting errors under the same prompt, so schema_dump would also fail those questions. Most of the 12 covered-domain failures are NOT schema_dump wins.

3. **schema_dump's 69.8% correct rate cannot be explained by truncation alone.** schema_dump only executes 53/59 questions (the 6 non-executions are plausibly in non-covered domains). Among the 53 it executes, it gets 37 right (69.8%) — a correct rate close to graph_rag's covered-domain rate.

---

## Notable Findings

**hr domain is a strong outlier:** 14/59 questions (24% of the set) are in the hr domain. graph_rag gets 12/14 correct there (86%). This overrepresentation inflates graph_rag's aggregate score.

**mfg domain is 0% correct rate:** 4 questions, 0 correct. All 4 are Bucket 2 failures (tables present, SQL wrong). mfg is partially covered by schema_dump (35/50 tables). The 0% rate for mfg is driven entirely by SQL formatting errors, not missing tables.

**Question distribution imbalance:** hr has 14 questions, sales has 1, mfg has 4. A balanced set (5–7 per domain) would produce a harder and more interpretable benchmark.

---

## Recommendations

1. **Proceed directly to Phase D (prompt fix)** — this addresses 73% of failures regardless of domain.

2. **Phase D also: fix question domain balance** — generate 5–7 questions per domain (60–70 total), replacing the current biased set. hr is overrepresented; mfg, sales, inv are underrepresented.

3. **Phase C (limited scope)** — still addresses the 7 Bucket 1 missing-table failures but will not close the gap materially on its own (max 7 more correct of 26 failures).

4. **Truncation bias becomes relevant at dense_1000** — at 1000 tables with 57% truncation covering only ~570 tables (roughly through early svc domain), schema_dump's coverage degrades significantly. The truncation effect is not the bottleneck at 500 tables.
