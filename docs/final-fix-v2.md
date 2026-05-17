# Final Fix v2: what the post-fix client eval shows still needs fixing

Scope: what the fresh client eval (run `938043512057347`, artifacts
`dbxcarta_client_local_20260517T020424Z.json`) tells us after the
graph_retriever Defect A/B fix landed. Grading is a result-set
comparison of each arm's SQL against the per-question
`reference_sql` in `questions.json`.

## Headline

No remaining harness or pipeline code defect is surfaced by these
tests. The only defect, graph_rag returning empty context and
emitting an underscored catalog, is fixed and verified end-to-end
(graph_rag 0/12 to 12/12 executed, 100% correct). Every other
number in the run is the eval measuring what it is designed to
measure, not a bug.

| Arm | exec | correct | Is it a defect? | Action |
| --- | --- | --- | --- | --- |
| `no_context` | 2/12 | 100% (2/2) | No. Expected control behavior. | None |
| `schema_dump` | 12/12 | 83.3% (10/12) | No. Real arm-quality gap, the eval's intended signal. | None in code |
| `graph_rag` | 12/12 | 100% (12/12) | Fixed and verified. | None |

## no_context: the 16.7% execution rate is expected, not a regression

`no_context` is the control arm. It receives the question with no
schema, so it guesses identifiers. The 10 non-executing questions
fail for exactly that reason:

- 6 `UNRESOLVED_COLUMN`: invented column names such as
  `merchant_category`, `account_type`, `is_fraud_ring_member`,
  `account_region`, `graph_risk_score`.
- 4 `TABLE_OR_VIEW_NOT_FOUND`: invented table names such as
  `gold_accounts` qualified into the silver catalog, `communities`,
  `fraud_rings`, `account_similarity`.

This is the floor the other arms are measured against. There is
nothing to fix here. It is documented so the low rate is not
mistaken for a defect on a later read.

## schema_dump: the 2 misses are a quality gap, not a code bug

Both `schema_dump` misses execute cleanly and return rows. They are
graded incorrect because the model chose a plausible but wrong
table or column when handed the full multi-catalog dump, and the
result set diverges from the gold `reference_sql`.

- **fg_q10, "average graph risk score by account region."**
  Reference uses `gold_accounts.risk_score` grouped by `region`.
  graph_rag matched it. schema_dump joined `accounts.region` to
  `account_graph_features.risk_score`, the bronze graph-feature
  score, a different column with a different result.
- **fg_q12, "share of accounts in ring candidate communities by
  region."** Reference joins `gold_accounts` to
  `gold_fraud_ring_communities` on `community_id`. graph_rag used
  the denormalized `gold_accounts.is_ring_community` and produced a
  result-equivalent answer. schema_dump routed the join through
  `account_graph_features.community_id`, a different path with a
  different result.

This is the comparison working as intended. The full schema dump
exposes several `risk_score`-like and community-related tables
across bronze, silver, and gold, and the model picks an
un-curated path. graph_rag's retrieval scopes the context to the
gold curated tables and answers correctly. The result is the
signal the eval exists to produce: graph_rag 100 percent,
schema_dump 83.3 percent, no_context as the floor. Forcing
schema_dump higher would mean changing the dump arm to bias toward
gold tables, which would defeat the purpose of the baseline.
No code change is warranted.

## Conclusion

From these tests, nothing else needs a code fix. The graph_rag
defect is closed and verified. The two remaining sub-100 numbers
are, on inspection, the control floor and the intended
schema_dump-versus-graph_rag quality delta, not defects.

## Not from the tests (operational follow-ups)

These are closure items, not findings from the eval:

- The graph_retriever fix, its regression tests, and the runbook
  updates are uncommitted on branch `layers`. They need a commit
  when you decide to land them.
- `.env` repeatedly reverts to the schemapile/dense overlay between
  sessions. Any future finance-genie run needs it reconciled to the
  medallion overlay with `DBXCARTA_VERIFY_GATE=false` and the
  DBXcarta cluster first. This is the same operational hazard
  recorded in `take-4-finance-genie.md`.
