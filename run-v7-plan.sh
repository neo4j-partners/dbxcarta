#!/bin/bash
# Run the DBxCarta v7 implementation plan as a single Claude Code web session.
# Usage: ./run-v7-plan.sh
#
# This script launches one Claude Code remote session that works through the
# full v7 checklist in order:
#
#   Stage 4b  — Confirm verification suites green
#   Stage 7.1 — Re-embedding ledger
#   Stage 7.2 — Incremental scope via last_altered
#   Stage 7.3 — Parallel node writes via repartition
#
# Prerequisites:
#   - Claude Code CLI installed and authenticated
#   - GitHub account connected at claude.ai/code
#   - Claude GitHub app installed on this repository
#   - Cloud environment configured with Databricks and Neo4j secrets:
#       DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_WAREHOUSE_ID,
#       NEO4J_URI, NEO4J_USERNAME, NEO4J_PASSWORD
#     Set these at claude.ai/code → Settings → Environments before running.
#     Stage 4b runs pytest against live Databricks and Neo4j — without these
#     secrets the test suite will fail to authenticate.
#
# Monitor progress:
#   - Run /tasks in Claude Code CLI
#   - Visit claude.ai/code to see the session (named 'dbxcarta-v7-plan')
#   - Use the Claude mobile app
#
# When the session completes:
#   - Review diffs at claude.ai/code
#   - Use 'claude --teleport' to pull changes locally
#   - Commit and push from the local working tree

set -euo pipefail

echo "=== DBxCarta v7 Plan — Single-Session Full Checklist ==="
echo ""
echo "Launching one Claude Code web session to work through all v7 stages..."
echo ""

claude -n "dbxcarta-v7-plan" --remote "## Task: Implement the DBxCarta v7 Plan

Read \`docs/dbxcarta-v7-plan.md\` in full. It is the canonical plan and contains
every design decision, checklist item, and verification step. Work through the
checklist in order — Stage 4b first, then 7.1, 7.2, 7.3 — completing each item
before moving to the next. Do not skip items.

Also read:
- \`worklog/dbxcarta-v5-plan.md\` lines 1–110 for the goal statement and
  verification criteria
- \`docs/best-practices.md\` for project-level design rules
- \`src/dbxcarta/pipeline.py\`, \`src/dbxcarta/summary.py\`, and the test files
  each stage touches before modifying them

After completing each stage, run \`/compact\` to free context before starting
the next one."

echo ""
echo "=== Session launched ==="
echo ""
echo "Stages covered (in order):"
echo "  4b  — Confirm verification suites green"
echo "  7.1 — Re-embedding ledger"
echo "  7.2 — Incremental scope via last_altered"
echo "  7.3 — Parallel node writes via repartition"
echo ""
echo "Monitor progress:"
echo "  - Run /tasks in Claude Code CLI"
echo "  - Visit claude.ai/code to see the session (named 'dbxcarta-v7-plan')"
echo "  - Use the Claude mobile app"
echo ""
echo "When complete:"
echo "  - Review diffs at claude.ai/code"
echo "  - Use 'claude --teleport' to pull changes locally"
echo "  - Commit and push from the local working tree"
