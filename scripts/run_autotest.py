"""Automated end-to-end integration test harness for DBxCarta.

Phases:
  0  Preflight      — workspace + warehouse connectivity
  1  Unit tests     — fast pytest suite
  2  Schema setup   — teardown + fixture DDL
  3  Ingest run     — upload, submit, download RunSummary
  4  Assertions     — validate RunSummary contents
  5  Output JSON    — write results to volume

Usage:
    uv run python scripts/run_autotest.py
"""

from __future__ import annotations

import io
import json
import os
import re
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).parent.parent
load_dotenv(PROJECT_ROOT / ".env")

FIXTURE_SCHEMAS = [
    "dbxcarta_test_sales",
    "dbxcarta_test_inventory",
    "dbxcarta_test_hr",
    "dbxcarta_test_events",
]


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _make_ws():
    from databricks.sdk import WorkspaceClient
    profile = os.environ.get("DATABRICKS_PROFILE")
    return WorkspaceClient(profile=profile) if profile else WorkspaceClient()


def _run(cmd: list[str], *, env_overrides: dict[str, str] | None = None) -> subprocess.CompletedProcess:
    env = {**os.environ, **(env_overrides or {})}
    result = subprocess.run(cmd, capture_output=True, text=True, env=env, cwd=PROJECT_ROOT)
    if result.stdout:
        print(result.stdout, end="")
    if result.stderr:
        print(result.stderr, end="", file=sys.stderr)
    return result


# ---------------------------------------------------------------------------
# Phase 0 — Preflight
# ---------------------------------------------------------------------------

def phase0_preflight() -> dict:
    print("\n=== Phase 0: Preflight ===")
    errors: list[str] = []

    warehouse_id = os.environ.get("DATABRICKS_WAREHOUSE_ID", "")
    if not warehouse_id:
        errors.append("DATABRICKS_WAREHOUSE_ID not set")

    catalog = os.environ.get("DBXCARTA_CATALOG", "")
    if catalog != "dbxcarta-catalog":
        errors.append(f"DBXCARTA_CATALOG={catalog!r}, expected 'dbxcarta-catalog'")

    cluster_id = os.environ.get("DATABRICKS_CLUSTER_ID", "")
    compute_mode = os.environ.get("DATABRICKS_COMPUTE_MODE", "")
    if not cluster_id and compute_mode != "serverless":
        errors.append(
            "No compute configured: DATABRICKS_CLUSTER_ID unset and "
            "DATABRICKS_COMPUTE_MODE != serverless"
        )

    if errors:
        for e in errors:
            print(f"  ERROR: {e}")
        return {"status": "fail", "errors": errors}

    try:
        from dbxcarta.client.executor import preflight_warehouse

        ws = _make_ws()
        preflight_warehouse(ws, warehouse_id)
        print(f"  Warehouse {warehouse_id}: OK")
    except Exception as exc:
        return {"status": "fail", "errors": [str(exc)]}

    return {"status": "pass"}


# ---------------------------------------------------------------------------
# Phase 1 — Unit Test Gate
# ---------------------------------------------------------------------------

def phase1_unit_tests() -> dict:
    print("\n=== Phase 1: Unit Test Gate ===")
    result = _run([
        "uv", "run", "pytest", "tests/", "-x", "-q",
        "--ignore=tests/schema_graph",
        "--ignore=tests/sample_values",
        "--ignore=tests/integration",
    ])

    passed = 0
    failed = 0
    for line in result.stdout.splitlines():
        m = re.search(r"(\d+) passed", line)
        if m:
            passed = int(m.group(1))
        m = re.search(r"(\d+) failed", line)
        if m:
            failed = int(m.group(1))

    status = "pass" if result.returncode == 0 else "fail"
    return {"status": status, "passed": passed, "failed": failed}


# ---------------------------------------------------------------------------
# Phase 2 — Schema Teardown and Setup
# ---------------------------------------------------------------------------

def phase2_schema_setup() -> dict:
    print("\n=== Phase 2: Schema Teardown and Setup ===")
    catalog = os.environ["DBXCARTA_CATALOG"]
    volume_path = os.environ.get("DATABRICKS_VOLUME_PATH", "")

    # Teardown — best-effort (ignore returncode)
    print("  Teardown...")
    _run(
        ["uv", "run", "python", "scripts/run_demo.py", "--catalog", catalog, "--teardown"],
        env_overrides={"DATABRICKS_VOLUME_PATH": ""},
    )

    # Setup — external schema is out of scope for the autotest (no FKs, requires
    # cloud storage path). Clear DATABRICKS_VOLUME_PATH so run_demo.py skips it.
    print("  Setup...")
    result = _run(
        ["uv", "run", "python", "scripts/run_demo.py", "--catalog", catalog],
        env_overrides={"DATABRICKS_VOLUME_PATH": ""},
    )

    if result.returncode != 0:
        return {"status": "fail", "error": "Schema setup failed"}

    ok_count = result.stdout.count("] OK ")
    err_count = result.stdout.count("] ERR ")
    return {"status": "pass", "statements_ok": ok_count, "statements_failed": err_count}


# ---------------------------------------------------------------------------
# Phase 3 — Ingest Run
# ---------------------------------------------------------------------------

def phase3_ingest_run() -> dict:
    print("\n=== Phase 3: Ingest Run ===")

    summary_volume = os.environ.get("DBXCARTA_SUMMARY_VOLUME", "")
    if not summary_volume:
        return {"status": "fail", "error": "DBXCARTA_SUMMARY_VOLUME not set"}

    from databricks_job_runner.download import download_file, list_volume_files

    ws = _make_ws()

    print("  Uploading wheel...")
    r = _run(["uv", "run", "dbxcarta", "upload", "--wheel"])
    if r.returncode != 0:
        return {"status": "fail", "error": "wheel upload failed"}

    print("  Uploading scripts...")
    r = _run(["uv", "run", "dbxcarta", "upload", "--all"])
    if r.returncode != 0:
        return {"status": "fail", "error": "scripts upload failed"}

    files_before = set(list_volume_files(ws, summary_volume))

    print("  Submitting pipeline run...")
    schemas_val = ",".join(FIXTURE_SCHEMAS)
    r = _run(
        ["uv", "run", "dbxcarta", "submit", "run_dbxcarta.py"],
        env_overrides={"DBXCARTA_SCHEMAS": schemas_val},
    )
    if r.returncode != 0:
        return {"status": "fail", "error": "pipeline run failed"}

    run_id: str | None = None
    for line in r.stdout.splitlines():
        m = re.search(r"Run ID:\s+(\d+)", line)
        if m:
            run_id = m.group(1)
            break

    if not run_id:
        return {"status": "fail", "error": "Could not parse run_id from submit output"}

    print(f"  run_id={run_id}")

    files_after = set(list_volume_files(ws, summary_volume))
    new_files = files_after - files_before
    summary_pat = re.compile(r"^dbxcarta_.*\.json$")
    matches = sorted(f for f in new_files if summary_pat.match(f))
    if not matches:
        return {
            "status": "fail",
            "run_id": run_id,
            "error": (
                f"No new RunSummary JSON written to {summary_volume} after run {run_id}. "
                f"New files seen: {sorted(new_files)!r}"
            ),
        }

    filename = matches[-1]  # lexicographic sort puts newest YYYYMMDDTHHMMSSZ last
    remote_path = filename if filename.startswith("/Volumes") else f"{summary_volume}/{filename}"

    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as tmp:
        local_path = Path(tmp.name)

    try:
        download_file(ws, remote_path, local_path)
        run_summary = json.loads(local_path.read_text())
    finally:
        local_path.unlink(missing_ok=True)

    print(f"  RunSummary: {filename}")
    return {"status": "pass", "run_id": run_id, "run_summary": run_summary}


# ---------------------------------------------------------------------------
# Phase 4 — Assertions
# ---------------------------------------------------------------------------

def phase4_assertions(run_summary: dict) -> dict:
    print("\n=== Phase 4: Assertions ===")
    failed: list[str] = []

    def assert_eq(key: str, actual, expected) -> None:
        if actual != expected:
            failed.append(f"{key}: expected {expected!r}, got {actual!r}")

    def assert_ge(key: str, actual, minimum) -> None:
        if actual is None or actual < minimum:
            failed.append(f"{key}: expected >= {minimum}, got {actual!r}")

    def assert_present(key: str, value) -> None:
        if not value:
            failed.append(f"{key}: expected non-empty, got {value!r}")

    assert_eq("status", run_summary.get("status"), "success")
    assert_eq("error", run_summary.get("error"), None)

    rc = run_summary.get("row_counts", {})
    assert_ge("row_counts.schemas", rc.get("schemas"), 4)
    assert_ge("row_counts.tables", rc.get("tables"), 19)
    assert_ge("row_counts.fk_declared", rc.get("fk_declared"), 16)
    assert_ge("row_counts.fk_edges", rc.get("fk_edges"), 16)
    assert_present("neo4j_counts", run_summary.get("neo4j_counts"))

    for f in failed:
        print(f"  FAIL: {f}")
    if not failed:
        print("  All assertions passed.")

    return {"status": "pass" if not failed else "fail", "failed": failed}


# ---------------------------------------------------------------------------
# Phase 5 — Output JSON
# ---------------------------------------------------------------------------

def phase5_write_output(phases: dict, catalog: str) -> dict:
    print("\n=== Phase 5: Write Output JSON ===")
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    overall_phases = {k: v for k, v in phases.items() if k != "output"}
    overall = "pass" if all(p["status"] == "pass" for p in overall_phases.values()) else "fail"

    output: dict = {
        "autotest_run_at": _now_iso(),
        "catalog": catalog,
        "fixture_schemas": FIXTURE_SCHEMAS,
        "phases": phases,
        "overall": overall,
    }
    if overall == "fail":
        output["failed_phases"] = [k for k, v in overall_phases.items() if v["status"] != "pass"]

    summary_volume = os.environ.get("DBXCARTA_SUMMARY_VOLUME", "")
    if not summary_volume:
        print("  WARNING: DBXCARTA_SUMMARY_VOLUME not set — skipping volume write")
        return {"status": "pass", "written": False}

    ws = _make_ws()
    remote_path = f"{summary_volume}/autotest/autotest_results_{ts}.json"
    content = json.dumps(output, indent=2).encode()

    ws.files.upload(remote_path, io.BytesIO(content), overwrite=True)
    print(f"  Written: {remote_path}")
    return {"status": "pass", "path": remote_path}


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def _abort(phases: dict) -> None:
    failed = [k for k, v in phases.items() if v["status"] != "pass"]
    print(f"\nAborted — failed phases: {failed}")
    sys.exit(1)


def main() -> None:
    phases: dict[str, dict] = {}

    phases["preflight"] = phase0_preflight()
    if phases["preflight"]["status"] != "pass":
        _abort(phases)

    phases["unit_tests"] = phase1_unit_tests()
    if phases["unit_tests"]["status"] != "pass":
        _abort(phases)

    phases["schema_setup"] = phase2_schema_setup()
    if phases["schema_setup"]["status"] != "pass":
        _abort(phases)

    ingest = phase3_ingest_run()
    run_summary = ingest.pop("run_summary", {})
    phases["ingest_run"] = ingest
    if ingest["status"] != "pass":
        _abort(phases)

    phases["ingest_run"]["run_summary"] = run_summary
    phases["assertions"] = phase4_assertions(run_summary)

    phases["output"] = phase5_write_output(phases, os.environ.get("DBXCARTA_CATALOG", ""))

    overall = "pass" if all(p["status"] == "pass" for p in phases.values()) else "fail"
    print(f"\n{'=' * 50}")
    print(f"Overall: {overall.upper()}")
    for name, result in phases.items():
        print(f"  {name}: {result['status']}")

    sys.exit(0 if overall == "pass" else 1)


if __name__ == "__main__":
    main()
