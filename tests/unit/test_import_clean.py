from __future__ import annotations

import subprocess
import sys
import textwrap
from typing import Any


def test_public_import_does_not_require_spark_modules() -> None:
    code = textwrap.dedent(
        """
        import importlib.abc
        import sys

        class BlockSpark(importlib.abc.MetaPathFinder):
            def find_spec(self, fullname, path=None, target=None):
                blocked = ("pyspark", "py4j", "delta")
                if fullname in blocked or fullname.startswith(tuple(f"{name}." for name in blocked)):
                    raise ModuleNotFoundError(fullname)
                return None

        sys.meta_path.insert(0, BlockSpark())

        import dbxcarta

        assert "run_dbxcarta" in dbxcarta.__all__
        assert "run_client" in dbxcarta.__all__
        """
    )
    subprocess.run([sys.executable, "-c", code], check=True)


def test_run_dbxcarta_accepts_explicit_settings_and_spark(monkeypatch) -> None:
    from dbxcarta import Settings
    import dbxcarta.ingest.pipeline as pipeline

    settings = Settings(
        dbxcarta_catalog="main",
        dbxcarta_schemas="schema_one,schema_two",
        dbxcarta_summary_volume="/Volumes/main/default/dbxcarta/summaries",
        dbxcarta_summary_table="main.default.dbxcarta_runs",
    )
    fake_spark = object()
    calls: dict[str, Any] = {}

    def fake_run(spark, passed_settings, schema_list, summary) -> None:
        calls["run"] = (spark, passed_settings, schema_list, summary.status)

    def fake_emit(summary, spark, volume_path, table_name) -> None:
        calls["emit"] = (spark, volume_path, table_name, summary.status)

    monkeypatch.setattr(pipeline, "_run", fake_run)
    monkeypatch.setattr(pipeline.RunSummary, "emit", fake_emit)

    pipeline.run_dbxcarta(settings=settings, spark=fake_spark)

    assert calls["run"] == (
        fake_spark,
        settings,
        ["schema_one", "schema_two"],
        "running",
    )
    assert calls["emit"] == (
        fake_spark,
        "/Volumes/main/default/dbxcarta/summaries",
        "main.default.dbxcarta_runs",
        "success",
    )
