from __future__ import annotations

import subprocess
import sys


def _loaded_modules_after(import_statement: str) -> set[str]:
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            f"{import_statement}; import sys; print('\\n'.join(sorted(sys.modules)))",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    return set(result.stdout.splitlines())


def _matching_modules(loaded: set[str], forbidden: set[str]) -> set[str]:
    return {
        module
        for module in loaded
        for prefix in forbidden
        if module == prefix or module.startswith(f"{prefix}.")
    }


def test_top_level_namespace_has_no_compatibility_reexports() -> None:
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import dbxcarta; "
            "print(dbxcarta.__file__); "
            "print(hasattr(dbxcarta, 'run_dbxcarta')); "
            "print(hasattr(dbxcarta, 'Settings')); "
            "print(hasattr(dbxcarta, 'run_client'))",
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    file_value, has_run, has_settings, has_client = result.stdout.splitlines()
    assert file_value == "None"
    assert has_run == "False"
    assert has_settings == "False"
    assert has_client == "False"


def test_core_does_not_load_extension_or_spark_modules() -> None:
    forbidden = {
        "databricks.sdk",
        "dbxcarta.client",
        "dbxcarta.presets",
        "dbxcarta.spark",
        "delta",
        "py4j",
        "pyspark",
    }

    leaked = _matching_modules(_loaded_modules_after("import dbxcarta.core"), forbidden)

    assert not leaked, f"Forbidden modules loaded by dbxcarta.core: {sorted(leaked)}"


def test_client_root_does_not_load_spark_or_databricks_modules() -> None:
    forbidden = {
        "databricks.sdk",
        "dbxcarta.presets",
        "dbxcarta.spark",
        "delta",
        "py4j",
        "pyspark",
    }

    leaked = _matching_modules(_loaded_modules_after("import dbxcarta.client"), forbidden)

    assert not leaked, f"Forbidden modules loaded by dbxcarta.client: {sorted(leaked)}"


def test_spark_root_does_not_load_client_eval_or_presets() -> None:
    forbidden = {
        "dbxcarta.client.eval",
        "dbxcarta.presets",
    }

    leaked = _matching_modules(_loaded_modules_after("import dbxcarta.spark"), forbidden)

    assert not leaked, f"Forbidden modules loaded by dbxcarta.spark: {sorted(leaked)}"
