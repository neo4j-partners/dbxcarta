from __future__ import annotations

import ast
import subprocess
import sys
from pathlib import Path


_REPO_ROOT = Path(__file__).resolve().parents[2]


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


def _imported_modules(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(), filename=str(path))
    modules: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            modules.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            modules.add(node.module)
    return modules


def _layer_source_files(layer: str) -> list[Path]:
    package_root = (
        _REPO_ROOT
        / "packages"
        / f"dbxcarta-{layer}"
        / "src"
        / "dbxcarta"
        / layer
    )
    return sorted(package_root.rglob("*.py"))


def _matches_forbidden(module: str, forbidden: tuple[str, ...]) -> bool:
    return any(
        module == prefix or module.startswith(f"{prefix}.") for prefix in forbidden
    )


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
        "dbxcarta.client",
        "dbxcarta.presets",
    }

    leaked = _matching_modules(_loaded_modules_after("import dbxcarta.spark"), forbidden)

    assert not leaked, f"Forbidden modules loaded by dbxcarta.spark: {sorted(leaked)}"


def test_source_imports_preserve_core_and_extension_boundaries() -> None:
    forbidden_by_layer = {
        "core": ("dbxcarta.spark", "dbxcarta.client", "dbxcarta.presets"),
        "spark": ("dbxcarta.client", "dbxcarta.presets"),
        "client": ("dbxcarta.spark", "dbxcarta.presets"),
    }
    violations: list[str] = []

    for layer, forbidden in forbidden_by_layer.items():
        for path in _layer_source_files(layer):
            for module in sorted(_imported_modules(path)):
                if _matches_forbidden(module, forbidden):
                    rel = path.relative_to(_REPO_ROOT)
                    violations.append(f"{rel}: imports {module}")

    assert not violations, "Forbidden cross-layer imports:\n" + "\n".join(violations)


# Note: dbxcarta.presets intentionally depends on dbxcarta.spark and
# dbxcarta.client (Decision 2 in docs/proposal/clean-boundaries.md). It is
# the operational umbrella that wires extensions together at the application
# boundary, so a "presets must not import other extensions" check would
# contradict the design.
