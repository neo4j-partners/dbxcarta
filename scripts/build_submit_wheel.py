"""Build the self-contained ``dbxcarta-submit`` wheel for publishing.

``dbxcarta-submit`` imports a small subset of ``dbxcarta.core``, but the
published wheel must not depend on the unpublished ``dbxcarta-core``
distribution. This script bundles only the core modules submit imports into
submit's source tree, builds the wheel, verifies the wheel physically carries
``dbxcarta/core``, then removes the copied modules so the working tree is left
unchanged.

This is the narrowed twin of ``_core_bundled_into`` in
``dbxcarta.submit.cli``: that one copies the whole of ``dbxcarta/core`` into the
materialize wheel for the Volume-staging ``publish-wheels`` path; this one
copies a curated subset into the submit wheel for a PyPI build. The source of
truth stays single in ``dbxcarta-core``.

The curated list below must track submit's real imports. The clean-room install
test (``tests/submit/test_submit_wheel_selfcontained.py``) is the drift guard:
it installs the built wheel alone and exercises the submit path, so a missing
module fails there instead of on a cluster.

Run from anywhere::

    uv run python scripts/build_submit_wheel.py
"""

from __future__ import annotations

import shutil
import subprocess
import sys
import zipfile
from pathlib import Path

# The ``dbxcarta.core`` modules submit imports, plus the package marker files.
# Import-closed: ``catalogs`` and ``config`` import only ``identifiers``; none of
# the seven reach ``materialize.py``, ``questions.py``, or ``sql_safety.py``, and
# ``core/__init__.py`` re-exports nothing, so copying this subset cannot drag in
# an excluded module.
_CURATED_CORE_FILES: tuple[str, ...] = (
    "__init__.py",
    "py.typed",
    "catalogs.py",
    "config.py",
    "env.py",
    "executor.py",
    "identifiers.py",
    "volume_io.py",
    "workspace.py",
)

_REPO_ROOT = Path(__file__).resolve().parent.parent
_SUBMIT_DIR = _REPO_ROOT / "packages" / "dbxcarta-submit"
_CORE_SRC = _REPO_ROOT / "packages" / "dbxcarta-core" / "src" / "dbxcarta" / "core"
_BUNDLED_CORE = _SUBMIT_DIR / "src" / "dbxcarta" / "core"


def _copy_curated_core() -> None:
    """Copy the curated core subset into submit's source tree for the build."""
    if _BUNDLED_CORE.exists():
        shutil.rmtree(_BUNDLED_CORE)
    _BUNDLED_CORE.mkdir(parents=True)
    for name in _CURATED_CORE_FILES:
        src = _CORE_SRC / name
        if not src.is_file():
            raise FileNotFoundError(f"curated core file not found: {src}")
        shutil.copy2(src, _BUNDLED_CORE / name)


def _latest_wheel(dist_dir: Path) -> Path:
    """Return the most recently built ``dbxcarta-submit`` wheel under ``dist_dir``."""
    wheels = sorted(dist_dir.glob("dbxcarta_submit-*.whl"), key=lambda p: p.stat().st_mtime)
    if not wheels:
        raise RuntimeError(f"no dbxcarta-submit wheel produced in {dist_dir}")
    return wheels[-1]


def _assert_wheel_bundles_core(wheel_path: Path) -> None:
    """Fail loudly if the built wheel is missing the bundled ``dbxcarta/core``.

    The published wheel declares no ``dbxcarta-core`` dependency, so it must
    physically carry the core modules. A missing core package surfaces here, at
    build time, instead of as an ``ImportError`` on first ``import
    dbxcarta.core`` at runtime.
    """
    with zipfile.ZipFile(wheel_path) as zf:
        names = zf.namelist()
    if not any(n.startswith("dbxcarta/core/") for n in names):
        raise RuntimeError(
            f"built wheel {wheel_path.name} does not bundle dbxcarta/core; "
            "the self-contained submit wheel must physically carry the core modules"
        )


def main() -> int:
    _copy_curated_core()
    try:
        subprocess.run(
            ["uv", "build", "--wheel", "--package", "dbxcarta-submit"],
            cwd=_REPO_ROOT,
            check=True,
        )
    finally:
        # Always remove the copied core, even if the build fails, so the working
        # tree is left exactly as it was found.
        shutil.rmtree(_BUNDLED_CORE, ignore_errors=True)

    wheel = _latest_wheel(_REPO_ROOT / "dist")
    _assert_wheel_bundles_core(wheel)
    print(f"built self-contained wheel: {wheel}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
