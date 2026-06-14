"""Clean-room install test for the self-contained ``dbxcarta-submit`` wheel.

The published wheel declares no ``dbxcarta-core`` dependency and instead bundles
the curated core subset submit imports (see ``scripts/build_submit_wheel.py``).
This is the drift guard for that subset: it builds the wheel, installs it alone
in a throwaway virtualenv with no dbxcarta source on the path, and exercises the
submit import graph. If a future change makes submit import an 8th core module
that the curated list omits, in-repo dev still passes because the workspace
carries the whole of core, but this test fails because the lone wheel does not.

Marked ``slow``: it builds a wheel and provisions a fresh environment that
installs ``databricks-job-runner`` from the network, so the default fast suite
skips it (see the pytest ``addopts`` in the root pyproject).
"""

from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[2]
_BUILD_SCRIPT = _REPO_ROOT / "scripts" / "build_submit_wheel.py"
_UV = shutil.which("uv") or "uv"

# Run in the clean venv: import the package and every bundled core module, then
# drive the helper far enough to force the cli -> dbxcarta.core import graph. A
# bogus wheel path raises FileNotFoundError before any network call, so this
# proves the import closure resolves from the lone wheel without a workspace.
_CLEANROOM_CHECK = """
import dbxcarta.submit
from dbxcarta.submit import submit_neocarta_ingest

# Every core module the curated bundle must carry.
import dbxcarta.core.catalogs
import dbxcarta.core.config
import dbxcarta.core.env
import dbxcarta.core.executor
import dbxcarta.core.identifiers
import dbxcarta.core.volume_io
import dbxcarta.core.workspace

from pathlib import Path

try:
    submit_neocarta_ingest(Path("/nonexistent/neocarta-0.0.0-py3-none-any.whl"))
except FileNotFoundError:
    pass
else:
    raise AssertionError("expected FileNotFoundError for a missing wheel")

print("cleanroom-ok")
"""


def _build_wheel() -> Path:
    subprocess.run([sys.executable, str(_BUILD_SCRIPT)], cwd=_REPO_ROOT, check=True)
    wheels = sorted((_REPO_ROOT / "dist").glob("dbxcarta_submit-*.whl"))
    if not wheels:
        raise AssertionError("build_submit_wheel.py produced no wheel")
    return wheels[-1]


@pytest.mark.slow
def test_submit_wheel_installs_and_imports_alone(tmp_path: Path) -> None:
    wheel = _build_wheel()

    # Use uv to build the clean-room venv and install into it: the uv-managed
    # CPython this repo runs on has no working ``ensurepip``, so stdlib
    # ``venv.create(with_pip=True)`` aborts. ``uv venv`` + ``uv pip install``
    # is the repo's own toolchain and sidesteps that.
    venv_dir = tmp_path / "cleanroom"
    subprocess.run([_UV, "venv", str(venv_dir)], check=True)
    py = venv_dir / "bin" / "python"

    subprocess.run(
        [_UV, "pip", "install", "--python", str(py), str(wheel)],
        check=True,
    )

    result = subprocess.run(
        [str(py), "-c", _CLEANROOM_CHECK],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, (
        f"clean-room import failed:\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    )
    assert "cleanroom-ok" in result.stdout
