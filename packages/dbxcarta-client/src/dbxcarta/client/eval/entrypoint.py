"""``dbxcarta-client`` — local entrypoint for the Text2SQL evaluation harness.

The client runs locally now (no cluster), so it loads the selected example
overlay and the base ``.env`` itself before running the harness, the same way
the operator CLI does: ``DBXCARTA_ENV_FILE`` (or ``--env-file``) selects the
overlay, and ``load_env_files`` applies overlay-then-base with ``override=False``
so a real exported process env still wins. ``inject_params`` is kept for any
forwarded ``KEY=VALUE`` argv.
"""

from __future__ import annotations

import sys

from dbxcarta.client.eval import run_client
from dbxcarta.core.env import (
    EnvFileError,
    inject_params,
    load_env_files,
    resolve_env_files,
)


def main() -> None:
    try:
        files, cleaned_argv = resolve_env_files(sys.argv[1:])
    except EnvFileError as exc:
        # Match the operator CLI: a bad/missing selected overlay is a clean
        # ``error: ...`` on stderr and exit 2, never an uncaught traceback.
        print(f"error: {exc}", file=sys.stderr)
        raise SystemExit(2) from None
    # Strip the consumed ``--env-file`` option so ``inject_params`` only sees
    # forwarded KEY=VALUE args.
    sys.argv[1:] = cleaned_argv
    load_env_files(files)
    inject_params()
    run_client()


if __name__ == "__main__":
    main()
