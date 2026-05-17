"""Client bootstrap helpers."""

from __future__ import annotations

import os
import sys


def inject_params() -> None:
    """Parse KEY=VALUE argv parameters into os.environ.

    The CLI runner forwards .env variables as positional KEY=VALUE args.
    Uses setdefault so pre-existing environment variables take precedence.
    """
    remaining: list[str] = []
    for arg in sys.argv[1:]:
        if "=" in arg and not arg.startswith("-"):
            key, _, value = arg.partition("=")
            os.environ.setdefault(key, value)
        else:
            remaining.append(arg)
    sys.argv[1:] = remaining
