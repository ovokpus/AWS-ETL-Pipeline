"""Filesystem layout for the local data lake."""

from __future__ import annotations

import os
from pathlib import Path

# Repo root is the parent of this package. Docker sets LAKE_DIR explicitly,
# so this fallback is only for a checkout run with ``python -m local_emulator``.
REPO_ROOT = Path(__file__).resolve().parent.parent


def resolve_lake_dir() -> Path:
    """Return the lake directory from ``LAKE_DIR`` or ``<repo>/.local-lake``."""
    configured = os.environ.get("LAKE_DIR")
    if configured:
        return Path(configured).expanduser().resolve()
    return (REPO_ROOT / ".local-lake").resolve()
