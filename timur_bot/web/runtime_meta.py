from __future__ import annotations

import os
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import lru_cache


@dataclass(frozen=True)
class RuntimeMeta:
    version: str
    source: str
    deployed_at: str


def _read_git_sha() -> str | None:
    try:
        output = subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            stderr=subprocess.DEVNULL,
            text=True,
        )
    except Exception:
        return None
    value = output.strip()
    return value or None


@lru_cache(maxsize=1)
def get_runtime_meta() -> RuntimeMeta:
    explicit = os.getenv("TIMUR_VERSION") or os.getenv("AMVERA_GIT_SHA") or os.getenv("GIT_COMMIT")
    git_sha = explicit or _read_git_sha() or "dev"
    source = "env" if explicit else ("git" if git_sha != "dev" else "fallback")
    deployed_at = os.getenv("AMVERA_DEPLOY_TIME") or datetime.now(timezone.utc).isoformat()
    return RuntimeMeta(version=git_sha, source=source, deployed_at=deployed_at)
