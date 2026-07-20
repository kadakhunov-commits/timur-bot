"""Process-local priority guard that keeps background LLM work away from live replies."""

from __future__ import annotations

import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Iterator


DEFAULT_BACKGROUND_QUIET_SECONDS = 300.0


@dataclass(frozen=True)
class BackgroundReservation:
    acquired: bool
    purpose: str
    reason: str = ""
    retry_after_seconds: float = 0.0


_STATE_LOCK = threading.Lock()
_FOREGROUND_ACTIVE = 0
_BACKGROUND_ACTIVE = 0
_LAST_FOREGROUND_ACTIVITY = time.monotonic()


@contextmanager
def foreground_activity(purpose: str) -> Iterator[None]:
    del purpose
    global _FOREGROUND_ACTIVE, _LAST_FOREGROUND_ACTIVITY
    with _STATE_LOCK:
        _FOREGROUND_ACTIVE += 1
        _LAST_FOREGROUND_ACTIVITY = time.monotonic()
    try:
        yield
    finally:
        with _STATE_LOCK:
            _FOREGROUND_ACTIVE = max(0, _FOREGROUND_ACTIVE - 1)
            _LAST_FOREGROUND_ACTIVITY = time.monotonic()


def reserve_background(
    purpose: str,
    *,
    quiet_seconds: float = DEFAULT_BACKGROUND_QUIET_SECONDS,
    now: float | None = None,
) -> BackgroundReservation:
    global _BACKGROUND_ACTIVE
    current = time.monotonic() if now is None else float(now)
    quiet_window = max(0.0, float(quiet_seconds))
    with _STATE_LOCK:
        if _FOREGROUND_ACTIVE > 0:
            return BackgroundReservation(False, purpose, "foreground_active", quiet_window)
        if _BACKGROUND_ACTIVE > 0:
            return BackgroundReservation(False, purpose, "background_active", 1.0)
        elapsed = max(0.0, current - _LAST_FOREGROUND_ACTIVITY)
        if elapsed < quiet_window:
            return BackgroundReservation(False, purpose, "foreground_recent", quiet_window - elapsed)
        _BACKGROUND_ACTIVE += 1
        return BackgroundReservation(True, purpose)


def release_background(reservation: BackgroundReservation) -> None:
    global _BACKGROUND_ACTIVE
    if not reservation.acquired:
        return
    with _STATE_LOCK:
        _BACKGROUND_ACTIVE = max(0, _BACKGROUND_ACTIVE - 1)


def snapshot(*, now: float | None = None) -> dict[str, float | int]:
    current = time.monotonic() if now is None else float(now)
    with _STATE_LOCK:
        return {
            "foreground_active": _FOREGROUND_ACTIVE,
            "background_active": _BACKGROUND_ACTIVE,
            "seconds_since_foreground": max(0.0, current - _LAST_FOREGROUND_ACTIVITY),
        }


def reset_for_tests(*, last_foreground_activity: float | None = None) -> None:
    global _FOREGROUND_ACTIVE, _BACKGROUND_ACTIVE, _LAST_FOREGROUND_ACTIVITY
    with _STATE_LOCK:
        _FOREGROUND_ACTIVE = 0
        _BACKGROUND_ACTIVE = 0
        _LAST_FOREGROUND_ACTIVITY = (
            time.monotonic() if last_foreground_activity is None else float(last_foreground_activity)
        )
