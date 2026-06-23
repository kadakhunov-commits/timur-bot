"""Feature gate: turn a subscription tier into concrete runtime limits (Phase 2).

Pure helpers over the feature dict returned by ``BillingEngine.effective_features``.
They decide what тимур is allowed to do in a given chat — which is how the
subscription actually changes behavior instead of just sitting in a ledger.

The headline lever is ``memory_depth``: free chats get a shallow тимур
(self-card + basic chat), paid chats unlock friend dossiers, long memory and
episodic callbacks. Memory depth is the thing worth paying for.
"""

from __future__ import annotations

from typing import Any, Dict, List

MEMORY_SHORT = "short"
MEMORY_STANDARD = "standard"
MEMORY_FULL = "full"
_DEPTH_RANK = {MEMORY_SHORT: 0, MEMORY_STANDARD: 1, MEMORY_FULL: 2}

# What a chat gets with no active subscription. Mirrors the free tier in
# BillingEngine.tier_features so the gate is safe even if billing is unavailable.
FREE_FEATURES: Dict[str, Any] = {
    "tier": "free_promo",
    "max_daily_replies": 30,
    "persona_modes": ["default", "chill"],
    "voice": False,
    "friend_dossiers": False,
    "episodic_memory": False,
    "memory_depth": MEMORY_SHORT,
    "watermark": True,
}


def memory_depth(features: Dict[str, Any] | None) -> str:
    depth = str((features or {}).get("memory_depth") or MEMORY_SHORT)
    return depth if depth in _DEPTH_RANK else MEMORY_SHORT


def depth_at_least(features: Dict[str, Any] | None, level: str) -> bool:
    return _DEPTH_RANK.get(memory_depth(features), 0) >= _DEPTH_RANK.get(level, 0)


def allowed_modes(features: Dict[str, Any] | None) -> List[str]:
    modes = (features or {}).get("persona_modes")
    if isinstance(modes, list) and modes:
        return [str(m) for m in modes]
    return list(FREE_FEATURES["persona_modes"])


def is_mode_allowed(features: Dict[str, Any] | None, mode: str) -> bool:
    return str(mode) in allowed_modes(features)


def gate_mode(features: Dict[str, Any] | None, mode: str, *, fallback: str = "default") -> str:
    """Return the requested mode if the tier allows it, else the fallback."""
    return mode if is_mode_allowed(features, mode) else fallback


def voice_allowed(features: Dict[str, Any] | None) -> bool:
    feats = features or {}
    return bool(feats.get("voice", feats.get("voice_circles", False)))


def friend_dossiers_allowed(features: Dict[str, Any] | None) -> bool:
    return bool((features or {}).get("friend_dossiers", False))


def episodic_memory_allowed(features: Dict[str, Any] | None) -> bool:
    return bool((features or {}).get("episodic_memory", False))


def daily_reply_cap(features: Dict[str, Any] | None) -> int:
    try:
        return int((features or {}).get("max_daily_replies", FREE_FEATURES["max_daily_replies"]))
    except (TypeError, ValueError):
        return int(FREE_FEATURES["max_daily_replies"])


def within_daily_reply_cap(features: Dict[str, Any] | None, used_today: int) -> bool:
    return int(used_today) < daily_reply_cap(features)
