"""Self-model: a canonical, self-consistent identity for тимур (Phase 0).

The bot keeps ONE global self-profile across all chats (the same person
everywhere). It is a set of attribute slots, each either:

- ``canon``  — locked identity from the persona, never contradicted;
- ``learned`` — facts тимур stated himself, accepted only while consistent.

``register_self_claim`` is the contradiction guard (M2): a new self-claim that
conflicts with a locked or well-established slot is rejected instead of stored,
so тимур never says he is two ages or graduated twice.

``build_self_card_prompt`` renders a compact first-person identity block that is
injected into every reply prompt (M1), making self-awareness deterministic
instead of probabilistic.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any, Dict, List

# Locked identity, seeded from config/persona.yaml. These are facts about тимур
# that must stay stable; a chat message can never overwrite them.
CANON_SELF_FACTS: Dict[str, str] = {
    "name": "тимур",
    "nature": "бот в чате дискордники",
    "age": "22",
    "city": "казань",
    "residence": "казань",
    "origin": "казань",
    "school": "школа №18",
    "university": "кфу",
    "faculty": "мехмат",
}

# A learned slot only overrides a prior learned slot while the prior one is still
# "soft": below this confidence and seen at most once. Past that it is treated as
# established and conflicting claims are rejected.
_ESTABLISHED_CONFIDENCE = 0.8

# Render order + first-person phrasing for the identity card.
_SELF_PHRASING: Dict[str, str] = {
    "name": "меня зовут {v}",
    "nature": "я — {v}",
    "full_name": "полное имя — {v}",
    "surname": "моя фамилия {v}",
    "age": "мне {v}",
    "city": "мой город — {v}",
    "residence": "живу в {v}",
    "origin": "родом из {v}",
    "birth_place": "родился в {v}",
    "school": "моя школа — {v}",
    "university": "универ — {v}",
    "faculty": "факультет — {v}",
    "work": "работаю: {v}",
    "job": "работаю: {v}",
}
_RENDER_ORDER: List[str] = [
    "name",
    "nature",
    "surname",
    "full_name",
    "age",
    "city",
    "residence",
    "origin",
    "birth_place",
    "school",
    "university",
    "faculty",
    "work",
    "job",
]
# Place-like attributes that often repeat the same city; collapse duplicates.
_PLACE_ATTRS = {"city", "residence", "origin", "birth_place"}


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _clean_value(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip()).strip(" .,!?:;")


def _norm(value: Any) -> str:
    return _clean_value(value).lower().replace("ё", "е")


def ensure_self_profile(memory: Dict[str, Any]) -> Dict[str, Any]:
    """Return the global self-profile, seeding canon slots on first access."""
    profile = memory.setdefault("self_profile", {})
    slots: Dict[str, Any] = profile.setdefault("slots", {})
    for attribute, value in CANON_SELF_FACTS.items():
        slot = slots.get(attribute)
        if slot is None:
            slots[attribute] = {
                "attribute": attribute,
                "value": value,
                "source": "canon",
                "confidence": 1.0,
                "evidence_count": 1,
                "locked": True,
                "updated_at": _utc_now_iso(),
            }
        else:
            # Keep canon authoritative even if an old memory drifted.
            if slot.get("source") == "canon":
                slot["value"] = value
                slot["locked"] = True
    profile.setdefault("rejected", [])
    return profile


def register_self_claim(
    memory: Dict[str, Any],
    attribute: str,
    value: str,
    *,
    confidence: float = 0.5,
    source: str = "learned",
) -> Dict[str, Any]:
    """Apply the consistency guard to a self-claim and persist if accepted.

    Returns a result dict with ``status`` in
    {``accepted``, ``reinforced``, ``rejected``, ``ignored``}.
    """
    attribute = str(attribute or "").strip()
    value_clean = _clean_value(value)
    if not attribute or not value_clean:
        return {"status": "ignored", "reason": "empty", "attribute": attribute}

    profile = ensure_self_profile(memory)
    slots: Dict[str, Any] = profile["slots"]
    slot = slots.get(attribute)
    new_norm = _norm(value_clean)

    if slot is None:
        slots[attribute] = {
            "attribute": attribute,
            "value": value_clean,
            "source": source,
            "confidence": round(float(confidence), 3),
            "evidence_count": 1,
            "locked": False,
            "updated_at": _utc_now_iso(),
        }
        return {"status": "accepted", "reason": "new", "attribute": attribute, "value": value_clean}

    if _norm(slot.get("value")) == new_norm:
        slot["evidence_count"] = int(slot.get("evidence_count", 1)) + 1
        slot["confidence"] = round(max(float(slot.get("confidence", 0.0)), float(confidence)), 3)
        slot["updated_at"] = _utc_now_iso()
        return {"status": "reinforced", "attribute": attribute, "value": slot["value"]}

    # Conflicting value: reject if the existing slot is canon/locked or established.
    established = (
        float(slot.get("confidence", 0.0)) >= _ESTABLISHED_CONFIDENCE
        and int(slot.get("evidence_count", 1)) >= 2
    )
    if slot.get("locked") or slot.get("source") == "canon" or established:
        reason = "contradicts_canon" if (slot.get("locked") or slot.get("source") == "canon") else "contradicts_established"
        _log_rejection(profile, attribute, value_clean, str(slot.get("value", "")), reason)
        return {
            "status": "rejected",
            "reason": reason,
            "attribute": attribute,
            "value": value_clean,
            "kept": slot.get("value"),
        }

    # Soft learned slot — allow a retcon, the newer claim wins.
    slots[attribute] = {
        "attribute": attribute,
        "value": value_clean,
        "source": source,
        "confidence": round(float(confidence), 3),
        "evidence_count": 1,
        "locked": False,
        "updated_at": _utc_now_iso(),
    }
    return {"status": "accepted", "reason": "retcon", "attribute": attribute, "value": value_clean}


def _log_rejection(profile: Dict[str, Any], attribute: str, attempted: str, kept: str, reason: str) -> None:
    rejected: List[Dict[str, Any]] = profile.setdefault("rejected", [])
    rejected.append(
        {
            "attribute": attribute,
            "attempted": attempted,
            "kept": kept,
            "reason": reason,
            "ts": _utc_now_iso(),
        }
    )
    if len(rejected) > 40:
        del rejected[:-40]


def build_self_card_prompt(memory: Dict[str, Any], *, max_slots: int = 10) -> str:
    """Compact first-person identity block injected into every prompt."""
    profile = ensure_self_profile(memory)
    slots: Dict[str, Any] = profile.get("slots", {})
    if not slots:
        return ""

    ordered_keys = [key for key in _RENDER_ORDER if key in slots]
    ordered_keys += [key for key in slots if key not in _RENDER_ORDER]

    lines: List[str] = []
    seen_place_values: set[str] = set()
    for attribute in ordered_keys:
        slot = slots.get(attribute) or {}
        value = _clean_value(slot.get("value"))
        if not value:
            continue
        if attribute in _PLACE_ATTRS:
            norm = _norm(value)
            if norm in seen_place_values:
                continue
            seen_place_values.add(norm)
        template = _SELF_PHRASING.get(attribute, attribute + " — {v}")
        lines.append("- " + template.format(v=value))
        if len(lines) >= max_slots:
            break

    if not lines:
        return ""
    header = "кто я (это твердо про меня, не противоречу этому и не выдумываю другое):"
    return header + "\n" + "\n".join(lines)
