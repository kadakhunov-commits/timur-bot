"""Stateful reply policy for Timur's active conversations and rare snipes."""

from __future__ import annotations

import re
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable


def _now() -> datetime:
    return datetime.utcnow()


def _parse_ts(value: Any) -> datetime | None:
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).replace(tzinfo=None)
    except (TypeError, ValueError):
        return None


def _tokens(text: str) -> set[str]:
    return {token.lower() for token in re.findall(r"[a-zа-яё0-9]{3,}", text or "", re.I)}


def ensure_policy_state(chat_mem: Dict[str, Any]) -> Dict[str, Any]:
    layers = chat_mem.setdefault("memory_layers", {})
    state = layers.setdefault("adaptive_humor", {})
    state.setdefault("dialogue", {})
    state.setdefault("last_snipe_ts", None)
    state.setdefault("human_messages_since_snipe", 0)
    state.setdefault("human_messages_since_snipe_attempt", 0)
    state.setdefault("human_messages_since_reply", 0)
    state.setdefault("human_messages_since_interjection_check", 0)
    return state


def note_human_message(chat_mem: Dict[str, Any]) -> None:
    state = ensure_policy_state(chat_mem)
    state["human_messages_since_snipe"] = int(state.get("human_messages_since_snipe", 0)) + 1
    state["human_messages_since_snipe_attempt"] = int(state.get("human_messages_since_snipe_attempt", 0)) + 1
    state["human_messages_since_reply"] = int(state.get("human_messages_since_reply", 0)) + 1
    state["human_messages_since_interjection_check"] = int(state.get("human_messages_since_interjection_check", 0)) + 1


def ordinary_reply_allowed(chat_mem: Dict[str, Any], *, min_human_messages: int) -> bool:
    state = ensure_policy_state(chat_mem)
    return int(state.get("human_messages_since_reply", 0)) >= max(1, int(min_human_messages))


def mark_reply_sent(chat_mem: Dict[str, Any]) -> None:
    ensure_policy_state(chat_mem)["human_messages_since_reply"] = 0


def interjection_check_allowed(chat_mem: Dict[str, Any], *, min_human_messages: int) -> bool:
    state = ensure_policy_state(chat_mem)
    return int(state.get("human_messages_since_interjection_check", 0)) >= max(1, int(min_human_messages))


def mark_interjection_checked(chat_mem: Dict[str, Any]) -> None:
    ensure_policy_state(chat_mem)["human_messages_since_interjection_check"] = 0


def activate_dialogue(
    chat_mem: Dict[str, Any],
    *,
    initiator_id: int,
    text: str,
    now: datetime | None = None,
) -> None:
    state = ensure_policy_state(chat_mem)
    current = now or _now()
    state["dialogue"] = {
        "initiator_id": int(initiator_id),
        "participants": [int(initiator_id)],
        "topic_tokens": sorted(_tokens(text))[:16],
        "last_activity_ts": current.isoformat(),
        "unrelated_streak": 0,
    }


def continue_dialogue(
    chat_mem: Dict[str, Any],
    *,
    user_id: int,
    text: str,
    window_minutes: int,
    now: datetime | None = None,
) -> bool:
    """Return whether a non-explicit message belongs to Timur's open thread."""
    state = ensure_policy_state(chat_mem)
    dialogue = state.get("dialogue")
    if not isinstance(dialogue, dict):
        return False
    current = now or _now()
    last = _parse_ts(dialogue.get("last_activity_ts"))
    if not last or current - last > timedelta(minutes=max(1, int(window_minutes))):
        state["dialogue"] = {}
        return False

    initiator_id = int(dialogue.get("initiator_id", 0) or 0)
    if int(user_id) == initiator_id:
        dialogue["last_activity_ts"] = current.isoformat()
        dialogue["unrelated_streak"] = 0
        return True

    current_tokens = _tokens(text)
    topic_tokens = {str(token) for token in dialogue.get("topic_tokens", [])}
    short_followup = re.sub(r"\s+", " ", text.lower()).strip() in {
        "а почему",
        "и че",
        "и чё",
        "ну да",
        "ну и",
        "а дальше",
        "серьезно",
        "серьёзно",
    }
    related = bool(current_tokens & topic_tokens) or short_followup
    if related:
        participants = {int(item) for item in dialogue.get("participants", []) if str(item).lstrip("-").isdigit()}
        participants.add(int(user_id))
        dialogue["participants"] = sorted(participants)
        dialogue["topic_tokens"] = sorted((topic_tokens | current_tokens))[:20]
        dialogue["last_activity_ts"] = current.isoformat()
        dialogue["unrelated_streak"] = 0
        return True

    dialogue["unrelated_streak"] = int(dialogue.get("unrelated_streak", 0)) + 1
    if int(dialogue["unrelated_streak"]) >= 3:
        state["dialogue"] = {}
    return False


def snipe_allowed(
    chat_mem: Dict[str, Any],
    *,
    cooldown_minutes: int,
    min_human_messages: int,
    now: datetime | None = None,
) -> bool:
    state = ensure_policy_state(chat_mem)
    last = _parse_ts(state.get("last_snipe_ts"))
    attempts = int(state.get("human_messages_since_snipe_attempt", 0))
    if not last:
        return attempts >= max(1, int(min_human_messages))
    current = now or _now()
    elapsed = current - last
    return (
        elapsed >= timedelta(minutes=max(1, int(cooldown_minutes)))
        and int(state.get("human_messages_since_snipe", 0)) >= max(1, int(min_human_messages))
        and attempts >= max(1, int(min_human_messages))
    )


def mark_snipe_attempt(chat_mem: Dict[str, Any]) -> None:
    ensure_policy_state(chat_mem)["human_messages_since_snipe_attempt"] = 0


def mark_snipe_sent(chat_mem: Dict[str, Any], *, now: datetime | None = None) -> None:
    state = ensure_policy_state(chat_mem)
    state["last_snipe_ts"] = (now or _now()).isoformat()
    state["human_messages_since_snipe"] = 0
    state["human_messages_since_snipe_attempt"] = 0
