"""Stateful reply policy for Timur's active conversations and rare snipes."""

from __future__ import annotations

import re
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable


_DIALOGUE_STOPWORDS = {
    "тимур", "тимура", "тимуру", "что", "как", "это", "там", "тут", "уже", "еще", "ещё",
    "надо", "можно", "будет", "быть", "думаешь", "скажи", "ответь", "почему", "когда", "куда",
    "про", "для", "или", "тебе", "меня", "тебя", "его", "она", "они", "вообще", "просто",
    "сейчас", "сегодня",
}


def _now() -> datetime:
    return datetime.utcnow()


def _parse_ts(value: Any) -> datetime | None:
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).replace(tzinfo=None)
    except (TypeError, ValueError):
        return None


def _tokens(text: str) -> set[str]:
    result: set[str] = set()
    for raw in re.findall(r"[a-zа-яё0-9]{3,}", text or "", re.I):
        token = raw.lower().replace("ё", "е")
        if token in _DIALOGUE_STOPWORDS:
            continue
        result.add(token)
        for suffix in ("ами", "ями", "ого", "ему", "ому", "ах", "ях", "ам", "ям", "ов", "ев", "ой", "ый", "ая", "яя", "ое", "ее", "ы", "и", "а", "я", "у", "ю", "е"):
            if token.endswith(suffix) and len(token) - len(suffix) >= 3:
                result.add(token[: -len(suffix)])
                break
    return result


def ensure_policy_state(chat_mem: Dict[str, Any]) -> Dict[str, Any]:
    layers = chat_mem.setdefault("memory_layers", {})
    state = layers.setdefault("adaptive_humor", {})
    state.setdefault("dialogue", {})
    state.setdefault("pending_followups", {})
    state.setdefault("last_snipe_ts", None)
    state.setdefault("human_messages_since_snipe", 0)
    state.setdefault("human_messages_since_snipe_attempt", 0)
    state.setdefault("human_messages_since_reply", 0)
    state.setdefault("human_messages_since_interjection_check", 0)
    return state


def note_human_message(chat_mem: Dict[str, Any]) -> None:
    state = ensure_policy_state(chat_mem)
    state["human_messages_total"] = int(state.get("human_messages_total", 0)) + 1
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
    pending = state.get("pending_followups")
    if not isinstance(pending, dict):
        pending = {}
        state["pending_followups"] = pending
    pending[str(int(initiator_id))] = {
        "last_activity_ts": current.isoformat(),
        "topic_tokens": sorted(_tokens(text))[:16],
    }
    if len(pending) > 16:
        oldest = sorted(
            pending,
            key=lambda key: _parse_ts(pending[key].get("last_activity_ts")) or datetime.min,
        )
        for key in oldest[:-16]:
            pending.pop(key, None)
    state["dialogue"] = {
        "initiator_id": int(initiator_id),
        "participants": [int(initiator_id)],
        "topic_tokens": sorted(_tokens(text))[:16],
        "last_activity_ts": current.isoformat(),
        "unrelated_streak": 0,
        "open_followup": True,
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
    current = now or _now()
    window = timedelta(minutes=max(1, int(window_minutes)))
    pending = state.get("pending_followups")
    if not isinstance(pending, dict):
        pending = {}
        state["pending_followups"] = pending
    for key, row in list(pending.items()):
        pending_ts = _parse_ts(row.get("last_activity_ts")) if isinstance(row, dict) else None
        if not pending_ts or current - pending_ts > window:
            pending.pop(key, None)

    pending_row = pending.pop(str(int(user_id)), None)
    if isinstance(pending_row, dict):
        pending_tokens = {str(token) for token in pending_row.get("topic_tokens", [])}
        current_tokens = _tokens(text)
        state["dialogue"] = {
            "initiator_id": int(user_id),
            "participants": [int(user_id)],
            "topic_tokens": sorted(pending_tokens | current_tokens)[:20],
            "last_activity_ts": current.isoformat(),
            "unrelated_streak": 0,
            "open_followup": False,
        }
        return True

    dialogue = state.get("dialogue")
    if not isinstance(dialogue, dict):
        return False
    last = _parse_ts(dialogue.get("last_activity_ts"))
    if not last or current - last > window:
        state["dialogue"] = {}
        return False

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
    initiator_id = int(dialogue.get("initiator_id", 0) or 0)
    related = bool(current_tokens & topic_tokens) or (int(user_id) == initiator_id and short_followup)
    if related:
        participants = {int(item) for item in dialogue.get("participants", []) if str(item).lstrip("-").isdigit()}
        participants.add(int(user_id))
        dialogue["participants"] = sorted(participants)
        dialogue["topic_tokens"] = sorted((topic_tokens | current_tokens))[:20]
        dialogue["last_activity_ts"] = current.isoformat()
        dialogue["unrelated_streak"] = 0
        if int(user_id) == initiator_id:
            dialogue["open_followup"] = False
        return True

    if bool(dialogue.get("open_followup", False)) and int(user_id) != initiator_id:
        # Background chatter must not consume the one natural next turn that
        # belongs to the person who was actually talking with Timur.
        return False

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
