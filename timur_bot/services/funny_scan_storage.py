from __future__ import annotations

import json
from copy import deepcopy
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

STATUS_NEW = "new"
STATUS_APPROVED = "approved"
STATUS_REJECTED = "rejected"
STATUS_SENT = "sent"
VALID_STATUSES = {STATUS_NEW, STATUS_APPROVED, STATUS_REJECTED, STATUS_SENT}


def _now_iso() -> str:
    return datetime.utcnow().isoformat()


def _today_str() -> str:
    return date.today().isoformat()


def _clamp_int(value: Any, low: int, high: int, default: int) -> int:
    try:
        parsed = int(value)
    except Exception:
        parsed = default
    return max(low, min(high, parsed))


def _norm_emoji(value: str) -> str:
    return str(value or "").replace("\ufe0f", "").strip()


def default_funny_scan_config(owner_id: int, runtime_defaults: Dict[str, Any] | None = None) -> Dict[str, Any]:
    defaults = runtime_defaults if isinstance(runtime_defaults, dict) else {}
    intensity_profiles = deepcopy(defaults.get("intensity_profiles") or {})
    if not intensity_profiles:
        intensity_profiles = {
            "cheap": {
                "stage1_min_score": 50,
                "review_threshold": 78,
                "max_candidates_per_scan": 20,
                "max_llm_candidates_per_scan": 6,
                "llm_max_context_messages": 8,
            },
            "balanced": {
                "stage1_min_score": 42,
                "review_threshold": 70,
                "max_candidates_per_scan": 30,
                "max_llm_candidates_per_scan": 12,
                "llm_max_context_messages": 12,
            },
            "deep": {
                "stage1_min_score": 35,
                "review_threshold": 64,
                "max_candidates_per_scan": 45,
                "max_llm_candidates_per_scan": 18,
                "llm_max_context_messages": 14,
            },
        }

    return {
        "enabled": bool(defaults.get("enabled", False)),
        "owner_dm_chat_id": int(defaults.get("owner_dm_chat_id", owner_id)),
        "sources": [],
        "scan_period_hours": _clamp_int(defaults.get("scan_period_hours"), 1, 24 * 30, 24),
        "scan_schedule_minutes": _clamp_int(defaults.get("scan_schedule_minutes"), 15, 24 * 60, 60),
        "intensity": str(defaults.get("intensity", "balanced")),
        "stage1_min_score": _clamp_int(defaults.get("stage1_min_score"), 0, 100, 42),
        "review_threshold": _clamp_int(defaults.get("review_threshold"), 0, 100, 70),
        "max_candidates_per_scan": _clamp_int(defaults.get("max_candidates_per_scan"), 1, 300, 30),
        "max_llm_candidates_per_scan": _clamp_int(defaults.get("max_llm_candidates_per_scan"), 1, 100, 12),
        "daily_token_budget": _clamp_int(defaults.get("daily_token_budget"), 1000, 10_000_000, 50_000),
        "daily_token_hard_stop": _clamp_int(defaults.get("daily_token_hard_stop"), 1000, 10_000_000, 55_000),
        "daily_forward_limit": _clamp_int(defaults.get("daily_forward_limit"), 1, 500, 20),
        "llm_model": str(defaults.get("llm_model", "gpt-4o-mini")),
        "llm_max_context_messages": _clamp_int(defaults.get("llm_max_context_messages"), 3, 40, 12),
        "llm_max_chars_per_message": _clamp_int(defaults.get("llm_max_chars_per_message"), 40, 1000, 220),
        "intensity_profiles": intensity_profiles,
    }


def normalize_sources(raw_sources: Any) -> List[Dict[str, Any]]:
    if not isinstance(raw_sources, list):
        return []
    normalized: List[Dict[str, Any]] = []
    seen: set[int] = set()
    for item in raw_sources:
        if not isinstance(item, dict):
            continue
        try:
            chat_id = int(item.get("chat_id"))
        except Exception:
            continue
        if chat_id in seen:
            continue
        seen.add(chat_id)
        normalized.append(
            {
                "chat_id": chat_id,
                "title": str(item.get("title") or ""),
                "enabled": bool(item.get("enabled", True)),
            }
        )
    normalized.sort(key=lambda x: x["chat_id"])
    return normalized


def ensure_funny_scan_config(
    config: Dict[str, Any],
    *,
    owner_id: int,
    runtime_defaults: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    settings = config.setdefault("funny_scan", {})
    defaults = default_funny_scan_config(owner_id=owner_id, runtime_defaults=runtime_defaults)
    for key, value in defaults.items():
        settings.setdefault(key, deepcopy(value))

    settings["owner_dm_chat_id"] = int(settings.get("owner_dm_chat_id", owner_id))
    settings["scan_period_hours"] = _clamp_int(settings.get("scan_period_hours"), 1, 24 * 30, defaults["scan_period_hours"])
    settings["scan_schedule_minutes"] = _clamp_int(
        settings.get("scan_schedule_minutes"), 15, 24 * 60, defaults["scan_schedule_minutes"]
    )
    settings["stage1_min_score"] = _clamp_int(settings.get("stage1_min_score"), 0, 100, defaults["stage1_min_score"])
    settings["review_threshold"] = _clamp_int(settings.get("review_threshold"), 0, 100, defaults["review_threshold"])
    settings["max_candidates_per_scan"] = _clamp_int(
        settings.get("max_candidates_per_scan"), 1, 300, defaults["max_candidates_per_scan"]
    )
    settings["max_llm_candidates_per_scan"] = _clamp_int(
        settings.get("max_llm_candidates_per_scan"), 1, 100, defaults["max_llm_candidates_per_scan"]
    )
    settings["daily_token_budget"] = _clamp_int(settings.get("daily_token_budget"), 1000, 10_000_000, defaults["daily_token_budget"])
    settings["daily_token_hard_stop"] = _clamp_int(
        settings.get("daily_token_hard_stop"), 1000, 10_000_000, defaults["daily_token_hard_stop"]
    )
    settings["daily_forward_limit"] = _clamp_int(settings.get("daily_forward_limit"), 1, 500, defaults["daily_forward_limit"])
    settings["llm_max_context_messages"] = _clamp_int(
        settings.get("llm_max_context_messages"), 3, 40, defaults["llm_max_context_messages"]
    )
    settings["llm_max_chars_per_message"] = _clamp_int(
        settings.get("llm_max_chars_per_message"), 40, 1000, defaults["llm_max_chars_per_message"]
    )
    settings["llm_model"] = str(settings.get("llm_model") or defaults["llm_model"])
    settings["enabled"] = bool(settings.get("enabled", False))
    settings["sources"] = normalize_sources(settings.get("sources"))
    settings["intensity_profiles"] = defaults["intensity_profiles"] | dict(settings.get("intensity_profiles") or {})
    if str(settings.get("intensity")) not in {"cheap", "balanced", "deep"}:
        settings["intensity"] = "balanced"
    return settings


def apply_intensity_profile(settings: Dict[str, Any], intensity: str) -> Dict[str, Any]:
    profile = (settings.get("intensity_profiles") or {}).get(intensity)
    if not isinstance(profile, dict):
        return settings
    settings["intensity"] = intensity
    for key in (
        "stage1_min_score",
        "review_threshold",
        "max_candidates_per_scan",
        "max_llm_candidates_per_scan",
        "llm_max_context_messages",
    ):
        if key in profile:
            settings[key] = int(profile[key])
    return settings


def upsert_source(settings: Dict[str, Any], chat_id: int, title: str = "", enabled: bool = True) -> None:
    sources = normalize_sources(settings.get("sources"))
    existing = next((item for item in sources if int(item["chat_id"]) == int(chat_id)), None)
    if existing:
        if title:
            existing["title"] = title
        existing["enabled"] = bool(enabled)
    else:
        sources.append({"chat_id": int(chat_id), "title": title, "enabled": bool(enabled)})
    settings["sources"] = normalize_sources(sources)


def toggle_source(settings: Dict[str, Any], chat_id: int) -> bool:
    sources = normalize_sources(settings.get("sources"))
    for source in sources:
        if int(source["chat_id"]) == int(chat_id):
            source["enabled"] = not bool(source.get("enabled", True))
            settings["sources"] = sources
            return bool(source["enabled"])
    return False


def default_funny_scan_state() -> Dict[str, Any]:
    return {
        "budget": {
            "day": _today_str(),
            "tokens_used": 0,
            "llm_calls_used": 0,
            "forwards_sent": 0,
            "pending_forwards": 0,
        },
        "state": {
            "last_scan_ts": None,
            "last_scan_by_source": {},
            "last_candidate_seq": 0,
        },
        "reaction_index": {},
        "candidates": {},
        "candidate_order": [],
    }


def ensure_state_schema(state: Dict[str, Any]) -> Dict[str, Any]:
    state.setdefault("budget", {})
    budget = state["budget"]
    budget.setdefault("day", _today_str())
    budget.setdefault("tokens_used", 0)
    budget.setdefault("llm_calls_used", 0)
    budget.setdefault("forwards_sent", 0)
    budget.setdefault("pending_forwards", 0)

    state.setdefault("state", {})
    state_state = state["state"]
    state_state.setdefault("last_scan_ts", None)
    state_state.setdefault("last_scan_by_source", {})
    state_state.setdefault("last_candidate_seq", 0)
    if not isinstance(state_state.get("last_scan_by_source"), dict):
        state_state["last_scan_by_source"] = {}

    if not isinstance(state.get("reaction_index"), dict):
        state["reaction_index"] = {}
    if not isinstance(state.get("candidates"), dict):
        state["candidates"] = {}
    if not isinstance(state.get("candidate_order"), list):
        state["candidate_order"] = []
    return state


def load_state(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return default_funny_scan_state()
    try:
        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)
        if not isinstance(raw, dict):
            return default_funny_scan_state()
        return ensure_state_schema(raw)
    except Exception:
        return default_funny_scan_state()


def save_state(path: Path, state: Dict[str, Any]) -> None:
    ensure_state_schema(state)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def ensure_budget_day(state: Dict[str, Any], *, today: str | None = None) -> Dict[str, Any]:
    ensure_state_schema(state)
    budget = state["budget"]
    day = today or _today_str()
    if str(budget.get("day")) != day:
        budget["day"] = day
        budget["tokens_used"] = 0
        budget["llm_calls_used"] = 0
        budget["forwards_sent"] = 0
        budget["pending_forwards"] = 0
    return budget


def register_token_usage(state: Dict[str, Any], tokens: int) -> None:
    budget = ensure_budget_day(state)
    budget["tokens_used"] = max(0, int(budget.get("tokens_used", 0)) + max(0, int(tokens)))
    budget["llm_calls_used"] = max(0, int(budget.get("llm_calls_used", 0)) + 1)


def register_forward_usage(state: Dict[str, Any]) -> None:
    budget = ensure_budget_day(state)
    budget["forwards_sent"] = max(0, int(budget.get("forwards_sent", 0)) + 1)


def hard_budget_reached(settings: Dict[str, Any], state: Dict[str, Any]) -> bool:
    budget = ensure_budget_day(state)
    hard_stop = int(settings.get("daily_token_hard_stop", 0))
    return hard_stop > 0 and int(budget.get("tokens_used", 0)) >= hard_stop


def soft_budget_ratio(settings: Dict[str, Any], state: Dict[str, Any]) -> float:
    budget = ensure_budget_day(state)
    limit = max(1, int(settings.get("daily_token_budget", 1)))
    return float(budget.get("tokens_used", 0)) / float(limit)


def estimate_tokens_fallback(payload: str) -> int:
    return max(1, (len(payload or "") // 4) + 1)


def _candidate_signature(candidate: Dict[str, Any]) -> str:
    # Keep signature stable across boundary refinements.
    return f"{candidate.get('source_chat_id')}:{candidate.get('anchor_message_id')}"


def candidate_signature(candidate: Dict[str, Any]) -> str:
    return _candidate_signature(candidate)


def has_candidate_signature(state: Dict[str, Any], candidate: Dict[str, Any]) -> bool:
    ensure_state_schema(state)
    signature = _candidate_signature(candidate)
    for existing in state["candidates"].values():
        if isinstance(existing, dict) and _candidate_signature(existing) == signature:
            return True
    return False


def next_candidate_id(state: Dict[str, Any]) -> str:
    ensure_state_schema(state)
    seq = int(state["state"].get("last_candidate_seq", 0)) + 1
    state["state"]["last_candidate_seq"] = seq
    return f"fc_{seq:06d}"


def add_candidate(state: Dict[str, Any], candidate: Dict[str, Any], *, max_keep: int = 1200) -> Tuple[str, bool]:
    ensure_state_schema(state)
    signature = _candidate_signature(candidate)
    candidates = state["candidates"]

    for candidate_id, existing in candidates.items():
        if _candidate_signature(existing) == signature:
            return candidate_id, False

    candidate_id = str(candidate.get("id") or next_candidate_id(state))
    payload = dict(candidate)
    payload["id"] = candidate_id
    payload.setdefault("status", STATUS_NEW)
    payload.setdefault("show_to_owner", False)
    payload.setdefault("preview_sent_at", None)
    payload.setdefault("approved_at", None)
    payload.setdefault("rejected_at", None)
    payload.setdefault("sent_at", None)
    payload.setdefault("forward_error", None)
    payload.setdefault("meta", {})
    payload.setdefault("signals_pos", [])
    payload.setdefault("signals_neg", [])
    payload.setdefault("score", None)
    candidates[candidate_id] = payload

    order = state["candidate_order"]
    if candidate_id not in order:
        order.append(candidate_id)

    if len(order) > max_keep:
        overflow = len(order) - max_keep
        to_remove = order[:overflow]
        for old_id in to_remove:
            candidates.pop(old_id, None)
        del order[:overflow]

    return candidate_id, True


def list_candidates(state: Dict[str, Any], *, status: str | None = None, limit: int = 20) -> List[Dict[str, Any]]:
    ensure_state_schema(state)
    result: List[Dict[str, Any]] = []
    for candidate_id in reversed(state["candidate_order"]):
        candidate = state["candidates"].get(candidate_id)
        if not isinstance(candidate, dict):
            continue
        if status and str(candidate.get("status")) != status:
            continue
        result.append(candidate)
        if len(result) >= limit:
            break
    return result


def get_candidate(state: Dict[str, Any], candidate_id: str) -> Dict[str, Any] | None:
    ensure_state_schema(state)
    candidate = state["candidates"].get(str(candidate_id))
    if not isinstance(candidate, dict):
        return None
    return candidate


def set_candidate_status(
    state: Dict[str, Any],
    candidate_id: str,
    status: str,
    *,
    forward_error: str | None = None,
) -> bool:
    if status not in VALID_STATUSES:
        return False
    candidate = get_candidate(state, candidate_id)
    if not candidate:
        return False
    candidate["status"] = status
    now_ts = _now_iso()
    if status == STATUS_APPROVED:
        candidate["approved_at"] = now_ts
    elif status == STATUS_REJECTED:
        candidate["rejected_at"] = now_ts
    elif status == STATUS_SENT:
        candidate["sent_at"] = now_ts
    if forward_error is not None:
        candidate["forward_error"] = forward_error
    return True


def set_preview_sent(state: Dict[str, Any], candidate_id: str, *, preview_message_id: int | None = None) -> bool:
    candidate = get_candidate(state, candidate_id)
    if not candidate:
        return False
    candidate["preview_sent_at"] = _now_iso()
    meta = candidate.setdefault("meta", {})
    if preview_message_id is not None:
        meta["preview_message_id"] = int(preview_message_id)
    return True


def update_last_scan(state: Dict[str, Any], source_chat_id: int, *, ts: str | None = None) -> None:
    ensure_state_schema(state)
    now_ts = ts or _now_iso()
    state["state"]["last_scan_ts"] = now_ts
    per_source = state["state"].setdefault("last_scan_by_source", {})
    per_source[str(source_chat_id)] = now_ts


def apply_reaction_delta(
    state: Dict[str, Any],
    *,
    chat_id: int,
    message_id: int,
    old_emojis: Iterable[str],
    new_emojis: Iterable[str],
    heart_emojis: Iterable[str],
    laugh_emojis: Iterable[str],
) -> Dict[str, Any]:
    ensure_state_schema(state)
    hearts = {_norm_emoji(x) for x in heart_emojis if _norm_emoji(x)}
    laughs = {_norm_emoji(x) for x in laugh_emojis if _norm_emoji(x)}

    old_set = {_norm_emoji(x) for x in old_emojis if _norm_emoji(x)}
    new_set = {_norm_emoji(x) for x in new_emojis if _norm_emoji(x)}
    key = f"{int(chat_id)}:{int(message_id)}"
    entry = state["reaction_index"].setdefault(
        key,
        {"total": 0, "heart": 0, "laugh": 0, "updated_at": _now_iso()},
    )

    for emoji in old_set:
        entry["total"] = max(0, int(entry.get("total", 0)) - 1)
        if emoji in hearts:
            entry["heart"] = max(0, int(entry.get("heart", 0)) - 1)
        if emoji in laughs:
            entry["laugh"] = max(0, int(entry.get("laugh", 0)) - 1)

    for emoji in new_set:
        entry["total"] = max(0, int(entry.get("total", 0)) + 1)
        if emoji in hearts:
            entry["heart"] = max(0, int(entry.get("heart", 0)) + 1)
        if emoji in laughs:
            entry["laugh"] = max(0, int(entry.get("laugh", 0)) + 1)

    entry["updated_at"] = _now_iso()
    return entry


def get_reaction_stats(state: Dict[str, Any], chat_id: int, message_id: int) -> Dict[str, int]:
    ensure_state_schema(state)
    key = f"{int(chat_id)}:{int(message_id)}"
    raw = state["reaction_index"].get(key) or {}
    return {
        "total": max(0, int(raw.get("total", 0))),
        "heart": max(0, int(raw.get("heart", 0))),
        "laugh": max(0, int(raw.get("laugh", 0))),
    }
