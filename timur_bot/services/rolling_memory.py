"""Four-day, chat-local conversational memories built from sampled message clips."""

from __future__ import annotations

import hashlib
import json
import random
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List


ROLLING_MEMORY_SCHEMA_VERSION = 1
_NOISE_RE = re.compile(r"^[\W_]*(?:а?ха(?:ха)+|л+о+л+|ору+|ок+|ага+)?[\W_]*$", re.I)
_TOKEN_RE = re.compile(r"[a-zа-яё0-9]{3,}", re.I)
_STOPWORDS = {
    "это", "как", "что", "где", "когда", "кто", "или", "для", "тут", "там", "так", "уже",
    "если", "тогда", "типа", "вот", "тебя", "тебе", "мне", "его", "она", "они", "просто",
}
_SUFFIXES = (
    "ами", "ями", "ого", "его", "ому", "ему", "ыми", "ими", "ах", "ях", "ов", "ев", "ой", "ей",
    "ая", "яя", "ою", "ею", "ам", "ям", "ие", "ые", "ый", "ий", "ом", "ем", "у", "ю", "ы", "и",
    "е", "а", "я", "о",
)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _as_utc(raw: Any) -> datetime | None:
    if isinstance(raw, datetime):
        parsed = raw
    else:
        text = str(raw or "").strip()
        if not text:
            return None
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat()


def _clean_text(raw: Any, *, limit: int) -> str:
    text = re.sub(r"\s+", " ", str(raw or "")).strip()
    if len(text) <= limit:
        return text
    return text[:limit].rsplit(" ", 1)[0].strip() or text[:limit]


def _tokens(text: Any) -> List[str]:
    out: List[str] = []
    for token in _TOKEN_RE.findall(str(text or "").lower().replace("ё", "е")):
        if token in _STOPWORDS:
            continue
        normalized = token
        for suffix in _SUFFIXES:
            if normalized.endswith(suffix) and len(normalized) - len(suffix) >= 3:
                normalized = normalized[: -len(suffix)]
                break
        if normalized not in out:
            out.append(normalized)
    return out


def normalize_settings(raw: Any) -> Dict[str, Any]:
    data = raw if isinstance(raw, dict) else {}
    return {
        "schema_version": ROLLING_MEMORY_SCHEMA_VERSION,
        "enabled": bool(data.get("enabled", True)),
        "sample_rate": max(0.0, min(1.0, float(data.get("sample_rate", 0.10)))),
        "recall_rate": max(0.0, min(1.0, float(data.get("recall_rate", 0.15)))),
        "ttl_days": max(1, min(30, int(data.get("ttl_days", 4)))),
        "max_items_per_chat": max(1, min(1000, int(data.get("max_items_per_chat", 120)))),
        "max_pending_per_chat": max(1, min(200, int(data.get("max_pending_per_chat", 30)))),
        "process_interval_seconds": max(10, min(3600, int(data.get("process_interval_seconds", 60)))),
        "max_summaries_per_chat_per_day": max(
            1, min(500, int(data.get("max_summaries_per_chat_per_day", 20)))
        ),
        "daily_token_budget_per_chat": max(500, min(1_000_000, int(data.get("daily_token_budget_per_chat", 8000)))),
        "context_messages": max(1, min(8, int(data.get("context_messages", 3)))),
        "summary_max_chars": max(40, min(500, int(data.get("summary_max_chars", 180)))),
        "summary_max_tokens": max(40, min(500, int(data.get("summary_max_tokens", 120)))),
    }


def ensure_state(
    chat_mem: Dict[str, Any],
    settings: Dict[str, Any],
    *,
    now: datetime | None = None,
) -> Dict[str, Any]:
    layers = chat_mem.setdefault("memory_layers", {})
    state = layers.setdefault("rolling_memory", {})
    if not isinstance(state, dict):
        state = {}
        layers["rolling_memory"] = state
    state["schema_version"] = ROLLING_MEMORY_SCHEMA_VERSION
    state.setdefault("pending", [])
    state.setdefault("items", [])
    state.setdefault("daily_usage", {})
    state.setdefault("metrics", {})
    state.setdefault("last_processed_at", None)
    state.setdefault("last_error", None)
    prune_state(state, settings, now=now)
    return state


def prune_state(state: Dict[str, Any], settings: Dict[str, Any], *, now: datetime | None = None) -> None:
    current = now or _utc_now()
    items = state.get("items", [])
    if not isinstance(items, list):
        items = []
    kept_items = [item for item in items if (_as_utc(item.get("expires_at")) or current) > current]
    removed = len(items) - len(kept_items)
    kept_items.sort(key=lambda item: str(item.get("created_at", "")))
    max_items = int(settings["max_items_per_chat"])
    if len(kept_items) > max_items:
        removed += len(kept_items) - max_items
        kept_items = kept_items[-max_items:]
    state["items"] = kept_items
    if removed:
        metrics = state.setdefault("metrics", {})
        metrics["expired_or_evicted"] = int(metrics.get("expired_or_evicted", 0)) + removed

    pending = state.get("pending", [])
    if not isinstance(pending, list):
        pending = []
    ttl = timedelta(days=int(settings["ttl_days"]))
    pending = [
        item
        for item in pending
        if current - (_as_utc(item.get("anchor_ts")) or _as_utc(item.get("enqueued_at")) or current) < ttl
    ]
    state["pending"] = pending[-int(settings["max_pending_per_chat"]):]

    usage = state.get("daily_usage", {})
    today = current.date().isoformat()
    if not isinstance(usage, dict) or str(usage.get("date", "")) != today:
        state["daily_usage"] = {"date": today, "summaries": 0, "tokens": 0}


def should_sample(chat_id: int, message_id: int, sample_rate: float) -> bool:
    rate = max(0.0, min(1.0, float(sample_rate)))
    digest = hashlib.sha256(f"{int(chat_id)}:{int(message_id)}".encode("utf-8")).digest()
    bucket = int.from_bytes(digest[:8], "big") / float(2**64)
    return bucket < rate


def _eligible_anchor(rec: Dict[str, Any]) -> bool:
    text = _clean_text(rec.get("text"), limit=1000)
    if bool(rec.get("is_bot")) or not text or text.startswith("/") or len(text) < 8:
        return False
    return not bool(_NOISE_RE.fullmatch(text))


def enqueue_from_history(
    chat_mem: Dict[str, Any],
    *,
    chat_id: int,
    anchor: Dict[str, Any],
    settings: Dict[str, Any],
    now: datetime | None = None,
    force: bool = False,
) -> bool:
    if not settings.get("enabled") or not _eligible_anchor(anchor):
        return False
    message_id = int(anchor.get("message_id", 0) or 0)
    if not message_id or (not force and not should_sample(chat_id, message_id, float(settings["sample_rate"]))):
        return False

    current = now or _utc_now()
    state = ensure_state(chat_mem, settings, now=current)
    candidate_id = hashlib.sha1(f"rolling:{int(chat_id)}:{message_id}".encode("utf-8")).hexdigest()[:24]
    if any(str(item.get("id")) == candidate_id for item in state["pending"] + state["items"]):
        return False

    history = chat_mem.get("history", [])
    context: List[Dict[str, Any]] = []
    for rec in history:
        if int(rec.get("message_id", 0) or 0) > message_id:
            continue
        text = _clean_text(rec.get("text"), limit=500)
        if not text:
            continue
        context.append(
            {
                "message_id": int(rec.get("message_id", 0) or 0),
                "name": _clean_text(rec.get("name") or rec.get("username") or rec.get("user_id"), limit=80),
                "text": text,
                "is_bot": bool(rec.get("is_bot")),
                "ts": str(rec.get("ts", "")),
            }
        )
    context = context[-int(settings["context_messages"]):]
    if not context:
        return False

    state["pending"].append(
        {
            "id": candidate_id,
            "anchor_message_id": message_id,
            "anchor_ts": str(anchor.get("ts") or _iso(current)),
            "source_message_ids": [int(item["message_id"]) for item in context if int(item["message_id"]) > 0],
            "context": context,
            "enqueued_at": _iso(current),
            "attempts": 0,
            "next_attempt_at": _iso(current),
        }
    )
    state["pending"] = state["pending"][-int(settings["max_pending_per_chat"]):]
    metrics = state.setdefault("metrics", {})
    metrics["enqueued"] = int(metrics.get("enqueued", 0)) + 1
    return True


def next_pending(state: Dict[str, Any], settings: Dict[str, Any], *, now: datetime | None = None) -> Dict[str, Any] | None:
    current = now or _utc_now()
    prune_state(state, settings, now=current)
    usage = state["daily_usage"]
    if int(usage.get("summaries", 0)) >= int(settings["max_summaries_per_chat_per_day"]):
        return None
    if int(usage.get("tokens", 0)) >= int(settings["daily_token_budget_per_chat"]):
        return None
    for candidate in state["pending"]:
        if (_as_utc(candidate.get("next_attempt_at")) or current) <= current:
            return candidate
    return None


def build_summary_messages(candidate: Dict[str, Any], settings: Dict[str, Any]) -> List[Dict[str, str]]:
    clip = [
        {
            "message_id": int(item.get("message_id", 0) or 0),
            "author": _clean_text(item.get("name"), limit=80),
            "text": _clean_text(item.get("text"), limit=500),
            "is_bot": bool(item.get("is_bot")),
        }
        for item in candidate.get("context", [])
        if isinstance(item, dict) and _clean_text(item.get("text"), limit=500)
    ]
    return [
        {
            "role": "system",
            "content": (
                "Ты сжимаешь живой фрагмент дружеского чата в краткое воспоминание. "
                "Не додумывай факты и не оценивай людей. keep=false для служебного шума, односложной реакции или "
                "фрагмента без самостоятельного смысла. Верни только JSON: "
                '{"keep":true,"summary":"...","keywords":["..."],"participants":["..."]}. '
                f"summary — до {int(settings['summary_max_chars'])} знаков; keywords — до 8; participants — до 6."
            ),
        },
        {"role": "user", "content": json.dumps({"messages": clip}, ensure_ascii=False)},
    ]


def parse_summary(raw: str, settings: Dict[str, Any]) -> Dict[str, Any] | None:
    try:
        payload = json.loads(str(raw or ""))
    except (TypeError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict) or not isinstance(payload.get("keep"), bool):
        return None
    if payload["keep"] is False:
        return {"keep": False, "summary": "", "keywords": [], "participants": []}
    if not isinstance(payload.get("summary"), str):
        return None
    if not isinstance(payload.get("keywords", []), list) or not isinstance(payload.get("participants", []), list):
        return None
    if any(not isinstance(item, str) for item in payload.get("keywords", []) + payload.get("participants", [])):
        return None
    summary = _clean_text(payload["summary"], limit=int(settings["summary_max_chars"]))
    if not summary:
        return None
    return {
        "keep": True,
        "summary": summary,
        "keywords": [_clean_text(item, limit=40) for item in payload.get("keywords", [])[:8] if _clean_text(item, limit=40)],
        "participants": [
            _clean_text(item, limit=80) for item in payload.get("participants", [])[:6] if _clean_text(item, limit=80)
        ],
    }


def complete_candidate(
    state: Dict[str, Any],
    candidate: Dict[str, Any],
    decision: Dict[str, Any],
    *,
    token_usage: int,
    settings: Dict[str, Any],
    now: datetime | None = None,
) -> Dict[str, Any] | None:
    current = now or _utc_now()
    state["pending"] = [item for item in state.get("pending", []) if str(item.get("id")) != str(candidate.get("id"))]
    usage = state.setdefault("daily_usage", {"date": current.date().isoformat(), "summaries": 0, "tokens": 0})
    usage["summaries"] = int(usage.get("summaries", 0)) + 1
    usage["tokens"] = int(usage.get("tokens", 0)) + max(0, int(token_usage))
    metrics = state.setdefault("metrics", {})
    metrics["processed"] = int(metrics.get("processed", 0)) + 1
    state["last_processed_at"] = _iso(current)
    state["last_error"] = None
    if not decision.get("keep"):
        metrics["rejected"] = int(metrics.get("rejected", 0)) + 1
        return None

    anchor_ts = _as_utc(candidate.get("anchor_ts")) or current
    expires_at = anchor_ts + timedelta(days=int(settings["ttl_days"]))
    if expires_at <= current:
        metrics["expired_or_evicted"] = int(metrics.get("expired_or_evicted", 0)) + 1
        return None
    context = candidate.get("context", [])
    excerpt = " | ".join(
        f"{_clean_text(item.get('name'), limit=50)}: {_clean_text(item.get('text'), limit=180)}"
        for item in context
        if isinstance(item, dict)
    )
    item = {
        "id": str(candidate.get("id")),
        "summary": str(decision.get("summary", "")),
        "source_excerpt": _clean_text(excerpt, limit=600),
        "source_message_ids": list(candidate.get("source_message_ids", [])),
        "participants": list(decision.get("participants", [])),
        "keywords": list(decision.get("keywords", [])),
        "created_at": _iso(anchor_ts),
        "expires_at": _iso(expires_at),
        "last_recalled_at": None,
        "recall_count": 0,
    }
    state.setdefault("items", []).append(item)
    metrics["created"] = int(metrics.get("created", 0)) + 1
    prune_state(state, settings, now=current)
    return item


def fail_candidate(
    state: Dict[str, Any],
    candidate: Dict[str, Any],
    error: str,
    *,
    token_usage: int = 0,
    now: datetime | None = None,
) -> bool:
    current = now or _utc_now()
    attempts = int(candidate.get("attempts", 0)) + 1
    candidate["attempts"] = attempts
    state["last_error"] = _clean_text(error, limit=300)
    metrics = state.setdefault("metrics", {})
    metrics["processing_errors"] = int(metrics.get("processing_errors", 0)) + 1
    usage = state.setdefault("daily_usage", {"date": current.date().isoformat(), "summaries": 0, "tokens": 0})
    usage["summaries"] = int(usage.get("summaries", 0)) + 1
    usage["tokens"] = int(usage.get("tokens", 0)) + max(0, int(token_usage))
    retry_delays = (5, 15, 60)
    if attempts > len(retry_delays):
        state["pending"] = [item for item in state.get("pending", []) if str(item.get("id")) != str(candidate.get("id"))]
        metrics["failed"] = int(metrics.get("failed", 0)) + 1
        return False
    candidate["next_attempt_at"] = _iso(current + timedelta(minutes=retry_delays[attempts - 1]))
    return True


def select_recall(
    state: Dict[str, Any],
    query_text: str,
    settings: Dict[str, Any],
    *,
    now: datetime | None = None,
    rng: Any = random,
    force: bool = False,
) -> Dict[str, Any] | None:
    current = now or _utc_now()
    prune_state(state, settings, now=current)
    if not settings.get("enabled") or (not force and rng.random() >= float(settings["recall_rate"])):
        return None
    query_tokens = set(_tokens(query_text))
    query_low = str(query_text or "").lower()
    scored: List[tuple[float, Dict[str, Any]]] = []
    for item in state.get("items", []):
        item_tokens = set(_tokens(item.get("summary"))) | set(_tokens(" ".join(item.get("keywords", []))))
        overlap = len(query_tokens & item_tokens)
        participant_hits = sum(1 for name in item.get("participants", []) if str(name).lower() in query_low)
        if overlap <= 0 and participant_hits <= 0:
            continue
        created = _as_utc(item.get("created_at")) or current
        age_hours = max(0.0, (current - created).total_seconds() / 3600.0)
        freshness = max(0.0, 4.0 - age_hours / 24.0)
        repetition_penalty = 1.0 / (1.0 + int(item.get("recall_count", 0)))
        scored.append(((overlap * 10.0 + participant_hits * 7.0 + freshness) * repetition_penalty, item))
    if not scored:
        return None
    top = sorted(scored, key=lambda pair: (-pair[0], str(pair[1].get("created_at", ""))))[:5]
    chosen = rng.choices([item for _, item in top], weights=[max(0.01, score) for score, _ in top], k=1)[0]
    chosen["last_recalled_at"] = _iso(current)
    chosen["recall_count"] = int(chosen.get("recall_count", 0)) + 1
    metrics = state.setdefault("metrics", {})
    metrics["recalled"] = int(metrics.get("recalled", 0)) + 1
    return chosen


def format_recall_prompt(item: Dict[str, Any] | None) -> str:
    if not item:
        return ""
    return (
        "живое воспоминание из этого чата:\n"
        f"- {str(item.get('summary', '')).strip()}\n"
        "используй только если отсылка звучит естественно; не пересказывай воспоминание и не выдумывай детали"
    )


def status_snapshot(state: Dict[str, Any], settings: Dict[str, Any], *, now: datetime | None = None) -> Dict[str, Any]:
    prune_state(state, settings, now=now)
    return {
        "enabled": bool(settings.get("enabled")),
        "pending": len(state.get("pending", [])),
        "active": len(state.get("items", [])),
        "daily_usage": dict(state.get("daily_usage", {})),
        "metrics": dict(state.get("metrics", {})),
        "last_processed_at": state.get("last_processed_at"),
        "last_error": state.get("last_error"),
        "latest": list(reversed(state.get("items", [])[-5:])),
    }
