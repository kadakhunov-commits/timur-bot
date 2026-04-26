from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Iterable, List


def _fmt_ts(ts: str) -> str:
    raw = str(ts or "").strip()
    if not raw:
        return "-"
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00")).replace(tzinfo=None)
        return dt.strftime("%Y-%m-%d %H:%M")
    except Exception:
        return raw[:16]


def format_funny_status(settings: Dict[str, Any], state: Dict[str, Any]) -> str:
    budget = state.get("budget", {}) if isinstance(state.get("budget"), dict) else {}
    data = state.get("state", {}) if isinstance(state.get("state"), dict) else {}
    candidates = state.get("candidates", {}) if isinstance(state.get("candidates"), dict) else {}

    by_status = {"new": 0, "approved": 0, "rejected": 0, "sent": 0}
    for item in candidates.values():
        if not isinstance(item, dict):
            continue
        status = str(item.get("status") or "new")
        if status in by_status:
            by_status[status] += 1

    enabled_sources = [s for s in (settings.get("sources") or []) if isinstance(s, dict) and bool(s.get("enabled", True))]
    return (
        "смешные моменты (scanner)\n"
        f"enabled: {'on' if settings.get('enabled') else 'off'}\n"
        f"intensity: {settings.get('intensity', 'balanced')}\n"
        f"sources active: {len(enabled_sources)}\n"
        f"period: {settings.get('scan_period_hours', 24)}h, schedule: {settings.get('scan_schedule_minutes', 60)}m\n"
        f"thresholds: stage1={settings.get('stage1_min_score', 42)} review={settings.get('review_threshold', 70)}\n"
        f"limits: cand={settings.get('max_candidates_per_scan', 30)}, llm={settings.get('max_llm_candidates_per_scan', 12)}, fwd/day={settings.get('daily_forward_limit', 20)}\n"
        f"budget: {budget.get('tokens_used', 0)}/{settings.get('daily_token_budget', 0)} (hard {settings.get('daily_token_hard_stop', 0)}) day={budget.get('day', '-')}\n"
        f"queue: new={by_status['new']} approved={by_status['approved']} rejected={by_status['rejected']} sent={by_status['sent']}\n"
        f"last_scan: {_fmt_ts(str(data.get('last_scan_ts') or ''))}"
    )


def format_funny_sources(settings: Dict[str, Any], *, known_sources: Iterable[Dict[str, Any]]) -> str:
    current = {int(item.get("chat_id")): item for item in (settings.get("sources") or []) if isinstance(item, dict)}
    rows: List[str] = ["источники сканирования:"]
    for source in known_sources:
        if not isinstance(source, dict):
            continue
        chat_id = int(source.get("chat_id", 0))
        title = str(source.get("title") or source.get("name") or source.get("chat_id") or chat_id)
        current_item = current.get(chat_id)
        is_on = bool(current_item.get("enabled", True)) if current_item else False
        rows.append(f"- {'[x]' if is_on else '[ ]'} {title} ({chat_id})")
    if len(rows) == 1:
        rows.append("- пока нет доступных чатов с историей")
    rows.append("нажми на кнопку чата ниже, чтобы переключить источник")
    return "\n".join(rows)


def format_funny_candidates_list(candidates: Iterable[Dict[str, Any]]) -> str:
    lines = ["кандидаты (new):"]
    count = 0
    for item in candidates:
        if not isinstance(item, dict):
            continue
        count += 1
        lines.append(
            f"- {item.get('id')} | score={item.get('score') if item.get('score') is not None else item.get('pre_score')} | "
            f"chat={item.get('source_chat_id')} | {item.get('status', 'new')}"
        )
    if count == 0:
        lines.append("- пока пусто")
    return "\n".join(lines)


def format_funny_candidate_preview(candidate: Dict[str, Any]) -> str:
    lines = [
        f"кандидат: {candidate.get('id')}",
        f"источник: {candidate.get('source_chat_title') or candidate.get('source_chat_id')}",
        f"score: {candidate.get('score') if candidate.get('score') is not None else candidate.get('pre_score')}",
        f"статус: {candidate.get('status', 'new')}",
        f"границы: {candidate.get('time_start', '-')[:19]} -> {candidate.get('time_end', '-')[:19]}",
    ]
    reason = str(candidate.get("llm_reason_short") or "").strip()
    if reason:
        lines.append(f"почему: {reason}")

    lines.append("")
    lines.append("фрагмент:")
    for row in (candidate.get("cluster_messages") or [])[:14]:
        if not isinstance(row, dict):
            continue
        author = str(row.get("author") or "unknown")[:24]
        text = str(row.get("text") or "").strip()
        if len(text) > 220:
            text = text[:220] + "…"
        lines.append(f"{author}: {text}")
    return "\n".join(lines)
