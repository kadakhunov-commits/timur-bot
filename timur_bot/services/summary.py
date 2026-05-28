from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Awaitable, Callable, Dict, List, Literal
from zoneinfo import ZoneInfo

SUMMARY_MAX_MESSAGES = 5000
SUMMARY_MIN_MESSAGES_FOR_GENERAL_TOPIC = 6

Mode = Literal["last_n", "since_time", "from_message"]
WindowStatus = Literal["ok", "empty", "too_many", "not_found"]


@dataclass
class SummaryRequest:
    mode: Mode
    n: int | None = None
    since_utc: datetime | None = None
    since_label: str | None = None
    from_message_id: int | None = None


@dataclass
class SummaryWindow:
    status: WindowStatus
    selected_total: int
    text_messages: List[Dict[str, Any]]
    requested_limit: int = SUMMARY_MAX_MESSAGES


def usage_hint() -> str:
    return (
        "юзай так: /summary N или /summary since 14:00 или реплаем на сообщение и /summary"
    )


def parse_summary_request(
    raw_args: str,
    *,
    reply_message_id: int | None,
    now_utc: datetime | None = None,
    tz_name: str = "Europe/Moscow",
) -> tuple[SummaryRequest | None, str | None]:
    args = (raw_args or "").strip()
    if not args:
        if reply_message_id:
            return SummaryRequest(mode="from_message", from_message_id=int(reply_message_id)), None
        return None, usage_hint()

    if re.fullmatch(r"\d{1,5}", args):
        n = int(args)
        if n <= 0:
            return None, "число должно быть больше нуля"
        return SummaryRequest(mode="last_n", n=n), None

    m = re.fullmatch(r"since\s+(\d{1,2}):(\d{2})", args, flags=re.IGNORECASE)
    if m:
        hour = int(m.group(1))
        minute = int(m.group(2))
        if not (0 <= hour <= 23 and 0 <= minute <= 59):
            return None, "время кривое, формат нужен такой: /summary since 14:00"
        now_utc = now_utc or datetime.utcnow().replace(tzinfo=timezone.utc)
        local_tz = ZoneInfo(tz_name)
        now_local = now_utc.astimezone(local_tz)
        since_local = now_local.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if since_local > now_local:
            since_local = since_local - timedelta(days=1)
        since_utc = since_local.astimezone(timezone.utc).replace(tzinfo=None)
        return SummaryRequest(mode="since_time", since_utc=since_utc, since_label=f"{hour:02d}:{minute:02d}"), None

    return None, usage_hint()


def _parse_iso_ts(ts: str) -> datetime | None:
    raw = (ts or "").strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).replace(tzinfo=None)
    except ValueError:
        return None


def select_summary_window(
    history: List[Dict[str, Any]],
    request: SummaryRequest,
    *,
    max_messages: int = SUMMARY_MAX_MESSAGES,
) -> SummaryWindow:
    rows = history if isinstance(history, list) else []
    selected: List[Dict[str, Any]] = []

    if request.mode == "last_n":
        n = int(request.n or 0)
        selected = rows[-n:] if n > 0 else []
    elif request.mode == "since_time":
        since_utc = request.since_utc
        if since_utc is None:
            return SummaryWindow(status="empty", selected_total=0, text_messages=[], requested_limit=max_messages)
        for rec in rows:
            ts = _parse_iso_ts(str(rec.get("ts", "")))
            if ts and ts >= since_utc:
                selected.append(rec)
    else:
        target_message_id = int(request.from_message_id or 0)
        start_idx = None
        for i, rec in enumerate(rows):
            rec_id_raw = rec.get("message_id")
            try:
                rec_id = int(rec_id_raw)
            except Exception:
                rec_id = 0
            if rec_id == target_message_id:
                start_idx = i
                break
        if start_idx is None:
            return SummaryWindow(status="not_found", selected_total=0, text_messages=[], requested_limit=max_messages)
        selected = rows[start_idx:]

    selected_total = len(selected)
    if selected_total == 0:
        return SummaryWindow(status="empty", selected_total=0, text_messages=[], requested_limit=max_messages)
    if selected_total > max_messages:
        return SummaryWindow(
            status="too_many",
            selected_total=selected_total,
            text_messages=[],
            requested_limit=max_messages,
        )

    text_messages: List[Dict[str, Any]] = []
    for rec in selected:
        text = str(rec.get("text", "")).strip()
        if not text:
            continue
        name = str(rec.get("name") or rec.get("username") or rec.get("user_id") or "unknown")
        text_messages.append(
            {
                "name": name,
                "text": text,
                "ts": str(rec.get("ts", "")),
                "is_bot": bool(rec.get("is_bot", False)),
                "user_id": rec.get("user_id"),
                "message_id": rec.get("message_id"),
            }
        )

    if not text_messages:
        return SummaryWindow(status="empty", selected_total=selected_total, text_messages=[], requested_limit=max_messages)

    return SummaryWindow(status="ok", selected_total=selected_total, text_messages=text_messages, requested_limit=max_messages)


def _format_ts_for_transcript(raw_ts: str, *, tz_name: str) -> str:
    dt = _parse_iso_ts(raw_ts)
    if not dt:
        return "--:--"
    dt_utc = dt.replace(tzinfo=timezone.utc)
    dt_local = dt_utc.astimezone(ZoneInfo(tz_name))
    return dt_local.strftime("%d.%m %H:%M")


def build_transcript_lines(messages: List[Dict[str, Any]], *, tz_name: str) -> List[str]:
    lines: List[str] = []
    for item in messages:
        text = re.sub(r"\s+", " ", str(item.get("text", "")).strip())
        if not text:
            continue
        stamp = _format_ts_for_transcript(str(item.get("ts", "")), tz_name=tz_name)
        speaker = str(item.get("name", "unknown")).strip() or "unknown"
        bot_tag = " [bot]" if bool(item.get("is_bot", False)) else ""
        lines.append(f"{stamp} | {speaker}{bot_tag}: {text}")
    return lines


def _chunk_lines(lines: List[str], *, max_chunks: int = 8, max_chunk_chars: int = 24000) -> List[List[str]]:
    if not lines:
        return []
    target_chunks = min(max_chunks, max(1, math.ceil(len(lines) / 600)))
    base_size = max(1, math.ceil(len(lines) / target_chunks))
    initial = [lines[i:i + base_size] for i in range(0, len(lines), base_size)]
    chunks: List[List[str]] = []
    for chunk in initial:
        cur: List[str] = []
        cur_size = 0
        for line in chunk:
            add = len(line) + 1
            if cur and cur_size + add > max_chunk_chars:
                chunks.append(cur)
                cur = [line]
                cur_size = add
            else:
                cur.append(line)
                cur_size += add
        if cur:
            chunks.append(cur)
    return chunks


def _extract_json(raw: str) -> Dict[str, Any] | None:
    text = (raw or "").strip()
    if not text:
        return None
    fence = re.search(r"```(?:json)?\s*(\{.*\})\s*```", text, flags=re.DOTALL | re.IGNORECASE)
    if fence:
        text = fence.group(1).strip()
    else:
        left = text.find("{")
        right = text.rfind("}")
        if left >= 0 and right > left:
            text = text[left:right + 1]
    try:
        payload = json.loads(text)
    except Exception:
        return None
    if isinstance(payload, dict):
        return payload
    return None


def _normalize_chunk_payload(raw: Dict[str, Any] | None) -> Dict[str, Any]:
    if not isinstance(raw, dict):
        return {"topics": [], "announcements": []}
    topics = raw.get("topics")
    announcements = raw.get("announcements")
    return {
        "topics": topics if isinstance(topics, list) else [],
        "announcements": announcements if isinstance(announcements, list) else [],
    }


def _normalize_final_payload(raw: Dict[str, Any] | None) -> Dict[str, Any]:
    if not isinstance(raw, dict):
        return {}
    topic_messages = raw.get("topic_messages")
    if not isinstance(topic_messages, list):
        topic_messages = []
    cleaned_topics = [str(x).strip() for x in topic_messages if str(x).strip()]
    announcements_message = str(raw.get("announcements_message", "")).strip()
    fallback_message = str(raw.get("fallback_message", "")).strip()
    return {
        "topic_messages": cleaned_topics[:8],
        "announcements_message": announcements_message,
        "fallback_message": fallback_message,
    }


def _clean_plain_message(raw: str) -> str:
    text = (raw or "").strip()
    if not text:
        return ""
    fenced = re.search(r"```(?:json)?\s*(.*?)\s*```", text, flags=re.DOTALL | re.IGNORECASE)
    if fenced:
        text = fenced.group(1).strip()
    payload = _extract_json(text)
    if isinstance(payload, dict):
        candidate = str(payload.get("message", "")).strip()
        if candidate:
            return candidate
    return text.strip().strip("\"'")


async def build_summary_messages(
    *,
    text_messages: List[Dict[str, Any]],
    tz_name: str,
    system_prompt: str,
    active_mode: str,
    mode_prompt: str,
    style_settings: str,
    bio_settings: str,
    llm_call: Callable[[List[Dict[str, str]], int, float], Awaitable[str]],
) -> List[str]:
    lines = build_transcript_lines(text_messages, tz_name=tz_name)
    if not lines:
        return []

    chunks = _chunk_lines(lines)
    chunk_reports: List[Dict[str, Any]] = []
    human_messages = sum(1 for x in text_messages if not bool(x.get("is_bot", False)))
    bot_messages = sum(1 for x in text_messages if bool(x.get("is_bot", False)))

    for idx, chunk in enumerate(chunks, start=1):
        analysis_prompt = (
            "сделай анализ куска переписки и верни только json.\n"
            "нужно выявить темы и важные объявления.\n"
            "не включай короткие/одноразовые темы.\n"
            "темы, где в основном пишет бот, считай второстепенными.\n"
            "формат json:\n"
            "{\n"
            '  "topics": [\n'
            "    {\n"
            '      "title": "краткий заголовок",\n'
            '      "summary": "1-2 предложения по сути",\n'
            '      "participant_driven": true,\n'
            '      "importance": 0,\n'
            '      "message_count": 0\n'
            "    }\n"
            "  ],\n"
            '  "announcements": [\n'
            "    {\n"
            '      "text": "суть объявления",\n'
            '      "kind": "meeting|proposal|notice|other"\n'
            "    }\n"
            "  ]\n"
            "}\n"
            f"это chunk {idx}/{len(chunks)}.\n"
            "переписка:\n"
            + "\n".join(chunk)
        )
        raw = await llm_call(
            [
                {"role": "system", "content": "ты аналитик чатов. отвечай строго валидным json без пояснений."},
                {"role": "user", "content": analysis_prompt},
            ],
            900,
            0.2,
        )
        chunk_reports.append(_normalize_chunk_payload(_extract_json(raw)))

    character_prompt = (
        f"{system_prompt}\n\n"
        "режим summary:\n"
        "- игнорируй настроение/муд полностью\n"
        "- сохрани только характер тимура и его манеру речи\n"
        "- всегда строчные буквы, без эмодзи\n"
        "- каждое сообщение: 1-2 коротких предложения\n"
        f"- активный режим личности: {active_mode}\n"
        f"- инструкция режима: {mode_prompt}\n"
    )
    if style_settings:
        character_prompt += f"\nдоп стиль от владельца:\n{style_settings}\n"
    if bio_settings:
        character_prompt += f"\nбио тимура от владельца:\n{bio_settings}\n"

    final_prompt = (
        "собери итоговое summary из json-отчетов.\n"
        "правила:\n"
        "- разбей summary по темам: одна тема = одно отдельное сообщение\n"
        "- не включай совсем короткие или одиночные темы\n"
        "- темы где в основном диалог с ботом пропускай, если там нет реально важного для участников\n"
        "- важные объявления (встречи, предложения, сборы, организационные штуки) вынеси отдельным сообщением, если они не часть больших тем\n"
        f"- если total_text_messages >= {SUMMARY_MIN_MESSAGES_FOR_GENERAL_TOPIC}, обязательно дай минимум один topic_messages как общую тему, не пиши что пусто\n"
        "- если сообщений совсем мало и темы реально не сложились, можно дать fallback_message\n"
        "- отвечай только json, без текста вокруг\n"
        "формат json:\n"
        "{\n"
        '  "topic_messages": ["...", "..."],\n'
        '  "announcements_message": "...",\n'
        '  "fallback_message": "..."\n'
        "}\n"
        f"статистика диапазона: human_messages={human_messages}, bot_messages={bot_messages}, total_text_messages={len(text_messages)}.\n"
        "json-отчеты по кускам:\n"
        + json.dumps(chunk_reports, ensure_ascii=False)
    )

    final_raw = await llm_call(
        [
            {"role": "system", "content": character_prompt},
            {"role": "user", "content": final_prompt},
        ],
        1200,
        0.55,
    )
    payload = _normalize_final_payload(_extract_json(final_raw))
    topic_messages: List[str] = payload.get("topic_messages", [])
    announcements_message = str(payload.get("announcements_message", "")).strip()
    fallback_message = str(payload.get("fallback_message", "")).strip()

    output: List[str] = []
    output.extend(topic_messages)
    if announcements_message:
        output.append(announcements_message)
    if not output and fallback_message:
        output.append(fallback_message)
    if not output and len(text_messages) >= SUMMARY_MIN_MESSAGES_FOR_GENERAL_TOPIC:
        general_prompt = (
            "в диапазоне много сообщений, но явные отдельные темы не оформлены.\n"
            "сделай одну общую тему в стиле тимура про то, что в целом обсуждали.\n"
            "не говори что пусто.\n"
            'верни только json вида {"message":"..."}.\n'
            "json-отчеты по кускам:\n"
            + json.dumps(chunk_reports, ensure_ascii=False)
        )
        general_raw = await llm_call(
            [
                {"role": "system", "content": character_prompt},
                {"role": "user", "content": general_prompt},
            ],
            180,
            0.55,
        )
        general_message = _clean_plain_message(general_raw)
        if general_message:
            output.append(general_message)
    return output
