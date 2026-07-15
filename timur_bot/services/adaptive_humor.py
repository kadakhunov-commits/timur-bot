"""Prompt and JSON helpers for rare, quality-gated chat interjections."""

from __future__ import annotations

import json
import re
from typing import Any, Dict, Iterable, List


def _compact_history(history: Iterable[Dict[str, Any]], *, limit: int = 8) -> str:
    lines = []
    for row in list(history)[-limit:]:
        text = re.sub(r"\s+", " ", str(row.get("text", ""))).strip()
        if not text:
            continue
        author = str(row.get("name") or row.get("username") or row.get("user_id") or "кто-то")
        lines.append(f"{author}: {text[:220]}")
    return "\n".join(lines)


def _json(text: str) -> Dict[str, Any]:
    raw = (text or "").strip()
    if not raw:
        return {}
    if not raw.startswith("{"):
        start, end = raw.find("{"), raw.rfind("}")
        if start >= 0 and end > start:
            raw = raw[start : end + 1]
    try:
        value = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    return value if isinstance(value, dict) else {}


def opportunity_messages(history: Iterable[Dict[str, Any]]) -> List[Dict[str, str]]:
    return [
        {
            "role": "system",
            "content": (
                "Ты строгий редактор тайминга для живого чата. Оцени, стоит ли редкому участнику "
                "вклиниться с коротким дружеским подъебом. Не предлагай реплику. Верни JSON "
                '{"score":0..100,"reason":"до 120 символов"}. Высокий score только если без него '
                "действительно теряется очевидная смешная добивка; иначе ставь ниже 85. Не повышай "
                "оценку только потому, что в сообщении есть смешное слово, оговорка или мем."
            ),
        },
        {"role": "user", "content": "чат:\n" + _compact_history(history)},
    ]


def candidates_messages(history: Iterable[Dict[str, Any]], humor_hint: str, *, count: int) -> List[Dict[str, str]]:
    return [
        {
            "role": "system",
            "content": (
                "Ты пишешь редкую меткую реплику для дружеского русского чата. Верни только JSON "
                '{"candidates":["..."]}. Нужны разные короткие варианты: строчные буквы, без эмодзи, '
                "без объяснений и личного унижения. Не копируй старые шутки дословно. Не повторяй "
                "буквально слово, оговорку или тему из последнего сообщения; добавь новый образ или "
                "поворот. Никогда не шути о том, умеешь ли ты шутить, и не делай шутки про ИИ."
            ),
        },
        {
            "role": "user",
            "content": f"контекст:\n{_compact_history(history)}\n\nориентиры вкуса:\n{humor_hint}\n\nвариантов: {count}",
        },
    ]


def judge_messages(history: Iterable[Dict[str, Any]], candidates: Iterable[str]) -> List[Dict[str, str]]:
    return [
        {
            "role": "system",
            "content": (
                "Ты строгий судья юмора конкретного чата. Выбери максимум один вариант, только если он "
                "контекстный, короткий, не повторяет старый мем и действительно лучше молчания. Верни JSON "
                '{"score":0..100,"winner":"точный текст или пусто","reason":"до 120 символов"}. '
                "Отклоняй каламбуры, которые просто повторяют последнее слово, пересказ контекста и "
                "самоиронию о качестве юмора. Если ни один не тянет, winner должен быть пустой строкой."
            ),
        },
        {
            "role": "user",
            "content": "чат:\n" + _compact_history(history) + "\n\nварианты:\n" + "\n".join(f"- {x}" for x in candidates),
        },
    ]


def parse_opportunity(text: str) -> int:
    try:
        return max(0, min(100, int(_json(text).get("score", 0))))
    except (TypeError, ValueError):
        return 0


def parse_candidates(text: str, *, limit: int) -> List[str]:
    raw = _json(text).get("candidates", [])
    if not isinstance(raw, list):
        return []
    out = []
    for item in raw:
        cleaned = re.sub(r"\s+", " ", str(item or "")).strip()
        if cleaned and cleaned not in out:
            out.append(cleaned[:280])
        if len(out) >= limit:
            break
    return out


def parse_judgement(text: str) -> tuple[int, str]:
    payload = _json(text)
    try:
        score = max(0, min(100, int(payload.get("score", 0))))
    except (TypeError, ValueError):
        score = 0
    return score, re.sub(r"\s+", " ", str(payload.get("winner", ""))).strip()[:280]
