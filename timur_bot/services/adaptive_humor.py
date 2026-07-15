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


def interjection_messages(history: Iterable[Dict[str, Any]], humor_hint: str) -> List[Dict[str, str]]:
    return [
        {
            "role": "system",
            "content": (
                "Ты одновременно автор и строгий редактор короткой реплики для живого русского чата. "
                "Мысленно придумай три варианта и верни только лучший в JSON "
                '{"score":0..100,"reply":"текст или пусто"}. Score выше 85 — только если реплика '
                "точная, контекстная и лучше молчания. Не повторяй последнее слово или оговорку, не "
                "пересказывай контекст, не шути о качестве своего юмора и не упоминай ИИ. Если сильной "
                "реплики нет, reply должен быть пустым."
            ),
        },
        {"role": "user", "content": f"чат:\n{_compact_history(history)}\n\nориентиры вкуса:\n{humor_hint}"},
    ]


def parse_interjection(text: str) -> tuple[int, str]:
    payload = _json(text)
    try:
        score = max(0, min(100, int(payload.get("score", 0))))
    except (TypeError, ValueError):
        score = 0
    return score, re.sub(r"\s+", " ", str(payload.get("reply", ""))).strip()[:280]
