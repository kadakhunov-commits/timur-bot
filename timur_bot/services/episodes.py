"""Episodic memory: тимур remembers vivid shared moments (Phase 1, M4).

Beyond atomic facts, a real chat member recalls *moments* — "а помнишь как ты
психанул", "помнишь как тебя все хвалили". This keeps a small per-chat ring
buffer of salient events (those with a strong emotional charge) and recalls them
by relevance to the current message, so callbacks feel lived rather than listed.

Deterministic and cheap: no LLM, salience is a quick marker-based valence.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List

MAX_EPISODES = 60
_SALIENCE_THRESHOLD = 3.0
_SUMMARY_MAX_CHARS = 120

_STOPWORDS = {
    "это", "как", "что", "где", "когда", "кто", "или", "для", "тут", "там",
    "так", "уже", "если", "тогда", "типа", "вот", "тебя", "тебе", "мне", "его",
    "она", "они", "был", "была", "были", "есть", "очень", "просто", "потом",
    "the", "and", "you", "that", "this", "with", "just",
}


# Light suffix stripper so russian inflections match ("рыбалке"/"рыбалку" ->
# "рыбалк", "щука"/"щукой" -> "щук"). Cheap, no morphology library.
_SUFFIXES = (
    "ами", "ями", "ого", "его", "ому", "ему", "ыми", "ими", "ах", "ях",
    "ов", "ев", "ой", "ей", "ая", "яя", "ою", "ею", "ам", "ям", "ие", "ые",
    "ый", "ий", "ом", "ем", "у", "ю", "ы", "и", "е", "а", "я", "о",
)


def _stem(token: str) -> str:
    for suffix in _SUFFIXES:
        if token.endswith(suffix) and len(token) - len(suffix) >= 3:
            return token[: -len(suffix)]
    return token


def _keywords(text: str, *, limit: int = 8) -> List[str]:
    tokens: List[str] = []
    seen: set[str] = set()
    for raw in re.findall(r"[a-zA-Zа-яА-ЯёЁ0-9]{3,}", str(text or "")):
        token = raw.lower().replace("ё", "е")
        if token.isdigit() or token in _STOPWORDS or token in seen:
            continue
        seen.add(token)
        tokens.append(token)
        if len(tokens) >= limit:
            break
    return tokens


def _stem_set(tokens) -> set[str]:
    return {_stem(token) for token in tokens}

_POSITIVE = ("красава", "спасибо", "хорош", "люблю", "ты лучший", "молодец", "топ", "респект", "ору", "лол", "ахах")
_NEGATIVE = ("дебил", "идиот", "чмо", "мраз", "нахуй", "пошел", "бесишь", "ненавиж", "заткнись", "психанул", "бомбит")
_TENDER = ("держись", "все ок", "всё ок", "не парься", "сочувств", "обнимаю", "люблю тебя")


def message_valence(text: str) -> float:
    """Quick emotional charge of a message in roughly [-10, 10]."""
    low = str(text or "").lower()
    if not low.strip():
        return 0.0
    valence = 0.0
    if any(marker in low for marker in _POSITIVE):
        valence += 4.0
    if any(marker in low for marker in _TENDER):
        valence += 4.5
    if any(marker in low for marker in _NEGATIVE):
        valence -= 6.0
    exclam = text.count("!")
    if exclam >= 2:
        valence += 1.5 if valence >= 0 else -1.5
    caps_tokens = [t for t in re.findall(r"[A-Za-zА-Яа-яЁё]{3,}", text) if t.isupper()]
    if len(caps_tokens) >= 2:
        valence += -2.0 if valence < 0 else 2.0
    return round(max(-10.0, min(10.0, valence)), 2)


def _compact(text: str) -> str:
    compact = re.sub(r"\s+", " ", str(text or "").strip())
    if len(compact) <= _SUMMARY_MAX_CHARS:
        return compact
    return compact[: _SUMMARY_MAX_CHARS - 1].rstrip() + "…"


def maybe_log_episode(
    chat_mem: Dict[str, Any],
    *,
    actor: str,
    text: str,
    valence: float,
    ts: str,
) -> bool:
    """Append a salient moment to the chat's episode buffer; return True if logged."""
    if abs(valence) < _SALIENCE_THRESHOLD:
        return False
    summary = _compact(text)
    if not summary:
        return False
    episodes: List[Dict[str, Any]] = chat_mem.setdefault("episodes", [])
    episodes.append(
        {
            "ts": ts,
            "actor": str(actor or "кто-то"),
            "summary": summary,
            "valence": float(valence),
            "keywords": _keywords(text, limit=8),
        }
    )
    if len(episodes) > MAX_EPISODES:
        del episodes[: len(episodes) - MAX_EPISODES]
    return True


def recall_episodes(chat_mem: Dict[str, Any], query_text: str, *, limit: int = 2) -> List[str]:
    """Return compact 'я помню как…' lines relevant to the current message."""
    episodes = chat_mem.get("episodes", [])
    if not isinstance(episodes, list) or not episodes:
        return []
    query_keys = _stem_set(_keywords(query_text, limit=10))
    if not query_keys:
        return []

    scored: List[tuple[float, Dict[str, Any]]] = []
    for index, ep in enumerate(episodes):
        ep_keys = _stem_set(ep.get("keywords") or [])
        overlap = len(query_keys & ep_keys)
        if overlap <= 0:
            continue
        recency = index / max(1, len(episodes) - 1)
        score = overlap * 10.0 + abs(float(ep.get("valence", 0.0))) + recency * 2.0
        scored.append((score, ep))

    scored.sort(key=lambda item: -item[0])
    lines: List[str] = []
    for _, ep in scored[:limit]:
        tone = "тепло" if float(ep.get("valence", 0.0)) >= 0 else "на нервах"
        lines.append(f"я помню как {ep.get('actor')} писал ({tone}): {ep.get('summary')}")
    return lines


def build_episodes_block(lines: List[str]) -> str:
    if not lines:
        return ""
    return "из нашей общей истории:\n" + "\n".join(f"- {line}" for line in lines)
