"""Participant memory: dossiers on the people тимур talks to (Phase 1, M3).

Two halves:

- **Bidirectional extraction** — learn facts from what a participant says about
  themselves ("я из питера", "мне 25", "работаю в яндексе"), not just from the
  bot's own replies. Facts are stored on the existing per-chat fact graph under
  the ``user:<id>`` entity, so the miniapp fact-map already renders them.
- **Dossier** — a first-person "что я помню про <друга>" block (known facts,
  relationship tone, личные мемы) injected when that friend speaks or is
  mentioned, so тимур references shared history like a real chat member.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Tuple

from timur_bot.services.fact_memory import build_fact_record, upsert_claim_facts

# Speaker-anchored self-statements: the person is talking about themselves, so
# the subject is the speaker (unlike fact_memory which infers from a question).
# Capture clauses stop at sentence punctuation so a value never spans into the
# next statement (e.g. "я живу в питере. я работаю..." -> "питере", not the rest).
_CLAUSE = r"([^.,;!?\n]{2,60})"
_SELF_STATEMENT_PATTERNS: Tuple[Tuple[re.Pattern[str], str, float], ...] = (
    (re.compile(r"\bменя\s+зовут\s+([a-zа-яё-]{2,40}(?:\s+[a-zа-яё-]{2,40})?)", re.I), "full_name", 0.9),
    (re.compile(r"\b(?:моя\s+)?фамилия\s+(?:это\s+|[-—:]\s*)?([a-zа-яё-]{2,40})", re.I), "surname", 0.9),
    (re.compile(r"\bмне\s+(\d{1,2})\s*(?:лет|год|года)?\b", re.I), "age", 0.85),
    (re.compile(r"\bя\s+роди?л(?:ся|ась)\s+в\s+" + _CLAUSE, re.I), "birth_place", 0.9),
    (re.compile(r"\bя\s+жив[уё]\w*\s+в\s+" + _CLAUSE, re.I), "residence", 0.85),
    (re.compile(r"\bя\s+родом\s+из\s+" + _CLAUSE, re.I), "origin", 0.85),
    (re.compile(r"\bя\s+(?:сам\s+)?из\s+([a-zа-яё-]{3,30})\b", re.I), "origin", 0.75),
    (re.compile(r"\bя\s+работа[юе]\w*\s+(?:в\s+|на\s+)?" + _CLAUSE, re.I), "work", 0.8),
    (re.compile(r"\bя\s+уч[ауеи]\w*\s+в\s+" + _CLAUSE, re.I), "education_place", 0.8),
    (re.compile(r"\bя\s+люблю\s+" + _CLAUSE, re.I), "likes", 0.7),
)

_RAPPORT_POSITIVE = ("красава", "спасибо", "хорош", "люблю", "ты лучший", "молодец", "топ", "респект", "обнял")
_RAPPORT_NEGATIVE = ("дебил", "идиот", "чмо", "мраз", "тупой", "заткнись", "бесишь", "ненавиж")
_RAPPORT_MIN, _RAPPORT_MAX = -12.0, 12.0

_ATTR_PHRASING: Dict[str, str] = {
    "full_name": "его зовут {v}",
    "surname": "фамилия {v}",
    "age": "ему {v}",
    "birth_place": "родился в {v}",
    "residence": "живёт в {v}",
    "origin": "родом из {v}",
    "hometown": "родом из {v}",
    "work": "работает: {v}",
    "education_place": "учится/учился в {v}",
    "school": "школа: {v}",
    "university": "универ: {v}",
    "likes": "любит {v}",
}


def _clean_capture(value: str) -> str:
    cleaned = re.sub(r"\s+", " ", str(value or "").strip())
    cleaned = re.sub(r"\s+(но|а|и|потому|так как|чтобы)\s+.*$", "", cleaned, flags=re.I)
    return cleaned.strip(" \"'.,!?:;-")


def extract_participant_facts(text: str) -> List[Tuple[str, str, float]]:
    """Return (attribute, value, confidence) for self-statements in a message."""
    compact = re.sub(r"\s+", " ", str(text or "").strip())
    if not compact:
        return []
    seen: set[str] = set()
    facts: List[Tuple[str, str, float]] = []
    for pattern, attribute, confidence in _SELF_STATEMENT_PATTERNS:
        match = pattern.search(compact)
        if not match:
            continue
        value = _clean_capture(match.group(1))
        if not value or len(value) > 90:
            if value:
                value = value[:90].rsplit(" ", 1)[0].strip()
            if not value:
                continue
        key = f"{attribute}|{value.lower()}"
        if key in seen:
            continue
        seen.add(key)
        facts.append((attribute, value, confidence))
    return facts


def _participant_title(name: str, username: str, user_id: int) -> str:
    if name:
        return name
    if username:
        return f"@{username}"
    return f"user {user_id}"


def learn_participant_facts(
    chat_mem: Dict[str, Any],
    *,
    user_id: int,
    name: str,
    username: str,
    text: str,
) -> List[Dict[str, Any]]:
    """Extract self-stated facts from a participant message and store them."""
    raw_facts = extract_participant_facts(text)
    if not raw_facts:
        return []

    entity_id = f"user:{int(user_id)}"
    title = _participant_title(name, username, user_id)
    records = [
        build_fact_record(
            entity_id=entity_id,
            entity_title=title,
            entity_kind="user",
            attribute=attribute,
            value=value,
            source="self_statement",
            confidence=confidence,
            question_text="",
            reply_text=text,
        )
        for attribute, value, confidence in raw_facts
    ]
    return upsert_claim_facts(chat_mem, records)


def update_rapport(chat_mem: Dict[str, Any], user_id: int, text: str) -> float:
    """Nudge how warmly тимур reads this person, based on how they treat him."""
    participants = chat_mem.setdefault("participants", {})
    p = participants.get(str(user_id))
    if p is None:
        return 0.0
    low = str(text or "").lower()
    delta = 0.0
    if any(marker in low for marker in _RAPPORT_POSITIVE):
        delta += 1.5
    if any(marker in low for marker in _RAPPORT_NEGATIVE):
        delta -= 2.5
    if delta:
        rapport = float(p.get("rapport", 0.0)) + delta
        p["rapport"] = round(max(_RAPPORT_MIN, min(_RAPPORT_MAX, rapport)), 3)
    return float(p.get("rapport", 0.0))


def _rapport_label(rapport: float) -> str:
    if rapport >= 4.0:
        return "тёплые, он свой"
    if rapport <= -4.0:
        return "напряжённые, он часто наезжает"
    return "ровные"


def _participant_facts(chat_mem: Dict[str, Any], user_id: int, *, limit: int) -> List[Tuple[str, str]]:
    graph = chat_mem.get("memory_layers", {}).get("fact_graph", {})
    entity_id = f"user:{int(user_id)}"
    facts = [
        fact
        for fact in graph.get("facts", [])
        if str(fact.get("entity_id")) == entity_id and str(fact.get("value", "")).strip()
    ]
    facts.sort(
        key=lambda item: (-float(item.get("weight", 0.0)), -float(item.get("confidence", 0.0)), str(item.get("attribute", "")))
    )
    out: List[Tuple[str, str]] = []
    seen_attrs: set[str] = set()
    for fact in facts:
        attribute = str(fact.get("attribute", ""))
        if attribute in seen_attrs:
            continue
        seen_attrs.add(attribute)
        out.append((attribute, str(fact.get("value", "")).strip()))
        if len(out) >= limit:
            break
    return out


def _top_keywords(participant: Dict[str, Any], *, limit: int = 4) -> List[str]:
    kw = participant.get("keywords", {})
    if not isinstance(kw, dict) or not kw:
        return []
    ordered = sorted(kw.items(), key=lambda item: (-float(item[1]), item[0]))
    return [token for token, _ in ordered[:limit]]


def build_participant_dossier(chat_mem: Dict[str, Any], user_id: int, *, max_facts: int = 5) -> str:
    """First-person 'что я помню про <друга>' block, or '' if nothing notable."""
    participants = chat_mem.get("participants", {})
    p = participants.get(str(int(user_id)))
    if not p:
        return ""

    name = str(p.get("name") or (f"@{p['username']}" if p.get("username") else "") or f"user {user_id}")
    facts = _participant_facts(chat_mem, user_id, limit=max_facts)
    rapport = float(p.get("rapport", 0.0))
    keywords = _top_keywords(p)

    lines: List[str] = []
    for attribute, value in facts:
        template = _ATTR_PHRASING.get(attribute, attribute + ": {v}")
        lines.append("- " + template.format(v=value))
    if abs(rapport) >= 4.0:
        lines.append(f"- наши отношения: {_rapport_label(rapport)}")
    if keywords:
        lines.append("- его темы/мемы: " + ", ".join(keywords))

    if not lines:
        return ""
    return f"что я помню про {name}:\n" + "\n".join(lines)
