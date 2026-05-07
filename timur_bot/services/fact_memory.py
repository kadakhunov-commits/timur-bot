from __future__ import annotations

import re
from datetime import datetime, timezone
from hashlib import sha1
from typing import Any, Dict, Iterable, List, Optional, Tuple

MAX_FACTS = 600
MAX_TAGS_PER_FACT = 10
MAX_EDGES = 1200

_STOPWORDS = {
    "это", "как", "что", "где", "когда", "кто", "или", "для", "тут", "там",
    "его", "ее", "её", "она", "они", "оно", "мой", "моя", "мое", "моё", "твой",
    "твоя", "твое", "твоё", "ваш", "ваша", "есть", "был", "была", "были", "просто",
    "очень", "потом", "после", "снова", "типа", "если", "тогда", "чисто", "блин",
    "короче", "ну", "да", "нет", "ага", "вот", "тебя", "тебе", "мне", "ему", "ней",
    "него", "нее", "нее", "их", "нас", "вам", "где-то", "который", "которая",
}
_QUESTION_HINTS = (
    "фамил", "родил", "родом", "жив", "возраст", "лет", "зовут", "имя", "учил",
    "школ", "универ", "вуз", "поступ", "откуда", "работ", "кто ты", "кто он",
    "кто она", "био", "где", "откуда",
)
_BOT_MARKERS = ("ты", "тебя", "твой", "твоя", "твоё", "тимур")
_SELF_PATTERNS: List[Tuple[str, re.Pattern[str]]] = [
    ("surname", re.compile(r"\b(?:моя\s+)?фамили[яи]\s*(?:это|:|[-—])?\s*([a-zа-яё-]{2,40})", re.I)),
    ("full_name", re.compile(r"\bменя\s+зовут\s+([a-zа-яё-]{2,40}(?:\s+[a-zа-яё-]{2,40})?)", re.I)),
    ("birth_place", re.compile(r"\bродил(?:ся|ась)\s+в\s+([a-zа-яё0-9 .,-]{2,80})", re.I)),
    ("residence", re.compile(r"\bжив[еу][тм]?\s+в\s+([a-zа-яё0-9 .,-]{2,80})", re.I)),
    ("age", re.compile(r"\b(?:мне|ему|ей)\s+(\d{1,2})\b", re.I)),
    ("school", re.compile(r"\bучил(?:ся|ась)\s+в\s+([a-zа-яё0-9№# .,-]{2,80})", re.I)),
    ("university", re.compile(r"\b(?:поступил|поступила|учусь|учился|училась)\s+в\s+([a-zа-яё0-9№# .,-]{2,80})", re.I)),
    ("origin", re.compile(r"\bродом\s+из\s+([a-zа-яё0-9 .,-]{2,80})", re.I)),
]
_QUESTION_ATTR_MAP: List[Tuple[str, str]] = [
    ("фамил", "surname"),
    ("зовут", "full_name"),
    ("имя", "full_name"),
    ("родил", "birth_place"),
    ("родом", "origin"),
    ("откуда", "origin"),
    ("жив", "residence"),
    ("возраст", "age"),
    ("лет", "age"),
    ("школ", "school"),
    ("учил", "school"),
    ("универ", "university"),
    ("вуз", "university"),
    ("поступ", "university"),
]


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def ensure_fact_graph(chat_mem: Dict[str, Any]) -> Dict[str, Any]:
    layers = chat_mem.setdefault("memory_layers", {})
    graph = layers.setdefault("fact_graph", {})
    graph.setdefault("entities", {})
    graph.setdefault("facts", [])
    graph.setdefault("edges", {})
    graph.setdefault("recent_fact_ids", [])
    return graph


def normalize_token(token: str) -> str:
    return re.sub(r"\s+", " ", str(token or "").strip().lower().replace("ё", "е"))


def tagify(text: str, limit: int = MAX_TAGS_PER_FACT) -> List[str]:
    raw = re.findall(r"[a-zа-яё0-9-]{3,}", normalize_token(text))
    tags: List[str] = []
    for token in raw:
        if token in _STOPWORDS or token.isdigit():
            continue
        if token not in tags:
            tags.append(token)
        if len(tags) >= limit:
            break
    return tags


def make_entity_id(*, user_id: Optional[int], username: str = "", name: str = "", is_bot: bool = False) -> str:
    if is_bot:
        return "bot:self"
    if user_id:
        return f"user:{int(user_id)}"
    handle = normalize_token(username or name)
    if handle:
        return f"alias:{handle}"
    return "alias:unknown"


def ensure_entity(
    chat_mem: Dict[str, Any],
    *,
    entity_id: str,
    title: str,
    kind: str,
    aliases: Optional[Iterable[str]] = None,
) -> Dict[str, Any]:
    graph = ensure_fact_graph(chat_mem)
    entities = graph.setdefault("entities", {})
    entity = entities.setdefault(
        entity_id,
        {
            "id": entity_id,
            "title": title,
            "kind": kind,
            "aliases": [],
            "updated_at": None,
        },
    )
    if title:
        entity["title"] = title
    current_aliases = list(entity.get("aliases", []))
    for alias in aliases or []:
        clean = str(alias or "").strip()
        if clean and clean not in current_aliases:
            current_aliases.append(clean)
    entity["aliases"] = current_aliases[:12]
    entity["updated_at"] = _utc_now_iso()
    return entity


def infer_question_attribute(question_text: str) -> str:
    clean = normalize_token(question_text)
    for needle, attr in _QUESTION_ATTR_MAP:
        if needle in clean:
            return attr
    return "claim"


def _extract_value_by_attr(attribute: str, reply_text: str) -> str:
    clean = re.sub(r"\s+", " ", str(reply_text or "")).strip()
    if not clean:
        return ""
    for attr, pattern in _SELF_PATTERNS:
        if attr != attribute:
            continue
        match = pattern.search(clean)
        if match:
            return match.group(1).strip(" .,!?:;")
    if attribute == "surname" and len(clean.split()) <= 3:
        return clean.strip(" .,!?:;")
    if attribute == "age":
        match = re.search(r"\b(\d{1,2})\b", clean)
        if match:
            return match.group(1)
    if attribute == "claim" and len(clean) <= 120:
        return clean.strip(" .,!?:;")
    return ""


def _entity_candidates_from_question(chat_mem: Dict[str, Any], question_text: str) -> List[Tuple[str, str, str]]:
    text_low = normalize_token(question_text)
    candidates: List[Tuple[str, str, str]] = []
    if any(marker in text_low for marker in _BOT_MARKERS):
        candidates.append(("bot:self", "тимур", "bot"))

    participants = chat_mem.get("participants", {})
    for pdata in participants.values():
        user_id = int(pdata.get("user_id", 0))
        if not user_id:
            continue
        username = str(pdata.get("username") or "").strip()
        name = str(pdata.get("name") or "").strip()
        checks = [f"@{normalize_token(username)}" if username else "", normalize_token(username), normalize_token(name)]
        if any(check and check in text_low for check in checks):
            title = f"@{username}" if username else (name or f"user {user_id}")
            candidates.append((f"user:{user_id}", title, "user"))
    return candidates


def infer_fact_subject(
    chat_mem: Dict[str, Any],
    question_text: str,
    reply_text: str,
) -> Tuple[str, str, str]:
    question_candidates = _entity_candidates_from_question(chat_mem, question_text)
    if question_candidates:
        return question_candidates[0]

    clean_reply = normalize_token(reply_text)
    if re.search(r"\bя\b", clean_reply) or re.search(r"\bмне\b", clean_reply):
        return ("bot:self", "тимур", "bot")

    if question_text and any(hint in normalize_token(question_text) for hint in _QUESTION_HINTS):
        return ("bot:self", "тимур", "bot")

    return ("bot:self", "тимур", "bot")


def build_fact_record(
    *,
    entity_id: str,
    entity_title: str,
    entity_kind: str,
    attribute: str,
    value: str,
    source: str,
    confidence: float,
    question_text: str,
    reply_text: str,
) -> Dict[str, Any]:
    compact_value = re.sub(r"\s+", " ", value).strip()
    text = f"{entity_title}: {attribute} = {compact_value}"
    tags = tagify(" ".join([entity_title, attribute, compact_value, question_text]))[:MAX_TAGS_PER_FACT]
    fact_key = normalize_token(f"{entity_id}|{attribute}|{compact_value}")
    fact_id = sha1(fact_key.encode("utf-8")).hexdigest()[:12]
    aliases = []
    if attribute not in tags:
        aliases.append(attribute)
    return {
        "id": fact_id,
        "entity_id": entity_id,
        "entity_title": entity_title,
        "entity_kind": entity_kind,
        "attribute": attribute,
        "value": compact_value,
        "text": text,
        "source": source,
        "confidence": round(float(confidence), 3),
        "tags": tags,
        "aliases": aliases,
        "weight": 1.0,
        "created_at": _utc_now_iso(),
        "updated_at": _utc_now_iso(),
        "question": re.sub(r"\s+", " ", question_text).strip()[:220],
        "reply": re.sub(r"\s+", " ", reply_text).strip()[:220],
    }


def extract_claim_facts(chat_mem: Dict[str, Any], question_text: str, reply_text: str) -> List[Dict[str, Any]]:
    clean_q = re.sub(r"\s+", " ", str(question_text or "")).strip()
    clean_r = re.sub(r"\s+", " ", str(reply_text or "")).strip()
    if not clean_q or not clean_r:
        return []

    entity_id, entity_title, entity_kind = infer_fact_subject(chat_mem, clean_q, clean_r)
    attribute = infer_question_attribute(clean_q)
    value = _extract_value_by_attr(attribute, clean_r)
    if not value:
        return []

    if len(value) > 90:
        value = value[:90].rsplit(" ", 1)[0].strip()
    if not value:
        return []

    confidence = 0.92 if attribute != "claim" else 0.66
    source = "self_claim" if entity_id == "bot:self" else "other_claim"
    return [
        build_fact_record(
            entity_id=entity_id,
            entity_title=entity_title,
            entity_kind=entity_kind,
            attribute=attribute,
            value=value,
            source=source,
            confidence=confidence,
            question_text=clean_q,
            reply_text=clean_r,
        )
    ]


def _edge_key(left: str, right: str) -> str:
    a, b = sorted([left, right])
    return f"{a}|{b}"


def upsert_claim_facts(chat_mem: Dict[str, Any], facts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not facts:
        return []

    graph = ensure_fact_graph(chat_mem)
    ensure_entity(chat_mem, entity_id="bot:self", title="тимур", kind="bot", aliases=["тимур"])
    stored = graph.setdefault("facts", [])
    recent_ids = graph.setdefault("recent_fact_ids", [])
    edges = graph.setdefault("edges", {})
    touched: List[Dict[str, Any]] = []

    for fact in facts:
        ensure_entity(
            chat_mem,
            entity_id=str(fact.get("entity_id")),
            title=str(fact.get("entity_title") or fact.get("entity_id") or "entity"),
            kind=str(fact.get("entity_kind") or "entity"),
            aliases=fact.get("aliases", []),
        )
        existing = next((item for item in stored if item.get("id") == fact.get("id")), None)
        if existing:
            existing["weight"] = round(float(existing.get("weight", 1.0)) + 1.0, 3)
            existing["confidence"] = max(float(existing.get("confidence", 0.0)), float(fact.get("confidence", 0.0)))
            existing["updated_at"] = _utc_now_iso()
            existing["reply"] = fact.get("reply", existing.get("reply", ""))
            existing["question"] = fact.get("question", existing.get("question", ""))
            touched.append(existing)
        else:
            stored.append(fact)
            touched.append(fact)

        entity_id = str(fact.get("entity_id"))
        for tag in fact.get("tags", [])[:MAX_TAGS_PER_FACT]:
            key = _edge_key(entity_id, f"tag:{tag}")
            edges[key] = round(float(edges.get(key, 0.0)) + 1.0, 3)
        attr_key = _edge_key(entity_id, f"attr:{fact.get('attribute')}")
        edges[attr_key] = round(float(edges.get(attr_key, 0.0)) + 1.2, 3)

        fact_id = str(fact.get("id"))
        if fact_id in recent_ids:
            recent_ids.remove(fact_id)
        recent_ids.append(fact_id)

    stored.sort(
        key=lambda item: (
            -float(item.get("weight", 0.0)),
            -float(item.get("confidence", 0.0)),
            str(item.get("entity_id", "")),
            str(item.get("attribute", "")),
        )
    )
    if len(stored) > MAX_FACTS:
        del stored[MAX_FACTS:]
    if len(recent_ids) > 80:
        del recent_ids[:-80]

    if len(edges) > MAX_EDGES:
        top = sorted(edges.items(), key=lambda item: (-float(item[1]), item[0]))[:MAX_EDGES]
        graph["edges"] = dict(top)

    return touched
