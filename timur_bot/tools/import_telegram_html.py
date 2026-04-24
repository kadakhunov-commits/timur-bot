from __future__ import annotations

import argparse
import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from html import unescape
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

import yaml

from timur_bot.services.text_processing import (
    detect_archetype_scores,
    extract_keywords,
)
from timur_bot.services.humor import add_joke_bit, ensure_humor_schema

AUTO_STYLE_MARKER = "автопрофиль чата (generated):"
OWNER_OVERRIDE_MARKER = "=== owner override ==="


@dataclass
class MessageRecord:
    message_id: int
    from_name: str
    text: str
    ts: str


@dataclass
class ParsedResult:
    messages: List[MessageRecord]
    skipped_service: int
    skipped_empty: int


class TelegramHtmlParser(HTMLParser):
    """Parses Telegram export HTML into plain text message records."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.messages: List[MessageRecord] = []
        self._current: Optional[Dict[str, Any]] = None
        self._message_depth = 0

        self._field: Optional[str] = None
        self._field_depth = 0
        self._text_parts: List[str] = []

        self._last_from_name = ""
        self.skipped_service = 0
        self.skipped_empty = 0

    def handle_starttag(self, tag: str, attrs: List[Tuple[str, Optional[str]]]) -> None:
        attrs_map = {k: (v or "") for k, v in attrs}

        if self._current is not None:
            self._message_depth += 1

        if tag == "br" and self._field == "text":
            self._text_parts.append("\n")
            return

        if tag != "div":
            if self._field is not None:
                self._field_depth += 1
            return

        classes = set((attrs_map.get("class", "")).split())
        message_id_raw = attrs_map.get("id", "")
        id_match = re.fullmatch(r"message-?(?P<id>\d+)", message_id_raw)

        if self._current is None and "message" in classes and id_match:
            if "service" in classes:
                self._current = {
                    "is_service": True,
                    "id": message_id_raw,
                }
                self._message_depth = 1
                return

            try:
                message_id = int(id_match.group("id"))
            except ValueError:
                message_id = -1

            self._current = {
                "is_service": False,
                "message_id": message_id,
                "from_name": "",
                "text": "",
                "ts_raw": "",
            }
            self._message_depth = 1
            return

        if self._current is None:
            return

        if self._field is not None:
            self._field_depth += 1
            return

        if "from_name" in classes:
            self._field = "from_name"
            self._field_depth = 1
            self._text_parts = []
            return

        if "text" in classes:
            self._field = "text"
            self._field_depth = 1
            self._text_parts = []
            return

        if "date" in classes and "details" in classes:
            self._current["ts_raw"] = attrs_map.get("title", "").strip()

    def handle_endtag(self, tag: str) -> None:
        if self._field is not None:
            self._field_depth -= 1
            if self._field_depth <= 0:
                value = self._normalize_text("".join(self._text_parts))
                if self._current is not None:
                    self._current[self._field] = value
                self._field = None
                self._field_depth = 0
                self._text_parts = []

        if self._current is not None:
            self._message_depth -= 1
            if self._message_depth <= 0:
                self._finalize_current()
                self._current = None
                self._message_depth = 0

    def handle_data(self, data: str) -> None:
        if self._field is None:
            return
        self._text_parts.append(data)

    @staticmethod
    def _normalize_text(value: str) -> str:
        text = unescape(value or "")
        text = text.replace("\xa0", " ")
        text = re.sub(r"\s+", " ", text).strip()
        return text

    def _finalize_current(self) -> None:
        assert self._current is not None
        if self._current.get("is_service"):
            self.skipped_service += 1
            return

        raw_text = str(self._current.get("text", "")).strip()
        from_name = str(self._current.get("from_name", "")).strip() or self._last_from_name
        if not raw_text or not from_name:
            self.skipped_empty += 1
            return

        ts = _parse_export_ts_to_utc_iso(str(self._current.get("ts_raw", "")))
        if not ts:
            self.skipped_empty += 1
            return

        self._last_from_name = from_name
        self.messages.append(
            MessageRecord(
                message_id=int(self._current.get("message_id", -1)),
                from_name=from_name,
                text=raw_text,
                ts=ts,
            )
        )


def _parse_export_ts_to_utc_iso(raw: str) -> str:
    source = (raw or "").strip()
    if not source:
        return ""

    for fmt in ("%d.%m.%Y %H:%M:%S UTC%z", "%d.%m.%Y %H:%M:%S"):
        try:
            dt = datetime.strptime(source, fmt)
            if dt.tzinfo is None:
                return dt.isoformat()
            utc_dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
            return utc_dt.isoformat()
        except ValueError:
            continue
    return ""


def _normalize_name(name: str) -> str:
    return re.sub(r"\s+", " ", (name or "").strip().lower())


def _read_yaml(path: Path) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    if not isinstance(data, dict):
        raise ValueError(f"Expected mapping in YAML file: {path}")
    return data


def _load_lexicon(root: Path) -> Tuple[Set[str], Set[str], Set[str], Dict[str, Set[str]]]:
    lexicon = _read_yaml(root / "config" / "lexicon.yaml")

    def to_set(items: Any) -> Set[str]:
        if not isinstance(items, list):
            return set()
        return {str(x).strip().lower() for x in items if str(x).strip()}

    archetype_raw = lexicon.get("archetype_lexicon") or {}
    archetypes: Dict[str, Set[str]] = {}
    if isinstance(archetype_raw, dict):
        for k, v in archetype_raw.items():
            archetypes[str(k)] = to_set(v)

    return (
        to_set(lexicon.get("rus_stopwords")),
        to_set(lexicon.get("en_stopwords")),
        to_set(lexicon.get("profanity_markers")),
        archetypes,
    )


def _load_runtime_limits(root: Path) -> Dict[str, int]:
    runtime = _read_yaml(root / "config" / "runtime.yaml")
    limits = runtime.get("limits") if isinstance(runtime.get("limits"), dict) else {}
    return {
        "max_history_per_chat": int(limits.get("max_history_per_chat", 100)),
        "max_log_per_chat": int(limits.get("max_log_per_chat", 1000)),
        "max_user_samples": int(limits.get("max_user_samples", 20)),
        "max_quotes_per_user": int(limits.get("max_quotes_per_user", 6)),
        "max_keywords_per_user": int(limits.get("max_keywords_per_user", 40)),
        "max_topic_edges": int(limits.get("max_topic_edges", 300)),
        "max_user_relations": int(limits.get("max_user_relations", 300)),
    }


def _default_memory(system_prompt: str = "") -> Dict[str, Any]:
    return {
        "chats": {},
        "users": {},
        "config": {
            "system_prompt": system_prompt,
            "style_settings": "",
            "bio": "",
            "toxicity_level": 45,
            "active_mode": "default",
            "mode_overrides": {},
            "last_random_story_ts": None,
            "vision_usage": {},
            "life": {
                "enabled": True,
                "timezone": "Europe/Moscow",
                "daily_target": 3,
                "quiet_hours": {"start": "00:00", "end": "10:00"},
                "cooldown_per_chat_minutes": 360,
                "slots_date": "",
                "daily_slots": [],
                "sent_slots": [],
                "chat_last_emit": {},
                "story_log": [],
                "last_story_id": 0,
                "last_emit_ts": None,
                "last_emit_chat_id": None,
            },
        },
    }


def load_memory(memory_path: Path) -> Dict[str, Any]:
    if not memory_path.exists():
        return _default_memory()
    with open(memory_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        return _default_memory()
    data.setdefault("chats", {})
    data.setdefault("users", {})
    data.setdefault("config", {})
    return data


def save_memory(memory_path: Path, memory: Dict[str, Any]) -> None:
    with open(memory_path, "w", encoding="utf-8") as f:
        json.dump(memory, f, ensure_ascii=False, indent=2)


def ensure_chat_schema(chat: Dict[str, Any]) -> Dict[str, Any]:
    chat.setdefault("history", [])
    chat.setdefault("log", [])
    chat.setdefault("last_meme", None)
    chat.setdefault("participants", {})
    chat.setdefault("user_relations", {})
    chat.setdefault("topic_edges", {})
    layers = chat.setdefault("memory_layers", {})
    layers.setdefault("recent_messages", [])
    layers.setdefault("recent_facts", [])
    layers.setdefault("long_facts", [])
    layers.setdefault("summary", {"chat": "", "updated_at": None})
    layers.setdefault("imported_message_keys", [])
    ensure_humor_schema(chat)
    return chat


def ensure_user_schema(user: Dict[str, Any]) -> Dict[str, Any]:
    user.setdefault("name", "")
    user.setdefault("username", "")
    user.setdefault("count", 0)
    user.setdefault("samples", [])
    user.setdefault("events", [])
    user.setdefault("bio", "")
    return user


def _prune_counter(counter: Dict[str, Any], limit: int) -> Dict[str, Any]:
    if len(counter) <= limit:
        return counter
    top = sorted(counter.items(), key=lambda x: (-float(x[1]), x[0]))[:limit]
    return dict(top)


def _prune_records(records: List[Dict[str, Any]], limit: int) -> List[Dict[str, Any]]:
    if len(records) <= limit:
        return records
    return records[-limit:]


def _norm_fact_key(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip().lower())


def _parse_iso(value: str) -> datetime | None:
    raw = (value or "").strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw)
    except ValueError:
        return None


def _upsert_long_fact(layers: Dict[str, Any], text: str, last_seen_ts: str, boost: float = 1.0) -> None:
    long_facts = layers.setdefault("long_facts", [])
    key = _norm_fact_key(text)
    if not key:
        return
    for fact in long_facts:
        if _norm_fact_key(str(fact.get("text", ""))) == key:
            fact["strength"] = float(fact.get("strength", 0.0)) + float(boost)
            fact["last_seen_ts"] = last_seen_ts
            break
    else:
        long_facts.append(
            {
                "text": text,
                "last_seen_ts": last_seen_ts,
                "strength": float(boost),
            }
        )


def _compact_layers(
    chat_mem: Dict[str, Any],
    *,
    recent_days: int,
    max_recent_messages: int,
    max_recent_facts: int,
    max_long_facts: int,
    now_utc: datetime,
) -> None:
    layers = chat_mem.setdefault("memory_layers", {})
    recent_messages = layers.setdefault("recent_messages", [])
    recent_facts = layers.setdefault("recent_facts", [])

    if len(recent_messages) > max_recent_messages:
        del recent_messages[:-max_recent_messages]

    cutoff = now_utc - timedelta(days=recent_days)
    kept_recent_facts = []
    for fact in recent_facts:
        ts = _parse_iso(str(fact.get("ts", "")))
        if ts and ts < cutoff:
            _upsert_long_fact(
                layers,
                text=str(fact.get("text", "")),
                last_seen_ts=str(fact.get("ts", "")),
                boost=float(fact.get("weight", 1.0)),
            )
        else:
            kept_recent_facts.append(fact)

    kept_recent_facts.sort(key=lambda x: str(x.get("ts", "")))
    if len(kept_recent_facts) > max_recent_facts:
        kept_recent_facts = kept_recent_facts[-max_recent_facts:]
    layers["recent_facts"] = kept_recent_facts

    long_facts = layers.setdefault("long_facts", [])
    long_facts.sort(key=lambda x: (-float(x.get("strength", 0.0)), str(x.get("text", ""))))
    if len(long_facts) > max_long_facts:
        del long_facts[max_long_facts:]


def _backfill_layers_from_records(
    chat_mem: Dict[str, Any],
    *,
    records: List[Dict[str, Any]],
    recent_days: int,
    max_recent_messages: int,
    max_recent_facts: int,
    max_long_facts: int,
    now_utc: datetime,
) -> None:
    layers = chat_mem.setdefault("memory_layers", {})
    recent_messages = layers.setdefault("recent_messages", [])
    recent_facts = layers.setdefault("recent_facts", [])
    long_facts = layers.setdefault("long_facts", [])
    if recent_messages or recent_facts or long_facts:
        return

    cutoff = now_utc - timedelta(days=recent_days)
    ordered = sorted(records, key=lambda r: (str(r.get("ts", "")), int(r.get("message_id") or -1)))
    for rec in ordered:
        text = str(rec.get("text", "")).strip()
        if not text:
            continue
        ts = str(rec.get("ts", ""))
        msg_dt = _parse_iso(ts)
        fact_text = f"{rec.get('name') or rec.get('username') or rec.get('user_id')}: {text}"

        recent_messages.append(
            {
                "user_id": rec.get("user_id"),
                "name": rec.get("name", ""),
                "username": rec.get("username", ""),
                "text": text,
                "ts": ts,
                "message_id": rec.get("message_id"),
            }
        )

        if msg_dt and msg_dt >= cutoff:
            recent_facts.append({"text": fact_text, "ts": ts, "weight": 1.0})
        else:
            _upsert_long_fact(layers, text=fact_text, last_seen_ts=ts, boost=1.0)

    _compact_layers(
        chat_mem,
        recent_days=recent_days,
        max_recent_messages=max_recent_messages,
        max_recent_facts=max_recent_facts,
        max_long_facts=max_long_facts,
        now_utc=now_utc,
    )


def _synthetic_user_id(name: str) -> int:
    digest = hashlib.sha1(_normalize_name(name).encode("utf-8")).hexdigest()
    offset = int(digest[:12], 16) % 100_000_000_000
    return 900_000_000_000 + offset


def _top_items(counter: Dict[str, Any], n: int = 5) -> List[Tuple[str, float]]:
    items: List[Tuple[str, float]] = []
    for k, v in counter.items():
        try:
            items.append((k, float(v)))
        except Exception:
            continue
    items.sort(key=lambda x: (-x[1], x[0]))
    return items[:n]


def _relation_key(a: int, b: int) -> str:
    x, y = sorted([int(a), int(b)])
    return f"{x}|{y}"


def _topic_edge_key(a: str, b: str) -> str:
    x, y = sorted([a, b])
    return f"{x}|{y}"


def _message_key(message_id: int, ts: str, text: str) -> str:
    digest = hashlib.sha1(f"{message_id}|{ts}|{text}".encode("utf-8")).hexdigest()
    return digest[:20]


def _extract_mentions(participants: Dict[str, Any], text: str, author_id: int) -> List[int]:
    text_low = (text or "").lower()
    result: List[int] = []
    for uid_str, pdata in participants.items():
        uid = int(uid_str)
        if uid == author_id:
            continue
        name = (pdata.get("name") or "").lower().strip()
        if len(name) >= 2 and name in text_low:
            result.append(uid)
    return result


def parse_export_dir(src: Path) -> ParsedResult:
    files = sorted(
        src.glob("messages*.html"),
        key=lambda p: int(re.search(r"messages(\d+)?\.html$", p.name).group(1) or "0"),  # type: ignore[union-attr]
    )
    if not files:
        raise FileNotFoundError(f"No messages*.html files found in {src}")

    messages: List[MessageRecord] = []
    skipped_service = 0
    skipped_empty = 0

    for path in files:
        parser = TelegramHtmlParser()
        with open(path, "r", encoding="utf-8") as f:
            parser.feed(f.read())
        messages.extend(parser.messages)
        skipped_service += parser.skipped_service
        skipped_empty += parser.skipped_empty

    messages.sort(key=lambda m: (m.ts, m.message_id))
    return ParsedResult(
        messages=messages,
        skipped_service=skipped_service,
        skipped_empty=skipped_empty,
    )


def _resolve_user_ids(
    memory: Dict[str, Any],
    chat_id: int,
    names: Iterable[str],
) -> Tuple[Dict[str, int], List[str]]:
    warnings: List[str] = []
    index: Dict[str, Set[int]] = {}

    chat_mem = ensure_chat_schema(memory.setdefault("chats", {}).setdefault(str(chat_id), {}))

    for uid_str, user in memory.get("users", {}).items():
        norm = _normalize_name(str(user.get("name") or ""))
        if norm:
            index.setdefault(norm, set()).add(int(uid_str))

    for uid_str, pdata in chat_mem.get("participants", {}).items():
        norm = _normalize_name(str(pdata.get("name") or ""))
        if norm:
            index.setdefault(norm, set()).add(int(uid_str))

    resolved: Dict[str, int] = {}
    for raw_name in names:
        norm = _normalize_name(raw_name)
        if not norm:
            continue
        options = sorted(index.get(norm, set()))
        if len(options) == 1:
            resolved[raw_name] = options[0]
            continue
        if len(options) > 1:
            resolved[raw_name] = options[0]
            warnings.append(f"ambiguous name '{raw_name}' matched existing ids {options}, picked {options[0]}")
            continue
        generated = _synthetic_user_id(raw_name)
        resolved[raw_name] = generated
        index.setdefault(norm, set()).add(generated)

    return resolved, warnings


def _update_graph(
    chat_mem: Dict[str, Any],
    user_id: int,
    text: str,
    keywords: List[str],
    limits: Dict[str, int],
) -> None:
    user_relations = chat_mem.setdefault("user_relations", {})
    topic_edges = chat_mem.setdefault("topic_edges", {})

    for uid in _extract_mentions(chat_mem.get("participants", {}), text, author_id=user_id):
        rel_key = _relation_key(user_id, uid)
        user_relations[rel_key] = float(user_relations.get(rel_key, 0.0)) + 1.0

    for token in keywords:
        e = _topic_edge_key(f"u:{user_id}", f"k:{token}")
        topic_edges[e] = float(topic_edges.get(e, 0.0)) + 1.0

    for i in range(len(keywords)):
        for j in range(i + 1, len(keywords)):
            e = _topic_edge_key(f"k:{keywords[i]}", f"k:{keywords[j]}")
            topic_edges[e] = float(topic_edges.get(e, 0.0)) + 0.3

    chat_mem["user_relations"] = _prune_counter(user_relations, limits["max_user_relations"])
    chat_mem["topic_edges"] = _prune_counter(topic_edges, limits["max_topic_edges"])


def _build_style_profile(
    messages: Sequence[MessageRecord],
    rus_stopwords: Set[str],
    en_stopwords: Set[str],
    profanity_markers: Set[str],
) -> str:
    if not messages:
        return ""

    total = len(messages)
    total_chars = sum(len(m.text) for m in messages)
    short_count = sum(1 for m in messages if len(m.text) < 35)
    question_count = sum(1 for m in messages if "?" in m.text)
    caps_count = sum(1 for m in messages if re.search(r"[A-ZА-ЯЁ]{3,}", m.text))
    profanity_count = sum(
        1 for m in messages if any(marker in m.text.lower() for marker in profanity_markers)
    )

    token_counter: Dict[str, int] = {}
    meme_terms = ["лол", "кек", "ор", "угар", "ржака", "кринж", "ахаха", "пхаха"]
    meme_counter: Dict[str, int] = {k: 0 for k in meme_terms}

    for msg in messages:
        kws = extract_keywords(msg.text, rus_stopwords=rus_stopwords, en_stopwords=en_stopwords, limit=8)
        for token in kws:
            token_counter[token] = token_counter.get(token, 0) + 1
        low = msg.text.lower()
        for term in meme_terms:
            if term in low:
                meme_counter[term] += 1

    top_tokens = ", ".join(k for k, _ in _top_items(token_counter, n=12)) or "нет"
    top_memes = ", ".join(k for k, v in sorted(meme_counter.items(), key=lambda x: (-x[1], x[0])) if v > 0) or "нет"

    avg_len = int(total_chars / total)

    return "\n".join(
        [
            AUTO_STYLE_MARKER,
            f"- выборка: {total} сообщений",
            f"- средняя длина реплики: {avg_len} символов",
            f"- короткие реплики (<35): {round(short_count / total * 100, 1)}%",
            f"- вопросы: {round(question_count / total * 100, 1)}%",
            f"- капсовые акценты: {round(caps_count / total * 100, 1)}%",
            f"- грубая лексика: {round(profanity_count / total * 100, 1)}%",
            f"- частые темы/слова: {top_tokens}",
            f"- мемные маркеры: {top_memes}",
            "- ориентир для ответа: коротко, разговорно, с локальными отсылками, без длинных объяснений",
        ]
    )


def _merge_style_settings(old: str, generated: str) -> str:
    old_text = (old or "").strip()
    generated_text = (generated or "").strip()
    if not generated_text:
        return old_text

    preserved_owner = old_text
    if old_text.startswith(AUTO_STYLE_MARKER):
        parts = old_text.split(OWNER_OVERRIDE_MARKER, 1)
        preserved_owner = parts[1].strip() if len(parts) == 2 else ""

    if preserved_owner:
        return f"{generated_text}\n\n{OWNER_OVERRIDE_MARKER}\n{preserved_owner}".strip()
    return generated_text


def _build_joke_bank_from_messages(
    chat_mem: Dict[str, Any],
    messages: Sequence[MessageRecord],
    *,
    rus_stopwords: Set[str],
    en_stopwords: Set[str],
    profanity_markers: Set[str],
) -> None:
    candidates: Dict[str, float] = {}
    for msg in messages:
        text = re.sub(r"\s+", " ", msg.text or "").strip()
        low = text.lower()
        if not text or len(text) > 90:
            continue
        score = 0.0
        if len(text) <= 35:
            score += 1.0
        if any(marker in low for marker in profanity_markers):
            score += 1.0
        if any(marker in low for marker in ("лол", "ор", "ахаха", "пхаха", "кринж", "кек")):
            score += 1.5
        if len(extract_keywords(text, rus_stopwords=rus_stopwords, en_stopwords=en_stopwords, limit=4)) >= 2:
            score += 0.4
        if score >= 1.4:
            candidates[text] = candidates.get(text, 0.0) + score

    top = sorted(candidates.items(), key=lambda x: (-x[1], x[0]))[:60]
    for text, weight in top:
        try:
            add_joke_bit(chat_mem, text, source="import", weight=min(weight, 8.0))
        except ValueError:
            continue


def import_messages(
    memory: Dict[str, Any],
    parsed: ParsedResult,
    *,
    chat_id: int,
    mode: str,
    limits: Dict[str, int],
    rus_stopwords: Set[str],
    en_stopwords: Set[str],
    profanity_markers: Set[str],
    archetypes: Dict[str, Set[str]],
    apply_style_profile: bool,
    recent_days: int,
    max_recent_messages: int,
    max_recent_facts: int,
    max_long_facts: int,
    keep_raw_log: bool = False,
    now_utc: datetime | None = None,
) -> Dict[str, Any]:
    now_utc = now_utc or datetime.utcnow()
    before_history = len(ensure_chat_schema(memory.setdefault("chats", {}).setdefault(str(chat_id), {})).get("history", []))
    before_samples_total = sum(len(ensure_user_schema(u).get("samples", [])) for u in memory.setdefault("users", {}).values())
    before_layers = ensure_chat_schema(memory.setdefault("chats", {}).setdefault(str(chat_id), {})).get("memory_layers", {})
    before_recent_messages = len(before_layers.get("recent_messages", []))
    before_recent_facts = len(before_layers.get("recent_facts", []))
    before_long_facts = len(before_layers.get("long_facts", []))

    if mode == "replace":
        memory.setdefault("chats", {})[str(chat_id)] = ensure_chat_schema({})

    chat_mem = ensure_chat_schema(memory.setdefault("chats", {}).setdefault(str(chat_id), {}))
    layers = chat_mem.setdefault("memory_layers", {})
    _backfill_layers_from_records(
        chat_mem,
        records=chat_mem.get("log", []) or chat_mem.get("history", []),
        recent_days=recent_days,
        max_recent_messages=max_recent_messages,
        max_recent_facts=max_recent_facts,
        max_long_facts=max_long_facts,
        now_utc=now_utc,
    )

    resolved_ids, warnings = _resolve_user_ids(memory, chat_id, [m.from_name for m in parsed.messages])

    history = chat_mem.setdefault("history", [])
    log = chat_mem.setdefault("log", [])

    existing_keys = {
        _message_key(int(r.get("message_id") or -1), str(r.get("ts") or ""), str(r.get("text") or ""))
        for r in (history + log)
    }
    imported_keys = layers.setdefault("imported_message_keys", [])
    existing_keys.update(str(k) for k in imported_keys)

    imported = 0
    deduped = 0
    by_user_count: Dict[int, int] = {}
    imported_records: List[Dict[str, Any]] = []
    token_counter: Dict[str, int] = {}

    for msg in parsed.messages:
        user_id = resolved_ids.get(msg.from_name)
        if not user_id:
            continue

        key = _message_key(msg.message_id, msg.ts, msg.text)
        if key in existing_keys:
            if key not in imported_keys:
                imported_keys.append(key)
            deduped += 1
            continue
        existing_keys.add(key)
        imported_keys.append(key)

        user = ensure_user_schema(memory.setdefault("users", {}).setdefault(str(user_id), {}))
        user["name"] = msg.from_name
        user.setdefault("username", "")
        user["count"] = int(user.get("count", 0)) + 1

        samples = user.setdefault("samples", [])
        samples.append(msg.text)
        if len(samples) > limits["max_user_samples"]:
            del samples[:-limits["max_user_samples"]]

        rec = {
            "user_id": user_id,
            "name": msg.from_name,
            "username": user.get("username", ""),
            "text": msg.text,
            "ts": msg.ts,
            "is_bot": False,
            "message_id": msg.message_id,
        }
        history.append(rec)
        if keep_raw_log:
            log.append(rec)
        imported_records.append(rec)
        imported += 1
        by_user_count[user_id] = by_user_count.get(user_id, 0) + 1

        participants = chat_mem.setdefault("participants", {})
        p = participants.setdefault(
            str(user_id),
            {
                "user_id": user_id,
                "name": msg.from_name,
                "username": user.get("username", ""),
                "message_count": 0,
                "last_seen": msg.ts,
                "quotes": [],
                "keywords": {},
                "archetypes": {},
                "style": {
                    "questions": 0,
                    "profanity": 0,
                    "caps": 0,
                    "short_msgs": 0,
                },
            },
        )

        p["name"] = msg.from_name
        p["username"] = user.get("username", "")
        p["message_count"] = int(p.get("message_count", 0)) + 1
        p["last_seen"] = msg.ts

        style = p.setdefault("style", {})
        style["questions"] = int(style.get("questions", 0)) + int("?" in msg.text)
        style["short_msgs"] = int(style.get("short_msgs", 0)) + int(len(msg.text) < 35)
        if re.search(r"[A-ZА-ЯЁ]{3,}", msg.text):
            style["caps"] = int(style.get("caps", 0)) + 1
        if any(marker in msg.text.lower() for marker in profanity_markers):
            style["profanity"] = int(style.get("profanity", 0)) + 1

        quotes = p.setdefault("quotes", [])
        if msg.text not in quotes:
            quotes.append(msg.text)
            if len(quotes) > limits["max_quotes_per_user"]:
                del quotes[:-limits["max_quotes_per_user"]]

        kws = extract_keywords(msg.text, rus_stopwords=rus_stopwords, en_stopwords=en_stopwords, limit=6)
        for token in kws:
            token_counter[token] = token_counter.get(token, 0) + 1
        kw_counter = p.setdefault("keywords", {})
        for token in kws:
            kw_counter[token] = float(kw_counter.get(token, 0.0)) + 1.0
        p["keywords"] = _prune_counter(kw_counter, limits["max_keywords_per_user"])

        arch_counter = p.setdefault("archetypes", {})
        for name, score in detect_archetype_scores(
            msg.text,
            kws,
            archetype_lexicon=archetypes,
            rus_stopwords=rus_stopwords,
            en_stopwords=en_stopwords,
        ).items():
            if score > 0:
                arch_counter[name] = float(arch_counter.get(name, 0.0)) + float(score)

        _update_graph(chat_mem, user_id, msg.text, kws, limits)

        # Build layered memory for runtime context.
        recent_messages = layers.setdefault("recent_messages", [])
        recent_messages.append(
            {
                "user_id": user_id,
                "name": msg.from_name,
                "username": user.get("username", ""),
                "text": msg.text,
                "ts": msg.ts,
                "message_id": msg.message_id,
            }
        )

        fact_text = f"{msg.from_name}: {msg.text}"
        msg_dt = _parse_iso(msg.ts)
        cutoff = now_utc - timedelta(days=recent_days)
        if msg_dt and msg_dt >= cutoff:
            layers.setdefault("recent_facts", []).append(
                {
                    "text": fact_text,
                    "ts": msg.ts,
                    "weight": 1.0,
                }
            )
        else:
            _upsert_long_fact(
                layers,
                text=fact_text,
                last_seen_ts=msg.ts,
                boost=1.0,
            )

    history.sort(key=lambda r: (str(r.get("ts", "")), int(r.get("message_id") or -1)))
    log.sort(key=lambda r: (str(r.get("ts", "")), int(r.get("message_id") or -1)))

    chat_mem["history"] = _prune_records(history, limits["max_history_per_chat"])
    chat_mem["log"] = _prune_records(log, min(limits["max_log_per_chat"], max_recent_messages))
    if len(imported_keys) > 30000:
        del imported_keys[:-30000]
    _compact_layers(
        chat_mem,
        recent_days=recent_days,
        max_recent_messages=max_recent_messages,
        max_recent_facts=max_recent_facts,
        max_long_facts=max_long_facts,
        now_utc=now_utc,
    )

    top_keywords = ", ".join(k for k, _ in _top_items(token_counter, n=8)) if token_counter else "нет"
    layers.setdefault("summary", {})
    layers["summary"]["chat"] = (
        f"импорт: {imported} новых сообщений; участников: {len(by_user_count)}; "
        f"топ-темы: {top_keywords}"
    )
    layers["summary"]["updated_at"] = now_utc.isoformat()
    _build_joke_bank_from_messages(
        chat_mem,
        parsed.messages,
        rus_stopwords=rus_stopwords,
        en_stopwords=en_stopwords,
        profanity_markers=profanity_markers,
    )

    if apply_style_profile:
        profile = _build_style_profile(parsed.messages, rus_stopwords, en_stopwords, profanity_markers)
        cfg = memory.setdefault("config", {})
        cfg["style_settings"] = _merge_style_settings(str(cfg.get("style_settings") or ""), profile)

    top_participants = []
    for uid, cnt in sorted(by_user_count.items(), key=lambda x: (-x[1], x[0]))[:10]:
        name = memory.get("users", {}).get(str(uid), {}).get("name") or str(uid)
        top_participants.append({"user_id": uid, "name": name, "messages": cnt})

    after_samples_total = sum(len(ensure_user_schema(u).get("samples", [])) for u in memory.setdefault("users", {}).values())
    after_layers = chat_mem.get("memory_layers", {})

    return {
        "imported": imported,
        "deduped": deduped,
        "imported_records": imported_records,
        "before_history": before_history,
        "after_history": len(chat_mem.get("history", [])),
        "before_samples_total": before_samples_total,
        "after_samples_total": after_samples_total,
        "before_recent_messages": before_recent_messages,
        "after_recent_messages": len(after_layers.get("recent_messages", [])),
        "before_recent_facts": before_recent_facts,
        "after_recent_facts": len(after_layers.get("recent_facts", [])),
        "before_long_facts": before_long_facts,
        "after_long_facts": len(after_layers.get("long_facts", [])),
        "top_participants": top_participants,
        "warnings": warnings,
    }


def _build_report(parsed: ParsedResult, result: Dict[str, Any], dry_run: bool) -> str:
    lines = [
        f"mode: {'dry-run' if dry_run else 'apply'}",
        f"parsed messages: {len(parsed.messages)}",
        f"skipped service: {parsed.skipped_service}",
        f"skipped empty: {parsed.skipped_empty}",
        f"imported: {result['imported']}",
        f"deduped: {result['deduped']}",
        f"history size: {result['before_history']} -> {result['after_history']}",
        f"samples total: {result['before_samples_total']} -> {result['after_samples_total']}",
        f"recent_messages: {result['before_recent_messages']} -> {result['after_recent_messages']}",
        f"recent_facts: {result['before_recent_facts']} -> {result['after_recent_facts']}",
        f"long_facts: {result['before_long_facts']} -> {result['after_long_facts']}",
        "top participants:",
    ]

    for item in result["top_participants"]:
        lines.append(f"- {item['name']} ({item['user_id']}): {item['messages']}")

    warnings = result.get("warnings") or []
    if warnings:
        lines.append("warnings:")
        for w in warnings:
            lines.append(f"- {w}")

    return "\n".join(lines)


def _backup_memory(memory_path: Path) -> Path:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = memory_path.with_name(f"memory.backup.{ts}.json")
    backup_path.write_text(memory_path.read_text(encoding="utf-8"), encoding="utf-8")
    return backup_path


def _append_archive_jsonl(archive_path: Path, records: List[Dict[str, Any]]) -> int:
    if not records:
        return 0
    archive_path.parent.mkdir(parents=True, exist_ok=True)
    with open(archive_path, "a", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    return len(records)


def compact_existing_layers(
    memory: Dict[str, Any],
    *,
    chat_id: int,
    recent_days: int,
    max_recent_messages: int,
    max_recent_facts: int,
    max_long_facts: int,
) -> Dict[str, int]:
    chat_mem = ensure_chat_schema(memory.setdefault("chats", {}).setdefault(str(chat_id), {}))
    _backfill_layers_from_records(
        chat_mem,
        records=chat_mem.get("log", []) or chat_mem.get("history", []),
        recent_days=recent_days,
        max_recent_messages=max_recent_messages,
        max_recent_facts=max_recent_facts,
        max_long_facts=max_long_facts,
        now_utc=datetime.utcnow(),
    )
    layers = chat_mem.setdefault("memory_layers", {})
    before = {
        "recent_messages": len(layers.get("recent_messages", [])),
        "recent_facts": len(layers.get("recent_facts", [])),
        "long_facts": len(layers.get("long_facts", [])),
    }
    _compact_layers(
        chat_mem,
        recent_days=recent_days,
        max_recent_messages=max_recent_messages,
        max_recent_facts=max_recent_facts,
        max_long_facts=max_long_facts,
        now_utc=datetime.utcnow(),
    )
    chat_mem["log"] = _prune_records(chat_mem.get("log", []), max_recent_messages)
    after_layers = chat_mem.get("memory_layers", {})
    after = {
        "recent_messages": len(after_layers.get("recent_messages", [])),
        "recent_facts": len(after_layers.get("recent_facts", [])),
        "long_facts": len(after_layers.get("long_facts", [])),
    }
    return {**{f"before_{k}": v for k, v in before.items()}, **{f"after_{k}": v for k, v in after.items()}}


def main() -> int:
    parser = argparse.ArgumentParser(description="Import Telegram HTML export into bot memory.json")
    parser.add_argument("--src", help="Path to Telegram export directory")
    parser.add_argument("--chat-id", type=int, required=True, help="Target chat_id in memory.json")
    parser.add_argument(
        "--mode",
        choices=["merge", "replace"],
        default="merge",
        help="Import mode: merge (default) or replace target chat state",
    )
    parser.add_argument("--dry-run", action="store_true", help="Parse and report only, do not write")
    parser.add_argument("--recent-days", type=int, default=14, help="Recent memory horizon in days")
    parser.add_argument("--max-recent-messages", type=int, default=24, help="Max recent messages in memory layers")
    parser.add_argument("--max-recent-facts", type=int, default=120, help="Max recent facts in memory layers")
    parser.add_argument("--max-long-facts", type=int, default=400, help="Max long facts in memory layers")
    parser.add_argument("--archive-path", help="Optional path to append imported raw records as jsonl")
    parser.add_argument("--compact-only", action="store_true", help="Compact existing layers without parsing export")
    parser.add_argument("--no-raw-log", action="store_true", default=True, help="Keep raw imported Telegram text out of memory.log (default)")
    parser.add_argument("--keep-raw-log", action="store_true", help="Keep imported Telegram text in memory.log")
    parser.add_argument(
        "--apply-style-profile",
        action="store_true",
        help="Generate chat style profile and save into config.style_settings",
    )
    parser.add_argument(
        "--memory-path",
        default="memory.json",
        help="Path to memory.json (default: ./memory.json)",
    )

    args = parser.parse_args()

    memory_path = Path(args.memory_path).expanduser().resolve()
    repo_root = Path(__file__).resolve().parents[2]

    rus_stopwords, en_stopwords, profanity_markers, archetypes = _load_lexicon(repo_root)
    limits = _load_runtime_limits(repo_root)
    memory = load_memory(memory_path)

    if args.compact_only:
        compact_stats = compact_existing_layers(
            memory,
            chat_id=args.chat_id,
            recent_days=max(1, int(args.recent_days)),
            max_recent_messages=max(1, int(args.max_recent_messages)),
            max_recent_facts=max(1, int(args.max_recent_facts)),
            max_long_facts=max(1, int(args.max_long_facts)),
        )
        print(
            "compact-only:\n"
            f"- recent_messages: {compact_stats['before_recent_messages']} -> {compact_stats['after_recent_messages']}\n"
            f"- recent_facts: {compact_stats['before_recent_facts']} -> {compact_stats['after_recent_facts']}\n"
            f"- long_facts: {compact_stats['before_long_facts']} -> {compact_stats['after_long_facts']}"
        )
        if args.dry_run:
            return 0
        if memory_path.exists():
            backup_path = _backup_memory(memory_path)
            print(f"backup created: {backup_path}")
        save_memory(memory_path, memory)
        print(f"memory updated: {memory_path}")
        return 0

    if not args.src:
        raise SystemExit("--src is required unless --compact-only is set")

    src = Path(args.src).expanduser().resolve()
    if not src.exists() or not src.is_dir():
        raise SystemExit(f"src directory not found: {src}")

    parsed = parse_export_dir(src)

    result = import_messages(
        memory,
        parsed,
        chat_id=args.chat_id,
        mode=args.mode,
        limits=limits,
        rus_stopwords=rus_stopwords,
        en_stopwords=en_stopwords,
        profanity_markers=profanity_markers,
        archetypes=archetypes,
        apply_style_profile=bool(args.apply_style_profile),
        recent_days=max(1, int(args.recent_days)),
        max_recent_messages=max(1, int(args.max_recent_messages)),
        max_recent_facts=max(1, int(args.max_recent_facts)),
        max_long_facts=max(1, int(args.max_long_facts)),
        keep_raw_log=bool(args.keep_raw_log),
    )

    print(_build_report(parsed, result, args.dry_run))

    if args.dry_run:
        return 0

    if memory_path.exists():
        backup_path = _backup_memory(memory_path)
        print(f"backup created: {backup_path}")

    archived = 0
    if args.archive_path:
        archive_path = Path(args.archive_path).expanduser().resolve()
        archived = _append_archive_jsonl(archive_path, result.get("imported_records", []))
        print(f"archive appended: {archive_path} ({archived} records)")

    save_memory(memory_path, memory)
    print(f"memory updated: {memory_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
