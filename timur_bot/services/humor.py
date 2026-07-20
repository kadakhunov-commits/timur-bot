"""Canonical humor memory, feedback and retrieval for Timur v2."""

from __future__ import annotations

import re
from datetime import datetime, timedelta
from hashlib import sha1
from typing import Any, Dict, Iterable, List, Optional, Sequence


FUNNY_REACTIONS = {"❤", "❤️"}
FUNNY_TEXT = {"лол", "lol", "ахах", "ахаха", "хаха", "пхаха", "ору", "ор"}
UNFUNNY_TEXT = {"несмешно", "не смешно", "кринж", "хуйня", "не смешной", "туп", "тупо"}

HUMOR_SCHEMA_VERSION = 2
MAX_HUMOR_SCENES = 400
MAX_DECISIONS = 200
CALLBACK_COOLDOWN_DAYS = 14
CALLBACK_COOLDOWN_MESSAGES = 100
CALLBACK_WINDOW = 20
CALLBACK_MAX_IN_WINDOW = 2
POSITIVE_SCENE_SIGNALS = {"heart", "direct_laugh", "adjacent_laugh"}
DAILY_SIGNATURE = "у вас как с этим обычно"

_SEMANTIC_STOPWORDS = {
    "это", "как", "что", "для", "тебе", "меня", "тебя", "его", "она", "они", "уже", "еще", "ещё",
    "просто", "только", "когда", "потом", "если", "или", "так", "там", "тут", "вот", "ну", "да", "нет",
}
_PARTICIPANT_NON_NAME_TOKENS = {
    "чел", "челик", "бро", "брат", "братан", "друг", "друган", "админ", "модер", "бот",
    "тот", "этот", "эта", "ваш", "наш",
}
_LEGACY_CONTAMINATION_RE = re.compile(
    r"\bмит(?:я|ю|и|е|ей)\b|\bкадыр(?:а|у|ом|е|ы|и)?\b|"
    r"(?:удал\w*|сн[её]с\w*|ст[её]р\w*)[^\n]{0,40}сообщ|"
    r"сообщ\w*[^\n]{0,40}(?:удал\w*|сн[её]с\w*|ст[её]р\w*)",
    re.I,
)
_DELETED_SCENE_RE = re.compile(
    r"(?:удал\w*|сн[её]с\w*|ст[её]р\w*)[^\n]{0,40}сообщ|"
    r"сообщ\w*[^\n]{0,40}(?:удал\w*|сн[её]с\w*|ст[её]р\w*)",
    re.I,
)
_ROAST_TRIGGER_PATTERNS = (
    re.compile(r"поджар", re.I),
    re.compile(r"прожар", re.I),
    re.compile(r"обосри", re.I),
    re.compile(r"раз[ъь]?еб", re.I),
    re.compile(r"уничтож", re.I),
)


def _now_iso() -> str:
    return datetime.utcnow().isoformat()


def ensure_daily_signature(text: str) -> str:
    """Return one exact daily-lore signature after the story body."""
    clean = str(text or "").strip()
    while clean.lower().replace("ё", "е").endswith(DAILY_SIGNATURE):
        clean = clean[: -len(DAILY_SIGNATURE)].rstrip()
    return f"{clean}\n{DAILY_SIGNATURE}" if clean else DAILY_SIGNATURE


def _clean(value: Any, *, limit: int = 280) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()[:limit]


def _parse_ts(value: Any) -> datetime | None:
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).replace(tzinfo=None)
    except (TypeError, ValueError):
        return None


def _tokens(text: str) -> set[str]:
    return {
        token.lower().replace("ё", "е")
        for token in re.findall(r"[a-zA-Zа-яА-ЯёЁ0-9]{3,}", text or "")
        if token.lower().replace("ё", "е") not in _SEMANTIC_STOPWORDS
    }


def make_bit_id(text: str) -> str:
    return sha1(_clean(text, limit=2000).lower().encode("utf-8")).hexdigest()[:12]


def _scene_id(message_id: int, text: str, source: str) -> str:
    return sha1(f"{int(message_id)}|{_clean(text, limit=2000).lower()}|{source}".encode("utf-8")).hexdigest()[:16]


def _legacy_payload_text(example: Dict[str, Any]) -> str:
    return " ".join(
        [
            str(example.get("good_reply", "")),
            *(str(item.get("text", "")) for item in example.get("context", []) if isinstance(item, dict)),
        ]
    )


def _legacy_confirmed(example: Dict[str, Any]) -> bool:
    signals = {str(item) for item in example.get("signals", [])}
    return bool(signals & POSITIVE_SCENE_SIGNALS) and not bool(example.get("provisional"))


def _legacy_example_to_scene(example: Dict[str, Any]) -> Dict[str, Any]:
    message_id = int(example.get("source_message_id", 0) or 0)
    text = _clean(example.get("good_reply"), limit=280)
    signals = [str(item)[:60] for item in example.get("signals", []) if str(item) in POSITIVE_SCENE_SIGNALS]
    return {
        "id": _scene_id(message_id, text, "legacy_confirmed"),
        "schema_version": HUMOR_SCHEMA_VERSION,
        "source": "human_observed",
        "output_kind": "human",
        "output_message_id": message_id,
        "trigger_message_id": 0,
        "context": list(example.get("context", []))[-8:],
        "selected_text": text,
        "action": "OBSERVED",
        "scene_type": "",
        "relation": "",
        "setup": "",
        "mechanism": _clean(example.get("mechanism"), limit=80),
        "candidates": [],
        "callback_keys": [],
        "feedback": [
            {"rating": "funny", "source": signal, "user_id": None, "ts": _now_iso()}
            for signal in signals
        ],
        "human_message_index": 0,
        "created_ts": str(example.get("created_ts") or _now_iso()),
        "latency_ms": 0,
        "token_usage": 0,
    }


def _legacy_output_to_scene(chat_mem: Dict[str, Any], output: Dict[str, Any]) -> Dict[str, Any] | None:
    text = _clean(output.get("text"), limit=280)
    if not text:
        return None
    feedback: List[Dict[str, Any]] = []
    for item in output.get("feedback", []):
        if not isinstance(item, dict) or item.get("rating") != "funny":
            continue
        source = str(item.get("source", ""))
        # v1 used "reaction" for the only positive emoji (a heart) and
        # "reply_text" for an explicit textual laugh.
        mapped_source = "heart" if source in {"reaction", "heart"} else "direct_laugh"
        feedback.append(
            {
                "rating": "funny",
                "source": mapped_source,
                "user_id": item.get("user_id"),
                "ts": str(item.get("ts") or _now_iso()),
            }
        )
    if not feedback:
        return None

    message_id = int(output.get("message_id", 0) or 0)
    context = [
        {key: row.get(key) for key in ("user_id", "name", "username", "text", "ts", "message_id", "reply_to_message_id")}
        for row in chat_mem.get("history", [])
        if isinstance(row, dict)
        and str(row.get("text", "")).strip()
        and (message_id <= 0 or int(row.get("message_id", 0) or 0) < message_id)
    ][-8:]
    payload = text + " " + " ".join(str(row.get("text", "")) for row in context)
    if _LEGACY_CONTAMINATION_RE.search(payload):
        return None
    setup = _clean(" ".join(str(row.get("text", "")) for row in context[-3:]), limit=240)
    return {
        "id": _scene_id(message_id, text, "legacy_bot_confirmed"),
        "schema_version": HUMOR_SCHEMA_VERSION,
        "source": "bot",
        "output_kind": "legacy",
        "output_message_id": message_id,
        "trigger_message_id": int(context[-1].get("message_id", 0) or 0) if context else 0,
        "context": context,
        "selected_text": text,
        "action": "JOKE",
        "scene_type": _infer_scene_type(str(context[-1].get("text", "")) if context else ""),
        "relation": "legacy_chat",
        "setup": setup,
        "mechanism": _clean(output.get("mode"), limit=80),
        "candidates": [],
        "callback_keys": [],
        "feedback": feedback,
        "human_message_index": 0,
        "created_ts": str(output.get("ts") or _now_iso()),
        "latency_ms": 0,
        "token_usage": 0,
        "length_chars": len(text),
        "irrelevant_name_count": 0,
    }


def ensure_humor_schema(chat_mem: Dict[str, Any]) -> Dict[str, Any]:
    """Install v2 and move every v1 owner into a reversible quarantine."""
    layers = chat_mem.setdefault("memory_layers", {})
    version = int(layers.get("humor_schema_version", 0) or 0)
    if version < HUMOR_SCHEMA_VERSION:
        legacy = layers.setdefault("legacy_humor_v1", {})
        legacy_examples = list(layers.get("funny_examples", []))
        legacy_outputs = list(layers.get("bot_outputs", []))
        for key in ("joke_bank", "funny_examples", "bot_outputs"):
            # Persistent user data is quarantined losslessly even if a partial
            # legacy container was created by an earlier build.
            archived = legacy.setdefault(key, [])
            archived.extend(list(layers.get(key, [])))
            layers[key] = []
        for key in ("overused_bits", "humor_stats"):
            value = layers.get(key, {})
            active_map = dict(value) if isinstance(value, dict) else {}
            if key not in legacy:
                legacy[key] = active_map
            elif active_map:
                # Keep both snapshots when an earlier partial migration already
                # populated the canonical legacy key; merging nested counters
                # would destroy their original meaning.
                legacy.setdefault(f"{key}_additional_snapshots", []).append(active_map)
            layers[key] = {}

        scenes = layers.setdefault("humor_scenes_v2", [])
        migrated = 0
        bot_outputs_migrated = 0
        contaminated = 0
        for example in legacy_examples:
            if not isinstance(example, dict) or not _legacy_confirmed(example):
                continue
            if _LEGACY_CONTAMINATION_RE.search(_legacy_payload_text(example)):
                contaminated += 1
                continue
            scene = _legacy_example_to_scene(example)
            if not any(item.get("id") == scene["id"] for item in scenes if isinstance(item, dict)):
                scenes.append(scene)
                migrated += 1
        for output in legacy_outputs:
            if not isinstance(output, dict):
                continue
            scene = _legacy_output_to_scene(chat_mem, output)
            if scene is None:
                if any(
                    isinstance(item, dict) and item.get("rating") == "funny"
                    for item in output.get("feedback", [])
                ):
                    contaminated += 1
                continue
            if not any(item.get("id") == scene["id"] for item in scenes if isinstance(item, dict)):
                scenes.append(scene)
                bot_outputs_migrated += 1
        legacy["migration"] = {
            "migrated_at": _now_iso(),
            "confirmed_scenes_migrated": migrated,
            "confirmed_bot_outputs_migrated": bot_outputs_migrated,
            "confirmed_contaminated_quarantined": contaminated,
            "bot_output_migration_revision": 1,
            "reversible": True,
        }
        layers["humor_schema_version"] = HUMOR_SCHEMA_VERSION

    legacy = layers.setdefault("legacy_humor_v1", {})
    scenes = layers.setdefault("humor_scenes_v2", [])
    migration = legacy.setdefault("migration", {})
    if int(migration.get("bot_output_migration_revision", 0) or 0) < 1:
        recovered = 0
        for output in legacy.get("bot_outputs", []):
            if not isinstance(output, dict):
                continue
            scene = _legacy_output_to_scene(chat_mem, output)
            if scene and not any(item.get("id") == scene["id"] for item in scenes if isinstance(item, dict)):
                scenes.append(scene)
                recovered += 1
        migration["confirmed_bot_outputs_migrated"] = int(
            migration.get("confirmed_bot_outputs_migrated", 0) or 0
        ) + recovered
        migration["bot_output_migration_revision"] = 1
        migration.setdefault("reversible", True)
    layers.setdefault("humor_stats_v2", {"mechanisms": {}, "sent": 0, "hearts": 0, "laughs": 0})
    layers.setdefault("humor_decisions_v2", [])
    layers.setdefault("humor_daily_usage_v2", {})
    layers.setdefault("joke_bank", [])
    layers.setdefault("funny_examples", [])
    layers.setdefault("bot_outputs", [])
    return layers


def _legacy_list(chat_mem: Dict[str, Any], key: str) -> List[Dict[str, Any]]:
    legacy = ensure_humor_schema(chat_mem).setdefault("legacy_humor_v1", {})
    return legacy.setdefault(key, [])


def add_joke_bit(
    chat_mem: Dict[str, Any],
    text: str,
    *,
    source: str = "manual",
    tags: Optional[List[str]] = None,
    weight: float = 1.0,
) -> Dict[str, Any]:
    """Compatibility import: v1 bits are quarantined and never retrieved."""
    clean = _clean(text, limit=280)
    if not clean:
        raise ValueError("empty joke bit")
    bit_id = make_bit_id(clean)
    bank = _legacy_list(chat_mem, "joke_bank")
    for bit in bank:
        if bit.get("id") == bit_id:
            bit["weight"] = float(bit.get("weight", 1.0)) + float(weight)
            bit["last_seen_ts"] = _now_iso()
            return bit
    bit = {
        "id": bit_id,
        "text": clean,
        "source": source,
        "tags": list(tags or []),
        "weight": float(weight),
        "uses": 0,
        "funny": 0,
        "unfunny": 0,
        "quarantined": True,
        "last_seen_ts": _now_iso(),
    }
    bank.append(bit)
    return bit


def add_funny_example(
    chat_mem: Dict[str, Any],
    *,
    context: List[Dict[str, str]],
    good_reply: str,
    tags: Optional[List[str]] = None,
    source: str = "curated",
    weight: float = 2.0,
    after_context: Optional[List[Dict[str, str]]] = None,
    mechanism: str = "",
    signals: Optional[List[str]] = None,
    confidence: float = 0.0,
    source_message_id: int = 0,
    provisional: bool = False,
) -> Optional[Dict[str, Any]]:
    """Compatibility import: unverified curated examples stay quarantined."""
    clean_reply = _clean(good_reply, limit=280)
    if not clean_reply:
        return None
    clean_context = [
        {"author": _clean(item.get("author"), limit=80), "text": _clean(item.get("text"), limit=220)}
        for item in context[-8:]
        if _clean(item.get("text"), limit=220)
    ]
    example_id = make_bit_id(clean_reply + "|" + "|".join(item["text"] for item in clean_context))
    examples = _legacy_list(chat_mem, "funny_examples")
    for example in examples:
        if example.get("id") == example_id:
            example["weight"] = float(example.get("weight", 1.0)) + float(weight)
            return example
    example = {
        "id": example_id,
        "context": clean_context,
        "good_reply": clean_reply,
        "tags": list(tags or []),
        "source": source,
        "weight": float(weight),
        "after_context": list(after_context or [])[:4],
        "mechanism": _clean(mechanism, limit=80),
        "signals": list(signals or [])[:12],
        "confidence": max(0.0, min(1.0, float(confidence))),
        "source_message_id": int(source_message_id or 0),
        "provisional": bool(provisional),
        "quarantined": True,
        "created_ts": _now_iso(),
    }
    examples.append(example)
    return example


def classify_text_feedback(text: str) -> Optional[str]:
    clean = _clean(text, limit=80).lower()
    compact = re.sub(r"[^a-zа-яё]+", "", clean, flags=re.I)
    funny_compact = {re.sub(r"[^a-zа-яё]+", "", item, flags=re.I) for item in FUNNY_TEXT}
    repeated_laugh = bool(
        re.fullmatch(r"(?:ах){2,}а?", compact)
        or re.fullmatch(r"п?(?:ха){2,}", compact)
        or re.fullmatch(r"ло+л+", compact)
        or re.fullmatch(r"ору+", compact)
    )
    if compact in funny_compact or repeated_laugh:
        return "funny"
    if clean in UNFUNNY_TEXT or compact in {re.sub(r"\s+", "", item) for item in UNFUNNY_TEXT}:
        return "unfunny"
    return None


def classify_reactions(reactions: Iterable[Any]) -> Optional[str]:
    """Only a heart is a positive signal; every other reaction is neutral."""
    hearts = {item.replace("\ufe0f", "") for item in FUNNY_REACTIONS}
    for reaction in reactions or []:
        emoji = reaction.get("emoji") if isinstance(reaction, dict) else getattr(reaction, "emoji", None)
        if str(emoji or "").replace("\ufe0f", "") in hearts:
            return "funny"
    return None


def infer_scene_mechanism(context: Iterable[Dict[str, Any]], punchline: str) -> str:
    before = " ".join(str(item.get("text", "")) for item in context).lower()
    clean = _clean(punchline, limit=280).lower()
    if any(token in clean for token in ("как будто", "будто", "официально")):
        return "status_shift"
    if len(clean) <= 45 and _tokens(clean) & _tokens(before):
        return "logical_continuation"
    if len(clean) <= 60:
        return "understatement"
    return "contextual_turn"


def _infer_scene_type(text: str) -> str:
    clean = _clean(text, limit=280).lower()
    if clean in {"резонно", "пон", "понял", "ок", "ясно", "согласен"}:
        return "short_ack"
    if any(marker in clean for marker in ("мне плохо", "умер", "болею", "страшно", "помоги")):
        return "serious"
    if "?" in clean:
        return "question"
    return "banter"


def infer_scene_type(text: str) -> str:
    return _infer_scene_type(text)


def snapshot_scene_context(
    chat_mem: Dict[str, Any],
    *,
    trigger_message_id: int = 0,
    limit: int = 8,
    reply_depth: int = 3,
) -> List[Dict[str, Any]]:
    history = [row for row in chat_mem.get("history", []) if isinstance(row, dict) and _clean(row.get("text"), limit=220)]
    if not history:
        return []
    by_id = {int(row.get("message_id", 0) or 0): idx for idx, row in enumerate(history)}
    trigger_idx = by_id.get(int(trigger_message_id or 0), len(history) - 1)
    bounded_limit = max(1, int(limit))
    required = {trigger_idx}
    cursor = trigger_idx
    for _ in range(max(0, reply_depth)):
        if cursor < 0 or cursor >= len(history):
            break
        reply_id = int(history[cursor].get("reply_to_message_id", 0) or 0)
        if not reply_id or reply_id not in by_id:
            break
        cursor = by_id[reply_id]
        required.add(cursor)
    selected = set(required)
    for idx in range(len(history) - 1, -1, -1):
        if len(selected) >= bounded_limit:
            break
        selected.add(idx)
    rows = [history[idx] for idx in sorted(selected)[:bounded_limit]]
    keys = (
        "user_id", "name", "username", "text", "ts", "is_bot", "message_id", "reply_to_message_id",
        "is_forward", "forward_origin_chat_id", "forward_origin_chat_title", "forward_origin_message_id",
    )
    return [{key: row.get(key) for key in keys if row.get(key) not in (None, "", 0, False)} for row in rows]


def _human_message_index(chat_mem: Dict[str, Any]) -> int:
    state = chat_mem.setdefault("memory_layers", {}).setdefault("adaptive_humor", {})
    return int(state.get("human_messages_total", 0) or 0)


def _append_scene(chat_mem: Dict[str, Any], scene: Dict[str, Any]) -> Dict[str, Any]:
    scenes = ensure_humor_schema(chat_mem).setdefault("humor_scenes_v2", [])
    for existing in scenes:
        if existing.get("id") == scene.get("id"):
            return existing
    scenes.append(scene)
    del scenes[:-MAX_HUMOR_SCENES]
    return scene


def _append_bot_timeline(
    chat_mem: Dict[str, Any],
    *,
    message_id: int,
    text: str,
    reply_to_message_id: int = 0,
) -> None:
    if any(int(row.get("message_id", 0) or 0) == int(message_id) for row in chat_mem.get("history", [])):
        return
    row = {
        "user_id": 0,
        "name": "тимур",
        "username": "",
        "text": _clean(text, limit=1000),
        "ts": _now_iso(),
        "is_bot": True,
        "message_id": int(message_id),
        "reply_to_message_id": int(reply_to_message_id or 0),
    }
    history = chat_mem.setdefault("history", [])
    history.append(row)
    del history[:-320]
    log = chat_mem.setdefault("log", [])
    log.append(dict(row))
    del log[:-800]


def record_bot_output(
    chat_mem: Dict[str, Any],
    *,
    message_id: int,
    text: str,
    plan: Optional[Dict[str, Any]],
    output_kind: str = "direct",
    trigger_message_id: int = 0,
    reply_to_message_id: int = 0,
) -> Optional[Dict[str, Any]]:
    layers = ensure_humor_schema(chat_mem)
    existing = find_humor_scene(chat_mem, message_id)
    if existing:
        return existing
    bounded_context = list(
        (plan or {}).get("context")
        or snapshot_scene_context(chat_mem, trigger_message_id=trigger_message_id)
    )[-8:]
    _append_bot_timeline(
        chat_mem,
        message_id=message_id,
        text=text,
        reply_to_message_id=reply_to_message_id or trigger_message_id,
    )
    if output_kind == "daily_lore":
        return None
    plan = plan or {}
    scene = {
        "id": _scene_id(message_id, text, "bot"),
        "schema_version": HUMOR_SCHEMA_VERSION,
        "source": "bot",
        "output_kind": output_kind,
        "output_message_id": int(message_id),
        "trigger_message_id": int(trigger_message_id or plan.get("trigger_message_id", 0) or 0),
        "context": bounded_context,
        "selected_text": _clean(text, limit=280),
        "action": str(plan.get("action") or ("JOKE" if output_kind == "ambient" else "ANSWER")),
        "scene_type": _clean(plan.get("scene_type"), limit=80),
        "relation": _clean(plan.get("relation"), limit=80),
        "setup": _clean(plan.get("setup"), limit=240),
        "mechanism": _clean(plan.get("mechanism"), limit=80),
        "candidates": list(plan.get("candidates") or [])[:4],
        "callback_keys": [str(item)[:80] for item in plan.get("callback_keys", []) if str(item).strip()][:4],
        "feedback": [],
        "human_message_index": _human_message_index(chat_mem),
        "created_ts": _now_iso(),
        "latency_ms": max(0, int(plan.get("latency_ms", 0) or 0)),
        "token_usage": max(0, int(plan.get("token_usage", 0) or 0)),
        "length_chars": len(_clean(text, limit=280)),
        "irrelevant_name_count": max(0, int(plan.get("irrelevant_name_count", 0) or 0)),
    }
    layers.setdefault("humor_stats_v2", {}).setdefault("sent", 0)
    layers["humor_stats_v2"]["sent"] += 1
    return _append_scene(chat_mem, scene)


def find_humor_scene(chat_mem: Dict[str, Any], message_id: int) -> Optional[Dict[str, Any]]:
    scenes = ensure_humor_schema(chat_mem).get("humor_scenes_v2", [])
    return next(
        (scene for scene in reversed(scenes) if int(scene.get("output_message_id", -1)) == int(message_id)),
        None,
    )


def _rebuild_mechanism_stats(chat_mem: Dict[str, Any]) -> None:
    layers = ensure_humor_schema(chat_mem)
    mechanisms: Dict[str, Dict[str, int]] = {}
    hearts = 0
    laughs = 0
    for scene in layers.get("humor_scenes_v2", []):
        key = f"{scene.get('scene_type') or 'unknown'}|{scene.get('mechanism') or 'unknown'}"
        bucket = mechanisms.setdefault(key, {"uses": 0, "funny": 0, "unfunny": 0})
        if scene.get("source") == "bot":
            bucket["uses"] += 1
        ratings = {str(item.get("rating")) for item in scene.get("feedback", []) if isinstance(item, dict)}
        if "funny" in ratings:
            bucket["funny"] += 1
        if "unfunny" in ratings:
            bucket["unfunny"] += 1
        hearts += sum(1 for item in scene.get("feedback", []) if item.get("source") == "heart" and item.get("rating") == "funny")
        laughs += sum(
            1
            for item in scene.get("feedback", [])
            if item.get("source") in {"direct_laugh", "adjacent_laugh", "reply_text"} and item.get("rating") == "funny"
        )
    stats = layers.setdefault("humor_stats_v2", {})
    stats["mechanisms"] = mechanisms
    stats["hearts"] = hearts
    stats["laughs"] = laughs


def _upsert_feedback(
    scene: Dict[str, Any],
    *,
    rating: str,
    source: str,
    user_id: Optional[int],
) -> bool:
    feedback = scene.setdefault("feedback", [])
    identity = (str(source), int(user_id or 0))
    for item in feedback:
        if (str(item.get("source")), int(item.get("user_id", 0) or 0)) == identity:
            if item.get("rating") == rating:
                return False
            item["rating"] = rating
            item["ts"] = _now_iso()
            return True
    feedback.append({"rating": rating, "source": source, "user_id": user_id, "ts": _now_iso()})
    return True


def apply_feedback(
    chat_mem: Dict[str, Any],
    *,
    message_id: int,
    rating: str,
    source: str,
    user_id: Optional[int] = None,
) -> bool:
    if rating not in {"funny", "unfunny"}:
        return False
    scene = find_humor_scene(chat_mem, message_id)
    if not scene or scene.get("output_kind") == "daily_lore":
        return False
    changed = _upsert_feedback(scene, rating=rating, source=source, user_id=user_id)
    if changed:
        _rebuild_mechanism_stats(chat_mem)
    return True


def set_heart_feedback(
    chat_mem: Dict[str, Any],
    *,
    message_id: int,
    user_id: Optional[int],
    active: bool,
) -> bool:
    scene = find_humor_scene(chat_mem, message_id)
    if not scene or scene.get("output_kind") == "daily_lore":
        return False
    feedback = scene.setdefault("feedback", [])
    before = len(feedback)
    feedback[:] = [
        item
        for item in feedback
        if not (item.get("source") == "heart" and int(item.get("user_id", 0) or 0) == int(user_id or 0))
    ]
    changed = len(feedback) != before
    if active:
        feedback.append({"rating": "funny", "source": "heart", "user_id": user_id, "ts": _now_iso()})
        changed = True
    if changed:
        _rebuild_mechanism_stats(chat_mem)
    return changed


def learn_funny_scene(
    chat_mem: Dict[str, Any],
    *,
    context: List[Dict[str, str]],
    punchline: str,
    after_context: List[Dict[str, str]],
    signals: List[str],
    mechanism: str = "",
    confidence: float = 0.0,
    source_message_id: int = 0,
    signal_user_id: Optional[int] = None,
) -> Optional[Dict[str, Any]]:
    del after_context, confidence
    clean = _clean(punchline, limit=280)
    confirmed_signals = [str(signal) for signal in dict.fromkeys(signals) if str(signal) in POSITIVE_SCENE_SIGNALS]
    if not clean or not confirmed_signals:
        return None
    existing = find_humor_scene(chat_mem, source_message_id)
    if existing:
        for signal in confirmed_signals:
            _upsert_feedback(existing, rating="funny", source=str(signal), user_id=signal_user_id)
        _rebuild_mechanism_stats(chat_mem)
        return existing
    context_rows = list(context)[-8:]
    scene = {
        "id": _scene_id(source_message_id, clean, "human"),
        "schema_version": HUMOR_SCHEMA_VERSION,
        "source": "human_observed",
        "output_kind": "human",
        "output_message_id": int(source_message_id or 0),
        "trigger_message_id": 0,
        "context": context_rows,
        "selected_text": clean,
        "action": "OBSERVED",
        "scene_type": _infer_scene_type(" ".join(str(item.get("text", "")) for item in context_rows)),
        "relation": "chat",
        "setup": _clean(" ".join(str(item.get("text", "")) for item in context_rows[-3:]), limit=240),
        "mechanism": _clean(mechanism, limit=80) or infer_scene_mechanism(context_rows, clean),
        "candidates": [],
        "callback_keys": [],
        "feedback": [
            {"rating": "funny", "source": str(signal)[:60], "user_id": signal_user_id, "ts": _now_iso()}
            for signal in confirmed_signals
        ],
        "human_message_index": _human_message_index(chat_mem),
        "created_ts": _now_iso(),
        "latency_ms": 0,
        "token_usage": 0,
    }
    saved = _append_scene(chat_mem, scene)
    _rebuild_mechanism_stats(chat_mem)
    return saved


def _participant_name_tokens(chat_mem: Dict[str, Any]) -> set[str]:
    result: set[str] = set()
    for item in chat_mem.get("participants", {}).values():
        if not isinstance(item, dict):
            continue
        for value in (str(item.get("name", "")), str(item.get("username", ""))):
            for token in _tokens(value):
                if token in _PARTICIPANT_NON_NAME_TOKENS:
                    continue
                result.add(token)
                if token.endswith("я") and len(token) >= 4:
                    stem = token[:-1]
                    result.update(stem + ending for ending in ("я", "ю", "и", "е", "ей", "ем"))
                elif token.endswith("а") and len(token) >= 4:
                    stem = token[:-1]
                    result.update(stem + ending for ending in ("а", "у", "ы", "е", "ой", "ом"))
                elif len(token) >= 4:
                    result.update(token + ending for ending in ("а", "у", "ом", "е", "ы", "и"))
    return result


def select_positive_example(
    chat_mem: Dict[str, Any],
    *,
    scene_type: str,
    relation: str,
    setup: str,
    current_text: str = "",
    mechanisms: Sequence[str] = (),
) -> Optional[Dict[str, Any]]:
    layers = ensure_humor_schema(chat_mem)
    participant_tokens = _participant_name_tokens(chat_mem)
    raw_query_tokens = _tokens(setup) | _tokens(current_text)
    query_tokens = raw_query_tokens - participant_tokens
    query_has_deleted_topic = bool(_DELETED_SCENE_RE.search(f"{setup} {current_text}"))
    ranked: List[tuple[float, Dict[str, Any]]] = []
    for scene in layers.get("humor_scenes_v2", []):
        if scene.get("output_kind") == "daily_lore" or not any(
            item.get("rating") == "funny" for item in scene.get("feedback", []) if isinstance(item, dict)
        ):
            continue
        raw_scene_tokens = (
            _tokens(str(scene.get("setup", "")))
            | _tokens(str(scene.get("selected_text", "")))
            | _tokens(" ".join(str(item.get("text", "")) for item in scene.get("context", []) if isinstance(item, dict)))
        )
        if (raw_scene_tokens & participant_tokens) - (raw_query_tokens & participant_tokens):
            continue
        scene_payload = " ".join(
            [
                str(scene.get("setup", "")),
                str(scene.get("selected_text", "")),
                *(str(item.get("text", "")) for item in scene.get("context", []) if isinstance(item, dict)),
            ]
        )
        if _DELETED_SCENE_RE.search(scene_payload) and not query_has_deleted_topic:
            continue
        scene_tokens = raw_scene_tokens - participant_tokens
        overlap = len(query_tokens & scene_tokens)
        same_type = bool(scene_type and scene_type == scene.get("scene_type"))
        same_relation = bool(relation and relation == scene.get("relation"))
        same_mechanism = bool(scene.get("mechanism") and scene.get("mechanism") in set(mechanisms))
        length_delta = abs(len(_clean(scene.get("selected_text"), limit=280)) - len(_clean(current_text, limit=280)))
        length_close = length_delta <= 20
        if not (
            (overlap >= 1 and (same_type or same_relation or same_mechanism))
            or (same_type and length_close)
        ):
            continue
        positives = sum(1 for item in scene.get("feedback", []) if item.get("rating") == "funny")
        score = (
            overlap * 2.0
            + (3.0 if same_type else 0.0)
            + (2.0 if same_relation else 0.0)
            + (2.0 if same_mechanism else 0.0)
            + (1.0 if length_delta <= 20 else 0.0)
            + min(2, positives)
        )
        ranked.append((score, scene))
    if not ranked:
        return None
    ranked.sort(key=lambda item: (item[0], str(item[1].get("created_ts", ""))), reverse=True)
    return ranked[0][1]


def recent_humor_outputs(chat_mem: Dict[str, Any], *, limit: int = 50) -> List[Dict[str, Any]]:
    scenes = ensure_humor_schema(chat_mem).get("humor_scenes_v2", [])
    return [
        {
            "text": str(scene.get("selected_text", "")),
            "mechanism": str(scene.get("mechanism", "")),
            "callback_keys": list(scene.get("callback_keys", [])),
            "created_ts": scene.get("created_ts"),
        }
        for scene in scenes
        if scene.get("source") == "bot" and scene.get("selected_text")
    ][-max(1, limit) :]


def callback_keys_on_cooldown(chat_mem: Dict[str, Any], *, now: datetime | None = None) -> set[str]:
    current = now or datetime.utcnow()
    human_index = _human_message_index(chat_mem)
    bot_scenes = [
        scene
        for scene in ensure_humor_schema(chat_mem).get("humor_scenes_v2", [])
        if scene.get("source") == "bot" and scene.get("output_kind") == "ambient"
    ]
    blocked: set[str] = set()
    for scene in bot_scenes:
        ts = _parse_ts(scene.get("created_ts"))
        message_gap = human_index - int(scene.get("human_message_index", 0) or 0)
        if (ts and current - ts < timedelta(days=CALLBACK_COOLDOWN_DAYS)) or message_gap < CALLBACK_COOLDOWN_MESSAGES:
            blocked.update(str(item) for item in scene.get("callback_keys", []) if str(item))
    recent = bot_scenes[-CALLBACK_WINDOW:]
    if sum(1 for scene in recent if scene.get("callback_keys")) >= CALLBACK_MAX_IN_WINDOW:
        blocked.add("*")
    return blocked


def record_humor_decision(
    chat_mem: Dict[str, Any],
    *,
    action: str,
    sent: bool,
    reason_codes: Sequence[str] = (),
    token_usage: int = 0,
    latency_ms: int = 0,
    charge_usage: bool = True,
) -> None:
    layers = ensure_humor_schema(chat_mem)
    today = datetime.utcnow().date().isoformat()
    usage = layers.setdefault("humor_daily_usage_v2", {})
    if charge_usage:
        usage[today] = int(usage.get(today, 0) or 0) + max(0, int(token_usage or 0))
    decisions = layers.setdefault("humor_decisions_v2", [])
    decisions.append(
        {
            "ts": _now_iso(),
            "action": str(action),
            "sent": bool(sent),
            "reason_codes": [str(item)[:60] for item in reason_codes][:8],
            "token_usage": max(0, int(token_usage or 0)),
            "latency_ms": max(0, int(latency_ms or 0)),
        }
    )
    del decisions[:-MAX_DECISIONS]


def background_tokens_used_today(chat_mem: Dict[str, Any]) -> int:
    usage = ensure_humor_schema(chat_mem).get("humor_daily_usage_v2", {})
    return int(usage.get(datetime.utcnow().date().isoformat(), 0) or 0)


def background_budget_blocked_today(chat_mem: Dict[str, Any]) -> bool:
    blocked = ensure_humor_schema(chat_mem).get("humor_budget_blocked_days_v2", {})
    return datetime.utcnow().date().isoformat() in blocked


def reserve_background_tokens(chat_mem: Dict[str, Any], amount: int) -> int:
    """Charge a conservative ceiling before an API call so cancellation is safe."""
    reserved = max(0, int(amount or 0))
    layers = ensure_humor_schema(chat_mem)
    today = datetime.utcnow().date().isoformat()
    usage = layers.setdefault("humor_daily_usage_v2", {})
    usage[today] = int(usage.get(today, 0) or 0) + reserved
    return reserved


def settle_background_tokens(chat_mem: Dict[str, Any], *, reserved: int, actual: int) -> int:
    """Refund unused reserve without ever crossing the preflight ceiling."""
    layers = ensure_humor_schema(chat_mem)
    today = datetime.utcnow().date().isoformat()
    usage = layers.setdefault("humor_daily_usage_v2", {})
    current = int(usage.get(today, 0) or 0)
    reserved_tokens = max(0, int(reserved or 0))
    reported_tokens = max(0, int(actual or 0))
    charged_tokens = min(reported_tokens, reserved_tokens)
    if reported_tokens > reserved_tokens:
        anomalies = layers.setdefault("humor_budget_anomalies_v2", [])
        anomalies.append(
            {
                "ts": _now_iso(),
                "reserved": reserved_tokens,
                "reported": reported_tokens,
            }
        )
        del anomalies[:-50]
        layers.setdefault("humor_budget_blocked_days_v2", {})[today] = {
            "ts": _now_iso(),
            "reason": "provider_usage_exceeded_reserve",
        }
    usage[today] = max(0, current - reserved_tokens + charged_tokens)
    return charged_tokens


def _median_int(values: Sequence[int]) -> int:
    ordered = sorted(int(value) for value in values)
    if not ordered:
        return 0
    middle = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[middle]
    return int(round((ordered[middle - 1] + ordered[middle]) / 2))


def humor_metrics(chat_mem: Dict[str, Any]) -> Dict[str, Any]:
    """Acceptance metrics over the latest 100 decisions and 20 sent jokes."""
    layers = ensure_humor_schema(chat_mem)
    decisions = list(layers.get("humor_decisions_v2", []))[-100:]
    sent = [
        scene
        for scene in layers.get("humor_scenes_v2", [])
        if scene.get("source") == "bot" and scene.get("output_kind") == "ambient"
    ][-20:]
    silence_count = sum(1 for item in decisions if str(item.get("action")) == "SILENCE")
    hearted = sum(
        1
        for scene in sent
        if any(
            item.get("source") == "heart" and item.get("rating") == "funny"
            for item in scene.get("feedback", [])
            if isinstance(item, dict)
        )
    )
    return {
        "decision_count": len(decisions),
        "silence_rate": round(silence_count / len(decisions), 4) if decisions else 0.0,
        "sent_sample_count": len(sent),
        "hearted_in_last_20": hearted,
        "median_length_chars": _median_int(
            [int(scene.get("length_chars", len(str(scene.get("selected_text", "")))) or 0) for scene in sent]
        ),
        "median_latency_ms": _median_int([int(item.get("latency_ms", 0) or 0) for item in decisions]),
        "tokens_last_100": sum(int(item.get("token_usage", 0) or 0) for item in decisions),
        "irrelevant_name_count": sum(int(scene.get("irrelevant_name_count", 0) or 0) for scene in sent),
        "ready_for_first_review": len(decisions) >= 100 and len(sent) >= 20,
    }


def choose_humor_plan(
    chat_mem: Dict[str, Any],
    *,
    text: str,
    user_id: int,
    user_name: str,
) -> Dict[str, Any]:
    scene_type = _infer_scene_type(text)
    example = select_positive_example(
        chat_mem,
        scene_type=scene_type,
        relation="direct",
        setup=text,
        current_text=text,
    )
    return {
        "action": "ANSWER",
        "mode": "direct",
        "scene_type": scene_type,
        "relation": "direct",
        "setup": _clean(text, limit=240),
        "target_user_id": int(user_id),
        "target_user_name": user_name,
        "mechanism": "",
        "callback_keys": [],
        "bit_ids": [],
        "examples": [example] if example else [],
        "context": snapshot_scene_context(chat_mem),
    }


def format_humor_prompt(plan: Dict[str, Any]) -> str:
    lines = [
        "режим ответа:",
        "- сначала ответь по смыслу; шутка необязательна",
        "- не вводи людей и факты, которых нет в текущей сцене",
        "- не используй старый callback только ради узнаваемости",
        "- не пиши определения вида «x — это когда» и «а то я думал»",
    ]
    examples = [item for item in plan.get("examples", []) if isinstance(item, dict)]
    if examples:
        example = examples[0]
        lines.append(
            "- похожий положительный пример по механизму, не копировать: "
            + _clean(example.get("selected_text"), limit=80)
        )
    return "\n".join(lines)


def format_bits(chat_mem: Dict[str, Any], limit: int = 10) -> str:
    bank = list(_legacy_list(chat_mem, "joke_bank"))
    if not bank:
        return "legacy bits пусты; v2 учится по сценам с сердцами"
    bank.sort(key=lambda item: (-float(item.get("weight", 0.0)), str(item.get("text", ""))))
    return "\n".join(f"- {item.get('text')} | quarantine" for item in bank[:limit])


def wants_roast(text: str) -> bool:
    clean = _clean(text, limit=280)
    return any(pattern.search(clean) for pattern in _ROAST_TRIGGER_PATTERNS)
