"""Pure helpers for the quality-gated ambient humor pipeline.

The writer never chooses its own work.  It describes the scene and produces
several short options; a separate critic may still prefer silence.
"""

from __future__ import annotations

import json
import re
from difflib import SequenceMatcher
from typing import Any, Dict, Iterable, List, Sequence


MAX_SCENE_MESSAGES = 8
MAX_SCENE_CHARS = 1200
MAX_AMBIENT_REPLY_CHARS = 60
MAX_CANDIDATES = 4

_BAD_TEMPLATE_PATTERNS = (
    re.compile(r"(?:^|[\s—-])(?:это|\S+)\s*[—-]\s*это когда\b", re.I),
    re.compile(r"\bэто когда\b", re.I),
    re.compile(r"\bа то я думал[аи]?\b", re.I),
    re.compile(r"\biq\b.*(?:комнат|табур|холодиль)", re.I),
    re.compile(r"\b(?:нейрон|извилин|интеллект)\w*\b.*(?:плав|умер|ноль|нет)", re.I),
    re.compile(r"\bнейрон\w*\b", re.I),
    re.compile(r"\biq\b\s*[:=—-]?\s*\d{1,2}\b", re.I),
    re.compile(r"\b(?:гений мысли|мозг отсутствует|ума палата|как у нищего)\b", re.I),
    re.compile(r"(?<![a-zа-я0-9])(?:дебил|идиот|клоун|тупой|нищий|кретин)(?![a-zа-я0-9])", re.I),
)
_DELETED_MESSAGES_RE = re.compile(
    r"(?:удал\w*|сн[её]с\w*|ст[её]р\w*)[^\n]{0,35}сообщ|сообщ\w*[^\n]{0,35}(?:удал\w*|сн[её]с\w*|ст[её]р\w*)",
    re.I,
)
_NON_NAME_TOKENS = {
    "он", "она", "они", "оно", "чел", "челик", "бро", "брат", "братан", "друг", "друган",
    "админ", "модер", "бот", "тот", "этот", "эта", "это", "там", "тут", "ваш", "наш",
}


def _clean(value: Any, *, limit: int = 280) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()[:limit]


def _normalized(value: Any) -> str:
    clean = _clean(value, limit=1000).lower().replace("ё", "е")
    return re.sub(r"[^a-zа-я0-9]+", " ", clean, flags=re.I).strip()


def _tokens(value: Any) -> set[str]:
    return {token for token in _normalized(value).split() if len(token) >= 3}


def _json_object(text: str) -> Dict[str, Any]:
    raw = (text or "").strip()
    if not raw or not raw.startswith("{") or not raw.endswith("}"):
        return {}
    try:
        value = json.loads(raw)
    except (TypeError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def render_scene(
    history: Iterable[Dict[str, Any]],
    *,
    limit: int = MAX_SCENE_MESSAGES,
    max_chars: int = MAX_SCENE_CHARS,
) -> str:
    """Render a bounded timeline while retaining reply/forward/bot structure."""
    rows = list(history)[-max(1, int(limit)) :]
    lines: List[str] = []
    for row in rows:
        text = _clean(row.get("text"), limit=220)
        if not text:
            continue
        author = _clean(row.get("name") or row.get("username") or row.get("user_id") or "кто-то", limit=80)
        meta: List[str] = []
        if row.get("message_id") is not None:
            meta.append(f"id={row.get('message_id')}")
        reply_id = row.get("reply_to_message_id") or row.get("reply_to")
        if reply_id:
            meta.append(f"reply={reply_id}")
        if bool(row.get("is_bot")):
            meta.append("bot")
        if any(
            row.get(key)
            for key in (
                "is_forward",
                "forward_from",
                "forward_origin",
                "forward_sender_name",
                "forward_date",
                "forward_origin_chat_id",
                "forward_origin_chat_title",
                "forward_origin_message_id",
            )
        ):
            meta.append("forward")
        prefix = f"[{' '.join(meta)}] " if meta else ""
        lines.append(f"{prefix}{author}: {text}")

    bounded: List[str] = []
    for line in reversed(lines):
        candidate = "\n".join(reversed([line, *bounded]))
        if len(candidate) > max_chars:
            if not bounded:
                bounded = [line[-max_chars:]]
            break
        bounded.insert(0, line)
    return "\n".join(bounded)[-max_chars:]


def director_writer_messages(
    history: Iterable[Dict[str, Any]],
    humor_hint: str = "",
    *,
    blocked_callback_keys: Iterable[str] = (),
    max_chars: int = MAX_AMBIENT_REPLY_CHARS,
) -> List[Dict[str, str]]:
    scene = render_scene(history)
    bounded_chars = max(1, int(max_chars))
    blocked = [_clean(item, limit=80) for item in blocked_callback_keys if _clean(item, limit=80)]
    system = (
        "Ты Director и Writer коротких реплик в живом русском чате друзей. Ты НЕ судья своих вариантов. "
        "Сначала пойми буквальный смысл и социальную динамику сцены. Если незакрытого комического поворота нет, "
        "верни should_attempt=false и пустой candidates. Не считай однословное согласие или техническое уточнение "
        "поводом само по себе. Если повод есть, создай ровно четыре варианта разными механизмами: логическое "
        "продолжение, смена статуса, конкретный образ и сухое преуменьшение. Каждый вариант — одна естественная "
        f"реплика до {bounded_chars} знаков. Не вводи людей и факты, которых нет в сцене; не повторяй исходную фразу; не пиши "
        "словарные определения, 'а то я думал', универсальные оскорбления или объяснение шутки. Callback допустим "
        "только при прямой связи со сценой. Верни только JSON без markdown: "
        '{"should_attempt":true,"setup":"...","target":"...","scene_type":"...","relation":"...",'
        '"forbidden_moves":["..."],"candidates":[{"text":"...","mechanism":"...","callback_key":""}]}. '
        "Никаких score, winner или самооценки."
    )
    user_parts = [f"сцена:\n{scene or '<пусто>'}"]
    if humor_hint.strip():
        user_parts.append(f"вкус чата (не копировать дословно):\n{_clean(humor_hint, limit=500)}")
    if blocked:
        user_parts.append("callbacks на кулдауне: " + ", ".join(blocked[:12]))
    return [{"role": "system", "content": system}, {"role": "user", "content": "\n\n".join(user_parts)}]


def parse_director(text: str) -> Dict[str, Any]:
    payload = _json_object(text)
    result: Dict[str, Any] = {
        "should_attempt": False,
        "setup": "",
        "target": "",
        "scene_type": "",
        "relation": "",
        "forbidden_moves": [],
        "candidates": [],
    }
    if not payload or any(key in payload for key in ("score", "winner", "winner_index")):
        return result
    required_types = {
        "should_attempt": bool,
        "setup": str,
        "target": str,
        "scene_type": str,
        "relation": str,
        "forbidden_moves": list,
        "candidates": list,
    }
    if any(key not in payload or not isinstance(payload[key], expected) for key, expected in required_types.items()):
        return result
    if not all(isinstance(item, str) for item in payload["forbidden_moves"]):
        return result
    result.update(
        {
            "should_attempt": payload.get("should_attempt") is True,
            "setup": _clean(payload.get("setup"), limit=240),
            "target": _clean(payload.get("target"), limit=120),
            "scene_type": _clean(payload.get("scene_type"), limit=80),
            "relation": _clean(payload.get("relation"), limit=80),
        }
    )
    result["forbidden_moves"] = [_clean(item, limit=120) for item in payload["forbidden_moves"] if _clean(item, limit=120)][:8]
    raw_candidates = payload["candidates"]
    if result["should_attempt"] and len(raw_candidates) != MAX_CANDIDATES:
        return {**result, "should_attempt": False, "candidates": []}
    if not result["should_attempt"] and raw_candidates:
        return result
    if isinstance(raw_candidates, list):
        for item in raw_candidates:
            if (
                not isinstance(item, dict)
                or set(item) - {"text", "mechanism", "callback_key"}
                or not all(isinstance(item.get(key), str) for key in ("text", "mechanism", "callback_key"))
            ):
                return {**result, "should_attempt": False, "candidates": []}
            candidate = {
                # Preserve newlines until the hard guard has a chance to reject them.
                "text": str(item.get("text") or "").strip()[:280],
                "mechanism": _clean(item.get("mechanism"), limit=80),
                "callback_key": _clean(item.get("callback_key"), limit=80),
            }
            if candidate["text"]:
                result["candidates"].append(candidate)
    if not result["should_attempt"]:
        result["candidates"] = []
    return result


def text_fingerprint(text: str) -> str:
    return " ".join(sorted(_tokens(text)))


def _near_duplicate(left: str, right: str) -> bool:
    a, b = _normalized(left), _normalized(right)
    if not a or not b:
        return False
    if a == b:
        return True
    a_tokens, b_tokens = _tokens(a), _tokens(b)
    union = a_tokens | b_tokens
    jaccard = (len(a_tokens & b_tokens) / len(union)) if union else 0.0
    return jaccard >= 0.67 or SequenceMatcher(None, a, b).ratio() >= 0.84


def _name_tokens(name: str) -> List[str]:
    return [
        token
        for token in _normalized(name).split()
        if len(token) >= 3 and token not in _NON_NAME_TOKENS
    ]


def _single_name_forms(token: str) -> set[str]:
    forms: set[str] = set()
    forms.add(token)
    if token.endswith("я") and len(token) >= 4:
        stem = token[:-1]
        forms.update(stem + ending for ending in ("я", "ю", "и", "е", "ей", "ем"))
    elif token.endswith("а") and len(token) >= 4:
        stem = token[:-1]
        forms.update(stem + ending for ending in ("а", "у", "ы", "е", "ой", "ом"))
    elif len(token) >= 4:
        forms.update(token + ending for ending in ("а", "у", "ом", "е", "ы", "и"))
    return forms


def _name_forms(name: str) -> set[str]:
    forms: set[str] = set()
    for token in _name_tokens(name):
        forms.update(_single_name_forms(token))
    return forms


def _contains_name(text: str, name: str) -> bool:
    return bool(set(_normalized(text).split()) & _name_forms(name))


def _derived_topic_callback_key(text: str, known_participant_names: Iterable[str]) -> str:
    if _DELETED_MESSAGES_RE.search(text):
        return "topic:deleted_messages"
    text_tokens = set(_normalized(text).split())
    for name in known_participant_names:
        for token in _name_tokens(name):
            if text_tokens & _single_name_forms(token):
                return f"person:{token}"
    return ""


def contextual_reference_reasons(
    text: str,
    *,
    history: Iterable[Dict[str, Any]],
    known_participant_names: Iterable[str],
) -> List[str]:
    """Return stale-reference violations without applying joke-only filters."""
    rows = list(history)
    scene_text = " ".join(
        " ".join(
            part
            for part in (
                _clean(row.get("name") or row.get("username"), limit=80),
                _clean(row.get("text"), limit=220),
            )
            if part
        )
        for row in rows
    )
    reasons: List[str] = []
    if _DELETED_MESSAGES_RE.search(text) and not _DELETED_MESSAGES_RE.search(scene_text):
        reasons.append("absent_deleted_messages")
    if any(
        _contains_name(text, name) and not _contains_name(scene_text, name)
        for name in known_participant_names
    ):
        reasons.append("absent_participant")
    return reasons


def strip_stale_context_references(
    text: str,
    *,
    history: Iterable[Dict[str, Any]],
    known_participant_names: Iterable[str],
) -> str:
    """Drop a stale joke tail while preserving a preceding factual answer."""
    rows = list(history)
    names = list(known_participant_names)
    parts = [part.strip() for part in re.split(r"(?<=[.!?])\s+|[\r\n]+", str(text or "")) if part.strip()]
    kept: List[str] = []
    for part in parts:
        if not contextual_reference_reasons(part, history=rows, known_participant_names=names):
            kept.append(part)
            continue
        # Models often attach the stale joke with a comma instead of making a
        # second sentence. Keep only the safe prefix; never reorder later text.
        safe_prefix: List[str] = []
        for clause in re.split(r"\s*(?:[,;]|—|\s-\s)\s*", part):
            clean_clause = clause.strip()
            if not clean_clause:
                continue
            if contextual_reference_reasons(clean_clause, history=rows, known_participant_names=names):
                break
            safe_prefix.append(clean_clause)
        if safe_prefix:
            kept.append(", ".join(safe_prefix))
    return _clean(" ".join(kept), limit=1000)


def _short_repeats_source(candidate: str, source: str) -> bool:
    candidate_tokens, source_tokens = _tokens(candidate), _tokens(source)
    if not candidate_tokens or not source_tokens or max(len(candidate_tokens), len(source_tokens)) > 4:
        return False
    overlap = len(candidate_tokens & source_tokens) / min(len(candidate_tokens), len(source_tokens))
    return overlap >= 0.8 and abs(len(candidate_tokens) - len(source_tokens)) <= 2


def filter_candidates(
    candidates: Iterable[Dict[str, Any]],
    *,
    history: Iterable[Dict[str, Any]],
    recent_outputs: Iterable[Any] = (),
    known_participant_names: Iterable[str] = (),
    blocked_callback_keys: Iterable[str] = (),
    max_chars: int = MAX_AMBIENT_REPLY_CHARS,
) -> List[Dict[str, str]]:
    rows = list(history)
    scene_text = " ".join(
        " ".join(
            part
            for part in (
                _clean(row.get("name") or row.get("username"), limit=80),
                _clean(row.get("text"), limit=220),
            )
            if part
        )
        for row in rows
        if _clean(row.get("text"), limit=220)
    )
    scene_lines = [_clean(row.get("text"), limit=220) for row in rows if _clean(row.get("text"), limit=220)]
    last_text = _clean(rows[-1].get("text"), limit=280) if rows else ""
    recent_items = list(recent_outputs)
    known_names = list(known_participant_names)
    recent_texts = [
        _clean(item.get("text"), limit=280) if isinstance(item, dict) else _clean(item, limit=280)
        for item in recent_items
    ]
    recent_mechanisms = [
        _clean(item.get("mechanism"), limit=80)
        for item in recent_items[-2:]
        if isinstance(item, dict) and _clean(item.get("mechanism"), limit=80)
    ]
    blocked = {
        "*" if str(item).strip() == "*" else _normalized(item)
        for item in blocked_callback_keys
        if str(item).strip() == "*" or _normalized(item)
    }
    result: List[Dict[str, str]] = []
    seen: List[str] = []

    for raw in candidates:
        if not isinstance(raw, dict):
            continue
        original_text = str(raw.get("text") or "").strip()
        text = _clean(original_text, limit=280)
        mechanism = _clean(raw.get("mechanism"), limit=80)
        callback_key = _clean(raw.get("callback_key"), limit=80)
        derived_callback_key = _derived_topic_callback_key(text, known_names)
        if derived_callback_key:
            # The model cannot evade a topic cooldown by renaming the same
            # Mitya/Kadyr/deleted-message bit or by returning an empty key.
            callback_key = derived_callback_key
        if not text or len(text) > max(1, int(max_chars)) or "\n" in original_text or "\r" in original_text:
            continue
        if any(pattern.search(text) for pattern in _BAD_TEMPLATE_PATTERNS):
            continue
        if mechanism and "callback" in _normalized(mechanism) and not callback_key:
            continue
        if last_text:
            normalized_text, normalized_last = _normalized(text), _normalized(last_text)
            if normalized_text == normalized_last or normalized_text.startswith(normalized_last + " "):
                continue
            if len(_tokens(text)) >= 3 and _near_duplicate(text, last_text):
                continue
        if any(
            (len(_tokens(text)) >= 3 and _near_duplicate(text, source_line))
            or _short_repeats_source(text, source_line)
            for source_line in scene_lines
        ):
            continue
        if any(_near_duplicate(text, previous) for previous in recent_texts if previous):
            continue
        if any(_near_duplicate(text, previous) for previous in seen):
            continue
        if callback_key and ("*" in blocked or _normalized(callback_key) in blocked):
            continue
        if any(blocked_key != "*" and blocked_key in _normalized(text) for blocked_key in blocked):
            continue
        if len(recent_mechanisms) == 2 and mechanism and all(_normalized(mechanism) == _normalized(x) for x in recent_mechanisms):
            continue
        if _DELETED_MESSAGES_RE.search(text) and not _DELETED_MESSAGES_RE.search(scene_text):
            continue
        absent_name = any(
            _contains_name(text, name) and not _contains_name(scene_text, name)
            for name in known_names
        )
        if absent_name:
            continue
        result.append({"text": text, "mechanism": mechanism, "callback_key": callback_key})
        seen.append(text)
        if len(result) >= MAX_CANDIDATES:
            break
    return result


def critic_messages(
    history: Iterable[Dict[str, Any]],
    candidates: Sequence[Dict[str, Any]],
    *,
    positive_example: Dict[str, Any] | None = None,
    recent_output_fingerprints: Iterable[str] = (),
    max_chars: int = MAX_AMBIENT_REPLY_CHARS,
) -> List[Dict[str, str]]:
    bounded_chars = max(1, int(max_chars))
    compact_candidates = [
        {
            "index": index,
            "text": _clean(item.get("text"), limit=bounded_chars),
            "mechanism": _clean(item.get("mechanism"), limit=80),
        }
        for index, item in enumerate(candidates[:MAX_CANDIDATES])
    ]
    system = (
        f"Ты независимый строгий редактор, не автор вариантов. Реплика должна быть не длиннее {bounded_chars} знаков. "
        "Выбери реплику только если она заметно лучше SILENCE. "
        "Оцени локальную точность (0–30), неожиданность без случайного скачка (0–20), естественность живого участника "
        "чата (0–20), краткость (0–10), свежесть против недавних реплик (0–20). Обнули вариант за отсутствующего "
        "человека, выдуманную предпосылку, пересказ setup, словарное определение, generic insult, объяснение шутки "
        "или натужный callback. Верни только JSON: "
        '{"winner_index":0,"score":0,"reason_codes":["..."]}. '
        "winner_index использует индексы из списка; null означает SILENCE. Для отправки нужен score не ниже 85."
    )
    user: Dict[str, Any] = {
        "scene": render_scene(history),
        "candidates": compact_candidates,
        "silence_is_valid": True,
        "recent_fingerprints": list(recent_output_fingerprints)[-20:],
    }
    if positive_example:
        user["one_positive_example"] = {
            "setup": _clean(positive_example.get("setup") or positive_example.get("setup_summary"), limit=180),
            "reply": _clean(positive_example.get("selected_text") or positive_example.get("good_reply"), limit=80),
            "mechanism": _clean(positive_example.get("mechanism"), limit=80),
        }
    return [{"role": "system", "content": system}, {"role": "user", "content": json.dumps(user, ensure_ascii=False)}]


def parse_critic(
    text: str,
    *,
    candidate_count: int | None = None,
) -> tuple[int | None, int, List[str]]:
    payload = _json_object(text)
    if not payload:
        return None, 0, ["invalid_json"]
    raw_score = payload.get("score")
    score = max(0, min(100, raw_score)) if isinstance(raw_score, int) and not isinstance(raw_score, bool) else 0
    raw_winner = payload.get("winner_index")
    winner: int | None
    winner = raw_winner if isinstance(raw_winner, int) and not isinstance(raw_winner, bool) else None
    if winner is not None and (winner < 0 or (candidate_count is not None and winner >= candidate_count)):
        winner = None
    raw_reasons = payload.get("reason_codes")
    reasons = (
        [_clean(item, limit=60) for item in raw_reasons if _clean(item, limit=60)][:8]
        if isinstance(raw_reasons, list)
        else []
    )
    return winner, score, reasons
