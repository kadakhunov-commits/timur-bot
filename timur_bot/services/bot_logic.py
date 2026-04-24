#!/usr/bin/env python3
import asyncio
import base64
import functools
import io
import json
import logging
import random
import re
import subprocess
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple

from billing_system import BillingEngine, BillingError
from openai import OpenAI
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, InputFile, Message, Update
from telegram.ext import (
    ContextTypes,
)
from timur_bot.core.config import ConfigError, load_app_config
from timur_bot.services.text_processing import (
    detect_archetype_scores as detect_archetype_scores_service,
    extract_keywords as extract_keywords_service,
    normalize_token as normalize_token_service,
    sanitize_reply_text as sanitize_reply_text_service,
    split_into_chain as split_into_chain_service,
    top_items as top_items_service,
)
from timur_bot.services.voice_tts import synthesize_ogg_opus_from_text

# =========================
# БАЗОВАЯ НАСТРОЙКА
# =========================

BASE_DIR = Path(__file__).resolve().parent
ROOT_DIR = BASE_DIR.parents[1]
try:
    APP_CONFIG = load_app_config(ROOT_DIR)
except ConfigError as e:
    raise RuntimeError(str(e)) from e

MEMORY_PATH = APP_CONFIG.memory_path
BILLING_PATH = APP_CONFIG.billing_path
TELEGRAM_BOT_TOKEN = APP_CONFIG.telegram_bot_token
OPENAI_API_KEY = APP_CONFIG.openai_api_key
OPENAI_BASE_URL = APP_CONFIG.openai_base_url
GEMINI_API_KEY = APP_CONFIG.gemini_api_key

client = OpenAI(
    api_key=OPENAI_API_KEY,
    **({"base_url": OPENAI_BASE_URL} if OPENAI_BASE_URL else {}),
)

OWNER_ID = APP_CONFIG.owner_id
TEXT_MODEL = APP_CONFIG.text_model
VISION_MODEL = APP_CONFIG.vision_model
VOICE_MODEL = APP_CONFIG.voice_model
VOICE_NAME = APP_CONFIG.voice_name
VOICE_STYLE_PROMPT = APP_CONFIG.voice_style_prompt
MAX_HISTORY_PER_CHAT = APP_CONFIG.max_history_per_chat
MAX_LOG_PER_CHAT = APP_CONFIG.max_log_per_chat
MAX_USER_SAMPLES = APP_CONFIG.max_user_samples
MAX_QUOTES_PER_USER = APP_CONFIG.max_quotes_per_user
MAX_KEYWORDS_PER_USER = APP_CONFIG.max_keywords_per_user
MAX_TOPIC_EDGES = APP_CONFIG.max_topic_edges
MAX_USER_RELATIONS = APP_CONFIG.max_user_relations
GLOBAL_DAILY_VISION_LIMIT = APP_CONFIG.global_daily_vision_limit
CHAT_DAILY_VISION_LIMIT = APP_CONFIG.chat_daily_vision_limit
USER_DAILY_VISION_LIMIT = APP_CONFIG.user_daily_vision_limit
GLOBAL_DAILY_VOICE_LIMIT = APP_CONFIG.global_daily_voice_limit
CHAT_DAILY_VOICE_LIMIT = APP_CONFIG.chat_daily_voice_limit
MAX_VOICE_CHARS = APP_CONFIG.max_voice_chars
BASE_REPLY_CHANCE = APP_CONFIG.base_reply_chance
CHAIN_REPLY_CHANCE = APP_CONFIG.chain_reply_chance
MEM_REPLY_CHANCE = APP_CONFIG.mem_reply_chance
PHOTO_RANDOM_REPLY_CHANCE = APP_CONFIG.photo_random_reply_chance
VOICE_REPLY_CHANCE = APP_CONFIG.voice_reply_chance
MEMES = APP_CONFIG.memes
YOUTUBE_LINKS = APP_CONFIG.youtube_links
RUS_STOPWORDS = APP_CONFIG.rus_stopwords
EN_STOPWORDS = APP_CONFIG.en_stopwords
PROFANITY_MARKERS = APP_CONFIG.profanity_markers
ARCHETYPE_LEXICON = APP_CONFIG.archetype_lexicon
PERSONA_MODES = APP_CONFIG.persona_modes

# =========================
# ЛОГИ
# =========================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("timur-bot")
billing = BillingEngine(BILLING_PATH, logger=logger)


# =========================
# SYSTEM PROMPT
# =========================

DEFAULT_SYSTEM_PROMPT = APP_CONFIG.default_system_prompt


# =========================
# ПАМЯТЬ
# =========================

def default_memory() -> Dict[str, Any]:
    return {
        "chats": {},
        "users": {},
        "config": {
            "system_prompt": DEFAULT_SYSTEM_PROMPT,
            "style_settings": APP_CONFIG.default_style_settings,
            "bio": APP_CONFIG.default_bio,
            "toxicity_level": APP_CONFIG.default_toxicity_level,
            "active_mode": APP_CONFIG.default_active_mode,
            "mode_overrides": {},
            "last_random_story_ts": None,
            "vision_usage": {},
            "voice_usage": {},
        },
    }


def _ensure_chat_schema(chat: Dict[str, Any]) -> Dict[str, Any]:
    chat.setdefault("history", [])
    chat.setdefault("log", [])
    chat.setdefault("last_meme", None)
    chat.setdefault("participants", {})
    chat.setdefault("user_relations", {})
    chat.setdefault("topic_edges", {})
    return chat


def load_memory() -> Dict[str, Any]:
    if not MEMORY_PATH.exists():
        return default_memory()

    try:
        with open(MEMORY_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)

        data.setdefault("chats", {})
        data.setdefault("users", {})
        cfg = data.setdefault("config", {})
        cfg.setdefault("system_prompt", DEFAULT_SYSTEM_PROMPT)
        cfg.setdefault("style_settings", APP_CONFIG.default_style_settings)
        cfg.setdefault("bio", APP_CONFIG.default_bio)
        cfg.setdefault("toxicity_level", APP_CONFIG.default_toxicity_level)
        cfg.setdefault("active_mode", APP_CONFIG.default_active_mode)
        cfg.setdefault("mode_overrides", {})
        cfg.setdefault("last_random_story_ts", None)
        cfg.setdefault("vision_usage", {})
        cfg.setdefault("voice_usage", {})

        for _, chat in data["chats"].items():
            _ensure_chat_schema(chat)

        return data

    except Exception as e:
        logger.error("Failed to load memory.json: %s", e)
        return default_memory()


def save_memory(memory: Dict[str, Any]) -> None:
    try:
        with open(MEMORY_PATH, "w", encoding="utf-8") as f:
            json.dump(memory, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error("Failed to save memory.json: %s", e)


def get_chat_mem(memory: Dict[str, Any], chat_id: int) -> Dict[str, Any]:
    chats = memory.setdefault("chats", {})
    chat = chats.setdefault(str(chat_id), {})
    return _ensure_chat_schema(chat)


def get_user_mem(memory: Dict[str, Any], user_id: int) -> Dict[str, Any]:
    users = memory.setdefault("users", {})
    user = users.setdefault(str(user_id), {})
    user.setdefault("name", "")
    user.setdefault("username", "")
    user.setdefault("count", 0)
    user.setdefault("samples", [])
    user.setdefault("events", [])
    user.setdefault("bio", "")
    return user


def normalize_token(token: str) -> str:
    return normalize_token_service(token)


def extract_keywords(text: str, limit: int = 6) -> List[str]:
    return extract_keywords_service(
        text,
        limit=limit,
        rus_stopwords=RUS_STOPWORDS,
        en_stopwords=EN_STOPWORDS,
    )


def detect_archetype_scores(text: str, keywords: List[str]) -> Dict[str, int]:
    return detect_archetype_scores_service(
        text,
        keywords,
        archetype_lexicon=ARCHETYPE_LEXICON,
        rus_stopwords=RUS_STOPWORDS,
        en_stopwords=EN_STOPWORDS,
    )


def _relation_key(a: int, b: int) -> str:
    x, y = sorted([int(a), int(b)])
    return f"{x}|{y}"


def _topic_edge_key(a: str, b: str) -> str:
    x, y = sorted([a, b])
    return f"{x}|{y}"


def _prune_counter_dict(counter: Dict[str, Any], limit: int) -> Dict[str, Any]:
    if len(counter) <= limit:
        return counter
    top = sorted(counter.items(), key=lambda x: (-float(x[1]), x[0]))[:limit]
    return dict(top)


def _extract_message_text(message: Message) -> str:
    return (message.text or message.caption or "").strip()


def _extract_user_mentions_by_text(chat_mem: Dict[str, Any], text: str, author_id: int) -> List[int]:
    mentions: List[int] = []
    text_low = (text or "").lower()
    participants = chat_mem.get("participants", {})

    for uid_str, pdata in participants.items():
        uid = int(uid_str)
        if uid == author_id:
            continue

        candidates = [
            (pdata.get("name") or "").lower().strip(),
            (pdata.get("username") or "").lower().strip(),
        ]
        for c in candidates:
            if not c:
                continue
            # имя с 2+ символами считаем валидным сигналом
            if len(c) >= 2 and c in text_low:
                mentions.append(uid)
                break

    return mentions


def _update_participant_portrait(chat_mem: Dict[str, Any], message: Message, text: str, user_keywords: List[str]) -> None:
    tg_user = message.from_user
    if not tg_user:
        return

    participants = chat_mem.setdefault("participants", {})
    p = participants.setdefault(str(tg_user.id), {
        "user_id": tg_user.id,
        "name": tg_user.first_name or "",
        "username": tg_user.username or "",
        "message_count": 0,
        "last_seen": None,
        "quotes": [],
        "keywords": {},
        "archetypes": {},
        "style": {
            "questions": 0,
            "profanity": 0,
            "caps": 0,
            "short_msgs": 0,
        },
    })

    p["name"] = tg_user.first_name or p.get("name", "")
    p["username"] = tg_user.username or p.get("username", "")
    p["message_count"] = int(p.get("message_count", 0)) + 1
    p["last_seen"] = datetime.utcnow().isoformat()

    style = p.setdefault("style", {})
    style["questions"] = int(style.get("questions", 0)) + int("?" in text)
    style["short_msgs"] = int(style.get("short_msgs", 0)) + int(len(text) < 35)

    up = sum(1 for ch in text if ch.isupper())
    low = sum(1 for ch in text if ch.islower())
    caps_ratio = (up / (up + low)) if (up + low) else 0.0
    if caps_ratio > 0.4:
        style["caps"] = int(style.get("caps", 0)) + 1

    text_low = text.lower()
    if any(marker in text_low for marker in PROFANITY_MARKERS):
        style["profanity"] = int(style.get("profanity", 0)) + 1

    if text:
        quotes = p.setdefault("quotes", [])
        if len(text) <= 120:
            quotes.append(text)
            if len(quotes) > MAX_QUOTES_PER_USER:
                quotes.pop(0)

    kw = p.setdefault("keywords", {})
    for token in user_keywords:
        kw[token] = float(kw.get(token, 0.0)) + 1.0
    p["keywords"] = _prune_counter_dict(kw, MAX_KEYWORDS_PER_USER)

    archetypes = p.setdefault("archetypes", {})
    for name, val in detect_archetype_scores(text, user_keywords).items():
        if val:
            archetypes[name] = float(archetypes.get(name, 0.0)) + float(val)


def _update_association_graph(chat_mem: Dict[str, Any], message: Message, text: str, user_keywords: List[str]) -> None:
    tg_user = message.from_user
    if not tg_user:
        return

    user_id = tg_user.id

    # Связи пользователь <-> тема
    topic_edges = chat_mem.setdefault("topic_edges", {})
    for token in user_keywords:
        edge_key = f"u:{user_id}|k:{token}"
        topic_edges[edge_key] = float(topic_edges.get(edge_key, 0.0)) + 1.0

    # Связи тема <-> тема
    for i in range(len(user_keywords)):
        for j in range(i + 1, len(user_keywords)):
            e = _topic_edge_key(f"k:{user_keywords[i]}", f"k:{user_keywords[j]}")
            topic_edges[e] = float(topic_edges.get(e, 0.0)) + 0.6

    chat_mem["topic_edges"] = _prune_counter_dict(topic_edges, MAX_TOPIC_EDGES)

    # Связи пользователь <-> пользователь
    user_rel = chat_mem.setdefault("user_relations", {})

    if message.reply_to_message and message.reply_to_message.from_user:
        target_id = message.reply_to_message.from_user.id
        if target_id != user_id:
            k = _relation_key(user_id, target_id)
            user_rel[k] = float(user_rel.get(k, 0.0)) + 2.0

    for mentioned in _extract_user_mentions_by_text(chat_mem, text, user_id):
        k = _relation_key(user_id, mentioned)
        user_rel[k] = float(user_rel.get(k, 0.0)) + 1.2

    chat_mem["user_relations"] = _prune_counter_dict(user_rel, MAX_USER_RELATIONS)


def update_memory_with_message(memory: Dict[str, Any], message: Message) -> None:
    chat_id = message.chat_id
    tg_user = message.from_user

    if not tg_user:
        return

    chat_mem = get_chat_mem(memory, chat_id)
    user_mem = get_user_mem(memory, tg_user.id)

    text = _extract_message_text(message)

    user_mem["name"] = tg_user.first_name or user_mem.get("name", "")
    user_mem["username"] = tg_user.username or user_mem.get("username", "")
    user_mem["count"] = user_mem.get("count", 0) + 1

    if text:
        samples = user_mem["samples"]
        samples.append(text)
        if len(samples) > MAX_USER_SAMPLES:
            samples.pop(0)

    rec = {
        "user_id": tg_user.id,
        "name": user_mem["name"],
        "username": user_mem["username"],
        "text": text,
        "ts": datetime.utcnow().isoformat(),
        "is_bot": tg_user.is_bot,
        "message_id": message.message_id,
    }

    history = chat_mem["history"]
    history.append(rec)
    if len(history) > MAX_HISTORY_PER_CHAT:
        history.pop(0)

    log = chat_mem["log"]
    log.append(rec)
    if len(log) > MAX_LOG_PER_CHAT:
        log.pop(0)

    if text:
        keywords = extract_keywords(text)
        _update_participant_portrait(chat_mem, message, text, keywords)
        _update_association_graph(chat_mem, message, text, keywords)

    save_memory(memory)
    try:
        billing.register_activity(
            chat_id=chat_id,
            user_id=tg_user.id,
            username=tg_user.username or "",
            name=tg_user.first_name or "",
            is_bot=bool(tg_user.is_bot),
        )
    except Exception as e:
        logger.error("Billing activity update failed: %s", e)


# =========================
# VISION ЛИМИТЫ
# =========================

def _today_str() -> str:
    return date.today().isoformat()


def can_use_vision(memory: Dict[str, Any], chat_id: int, user_id: int) -> bool:
    cfg = memory.setdefault("config", {})
    vu = cfg.setdefault("vision_usage", {})

    today = _today_str()
    stats = vu.setdefault(today, {
        "global": 0,
        "chats": {},
        "users": {},
    })

    if stats["global"] >= GLOBAL_DAILY_VISION_LIMIT:
        return False

    if stats["chats"].get(str(chat_id), 0) >= CHAT_DAILY_VISION_LIMIT:
        return False

    if stats["users"].get(str(user_id), 0) >= USER_DAILY_VISION_LIMIT:
        return False

    return True


def increase_vision_counters(memory: Dict[str, Any], chat_id: int, user_id: int) -> None:
    cfg = memory.setdefault("config", {})
    vu = cfg.setdefault("vision_usage", {})

    today = _today_str()
    stats = vu.setdefault(today, {
        "global": 0,
        "chats": {},
        "users": {},
    })

    stats["global"] += 1
    stats["chats"][str(chat_id)] = stats["chats"].get(str(chat_id), 0) + 1
    stats["users"][str(user_id)] = stats["users"].get(str(user_id), 0) + 1

    save_memory(memory)


def can_send_voice(memory: Dict[str, Any], chat_id: int) -> bool:
    cfg = memory.setdefault("config", {})
    vu = cfg.setdefault("voice_usage", {})

    today = _today_str()
    stats = vu.setdefault(today, {
        "global": 0,
        "chats": {},
    })

    if stats["global"] >= GLOBAL_DAILY_VOICE_LIMIT:
        return False

    if stats["chats"].get(str(chat_id), 0) >= CHAT_DAILY_VOICE_LIMIT:
        return False

    return True


def increase_voice_counters(memory: Dict[str, Any], chat_id: int) -> None:
    cfg = memory.setdefault("config", {})
    vu = cfg.setdefault("voice_usage", {})

    today = _today_str()
    stats = vu.setdefault(today, {
        "global": 0,
        "chats": {},
    })

    stats["global"] += 1
    stats["chats"][str(chat_id)] = stats["chats"].get(str(chat_id), 0) + 1
    save_memory(memory)


# =========================
# ЛОГИКА ОТВЕТОВ
# =========================

def is_name_mentioned(text: str) -> bool:
    text_low = text.lower()
    patterns = [
        r"\btimur\b",
        r"\bтимур\b",
        r"\bтёма\b",
        r"\bтема\b",
        r"\bтёмыч\b",
        r"\bтимурчик\b",
    ]
    return any(re.search(p, text_low) for p in patterns)


def looks_like_address_to_bot(text: str) -> bool:
    text_low = text.lower()
    phrases = [
        "как думаешь",
        "что думаешь",
        "ну скажи",
        "ну чё там",
        "ну че там",
        "твое мнение",
        "скажи уже",
        "ты как",
        "ты че",
        "ты чё",
        "тимур",
    ]
    return any(p in text_low for p in phrases)


def is_voice_codeword(text: str) -> bool:
    norm = re.sub(r"\s+", " ", (text or "").strip().lower())
    return "тимур отправь голосовое" in norm


def should_reply(memory: Dict[str, Any], message: Message, bot_id: int) -> bool:
    if not message.text and not message.caption:
        return False

    text = _extract_message_text(message)
    tg_user = message.from_user

    if not tg_user:
        return False

    if tg_user.id == bot_id:
        return False

    if message.reply_to_message and message.reply_to_message.from_user:
        if message.reply_to_message.from_user.id == bot_id:
            return True

    if is_name_mentioned(text):
        return True

    if looks_like_address_to_bot(text):
        chance = random.uniform(0.75, 1.0)
        roll = random.random()
        logger.info("Address chance=%.2f roll=%.2f", chance, roll)
        return roll < chance

    roll = random.random()
    logger.info("Base chance=%.2f roll=%.2f", BASE_REPLY_CHANCE, roll)
    return roll < BASE_REPLY_CHANCE


# =========================
# ПРОМПТ ДЛЯ OPENAI
# =========================

def get_system_prompt(memory: Dict[str, Any]) -> str:
    cfg = memory.setdefault("config", {})
    return cfg.get("system_prompt") or DEFAULT_SYSTEM_PROMPT


def get_style_settings(memory: Dict[str, Any]) -> str:
    cfg = memory.setdefault("config", {})
    return str(cfg.get("style_settings") or "").strip()


def get_bio_settings(memory: Dict[str, Any]) -> str:
    cfg = memory.setdefault("config", {})
    return str(cfg.get("bio") or "").strip()


def get_toxicity_level(memory: Dict[str, Any]) -> int:
    cfg = memory.setdefault("config", {})
    raw = cfg.get("toxicity_level", 82)
    try:
        val = int(raw)
    except Exception:
        val = 82
    return max(0, min(100, val))


def get_active_mode(memory: Dict[str, Any]) -> str:
    cfg = memory.setdefault("config", {})
    mode = str(cfg.get("active_mode", "default")).strip().lower()
    if mode not in PERSONA_MODES:
        mode = "default"
    return mode


def get_mode_prompt(memory: Dict[str, Any]) -> str:
    cfg = memory.setdefault("config", {})
    active_mode = get_active_mode(memory)
    overrides = cfg.get("mode_overrides", {})
    if isinstance(overrides, dict):
        custom = str(overrides.get(active_mode, "")).strip()
        if custom:
            return custom
    return PERSONA_MODES.get(active_mode, PERSONA_MODES["default"])


def select_user_profile(memory: Dict[str, Any], user_id: int) -> str:
    user_mem = get_user_mem(memory, user_id)
    pieces = []

    if user_mem.get("bio"):
        pieces.append(f"биография пользователя: {user_mem['bio']}")

    samples = user_mem.get("samples", [])
    if samples:
        last_samples = samples[-5:]
        pieces.append("типичные фразы пользователя: " + " | ".join(last_samples))

    events = user_mem.get("events", [])
    if events:
        last_events = events[-4:]
        event_text = "; ".join(
            e.get("event", "") for e in last_events if e.get("event")
        )
        if event_text:
            pieces.append("важные события пользователя: " + event_text)

    return "\n".join(pieces)


def select_chat_history_for_context(memory: Dict[str, Any], chat_id: int) -> List[Dict[str, Any]]:
    chat_mem = get_chat_mem(memory, chat_id)
    history = chat_mem.get("history", [])
    return history[-12:]


def select_old_random_memories(memory: Dict[str, Any], chat_id: int) -> List[str]:
    chat_mem = get_chat_mem(memory, chat_id)
    log = chat_mem.get("log", [])

    if len(log) < 20:
        return []

    sample = random.sample(log, k=min(3, len(log)))
    lines = []

    for rec in sample:
        text = rec.get("text", "")
        if text:
            name = rec.get("name") or rec.get("username") or str(rec.get("user_id"))
            lines.append(f"{name}: {text}")

    return lines


def _top_items(counter: Dict[str, Any], n: int = 3) -> List[Tuple[str, float]]:
    return top_items_service(counter, n=n)


def build_association_context(memory: Dict[str, Any], chat_id: int, focus_user_id: int) -> str:
    chat_mem = get_chat_mem(memory, chat_id)
    participants = chat_mem.get("participants", {})
    if not participants:
        return ""

    lines: List[str] = []
    lines.append("персонажи беседы:")

    p_sorted = sorted(
        participants.values(),
        key=lambda p: int(p.get("message_count", 0)),
        reverse=True,
    )[:10]

    for p in p_sorted:
        uid = int(p.get("user_id", 0))
        name = p.get("name") or p.get("username") or str(uid)
        uname = p.get("username") or ""
        label = f"{name} (@{uname})" if uname else name

        archetypes = _top_items(p.get("archetypes", {}), n=2)
        role_text = ", ".join(a for a, _ in archetypes) if archetypes else "хаотик"

        kws = _top_items(p.get("keywords", {}), n=4)
        kw_text = ", ".join(k for k, _ in kws) if kws else "без явных тем"

        style = p.get("style", {})
        style_parts = []
        if int(style.get("profanity", 0)) >= 2:
            style_parts.append("любит мат")
        if int(style.get("questions", 0)) >= 3:
            style_parts.append("часто допрашивает")
        if int(style.get("short_msgs", 0)) >= 4:
            style_parts.append("рубит коротко")

        style_text = "; ".join(style_parts) if style_parts else "обычный вайб"
        lines.append(f"- {label}: роль {role_text}; темы {kw_text}; стиль {style_text}")

    rel = chat_mem.get("user_relations", {})
    rel_lines = []
    for key, weight in rel.items():
        try:
            a_str, b_str = key.split("|", 1)
            a, b = int(a_str), int(b_str)
            if focus_user_id not in (a, b):
                continue
            other = b if a == focus_user_id else a
            p = participants.get(str(other), {})
            n = p.get("name") or p.get("username") or str(other)
            rel_lines.append((n, float(weight)))
        except Exception:
            continue

    if rel_lines:
        rel_lines.sort(key=lambda x: (-x[1], x[0]))
        top_rel = ", ".join(name for name, _ in rel_lines[:4])
        lines.append(f"для текущего собеседника самые связанные персонажи: {top_rel}")

    topic_edges = chat_mem.get("topic_edges", {})
    cloud = []
    for k, w in topic_edges.items():
        if "|" not in k:
            continue
        left, right = k.split("|", 1)
        if left.startswith("k:") and right.startswith("k:"):
            cloud.append((left[2:], right[2:], float(w)))

    if cloud:
        cloud.sort(key=lambda x: (-x[2], x[0], x[1]))
        formatted = "; ".join(f"{a}<->{b}" for a, b, _ in cloud[:6])
        lines.append("ассоциативное облако тем: " + formatted)

    return "\n".join(lines)


def build_chat_messages(memory: Dict[str, Any], message: Message) -> List[Dict[str, Any]]:
    tg_user = message.from_user
    assert tg_user is not None

    system_prompt = get_system_prompt(memory)
    user_profile = select_user_profile(memory, tg_user.id)
    chat_history = select_chat_history_for_context(memory, message.chat_id)
    random_memories = select_old_random_memories(memory, message.chat_id)
    association_context = build_association_context(memory, message.chat_id, tg_user.id)

    hist_lines = []
    for rec in chat_history:
        name = rec.get("name") or rec.get("username") or str(rec.get("user_id"))
        txt = rec.get("text", "")
        if txt:
            hist_lines.append(f"{name}: {txt}")

    full_system = system_prompt + "\n\n"
    full_system += (
        "правила стиля:\n"
        "- всегда используй только строчные буквы\n"
        "- без эмодзи\n"
        "- максимум 2 очень коротких предложения в одном сообщении\n"
        "- можно токсично и колко, но в формате шутки и подкола\n"
        "- нельзя призывать к насилию или унижать по защищенным признакам\n"
        "- не объясняй как ты думаешь, просто говори\n"
        "- иногда можешь вспомнить, что митя когда-то удалил все сообщения кадыра, как локальный мем\n"
    )

    toxicity = get_toxicity_level(memory)
    full_system += f"\nуровень прожарки: {toxicity}/100\n"
    full_system += f"активный режим личности: {get_active_mode(memory)}\n"
    full_system += "инструкция режима: " + get_mode_prompt(memory) + "\n"

    style_settings = get_style_settings(memory)
    if style_settings:
        full_system += "\nдоп стиль от владельца:\n" + style_settings + "\n"

    bio_settings = get_bio_settings(memory)
    if bio_settings:
        full_system += "\nбио тимура от владельца:\n" + bio_settings + "\n"

    if user_profile:
        full_system += "\nинфа о собеседнике:\n" + user_profile

    if association_context:
        full_system += "\n\nкарта персонажей и ассоциаций:\n" + association_context

    if hist_lines:
        full_system += "\n\nпоследние сообщения в чате:\n" + "\n".join(hist_lines)

    if random_memories:
        full_system += "\n\nстарые моменты чата, к которым можно сделать отсылку:\n"
        for line in random_memories:
            full_system += f"- {line}\n"

    user_text = _extract_message_text(message)

    return [
        {"role": "system", "content": full_system},
        {"role": "user", "content": user_text},
    ]


# =========================
# OPENAI
# =========================

async def call_openai_text(messages: List[Dict[str, Any]]) -> str:
    try:
        response = await asyncio.to_thread(
            client.chat.completions.create,
            model=TEXT_MODEL,
            messages=messages,
            max_tokens=150,
            temperature=0.85,
        )
        return (response.choices[0].message.content or "").strip()

    except Exception as e:
        logger.error("OpenAI text error: %s", e)
        return ""


async def call_openai_vision(memory: Dict[str, Any], message: Message, image_b64: str) -> str:
    text_context = (
        "тебе прислали фотку в чате. "
        "сделай короткую смешную токсичную реакцию в стиле дружеской прожарки, "
        "без технического описания, максимум 1–2 коротких фразы. "
        "без эмодзи, маленькими буквами."
    )

    history = select_chat_history_for_context(memory, message.chat_id)
    hist_lines = []

    for rec in history:
        name = rec.get("name") or rec.get("username") or str(rec.get("user_id"))
        txt = rec.get("text", "")
        if txt:
            hist_lines.append(f"{name}: {txt}")

    if hist_lines:
        text_context += "\n\nпоследние сообщения в чате:\n" + "\n".join(hist_lines)

    msg_content = [
        {"type": "text", "text": text_context},
        {
            "type": "image_url",
            "image_url": {
                "url": f"data:image/jpeg;base64,{image_b64}"
            },
        },
    ]

    try:
        response = await asyncio.to_thread(
            client.chat.completions.create,
            model=VISION_MODEL,
            messages=[
                {"role": "system", "content": get_system_prompt(memory)},
                {"role": "user", "content": msg_content},
            ],
            max_tokens=100,
            temperature=0.85,
        )
        return (response.choices[0].message.content or "").strip()

    except Exception as e:
        logger.error("OpenAI vision error: %s", e)
        return ""


# =========================
# ОБРАБОТКА ТЕКСТА
# =========================

def sanitize_reply_text(raw: str) -> str:
    return sanitize_reply_text_service(raw)


def split_into_chain(text: str) -> List[str]:
    return split_into_chain_service(text)


# =========================
# ОТПРАВКА
# =========================

async def send_reply_with_style(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    memory: Dict[str, Any],
    reply_text: str,
    force_voice: bool = False,
) -> None:
    del context
    message = update.effective_message

    if not message:
        return

    reply_text = sanitize_reply_text(reply_text)

    if not reply_text:
        logger.info("Empty reply after sanitizing, skipping")
        return

    use_watermark = False
    watermark_text = ""
    try:
        use_watermark, watermark_text = billing.should_apply_free_watermark(message.chat_id)
        if use_watermark and watermark_text:
            reply_text = f"{reply_text}\n\n{watermark_text}"
    except Exception as e:
        logger.error("Billing watermark check failed: %s", e)

    use_meme = random.random() < MEM_REPLY_CHANCE and (MEMES or YOUTUBE_LINKS)

    if use_meme:
        logger.info("Using meme/video instead of text")

        if MEMES and (not YOUTUBE_LINKS or random.random() < 0.5):
            meme_url = random.choice(MEMES)
            await message.reply_text(meme_url)
        else:
            yt = random.choice(YOUTUBE_LINKS)
            await message.reply_text(yt)

        return

    can_try_voice = bool(GEMINI_API_KEY) and can_send_voice(memory, message.chat_id) and (
        force_voice or (not use_watermark and random.random() < VOICE_REPLY_CHANCE)
    )
    if can_try_voice:
        voice_text = re.sub(r"\s+", " ", re.sub(r"https?://\S+", "", reply_text)).strip()
        if len(voice_text) > MAX_VOICE_CHARS:
            voice_text = voice_text[:MAX_VOICE_CHARS].rsplit(" ", 1)[0].strip()
        if voice_text:
            tts_text = voice_text
            if VOICE_STYLE_PROMPT:
                tts_text = f"{VOICE_STYLE_PROMPT}\n{voice_text}"
            try:
                voice_ogg = await asyncio.to_thread(
                    synthesize_ogg_opus_from_text,
                    api_key=GEMINI_API_KEY,
                    model=VOICE_MODEL,
                    voice_name=VOICE_NAME,
                    text=tts_text,
                )
                buf = io.BytesIO(voice_ogg)
                buf.name = "timur_voice.ogg"
                await message.reply_voice(voice=InputFile(buf))
                if use_watermark and watermark_text:
                    await message.reply_text(watermark_text)
                increase_voice_counters(memory, message.chat_id)
                logger.info("Voice reply sent in chat %s", message.chat_id)
                return
            except Exception as e:
                logger.error("Voice generation failed, fallback to text: %s", e)

    if random.random() < CHAIN_REPLY_CHANCE:
        parts = split_into_chain(reply_text)

        if not parts:
            parts = [reply_text]

        for part in parts:
            await message.reply_text(part)
            await asyncio.sleep(random.uniform(0.2, 0.6))
    else:
        await message.reply_text(reply_text)


# =========================
# ADMIN PANEL
# =========================

def _is_owner(update: Update) -> bool:
    user = update.effective_user
    return bool(user and user.id == OWNER_ID)


def _admin_main_keyboard(chat_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("режим личности", callback_data=f"adm:mode_menu:{chat_id}"),
                InlineKeyboardButton("вредность", callback_data=f"adm:heat_menu:{chat_id}"),
            ],
            [
                InlineKeyboardButton("облака ассоциаций", callback_data=f"adm:cloud_menu:{chat_id}"),
            ],
            [
                InlineKeyboardButton("редактировать промпт", callback_data=f"adm:input:system_prompt:{chat_id}"),
                InlineKeyboardButton("редактировать стиль", callback_data=f"adm:input:style:{chat_id}"),
            ],
            [
                InlineKeyboardButton("редактировать био", callback_data=f"adm:input:bio:{chat_id}"),
                InlineKeyboardButton("обновить из github", callback_data=f"adm:update_pull:{chat_id}"),
            ],
            [
                InlineKeyboardButton("обновить экран", callback_data=f"adm:root:{chat_id}"),
            ],
        ]
    )


def _admin_mode_keyboard(chat_id: int, active_mode: str) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    mode_names = list(PERSONA_MODES.keys())
    for i in range(0, len(mode_names), 2):
        line: List[InlineKeyboardButton] = []
        for mode in mode_names[i:i + 2]:
            marker = "● " if mode == active_mode else ""
            line.append(
                InlineKeyboardButton(
                    f"{marker}{mode}",
                    callback_data=f"adm:set_mode:{mode}:{chat_id}",
                )
            )
        rows.append(line)

    rows.append(
        [
            InlineKeyboardButton("кастомизировать режим", callback_data=f"adm:mode_edit_menu:{chat_id}"),
            InlineKeyboardButton("назад", callback_data=f"adm:root:{chat_id}"),
        ]
    )
    return InlineKeyboardMarkup(rows)


def _admin_mode_edit_keyboard(chat_id: int) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    mode_names = list(PERSONA_MODES.keys())
    for i in range(0, len(mode_names), 2):
        line: List[InlineKeyboardButton] = []
        for mode in mode_names[i:i + 2]:
            line.append(
                InlineKeyboardButton(
                    f"редактировать {mode}",
                    callback_data=f"adm:input:mode:{mode}:{chat_id}",
                )
            )
        rows.append(line)
    rows.append([InlineKeyboardButton("назад", callback_data=f"adm:mode_menu:{chat_id}")])
    return InlineKeyboardMarkup(rows)


def _admin_heat_keyboard(chat_id: int, heat: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("-20", callback_data=f"adm:heat_delta:-20:{chat_id}"),
                InlineKeyboardButton("-5", callback_data=f"adm:heat_delta:-5:{chat_id}"),
                InlineKeyboardButton("+5", callback_data=f"adm:heat_delta:5:{chat_id}"),
                InlineKeyboardButton("+20", callback_data=f"adm:heat_delta:20:{chat_id}"),
            ],
            [
                InlineKeyboardButton("0", callback_data=f"adm:heat_set:0:{chat_id}"),
                InlineKeyboardButton("50", callback_data=f"adm:heat_set:50:{chat_id}"),
                InlineKeyboardButton("100", callback_data=f"adm:heat_set:100:{chat_id}"),
            ],
            [
                InlineKeyboardButton("ввести вручную", callback_data=f"adm:input:heat:{chat_id}"),
                InlineKeyboardButton("назад", callback_data=f"adm:root:{chat_id}"),
            ],
        ]
    )


def _admin_cloud_users_keyboard(memory: Dict[str, Any], chat_id: int) -> InlineKeyboardMarkup:
    chat_mem = get_chat_mem(memory, chat_id)
    participants = chat_mem.get("participants", {})
    p_sorted = sorted(
        participants.values(),
        key=lambda p: int(p.get("message_count", 0)),
        reverse=True,
    )[:12]

    rows: List[List[InlineKeyboardButton]] = []
    for p in p_sorted:
        uid = int(p.get("user_id", 0))
        if not uid:
            continue
        name = p.get("name") or p.get("username") or str(uid)
        rows.append(
            [InlineKeyboardButton(name[:28], callback_data=f"adm:cloud_user:{uid}:{chat_id}")]
        )

    rows.append([InlineKeyboardButton("назад", callback_data=f"adm:root:{chat_id}")])
    return InlineKeyboardMarkup(rows)


def _format_admin_status(memory: Dict[str, Any], chat_id: int) -> str:
    chat_mem = get_chat_mem(memory, chat_id)
    participants_cnt = len(chat_mem.get("participants", {}))
    relations_cnt = len(chat_mem.get("user_relations", {}))
    topic_edges_cnt = len(chat_mem.get("topic_edges", {}))
    return (
        "админ панель тимура\n"
        f"чат: {chat_id}\n"
        f"режим: {get_active_mode(memory)}\n"
        f"вредность: {get_toxicity_level(memory)}/100\n"
        f"персонажей в памяти: {participants_cnt}\n"
        f"связей user-user: {relations_cnt}\n"
        f"ребер облака тем: {topic_edges_cnt}"
    )


def _format_cloud_for_user(memory: Dict[str, Any], chat_id: int, user_id: int) -> str:
    chat_mem = get_chat_mem(memory, chat_id)
    participants = chat_mem.get("participants", {})
    p = participants.get(str(user_id))
    if not p:
        return "по этому персонажу пока нет данных"

    name = p.get("name") or p.get("username") or str(user_id)
    archetypes = _top_items(p.get("archetypes", {}), n=4)
    keywords = _top_items(p.get("keywords", {}), n=8)
    archetype_text = ", ".join(a for a, _ in archetypes) if archetypes else "нет"
    keyword_text = ", ".join(k for k, _ in keywords) if keywords else "нет"

    topic_links = []
    for edge, weight in chat_mem.get("topic_edges", {}).items():
        if edge.startswith(f"u:{user_id}|k:"):
            topic = edge.split("|", 1)[1].replace("k:", "", 1)
            topic_links.append((topic, float(weight)))
    topic_links.sort(key=lambda x: (-x[1], x[0]))

    rel = []
    for key, weight in chat_mem.get("user_relations", {}).items():
        if "|" not in key:
            continue
        a_str, b_str = key.split("|", 1)
        a, b = int(a_str), int(b_str)
        if user_id not in (a, b):
            continue
        other = b if a == user_id else a
        pdata = participants.get(str(other), {})
        other_name = pdata.get("name") or pdata.get("username") or str(other)
        rel.append((other_name, float(weight)))
    rel.sort(key=lambda x: (-x[1], x[0]))

    lines = [
        f"ассоциативная карта: {name}",
        f"сообщений: {p.get('message_count', 0)}",
        "архетипы: " + archetype_text,
        "ключевые слова: " + keyword_text,
    ]
    if topic_links:
        lines.append("сильные темы: " + ", ".join(t for t, _ in topic_links[:8]))
    if rel:
        lines.append("связи с людьми: " + ", ".join(n for n, _ in rel[:6]))
    return "\n".join(lines)


def _set_admin_pending(context: ContextTypes.DEFAULT_TYPE, payload: Dict[str, Any]) -> None:
    context.user_data["admin_pending"] = payload


def _clear_admin_pending(context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data.pop("admin_pending", None)


async def _run_git_pull() -> str:
    try:
        proc = await asyncio.to_thread(
            subprocess.run,
            ["git", "pull", "--rebase", "origin", "main"],
            cwd=str(ROOT_DIR),
            text=True,
            capture_output=True,
            timeout=60,
            check=False,
        )
        out = (proc.stdout or "").strip()
        err = (proc.stderr or "").strip()
        result = out if out else err
        if not result:
            result = "команда выполнена без вывода"
        if len(result) > 3000:
            result = result[:3000] + "\n...\n[обрезано]"
        return result
    except Exception as e:
        return f"ошибка обновления: {e}"


async def admin_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    del context
    if not _is_owner(update):
        return
    message = update.message
    if not message:
        return
    memory = load_memory()
    chat_id = message.chat_id
    await message.reply_text(
        _format_admin_status(memory, chat_id),
        reply_markup=_admin_main_keyboard(chat_id),
    )


async def _handle_admin_pending_text(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    memory: Dict[str, Any],
) -> bool:
    if not _is_owner(update):
        return False
    message = update.effective_message
    if not message:
        return False

    pending = context.user_data.get("admin_pending")
    if not pending:
        return False

    text = _extract_message_text(message)
    if not text:
        return True

    if text.strip().lower() == "/cancel":
        _clear_admin_pending(context)
        await message.reply_text("действие отменено")
        return True

    cfg = memory.setdefault("config", {})
    action = pending.get("action", "")

    if action == "system_prompt":
        cfg["system_prompt"] = text.strip()
        save_memory(memory)
        _clear_admin_pending(context)
        await message.reply_text("system prompt обновлен")
        return True

    if action == "style":
        cfg["style_settings"] = text.strip()
        save_memory(memory)
        _clear_admin_pending(context)
        await message.reply_text("style settings обновлены")
        return True

    if action == "bio":
        cfg["bio"] = text.strip()
        save_memory(memory)
        _clear_admin_pending(context)
        await message.reply_text("био обновлено")
        return True

    if action == "heat":
        try:
            heat = int(text.strip())
        except ValueError:
            await message.reply_text("нужно число 0..100 или /cancel")
            return True
        cfg["toxicity_level"] = max(0, min(100, heat))
        save_memory(memory)
        _clear_admin_pending(context)
        await message.reply_text(f"вредность обновлена: {cfg['toxicity_level']}")
        return True

    if action == "mode_override":
        mode = str(pending.get("mode", "default"))
        overrides = cfg.setdefault("mode_overrides", {})
        overrides[mode] = text.strip()
        save_memory(memory)
        _clear_admin_pending(context)
        await message.reply_text(f"кастомный текст для режима {mode} обновлен")
        return True

    _clear_admin_pending(context)
    await message.reply_text("неизвестное действие, сбросил ожидание")
    return True


async def admin_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or not _is_owner(update):
        if query:
            await query.answer("нет доступа", show_alert=True)
        return

    data = query.data or ""
    memory = load_memory()

    parts = data.split(":")
    if len(parts) < 3 or parts[0] != "adm":
        await query.answer()
        return

    action = parts[1]
    await query.answer()

    if action == "root":
        chat_id = int(parts[2])
        _clear_admin_pending(context)
        await query.edit_message_text(
            _format_admin_status(memory, chat_id),
            reply_markup=_admin_main_keyboard(chat_id),
        )
        return

    if action == "mode_menu":
        chat_id = int(parts[2])
        await query.edit_message_text(
            "выбери режим личности",
            reply_markup=_admin_mode_keyboard(chat_id, get_active_mode(memory)),
        )
        return

    if action == "mode_edit_menu":
        chat_id = int(parts[2])
        await query.edit_message_text(
            "выбери режим, для которого меняем кастомный текст",
            reply_markup=_admin_mode_edit_keyboard(chat_id),
        )
        return

    if action == "set_mode" and len(parts) >= 4:
        mode = parts[2]
        chat_id = int(parts[3])
        if mode not in PERSONA_MODES:
            await query.answer("неизвестный режим", show_alert=True)
            return
        memory.setdefault("config", {})["active_mode"] = mode
        save_memory(memory)
        await query.edit_message_text(
            f"режим переключен: {mode}",
            reply_markup=_admin_mode_keyboard(chat_id, mode),
        )
        return

    if action == "heat_menu":
        chat_id = int(parts[2])
        heat = get_toxicity_level(memory)
        await query.edit_message_text(
            f"текущая вредность: {heat}/100",
            reply_markup=_admin_heat_keyboard(chat_id, heat),
        )
        return

    if action == "heat_delta" and len(parts) >= 4:
        delta = int(parts[2])
        chat_id = int(parts[3])
        cfg = memory.setdefault("config", {})
        heat = get_toxicity_level(memory) + delta
        cfg["toxicity_level"] = max(0, min(100, heat))
        save_memory(memory)
        await query.edit_message_text(
            f"текущая вредность: {cfg['toxicity_level']}/100",
            reply_markup=_admin_heat_keyboard(chat_id, cfg["toxicity_level"]),
        )
        return

    if action == "heat_set" and len(parts) >= 4:
        val = int(parts[2])
        chat_id = int(parts[3])
        memory.setdefault("config", {})["toxicity_level"] = max(0, min(100, val))
        save_memory(memory)
        await query.edit_message_text(
            f"текущая вредность: {get_toxicity_level(memory)}/100",
            reply_markup=_admin_heat_keyboard(chat_id, get_toxicity_level(memory)),
        )
        return

    if action == "cloud_menu":
        chat_id = int(parts[2])
        await query.edit_message_text(
            "выбери персонажа для просмотра ассоциативного облака",
            reply_markup=_admin_cloud_users_keyboard(memory, chat_id),
        )
        return

    if action == "cloud_user" and len(parts) >= 4:
        user_id = int(parts[2])
        chat_id = int(parts[3])
        await query.edit_message_text(
            _format_cloud_for_user(memory, chat_id, user_id),
            reply_markup=InlineKeyboardMarkup(
                [
                    [InlineKeyboardButton("к списку", callback_data=f"adm:cloud_menu:{chat_id}")],
                    [InlineKeyboardButton("в главное меню", callback_data=f"adm:root:{chat_id}")],
                ]
            ),
        )
        return

    if action == "input":
        input_kind = parts[2]
        if input_kind == "mode" and len(parts) >= 5:
            mode = parts[3]
            chat_id = int(parts[4])
            _set_admin_pending(context, {"action": "mode_override", "mode": mode})
            await query.edit_message_text(
                f"пришли новый текст для режима {mode} одним сообщением\n/cancel чтобы отменить",
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("назад", callback_data=f"adm:mode_edit_menu:{chat_id}")]]
                ),
            )
            return

        chat_id = int(parts[3]) if len(parts) >= 4 else query.message.chat_id
        if input_kind in {"system_prompt", "style", "bio", "heat"}:
            _set_admin_pending(context, {"action": input_kind})
            hints = {
                "system_prompt": "пришли новый system prompt одним сообщением",
                "style": "пришли новый style settings одним сообщением",
                "bio": "пришли новое био тимура одним сообщением",
                "heat": "пришли число 0..100",
            }
            await query.edit_message_text(
                hints[input_kind] + "\n/cancel чтобы отменить",
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("назад", callback_data=f"adm:root:{chat_id}")]]
                ),
            )
            return

    if action == "update_pull":
        chat_id = int(parts[2])
        await query.edit_message_text("обновляю из github, пару секунд...")
        pull_result = await _run_git_pull()
        await query.edit_message_text(
            "результат обновления:\n" + pull_result,
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("в меню", callback_data=f"adm:root:{chat_id}")]]
            ),
        )
        return

    await query.answer("непонятная команда панели", show_alert=True)


# =========================
# TELEGRAM HANDLERS
# =========================

async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    del context
    if update.message:
        await update.message.reply_text("я тут давай базарь")


async def text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    memory = load_memory()
    message = update.effective_message

    if not message or not message.from_user:
        return

    if await _handle_admin_pending_text(update, context, memory):
        return

    logger.info(
        "Text from %s (%s) in chat %s: %s",
        message.from_user.id,
        message.from_user.username,
        message.chat_id,
        message.text,
    )

    update_memory_with_message(memory, message)

    bot_id = (await context.bot.get_me()).id

    if not should_reply(memory, message, bot_id):
        logger.info("Decided not to reply")
        return

    messages = build_chat_messages(memory, message)
    reply_text = await call_openai_text(messages)

    force_voice = is_voice_codeword(_extract_message_text(message))
    await send_reply_with_style(update, context, memory, reply_text, force_voice=force_voice)


async def photo_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    memory = load_memory()
    message = update.effective_message

    if not message or not message.from_user:
        return

    user = message.from_user
    chat_id = message.chat_id

    logger.info(
        "Photo from %s (%s) in chat %s",
        user.id,
        user.username,
        chat_id,
    )

    update_memory_with_message(memory, message)

    bot_id = (await context.bot.get_me()).id

    must_reply = False

    if message.reply_to_message and message.reply_to_message.from_user:
        if message.reply_to_message.from_user.id == bot_id:
            must_reply = True

    if (message.caption or "") and is_name_mentioned(message.caption):
        must_reply = True

    if not must_reply and random.random() < PHOTO_RANDOM_REPLY_CHANCE:
        must_reply = True

    if not must_reply:
        logger.info("Decided not to reply to photo")
        return

    if not can_use_vision(memory, chat_id, user.id):
        logger.info("Vision limit exceeded, skipping vision")
        return

    photo = message.photo[-1]
    file = await context.bot.get_file(photo.file_id)
    file_bytes = await file.download_as_bytearray()
    image_b64 = base64.b64encode(file_bytes).decode("utf-8")

    increase_vision_counters(memory, chat_id, user.id)

    reply_text = await call_openai_vision(memory, message, image_b64)
    reply_text = sanitize_reply_text(reply_text)

    if not reply_text:
        logger.info("Empty vision reply")
        return

    await message.reply_text(reply_text)


# =========================
# АДМИН-КОМАНДЫ
# =========================

def owner_only(func):
    @functools.wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        user = update.effective_user

        if not user or user.id != OWNER_ID:
            logger.warning("Unauthorized admin command from %s", user.id if user else "unknown")
            return

        return await func(update, context)

    return wrapper


def _fmt_rub(v: int) -> str:
    return f"{int(v)}₽"


def _parse_int(raw: str, default: int) -> int:
    try:
        return int(raw)
    except Exception:
        return default


def _format_invoice_rows(invoices: List[Dict[str, Any]]) -> str:
    if not invoices:
        return "инвойсов нет"
    lines = []
    for inv in invoices:
        lines.append(
            f"- {inv['invoice_id']} | user={inv['payer_user_id']} | {_fmt_rub(inv['amount_rub'])} | {inv['status']}"
        )
    return "\n".join(lines)


@owner_only
async def billhelp_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    del context
    if not update.message:
        return
    text = (
        "billing команды:\n"
        "/billquote <owner|split|free> [stars|yookassa] [payer_count]\n"
        "/billsetup <owner|split|free> [stars|yookassa] [payer_count] [standard|plus|free]\n"
        "/billstatus — статус биллинга чата\n"
        "/billinvoices — последние инвойсы\n"
        "/billpay <invoice_id> — мок оплата (можно не owner)\n"
        "/billabuse — отчет по антиабузу\n"
        "/billref create [commission_pct] [months]\n"
        "/billref apply <CODE>\n"
        "/billref balance [user_id]"
    )
    await update.message.reply_text(text)


@owner_only
async def billquote_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    del context
    message = update.message
    if not message:
        return

    parts = (message.text or "").strip().split()
    mode = parts[1] if len(parts) > 1 else "split"
    provider = parts[2] if len(parts) > 2 else "stars"
    payer_count = _parse_int(parts[3], 0) if len(parts) > 3 else 0

    try:
        quote = billing.get_quote(
            chat_id=message.chat_id,
            mode=mode,
            provider=provider,
            payer_count=(payer_count if payer_count > 0 else None),
        )
        payers = quote.payer_ids
        active = quote.active_users
        lines = [
            f"quote for chat={quote.chat_id}",
            f"mode={quote.mode}, provider={quote.provider}",
            f"active users={len(active)}: {', '.join(str(x) for x in active[:20]) or '-'}",
            f"total={_fmt_rub(quote.total_rub)} (per_active={_fmt_rub(quote.price_per_active_rub)}, min={_fmt_rub(quote.min_price_rub)}, max={_fmt_rub(quote.max_price_rub)})",
            f"payers={len(payers)}: {', '.join(str(x) for x in payers[:20]) or '-'}",
            f"activation ratio={quote.activation_ratio}",
        ]
        await message.reply_text("\n".join(lines))
    except BillingError as e:
        await message.reply_text(f"billing error: {e}")


@owner_only
async def billsetup_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    del context
    message = update.message
    if not message or not message.from_user:
        return

    parts = (message.text or "").strip().split()
    mode = parts[1] if len(parts) > 1 else "split"
    provider = parts[2] if len(parts) > 2 else "stars"
    payer_count = _parse_int(parts[3], 0) if len(parts) > 3 else 0
    tier_key = (parts[4] if len(parts) > 4 else "standard").lower()
    tier = "group_plus" if tier_key == "plus" else "free_promo" if tier_key == "free" else "group_standard"

    try:
        result = billing.create_subscription_cycle(
            chat_id=message.chat_id,
            initiated_by_user_id=message.from_user.id,
            mode=mode,
            provider=provider,
            payer_count=(payer_count if payer_count > 0 else None),
            tier=tier,
        )
    except BillingError as e:
        await message.reply_text(f"billing error: {e}")
        return

    sub = result["subscription"]
    invoices = result["invoices"]
    lines = [
        f"subscription created: {sub['subscription_id']}",
        f"mode={sub['mode']}, provider={sub['provider']}, tier={sub['tier']}",
        f"status={sub['status']}, total={_fmt_rub(sub['total_rub'])}",
        f"payers={len(sub['payer_ids'])}, invoices={len(invoices)}",
    ]
    if invoices:
        lines.append("invoices:")
        lines.append(_format_invoice_rows(invoices))
    await message.reply_text("\n".join(lines))


@owner_only
async def billstatus_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    del context
    message = update.message
    if not message:
        return

    summary = billing.get_chat_activity_summary(message.chat_id)
    ent = summary.get("entitlement")
    active_ids = summary.get("active_users", [])
    lines = [
        f"billing status chat={message.chat_id}",
        f"active users={summary.get('active_count', 0)}: {', '.join(str(x) for x in active_ids[:20]) or '-'}",
    ]
    if ent:
        lines.extend(
            [
                f"entitlement status={ent.get('status')}",
                f"tier={ent.get('tier')}, mode={ent.get('mode')}, provider={ent.get('provider')}",
                f"expires_at={ent.get('expires_at')}",
            ]
        )
    else:
        lines.append("entitlement: none")
    await message.reply_text("\n".join(lines))


@owner_only
async def billinvoices_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    del context
    message = update.message
    if not message:
        return
    invoices = billing.list_chat_invoices(message.chat_id, limit=20)
    await message.reply_text(_format_invoice_rows(invoices))


async def billpay_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    del context
    message = update.message
    if not message or not message.from_user:
        return

    parts = (message.text or "").strip().split()
    if len(parts) < 2:
        await message.reply_text("использование: /billpay <invoice_id>")
        return
    invoice_id = parts[1].strip()

    try:
        result = billing.pay_invoice_mock(
            invoice_id=invoice_id,
            paid_by_user_id=message.from_user.id,
        )
    except BillingError as e:
        await message.reply_text(f"billing error: {e}")
        return

    invoice = result["invoice"]
    sub = result["subscription"] or {}
    lines = [
        f"invoice paid: {invoice['invoice_id']}",
        f"amount={_fmt_rub(invoice['amount_rub'])}, status={invoice['status']}",
        f"subscription={sub.get('subscription_id')} status={sub.get('status')}",
    ]
    if sub.get("status") == "active":
        lines.append(f"activated until {sub.get('expires_at')}")
    await message.reply_text("\n".join(lines))


@owner_only
async def billabuse_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    del context
    message = update.message
    if not message:
        return
    rep = billing.get_abuse_report(chat_id=message.chat_id, limit=10)
    today_map = rep.get("invoice_batches_today", {})
    lines = [
        f"abuse report today={rep.get('today')}",
        f"invoice batches today (all chats): {today_map}",
    ]
    flags = rep.get("flags", [])
    if not flags:
        lines.append("flags: none")
    else:
        lines.append("flags:")
        for f in flags:
            lines.append(f"- {f.get('ts')} | {f.get('kind')} | {f.get('details')}")
    await message.reply_text("\n".join(lines))


@owner_only
async def billref_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    del context
    message = update.message
    if not message or not message.from_user:
        return

    parts = (message.text or "").strip().split()
    action = parts[1].lower() if len(parts) > 1 else "help"

    if action == "create":
        pct = _parse_int(parts[2], 10) if len(parts) > 2 else 10
        months = _parse_int(parts[3], 3) if len(parts) > 3 else 3
        try:
            prg = billing.create_affiliate_program(
                owner_user_id=message.from_user.id,
                commission_pct=pct,
                duration_months=months,
            )
            await message.reply_text(
                f"ref program created\nprogram_id={prg['program_id']}\ncode={prg['code']}\ncommission={prg['commission_pct']}%\nduration={prg['duration_months']}m"
            )
        except BillingError as e:
            await message.reply_text(f"billing error: {e}")
        return

    if action == "apply":
        if len(parts) < 3:
            await message.reply_text("использование: /billref apply <CODE>")
            return
        code = parts[2].strip()
        try:
            ap = billing.apply_referral_code(message.from_user.id, code)
            await message.reply_text(
                f"ref applied\nprogram={ap['program_id']}\nexpires_at={ap['expires_at']}"
            )
        except BillingError as e:
            await message.reply_text(f"billing error: {e}")
        return

    if action == "balance":
        uid = _parse_int(parts[2], message.from_user.id) if len(parts) > 2 else message.from_user.id
        bal = billing.get_affiliate_balance(uid)
        await message.reply_text(f"affiliate balance user={uid}: {_fmt_rub(bal)}")
        return

    await message.reply_text("использование: /billref create [pct] [months] | /billref apply <CODE> | /billref balance [user_id]")


@owner_only
async def setprompt_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    del context
    if not update.message:
        return

    parts = (update.message.text or "").split(" ", 1)

    if len(parts) < 2:
        await update.message.reply_text("после /setprompt должен быть текст")
        return

    memory = load_memory()
    memory.setdefault("config", {})["system_prompt"] = parts[1].strip()
    save_memory(memory)

    await update.message.reply_text("system_prompt обновлен")


@owner_only
async def appendprompt_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    del context
    if not update.message:
        return

    parts = (update.message.text or "").split(" ", 1)

    if len(parts) < 2:
        await update.message.reply_text("после /appendprompt должен быть текст")
        return

    memory = load_memory()
    cfg = memory.setdefault("config", {})
    base_prompt = cfg.get("system_prompt") or DEFAULT_SYSTEM_PROMPT
    cfg["system_prompt"] = base_prompt + "\n" + parts[1].strip()
    save_memory(memory)

    await update.message.reply_text("дописал в system_prompt")


@owner_only
async def showprompt_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    del context
    if not update.message:
        return

    memory = load_memory()
    prompt = get_system_prompt(memory)

    if len(prompt) > 3500:
        prompt = prompt[:3500] + "\n...\n[обрезано]"

    await update.message.reply_text(prompt)


@owner_only
async def resetprompt_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    del context
    if not update.message:
        return

    memory = load_memory()
    memory.setdefault("config", {})["system_prompt"] = DEFAULT_SYSTEM_PROMPT
    save_memory(memory)

    await update.message.reply_text("system_prompt сброшен на дефолт")


@owner_only
async def setbio_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    del context
    if not update.message:
        return

    parts = (update.message.text or "").split(" ", 1)

    if len(parts) < 2:
        await update.message.reply_text("после /setbio должен быть текст")
        return

    memory = load_memory()
    memory.setdefault("config", {})["bio"] = parts[1].strip()
    save_memory(memory)

    await update.message.reply_text("био тимура обновлено")


@owner_only
async def setstyle_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    del context
    if not update.message:
        return

    parts = (update.message.text or "").split(" ", 1)

    if len(parts) < 2:
        await update.message.reply_text("после /setstyle должен быть текст")
        return

    memory = load_memory()
    memory.setdefault("config", {})["style_settings"] = parts[1].strip()
    save_memory(memory)

    await update.message.reply_text("style_settings обновлены")


@owner_only
async def setheat_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    del context
    if not update.message:
        return

    parts = (update.message.text or "").split(" ", 1)
    if len(parts) < 2:
        await update.message.reply_text("использование: /setheat 0..100")
        return

    try:
        val = int(parts[1].strip())
    except ValueError:
        await update.message.reply_text("нужно целое число 0..100")
        return

    val = max(0, min(100, val))
    memory = load_memory()
    memory.setdefault("config", {})["toxicity_level"] = val
    save_memory(memory)

    await update.message.reply_text(f"уровень прожарки теперь {val}")


@owner_only
async def remember_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    del context
    if not update.message:
        return

    parts = (update.message.text or "").split(" ", 1)

    if len(parts) < 2:
        await update.message.reply_text("после /remember должен быть текст события")
        return

    memory = load_memory()

    if update.message.reply_to_message and update.message.reply_to_message.from_user:
        target_user = update.message.reply_to_message.from_user
    else:
        target_user = update.effective_user

    if not target_user:
        await update.message.reply_text("не смог определить пользователя")
        return

    user_mem = get_user_mem(memory, target_user.id)
    user_mem.setdefault("events", []).append({
        "event": parts[1].strip(),
        "raw": update.message.text,
        "ts": datetime.utcnow().isoformat(),
    })

    save_memory(memory)

    await update.message.reply_text(f"событие запомнил для user_id={target_user.id}")


@owner_only
async def whois_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    del context
    message = update.message
    if not message:
        return

    memory = load_memory()
    chat_mem = get_chat_mem(memory, message.chat_id)

    if message.reply_to_message and message.reply_to_message.from_user:
        target = message.reply_to_message.from_user
    else:
        target = message.from_user

    if not target:
        return

    p = chat_mem.get("participants", {}).get(str(target.id))
    if not p:
        await message.reply_text("по нему пока нет портрета")
        return

    archetypes = _top_items(p.get("archetypes", {}), n=3)
    keywords = _top_items(p.get("keywords", {}), n=6)
    style = p.get("style", {})

    rel = []
    for k, w in chat_mem.get("user_relations", {}).items():
        if "|" not in k:
            continue
        a_str, b_str = k.split("|", 1)
        a, b = int(a_str), int(b_str)
        if target.id not in (a, b):
            continue
        other = b if a == target.id else a
        pdata = chat_mem.get("participants", {}).get(str(other), {})
        name = pdata.get("name") or pdata.get("username") or str(other)
        rel.append((name, float(w)))
    rel.sort(key=lambda x: (-x[1], x[0]))

    lines = [
        f"портрет: {p.get('name') or p.get('username') or target.id}",
        f"сообщений: {p.get('message_count', 0)}",
        "архетипы: " + (", ".join(a for a, _ in archetypes) if archetypes else "нет"),
        "темы: " + (", ".join(k for k, _ in keywords) if keywords else "нет"),
        f"стиль: мат={style.get('profanity', 0)} вопросы={style.get('questions', 0)} капс={style.get('caps', 0)}",
    ]

    if rel:
        lines.append("с кем чаще сцепляется: " + ", ".join(name for name, _ in rel[:4]))

    await message.reply_text("\n".join(lines))


@owner_only
async def dump_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    del context
    if not update.message:
        return

    memory = load_memory()

    short = {
        "chats": list(memory.get("chats", {}).keys())[:20],
        "users": list(memory.get("users", {}).keys())[:20],
        "config": memory.get("config", {}),
    }

    text = json.dumps(short, ensure_ascii=False, indent=2)

    if len(text) > 3500:
        text = text[:3500] + "\n...\n[обрезано]"

    await update.message.reply_text(f"memory dump:\n{text}")


@owner_only
async def clearmemory_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    del context
    if not update.message:
        return

    memory = default_memory()
    save_memory(memory)

    await update.message.reply_text("memory.json очищен")


# =========================
# MAIN
# =========================

def main() -> None:
    from timur_bot.app.runner import main as run_main

    run_main()


if __name__ == "__main__":
    main()
