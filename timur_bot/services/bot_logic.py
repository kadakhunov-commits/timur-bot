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
import weakref
from urllib.parse import urlencode, urlsplit, urlunsplit, parse_qsl
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Tuple
from zoneinfo import ZoneInfo

from billing_system import BillingEngine, BillingError
from openai import OpenAI
from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InputFile,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
    Update,
    WebAppInfo,
)
from telegram.constants import ChatAction
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
from timur_bot.services.funny_scan_admin import (
    format_funny_candidate_preview,
    format_funny_candidates_list,
    format_funny_sources,
    format_funny_status,
)
from timur_bot.services.funny_scan_llm import evaluate_candidate_with_llm
from timur_bot.services.funny_scan_pipeline import build_stage1_candidates, extract_period_messages
from timur_bot.services.funny_scan_storage import (
    STATUS_APPROVED,
    STATUS_NEW,
    STATUS_REJECTED,
    STATUS_SENT,
    add_candidate,
    apply_intensity_profile,
    apply_reaction_delta,
    ensure_budget_day,
    ensure_funny_scan_config,
    get_candidate,
    hard_budget_reached,
    has_candidate_signature,
    list_candidates,
    load_state,
    register_forward_usage,
    register_token_usage,
    save_state,
    set_candidate_status,
    set_preview_sent,
    soft_budget_ratio,
    toggle_source,
    update_last_scan,
    upsert_source,
)
from timur_bot.services.humor import (
    add_joke_bit,
    apply_feedback,
    choose_humor_plan,
    classify_reactions,
    classify_text_feedback,
    ensure_humor_schema,
    format_bits,
    format_humor_prompt,
    record_bot_output,
)

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
MINIAPP_URL = APP_CONFIG.miniapp_url

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
FUNNY_SCAN_RUNTIME_DEFAULTS = APP_CONFIG.funny_scan_defaults
FUNNY_SCAN_LEXICON = APP_CONFIG.funny_scan_lexicon
RECENT_FACT_WINDOW_DAYS = 14
MAX_RECENT_MESSAGES = 24
MAX_RECENT_FACTS = 120
MAX_LONG_FACTS = 400
TOXIC_REPLY_PATTERNS = (
    re.compile(r"\b(дебил|идиот|туп(ой|ая)|ничтож|чмо|мразь)\b", re.IGNORECASE),
    re.compile(r"\b(stupid|idiot|moron)\b", re.IGNORECASE),
)
PROCESSED_EVENT_KEYS_LIMIT = 400
LIFE_STORY_LOG_LIMIT = 80
LIFE_LOOP_INTERVAL_SECONDS = 60
DEFAULT_LIFE_TIMEZONE = "Europe/Moscow"
LONG_FACT_USAGE_TRACK_LIMIT = 600
FUNNY_SCAN_STATE_PATH = ROOT_DIR / "data" / "funny_scan_state.json"
FUNNY_SCAN_LOOP_INTERVAL_SECONDS = 60
_INFLIGHT_EVENT_KEYS: set[str] = set()
_LIFE_TASK: asyncio.Task[Any] | None = None
_FUNNY_SCAN_TASK: asyncio.Task[Any] | None = None
_FUNNY_SCAN_LOCK = asyncio.Lock()
_FUNNY_SCAN_STATE_LOCK = asyncio.Lock()
_FUNNY_FORWARD_LOCKS: weakref.WeakValueDictionary[str, asyncio.Lock] = weakref.WeakValueDictionary()

# =========================
# ЛОГИ
# =========================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("timur-bot")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
billing = BillingEngine(BILLING_PATH, logger=logger)


@dataclass
class ReplyDecision:
    should_reply: bool
    reason: str
    threshold: float | None = None
    roll: float | None = None


def _log_reply_decision(kind: str, decision: ReplyDecision) -> None:
    if decision.threshold is not None and decision.roll is not None:
        logger.info(
            "Решение по %s: %s | причина=%s | шанс=%.2f | бросок=%.2f",
            kind,
            "ОТВЕЧАЮ" if decision.should_reply else "ПРОПУСКАЮ",
            decision.reason,
            decision.threshold,
            decision.roll,
        )
        return

    logger.info(
        "Решение по %s: %s | причина=%s",
        kind,
        "ОТВЕЧАЮ" if decision.should_reply else "ПРОПУСКАЮ",
        decision.reason,
    )


# =========================
# SYSTEM PROMPT
# =========================

DEFAULT_SYSTEM_PROMPT = APP_CONFIG.default_system_prompt


# =========================
# ПАМЯТЬ
# =========================


def _default_life_config() -> Dict[str, Any]:
    return {
        "enabled": True,
        "timezone": DEFAULT_LIFE_TIMEZONE,
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
    }


def _ensure_life_config(cfg: Dict[str, Any]) -> Dict[str, Any]:
    life = cfg.setdefault("life", {})
    defaults = _default_life_config()
    for key, value in defaults.items():
        if key == "quiet_hours":
            quiet = life.setdefault("quiet_hours", {})
            quiet.setdefault("start", value["start"])
            quiet.setdefault("end", value["end"])
            continue
        if key in {"daily_slots", "sent_slots", "story_log"}:
            current = life.get(key)
            if not isinstance(current, list):
                life[key] = list(value)
            continue
        if key == "chat_last_emit":
            current = life.get(key)
            if not isinstance(current, dict):
                life[key] = dict(value)
            continue
        life.setdefault(key, value)
    return life


def _ensure_funny_scan_config(cfg: Dict[str, Any]) -> Dict[str, Any]:
    return ensure_funny_scan_config(
        cfg,
        owner_id=OWNER_ID,
        runtime_defaults=FUNNY_SCAN_RUNTIME_DEFAULTS,
    )


def default_memory() -> Dict[str, Any]:
    default_life = _default_life_config()
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
            "life": default_life,
            "funny_scan": _ensure_funny_scan_config({}).copy(),
        },
    }


def _ensure_chat_schema(chat: Dict[str, Any]) -> Dict[str, Any]:
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
    layers.setdefault("processed_event_keys", [])
    ensure_humor_schema(chat)
    return chat


def _parse_iso_ts(ts: str) -> datetime | None:
    raw = (ts or "").strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).replace(tzinfo=None)
    except ValueError:
        return None


def _norm_fact_key(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip().lower())


def _upsert_long_fact(chat_mem: Dict[str, Any], fact_text: str, ts: str, boost: float = 1.0) -> None:
    layers = chat_mem.setdefault("memory_layers", {})
    long_facts = layers.setdefault("long_facts", [])
    key = _norm_fact_key(fact_text)
    if not key:
        return

    for fact in long_facts:
        if _norm_fact_key(str(fact.get("text", ""))) == key:
            fact["strength"] = float(fact.get("strength", 0.0)) + float(boost)
            fact["last_seen_ts"] = ts
            break
    else:
        long_facts.append(
            {
                "text": fact_text,
                "last_seen_ts": ts,
                "strength": float(boost),
            }
        )

    long_facts.sort(key=lambda x: (-float(x.get("strength", 0.0)), str(x.get("text", ""))))
    if len(long_facts) > MAX_LONG_FACTS:
        del long_facts[MAX_LONG_FACTS:]


def _compact_memory_layers(chat_mem: Dict[str, Any], now_dt: datetime | None = None) -> None:
    layers = chat_mem.setdefault("memory_layers", {})
    recent_messages = layers.setdefault("recent_messages", [])
    recent_facts = layers.setdefault("recent_facts", [])
    now = now_dt or datetime.utcnow()

    if len(recent_messages) > MAX_RECENT_MESSAGES:
        del recent_messages[:-MAX_RECENT_MESSAGES]

    kept_facts = []
    for fact in recent_facts:
        ts = _parse_iso_ts(str(fact.get("ts", "")))
        if not ts:
            continue
        age_days = (now - ts).days
        if age_days > RECENT_FACT_WINDOW_DAYS:
            _upsert_long_fact(
                chat_mem,
                fact_text=str(fact.get("text", "")),
                ts=str(fact.get("ts", "")),
                boost=float(fact.get("weight", 1.0)),
            )
            continue
        kept_facts.append(fact)

    kept_facts.sort(key=lambda x: str(x.get("ts", "")))
    if len(kept_facts) > MAX_RECENT_FACTS:
        kept_facts = kept_facts[-MAX_RECENT_FACTS:]
    layers["recent_facts"] = kept_facts


def _update_memory_layers_with_message(chat_mem: Dict[str, Any], rec: Dict[str, Any]) -> None:
    layers = chat_mem.setdefault("memory_layers", {})
    recent_messages = layers.setdefault("recent_messages", [])
    recent_facts = layers.setdefault("recent_facts", [])

    recent_messages.append(
        {
            "user_id": rec.get("user_id"),
            "name": rec.get("name", ""),
            "username": rec.get("username", ""),
            "text": rec.get("text", ""),
            "ts": rec.get("ts", ""),
            "message_id": rec.get("message_id"),
        }
    )
    if len(recent_messages) > MAX_RECENT_MESSAGES:
        del recent_messages[:-MAX_RECENT_MESSAGES]

    text = str(rec.get("text", "")).strip()
    if text:
        recent_facts.append(
            {
                "text": f"{rec.get('name') or rec.get('username') or rec.get('user_id')}: {text}",
                "ts": rec.get("ts", ""),
                "weight": 1.0,
            }
        )

    _compact_memory_layers(chat_mem)


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
        _ensure_life_config(cfg)
        _ensure_funny_scan_config(cfg)

        for _, chat in data["chats"].items():
            _ensure_chat_schema(chat)

        return data

    except Exception as e:
        logger.error("Не удалось загрузить memory.json: %s", e)
        return default_memory()


def save_memory(memory: Dict[str, Any]) -> None:
    try:
        with open(MEMORY_PATH, "w", encoding="utf-8") as f:
            json.dump(memory, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error("Не удалось сохранить memory.json: %s", e)


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


def _get_funny_scan_settings(memory: Dict[str, Any]) -> Dict[str, Any]:
    cfg = memory.setdefault("config", {})
    return _ensure_funny_scan_config(cfg)


def _load_funny_scan_state() -> Dict[str, Any]:
    state = load_state(FUNNY_SCAN_STATE_PATH)
    ensure_budget_day(state)
    return state


def _save_funny_scan_state(state: Dict[str, Any]) -> None:
    save_state(FUNNY_SCAN_STATE_PATH, state)


def _known_scan_sources(memory: Dict[str, Any], settings: Dict[str, Any]) -> List[Dict[str, Any]]:
    by_id: Dict[int, Dict[str, Any]] = {}
    chats = memory.get("chats", {})
    if isinstance(chats, dict):
        for chat_id_raw, chat_mem in chats.items():
            try:
                chat_id = int(chat_id_raw)
            except Exception:
                continue
            history = chat_mem.get("history", []) if isinstance(chat_mem, dict) else []
            if not isinstance(history, list) or not history:
                continue
            by_id[chat_id] = {"chat_id": chat_id, "title": f"chat {chat_id}"}
    for source in settings.get("sources", []) if isinstance(settings.get("sources"), list) else []:
        if not isinstance(source, dict):
            continue
        chat_id = int(source.get("chat_id", 0))
        if not chat_id:
            continue
        by_id.setdefault(chat_id, {"chat_id": chat_id, "title": str(source.get("title") or f"chat {chat_id}")})
        if source.get("title"):
            by_id[chat_id]["title"] = str(source.get("title"))
    return sorted(by_id.values(), key=lambda x: x["chat_id"])


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


def _make_event_key(kind: str, chat_id: int, message_id: int) -> str:
    return f"{kind}:{chat_id}:{message_id}"


def _try_acquire_inflight_event(event_key: str) -> bool:
    if event_key in _INFLIGHT_EVENT_KEYS:
        return False
    _INFLIGHT_EVENT_KEYS.add(event_key)
    return True


def _release_inflight_event(event_key: str) -> None:
    _INFLIGHT_EVENT_KEYS.discard(event_key)


def _is_processed_event(chat_mem: Dict[str, Any], event_key: str) -> bool:
    layers = chat_mem.get("memory_layers", {})
    processed = layers.get("processed_event_keys", [])
    return isinstance(processed, list) and event_key in processed


def _mark_processed_event(chat_mem: Dict[str, Any], event_key: str) -> None:
    layers = chat_mem.setdefault("memory_layers", {})
    processed = layers.setdefault("processed_event_keys", [])
    if not isinstance(processed, list):
        processed = []
        layers["processed_event_keys"] = processed
    if event_key in processed:
        return
    processed.append(event_key)
    if len(processed) > PROCESSED_EVENT_KEYS_LIMIT:
        del processed[:-PROCESSED_EVENT_KEYS_LIMIT]


def _safe_zoneinfo(name: str) -> ZoneInfo:
    try:
        return ZoneInfo(name)
    except Exception:
        return ZoneInfo(DEFAULT_LIFE_TIMEZONE)


def _parse_hhmm_to_minute(raw: str, fallback: int) -> int:
    try:
        hh_str, mm_str = str(raw).strip().split(":", 1)
        hh = int(hh_str)
        mm = int(mm_str)
        if 0 <= hh <= 23 and 0 <= mm <= 59:
            return hh * 60 + mm
    except Exception:
        pass
    return fallback


def _minute_to_hhmm(minute: int) -> str:
    m = max(0, min(24 * 60 - 1, int(minute)))
    hh, mm = divmod(m, 60)
    return f"{hh:02d}:{mm:02d}"


def _is_quiet_minute(minute: int, start_minute: int, end_minute: int) -> bool:
    if start_minute == end_minute:
        return True
    if start_minute < end_minute:
        return start_minute <= minute < end_minute
    return minute >= start_minute or minute < end_minute


def _generate_daily_slots(life: Dict[str, Any], day_seed: int) -> List[int]:
    target = max(1, int(life.get("daily_target", 3)))
    quiet = life.get("quiet_hours", {}) if isinstance(life.get("quiet_hours"), dict) else {}
    quiet_start = _parse_hhmm_to_minute(quiet.get("start", "00:00"), 0)
    quiet_end = _parse_hhmm_to_minute(quiet.get("end", "10:00"), 10 * 60)

    allowed = [m for m in range(24 * 60) if not _is_quiet_minute(m, quiet_start, quiet_end)]
    if not allowed:
        return []
    if target >= len(allowed):
        return sorted(allowed)
    rng = random.Random(day_seed)
    return sorted(rng.sample(allowed, k=target))


def _refresh_life_daily_state(life: Dict[str, Any], now_local: datetime) -> None:
    date_key = now_local.date().isoformat()
    if str(life.get("slots_date", "")) == date_key:
        return
    seed = int(now_local.strftime("%Y%m%d"))
    life["slots_date"] = date_key
    life["daily_slots"] = _generate_daily_slots(life, day_seed=seed)
    life["sent_slots"] = []


def _append_story_log(memory: Dict[str, Any], text: str, *, source: str, chat_id: int | None) -> Dict[str, Any]:
    cfg = memory.setdefault("config", {})
    life = _ensure_life_config(cfg)
    story_id = int(life.get("last_story_id", 0)) + 1
    life["last_story_id"] = story_id
    entry = {
        "id": story_id,
        "text": text,
        "source": source,
        "chat_id": chat_id,
        "ts": datetime.utcnow().isoformat(),
    }
    log = life.setdefault("story_log", [])
    if not isinstance(log, list):
        log = []
        life["story_log"] = log
    log.append(entry)
    if len(log) > LIFE_STORY_LOG_LIMIT:
        del log[:-LIFE_STORY_LOG_LIMIT]
    return entry


def _get_last_story(memory: Dict[str, Any], *, chat_id: int | None = None) -> Dict[str, Any] | None:
    cfg = memory.setdefault("config", {})
    life = _ensure_life_config(cfg)
    log = life.get("story_log", [])
    if not isinstance(log, list) or not log:
        return None
    if chat_id is None:
        return log[-1]
    for entry in reversed(log):
        if int(entry.get("chat_id") or 0) == int(chat_id):
            return entry
    return log[-1]


def _looks_like_story_request(text: str) -> bool:
    clean = re.sub(r"\s+", " ", str(text or "")).strip().lower()
    if not clean:
        return False
    hints = (
        "расскажи историю",
        "расскажи че было",
        "что у тебя было",
        "че у тебя было",
        "историю расскажи",
        "что было",
    )
    return any(h in clean for h in hints)


def _select_proactive_chat_id(memory: Dict[str, Any], life: Dict[str, Any]) -> int | None:
    chats = memory.get("chats", {})
    if not isinstance(chats, dict) or not chats:
        return None
    cooldown_minutes = max(1, int(life.get("cooldown_per_chat_minutes", 360)))
    cooldown_delta = timedelta(minutes=cooldown_minutes)
    now_utc = datetime.utcnow()
    chat_last_emit = life.get("chat_last_emit", {})
    if not isinstance(chat_last_emit, dict):
        chat_last_emit = {}
        life["chat_last_emit"] = chat_last_emit

    eligible: List[Tuple[int, float]] = []
    for raw_chat_id, chat_mem in chats.items():
        try:
            chat_id = int(raw_chat_id)
        except Exception:
            continue
        last_emit_raw = str(chat_last_emit.get(str(chat_id), "")).strip()
        if last_emit_raw:
            last_emit_ts = _parse_iso_ts(last_emit_raw)
            if last_emit_ts and now_utc - last_emit_ts < cooldown_delta:
                continue
        history = chat_mem.get("history", []) if isinstance(chat_mem, dict) else []
        score = float(len(history[-40:])) if isinstance(history, list) else 0.0
        eligible.append((chat_id, max(1.0, score)))

    if not eligible:
        return None
    total = sum(weight for _, weight in eligible)
    roll = random.random() * total
    cumulative = 0.0
    for chat_id, weight in eligible:
        cumulative += weight
        if roll <= cumulative:
            return chat_id
    return eligible[-1][0]

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

    _update_memory_layers_with_message(chat_mem, rec)

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
        logger.error("Ошибка обновления активности в биллинге: %s", e)


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


def should_reply_decision(memory: Dict[str, Any], message: Message, bot_id: int) -> ReplyDecision:
    del memory
    if not message.text and not message.caption:
        return ReplyDecision(False, "нет текста или подписи")

    text = _extract_message_text(message)
    tg_user = message.from_user

    if not tg_user:
        return ReplyDecision(False, "не удалось определить автора сообщения")

    if tg_user.id == bot_id:
        return ReplyDecision(False, "сообщение от самого бота")

    if message.reply_to_message and message.reply_to_message.from_user:
        if message.reply_to_message.from_user.id == bot_id:
            return ReplyDecision(True, "прямой ответ на сообщение Тимура")

    if is_name_mentioned(text):
        return ReplyDecision(True, "в тексте упомянуто имя Тимура")

    if looks_like_address_to_bot(text):
        chance = random.uniform(0.75, 1.0)
        roll = random.random()
        return ReplyDecision(
            roll < chance,
            "сообщение похоже на обращение к Тимуру",
            threshold=chance,
            roll=roll,
        )

    roll = random.random()
    return ReplyDecision(
        roll < BASE_REPLY_CHANCE,
        "обычный случай, применён базовый шанс ответа",
        threshold=BASE_REPLY_CHANCE,
        roll=roll,
    )


def should_reply(memory: Dict[str, Any], message: Message, bot_id: int) -> bool:
    return should_reply_decision(memory, message, bot_id).should_reply


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
    default_heat = int(APP_CONFIG.default_toxicity_level)
    raw = cfg.get("toxicity_level", default_heat)
    try:
        val = int(raw)
    except Exception:
        val = default_heat
    return max(0, min(100, val))


def get_effective_toxicity_level(memory: Dict[str, Any]) -> int:
    base = get_toxicity_level(memory)
    mode = get_active_mode(memory)
    if mode == "chill":
        return min(base, 8)
    if mode == "default":
        return min(base, 20)
    return base


def is_blocked_memory_text(text: str) -> bool:
    del text
    return False


def looks_like_memory_request(text: str) -> bool:
    clean = re.sub(r"\s+", " ", str(text or "")).strip().lower()
    if not clean:
        return False
    hints = (
        "из памяти",
        "память",
        "вспомни",
        "вспоминай",
        "старое",
        "старый прикол",
        "что было",
    )
    return any(h in clean for h in hints)


def enforce_reply_guardrails(reply_text: str) -> str:
    clean = sanitize_reply_text(reply_text)
    if not clean:
        return ""
    if any(pattern.search(clean) for pattern in TOXIC_REPLY_PATTERNS):
        logger.warning("Смягчаю токсичный ответ LLM")
        return "ок без наездов давай по сути"
    return clean


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
    layers = chat_mem.get("memory_layers", {})
    recent = layers.get("recent_messages", [])
    if isinstance(recent, list) and recent:
        return recent[-12:]
    history = chat_mem.get("history", [])
    return history[-12:]


def select_recent_facts_for_context(memory: Dict[str, Any], chat_id: int) -> List[str]:
    chat_mem = get_chat_mem(memory, chat_id)
    layers = chat_mem.get("memory_layers", {})
    recent_facts = layers.get("recent_facts", [])
    if not isinstance(recent_facts, list) or not recent_facts:
        return []

    def _score(fact: Dict[str, Any]) -> Tuple[float, str]:
        ts = _parse_iso_ts(str(fact.get("ts", "")))
        age_days = (datetime.utcnow() - ts).days if ts else RECENT_FACT_WINDOW_DAYS
        recency_bonus = max(0.0, (RECENT_FACT_WINDOW_DAYS - age_days) / RECENT_FACT_WINDOW_DAYS)
        return (float(fact.get("weight", 1.0)) + recency_bonus, str(fact.get("text", "")))

    filtered_facts = [
        fact
        for fact in recent_facts
        if not is_blocked_memory_text(str(fact.get("text", "")))
    ]
    ranked = sorted(filtered_facts, key=_score, reverse=True)
    return [str(x.get("text", "")) for x in ranked[:4] if str(x.get("text", "")).strip()]


def select_old_random_memories(memory: Dict[str, Any], chat_id: int) -> List[str]:
    chat_mem = get_chat_mem(memory, chat_id)
    layers = chat_mem.get("memory_layers", {})
    long_facts = layers.get("long_facts", [])
    if not isinstance(long_facts, list) or not long_facts:
        return []

    filtered_facts = [
        fact
        for fact in long_facts
        if not is_blocked_memory_text(str(fact.get("text", "")))
    ]
    if not filtered_facts:
        return []

    usage = layers.setdefault("long_fact_usage", {})
    if not isinstance(usage, dict):
        usage = {}
        layers["long_fact_usage"] = usage

    now = datetime.utcnow()
    weighted: List[Tuple[Dict[str, Any], float]] = []
    for fact in filtered_facts:
        text = str(fact.get("text", "")).strip()
        if not text:
            continue
        key = normalize_token(text)[:120]
        meta = usage.get(key, {})
        last_seen = _parse_iso_ts(str(meta.get("last_used_ts", "")))
        used_count = int(meta.get("count", 0)) if isinstance(meta, dict) else 0

        base = max(0.05, float(fact.get("strength", 1.0)))
        penalty = 1.0 / (1.0 + used_count * 0.8)
        if last_seen:
            hours = (now - last_seen).total_seconds() / 3600.0
            if hours < 24:
                penalty *= 0.08
            elif hours < 72:
                penalty *= 0.2
            elif hours < 24 * 7:
                penalty *= 0.5
        weighted.append((fact, base * penalty))

    if not weighted:
        return []

    candidates = sorted(weighted, key=lambda x: x[1], reverse=True)[:8]
    facts = [item[0] for item in candidates]
    weights = [max(0.01, float(item[1])) for item in candidates]
    chosen = random.choices(facts, weights=weights, k=1)[0]
    chosen_text = str(chosen.get("text", "")).strip()
    if not chosen_text:
        return []

    key = normalize_token(chosen_text)[:120]
    meta = usage.get(key, {}) if isinstance(usage.get(key), dict) else {}
    usage[key] = {
        "last_used_ts": now.isoformat(),
        "count": int(meta.get("count", 0)) + 1,
    }
    if len(usage) > LONG_FACT_USAGE_TRACK_LIMIT:
        # Keep only the most recently used facts to cap memory growth.
        items = sorted(
            usage.items(),
            key=lambda kv: str((kv[1] or {}).get("last_used_ts", "")),
        )
        usage.clear()
        for k, v in items[-LONG_FACT_USAGE_TRACK_LIMIT:]:
            usage[k] = v

    return [chosen_text]


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


def build_humor_plan(memory: Dict[str, Any], message: Message) -> Dict[str, Any]:
    chat_mem = get_chat_mem(memory, message.chat_id)
    user = message.from_user
    user_id = int(user.id) if user else 0
    user_name = ""
    if user:
        user_name = user.first_name or user.username or str(user.id)
    return choose_humor_plan(
        chat_mem,
        text=_extract_message_text(message),
        user_id=user_id,
        user_name=user_name,
    )


def build_chat_messages(
    memory: Dict[str, Any],
    message: Message,
    humor_plan: Dict[str, Any] | None = None,
) -> List[Dict[str, Any]]:
    tg_user = message.from_user
    assert tg_user is not None

    system_prompt = get_system_prompt(memory)
    user_profile = select_user_profile(memory, tg_user.id)
    user_text = _extract_message_text(message)
    memory_requested = looks_like_memory_request(user_text)
    chat_history = select_chat_history_for_context(memory, message.chat_id)
    recent_facts = select_recent_facts_for_context(memory, message.chat_id)
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
        "гайд по стилю:\n"
        "- всегда используй только строчные буквы\n"
        "- без эмодзи\n"
        "- максимум 2 очень коротких предложения в одном сообщении\n"
        "- говори естественно и живо, как человек в чате\n"
        "- юмор дружеский и по ситуации, без агрессивных наездов\n"
        "- хорошая шутка = точное наблюдение + неожиданный образ + короткий добив\n"
        "- не используй заезженные шаблоны типа iq комнатной температуры или мои нейроны плавятся\n"
        "- если нет нормальной шутки, выбери сухую реакцию вместо натянутой прожарки\n"
        "- не зацикливайся на одном и том же старом факте, чаще меняй тему\n"
        "- не делай длинные объяснения, лучше коротко и по делу\n"
    )
    if get_active_mode(memory) == "chill":
        full_system += "- режим chill: без грубости, без прожарки, только мягкий дружеский тон\n"

    toxicity = get_effective_toxicity_level(memory)
    full_system += f"\nуровень прожарки: {toxicity}/100\n"
    full_system += f"активный режим личности: {get_active_mode(memory)}\n"
    full_system += "инструкция режима: " + get_mode_prompt(memory) + "\n"

    if humor_plan:
        full_system += "\n" + format_humor_prompt(humor_plan) + "\n"

    style_settings = get_style_settings(memory)
    if style_settings:
        full_system += "\nдоп стиль от владельца:\n" + style_settings + "\n"

    bio_settings = get_bio_settings(memory)
    if bio_settings:
        full_system += "\nбио тимура от владельца:\n" + bio_settings + "\n"

    if user_profile and random.random() < 0.6:
        full_system += "\nинфа о собеседнике:\n" + user_profile

    if association_context and (memory_requested or random.random() < 0.4):
        full_system += "\n\nкарта персонажей и ассоциаций:\n" + association_context

    if hist_lines:
        full_system += "\n\nпоследние сообщения в чате:\n" + "\n".join(hist_lines)

    if recent_facts and (memory_requested or random.random() < 0.4):
        full_system += "\n\nнедавние факты беседы (приоритет):\n"
        for line in recent_facts[:2]:
            full_system += f"- {line}\n"

    if random_memories and memory_requested:
        full_system += "\n\nдалекие факты беседы (редкие точечные отсылки):\n"
        for line in random_memories[:1]:
            full_system += f"- {line}\n"

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
        logger.error("Ошибка OpenAI при генерации текста: %s", e)
        return ""


async def call_openai_vision(
    memory: Dict[str, Any],
    message: Message,
    image_b64: str,
) -> str:
    text_context = (
        "тебе прислали фотку в чате. "
        "сделай короткую смешную ироничную реакцию в стиле дружеской подколки, "
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
        logger.error("Ошибка OpenAI при обработке изображения: %s", e)
        return ""


async def _run_with_typing(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    task_coro: Any,
) -> str:
    stop_event = asyncio.Event()

    async def _typing_pulse() -> None:
        while not stop_event.is_set():
            try:
                await context.bot.send_chat_action(chat_id=chat_id, action=ChatAction.TYPING)
            except Exception as e:
                logger.debug("Не удалось отправить typing action: %s", e)
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=4.0)
            except asyncio.TimeoutError:
                continue

    pulse_task = asyncio.create_task(_typing_pulse())
    try:
        result = await task_coro
        return str(result or "")
    finally:
        stop_event.set()
        try:
            await pulse_task
        except Exception:
            pass


async def _call_openai_story_text(memory: Dict[str, Any], *, proactive: bool = False) -> str:
    prompt = (
        "придумай короткую бытовую историю из жизни тимура на сегодня. "
        "формат: 1-2 короткие фразы, разговорно, живо, смешно, без грубых оскорблений, без эмодзи."
    )
    if proactive:
        prompt += " в конце добавь короткий вопрос в чат."
    try:
        response = await asyncio.to_thread(
            client.chat.completions.create,
            model=TEXT_MODEL,
            messages=[
                {"role": "system", "content": get_system_prompt(memory)},
                {"role": "user", "content": prompt},
            ],
            max_tokens=100,
            temperature=0.95,
        )
        return (response.choices[0].message.content or "").strip()
    except Exception as e:
        logger.error("Ошибка генерации истории: %s", e)
        return ""


async def _generate_story_text(memory: Dict[str, Any], *, proactive: bool = False) -> str:
    raw = await _call_openai_story_text(memory, proactive=proactive)
    clean = enforce_reply_guardrails(raw)
    if clean:
        return clean
    # Fallback only for provider failures/empty output.
    fallback = [
        "сегодня чуть не уехал без кроссовка в метро",
        "пока искал зарядку понял что держал ее в руке",
        "вышел за хлебом и вернулся с какой-то ерундой вместо него",
    ]
    text = random.choice(fallback)
    if proactive:
        text += "\nу вас день лучше идет?"
    return text


async def _emit_proactive_story(application: Any) -> None:
    memory = load_memory()
    cfg = memory.setdefault("config", {})
    life = _ensure_life_config(cfg)
    if not bool(life.get("enabled", True)):
        return

    tz = _safe_zoneinfo(str(life.get("timezone", DEFAULT_LIFE_TIMEZONE)))
    now_local = datetime.now(tz)
    now_utc = datetime.utcnow()
    _refresh_life_daily_state(life, now_local)

    now_minute = now_local.hour * 60 + now_local.minute
    daily_slots = [int(x) for x in life.get("daily_slots", []) if isinstance(x, (int, float, str))]
    sent_slots = [int(x) for x in life.get("sent_slots", []) if isinstance(x, (int, float, str))]
    due = [slot for slot in sorted(daily_slots) if slot <= now_minute and slot not in sent_slots]
    if not due:
        return

    chat_id = _select_proactive_chat_id(memory, life)
    if chat_id is None:
        return

    text = await _generate_story_text(memory, proactive=True)
    text = enforce_reply_guardrails(text)
    if not text:
        return

    await application.bot.send_message(chat_id=chat_id, text=text)
    slot = due[0]
    sent_slots.append(slot)
    life["sent_slots"] = sorted(set(sent_slots))
    chat_last_emit = life.setdefault("chat_last_emit", {})
    if not isinstance(chat_last_emit, dict):
        chat_last_emit = {}
        life["chat_last_emit"] = chat_last_emit
    chat_last_emit[str(chat_id)] = now_utc.isoformat()
    life["last_emit_ts"] = now_utc.isoformat()
    life["last_emit_chat_id"] = chat_id
    _append_story_log(memory, text, source="proactive", chat_id=chat_id)
    save_memory(memory)
    logger.info("Проактивная история отправлена: chat_id=%s slot=%s", chat_id, _minute_to_hhmm(slot))


async def _life_loop(application: Any) -> None:
    logger.info("Запускаю life loop Тимура")
    try:
        while True:
            try:
                await _emit_proactive_story(application)
            except Exception as e:
                logger.error("Ошибка life loop: %s", e)
            await asyncio.sleep(LIFE_LOOP_INTERVAL_SECONDS)
    except asyncio.CancelledError:
        logger.info("Life loop остановлен")
        raise


async def start_life_loop(application: Any) -> None:
    global _LIFE_TASK
    if _LIFE_TASK and not _LIFE_TASK.done():
        return
    _LIFE_TASK = application.create_task(_life_loop(application))


async def stop_life_loop() -> None:
    global _LIFE_TASK
    if not _LIFE_TASK:
        return
    _LIFE_TASK.cancel()
    try:
        await _LIFE_TASK
    except asyncio.CancelledError:
        pass
    finally:
        _LIFE_TASK = None


# =========================
# FUNNY SCAN LOOP
# =========================

def _adapt_funny_scan_settings(settings: Dict[str, Any], state: Dict[str, Any]) -> Dict[str, Any]:
    adapted = dict(settings)
    ratio = soft_budget_ratio(settings, state)
    if ratio < 0.8:
        return adapted
    adapted["stage1_min_score"] = min(100, int(adapted.get("stage1_min_score", 42)) + 8)
    adapted["max_llm_candidates_per_scan"] = max(1, int(adapted.get("max_llm_candidates_per_scan", 12)) // 2)
    adapted["llm_max_context_messages"] = max(4, int(adapted.get("llm_max_context_messages", 12)) - 4)
    return adapted


def _apply_boundary_to_candidate(candidate: Dict[str, Any], boundary: Dict[str, Any]) -> None:
    start_id = int(boundary.get("start_message_id", 0))
    end_id = int(boundary.get("end_message_id", 0))
    message_ids = [int(x) for x in (candidate.get("message_ids") or []) if int(x) > 0]
    if not message_ids or start_id <= 0 or end_id <= 0:
        return
    if start_id not in message_ids or end_id not in message_ids:
        return
    left = message_ids.index(start_id)
    right = message_ids.index(end_id)
    if left > right:
        left, right = right, left
    selected = message_ids[left : right + 1]
    if not selected:
        return
    candidate["message_ids"] = selected
    cluster_messages = [x for x in (candidate.get("cluster_messages") or []) if int(x.get("message_id", 0)) in set(selected)]
    if cluster_messages:
        candidate["cluster_messages"] = cluster_messages
        candidate["time_start"] = str(cluster_messages[0].get("ts", candidate.get("time_start", "")))
        candidate["time_end"] = str(cluster_messages[-1].get("ts", candidate.get("time_end", "")))


async def _send_funny_candidate_preview(
    application: Any,
    *,
    settings: Dict[str, Any],
    candidate_id: str,
) -> bool:
    async with _FUNNY_SCAN_STATE_LOCK:
        state = _load_funny_scan_state()
        candidate = get_candidate(state, candidate_id)
        if not candidate or candidate.get("preview_sent_at"):
            return False
        text = format_funny_candidate_preview(candidate)

    owner_chat_id = int(settings.get("owner_dm_chat_id", OWNER_ID))
    keyboard = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("Одобрить", callback_data=f"adm:funny:approve:{candidate_id}"),
                InlineKeyboardButton("Отклонить", callback_data=f"adm:funny:reject:{candidate_id}"),
            ],
            [InlineKeyboardButton("Открыть", callback_data=f"adm:funny:open:{candidate_id}")],
        ]
    )
    try:
        sent = await application.bot.send_message(
            chat_id=owner_chat_id,
            text=text[:3900],
            reply_markup=keyboard,
        )
    except Exception as e:
        logger.error("Не удалось отправить funny preview %s: %s", candidate_id, e)
        async with _FUNNY_SCAN_STATE_LOCK:
            state = _load_funny_scan_state()
            candidate = get_candidate(state, candidate_id)
            if candidate:
                candidate.setdefault("meta", {})["preview_error"] = str(e)
                _save_funny_scan_state(state)
        return False

    async with _FUNNY_SCAN_STATE_LOCK:
        state = _load_funny_scan_state()
        if not set_preview_sent(state, candidate_id, preview_message_id=sent.message_id):
            return False
        _save_funny_scan_state(state)
    return True


def _candidate_from_stage2(
    *,
    draft: Dict[str, Any],
    llm_result: Dict[str, Any],
    settings: Dict[str, Any],
    trigger: str,
) -> Dict[str, Any]:
    merged = dict(draft)
    merged["score"] = int(llm_result.get("score", 0))
    merged["show_to_owner"] = bool(llm_result.get("show_to_owner", False))
    merged["llm_reason_short"] = str(llm_result.get("reason_short", ""))[:240]
    merged["llm_boundary"] = dict(llm_result.get("boundary") or {})
    merged["signals_pos"] = sorted(set(list(merged.get("signals_pos") or []) + list(llm_result.get("positive_signals") or [])))
    merged["signals_neg"] = sorted(set(list(merged.get("signals_neg") or []) + list(llm_result.get("negative_signals") or [])))
    _apply_boundary_to_candidate(merged, merged["llm_boundary"])
    merged["status"] = STATUS_NEW
    merged["meta"] = {
        **dict(merged.get("meta") or {}),
        "intensity": str(settings.get("intensity", "balanced")),
        "trigger": trigger,
    }
    return merged


async def _run_funny_scan_once(application: Any, *, trigger: str) -> Dict[str, Any]:
    if _FUNNY_SCAN_LOCK.locked():
        return {"busy": True}
    async with _FUNNY_SCAN_LOCK:
        memory = load_memory()
        settings = _get_funny_scan_settings(memory)
        async with _FUNNY_SCAN_STATE_LOCK:
            state = _load_funny_scan_state()
            ensure_budget_day(state)
            if trigger == "scheduled" and not bool(settings.get("enabled", False)):
                return {"skipped": "disabled"}
            adapted_settings = _adapt_funny_scan_settings(settings, state)
            reaction_index_snapshot = dict(state.get("reaction_index", {}))

        sources = [src for src in settings.get("sources", []) if isinstance(src, dict) and bool(src.get("enabled", True))]
        summary = {
            "sources": 0,
            "stage1_candidates": 0,
            "llm_calls": 0,
            "created": 0,
            "previewed": 0,
            "skipped_budget": False,
            "deduped": 0,
        }
        chats = memory.get("chats", {}) if isinstance(memory.get("chats"), dict) else {}

        for source in sources:
            chat_id = int(source.get("chat_id", 0))
            if not chat_id:
                continue
            chat_mem = chats.get(str(chat_id))
            if not isinstance(chat_mem, dict):
                continue
            history = chat_mem.get("history", [])
            if not isinstance(history, list) or not history:
                continue

            scoped_messages = extract_period_messages(
                history,
                period_hours=int(adapted_settings.get("scan_period_hours", 24)),
            )
            if not scoped_messages:
                continue

            stage1_candidates = build_stage1_candidates(
                scoped_messages,
                source_chat_id=chat_id,
                source_chat_title=str(source.get("title") or f"chat {chat_id}"),
                reaction_index=reaction_index_snapshot,
                settings=adapted_settings,
                lexicon=FUNNY_SCAN_LEXICON,
            )
            summary["sources"] += 1
            summary["stage1_candidates"] += len(stage1_candidates)

            llm_limit = max(1, int(adapted_settings.get("max_llm_candidates_per_scan", 12)))
            for draft in stage1_candidates[:llm_limit]:
                async with _FUNNY_SCAN_STATE_LOCK:
                    state = _load_funny_scan_state()
                    ensure_budget_day(state)
                    if hard_budget_reached(adapted_settings, state):
                        summary["skipped_budget"] = True
                        break
                    if has_candidate_signature(state, draft):
                        summary["deduped"] += 1
                        continue

                try:
                    llm_result, tokens_used = await asyncio.to_thread(
                        evaluate_candidate_with_llm,
                        client,
                        model=str(adapted_settings.get("llm_model", TEXT_MODEL)),
                        candidate=draft,
                        max_context_messages=int(adapted_settings.get("llm_max_context_messages", 12)),
                        max_chars_per_message=int(adapted_settings.get("llm_max_chars_per_message", 220)),
                        review_threshold=int(adapted_settings.get("review_threshold", 70)),
                    )
                except Exception as e:
                    logger.error("funny scan LLM failed (chat=%s): %s", chat_id, e)
                    continue

                candidate = _candidate_from_stage2(
                    draft=draft,
                    llm_result=llm_result,
                    settings=adapted_settings,
                    trigger=trigger,
                )
                candidate_id = ""
                added = False
                async with _FUNNY_SCAN_STATE_LOCK:
                    state = _load_funny_scan_state()
                    ensure_budget_day(state)
                    if hard_budget_reached(adapted_settings, state):
                        summary["skipped_budget"] = True
                        break
                    if has_candidate_signature(state, draft):
                        summary["deduped"] += 1
                        continue
                    register_token_usage(state, int(tokens_used))
                    summary["llm_calls"] += 1
                    candidate_id, added = add_candidate(state, candidate)
                    if added:
                        summary["created"] += 1
                    _save_funny_scan_state(state)

                if (
                    added
                    and bool(candidate.get("show_to_owner"))
                    and int(candidate.get("score", 0)) >= int(adapted_settings.get("review_threshold", 70))
                ):
                    if await _send_funny_candidate_preview(
                        application,
                        settings=settings,
                        candidate_id=candidate_id,
                    ):
                        summary["previewed"] += 1

            async with _FUNNY_SCAN_STATE_LOCK:
                state = _load_funny_scan_state()
                update_last_scan(state, chat_id)
                _save_funny_scan_state(state)

            if summary["skipped_budget"]:
                break

        return summary


async def _funny_scan_loop(application: Any) -> None:
    logger.info("Запускаю funny scan loop")
    try:
        while True:
            try:
                memory = load_memory()
                settings = _get_funny_scan_settings(memory)
                if settings.get("enabled"):
                    async with _FUNNY_SCAN_STATE_LOCK:
                        state = _load_funny_scan_state()
                        ensure_budget_day(state)
                    last_scan_ts = str((state.get("state") or {}).get("last_scan_ts") or "")
                    last_dt = _parse_iso_ts(last_scan_ts)
                    due = False
                    if not last_dt:
                        due = True
                    else:
                        elapsed = (datetime.utcnow() - last_dt).total_seconds()
                        due = elapsed >= max(60, int(settings.get("scan_schedule_minutes", 60)) * 60)
                    if due:
                        await _run_funny_scan_once(application, trigger="scheduled")
            except Exception as e:
                logger.error("Ошибка funny scan loop: %s", e)
            await asyncio.sleep(FUNNY_SCAN_LOOP_INTERVAL_SECONDS)
    except asyncio.CancelledError:
        logger.info("Funny scan loop остановлен")
        raise


async def start_funny_scan_loop(application: Any) -> None:
    global _FUNNY_SCAN_TASK
    if _FUNNY_SCAN_TASK and not _FUNNY_SCAN_TASK.done():
        return
    _FUNNY_SCAN_TASK = application.create_task(_funny_scan_loop(application))


async def stop_funny_scan_loop() -> None:
    global _FUNNY_SCAN_TASK
    if not _FUNNY_SCAN_TASK:
        return
    _FUNNY_SCAN_TASK.cancel()
    try:
        await _FUNNY_SCAN_TASK
    except asyncio.CancelledError:
        pass
    finally:
        _FUNNY_SCAN_TASK = None


# =========================
# ОБРАБОТКА ТЕКСТА
# =========================

def sanitize_reply_text(raw: str) -> str:
    return sanitize_reply_text_service(raw)


def split_into_chain(text: str) -> List[str]:
    return split_into_chain_service(text)


def build_tts_input(reply_text: str, style_prompt: str) -> str:
    directives = re.findall(r"\[[^\]]+\]", style_prompt or "")
    prefix = " ".join(part.strip() for part in directives if part.strip()).strip()
    if prefix:
        return f"{prefix}\n{reply_text}"
    return reply_text


def _apply_feedback_to_reply(memory: Dict[str, Any], message: Message, rating: str, source: str) -> bool:
    if not message.reply_to_message:
        return False
    chat_mem = get_chat_mem(memory, message.chat_id)
    user_id = message.from_user.id if message.from_user else None
    ok = apply_feedback(
        chat_mem,
        message_id=message.reply_to_message.message_id,
        rating=rating,
        source=source,
        user_id=user_id,
    )
    if ok:
        save_memory(memory)
    return ok


async def _handle_text_feedback(update: Update, memory: Dict[str, Any]) -> bool:
    message = update.effective_message
    if not message or not message.reply_to_message:
        return False
    rating = classify_text_feedback(_extract_message_text(message))
    if not rating:
        return False
    _apply_feedback_to_reply(memory, message, rating, source="reply_text")
    return True


async def reaction_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    del context
    reaction = update.message_reaction
    if not reaction:
        return
    reaction_emoji: List[str] = []
    old_reaction_emoji: List[str] = []
    for item in reaction.new_reaction or []:
        emoji = getattr(item, "emoji", None)
        if emoji:
            reaction_emoji.append(str(emoji))
    for item in reaction.old_reaction or []:
        emoji = getattr(item, "emoji", None)
        if emoji:
            old_reaction_emoji.append(str(emoji))
    logger.info(
        "Получена reaction: chat_id=%s message_id=%s user_id=%s emoji=%s",
        reaction.chat.id,
        reaction.message_id,
        reaction.user.id if reaction.user else None,
        ",".join(reaction_emoji) if reaction_emoji else "<non-emoji>",
    )
    async with _FUNNY_SCAN_STATE_LOCK:
        state = _load_funny_scan_state()
        apply_reaction_delta(
            state,
            chat_id=reaction.chat.id,
            message_id=reaction.message_id,
            old_emojis=old_reaction_emoji,
            new_emojis=reaction_emoji,
            heart_emojis=FUNNY_SCAN_LEXICON.get("heart_emojis", []),
            laugh_emojis=FUNNY_SCAN_LEXICON.get("laugh_emojis", []),
        )
        _save_funny_scan_state(state)

    rating = classify_reactions(reaction.new_reaction)
    if not rating:
        logger.info("Reaction проигнорирована: не funny/unfunny по правилам")
        return
    memory = load_memory()
    chat_mem = get_chat_mem(memory, reaction.chat.id)
    user_id = reaction.user.id if reaction.user else None
    ok = apply_feedback(
        chat_mem,
        message_id=reaction.message_id,
        rating=rating,
        source="reaction",
        user_id=user_id,
    )
    if ok:
        save_memory(memory)
        logger.info("Reaction feedback применен: rating=%s message_id=%s", rating, reaction.message_id)
    else:
        logger.info("Reaction feedback пропущен: message_id=%s не найден в bot_outputs", reaction.message_id)


# =========================
# ОТПРАВКА
# =========================

async def send_reply_with_style(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    memory: Dict[str, Any],
    reply_text: str,
    force_voice: bool = False,
    humor_plan: Dict[str, Any] | None = None,
) -> None:
    del context
    message = update.effective_message

    if not message:
        return

    reply_text = enforce_reply_guardrails(reply_text)

    if not reply_text:
        logger.info("Ответ после очистки пустой, пропускаю отправку")
        return

    use_watermark = False
    watermark_text = ""
    try:
        use_watermark, watermark_text = billing.should_apply_free_watermark(message.chat_id)
        if use_watermark and watermark_text:
            reply_text = f"{reply_text}\n\n{watermark_text}"
    except Exception as e:
        logger.error("Ошибка проверки водяного знака биллинга: %s", e)

    # Вторая очистка после возможной инъекции watermark-текста.
    reply_text = enforce_reply_guardrails(reply_text)
    if not reply_text:
        logger.info("Ответ после post-watermark очистки пустой, пропускаю отправку")
        return

    can_try_voice = bool(GEMINI_API_KEY) and can_send_voice(memory, message.chat_id) and (
        force_voice or (not use_watermark and random.random() < VOICE_REPLY_CHANCE)
    )
    if can_try_voice:
        voice_text = re.sub(r"\s+", " ", re.sub(r"https?://\S+", "", reply_text)).strip()
        if len(voice_text) > MAX_VOICE_CHARS:
            voice_text = voice_text[:MAX_VOICE_CHARS].rsplit(" ", 1)[0].strip()
        if voice_text:
            tts_text = build_tts_input(voice_text, VOICE_STYLE_PROMPT)
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
            sent = await message.reply_text(part)
            chat_mem = get_chat_mem(memory, message.chat_id)
            record_bot_output(chat_mem, message_id=sent.message_id, text=part, plan=humor_plan)
            await asyncio.sleep(random.uniform(0.2, 0.6))
    else:
        sent = await message.reply_text(reply_text)
        chat_mem = get_chat_mem(memory, message.chat_id)
        record_bot_output(chat_mem, message_id=sent.message_id, text=reply_text, plan=humor_plan)

    save_memory(memory)


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
                InlineKeyboardButton("смешные моменты", callback_data=f"adm:funny:menu:{chat_id}"),
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


def _funny_menu_keyboard(chat_id: int, settings: Dict[str, Any]) -> InlineKeyboardMarkup:
    on_off_label = "выкл сканер" if settings.get("enabled") else "вкл сканер"
    intensity = str(settings.get("intensity", "balanced"))
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(on_off_label, callback_data=f"adm:funny:toggle:{chat_id}"),
                InlineKeyboardButton("сканировать сейчас", callback_data=f"adm:funny:scan_now:{chat_id}"),
            ],
            [
                InlineKeyboardButton("источники", callback_data=f"adm:funny:sources:{chat_id}"),
                InlineKeyboardButton("период", callback_data=f"adm:funny:period:{chat_id}"),
            ],
            [
                InlineKeyboardButton(f"intensity: {intensity}", callback_data=f"adm:funny:intensity:{chat_id}"),
                InlineKeyboardButton("порог", callback_data=f"adm:funny:threshold:{chat_id}"),
            ],
            [
                InlineKeyboardButton("лимиты", callback_data=f"adm:funny:limits:{chat_id}"),
                InlineKeyboardButton("бюджет", callback_data=f"adm:funny:budget:{chat_id}"),
            ],
            [
                InlineKeyboardButton("кандидаты new", callback_data=f"adm:funny:list:{chat_id}"),
                InlineKeyboardButton("назад", callback_data=f"adm:root:{chat_id}"),
            ],
        ]
    )


def _funny_sources_keyboard(chat_id: int, known_sources: List[Dict[str, Any]]) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    for source in known_sources[:24]:
        cid = int(source.get("chat_id", 0))
        title = str(source.get("title") or f"chat {cid}")
        rows.append([InlineKeyboardButton(title[:32], callback_data=f"adm:funny:source_toggle:{cid}:{chat_id}")])
    rows.append([InlineKeyboardButton("назад", callback_data=f"adm:funny:menu:{chat_id}")])
    return InlineKeyboardMarkup(rows)


def _funny_candidates_keyboard(chat_id: int, candidates: List[Dict[str, Any]]) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    for item in candidates[:12]:
        candidate_id = str(item.get("id") or "")
        if not candidate_id:
            continue
        score = item.get("score")
        if score is None:
            score = item.get("pre_score")
        rows.append(
            [
                InlineKeyboardButton(
                    f"{candidate_id} ({score})",
                    callback_data=f"adm:funny:open:{candidate_id}",
                )
            ]
        )
    rows.append([InlineKeyboardButton("назад", callback_data=f"adm:funny:menu:{chat_id}")])
    return InlineKeyboardMarkup(rows)


def _funny_preview_keyboard(chat_id: int, candidate_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("Одобрить", callback_data=f"adm:funny:approve:{candidate_id}"),
                InlineKeyboardButton("Отклонить", callback_data=f"adm:funny:reject:{candidate_id}"),
            ],
            [
                InlineKeyboardButton("Повторить forward", callback_data=f"adm:funny:retry:{candidate_id}"),
                InlineKeyboardButton("кандидаты", callback_data=f"adm:funny:list:{chat_id}"),
            ],
            [InlineKeyboardButton("назад", callback_data=f"adm:funny:menu:{chat_id}")],
        ]
    )


def _funny_period_keyboard(chat_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("6ч", callback_data=f"adm:funny:period_set:6:{chat_id}"),
                InlineKeyboardButton("24ч", callback_data=f"adm:funny:period_set:24:{chat_id}"),
                InlineKeyboardButton("3д", callback_data=f"adm:funny:period_set:72:{chat_id}"),
                InlineKeyboardButton("7д", callback_data=f"adm:funny:period_set:168:{chat_id}"),
            ],
            [
                InlineKeyboardButton("ввести вручную", callback_data=f"adm:input:funny_period:{chat_id}"),
                InlineKeyboardButton("назад", callback_data=f"adm:funny:menu:{chat_id}"),
            ],
        ]
    )


def _funny_intensity_keyboard(chat_id: int, active: str) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    line: List[InlineKeyboardButton] = []
    for mode in ("cheap", "balanced", "deep"):
        marker = "● " if mode == active else ""
        line.append(InlineKeyboardButton(f"{marker}{mode}", callback_data=f"adm:funny:intensity_set:{mode}:{chat_id}"))
    rows.append(line)
    rows.append([InlineKeyboardButton("назад", callback_data=f"adm:funny:menu:{chat_id}")])
    return InlineKeyboardMarkup(rows)


def _funny_threshold_keyboard(chat_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("60", callback_data=f"adm:funny:threshold_set:60:{chat_id}"),
                InlineKeyboardButton("70", callback_data=f"adm:funny:threshold_set:70:{chat_id}"),
                InlineKeyboardButton("80", callback_data=f"adm:funny:threshold_set:80:{chat_id}"),
            ],
            [
                InlineKeyboardButton("ввести вручную", callback_data=f"adm:input:funny_threshold:{chat_id}"),
                InlineKeyboardButton("назад", callback_data=f"adm:funny:menu:{chat_id}"),
            ],
        ]
    )


def _funny_limits_keyboard(chat_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("cand=20", callback_data=f"adm:funny:limit_set:max_candidates_per_scan:20:{chat_id}"),
                InlineKeyboardButton("cand=30", callback_data=f"adm:funny:limit_set:max_candidates_per_scan:30:{chat_id}"),
                InlineKeyboardButton("cand=45", callback_data=f"adm:funny:limit_set:max_candidates_per_scan:45:{chat_id}"),
            ],
            [
                InlineKeyboardButton("llm=6", callback_data=f"adm:funny:limit_set:max_llm_candidates_per_scan:6:{chat_id}"),
                InlineKeyboardButton("llm=12", callback_data=f"adm:funny:limit_set:max_llm_candidates_per_scan:12:{chat_id}"),
                InlineKeyboardButton("llm=18", callback_data=f"adm:funny:limit_set:max_llm_candidates_per_scan:18:{chat_id}"),
            ],
            [
                InlineKeyboardButton("fwd/day=10", callback_data=f"adm:funny:limit_set:daily_forward_limit:10:{chat_id}"),
                InlineKeyboardButton("fwd/day=20", callback_data=f"adm:funny:limit_set:daily_forward_limit:20:{chat_id}"),
                InlineKeyboardButton("fwd/day=40", callback_data=f"adm:funny:limit_set:daily_forward_limit:40:{chat_id}"),
            ],
            [InlineKeyboardButton("назад", callback_data=f"adm:funny:menu:{chat_id}")],
        ]
    )


def _funny_budget_keyboard(chat_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("30k", callback_data=f"adm:funny:budget_set:30000:{chat_id}"),
                InlineKeyboardButton("50k", callback_data=f"adm:funny:budget_set:50000:{chat_id}"),
                InlineKeyboardButton("80k", callback_data=f"adm:funny:budget_set:80000:{chat_id}"),
            ],
            [
                InlineKeyboardButton("ввести вручную", callback_data=f"adm:input:funny_budget:{chat_id}"),
                InlineKeyboardButton("назад", callback_data=f"adm:funny:menu:{chat_id}"),
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
    funny_settings = _get_funny_scan_settings(memory)
    funny_state = _load_funny_scan_state()
    funny_new = len(list_candidates(funny_state, status=STATUS_NEW, limit=9999))
    return (
        "админ панель тимура\n"
        f"чат: {chat_id}\n"
        f"режим: {get_active_mode(memory)}\n"
        f"вредность: {get_toxicity_level(memory)}/100\n"
        f"scanner: {'on' if funny_settings.get('enabled') else 'off'} | new={funny_new}\n"
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


def _upsert_query_params(url: str, params: Dict[str, str]) -> str:
    parts = urlsplit(url)
    query = dict(parse_qsl(parts.query, keep_blank_values=True))
    query.update(params)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), parts.fragment))


def _miniapp_persona_cards() -> List[Dict[str, str]]:
    cards: List[Dict[str, str]] = []
    for mode, prompt in PERSONA_MODES.items():
        title = mode.replace("_", " ").upper()
        cards.append({"id": mode, "title": title, "description": prompt[:120]})
    return cards


def _miniapp_members(memory: Dict[str, Any], chat_id: int) -> List[Dict[str, Any]]:
    chat_mem = get_chat_mem(memory, chat_id)
    participants = chat_mem.get("participants", {})
    p_sorted = sorted(
        participants.values(),
        key=lambda p: int(p.get("message_count", 0)),
        reverse=True,
    )[:8]
    members: List[Dict[str, Any]] = []
    for p in p_sorted:
        uid = int(p.get("user_id", 0))
        if not uid:
            continue
        name = p.get("name") or p.get("username") or f"user {uid}"
        username = p.get("username") or ""
        label = f"@{username}" if username else name
        archetypes = [name for name, _ in _top_items(p.get("archetypes", {}), n=3)] or ["хаотик"]
        tags = [name for name, _ in _top_items(p.get("keywords", {}), n=6)] or ["пока пусто"]
        rel_names: List[str] = []
        for key, _ in sorted(
            chat_mem.get("user_relations", {}).items(),
            key=lambda item: (-float(item[1]), item[0]),
        ):
            if "|" not in key:
                continue
            a_str, b_str = key.split("|", 1)
            a, b = int(a_str), int(b_str)
            if uid not in (a, b):
                continue
            other = b if a == uid else a
            other_data = participants.get(str(other), {})
            other_name = other_data.get("username") or other_data.get("name") or str(other)
            rel_names.append(f"@{other_name}" if other_data.get("username") else other_name)
            if len(rel_names) >= 4:
                break
        members.append(
            {
                "id": str(uid),
                "title": label,
                "tags": tags,
                "archetypes": archetypes,
                "links": rel_names or ["пока без связей"],
            }
        )
    return members


def _miniapp_billing_state(chat_id: int) -> Dict[str, Any]:
    summary = billing.get_chat_activity_summary(chat_id)
    active_count = int(summary.get("active_count", 0))
    active_users = summary.get("active_users", [])
    payer_count = 1 if active_count else 0
    quote_owner = billing.get_quote(
        chat_id=chat_id,
        mode="owner",
        provider="stars",
        payer_count=1 if payer_count else None,
    )
    split_payers = min(max(1, len(active_users)), 10) if active_users else 0
    return {
        "activeUsers30d": active_count,
        "payerCountOwner": payer_count,
        "payerCountSplit": split_payers,
        "pricePerRub": int(quote_owner.price_per_active_rub),
        "minRub": int(quote_owner.min_price_rub),
        "maxRub": int(quote_owner.max_price_rub),
        "activationRatio": float(quote_owner.activation_ratio),
        "provider": "stars",
    }


def build_miniapp_launch_url(memory: Dict[str, Any], chat_id: int) -> str:
    if not MINIAPP_URL:
        return ""
    cfg = memory.setdefault("config", {})
    members = _miniapp_members(memory, chat_id)
    payload = {
        "chatId": chat_id,
        "settings": {
            "activeMode": get_active_mode(memory),
            "heat": get_toxicity_level(memory),
            "previewText": PERSONA_MODES.get(get_active_mode(memory), ""),
            "personas": _miniapp_persona_cards(),
        },
        "memory": {
            "members": members,
            "selectedMember": members[0]["id"] if members else "",
        },
        "billing": _miniapp_billing_state(chat_id),
        "meta": {
            "systemPromptSet": bool(str(cfg.get("system_prompt") or "").strip()),
            "styleSet": bool(str(cfg.get("style_settings") or "").strip()),
            "bioSet": bool(str(cfg.get("bio") or "").strip()),
        },
    }
    encoded = base64.urlsafe_b64encode(
        json.dumps(payload, ensure_ascii=False).encode("utf-8")
    ).decode("ascii")
    return _upsert_query_params(MINIAPP_URL, {"state": encoded})


def _miniapp_reply_keyboard(url: str) -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [[KeyboardButton("открыть mini app", web_app=WebAppInfo(url=url))]],
        resize_keyboard=True,
        one_time_keyboard=True,
        selective=True,
        input_field_placeholder="нажми чтобы открыть панель",
    )


def _parse_miniapp_chat_id(message: Message) -> int:
    parts = ((message.text or "").strip().split(maxsplit=1))
    if len(parts) < 2:
        return int(message.chat_id)
    try:
        return int(parts[1].strip())
    except Exception:
        return int(message.chat_id)


def apply_miniapp_admin_config(memory: Dict[str, Any], chat_id: int, payload: Dict[str, Any]) -> str:
    if int(payload.get("chat_id", chat_id)) != int(chat_id):
        raise ValueError("chat mismatch")
    mode = str(payload.get("active_mode") or "").strip().lower()
    heat = payload.get("heat")
    cfg = memory.setdefault("config", {})
    if mode:
        if mode not in PERSONA_MODES:
            raise ValueError("unknown mode")
        cfg["active_mode"] = mode
    if heat is not None:
        cfg["toxicity_level"] = max(0, min(100, int(heat)))
    save_memory(memory)
    return (
        "mini app конфиг применен\n"
        f"режим: {get_active_mode(memory)}\n"
        f"вредность: {get_toxicity_level(memory)}/100"
    )


def build_miniapp_quote_text(chat_id: int, mode: str) -> str:
    normalized_mode = mode if mode in {"owner", "split"} else "owner"
    payer_count = 1 if normalized_mode == "owner" else None
    quote = billing.get_quote(
        chat_id=chat_id,
        mode=normalized_mode,
        provider="stars",
        payer_count=payer_count,
    )
    payer_ids = ", ".join(str(x) for x in quote.payer_ids[:10]) or "-"
    return "\n".join(
        [
            f"mini app квота chat={quote.chat_id}",
            f"mode={quote.mode}, provider={quote.provider}",
            f"active={len(quote.active_users)}, payers={len(quote.payer_ids)}",
            f"total={_fmt_rub(quote.total_rub)}",
            f"payer ids: {payer_ids}",
            "реальная оплата пока идет через billing-команды бота",
        ]
    )


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


def _resolve_funny_chat_id_from_candidate(candidate: Dict[str, Any], fallback_chat_id: int) -> int:
    source_chat_id = int(candidate.get("source_chat_id", 0))
    if source_chat_id:
        return source_chat_id
    return int(fallback_chat_id)


def _safe_int(raw: Any) -> int | None:
    try:
        return int(raw)
    except Exception:
        return None


def _forward_lock_for(candidate_id: str) -> asyncio.Lock:
    key = str(candidate_id or "")
    lock = _FUNNY_FORWARD_LOCKS.get(key)
    if lock is None:
        lock = asyncio.Lock()
        _FUNNY_FORWARD_LOCKS[key] = lock
    return lock


async def _forward_funny_candidate(
    *,
    bot: Any,
    settings: Dict[str, Any],
    candidate_id: str,
    action: str = "approve",
) -> Tuple[bool, str]:
    async with _forward_lock_for(candidate_id):
        async with _FUNNY_SCAN_STATE_LOCK:
            state = _load_funny_scan_state()
            candidate = get_candidate(state, candidate_id)
            if not candidate:
                return False, "кандидат не найден"

            status = str(candidate.get("status"))
            if status == STATUS_SENT:
                return False, "кандидат уже отправлен"
            if action == "retry" and status != STATUS_APPROVED:
                return False, "retry доступен только для approved кандидатов после ошибки forward"

            ensure_budget_day(state)
            budget = state.get("budget", {})
            forward_limit = int(settings.get("daily_forward_limit", 20))
            sent = int(budget.get("forwards_sent", 0))
            pending = int(budget.get("pending_forwards", 0))
            if sent + pending >= forward_limit:
                return False, f"достигнут дневной лимит forward ({forward_limit})"

            source_chat_id = int(candidate.get("source_chat_id", 0))
            owner_chat_id = int(settings.get("owner_dm_chat_id", OWNER_ID))
            message_ids = [int(x) for x in (candidate.get("message_ids") or []) if int(x) > 0]
            if not source_chat_id or not message_ids:
                return False, "в кандидате нет source_chat_id/message_ids"
            budget["pending_forwards"] = max(0, pending + 1)
            set_candidate_status(state, candidate_id, STATUS_APPROVED, forward_error=None)
            _save_funny_scan_state(state)

        try:
            await bot.forward_messages(
                chat_id=owner_chat_id,
                from_chat_id=source_chat_id,
                message_ids=message_ids,
            )
        except Exception as e:
            async with _FUNNY_SCAN_STATE_LOCK:
                state = _load_funny_scan_state()
                ensure_budget_day(state)
                budget = state.get("budget", {})
                budget["pending_forwards"] = max(0, int(budget.get("pending_forwards", 0)) - 1)
                set_candidate_status(state, candidate_id, STATUS_APPROVED, forward_error=str(e))
                _save_funny_scan_state(state)
            return False, f"forward failed: {e}"

        async with _FUNNY_SCAN_STATE_LOCK:
            state = _load_funny_scan_state()
            ensure_budget_day(state)
            budget = state.get("budget", {})
            budget["pending_forwards"] = max(0, int(budget.get("pending_forwards", 0)) - 1)
            candidate = get_candidate(state, candidate_id)
            if not candidate:
                _save_funny_scan_state(state)
                return False, "кандидат не найден после forward"
            if str(candidate.get("status")) == STATUS_SENT:
                _save_funny_scan_state(state)
                return True, "forward уже был отправлен"
            set_candidate_status(state, candidate_id, STATUS_SENT, forward_error=None)
            register_forward_usage(state)
            _save_funny_scan_state(state)
        return True, "forward выполнен"


async def _reject_funny_candidate(candidate_id: str) -> Tuple[bool, str]:
    async with _forward_lock_for(candidate_id):
        async with _FUNNY_SCAN_STATE_LOCK:
            state = _load_funny_scan_state()
            candidate = get_candidate(state, candidate_id)
            if not candidate:
                return False, "кандидат не найден"
            status = str(candidate.get("status"))
            if status == STATUS_SENT:
                return False, "кандидат уже отправлен"
            if status == STATUS_REJECTED:
                return True, "кандидат уже отклонен"
            set_candidate_status(state, candidate_id, STATUS_REJECTED, forward_error=None)
            _save_funny_scan_state(state)
        return True, "кандидат отклонен"


async def admin_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    del context
    if not _is_owner(update):
        return
    message = update.message
    if not message:
        return
    memory = load_memory()
    chat_id = message.chat_id
    settings = _get_funny_scan_settings(memory)
    if not any(int(item.get("chat_id", 0)) == int(chat_id) for item in settings.get("sources", []) if isinstance(item, dict)):
        upsert_source(settings, chat_id, title=f"chat {chat_id}", enabled=False)
        save_memory(memory)
    await message.reply_text(
        _format_admin_status(memory, chat_id),
        reply_markup=_admin_main_keyboard(chat_id),
    )
    miniapp_url = build_miniapp_launch_url(memory, chat_id)
    if miniapp_url and message.chat.type == "private":
        await message.reply_text(
            "открой mini app кнопкой снизу чтобы изменения пришли в этот чат как web_app_data\n"
            f"raw url:\n{miniapp_url}",
            reply_markup=_miniapp_reply_keyboard(miniapp_url),
        )
    elif miniapp_url:
        await message.reply_text(
            f"mini app для этого чата открывай в личке с ботом командой:\n/miniapp {chat_id}"
        )
    else:
        await message.reply_text("mini app не настроен: добавь MINIAPP_URL в .env")


async def miniapp_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    del context
    if not _is_owner(update):
        return
    message = update.message
    if not message:
        return
    memory = load_memory()
    target_chat_id = _parse_miniapp_chat_id(message)
    miniapp_url = build_miniapp_launch_url(memory, target_chat_id)
    if not miniapp_url:
        await message.reply_text("mini app не настроен: добавь MINIAPP_URL в .env")
        return
    await message.reply_text(
        f"панель для чата {target_chat_id} готова. запускай mini app через кнопку ниже.\n"
        f"raw url:\n{miniapp_url}",
        reply_markup=_miniapp_reply_keyboard(miniapp_url),
    )


async def miniappdebug_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    del context
    if not _is_owner(update):
        return
    message = update.message
    if not message:
        return
    memory = load_memory()
    target_chat_id = _parse_miniapp_chat_id(message)
    miniapp_url = build_miniapp_launch_url(memory, target_chat_id)
    parsed_base = urlsplit(MINIAPP_URL)
    parsed_launch = urlsplit(miniapp_url) if miniapp_url else urlsplit("")
    await message.reply_text(
        "\n".join(
            [
                "miniapp debug",
                f"base url: {MINIAPP_URL or '-'}",
                f"base host: {parsed_base.netloc or '-'}",
                f"target chat: {target_chat_id}",
                f"launch host: {parsed_launch.netloc or '-'}",
                f"launch url: {miniapp_url or '-'}",
                "если telegram открывает другой host, значит нажата старая кнопка или старый menu button в botfather",
            ]
        )
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

    if action in {"funny_period", "funny_threshold", "funny_budget"}:
        try:
            value = int(text.strip())
        except ValueError:
            await message.reply_text("нужно целое число или /cancel")
            return True
        settings = _get_funny_scan_settings(memory)
        if action == "funny_period":
            settings["scan_period_hours"] = max(1, min(24 * 30, value))
            reply = f"scan period обновлен: {settings['scan_period_hours']}h"
        elif action == "funny_threshold":
            settings["review_threshold"] = max(0, min(100, value))
            reply = f"review threshold обновлен: {settings['review_threshold']}"
        else:
            settings["daily_token_budget"] = max(1000, min(10_000_000, value))
            settings["daily_token_hard_stop"] = max(
                settings["daily_token_budget"],
                int(settings.get("daily_token_hard_stop", settings["daily_token_budget"])),
            )
            reply = (
                f"token budget обновлен: {settings['daily_token_budget']} "
                f"(hard stop={settings['daily_token_hard_stop']})"
            )
        save_memory(memory)
        _clear_admin_pending(context)
        await message.reply_text(reply)
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

    if action == "funny":
        sub = parts[2] if len(parts) >= 3 else "menu"
        settings = _get_funny_scan_settings(memory)
        state = _load_funny_scan_state()

        def _chat_id_fallback() -> int:
            if query.message:
                return int(query.message.chat_id)
            return int(update.effective_chat.id if update.effective_chat else OWNER_ID)

        def _part_int(index: int) -> int | None:
            if len(parts) <= index:
                return None
            return _safe_int(parts[index])

        if sub == "menu":
            parsed_chat_id = _part_int(3)
            chat_id = parsed_chat_id if parsed_chat_id is not None else _chat_id_fallback()
            await query.edit_message_text(
                format_funny_status(settings, state),
                reply_markup=_funny_menu_keyboard(chat_id, settings),
            )
            return

        if sub == "toggle":
            parsed_chat_id = _part_int(3)
            chat_id = parsed_chat_id if parsed_chat_id is not None else _chat_id_fallback()
            settings["enabled"] = not bool(settings.get("enabled", False))
            save_memory(memory)
            await query.edit_message_text(
                format_funny_status(settings, state),
                reply_markup=_funny_menu_keyboard(chat_id, settings),
            )
            return

        if sub == "sources":
            parsed_chat_id = _part_int(3)
            chat_id = parsed_chat_id if parsed_chat_id is not None else _chat_id_fallback()
            known_sources = _known_scan_sources(memory, settings)
            await query.edit_message_text(
                format_funny_sources(settings, known_sources=known_sources),
                reply_markup=_funny_sources_keyboard(chat_id, known_sources),
            )
            return

        if sub == "source_toggle" and len(parts) >= 5:
            source_chat_id = _part_int(3)
            chat_id = _part_int(4)
            if source_chat_id is None or chat_id is None:
                await query.answer("битый callback", show_alert=True)
                return
            known_by_id = {int(item["chat_id"]): item for item in _known_scan_sources(memory, settings)}
            if source_chat_id not in {int(s.get("chat_id", 0)) for s in settings.get("sources", []) if isinstance(s, dict)}:
                title = str(known_by_id.get(source_chat_id, {}).get("title") or f"chat {source_chat_id}")
                upsert_source(settings, source_chat_id, title=title, enabled=True)
            else:
                toggle_source(settings, source_chat_id)
            save_memory(memory)
            known_sources = _known_scan_sources(memory, settings)
            await query.edit_message_text(
                format_funny_sources(settings, known_sources=known_sources),
                reply_markup=_funny_sources_keyboard(chat_id, known_sources),
            )
            return

        if sub == "period":
            parsed_chat_id = _part_int(3)
            chat_id = parsed_chat_id if parsed_chat_id is not None else _chat_id_fallback()
            await query.edit_message_text(
                f"период анализа сейчас: {settings.get('scan_period_hours', 24)}h",
                reply_markup=_funny_period_keyboard(chat_id),
            )
            return

        if sub == "period_set" and len(parts) >= 5:
            parsed_hours = _part_int(3)
            chat_id = _part_int(4)
            if parsed_hours is None or chat_id is None:
                await query.answer("битый callback", show_alert=True)
                return
            hours = max(1, min(24 * 30, parsed_hours))
            settings["scan_period_hours"] = hours
            save_memory(memory)
            await query.edit_message_text(
                f"период анализа обновлен: {hours}h",
                reply_markup=_funny_period_keyboard(chat_id),
            )
            return

        if sub == "intensity":
            parsed_chat_id = _part_int(3)
            chat_id = parsed_chat_id if parsed_chat_id is not None else _chat_id_fallback()
            await query.edit_message_text(
                f"intensity сейчас: {settings.get('intensity', 'balanced')}",
                reply_markup=_funny_intensity_keyboard(chat_id, str(settings.get("intensity", "balanced"))),
            )
            return

        if sub == "intensity_set" and len(parts) >= 5:
            intensity = str(parts[3])
            chat_id = _part_int(4)
            if chat_id is None:
                await query.answer("битый callback", show_alert=True)
                return
            if intensity not in {"cheap", "balanced", "deep"}:
                await query.answer("неизвестный intensity", show_alert=True)
                return
            apply_intensity_profile(settings, intensity)
            save_memory(memory)
            await query.edit_message_text(
                format_funny_status(settings, state),
                reply_markup=_funny_menu_keyboard(chat_id, settings),
            )
            return

        if sub == "threshold":
            parsed_chat_id = _part_int(3)
            chat_id = parsed_chat_id if parsed_chat_id is not None else _chat_id_fallback()
            await query.edit_message_text(
                f"review threshold сейчас: {settings.get('review_threshold', 70)}",
                reply_markup=_funny_threshold_keyboard(chat_id),
            )
            return

        if sub == "threshold_set" and len(parts) >= 5:
            parsed_value = _part_int(3)
            chat_id = _part_int(4)
            if parsed_value is None or chat_id is None:
                await query.answer("битый callback", show_alert=True)
                return
            value = max(0, min(100, parsed_value))
            settings["review_threshold"] = value
            save_memory(memory)
            await query.edit_message_text(
                f"review threshold обновлен: {value}",
                reply_markup=_funny_threshold_keyboard(chat_id),
            )
            return

        if sub == "limits":
            parsed_chat_id = _part_int(3)
            chat_id = parsed_chat_id if parsed_chat_id is not None else _chat_id_fallback()
            await query.edit_message_text(
                (
                    "текущие лимиты:\n"
                    f"- max_candidates_per_scan={settings.get('max_candidates_per_scan', 30)}\n"
                    f"- max_llm_candidates_per_scan={settings.get('max_llm_candidates_per_scan', 12)}\n"
                    f"- daily_forward_limit={settings.get('daily_forward_limit', 20)}"
                ),
                reply_markup=_funny_limits_keyboard(chat_id),
            )
            return

        if sub == "limit_set" and len(parts) >= 6:
            field = str(parts[3])
            value = _part_int(4)
            chat_id = _part_int(5)
            if value is None or chat_id is None:
                await query.answer("битый callback", show_alert=True)
                return
            if field not in {"max_candidates_per_scan", "max_llm_candidates_per_scan", "daily_forward_limit"}:
                await query.answer("неизвестный лимит", show_alert=True)
                return
            if field == "max_candidates_per_scan":
                settings[field] = max(1, min(300, value))
            elif field == "max_llm_candidates_per_scan":
                settings[field] = max(1, min(100, value))
            else:
                settings[field] = max(1, min(500, value))
            save_memory(memory)
            await query.edit_message_text(
                (
                    "лимиты обновлены:\n"
                    f"- max_candidates_per_scan={settings.get('max_candidates_per_scan')}\n"
                    f"- max_llm_candidates_per_scan={settings.get('max_llm_candidates_per_scan')}\n"
                    f"- daily_forward_limit={settings.get('daily_forward_limit')}"
                ),
                reply_markup=_funny_limits_keyboard(chat_id),
            )
            return

        if sub == "budget":
            parsed_chat_id = _part_int(3)
            chat_id = parsed_chat_id if parsed_chat_id is not None else _chat_id_fallback()
            await query.edit_message_text(
                (
                    "токен-бюджет:\n"
                    f"- daily_token_budget={settings.get('daily_token_budget', 50000)}\n"
                    f"- daily_token_hard_stop={settings.get('daily_token_hard_stop', 55000)}\n"
                    f"- today used={state.get('budget', {}).get('tokens_used', 0)}"
                ),
                reply_markup=_funny_budget_keyboard(chat_id),
            )
            return

        if sub == "budget_set" and len(parts) >= 5:
            parsed_value = _part_int(3)
            chat_id = _part_int(4)
            if parsed_value is None or chat_id is None:
                await query.answer("битый callback", show_alert=True)
                return
            value = max(1000, min(10_000_000, parsed_value))
            settings["daily_token_budget"] = value
            settings["daily_token_hard_stop"] = max(int(settings.get("daily_token_hard_stop", value)), value)
            save_memory(memory)
            await query.edit_message_text(
                f"daily_token_budget обновлен: {value}",
                reply_markup=_funny_budget_keyboard(chat_id),
            )
            return

        if sub == "scan_now":
            parsed_chat_id = _part_int(3)
            chat_id = parsed_chat_id if parsed_chat_id is not None else _chat_id_fallback()
            await query.edit_message_text("сканирую сейчас, подожди пару секунд...")
            summary = await _run_funny_scan_once(context.application, trigger="manual")
            async with _FUNNY_SCAN_STATE_LOCK:
                state = _load_funny_scan_state()
            text = format_funny_status(settings, state) + "\n\n" + (
                f"scan result: sources={summary.get('sources', 0)}, stage1={summary.get('stage1_candidates', 0)}, "
                f"llm={summary.get('llm_calls', 0)}, created={summary.get('created', 0)}, previewed={summary.get('previewed', 0)}"
            )
            await query.edit_message_text(text, reply_markup=_funny_menu_keyboard(chat_id, settings))
            return

        if sub == "list":
            parsed_chat_id = _part_int(3)
            chat_id = parsed_chat_id if parsed_chat_id is not None else _chat_id_fallback()
            items = list_candidates(state, status=STATUS_NEW, limit=20)
            await query.edit_message_text(
                format_funny_candidates_list(items),
                reply_markup=_funny_candidates_keyboard(chat_id, items),
            )
            return

        if sub == "open" and len(parts) >= 4:
            candidate_id = str(parts[3])
            candidate = get_candidate(state, candidate_id)
            if not candidate:
                await query.answer("кандидат не найден", show_alert=True)
                return
            chat_id = _resolve_funny_chat_id_from_candidate(candidate, _chat_id_fallback())
            await query.edit_message_text(
                format_funny_candidate_preview(candidate),
                reply_markup=_funny_preview_keyboard(chat_id, candidate_id),
            )
            return

        if sub in {"approve", "reject", "retry"} and len(parts) >= 4:
            candidate_id = str(parts[3])
            async with _FUNNY_SCAN_STATE_LOCK:
                state = _load_funny_scan_state()
                candidate = get_candidate(state, candidate_id)
            if not candidate:
                await query.answer("кандидат не найден", show_alert=True)
                return
            chat_id = _resolve_funny_chat_id_from_candidate(candidate, _chat_id_fallback())
            if sub == "reject":
                ok, message_text = await _reject_funny_candidate(candidate_id)
                async with _FUNNY_SCAN_STATE_LOCK:
                    state = _load_funny_scan_state()
                    candidate = get_candidate(state, candidate_id) or candidate
                await query.edit_message_text(
                    (format_funny_candidate_preview(candidate) + "\n\n" + message_text)[:3900],
                    reply_markup=_funny_preview_keyboard(chat_id, candidate_id),
                )
                if not ok:
                    await query.answer(message_text, show_alert=True)
                return

            ok, message_text = await _forward_funny_candidate(
                bot=context.bot,
                settings=settings,
                candidate_id=candidate_id,
                action=sub,
            )
            async with _FUNNY_SCAN_STATE_LOCK:
                state = _load_funny_scan_state()
                candidate = get_candidate(state, candidate_id) or candidate
            text = format_funny_candidate_preview(candidate) + "\n\n" + message_text
            await query.edit_message_text(
                text[:3900],
                reply_markup=_funny_preview_keyboard(chat_id, candidate_id),
            )
            if ok:
                await query.answer("отправил форвард в личку")
            return

        await query.answer("непонятная funny-команда", show_alert=True)
        return

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
        if input_kind in {"system_prompt", "style", "bio", "heat", "funny_period", "funny_threshold", "funny_budget"}:
            _set_admin_pending(context, {"action": input_kind})
            hints = {
                "system_prompt": "пришли новый system prompt одним сообщением",
                "style": "пришли новый style settings одним сообщением",
                "bio": "пришли новое био тимура одним сообщением",
                "heat": "пришли число 0..100",
                "funny_period": "пришли период сканирования в часах (например 24)",
                "funny_threshold": "пришли review threshold 0..100",
                "funny_budget": "пришли daily token budget (например 50000)",
            }
            back_target = f"adm:funny:menu:{chat_id}" if input_kind.startswith("funny_") else f"adm:root:{chat_id}"
            await query.edit_message_text(
                hints[input_kind] + "\n/cancel чтобы отменить",
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("назад", callback_data=back_target)]]
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


async def web_app_data_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    del context
    message = update.effective_message
    if not message or not message.from_user:
        return
    if message.from_user.id != OWNER_ID:
        logger.warning("Unauthorized web_app_data from %s", message.from_user.id)
        return
    web_app_data = getattr(message, "web_app_data", None)
    if not web_app_data or not getattr(web_app_data, "data", ""):
        return

    try:
        raw = json.loads(web_app_data.data)
    except Exception:
        await message.reply_text("mini app прислал битый payload", reply_markup=ReplyKeyboardRemove())
        return

    action_type = str(raw.get("type") or "").strip().lower()
    payload = raw.get("payload") or {}
    target_chat_id = int(payload.get("chat_id") or message.chat_id)
    memory = load_memory()

    try:
        if action_type == "admin_config":
            reply_text = apply_miniapp_admin_config(memory, target_chat_id, payload)
        elif action_type == "billing_quote_request":
            reply_text = build_miniapp_quote_text(target_chat_id, str(payload.get("mode") or "owner"))
        else:
            reply_text = "mini app прислал неизвестное действие"
    except BillingError as e:
        reply_text = f"billing error: {e}"
    except Exception as e:
        logger.error("Mini app payload processing failed: %s", e)
        reply_text = f"mini app ошибка: {e}"

    await message.reply_text(reply_text, reply_markup=ReplyKeyboardRemove())


# =========================
# TELEGRAM HANDLERS
# =========================

async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    del context
    if update.message:
        await update.message.reply_text("я тут давай базарь")


async def story_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.message
    if not message:
        return

    memory = load_memory()
    parts = (message.text or "").split(" ", 1)
    arg = parts[1].strip().lower() if len(parts) > 1 else ""
    force_new = arg in {"new", "новая", "новую", "fresh"}

    entry = None if force_new else _get_last_story(memory, chat_id=message.chat_id)
    if entry:
        story_text = str(entry.get("text", "")).strip()
    else:
        story_text = await _run_with_typing(
            context,
            message.chat_id,
            _generate_story_text(memory, proactive=False),
        )
        _append_story_log(memory, story_text, source="command", chat_id=message.chat_id)
        save_memory(memory)

    story_text = enforce_reply_guardrails(story_text)
    if not story_text:
        story_text = "сегодня без лора давай позже"
    await message.reply_text(story_text)


async def text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    memory = load_memory()
    message = update.effective_message

    if not message or not message.from_user:
        return

    if await _handle_admin_pending_text(update, context, memory):
        return

    if await _handle_text_feedback(update, memory):
        return

    logger.info(
        "Входящее текстовое сообщение: user_id=%s username=%s chat_id=%s текст=%s",
        message.from_user.id,
        message.from_user.username,
        message.chat_id,
        message.text,
    )

    event_key = _make_event_key("text", message.chat_id, message.message_id)
    if not _try_acquire_inflight_event(event_key):
        logger.info("duplicate skipped: inflight key=%s", event_key)
        return
    try:
        chat_mem = get_chat_mem(memory, message.chat_id)
        if _is_processed_event(chat_mem, event_key):
            logger.info("duplicate skipped: processed key=%s", event_key)
            return
        _mark_processed_event(chat_mem, event_key)
        update_memory_with_message(memory, message)

        user_text = _extract_message_text(message)
        if _looks_like_story_request(user_text):
            entry = _get_last_story(memory, chat_id=message.chat_id)
            if entry:
                story_text = str(entry.get("text", "")).strip()
            else:
                story_text = await _run_with_typing(
                    context,
                    message.chat_id,
                    _generate_story_text(memory, proactive=False),
                )
                _append_story_log(memory, story_text, source="on_demand", chat_id=message.chat_id)
                save_memory(memory)
            await send_reply_with_style(update, context, memory, story_text, humor_plan=None)
            return

        bot_id = (await context.bot.get_me()).id

        decision = should_reply_decision(memory, message, bot_id)
        _log_reply_decision("тексту", decision)
        if not decision.should_reply:
            return

        logger.info(
            "Готовлю текстовый ответ: режим=%s токсичность=%s источник=LLM",
            get_active_mode(memory),
            get_effective_toxicity_level(memory),
        )
        humor_plan = build_humor_plan(memory, message)
        messages = build_chat_messages(memory, message, humor_plan=humor_plan)
        reply_text = await _run_with_typing(context, message.chat_id, call_openai_text(messages))

        force_voice = is_voice_codeword(user_text)
        await send_reply_with_style(
            update,
            context,
            memory,
            reply_text,
            force_voice=force_voice,
            humor_plan=humor_plan,
        )
    finally:
        _release_inflight_event(event_key)


async def photo_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    memory = load_memory()
    message = update.effective_message

    if not message or not message.from_user:
        return

    user = message.from_user
    chat_id = message.chat_id

    logger.info(
        "Входящее фото: user_id=%s username=%s chat_id=%s",
        user.id,
        user.username,
        chat_id,
    )

    event_key = _make_event_key("photo", message.chat_id, message.message_id)
    if not _try_acquire_inflight_event(event_key):
        logger.info("duplicate skipped: inflight key=%s", event_key)
        return
    try:
        chat_mem = get_chat_mem(memory, message.chat_id)
        if _is_processed_event(chat_mem, event_key):
            logger.info("duplicate skipped: processed key=%s", event_key)
            return
        _mark_processed_event(chat_mem, event_key)
        update_memory_with_message(memory, message)

        bot_id = (await context.bot.get_me()).id

        decision = ReplyDecision(False, "нет триггера для ответа на фото")

        if message.reply_to_message and message.reply_to_message.from_user:
            if message.reply_to_message.from_user.id == bot_id:
                decision = ReplyDecision(True, "фото отправлено в ответ на сообщение Тимура")

        if not decision.should_reply and (message.caption or "") and is_name_mentioned(message.caption):
            decision = ReplyDecision(True, "в подписи к фото упомянуто имя Тимура")

        if not decision.should_reply:
            roll = random.random()
            decision = ReplyDecision(
                roll < PHOTO_RANDOM_REPLY_CHANCE,
                "случайный ответ на фото по вероятности",
                threshold=PHOTO_RANDOM_REPLY_CHANCE,
                roll=roll,
            )

        _log_reply_decision("фото", decision)
        if not decision.should_reply:
            return

        if not can_use_vision(memory, chat_id, user.id):
            logger.info("Лимит vision исчерпан, фото пропускаю")
            return

        photo = message.photo[-1]
        file = await context.bot.get_file(photo.file_id)
        file_bytes = await file.download_as_bytearray()
        image_b64 = base64.b64encode(file_bytes).decode("utf-8")

        increase_vision_counters(memory, chat_id, user.id)

        reply_text = await _run_with_typing(context, message.chat_id, call_openai_vision(memory, message, image_b64))
        reply_text = sanitize_reply_text(reply_text)

        if not reply_text:
            logger.info("Vision-ответ получился пустым, пропускаю отправку")
            return

        await send_reply_with_style(update, context, memory, reply_text, humor_plan=None)
    finally:
        _release_inflight_event(event_key)


# =========================
# АДМИН-КОМАНДЫ
# =========================

def owner_only(func):
    @functools.wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        user = update.effective_user

        if not user or user.id != OWNER_ID:
            logger.warning("Неавторизованная admin-команда от user_id=%s", user.id if user else "unknown")
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
async def mode_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    del context
    message = update.message
    if not message:
        return

    memory = load_memory()
    parts = (message.text or "").split(" ", 1)
    if len(parts) < 2 or not parts[1].strip():
        current = get_active_mode(memory)
        modes = ", ".join(PERSONA_MODES.keys())
        await message.reply_text(f"текущий режим: {current}\nдоступные: {modes}\nиспользование: /mode <режим>")
        return

    requested = parts[1].strip().lower()
    if requested not in PERSONA_MODES:
        modes = ", ".join(PERSONA_MODES.keys())
        await message.reply_text(f"неизвестный режим: {requested}\nдоступные: {modes}")
        return

    memory.setdefault("config", {})["active_mode"] = requested
    save_memory(memory)
    await message.reply_text(f"режим переключен: {requested}")


@owner_only
async def setmode_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await mode_cmd(update, context)


@owner_only
async def showmode_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    del context
    message = update.message
    if not message:
        return
    memory = load_memory()
    await message.reply_text(f"текущий режим: {get_active_mode(memory)}")


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
async def bit_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    del context
    message = update.message
    if not message:
        return
    parts = (message.text or "").split(" ", 1)
    if len(parts) < 2 or not parts[1].strip():
        await message.reply_text("после /bit нужен текст прикола")
        return
    memory = load_memory()
    chat_mem = get_chat_mem(memory, message.chat_id)
    bit = add_joke_bit(chat_mem, parts[1].strip(), source="manual", weight=3.0)
    save_memory(memory)
    await message.reply_text(f"bit добавлен: {bit['text']}")


@owner_only
async def bits_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    del context
    message = update.message
    if not message:
        return
    memory = load_memory()
    chat_mem = get_chat_mem(memory, message.chat_id)
    await message.reply_text(format_bits(chat_mem))


@owner_only
async def funny_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    del context
    message = update.message
    if not message:
        return
    memory = load_memory()
    ok = _apply_feedback_to_reply(memory, message, "funny", source="owner_command")
    await message.reply_text("засчитал funny" if ok else "не нашел этот ответ в bot_outputs")


@owner_only
async def unfunny_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    del context
    message = update.message
    if not message:
        return
    memory = load_memory()
    ok = _apply_feedback_to_reply(memory, message, "unfunny", source="owner_command")
    await message.reply_text("засчитал unfunny" if ok else "не нашел этот ответ в bot_outputs")


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
