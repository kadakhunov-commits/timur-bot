from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Set

import yaml
from dotenv import load_dotenv

DEFAULT_MINIAPP_URL = ""
DEAD_MINIAPP_HOSTS = ("albasty-5ba44.web.app", "timur-bot-91825649.web.app")


class ConfigError(RuntimeError):
    pass


def _resolve_miniapp_url() -> str:
    raw_url = os.getenv("MINIAPP_URL", "").strip()
    if raw_url and any(host in raw_url for host in DEAD_MINIAPP_HOSTS):
        return ""
    return raw_url or DEFAULT_MINIAPP_URL


@dataclass(frozen=True)
class AppConfig:
    base_dir: Path
    memory_path: Path
    billing_path: Path
    telegram_bot_token: str
    openai_api_key: str
    openai_base_url: str
    gemini_api_key: str
    miniapp_url: str
    owner_id: int
    owner_ids: List[int]
    premium_chat_ids: List[int]
    text_model: str
    vision_model: str
    voice_model: str
    voice_name: str
    voice_style_prompt: str
    max_history_per_chat: int
    max_log_per_chat: int
    max_user_samples: int
    max_quotes_per_user: int
    max_keywords_per_user: int
    max_topic_edges: int
    max_user_relations: int
    global_daily_vision_limit: int
    chat_daily_vision_limit: int
    user_daily_vision_limit: int
    global_daily_voice_limit: int
    chat_daily_voice_limit: int
    max_voice_chars: int
    base_reply_chance: float
    chain_reply_chance: float
    mem_reply_chance: float
    photo_random_reply_chance: float
    voice_reply_chance: float
    memes: List[str]
    youtube_links: List[str]
    rus_stopwords: Set[str]
    en_stopwords: Set[str]
    profanity_markers: Set[str]
    archetype_lexicon: Dict[str, Set[str]]
    persona_modes: Dict[str, str]
    default_system_prompt: str
    default_style_settings: str
    default_bio: str
    default_toxicity_level: int
    default_active_mode: str
    adaptive_humor_defaults: Dict[str, Any]
    funny_scan_defaults: Dict[str, Any]
    funny_scan_lexicon: Dict[str, Any]
    mood_events_catalog: Dict[str, Any]


def _read_yaml(path: Path) -> Dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
    except FileNotFoundError as e:
        raise ConfigError(f"Missing config file: {path}") from e
    except yaml.YAMLError as e:
        raise ConfigError(f"Invalid YAML in {path}: {e}") from e
    if not isinstance(data, dict):
        raise ConfigError(f"Config file must contain mapping object: {path}")
    return data


def _read_yaml_optional(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    return _read_yaml(path)


def _coerce_set(items: Any, key: str) -> Set[str]:
    if items is None:
        return set()
    if not isinstance(items, list):
        raise ConfigError(f"Expected list for `{key}`")
    return {str(x).strip() for x in items if str(x).strip()}


def _coerce_str_list(items: Any) -> List[str]:
    if not isinstance(items, list):
        return []
    return [str(x).strip() for x in items if str(x).strip()]


def _normalize_funny_scan_defaults(raw: Any) -> Dict[str, Any]:
    data = raw if isinstance(raw, dict) else {}
    intensity_profiles_raw = data.get("intensity_profiles") if isinstance(data.get("intensity_profiles"), dict) else {}
    default_profiles = {
        "cheap": {
            "stage1_min_score": 50,
            "review_threshold": 78,
            "max_candidates_per_scan": 20,
            "max_llm_candidates_per_scan": 6,
            "llm_max_context_messages": 8,
        },
        "balanced": {
            "stage1_min_score": 42,
            "review_threshold": 70,
            "max_candidates_per_scan": 30,
            "max_llm_candidates_per_scan": 12,
            "llm_max_context_messages": 12,
        },
        "deep": {
            "stage1_min_score": 35,
            "review_threshold": 64,
            "max_candidates_per_scan": 45,
            "max_llm_candidates_per_scan": 18,
            "llm_max_context_messages": 14,
        },
    }
    profiles: Dict[str, Dict[str, int]] = {}
    for mode, mode_defaults in default_profiles.items():
        source = intensity_profiles_raw.get(mode) if isinstance(intensity_profiles_raw.get(mode), dict) else {}
        profiles[mode] = {
            "stage1_min_score": int(source.get("stage1_min_score", mode_defaults["stage1_min_score"])),
            "review_threshold": int(source.get("review_threshold", mode_defaults["review_threshold"])),
            "max_candidates_per_scan": int(source.get("max_candidates_per_scan", mode_defaults["max_candidates_per_scan"])),
            "max_llm_candidates_per_scan": int(
                source.get("max_llm_candidates_per_scan", mode_defaults["max_llm_candidates_per_scan"])
            ),
            "llm_max_context_messages": int(
                source.get("llm_max_context_messages", mode_defaults["llm_max_context_messages"])
            ),
        }
    return {
        "enabled": bool(data.get("enabled", False)),
        "gluboko_chat_id": int(data.get("gluboko_chat_id", 0) or 0),
        "main_chat_id": int(data.get("main_chat_id", 0) or 0),
        "backfill_start_date_msk": str(data.get("backfill_start_date_msk", "") or "").strip(),
        "owner_delivery_mode": str(data.get("owner_delivery_mode", "auto_forward") or "auto_forward"),
        "rule_min_hearts": int(data.get("rule_min_hearts", 3)),
        "rule_min_laugh_markers": int(data.get("rule_min_laugh_markers", 2)),
        "scan_period_hours": int(data.get("scan_period_hours", 24)),
        "scan_schedule_minutes": int(data.get("scan_schedule_minutes", 60)),
        "intensity": str(data.get("intensity", "balanced")),
        "stage1_min_score": int(data.get("stage1_min_score", 42)),
        "review_threshold": int(data.get("review_threshold", 70)),
        "max_candidates_per_scan": int(data.get("max_candidates_per_scan", 30)),
        "max_llm_candidates_per_scan": int(data.get("max_llm_candidates_per_scan", 12)),
        "daily_token_budget": int(data.get("daily_token_budget", 50000)),
        "daily_token_hard_stop": int(data.get("daily_token_hard_stop", 55000)),
        "daily_forward_limit": int(data.get("daily_forward_limit", 20)),
        "llm_model": str(data.get("llm_model", "gpt-4o-mini")),
        "llm_max_context_messages": int(data.get("llm_max_context_messages", 12)),
        "llm_max_chars_per_message": int(data.get("llm_max_chars_per_message", 220)),
        "intensity_profiles": profiles,
    }


def _normalize_adaptive_humor_defaults(raw: Any) -> Dict[str, Any]:
    data = raw if isinstance(raw, dict) else {}
    return {
        "schema_version": 3,
        "enabled": bool(data.get("enabled", True)),
        "auto_learn": bool(data.get("auto_learn", True)),
        "live_snipe_enabled": bool(data.get("live_snipe_enabled", True)),
        "participation_rate": max(0.0, min(1.0, float(data.get("participation_rate", 0.45)))),
        "min_human_messages_between_replies": max(1, int(data.get("min_human_messages_between_replies", 3))),
        "min_human_messages_between_checks": max(1, int(data.get("min_human_messages_between_checks", 3))),
        "interjection_timeout_seconds": max(1, min(10, int(data.get("interjection_timeout_seconds", 3)))),
        "reply_timeout_seconds": max(1, min(15, int(data.get("reply_timeout_seconds", 6)))),
        "dialogue_window_minutes": max(1, int(data.get("dialogue_window_minutes", 10))),
        "snipe_cooldown_minutes": max(1, int(data.get("snipe_cooldown_minutes", 10))),
        "min_human_messages": max(1, int(data.get("min_human_messages", 3))),
        "candidate_threshold": max(0, min(100, int(data.get("candidate_threshold", 85)))),
        "director_max_tokens": max(80, min(300, int(data.get("director_max_tokens", 180)))),
        "critic_max_tokens": max(20, min(100, int(data.get("critic_max_tokens", 40)))),
        "background_daily_token_budget": max(1000, min(100000, int(data.get("background_daily_token_budget", 12000)))),
        "ambient_reply_max_chars": max(20, min(120, int(data.get("ambient_reply_max_chars", 60)))),
        "direct_reply_max_chars": max(40, min(500, int(data.get("direct_reply_max_chars", 120)))),
    }


def _normalize_funny_scan_lexicon(raw: Any) -> Dict[str, Any]:
    data = raw if isinstance(raw, dict) else {}
    reaction_weights_raw = data.get("reaction_weights") if isinstance(data.get("reaction_weights"), dict) else {}
    return {
        "laugh_markers": _coerce_str_list(data.get("laugh_markers"))
        or ["лол", "ахах", "ахаха", "хаха", "пхаха", "ор", "ору", "ржака", "вынесло", "угар"],
        "habitual_laugh_markers": _coerce_str_list(data.get("habitual_laugh_markers"))
        or ["ахах", "ахаха", "хаха", "лол"],
        "sarcasm_markers": _coerce_str_list(data.get("sarcasm_markers"))
        or ["ага", "ну да", "конечно", "смешно", "ирония", "сарказм"],
        "toxicity_markers": _coerce_str_list(data.get("toxicity_markers"))
        or ["сук", "бля", "хуй", "пизд", "еб", "идиот", "дебил"],
        "noise_markers": _coerce_str_list(data.get("noise_markers")) or ["кринж", "жесть", "мда"],
        "heart_emojis": _coerce_str_list(data.get("heart_emojis")) or ["❤", "❤️"],
        "laugh_emojis": _coerce_str_list(data.get("laugh_emojis")) or ["😂", "🤣", "😹", "😆"],
        "extra_laugh_markers": _coerce_str_list(data.get("extra_laugh_markers")) or ["сука", "мука", "бля", "лол", "лоо"],
        "reaction_weights": {
            "total": float(reaction_weights_raw.get("total", 0.35)),
            "heart": float(reaction_weights_raw.get("heart", 1.4)),
            "laugh": float(reaction_weights_raw.get("laugh", 1.2)),
        },
        "pure_laugh_pattern": str(
            data.get(
                "pure_laugh_pattern",
                r"^(?:[!?.\s,;:()\-]*)(?:л+о+л+|а?ха(?:ха)+|пхаха+|ору+|кек+)(?:[!?.\s,;:()\-]*)$",
            )
        ),
    }


def _default_mood_events_catalog() -> Dict[str, Any]:
    return {
        "defaults": {
            "event_interval_hours_min": 3,
            "event_interval_hours_max": 6,
            "decay_hours_min": 4,
            "decay_hours_max": 8,
            "baseline_valence": 8,
            "baseline_energy": 52,
            "default_guard_level": 55,
            "default_chat_openness": 50,
        },
        "events": [],
    }


def _normalize_mood_events_catalog(raw: Any) -> Dict[str, Any]:
    base = _default_mood_events_catalog()
    if not isinstance(raw, dict):
        return base
    defaults = raw.get("defaults") if isinstance(raw.get("defaults"), dict) else {}
    merged_defaults = dict(base["defaults"])
    merged_defaults.update(defaults)
    events = raw.get("events") if isinstance(raw.get("events"), list) else []
    normalized_events: List[Dict[str, Any]] = []
    for item in events:
        if not isinstance(item, dict):
            continue
        key = str(item.get("key", "")).strip()
        if not key:
            continue
        normalized_events.append(item)
    return {
        "defaults": merged_defaults,
        "events": normalized_events,
    }


def load_app_config(base_dir: Path | None = None) -> AppConfig:
    root = (base_dir or Path(__file__).resolve().parents[2]).resolve()
    load_dotenv(root / ".env")

    persona = _read_yaml(root / "config" / "persona.yaml")
    lexicon = _read_yaml(root / "config" / "lexicon.yaml")
    runtime = _read_yaml(root / "config" / "runtime.yaml")
    mood_events_raw = _read_yaml_optional(root / "config" / "mood_events.yaml")

    telegram_bot_token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    openai_api_key = os.getenv("OPENAI_API_KEY", "").strip()
    openai_base_url = os.getenv("OPENAI_BASE_URL", "").strip()
    gemini_api_key = os.getenv("GEMINI_API_KEY", "").strip()
    miniapp_url = _resolve_miniapp_url()
    openai_text_model = os.getenv("OPENAI_TEXT_MODEL", "").strip()
    openai_vision_model = os.getenv("OPENAI_VISION_MODEL", "").strip()
    memory_path_env = os.getenv("MEMORY_PATH", "").strip()
    billing_path_env = os.getenv("BILLING_PATH", "").strip()
    if not telegram_bot_token:
        raise ConfigError("TELEGRAM_BOT_TOKEN not set in .env")
    if not openai_api_key:
        raise ConfigError("OPENAI_API_KEY not set in .env")

    defaults = persona.get("defaults") or {}
    modes = persona.get("modes") or {}
    if not isinstance(modes, dict) or not modes:
        raise ConfigError("persona.yaml: `modes` must be a non-empty mapping")

    archetype_raw = lexicon.get("archetype_lexicon") or {}
    if not isinstance(archetype_raw, dict):
        raise ConfigError("lexicon.yaml: `archetype_lexicon` must be a mapping")
    archetype_lexicon: Dict[str, Set[str]] = {
        str(k): _coerce_set(v, f"archetype_lexicon.{k}") for k, v in archetype_raw.items()
    }

    limits = runtime.get("limits") or {}
    probs = runtime.get("probabilities") or {}
    models = runtime.get("models") or {}
    funny_scan_defaults = _normalize_funny_scan_defaults(runtime.get("funny_scan"))
    adaptive_humor_defaults = _normalize_adaptive_humor_defaults(runtime.get("adaptive_humor"))
    funny_scan_lexicon = _normalize_funny_scan_lexicon(lexicon.get("funny_scan_lexicon"))
    mood_events_catalog = _normalize_mood_events_catalog(mood_events_raw)

    active_mode = str(defaults.get("active_mode", "default"))
    if active_mode not in modes:
        active_mode = "default" if "default" in modes else next(iter(modes.keys()))
    memory_path_env = os.getenv("MEMORY_PATH", "").strip()
    billing_path_env = os.getenv("BILLING_PATH", "").strip()

    owner_id = int(runtime.get("owner_id", 428469927))
    owner_ids_raw = runtime.get("owner_ids") if isinstance(runtime.get("owner_ids"), list) else []
    owner_ids: List[int] = []
    for item in owner_ids_raw:
        try:
            parsed = int(item)
        except Exception:
            continue
        if parsed not in owner_ids:
            owner_ids.append(parsed)
    if owner_id not in owner_ids:
        owner_ids.insert(0, owner_id)

    premium_chat_ids_raw = runtime.get("premium_chat_ids") if isinstance(runtime.get("premium_chat_ids"), list) else []
    premium_chat_ids: List[int] = []
    for item in premium_chat_ids_raw:
        try:
            parsed = int(item)
        except Exception:
            continue
        if parsed not in premium_chat_ids:
            premium_chat_ids.append(parsed)

    return AppConfig(
        base_dir=root,
        memory_path=Path(memory_path_env) if memory_path_env else root / "memory.json",
        billing_path=Path(billing_path_env) if billing_path_env else root / "billing_state.json",
        telegram_bot_token=telegram_bot_token,
        openai_api_key=openai_api_key,
        openai_base_url=openai_base_url,
        gemini_api_key=gemini_api_key,
        miniapp_url=miniapp_url,
        owner_id=owner_id,
        owner_ids=owner_ids,
        premium_chat_ids=premium_chat_ids,
        text_model=openai_text_model or str(models.get("text", "gpt-4o-mini")),
        vision_model=openai_vision_model or str(models.get("vision", "gpt-4o-mini")),
        voice_model=str(models.get("voice", "gemini-3.1-flash-tts-preview")),
        voice_name=str(models.get("voice_name", "Kore")),
        voice_style_prompt=str(
            models.get(
                "voice_style_prompt",
                "[slightly raspy] [casual, confident, warm] [light caucasian accent]",
            )
        ).strip(),
        max_history_per_chat=int(limits.get("max_history_per_chat", 100)),
        max_log_per_chat=int(limits.get("max_log_per_chat", 1000)),
        max_user_samples=int(limits.get("max_user_samples", 20)),
        max_quotes_per_user=int(limits.get("max_quotes_per_user", 6)),
        max_keywords_per_user=int(limits.get("max_keywords_per_user", 40)),
        max_topic_edges=int(limits.get("max_topic_edges", 300)),
        max_user_relations=int(limits.get("max_user_relations", 300)),
        global_daily_vision_limit=int(limits.get("global_daily_vision_limit", 50)),
        chat_daily_vision_limit=int(limits.get("chat_daily_vision_limit", 5)),
        user_daily_vision_limit=int(limits.get("user_daily_vision_limit", 5)),
        global_daily_voice_limit=int(limits.get("global_daily_voice_limit", 3)),
        chat_daily_voice_limit=int(limits.get("chat_daily_voice_limit", 1)),
        max_voice_chars=int(limits.get("max_voice_chars", 140)),
        base_reply_chance=float(probs.get("base_reply_chance", 0.08)),
        chain_reply_chance=float(probs.get("chain_reply_chance", 0.16)),
        mem_reply_chance=float(probs.get("mem_reply_chance", 0.08)),
        photo_random_reply_chance=float(probs.get("photo_random_reply_chance", 0.22)),
        voice_reply_chance=float(probs.get("voice_reply_chance", 0.015)),
        memes=[str(x) for x in (lexicon.get("memes") or [])],
        youtube_links=[str(x) for x in (lexicon.get("youtube_links") or [])],
        rus_stopwords=_coerce_set(lexicon.get("rus_stopwords"), "rus_stopwords"),
        en_stopwords=_coerce_set(lexicon.get("en_stopwords"), "en_stopwords"),
        profanity_markers=_coerce_set(lexicon.get("profanity_markers"), "profanity_markers"),
        archetype_lexicon=archetype_lexicon,
        persona_modes={str(k): str(v) for k, v in modes.items()},
        default_system_prompt=str(persona.get("default_system_prompt", "")).strip(),
        default_style_settings=str(defaults.get("style_settings", "")),
        default_bio=str(defaults.get("bio", "")),
        default_toxicity_level=int(defaults.get("toxicity_level", 45)),
        default_active_mode=active_mode,
        adaptive_humor_defaults=adaptive_humor_defaults,
        funny_scan_defaults=funny_scan_defaults,
        funny_scan_lexicon=funny_scan_lexicon,
        mood_events_catalog=mood_events_catalog,
    )
