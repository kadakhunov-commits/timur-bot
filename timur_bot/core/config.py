from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Set

import yaml
from dotenv import load_dotenv


class ConfigError(RuntimeError):
    pass


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


def _coerce_set(items: Any, key: str) -> Set[str]:
    if items is None:
        return set()
    if not isinstance(items, list):
        raise ConfigError(f"Expected list for `{key}`")
    return {str(x).strip() for x in items if str(x).strip()}


def load_app_config(base_dir: Path | None = None) -> AppConfig:
    root = (base_dir or Path(__file__).resolve().parents[2]).resolve()
    load_dotenv(root / ".env")

    persona = _read_yaml(root / "config" / "persona.yaml")
    lexicon = _read_yaml(root / "config" / "lexicon.yaml")
    runtime = _read_yaml(root / "config" / "runtime.yaml")

    telegram_bot_token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    openai_api_key = os.getenv("OPENAI_API_KEY", "").strip()
    openai_base_url = os.getenv("OPENAI_BASE_URL", "").strip()
    gemini_api_key = os.getenv("GEMINI_API_KEY", "").strip()
    miniapp_url = os.getenv("MINIAPP_URL", "").strip()
    openai_text_model = os.getenv("OPENAI_TEXT_MODEL", "").strip()
    openai_vision_model = os.getenv("OPENAI_VISION_MODEL", "").strip()
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

    active_mode = str(defaults.get("active_mode", "default"))
    if active_mode not in modes:
        active_mode = "default" if "default" in modes else next(iter(modes.keys()))
    memory_path_env = os.getenv("MEMORY_PATH", "").strip()
    billing_path_env = os.getenv("BILLING_PATH", "").strip()

    return AppConfig(
        base_dir=root,
        memory_path=Path(memory_path_env) if memory_path_env else root / "memory.json",
        billing_path=Path(billing_path_env) if billing_path_env else root / "billing_state.json",
        telegram_bot_token=telegram_bot_token,
        openai_api_key=openai_api_key,
        openai_base_url=openai_base_url,
        gemini_api_key=gemini_api_key,
        miniapp_url=miniapp_url,
        owner_id=int(runtime.get("owner_id", 428469927)),
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
    )
