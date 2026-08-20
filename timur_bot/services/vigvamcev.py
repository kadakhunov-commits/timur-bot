"""Canonical story, state, and publication workflow for VIGVAMCEV."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, Iterable, List, Mapping, Sequence

import yaml
from telegram import InputFile
from telegram.error import BadRequest, Forbidden, NetworkError, TimedOut

from timur_bot.services.vigvamcev_poster import compose_poster


LOGGER = logging.getLogger("timur-bot.vigvamcev")
_WORD_RE = re.compile(r"[а-яё]{3,}", re.IGNORECASE)
_CLONE_NAME_RE = re.compile(r"\b[А-ЯЁ][а-яё]{2,32}цев\b")
_JSON_FENCE_RE = re.compile(r"^```(?:json)?\s*|\s*```$", re.IGNORECASE)
_CAUSAL_MARKERS = ("потому", "поэтому", "из-за", "после", "когда", "однако", "но ", "так что")
_CLONE_BLOCK_RE = re.compile(r"^\s*[Кк][Лл][Оо][Нн]\s*[:\-—]\s*(.+)$", re.MULTILINE | re.IGNORECASE)
_SIC_BLOCK_RE = re.compile(r"^\s*SIC\s*[:\-—]\s*(.+)$", re.MULTILINE | re.IGNORECASE)
_DEFAULT_HASHTAGS = "#лор@vigvamcev #истории2@vigvamcev"


class VigvamcevError(RuntimeError):
    """Base error for the VIGVAMCEV workflow."""


class CorpusError(VigvamcevError):
    pass


class CandidateGenerationError(VigvamcevError):
    pass


class TransientTextRequestError(VigvamcevError):
    """A text-provider failure that is safe to retry for the same candidate."""


class DraftError(VigvamcevError):
    pass


TextRequest = Callable[[str, int], Awaitable[str]]


def _as_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _normalise_story(value: Any) -> str:
    """Collapse whitespace but keep the two required blocks on separate lines."""
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    if not text:
        return text
    text = re.sub(r"(?i)\s+(?=SIC\s*[:—])", chr(10) * 2, text)
    return text


def _normalise_key(value: Any) -> str:
    return _as_text(value).casefold().replace("ё", "е")


def _normalise_word(value: Any) -> str:
    word = _as_text(value).casefold().replace("ё", "ё")
    word = re.sub(r"[^а-яё]", "", word)
    return word


def clone_name_from_word(source_word: str) -> str:
    word = _normalise_word(source_word)
    if not word:
        return ""
    return word[:1].upper() + word[1:] + "цев"


def _token_set(text: str) -> set[str]:
    stopwords = {
        "этот",
        "который",
        "чтобы",
        "потом",
        "теперь",
        "самый",
        "один",
        "одна",
        "после",
        "когда",
        "быть",
        "весь",
        "свой",
        "себя",
        "этого",
        "было",
        "есть",
        "они",
        "ему",
        "её",
        "его",
    }
    return {token.casefold() for token in _WORD_RE.findall(text) if token.casefold() not in stopwords}


def _jaccard(left: set[str], right: set[str]) -> float:
    if not left or not right:
        return 0.0
    return len(left & right) / max(1, len(left | right))


def _extract_json_object(raw: Any) -> Dict[str, Any]:
    text = _JSON_FENCE_RE.sub("", str(raw or "").strip())
    if not text:
        return {}
    try:
        value = json.loads(text)
        return value if isinstance(value, dict) else {}
    except json.JSONDecodeError:
        pass
    decoder = json.JSONDecoder()
    for index, character in enumerate(text):
        if character != "{":
            continue
        try:
            value, _ = decoder.raw_decode(text[index:])
        except json.JSONDecodeError:
            continue
        return value if isinstance(value, dict) else {}
    return {}


def _coerce_string_list(value: Any) -> list[str]:
    if not isinstance(value, (list, tuple)):
        return []
    return [_as_text(item) for item in value if _as_text(item)]


def _compact_text(text: str, limit: int = 5000) -> str:
    text = str(text or "").strip()
    if len(text) <= limit:
        return text
    left = limit // 2
    right = limit - left
    return text[:left] + "\n[… середина источника сокращена …]\n" + text[-right:]


@dataclass(frozen=True)
class CanonPost:
    post_no: int
    experiments: tuple[int, ...]
    text_path: Path
    image_path: Path | None = None
    season: int = 1


@dataclass
class CanonCorpus:
    root: Path
    posts: list[CanonPost]
    lore_text: str
    name_words: set[str]
    known_names: set[str]
    visual_references: list[Path]
    manifest: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def load(cls, root: Path) -> "CanonCorpus":
        root = Path(root).resolve()
        manifest_path = root / "manifest.json"
        lexicon_path = root / "name_words.yaml"
        if not manifest_path.exists():
            raise CorpusError(f"VIGVAMCEV manifest missing: {manifest_path}")
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise CorpusError(f"cannot read VIGVAMCEV manifest: {manifest_path}") from exc
        if not isinstance(manifest, dict):
            raise CorpusError("VIGVAMCEV manifest must be an object")

        try:
            lexicon = yaml.safe_load(lexicon_path.read_text(encoding="utf-8")) or {}
        except (OSError, yaml.YAMLError) as exc:
            raise CorpusError(f"cannot read VIGVAMCEV name lexicon: {lexicon_path}") from exc
        raw_words = lexicon.get("words", []) if isinstance(lexicon, dict) else []
        name_words = {_normalise_word(word) for word in raw_words if _normalise_word(word)}
        if not name_words:
            raise CorpusError("VIGVAMCEV name lexicon is empty")

        canon_dir = root / "canon"
        lore_path = canon_dir / "lore.txt"
        if not lore_path.exists():
            raise CorpusError(f"VIGVAMCEV lore missing: {lore_path}")
        lore_text = lore_path.read_text(encoding="utf-8")
        posts: list[CanonPost] = []
        known_names = set(_normalise_key(name) for name in _CLONE_NAME_RE.findall(lore_text))
        for raw_post in manifest.get("posts", []):
            if not isinstance(raw_post, dict):
                continue
            try:
                post_no = int(raw_post["post_no"])
                text_path = root / str(raw_post["text"])
            except (KeyError, TypeError, ValueError) as exc:
                raise CorpusError(f"invalid post entry: {raw_post!r}") from exc
            if not text_path.exists():
                raise CorpusError(f"VIGVAMCEV story missing: {text_path}")
            image_raw = raw_post.get("image")
            image_path = root / str(image_raw) if image_raw else None
            if image_path and not image_path.exists():
                image_path = None
            experiments: list[int] = []
            for value in raw_post.get("experiments", []):
                try:
                    experiments.append(int(value))
                except (TypeError, ValueError):
                    continue
            season = int(raw_post.get("season", 1) or 1)
            posts.append(CanonPost(post_no, tuple(experiments), text_path, image_path, season))
            post_text = text_path.read_text(encoding="utf-8")
            known_names.update(_normalise_key(name) for name in _CLONE_NAME_RE.findall(post_text))
        if not posts:
            raise CorpusError("VIGVAMCEV manifest contains no posts")
        posts.sort(key=lambda item: item.post_no)

        references: list[Path] = []
        for raw_path in manifest.get("visual_references", []):
            path = root / str(raw_path)
            if path.exists():
                references.append(path)
        return cls(root, posts, lore_text, name_words, known_names, references, manifest)

    @property
    def last_experiment(self) -> int:
        experiments = [experiment for post in self.posts for experiment in post.experiments]
        return max(experiments or [0])

    @property
    def last_post(self) -> int:
        return max(post.post_no for post in self.posts)

    def story_texts(self) -> list[str]:
        return [post.text_path.read_text(encoding="utf-8") for post in self.posts]

    def context(self, *, max_chars: int = 36000) -> str:
        pieces = ["[LORE.TXT]\n" + _compact_text(self.lore_text, 9000)]
        for post in self.posts[-8:]:
            text = post.text_path.read_text(encoding="utf-8")
            label = f"[ПОСТ {post.post_no}; ЭКСПЕРИМЕНТЫ {','.join(map(str, post.experiments)) or 'без номера'}]"
            pieces.append(label + "\n" + _compact_text(text, 3600))
        result = "\n\n".join(pieces)
        return _compact_text(result, max_chars)


@dataclass(frozen=True)
class VigvamcevSettings:
    enabled: bool
    timezone: str
    publish_time: str
    channel_id: int
    asset_dir: Path
    identity_reference: Path | None
    identity_layer: Path | None
    poster_font: Path | None
    poster_mode: str
    variation_pool: list[str]
    full_style_prompt: str
    image_model: str
    image_api_base_url: str
    image_aspect_ratio: str
    image_resolution: str
    image_poll_interval_seconds: float
    image_timeout_seconds: float
    caption_min_chars: int
    caption_max_chars: int
    max_stage_attempts: int
    retry_backoff_seconds: float
    loop_interval_seconds: float
    story_max_tokens: int
    review_max_tokens: int
    text_timeout_seconds: float
    text_retry_backoff_seconds: float
    story_hashtags: str
    style_prompt: str
    image_prompt_suffix: str
    story_prompt: str
    review_prompt: str

    @classmethod
    def from_mapping(
        cls,
        raw: Mapping[str, Any],
        *,
        root: Path,
        channel_id: int,
        asset_dir: Path,
    ) -> "VigvamcevSettings":
        def integer(key: str, default: int) -> int:
            try:
                return int(raw.get(key, default))
            except (TypeError, ValueError):
                return default

        def number(key: str, default: float) -> float:
            try:
                return float(raw.get(key, default))
            except (TypeError, ValueError):
                return default

        asset_path = Path(asset_dir if asset_dir.is_absolute() else root / asset_dir)
        identity_raw = str(raw.get("identity_reference", "") or "").strip()
        identity_path = Path(identity_raw) if identity_raw else None
        if identity_path is not None and not identity_path.is_absolute():
            identity_path = asset_path / identity_path
        layer_raw = str(raw.get("identity_layer", "") or "").strip()
        layer_path = Path(layer_raw) if layer_raw else None
        if layer_path is not None and not layer_path.is_absolute():
            layer_path = asset_path / layer_path
        poster_font_raw = str(raw.get("poster_font", "") or "").strip()
        poster_font_path = Path(poster_font_raw) if poster_font_raw else None
        if poster_font_path is not None and not poster_font_path.is_absolute():
            poster_font_path = asset_path / poster_font_path
        poster_mode = str(raw.get("poster_mode", "full")).strip().lower()
        if poster_mode not in {"full", "compose"}:
            poster_mode = "full"
        raw_variations = raw.get("variation_pool", [])
        if not isinstance(raw_variations, list):
            raw_variations = []
        variation_pool = [str(item).strip() for item in raw_variations if str(item).strip()]
        full_style = str(raw.get("full_style_prompt", "") or "").strip()
        if not full_style:
            full_style = re.sub(
                r",?\s*no readable text,?\s*no logos\.?",
                "",
                str(raw.get("style_prompt", "") or ""),
            ).strip()
        return cls(
            enabled=bool(raw.get("enabled", True)),
            timezone=str(raw.get("timezone", "Europe/Moscow")),
            publish_time=str(raw.get("publish_time", "13:00")),
            channel_id=int(channel_id),
            asset_dir=asset_path,
            identity_reference=identity_path,
            identity_layer=layer_path,
            poster_font=poster_font_path,
            poster_mode=poster_mode,
            variation_pool=list(variation_pool),
            full_style_prompt=full_style,
            image_model=str(raw.get("image_model", "openai/gpt-5.4-image-2")),
            image_api_base_url=str(raw.get("image_api_base_url", "https://polza.ai/api/v1")).rstrip("/"),
            image_aspect_ratio=str(raw.get("image_aspect_ratio", "4:3")),
            image_resolution=str(raw.get("image_resolution", "2K")),
            image_poll_interval_seconds=number("image_poll_interval_seconds", 5.0),
            image_timeout_seconds=number("image_timeout_seconds", 300.0),
            caption_min_chars=integer("caption_min_chars", 600),
            caption_max_chars=integer("caption_max_chars", 900),
            max_stage_attempts=max(1, integer("max_stage_attempts", 3)),
            retry_backoff_seconds=max(0.0, number("retry_backoff_seconds", 30.0)),
            loop_interval_seconds=max(5.0, number("loop_interval_seconds", 30.0)),
            story_max_tokens=max(300, integer("story_max_tokens", 1400)),
            review_max_tokens=max(80, integer("review_max_tokens", 220)),
            text_timeout_seconds=max(10.0, number("text_timeout_seconds", 60.0)),
            text_retry_backoff_seconds=max(0.0, number("text_retry_backoff_seconds", 5.0)),
            story_hashtags=str(raw.get("story_hashtags", _DEFAULT_HASHTAGS)).strip(),
            style_prompt=str(raw.get("style_prompt", "")).strip(),
            image_prompt_suffix=str(raw.get("image_prompt_suffix", "")).strip(),
            story_prompt=str(raw.get("story_prompt", "")).strip(),
            review_prompt=str(raw.get("review_prompt", "")).strip(),
        )


@dataclass
class VigvamcevCandidate:
    post_no: int
    experiment_no: int
    clone_name: str
    source_word: str
    ability: str
    canon_anchors: list[str]
    conflict: str
    twist: str
    consequence: str
    next_hook: str
    visual_brief: str
    novelty_tags: list[str]
    story: str

    @classmethod
    def from_payload(cls, payload: Mapping[str, Any]) -> "VigvamcevCandidate":
        story = _normalise_story(payload.get("story") or payload.get("caption") or payload.get("public_story"))
        return cls(
            post_no=int(payload.get("post_no", 0) or 0),
            experiment_no=int(payload.get("experiment_no", 0) or 0),
            clone_name=_as_text(payload.get("clone_name")),
            source_word=_as_text(payload.get("source_word")),
            ability=_as_text(payload.get("ability")),
            canon_anchors=_coerce_string_list(payload.get("canon_anchors")),
            conflict=_as_text(payload.get("conflict")),
            twist=_as_text(payload.get("twist")),
            consequence=_as_text(payload.get("consequence")),
            next_hook=_as_text(payload.get("next_hook")),
            visual_brief=_as_text(payload.get("visual_brief")),
            novelty_tags=_coerce_string_list(payload.get("novelty_tags")),
            story=story,
        )

    def to_dict(self, *, hashtags: str = _DEFAULT_HASHTAGS, canon_status: str = "") -> dict[str, Any]:
        payload = {
            "post_no": self.post_no,
            "experiment_no": self.experiment_no,
            "clone_name": self.clone_name,
            "source_word": self.source_word,
            "ability": self.ability,
            "canon_anchors": list(self.canon_anchors),
            "conflict": self.conflict,
            "twist": self.twist,
            "consequence": self.consequence,
            "next_hook": self.next_hook,
            "visual_brief": self.visual_brief,
            "novelty_tags": list(self.novelty_tags),
            "story": self.story,
            "caption": format_caption(self, hashtags=hashtags),
        }
        if canon_status:
            payload["canon_status"] = canon_status
        return payload


@dataclass(frozen=True)
class PreparedDraft:
    candidate: VigvamcevCandidate
    image_path: Path
    image_sha256: str
    generation_id: str = ""


def format_caption(candidate: VigvamcevCandidate, *, hashtags: str = _DEFAULT_HASHTAGS) -> str:
    header = f"№ {candidate.post_no} · Эксперимент {candidate.experiment_no} · {candidate.clone_name}"
    body = candidate.story.strip()
    suffix = hashtags.strip()
    parts = [header, body]
    if suffix:
        parts.append(suffix)
    return "\n\n".join(part for part in parts if part)


def default_vigvamcev_state(corpus: CanonCorpus, settings: VigvamcevSettings) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "enabled": settings.enabled,
        "timezone": settings.timezone,
        "publish_time": settings.publish_time,
        "post_no": corpus.last_post if corpus.last_post >= 22 else 22,
        "experiment_no": corpus.last_experiment if corpus.last_experiment >= 44 else 44,
        "season": 2,
        "last_published": {},
        "draft": {},
        "used_names": sorted(corpus.known_names),
        "used_abilities": [],
        "used_motifs": [],
        "retry_state": {},
        "publish_status": "idle",
        "last_error": "",
        "history": [],
    }


def ensure_vigvamcev_state(config: dict[str, Any], corpus: CanonCorpus, settings: VigvamcevSettings) -> dict[str, Any]:
    state = config.setdefault("vigvamcev", {})
    if not isinstance(state, dict):
        state = {}
        config["vigvamcev"] = state
    defaults = default_vigvamcev_state(corpus, settings)
    for key, value in defaults.items():
        if key in {"used_names", "used_abilities", "used_motifs", "history"}:
            if not isinstance(state.get(key), list):
                state[key] = list(value)
        elif key == "draft" or key == "last_published" or key == "retry_state":
            if not isinstance(state.get(key), dict):
                state[key] = dict(value)
        else:
            state.setdefault(key, value)
    if not state.get("used_names"):
        state["used_names"] = sorted(corpus.known_names)
    return state


def _parse_time(value: str) -> tuple[int, int]:
    match = re.fullmatch(r"\s*(\d{1,2}):(\d{2})\s*", str(value or ""))
    if not match:
        return 13, 0
    hour, minute = int(match.group(1)), int(match.group(2))
    if hour > 23 or minute > 59:
        return 13, 0
    return hour, minute


def _safe_recent_history(state: Mapping[str, Any], limit: int = 8) -> list[dict[str, Any]]:
    history = state.get("history", [])
    if not isinstance(history, list):
        return []
    return [item for item in history[-limit:] if isinstance(item, dict)]


def _story_block_errors(story: str) -> list[str]:
    errors: list[str] = []
    clone_match = _CLONE_BLOCK_RE.search(story)
    sic_match = _SIC_BLOCK_RE.search(story)
    if not clone_match:
        errors.append("в истории нет блока «Клон: …»")
    elif len(clone_match.group(1).strip()) < 80:
        errors.append("блок «Клон: …» слишком короткий")
    if not sic_match:
        errors.append("в истории нет блока «SIC: …»")
    elif len(sic_match.group(1).strip()) < 80:
        errors.append("блок «SIC: …» слишком короткий")
    return errors


def build_story_prompt(
    corpus: CanonCorpus,
    state: Mapping[str, Any],
    settings: VigvamcevSettings,
    *,
    post_no: int,
    experiment_no: int,
) -> str:
    used_names = ", ".join(str(item) for item in state.get("used_names", [])[-80:]) or "нет данных"
    used_abilities = ", ".join(str(item) for item in state.get("used_abilities", [])[-60:]) or "нет данных"
    recent = []
    for item in _safe_recent_history(state):
        recent.append(
            f"- пост {item.get('post_no')}, эксперимент {item.get('experiment_no')}, "
            f"имя {item.get('clone_name')}: {_compact_text(str(item.get('story', item.get('caption', ''))), 650)}"
        )
    recent_text = "\n".join(recent) or "- это первый новый выпуск после импортированного канона"
    words = ", ".join(sorted(corpus.name_words))
    return f"""{settings.story_prompt}

Текущая задача: пост №{post_no}, эксперимент {experiment_no}, сезон 2.
Разрешённые исходные слова для имени: {words}
Уже использованные имена: {used_names}
Уже использованные способности: {used_abilities}

Последние опубликованные новые выпуски:
{recent_text}

Сжатый корпус канона:
{corpus.context()}

История обязана состоять ровно из двух содержательных блоков: строка «Клон: …» — про нового клона, строка «SIC: …» — про продолжение общего сюжета Фонда SIC.

Верни только JSON следующей формы:
{{
  "post_no": {post_no},
  "experiment_no": {experiment_no},
  "clone_name": "Имя на -цев",
  "source_word": "одно слово из разрешённого списка",
  "ability": "новая способность или дефект",
  "canon_anchors": ["конкретный подтверждённый факт"],
  "conflict": "причина конфликта",
  "twist": "поворот",
  "consequence": "последствие",
  "next_hook": "крючок следующего выпуска",
  "visual_brief": "описание сцены без текста на изображении",
  "novelty_tags": ["уникальный мотив", "уникальный визуальный приём"],
  "story": "два блока «Клон: …» и «SIC: …», суммарно 560–800 знаков"
}}
""".strip()


def build_review_prompt(candidate: VigvamcevCandidate, state: Mapping[str, Any], settings: VigvamcevSettings) -> str:
    recent = _safe_recent_history(state, limit=5)
    recent_text = "\n".join(
        f"- {item.get('clone_name')}: {_compact_text(str(item.get('story', item.get('caption', ''))), 500)}"
        for item in recent
    ) or "нет предыдущих новых выпусков"
    return f"""{settings.review_prompt}

Проверяемый кандидат:
{json.dumps(candidate.to_dict(hashtags=settings.story_hashtags), ensure_ascii=False, indent=2)}

Последние выпуски:
{recent_text}

Диапазон полной подписи: {settings.caption_min_chars}–{settings.caption_max_chars} знаков.
Ответь строго так: {{"ok": true, "reason": "краткая причина"}} или {{"ok": false, "reason": "что исправить"}}.
""".strip()


async def _wait_text_retry(settings: VigvamcevSettings, attempt: int) -> None:
    if attempt + 1 < settings.max_stage_attempts and settings.text_retry_backoff_seconds:
        await asyncio.sleep(settings.text_retry_backoff_seconds * (2**attempt))


def validate_candidate(
    candidate: VigvamcevCandidate,
    *,
    state: Mapping[str, Any],
    corpus: CanonCorpus,
    settings: VigvamcevSettings,
) -> list[str]:
    errors: list[str] = []
    expected_post = int(state.get("post_no", 22) or 22) + 1
    expected_experiment = int(state.get("experiment_no", 44) or 44) + 1
    if candidate.post_no != expected_post:
        errors.append(f"post_no должен быть {expected_post}")
    if candidate.experiment_no != expected_experiment:
        errors.append(f"experiment_no должен быть {expected_experiment}")
    source_word = _normalise_word(candidate.source_word)
    if source_word not in corpus.name_words:
        errors.append("source_word отсутствует в разрешённом словаре")
    expected_name = clone_name_from_word(source_word)
    if _normalise_key(candidate.clone_name) != _normalise_key(expected_name):
        errors.append("clone_name не соответствует source_word + 'цев'")
    used_names = {_normalise_key(value) for value in state.get("used_names", []) if _as_text(value)}
    if _normalise_key(candidate.clone_name) in used_names:
        errors.append("имя клона уже использовалось")
    if not candidate.ability:
        errors.append("не указана способность или дефект")
    ability_key = _normalise_key(candidate.ability)
    used_abilities = {_normalise_key(value) for value in state.get("used_abilities", []) if _as_text(value)}
    if ability_key and ability_key in used_abilities:
        errors.append("способность уже использовалась")
    if not candidate.canon_anchors:
        errors.append("нет подтверждённой опоры на канон")
    else:
        canonical_tokens = _token_set(
            corpus.lore_text + "\n" + "\n".join(corpus.story_texts())
        )
        anchor_tokens = _token_set(" ".join(candidate.canon_anchors))
        if not anchor_tokens or not (anchor_tokens & canonical_tokens):
            errors.append("опора canon_anchors не подтверждается локальным корпусом")
    if not candidate.conflict or not candidate.twist or not candidate.consequence or not candidate.next_hook:
        errors.append("не заполнены причинный конфликт, поворот, последствие или крючок")
    if not candidate.visual_brief:
        errors.append("нет visual_brief")
    if not candidate.novelty_tags:
        errors.append("нет novelty_tags")
    caption = format_caption(candidate, hashtags=settings.story_hashtags)
    if len(caption) < settings.caption_min_chars or len(caption) > settings.caption_max_chars:
        errors.append(f"caption имеет длину {len(caption)}, ожидалось {settings.caption_min_chars}–{settings.caption_max_chars}")
    if not any(marker in candidate.story.casefold() for marker in _CAUSAL_MARKERS):
        errors.append("в тексте не найден причинный связующий маркер")
    errors.extend(_story_block_errors(candidate.story))
    if "```" in candidate.story or "не могу" in candidate.story.casefold():
        errors.append("текст содержит служебный или отказной ответ модели")
    motif_keys = {_normalise_key(value) for value in state.get("used_motifs", []) if _as_text(value)}
    if motif_keys & {_normalise_key(value) for value in candidate.novelty_tags}:
        errors.append("основной novelty-мотив уже использовался")
    candidate_tokens = _token_set(candidate.story)
    for item in _safe_recent_history(state):
        previous_tokens = _token_set(str(item.get("story", item.get("caption", ""))))
        if _jaccard(candidate_tokens, previous_tokens) >= 0.74:
            errors.append("история слишком похожа на недавний выпуск")
            break
    return errors


async def generate_candidate(
    corpus: CanonCorpus,
    state: Mapping[str, Any],
    settings: VigvamcevSettings,
    *,
    text_request: TextRequest,
    reviewer: TextRequest | None,
    post_no: int,
    experiment_no: int,
) -> VigvamcevCandidate:
    prompt = build_story_prompt(corpus, state, settings, post_no=post_no, experiment_no=experiment_no)
    failures: list[str] = []
    for attempt in range(settings.max_stage_attempts):
        try:
            raw = await text_request(prompt, settings.story_max_tokens)
        except TransientTextRequestError as exc:
            failures.append(f"попытка {attempt + 1}: текстовый API временно недоступен: {exc}")
            await _wait_text_retry(settings, attempt)
            continue
        payload = _extract_json_object(raw)
        if not payload:
            failures.append(f"попытка {attempt + 1}: модель не вернула JSON")
            continue
        try:
            candidate = VigvamcevCandidate.from_payload(payload)
        except (TypeError, ValueError) as exc:
            failures.append(f"попытка {attempt + 1}: некорректные поля JSON: {exc}")
            continue
        errors = validate_candidate(candidate, state=state, corpus=corpus, settings=settings)
        if errors:
            failures.append(f"попытка {attempt + 1}: " + "; ".join(errors[:4]))
            continue
        if reviewer is not None:
            try:
                review_raw = await reviewer(build_review_prompt(candidate, state, settings), settings.review_max_tokens)
            except TransientTextRequestError as exc:
                failures.append(f"попытка {attempt + 1}: reviewer временно недоступен: {exc}")
                await _wait_text_retry(settings, attempt)
                continue
            review = _extract_json_object(review_raw)
            if not review or review.get("ok") is not True:
                failures.append(f"попытка {attempt + 1}: reviewer отклонил: {_as_text(review.get('reason')) or 'некорректный ответ'}")
                continue
        return candidate
    raise CandidateGenerationError("; ".join(failures[-3:]) or "не удалось создать канонический кандидат")


def build_visual_prompt(candidate: VigvamcevCandidate, settings: VigvamcevSettings) -> str:
    """Legacy scene prompt without variation; kept for compatibility."""
    return build_scene_prompt(candidate, settings, variation="")


def _pick_variation(settings: VigvamcevSettings, post_no: int) -> str:
    pool = list(settings.variation_pool)
    if not pool:
        return ""
    return pool[post_no % len(pool)]


def _variation_line(variation: str) -> str:
    if not variation:
        return ""
    return (
        "Composition variety for this post: "
        + variation
        + "\nKeep the series style, but make the layout clearly different from the previous posts."
    )


def build_scene_prompt(
    candidate: VigvamcevCandidate,
    settings: VigvamcevSettings,
    variation: str = "",
) -> str:
    variation_text = _variation_line(variation)
    return f"""{settings.style_prompt}

Сюжетная сцена для клона {candidate.clone_name} (эксперимент {candidate.experiment_no}):
{candidate.visual_brief}

{variation_text}

Способность: {candidate.ability}
Конфликт: {candidate.conflict}
Поворот: {candidate.twist}

Сохрани узнаваемое лицо по переданному референсу, но сделай новую абсурдную
фотошопную композицию с вырезанными фигурами, нелепыми объектами и ощущением
ручной склейки. Это фон/сцена для локального постера, а не готовая обложка.
{settings.image_prompt_suffix}""".strip()


def build_full_poster_prompt(
    candidate: VigvamcevCandidate,
    settings: VigvamcevSettings,
    variation: str = "",
) -> str:
    variation_text = _variation_line(variation)
    name = str(candidate.clone_name).upper()
    number_line = f"№{candidate.post_no} · ЭКСПЕРИМЕНТ {candidate.experiment_no}"
    return f"""{settings.full_style_prompt}

Create the FINAL cover poster of the series, not just a scene. 4:3 poster with:
- Large bold white header centered near the top: #ВИГВАМЦЕВ: ИСТОРИИ2
- A big diagonal black plate in the lower-right corner with the clone name in large white letters: {name}
- On the same black plate, a smaller single line: {number_line}

{variation_text}

Scene for the clone {candidate.clone_name} (experiment {candidate.experiment_no}):
{candidate.visual_brief}

Ability: {candidate.ability}
Conflict: {candidate.conflict}
Twist: {candidate.twist}

Keep the clone's face recognizable from the reference. Cutout collage with rough
edges, absurd proportions, yellow-orange background and black dots.

CRITICAL: reproduce every Cyrillic letter of the three texts exactly as written above.
Do not add any other readable text, letters, logos, or watermarks.""".strip()


def _decode_image_or_raise(content: bytes) -> None:
    import io

    from PIL import Image

    try:
        with Image.open(io.BytesIO(content)) as image:
            image.load()
    except Exception as exc:  # pragma: no cover - Pillow error text varies
        raise DraftError("модель вернула повреждённое изображение") from exc


def _image_sha256(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


class VigvamcevService:
    """Coordinates the VIGVAMCEV domain without owning Timur's personal lore."""

    def __init__(
        self,
        *,
        settings: VigvamcevSettings,
        corpus: CanonCorpus,
        image_client: Any,
        text_request: TextRequest,
        reviewer: TextRequest | None,
        load_memory: Callable[[], dict[str, Any]],
        save_memory: Callable[[dict[str, Any]], bool],
        memory_path: Path,
        owner_ids: Iterable[int] = (),
        logger: logging.Logger | None = None,
        compose: Callable[..., bytes] = compose_poster,
    ) -> None:
        self.settings = settings
        self.corpus = corpus
        self.image_client = image_client
        self.text_request = text_request
        self.reviewer = reviewer
        self.load_memory = load_memory
        self.save_memory = save_memory
        self.memory_path = Path(memory_path)
        self.owner_ids = [int(value) for value in owner_ids]
        self.logger = logger or LOGGER
        self.compose = compose
        self._lock = asyncio.Lock()
        self._task: asyncio.Task | None = None
        self._notified_retry_dates: set[str] = set()

    def _state(self, memory: dict[str, Any]) -> dict[str, Any]:
        return ensure_vigvamcev_state(memory.setdefault("config", {}), self.corpus, self.settings)

    def _save(self, memory: dict[str, Any]) -> None:
        if not self.save_memory(memory):
            raise DraftError("не удалось сохранить состояние VIGVAMCEV")

    def _artifact_dir(self) -> Path:
        directory = self.memory_path.parent / "vigvamcev_artifacts"
        directory.mkdir(parents=True, exist_ok=True)
        return directory

    def _reference_paths(self) -> list[Path]:
        references = list(self.corpus.visual_references)
        identity = self.settings.identity_reference
        if identity and identity.exists() and identity not in references:
            references.insert(0, identity)
        return references

    def _candidate_from_state(self, state: Mapping[str, Any]) -> VigvamcevCandidate | None:
        draft = state.get("draft", {})
        if not isinstance(draft, dict) or not isinstance(draft.get("candidate"), dict):
            return None
        try:
            return VigvamcevCandidate.from_payload(draft["candidate"])
        except (TypeError, ValueError):
            return None

    def _prepared_from_state(self, state: Mapping[str, Any]) -> PreparedDraft | None:
        draft = state.get("draft", {})
        if not isinstance(draft, dict) or draft.get("status") != "ready":
            return None
        candidate = self._candidate_from_state(state)
        image_raw = str(draft.get("image_path", "") or "")
        if not candidate or not image_raw:
            return None
        image_path = Path(image_raw)
        if not image_path.exists():
            return None
        return PreparedDraft(
            candidate=candidate,
            image_path=image_path,
            image_sha256=str(draft.get("image_sha256", "") or ""),
            generation_id=str(draft.get("generation_id", "") or ""),
        )

    def _prepared_errors(self, prepared: PreparedDraft, state: Mapping[str, Any]) -> list[str]:
        errors = validate_candidate(
            prepared.candidate,
            state=state,
            corpus=self.corpus,
            settings=self.settings,
        )
        if not prepared.image_path.is_file():
            errors.append("файл постера отсутствует")
        if prepared.image_sha256:
            try:
                actual_hash = _image_sha256(prepared.image_path.read_bytes())
            except OSError:
                actual_hash = ""
            if actual_hash != prepared.image_sha256:
                errors.append("хеш постера не совпадает с draft")
        return errors

    async def _candidate(self, memory: dict[str, Any], state: dict[str, Any], *, force_new: bool) -> VigvamcevCandidate:
        if not force_new:
            current = self._candidate_from_state(state)
            if current:
                return current
        post_no = int(state.get("post_no", 22) or 22) + 1
        experiment_no = int(state.get("experiment_no", 44) or 44) + 1
        candidate = await generate_candidate(
            self.corpus,
            state,
            self.settings,
            text_request=self.text_request,
            reviewer=self.reviewer,
            post_no=post_no,
            experiment_no=experiment_no,
        )
        state["draft"] = {
            "status": "text_ready",
            "candidate": candidate.to_dict(hashtags=self.settings.story_hashtags),
            "canon_status": "draft",
            "created_at": datetime.utcnow().isoformat(),
        }
        state["last_error"] = ""
        self._save(memory)
        return candidate

    async def _prepare_locked(self, *, force_new: bool = False) -> PreparedDraft:
        memory = self.load_memory()
        state = self._state(memory)
        if not force_new:
            prepared = self._prepared_from_state(state)
            if prepared:
                errors = self._prepared_errors(prepared, state)
                if errors:
                    state.setdefault("draft", {})["status"] = "failed"
                    state["last_error"] = "; ".join(errors[:4])
                    self._save(memory)
                    raise DraftError("draft не прошёл повторную проверку: " + "; ".join(errors[:4]))
                return prepared
        candidate = await self._candidate(memory, state, force_new=force_new)
        if not getattr(self.image_client, "configured", True):
            raise DraftError("Polza image API не настроен: задайте POLZA_AI_API_KEY")
        variation = _pick_variation(self.settings, candidate.post_no)
        if self.settings.poster_mode == "full":
            try:
                return await self._prepare_full_poster(candidate, state, memory, variation=variation)
            except Exception as full_exc:
                self.logger.warning(
                    "полная генерация постера не удалась (%s); переключаюсь на локальную сборку",
                    full_exc,
                )
                memory = self.load_memory()
                state = self._state(memory)
                state["last_error"] = "full_poster_fallback: " + str(full_exc)[:300]
                draft = state.setdefault("draft", {})
                draft["status"] = "image_retrying"
                draft["poster_mode"] = "composed"
                draft["error"] = str(full_exc)[:500]
                self._save(memory)
        return await self._prepare_compose_poster(candidate, state, memory, variation=variation)

    async def _prepare_full_poster(
        self,
        candidate: VigvamcevCandidate,
        state: dict[str, Any],
        memory: dict[str, Any],
        *,
        variation: str,
    ) -> PreparedDraft:
        failures: list[str] = []
        for attempt in range(self.settings.max_stage_attempts):
            try:
                generated = await self.image_client.generate_scene(
                    prompt=build_full_poster_prompt(candidate, self.settings, variation),
                    reference_paths=self._reference_paths(),
                )
                poster = generated.content
                _decode_image_or_raise(poster)
                artifact = self._artifact_dir() / f"post-{candidate.post_no:04d}-experiment-{candidate.experiment_no:04d}-full.png"
                artifact.write_bytes(poster)
                draft = state.setdefault("draft", {})
                draft.update(
                    {
                        "status": "ready",
                        "candidate": candidate.to_dict(hashtags=self.settings.story_hashtags),
                        "image_path": str(artifact),
                        "image_sha256": _image_sha256(poster),
                        "generation_id": str(getattr(generated, "generation_id", "") or ""),
                        "poster_mode": "full",
                        "updated_at": datetime.utcnow().isoformat(),
                    }
                )
                state["last_error"] = ""
                self._save(memory)
                return PreparedDraft(candidate, artifact, _image_sha256(poster), str(getattr(generated, "generation_id", "") or ""))
            except Exception as exc:
                failures.append(str(exc))
                state["last_error"] = ("full_poster: " + str(exc))[:500]
                draft = state.setdefault("draft", {})
                draft["status"] = "image_retrying" if attempt + 1 < self.settings.max_stage_attempts else "failed"
                draft["error"] = str(exc)[:500]
                draft["poster_mode"] = "full"
                self._save(memory)
                if attempt + 1 < self.settings.max_stage_attempts and self.settings.retry_backoff_seconds:
                    await asyncio.sleep(self.settings.retry_backoff_seconds * (attempt + 1))
        raise DraftError("полная генерация постера не удалась: " + "; ".join(failures[-3:]))

    async def _prepare_compose_poster(
        self,
        candidate: VigvamcevCandidate,
        state: dict[str, Any],
        memory: dict[str, Any],
        *,
        variation: str,
    ) -> PreparedDraft:
        failures: list[str] = []
        for attempt in range(self.settings.max_stage_attempts):
            try:
                generated = await self.image_client.generate_scene(
                    prompt=build_scene_prompt(candidate, self.settings, variation),
                    reference_paths=self._reference_paths(),
                )
                identity_layer_bytes = None
                if self.settings.identity_layer and self.settings.identity_layer.is_file():
                    identity_layer_bytes = self.settings.identity_layer.read_bytes()
                compose_kwargs = {}
                if self.settings.poster_font and self.settings.poster_font.is_file():
                    compose_kwargs["configured_font"] = str(self.settings.poster_font)
                if identity_layer_bytes:
                    compose_kwargs["identity_layer_bytes"] = identity_layer_bytes
                poster = self.compose(
                    generated.content,
                    candidate.clone_name,
                    post_no=candidate.post_no,
                    experiment_no=candidate.experiment_no,
                    **compose_kwargs,
                )
                if not poster:
                    raise DraftError("композитор вернул пустой постер")
                artifact = self._artifact_dir() / f"post-{candidate.post_no:04d}-experiment-{candidate.experiment_no:04d}-composed.png"
                artifact.write_bytes(poster)
                draft = state.setdefault("draft", {})
                draft.update(
                    {
                        "status": "ready",
                        "candidate": candidate.to_dict(hashtags=self.settings.story_hashtags),
                        "image_path": str(artifact),
                        "image_sha256": _image_sha256(poster),
                        "generation_id": str(getattr(generated, "generation_id", "") or ""),
                        "poster_mode": "composed",
                        "updated_at": datetime.utcnow().isoformat(),
                    }
                )
                state["last_error"] = ""
                self._save(memory)
                return PreparedDraft(candidate, artifact, _image_sha256(poster), str(getattr(generated, "generation_id", "") or ""))
            except Exception as exc:
                failures.append(str(exc))
                state["last_error"] = str(exc)[:500]
                draft = state.setdefault("draft", {})
                draft["status"] = "image_retrying" if attempt + 1 < self.settings.max_stage_attempts else "failed"
                draft["error"] = str(exc)[:500]
                self._save(memory)
                if attempt + 1 < self.settings.max_stage_attempts and self.settings.retry_backoff_seconds:
                    await asyncio.sleep(self.settings.retry_backoff_seconds * (attempt + 1))
        raise DraftError("локальная сборка постера не удалась: " + "; ".join(failures[-3:]))

    async def prepare_draft(self, *, force_new: bool = False) -> PreparedDraft:
        async with self._lock:
            return await self._prepare_locked(force_new=force_new)

    def _now_local(self, now: datetime | None = None) -> datetime:
        if now is not None:
            return now
        try:
            from zoneinfo import ZoneInfo

            return datetime.now(ZoneInfo(self.settings.timezone))
        except Exception:
            return datetime.now().astimezone()

    def is_due(self, now: datetime | None = None, state: Mapping[str, Any] | None = None) -> bool:
        current = self._now_local(now)
        hour, minute = _parse_time(self.settings.publish_time)
        if (current.hour, current.minute) < (hour, minute):
            return False
        state = state or {}
        last = state.get("last_published", {})
        if isinstance(last, dict) and str(last.get("published_date", "")) == current.date().isoformat():
            return False
        return True

    def _retry_allowed(self, state: dict[str, Any], current: datetime) -> bool:
        retry = state.setdefault("retry_state", {})
        if not isinstance(retry, dict):
            state["retry_state"] = retry = {}
        date_key = current.date().isoformat()
        if retry.get("date") != date_key:
            retry.clear()
            retry["date"] = date_key
            retry["attempts"] = 0
        next_retry_raw = str(retry.get("next_retry_at", "") or "")
        if next_retry_raw:
            try:
                if datetime.fromisoformat(next_retry_raw) > current:
                    return False
            except ValueError:
                pass
        attempts = int(retry.get("attempts", 0) or 0)
        if attempts >= self.settings.max_stage_attempts:
            return False
        retry["attempts"] = attempts + 1
        retry["next_retry_at"] = (current + timedelta(seconds=self.settings.retry_backoff_seconds * (attempts + 1))).isoformat()
        return True

    async def _notify_owner(self, application: Any, text: str) -> None:
        if not self.owner_ids or not application or not getattr(application, "bot", None):
            return
        for owner_id in self.owner_ids[:1]:
            try:
                await application.bot.send_message(chat_id=owner_id, text=text[:3500])
            except Exception:
                self.logger.exception("не удалось уведомить владельца о VIGVAMCEV")

    async def publish(self, application: Any, *, force: bool = False, now: datetime | None = None) -> PreparedDraft | None:
        current = self._now_local(now)
        memory = self.load_memory()
        state = self._state(memory)
        if not force and not self.is_due(current, state):
            return None
        if state.get("publish_status") in {"publishing", "publish_unknown"}:
            raise DraftError(f"публикация заблокирована состоянием {state.get('publish_status')}")
        if not self.settings.channel_id:
            raise DraftError("VIGVAMCEV_CHANNEL_ID не настроен")
        prepared = await self.prepare_draft()
        memory = self.load_memory()
        state = self._state(memory)
        errors = self._prepared_errors(prepared, state)
        if errors:
            state.setdefault("draft", {})["status"] = "failed"
            state["last_error"] = "; ".join(errors[:4])
            self._save(memory)
            raise DraftError("draft не прошёл проверку перед публикацией: " + "; ".join(errors[:4]))
        state["publish_status"] = "publishing"
        state["last_error"] = ""
        state.setdefault("draft", {})["status"] = "publishing"
        state["retry_state"] = {}
        self._save(memory)
        try:
            image_bytes = prepared.image_path.read_bytes()
            sent = await application.bot.send_photo(
                chat_id=self.settings.channel_id,
                photo=InputFile(image_bytes, filename=prepared.image_path.name),
                caption=format_caption(prepared.candidate, hashtags=self.settings.story_hashtags),
            )
        except Exception as exc:
            memory = self.load_memory()
            state = self._state(memory)
            unknown = isinstance(exc, (TimedOut, NetworkError, TimeoutError))
            state["last_error"] = str(exc)[:500]
            state["publish_status"] = "publish_unknown" if unknown else "idle"
            state.setdefault("draft", {})["status"] = "publish_unknown" if unknown else "ready"
            self._save(memory)
            if unknown:
                await self._notify_owner(application, "VIGVAMЦЕВ: исход публикации неизвестен, повтор заблокирован во избежание дубля.")
            raise

        memory = self.load_memory()
        state = self._state(memory)
        sent_message_id = getattr(sent, "message_id", None)
        published_date = current.date().isoformat()
        candidate = prepared.candidate
        state["post_no"] = candidate.post_no
        state["experiment_no"] = candidate.experiment_no
        state["publish_status"] = "idle"
        state["last_error"] = ""
        state["last_published"] = {
            "post_no": candidate.post_no,
            "experiment_no": candidate.experiment_no,
            "clone_name": candidate.clone_name,
            "message_id": sent_message_id,
            "published_date": published_date,
            "published_at": datetime.utcnow().isoformat(),
            "image_path": str(prepared.image_path),
            "image_sha256": prepared.image_sha256,
        }
        state["used_names"] = sorted(set(state.get("used_names", [])) | {candidate.clone_name})
        state["used_abilities"] = list(state.get("used_abilities", [])) + [candidate.ability]
        state["used_motifs"] = list(state.get("used_motifs", [])) + candidate.novelty_tags
        history = state.setdefault("history", [])
        history.append(
            candidate.to_dict(
                hashtags=self.settings.story_hashtags,
                canon_status="generated_canon",
            )
        )
        if len(history) > 100:
            del history[:-100]
        state["draft"] = {}
        self._save(memory)
        self.logger.info(
            "VIGVAMCEV опубликован: post=%s experiment=%s chat_id=%s message_id=%s",
            candidate.post_no,
            candidate.experiment_no,
            self.settings.channel_id,
            sent_message_id,
        )
        return prepared

    def status_text(self) -> str:
        memory = self.load_memory()
        state = self._state(memory)
        draft = state.get("draft", {}) if isinstance(state.get("draft"), dict) else {}
        last = state.get("last_published", {}) if isinstance(state.get("last_published"), dict) else {}
        next_post = int(state.get("post_no", 22) or 22) + 1
        next_experiment = int(state.get("experiment_no", 44) or 44) + 1
        return (
            "VIGVAMЦЕВ\n"
            f"включен: {bool(state.get('enabled', self.settings.enabled))}\n"
            f"канал: {self.settings.channel_id or 'не настроен'}\n"
            f"расписание: {self.settings.publish_time} {self.settings.timezone}\n"
            f"следующий: пост №{next_post}, эксперимент {next_experiment}\n"
            f"draft: {draft.get('status', 'нет')}\n"
            f"последний: {last.get('clone_name', 'нет')} ({last.get('published_date', '—')})\n"
            f"ошибка: {state.get('last_error') or 'нет'}"
        )

    async def handle_owner_command(self, update: Any, context: Any) -> None:
        message = getattr(update, "effective_message", None) or getattr(update, "message", None)
        if not message:
            return
        chat = getattr(message, "chat", None)
        if chat is not None and getattr(chat, "type", "") != "private":
            await message.reply_text("эти команды работают только в личке")
            return
        raw = str(getattr(message, "text", "") or "").split()
        action = raw[1].casefold() if len(raw) > 1 else "status"
        application = getattr(context, "application", None)
        try:
            if action == "status":
                await message.reply_text(self.status_text())
            elif action == "preview":
                prepared = await self.prepare_draft()
                await message.reply_photo(
                    photo=InputFile(prepared.image_path.read_bytes(), filename=prepared.image_path.name),
                    caption=format_caption(prepared.candidate, hashtags=self.settings.story_hashtags),
                )
            elif action == "retry":
                prepared = await self.prepare_draft()
                await message.reply_text(f"draft готов: пост №{prepared.candidate.post_no}, эксперимент {prepared.candidate.experiment_no}")
            elif action == "publish":
                if application is None:
                    raise DraftError("нет Telegram application")
                prepared = await self.publish(application, force=True)
                await message.reply_text(
                    "опубликовано" if prepared else "публикация не выполнена",
                )
            else:
                await message.reply_text("используй: /vigvamcev status|preview|retry|publish")
        except Exception as exc:
            self.logger.exception("VIGVAMCEV command failed: %s", action)
            await message.reply_text(f"VIGVAMЦЕВ: ошибка: {str(exc)[:700]}")

    async def maybe_publish(self, application: Any, *, now: datetime | None = None) -> None:
        current = self._now_local(now)
        memory = self.load_memory()
        state = self._state(memory)
        if not bool(state.get("enabled", self.settings.enabled)) or not self.settings.enabled:
            return
        if not self.settings.channel_id or not getattr(self.image_client, "configured", True):
            return
        if not self.is_due(current, state):
            return
        if state.get("publish_status") in {"publishing", "publish_unknown"}:
            return
        if not self._retry_allowed(state, current):
            date_key = current.date().isoformat()
            if date_key not in self._notified_retry_dates:
                self._notified_retry_dates.add(date_key)
                await self._notify_owner(application, "VIGVAMЦЕВ: дневной лимит автоматических попыток исчерпан; используй /vigvamcev retry или /vigvamcev publish.")
            self._save(memory)
            return
        self._save(memory)
        try:
            await self.publish(application, force=False, now=current)
        except Exception as exc:
            self.logger.exception("VIGVAMCEV automatic publish failed")
            memory = self.load_memory()
            state = self._state(memory)
            state["last_error"] = str(exc)[:500]
            self._save(memory)

    async def _loop(self, application: Any) -> None:
        self.logger.info("Запускаю VIGVAMCEV loop")
        try:
            while True:
                try:
                    await self.maybe_publish(application)
                except Exception:
                    self.logger.exception("ошибка VIGVAMCEV loop")
                await asyncio.sleep(self.settings.loop_interval_seconds)
        except asyncio.CancelledError:
            self.logger.info("VIGVAMCEV loop остановлен")
            raise

    async def start(self, application: Any) -> None:
        if self._task and not self._task.done():
            return
        self._task = asyncio.create_task(self._loop(application))

    async def stop(self) -> None:
        if not self._task:
            return
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass
        finally:
            self._task = None
