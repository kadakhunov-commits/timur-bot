from __future__ import annotations

import asyncio
import copy
import io
import json
from dataclasses import replace
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

from PIL import Image

from timur_bot.services.polza_image import GeneratedImage, PolzaImageClient
from timur_bot.services.vigvamcev import (
    CanonCorpus,
    DraftError,
    VigvamcevService,
    VigvamcevCandidate,
    VigvamcevSettings,
    build_full_poster_prompt,
    clone_name_from_word,
    default_vigvamcev_state,
    format_caption,
    generate_candidate,
    TransientTextRequestError,
    validate_candidate,
)
from timur_bot.services.vigvamcev_poster import POSTER_SIZE, compose_poster
from telegram.error import TimedOut


CORPUS_ROOT = Path(__file__).parents[1] / "assets" / "vigvamcev"


def _settings(**overrides) -> VigvamcevSettings:
    raw = {
        "caption_min_chars": 600,
        "caption_max_chars": 900,
        "max_stage_attempts": 2,
        "retry_backoff_seconds": 0,
        **overrides,
    }
    channel_id = int(raw.pop("channel_id", 0))
    return VigvamcevSettings.from_mapping(raw, root=Path.cwd(), channel_id=channel_id, asset_dir=CORPUS_ROOT)


def _scene_bytes() -> bytes:
    output = io.BytesIO()
    Image.new("RGB", (640, 480), (220, 140, 40)).save(output, format="PNG")
    return output.getvalue()


def _candidate() -> VigvamcevCandidate:
    story = (
        "Клон: Фотонцев — двадцать третий эксперимент Доктора Ю, клон, научившийся двигать фотоны. "
        "Свет вокруг него собирается в маленькие солнечные окна, поэтому охрана увидела сразу двадцать "
        "выходов и бросилась к каждому, и из-за этого лаборатория на минуту опустела. Клон понял, что свет "
        "повторяет только страх его собеседника, и превратил тревогу Доктора Ю в карту подземного этажа, "
        "которую нельзя прочитать при обычном свете."
        + chr(10) * 2
        + "SIC: Утечка из комплекса Фонда SIC показала, что световая карта Фотонцева ведёт прямо к "
        "запечатанному отсеку с «идеальным организмом». Профессор Галактикус приказал опечатать этаж, "
        "но отсек уже не пуст: следующий эксперимент получит не ключ, а нового сторожа."
    )
    return VigvamcevCandidate(
        post_no=23,
        experiment_no=45,
        clone_name="Фотонцев",
        source_word="фотон",
        ability="управление фотонами",
        canon_anchors=["Доктор Ю продолжает серию экспериментов"],
        conflict="способность ломает систему охраны",
        twist="свет повторяет страх собеседника",
        consequence="клон оставляет луч для следующего побега",
        next_hook="следующий эксперимент получает карту",
        visual_brief="клон в лаборатории, вокруг него окна из света и разъезжающиеся двери",
        novelty_tags=["световые окна", "карта из страха"],
        story=story,
    )


def test_corpus_includes_recovered_post_10_and_seed_state() -> None:
    corpus = CanonCorpus.load(CORPUS_ROOT)
    settings = _settings()
    state = default_vigvamcev_state(corpus, settings)

    post_10 = next(post for post in corpus.posts if post.post_no == 10)
    assert post_10.experiments == (32,)
    assert post_10.text_path.read_text(encoding="utf-8").startswith("№ 10/1025")
    assert corpus.last_experiment == 44
    assert state["post_no"] == 22
    assert state["experiment_no"] == 44
    assert "миллионцев" in state["used_names"]
    assert len(corpus.visual_references) >= 5


def test_name_and_candidate_validation_follow_series_rules() -> None:
    corpus = CanonCorpus.load(CORPUS_ROOT)
    settings = _settings()
    state = default_vigvamcev_state(corpus, settings)
    candidate = _candidate()

    assert clone_name_from_word("фотон") == "Фотонцев"
    assert len(format_caption(candidate, hashtags=settings.story_hashtags)) in range(600, 901)
    assert validate_candidate(candidate, state=state, corpus=corpus, settings=settings) == []


def test_candidate_rejects_used_name_and_motif() -> None:
    corpus = CanonCorpus.load(CORPUS_ROOT)
    settings = _settings()
    state = default_vigvamcev_state(corpus, settings)
    state["used_names"].append("Фотонцев")
    state["used_motifs"].append("световые окна")

    errors = validate_candidate(_candidate(), state=state, corpus=corpus, settings=settings)

    assert "имя клона уже использовалось" in errors
    assert "основной novelty-мотив уже использовался" in errors


def test_full_poster_prompt_contains_exact_text_and_variation() -> None:
    settings = _settings(poster_mode="full", variation_pool=["giant surreal object in front"], style_prompt="no readable text, no logos")
    candidate = _candidate()

    prompt = build_full_poster_prompt(candidate, settings, variation="giant surreal object in front")

    assert "#ВИГВАМЦЕВ: ИСТОРИИ2" in prompt
    assert "ФОТОНЦЕВ" in prompt
    assert "№23 · ЭКСПЕРИМЕНТ 45" in prompt
    assert "no readable text" not in prompt
    assert "giant surreal object in front" in prompt


def test_story_requires_clone_and_sic_blocks() -> None:
    corpus = CanonCorpus.load(CORPUS_ROOT)
    settings = _settings()
    state = default_vigvamcev_state(corpus, settings)
    candidate = _candidate()

    no_sic = replace(candidate, story=candidate.story.split("SIC:")[0].strip())
    no_clone = replace(candidate, story="SIC: " + candidate.story.split("SIC:")[1].strip())

    clone_missing = validate_candidate(no_clone, state=state, corpus=corpus, settings=settings)
    sic_missing = validate_candidate(no_sic, state=state, corpus=corpus, settings=settings)

    assert "в истории нет блока «Клон: …»" in clone_missing
    assert "в истории нет блока «SIC: …»" in sic_missing


def test_text_stage_retries_transient_provider_timeout() -> None:
    corpus = CanonCorpus.load(CORPUS_ROOT)
    settings = _settings(max_stage_attempts=2, text_retry_backoff_seconds=0)
    state = default_vigvamcev_state(corpus, settings)
    calls = 0

    async def text_request(_prompt: str, _max_tokens: int) -> str:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise TransientTextRequestError("Request timed out")
        return json.dumps(_candidate().to_dict(hashtags=settings.story_hashtags), ensure_ascii=False)

    async def reviewer(_prompt: str, _max_tokens: int) -> str:
        return '{"ok": true}'

    result = asyncio.run(
        generate_candidate(
            corpus,
            state,
            settings,
            text_request=text_request,
            reviewer=reviewer,
            post_no=23,
            experiment_no=45,
        )
    )

    assert result.clone_name == "Фотонцев"
    assert calls == 2


def test_reviewer_stage_retries_transient_provider_timeout() -> None:
    corpus = CanonCorpus.load(CORPUS_ROOT)
    settings = _settings(max_stage_attempts=2, text_retry_backoff_seconds=0)
    state = default_vigvamcev_state(corpus, settings)
    reviewer_calls = 0

    async def text_request(_prompt: str, _max_tokens: int) -> str:
        return json.dumps(_candidate().to_dict(hashtags=settings.story_hashtags), ensure_ascii=False)

    async def reviewer(_prompt: str, _max_tokens: int) -> str:
        nonlocal reviewer_calls
        reviewer_calls += 1
        if reviewer_calls == 1:
            raise TransientTextRequestError("Request timed out")
        return '{"ok": true}'

    result = asyncio.run(
        generate_candidate(
            corpus,
            state,
            settings,
            text_request=text_request,
            reviewer=reviewer,
            post_no=23,
            experiment_no=45,
        )
    )

    assert result.clone_name == "Фотонцев"
    assert reviewer_calls == 2


def test_poster_is_deterministic_size_and_nonempty() -> None:
    first = compose_poster(_scene_bytes(), "Фотонцев", post_no=23, experiment_no=45)
    second = compose_poster(_scene_bytes(), "Фотонцев", post_no=23, experiment_no=45)

    assert first == second
    with Image.open(io.BytesIO(first)) as image:
        assert image.size == POSTER_SIZE
        assert image.format == "PNG"


def test_poster_accepts_optional_transparent_identity_layer() -> None:
    layer = io.BytesIO()
    identity = Image.new("RGBA", (120, 160), (220, 30, 30, 180))
    identity.save(layer, format="PNG")

    poster = compose_poster(
        _scene_bytes(),
        "Фотонцев",
        post_no=23,
        experiment_no=45,
        identity_layer_bytes=layer.getvalue(),
    )

    with Image.open(io.BytesIO(poster)) as image:
        assert image.size == POSTER_SIZE


def test_polza_media_client_uploads_refs_and_polls_result(tmp_path: Path) -> None:
    reference = tmp_path / "reference.jpg"
    reference.write_bytes(b"reference")
    calls = []
    status_calls = 0

    def request(method: str, path: str, payload):
        nonlocal status_calls
        calls.append((method, path, payload))
        if path == "/storage/upload":
            return {"url": "https://cdn.example/reference.jpg"}
        if method == "POST" and path == "/media":
            assert payload["model"] == "openai/gpt-5.4-image-2"
            assert payload["input"]["aspect_ratio"] == "4:3"
            assert payload["input"]["images"] == ["https://cdn.example/reference.jpg"]
            assert payload["async"] is True
            return {"id": "gen_123", "status": "pending"}
        if method == "GET" and path == "/media/gen_123":
            status_calls += 1
            if status_calls == 1:
                return {"id": "gen_123", "status": "processing"}
            return {"id": "gen_123", "status": "completed", "data": {"url": "https://cdn.example/result.png"}}
        raise AssertionError((method, path))

    async def sleep(_seconds: float) -> None:
        return None

    client = PolzaImageClient(
        api_key="secret",
        request_json=request,
        download_url=lambda url: _scene_bytes(),
        sleep=sleep,
        poll_interval_seconds=0.01,
    )
    result = asyncio.run(client.generate_scene(prompt="сцена", reference_paths=[reference]))

    assert result.content == _scene_bytes()
    assert result.generation_id == "gen_123"
    assert [call[:2] for call in calls] == [
        ("POST", "/storage/upload"),
        ("POST", "/media"),
        ("GET", "/media/gen_123"),
        ("GET", "/media/gen_123"),
    ]


class _FakeImageClient:
    configured = True

    def __init__(self, content: bytes | None = None, error: Exception | None = None) -> None:
        self.content = content or _scene_bytes()
        self.error = error
        self.calls = 0
        self.reference_paths = []

    async def generate_scene(self, *, prompt: str, reference_paths):
        del prompt
        self.reference_paths = list(reference_paths)
        self.calls += 1
        if self.error:
            raise self.error
        return GeneratedImage(self.content, generation_id=f"gen-{self.calls}")


class _FakeBot:
    def __init__(self, result=None, error: Exception | None = None) -> None:
        self.result = result or SimpleNamespace(message_id=77)
        self.error = error
        self.photo_calls = []

    async def send_photo(self, **kwargs):
        self.photo_calls.append(kwargs)
        if self.error:
            raise self.error
        return self.result


def _service(
    tmp_path: Path,
    *,
    image_client=None,
    memory=None,
    channel_id=-100123,
    settings_overrides=None,
    compose=None,
) -> tuple[VigvamcevService, dict, _FakeBot]:
    corpus = CanonCorpus.load(CORPUS_ROOT)
    settings = _settings(channel_id=channel_id, retry_backoff_seconds=0, **(settings_overrides or {}))
    state = memory or {"config": {}}
    bot = _FakeBot()

    def load_memory():
        return copy.deepcopy(state)

    def save_memory(value):
        state.clear()
        state.update(copy.deepcopy(value))
        return True

    async def text_request(_prompt: str, _max_tokens: int) -> str:
        return json.dumps(_candidate().to_dict(hashtags=settings.story_hashtags), ensure_ascii=False)

    async def reviewer(_prompt: str, _max_tokens: int) -> str:
        return '{"ok": true, "reason": "ok"}'

    service = VigvamcevService(
        settings=settings,
        corpus=corpus,
        image_client=image_client or _FakeImageClient(),
        text_request=text_request,
        reviewer=reviewer,
        load_memory=load_memory,
        save_memory=save_memory,
        memory_path=tmp_path / "memory.json",
        owner_ids=[1],
        **({"compose": compose} if compose is not None else {}),
    )
    return service, state, bot


def test_service_reuses_ready_draft_without_regenerating_image(tmp_path: Path) -> None:
    image_client = _FakeImageClient()
    service, state, _ = _service(tmp_path, image_client=image_client)

    first = asyncio.run(service.prepare_draft())
    second = asyncio.run(service.prepare_draft())

    assert first.image_path == second.image_path
    assert image_client.calls == 1
    assert state["config"]["vigvamcev"]["draft"]["status"] == "ready"
    assert state["config"]["vigvamcev"]["draft"]["canon_status"] == "draft"
    assert state["config"]["vigvamcev"]["history"] == []
    assert CORPUS_ROOT / "references" / "clones.jpg" in image_client.reference_paths


def test_service_publishes_to_fixed_channel_and_advances_sequence(tmp_path: Path) -> None:
    service, state, bot = _service(tmp_path)
    application = SimpleNamespace(bot=bot)

    prepared = asyncio.run(service.publish(application, force=True, now=datetime(2026, 8, 20, 13, 0)))

    assert prepared is not None
    assert bot.photo_calls[0]["chat_id"] == -100123
    saved = state["config"]["vigvamcev"]
    assert saved["post_no"] == 23
    assert saved["experiment_no"] == 45
    assert saved["last_published"]["message_id"] == 77
    assert saved["history"][-1]["canon_status"] == "generated_canon"
    assert saved["draft"] == {}


def test_unknown_telegram_timeout_blocks_automatic_duplicate(tmp_path: Path) -> None:
    bot = _FakeBot(error=TimedOut("unknown"))
    service, state, _ = _service(tmp_path)
    application = SimpleNamespace(bot=bot)
    async def no_notify(*_args, **_kwargs):
        return None

    service._notify_owner = no_notify

    async def publish_once():
        with pytest.raises(Exception):
            await service.publish(application, force=True, now=datetime(2026, 8, 20, 13, 0))

    asyncio.run(publish_once())
    assert state["config"]["vigvamcev"]["publish_status"] == "publish_unknown"
    with pytest.raises(Exception, match="publish_unknown"):
        asyncio.run(service.publish(application, force=True, now=datetime(2026, 8, 20, 13, 1)))
    assert len(bot.photo_calls) == 1


def test_schedule_is_due_only_after_configured_time(tmp_path: Path) -> None:
    service, state, _ = _service(tmp_path)
    memory = state
    current_state = service._state(memory)

    assert service.is_due(datetime(2026, 8, 20, 12, 59), current_state) is False
    assert service.is_due(datetime(2026, 8, 20, 13, 0), current_state) is True
    current_state["last_published"] = {"published_date": "2026-08-20"}
    assert service.is_due(datetime(2026, 8, 20, 13, 1), current_state) is False


def test_prepare_full_mode_uses_model_poster_without_compose(tmp_path: Path) -> None:
    compose_calls: list = []

    def spy(*args, **kwargs):
        compose_calls.append((args, kwargs))
        return _scene_bytes()

    image_client = _FakeImageClient(content=_scene_bytes())
    service, state, _ = _service(
        tmp_path,
        image_client=image_client,
        settings_overrides={"poster_mode": "full"},
        compose=spy,
    )

    prepared = asyncio.run(service.prepare_draft())

    assert compose_calls == []
    assert prepared.image_path.exists()
    assert prepared.image_path.read_bytes() == _scene_bytes()
    saved = state["config"]["vigvamcev"]
    assert saved["draft"]["status"] == "ready"
    assert saved["draft"]["poster_mode"] == "full"


class _FullFailImageClient(_FakeImageClient):
    async def generate_scene(self, *, prompt: str, reference_paths):
        del reference_paths
        self.prompts.append(prompt)
        self.calls += 1
        if "FINAL cover poster" in prompt:
            raise DraftError("full failed")
        return GeneratedImage(self.content, generation_id=f"gen-{self.calls}")

    def __init__(self, content: bytes | None = None) -> None:
        super().__init__(content=content or _scene_bytes())
        self.prompts: list[str] = []


def test_full_mode_falls_back_to_local_compose(tmp_path: Path) -> None:
    compose_calls: list = []

    def spy(*args, **kwargs):
        compose_calls.append((args, kwargs))
        return _scene_bytes()

    service, state, _ = _service(
        tmp_path,
        image_client=_FullFailImageClient(),
        settings_overrides={"poster_mode": "full"},
        compose=spy,
    )

    prepared = asyncio.run(service.prepare_draft())

    assert len(compose_calls) == 1
    assert prepared.image_path.exists()
    saved = state["config"]["vigvamcev"]
    assert saved["draft"]["status"] == "ready"
    assert saved["draft"]["poster_mode"] == "composed"
