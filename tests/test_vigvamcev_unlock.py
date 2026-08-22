from __future__ import annotations

import asyncio
import copy
import io
import json
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

from PIL import Image

from timur_bot.services.vigvamcev import CanonCorpus, VigvamcevCandidate, VigvamcevService, VigvamcevSettings
from telegram.error import TimedOut

CORPUS_ROOT = Path(__file__).parents[1] / "assets" / "vigvamcev"


def _settings() -> VigvamcevSettings:
    return VigvamcevSettings.from_mapping(
        {"caption_min_chars": 600, "caption_max_chars": 900, "style_reference_count": 3, "max_stage_attempts": 2, "retry_backoff_seconds": 0},
        root=Path.cwd(), channel_id=-100123, asset_dir=CORPUS_ROOT,
    )


def _candidate() -> VigvamcevCandidate:
    clone_block = (
        "Клон: Фотонцев — двадцать третий эксперимент Доктора Ю, клон, научившийся двигать фотоны. "
        "Свет вокруг него собирается в маленькие солнечные окна, поэтому охрана увидела сразу двадцать "
        "выходов и бросилась к каждому, и из-за этого лаборатория на минуту опустела. Клон понял, что свет "
        "повторяет только страх его собеседника, и превратил тревогу Доктора Ю в карту подземного этажа, "
        "которую нельзя прочитать при обычном свете."
    )
    sic_block = (
        "SIC: Утечка из комплекса Фонда SIC показала, что световая карта Фотонцева ведёт прямо к "
        "запечатанному отсеку с «идеальным организмом». Профессор Галактикус приказал опечатать этаж, "
        "но отсек уже не пуст: следующий эксперимент получит не ключ, а нового сторожа."
    )
    return VigvamcevCandidate(
        post_no=23, experiment_no=45, clone_name="Фотонцев", source_word="фотон",
        ability="управление фотонами", canon_anchors=["Доктор Ю продолжает серию экспериментов"],
        conflict="способность ломает систему охраны", twist="свет повторяет страх собеседника",
        consequence="клон оставляет луч для следующего побега", next_hook="следующий эксперимент получает карту",
        visual_brief="клон в лаборатории, вокруг него окна из света и разъезжающиеся двери",
        novelty_tags=["световые окна", "карта из страха"], story=clone_block + chr(10) * 2 + sic_block,
    )


class _FakeImageClient:
    configured = True

    def __init__(self):
        self.calls = 0
        self.reference_paths: list[Path] = []

    async def generate_scene(self, *, prompt: str, reference_paths):
        del prompt
        self.reference_paths = list(reference_paths)
        self.calls += 1
        output = io.BytesIO()
        Image.new("RGB", (640, 480), (220, 140, 40)).save(output, format="PNG")
        from timur_bot.services.polza_image import GeneratedImage

        return GeneratedImage(output.getvalue(), generation_id=f"gen-{self.calls}")


class _FakeBot:
    def __init__(self, result=None, error=None):
        self.result = result or SimpleNamespace(message_id=77)
        self.error = error
        self.photo_calls = []

    async def send_photo(self, **kwargs):
        self.photo_calls.append(kwargs)
        if self.error:
            raise self.error
        return self.result


class _FakeMessage:
    def __init__(self):
        self.replies = []
        self.chat = SimpleNamespace(type="private")
        self.text = "/vigvamcev unlock"

    async def reply_text(self, text, **_kwargs):
        self.replies.append(text)


def _service(tmp_path: Path, bot: _FakeBot) -> tuple[VigvamcevService, dict]:
    corpus = CanonCorpus.load(CORPUS_ROOT)
    settings = _settings()
    state = {"config": {"vigvamcev": {}}}

    def load_memory():
        return copy.deepcopy(state)

    def save_memory(value):
        state.clear()
        state.update(copy.deepcopy(value))
        return True

    async def text_request(_prompt: str, _max_tokens: int) -> str:
        return json.dumps(_candidate().to_dict(hashtags=settings.story_hashtags), ensure_ascii=False)

    async def reviewer(_prompt: str, _max_tokens: int) -> str:
        return '{"ok": true}'

    service = VigvamcevService(
        settings=settings, corpus=corpus, image_client=_FakeImageClient(),
        text_request=text_request, reviewer=reviewer, load_memory=load_memory,
        save_memory=save_memory, memory_path=tmp_path / "memory.json", owner_ids=[1],
    )
    return service, state


def test_publish_unknown_blocks_and_unlock_restores_ready_draft(tmp_path: Path) -> None:
    bot = _FakeBot(error=TimedOut("unknown"))
    service, state = _service(tmp_path, bot)
    application = SimpleNamespace(bot=bot)

    async def no_notify(*_args, **_kwargs):
        return None

    service._notify_owner = no_notify

    with pytest.raises(Exception):
        asyncio.run(service.publish(application, force=True, now=datetime(2026, 8, 21, 13, 0)))

    saved = state["config"]["vigvamcev"]
    assert saved["publish_status"] == "publish_unknown"
    assert len(bot.photo_calls) == 1

    bot2 = _FakeBot()
    message = _FakeMessage()
    asyncio.run(service.handle_owner_command(SimpleNamespace(effective_message=message), SimpleNamespace(application=None)))

    saved_after_unlock = state["config"]["vigvamcev"]
    assert saved_after_unlock["publish_status"] == "idle"
    assert saved_after_unlock["draft"]["status"] == "ready"
    assert "разблокирована" in message.replies[-1]

    prepared = asyncio.run(service.publish(SimpleNamespace(bot=bot2), force=True, now=datetime(2026, 8, 21, 13, 5)))
    assert prepared is not None
    assert len(bot2.photo_calls) == 1
    saved_final = state["config"]["vigvamcev"]
    assert saved_final["post_no"] == 23
    assert saved_final["experiment_no"] == 45
