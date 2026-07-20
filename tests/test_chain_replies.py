import asyncio
import os
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

os.environ.setdefault("TELEGRAM_BOT_TOKEN", "test-token")
os.environ.setdefault("OPENAI_API_KEY", "test-api-key")

from timur_bot.services import bot_logic as runtime


class DummySentMessage:
    def __init__(self, message_id: int) -> None:
        self.message_id = message_id


class DummyBot:
    def __init__(self) -> None:
        self.sent_calls: list[dict[str, object]] = []
        self._next_message_id = 200

    async def send_message(self, chat_id: int, text: str) -> DummySentMessage:
        self.sent_calls.append({"chat_id": chat_id, "text": text})
        self._next_message_id += 1
        return DummySentMessage(self._next_message_id)


class DummyMessage:
    def __init__(self, chat_id: int = 123, with_bot: bool = True) -> None:
        self.chat_id = chat_id
        self.message_id = 99
        self.text = "исходная реплика"
        self.caption = None
        self.reply_to_message = None
        self.from_user = SimpleNamespace(id=7)
        self.reply_calls: list[dict[str, object]] = []
        self._next_message_id = 100
        self._bot = DummyBot() if with_bot else None

    async def reply_text(self, text: str, do_quote: bool = True) -> DummySentMessage:
        self.reply_calls.append({"text": text, "do_quote": do_quote})
        self._next_message_id += 1
        return DummySentMessage(self._next_message_id)

    def get_bot(self) -> DummyBot | None:
        return self._bot


def test_chain_reply_sends_only_first_part_as_reply() -> None:
    memory = runtime.default_memory()
    message = DummyMessage(with_bot=True)
    update = SimpleNamespace(effective_message=message)

    with (
        patch.object(runtime, "split_into_chain", return_value=["первый", "второй"]),
        patch.object(runtime, "save_memory"),
        patch.object(runtime.billing, "register_bot_reply"),
        patch.object(runtime.billing, "should_apply_free_watermark", return_value=(False, "")),
        patch.object(runtime.random, "random", return_value=0.0),
        patch.object(runtime.random, "uniform", return_value=0.0),
        patch.object(runtime.asyncio, "sleep", side_effect=_immediate_sleep),
    ):
        asyncio.run(runtime.send_reply_with_style(update, None, memory, "первый\nвторой"))

    assert message.reply_calls == [{"text": "первый", "do_quote": True}]
    assert message.get_bot() is not None
    assert message.get_bot().sent_calls == [{"chat_id": 123, "text": "второй"}]


def test_chain_reply_falls_back_to_non_quoted_reply_without_bot_sender() -> None:
    memory = runtime.default_memory()
    message = DummyMessage(with_bot=False)
    update = SimpleNamespace(effective_message=message)

    with (
        patch.object(runtime, "split_into_chain", return_value=["первый", "второй"]),
        patch.object(runtime, "save_memory"),
        patch.object(runtime.billing, "register_bot_reply"),
        patch.object(runtime.billing, "should_apply_free_watermark", return_value=(False, "")),
        patch.object(runtime.random, "random", return_value=0.0),
        patch.object(runtime.random, "uniform", return_value=0.0),
        patch.object(runtime.asyncio, "sleep", side_effect=_immediate_sleep),
    ):
        asyncio.run(runtime.send_reply_with_style(update, None, memory, "первый\nвторой"))

    assert message.reply_calls == [
        {"text": "первый", "do_quote": True},
        {"text": "второй", "do_quote": False},
    ]


def test_premium_chat_bypasses_free_reply_cap_and_watermark() -> None:
    memory = runtime.default_memory()
    memory["config"]["funny_scan"]["main_chat_id"] = 0
    message = DummyMessage(chat_id=123)
    update = SimpleNamespace(effective_message=message)

    with (
        patch.object(runtime, "PREMIUM_CHAT_IDS", {123}),
        patch.object(runtime, "save_memory"),
        patch.object(runtime, "_store_bot_claim_memory"),
        patch.object(runtime.billing, "bot_replies_today", return_value=30),
        patch.object(runtime.billing, "register_bot_reply"),
        patch.object(runtime.billing, "should_apply_free_watermark") as watermark,
        patch.object(runtime.random, "random", return_value=1.0),
    ):
        sent = asyncio.run(runtime.send_reply_with_style(update, None, memory, "ответ"))

    assert sent is True
    assert message.reply_calls == [{"text": "ответ", "do_quote": True}]
    watermark.assert_not_called()


def test_ambient_reply_is_one_short_message_and_does_not_open_dialogue() -> None:
    memory = runtime.default_memory()
    message = DummyMessage()
    update = SimpleNamespace(effective_message=message)
    long_reply = "очень точная добивка которая почему-то решила стать сильно длиннее допустимого лимита"

    with (
        patch.object(runtime, "save_memory"),
        patch.object(runtime, "_store_bot_claim_memory"),
        patch.object(runtime, "_is_main_chat", return_value=True),
        patch.object(runtime.billing, "register_bot_reply"),
        patch.object(runtime.billing, "should_apply_free_watermark", return_value=(False, "")),
    ):
        sent = asyncio.run(
            runtime.send_reply_with_style(
                update,
                None,
                memory,
                long_reply,
                humor_plan={"mode": "ambient", "mechanism": "logic"},
                is_snipe=True,
            )
        )

    assert sent is True
    assert len(message.reply_calls) == 1
    assert len(str(message.reply_calls[0]["text"])) <= 45
    chat = runtime.get_chat_mem(memory, 123)
    assert chat["memory_layers"]["adaptive_humor"]["dialogue"] == {}
    assert chat["memory_layers"]["humor_scenes_v2"][-1]["output_kind"] == "ambient"


def test_vision_style_reply_without_humor_plan_obeys_direct_short_limit() -> None:
    memory = runtime.default_memory()
    message = DummyMessage()
    update = SimpleNamespace(effective_message=message)
    long_reply = "это очень длинная реакция на фотографию, которая начинает объяснять шутку вместо короткой точной добивки"

    with (
        patch.object(runtime, "save_memory"),
        patch.object(runtime, "_store_bot_claim_memory"),
        patch.object(runtime, "_is_main_chat", return_value=True),
        patch.object(runtime.billing, "register_bot_reply"),
        patch.object(runtime.billing, "should_apply_free_watermark", return_value=(False, "")),
        patch.object(runtime.random, "random", return_value=1.0),
    ):
        sent = asyncio.run(runtime.send_reply_with_style(update, None, memory, long_reply))

    assert sent is True
    assert len(str(message.reply_calls[0]["text"])) <= 70


def test_explicit_long_reply_can_bypass_casual_limit() -> None:
    memory = runtime.default_memory()
    message = DummyMessage()
    update = SimpleNamespace(effective_message=message)
    long_story = " ".join(["история"] * 20)

    with (
        patch.object(runtime, "save_memory"),
        patch.object(runtime, "_store_bot_claim_memory"),
        patch.object(runtime, "_is_main_chat", return_value=True),
        patch.object(runtime.billing, "register_bot_reply"),
        patch.object(runtime.billing, "should_apply_free_watermark", return_value=(False, "")),
        patch.object(runtime.random, "random", return_value=1.0),
    ):
        sent = asyncio.run(
            runtime.send_reply_with_style(update, None, memory, long_story, allow_long_reply=True)
        )

    assert sent is True
    assert message.reply_calls[0]["text"] == long_story


def test_technical_fallback_does_not_open_sticky_dialogue() -> None:
    memory = runtime.default_memory()
    message = DummyMessage()
    update = SimpleNamespace(effective_message=message)

    with (
        patch.object(runtime, "save_memory"),
        patch.object(runtime, "_store_bot_claim_memory"),
        patch.object(runtime, "_is_main_chat", return_value=True),
        patch.object(runtime.billing, "register_bot_reply"),
        patch.object(runtime.billing, "should_apply_free_watermark", return_value=(False, "")),
        patch.object(runtime.random, "random", return_value=1.0),
    ):
        sent = asyncio.run(
            runtime.send_reply_with_style(
                update,
                None,
                memory,
                runtime.TECHNICAL_FALLBACK_REPLY,
                humor_plan={"mode": "direct", "context": []},
                open_dialogue=False,
            )
        )

    assert sent is True
    assert message.reply_calls[0]["text"] == runtime.TECHNICAL_FALLBACK_REPLY
    chat = runtime.get_chat_mem(memory, 123)
    assert chat["memory_layers"]["adaptive_humor"]["dialogue"] == {}


def test_watermark_is_included_inside_final_ambient_length_limit() -> None:
    memory = runtime.default_memory()
    message = DummyMessage()
    update = SimpleNamespace(effective_message=message)

    with (
        patch.object(runtime, "save_memory"),
        patch.object(runtime, "_store_bot_claim_memory"),
        patch.object(runtime, "get_chat_features", return_value={**runtime.feature_gate.FREE_FEATURES, "max_daily_replies": 1000}),
        patch.object(runtime, "_is_main_chat", return_value=False),
        patch.object(runtime.billing, "bot_replies_today", return_value=0),
        patch.object(runtime.billing, "register_bot_reply"),
        patch.object(runtime.billing, "should_apply_free_watermark", return_value=(True, "free тимур")),
    ):
        sent = asyncio.run(
            runtime.send_reply_with_style(
                update,
                None,
                memory,
                "эта реплика специально намного длиннее итогового ограничения фонового сообщения",
                humor_plan={"mode": "ambient", "mechanism": "logic"},
                is_snipe=True,
            )
        )

    final_text = str(message.reply_calls[0]["text"])
    assert sent is True
    assert len(final_text) <= 45
    assert final_text.endswith("free тимур")


def test_failed_telegram_send_does_not_consume_reply_quota() -> None:
    memory = runtime.default_memory()
    message = DummyMessage()
    message.reply_text = AsyncMock(side_effect=RuntimeError("telegram down"))
    update = SimpleNamespace(effective_message=message)

    with (
        patch.object(runtime, "_is_main_chat", return_value=True),
        patch.object(runtime.billing, "register_bot_reply") as register_reply,
        patch.object(runtime.billing, "should_apply_free_watermark", return_value=(False, "")),
        patch.object(runtime.random, "random", return_value=1.0),
    ):
        with pytest.raises(RuntimeError, match="telegram down"):
            asyncio.run(runtime.send_reply_with_style(update, None, memory, "коротко"))

    register_reply.assert_not_called()


def test_partial_chain_delivery_opens_dialogue_and_is_persisted() -> None:
    memory = runtime.default_memory()
    message = DummyMessage(with_bot=True)
    assert message.get_bot() is not None
    message.get_bot().send_message = AsyncMock(side_effect=RuntimeError("second part failed"))
    update = SimpleNamespace(effective_message=message)

    with (
        patch.object(runtime, "split_into_chain", return_value=["первый", "второй"]),
        patch.object(runtime, "save_memory") as save_memory,
        patch.object(runtime, "_store_bot_claim_memory") as store_claim,
        patch.object(runtime, "_is_main_chat", return_value=True),
        patch.object(runtime.billing, "register_bot_reply"),
        patch.object(runtime.billing, "should_apply_free_watermark", return_value=(False, "")),
        patch.object(runtime.random, "random", return_value=0.0),
        patch.object(runtime.random, "uniform", return_value=0.0),
        patch.object(runtime.asyncio, "sleep", side_effect=_immediate_sleep),
    ):
        sent = asyncio.run(runtime.send_reply_with_style(update, None, memory, "первый\nвторой"))

    assert sent is True
    assert save_memory.called
    chat = runtime.get_chat_mem(memory, 123)
    assert chat["memory_layers"]["adaptive_humor"]["dialogue"]["open_followup"] is True
    assert [item["text"] for item in chat["history"] if item.get("is_bot")] == ["первый"]
    store_claim.assert_called_once_with(memory, message, "первый")


def test_voice_success_is_not_duplicated_when_watermark_send_fails() -> None:
    memory = runtime.default_memory()
    message = DummyMessage(chat_id=124)
    message.reply_voice = AsyncMock(return_value=DummySentMessage(333))
    message.reply_text = AsyncMock(side_effect=RuntimeError("watermark failed"))
    update = SimpleNamespace(effective_message=message)

    with (
        patch.object(runtime, "GEMINI_API_KEY", "key"),
        patch.object(runtime, "reserve_voice_attempt", return_value=True) as reserve_voice,
        patch.object(runtime, "synthesize_ogg_opus_from_text", return_value=b"ogg") as synthesize,
        patch.object(runtime, "save_memory"),
        patch.object(runtime, "_store_bot_claim_memory"),
        patch.object(runtime, "get_chat_features", return_value={**runtime.feature_gate.FREE_FEATURES, "max_daily_replies": 1000}),
        patch.object(runtime, "_is_main_chat", return_value=False),
        patch.object(runtime.billing, "bot_replies_today", return_value=0),
        patch.object(runtime.billing, "register_bot_reply"),
        patch.object(runtime.billing, "should_apply_free_watermark", return_value=(True, "free тимур")),
    ):
        sent = asyncio.run(runtime.send_reply_with_style(update, None, memory, "голосом", force_voice=True))

    assert sent is True
    assert message.reply_voice.await_count == 1
    assert message.reply_text.await_count == 1
    reserve_voice.assert_called_once_with(memory, 124)
    assert synthesize.call_args.kwargs["timeout_seconds"] < runtime.VOICE_TTS_TIMEOUT_SECONDS


def test_voice_generation_timeout_falls_back_and_releases_send_lock() -> None:
    memory = runtime.default_memory()
    message = DummyMessage(chat_id=125)
    update = SimpleNamespace(effective_message=message)

    async def slow_thread(*args, **kwargs):
        del args, kwargs
        await asyncio.sleep(1)
        return b"ogg"

    with (
        patch.object(runtime, "GEMINI_API_KEY", "key"),
        patch.object(runtime, "VOICE_TTS_TIMEOUT_SECONDS", 0.01),
        patch.object(runtime, "reserve_voice_attempt", return_value=True) as reserve_voice,
        patch.object(runtime.asyncio, "to_thread", side_effect=slow_thread),
        patch.object(runtime, "save_memory"),
        patch.object(runtime, "_store_bot_claim_memory"),
        patch.object(runtime, "_is_main_chat", return_value=True),
        patch.object(runtime.billing, "register_bot_reply"),
        patch.object(runtime.billing, "should_apply_free_watermark", return_value=(False, "")),
        patch.object(runtime.random, "random", return_value=1.0),
    ):
        sent = asyncio.run(runtime.send_reply_with_style(update, None, memory, "голосом", force_voice=True))

    assert sent is True
    reserve_voice.assert_called_once_with(memory, 125)
    assert message.reply_calls == [{"text": "голосом", "do_quote": True}]


def test_parallel_sends_cannot_overshoot_daily_reply_cap() -> None:
    memory = runtime.default_memory()
    messages = [DummyMessage(chat_id=9876), DummyMessage(chat_id=9876)]
    updates = [SimpleNamespace(effective_message=message) for message in messages]
    counter = {"value": 0}

    async def delayed_reply(text: str, do_quote: bool = True) -> DummySentMessage:
        del text, do_quote
        await asyncio.sleep(0.01)
        return DummySentMessage(444)

    for message in messages:
        message.reply_text = AsyncMock(side_effect=delayed_reply)

    def register(_: int) -> int:
        counter["value"] += 1
        return counter["value"]

    async def run_both() -> list[bool]:
        return await asyncio.gather(
            runtime.send_reply_with_style(updates[0], None, memory, "раз"),
            runtime.send_reply_with_style(updates[1], None, memory, "два"),
        )

    with (
        patch.object(runtime, "get_chat_features", return_value={"max_daily_replies": 1, "tier": "free"}),
        patch.object(runtime, "_is_main_chat", return_value=False),
        patch.object(runtime.billing, "bot_replies_today", side_effect=lambda _: counter["value"]),
        patch.object(runtime.billing, "register_bot_reply", side_effect=register),
        patch.object(runtime.billing, "should_apply_free_watermark", return_value=(False, "")),
        patch.object(runtime, "save_memory"),
        patch.object(runtime, "_store_bot_claim_memory"),
        patch.object(runtime.random, "random", return_value=1.0),
    ):
        results = asyncio.run(run_both())

    assert results == [True, False]
    assert counter["value"] == 1
    assert sum(message.reply_text.await_count for message in messages) == 1


async def _immediate_sleep(_: float) -> None:
    return None
