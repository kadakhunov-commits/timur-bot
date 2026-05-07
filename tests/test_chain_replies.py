import asyncio
import os
from types import SimpleNamespace
from unittest.mock import patch

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


async def _immediate_sleep(_: float) -> None:
    return None
