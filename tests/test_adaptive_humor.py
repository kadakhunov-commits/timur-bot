import asyncio
import os
from types import SimpleNamespace

import pytest

os.environ.setdefault("TELEGRAM_BOT_TOKEN", "test-token")
os.environ.setdefault("OPENAI_API_KEY", "test-api-key")

from timur_bot.services import bot_logic as runtime
from timur_bot.services.adaptive_humor import interjection_messages, parse_interjection
from timur_bot.services.conversation_policy import mark_snipe_sent


def test_quality_prompts_reject_literal_repetition_and_meta_jokes() -> None:
    history = [{"name": "рустем", "text": "я коммит сделал новый и вот тёщу тут"}]
    prompt = interjection_messages(history, "")[0]["content"]

    assert "Мысленно придумай три варианта" in prompt
    assert "Не повторяй последнее слово" in prompt
    assert "качестве своего юмора" in prompt
    assert parse_interjection('{"score":91,"reply":"ветка теперь с приданым"}') == (91, "ветка теперь с приданым")


def test_regular_participation_uses_the_quality_pipeline_before_sending(monkeypatch: pytest.MonkeyPatch) -> None:
    memory = runtime.default_memory()
    chat = runtime.get_chat_mem(memory, 77)
    chat["history"] = [{"name": "рустем", "text": "я коммит сделал новый и вот тёщу тут"}]
    mark_snipe_sent(chat)
    memory["config"]["adaptive_humor"]["min_human_messages_between_checks"] = 2
    runtime.note_human_message(chat)
    runtime.note_human_message(chat)

    class Bot:
        async def send_chat_action(self, chat_id: int, action: str) -> None:
            del chat_id, action

    class User:
        id = 2
        first_name = "рустем"
        username = ""

    message = SimpleNamespace(chat_id=77, from_user=User(), text="я коммит сделал новый и вот тёщу тут", caption=None)
    update = SimpleNamespace(effective_message=message)
    context = SimpleNamespace(bot=Bot())
    responses = iter(['{"score":91,"reply":"ветка теперь с приданым"}'])
    sent: dict[str, object] = {}

    async def fake_call(*_args: object, **_kwargs: object) -> str:
        return next(responses)

    async def fake_send(
        _update: object,
        _context: object,
        _memory: object,
        reply_text: str,
        **kwargs: object,
    ) -> None:
        sent["reply_text"] = reply_text
        sent.update(kwargs)

    monkeypatch.setattr(runtime, "call_openai_with_params", fake_call)
    monkeypatch.setattr(runtime, "send_reply_with_style", fake_send)
    monkeypatch.setattr(runtime.random, "random", lambda: 0.1)

    assert asyncio.run(runtime._maybe_send_adaptive_snipe(update, context, memory)) is True
    assert sent["reply_text"] == "ветка теперь с приданым"
    assert sent["is_snipe"] is False
    with pytest.raises(StopIteration):
        next(responses)
