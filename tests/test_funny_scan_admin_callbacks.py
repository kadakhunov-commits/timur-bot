import asyncio
import os
from types import SimpleNamespace

import pytest

os.environ.setdefault("TELEGRAM_BOT_TOKEN", "test-token")
os.environ.setdefault("OPENAI_API_KEY", "test-api-key")

from timur_bot.services import bot_logic as runtime
from timur_bot.services.funny_scan_storage import default_funny_scan_state


class DummyQuery:
    def __init__(self, data: str, chat_id: int = 1) -> None:
        self.data = data
        self.message = SimpleNamespace(chat_id=chat_id)
        self.edited_text = ""
        self.reply_markup = None
        self.answer_calls = []

    async def answer(self, *args, **kwargs) -> None:
        self.answer_calls.append((args, kwargs))

    async def edit_message_text(self, text: str, reply_markup=None) -> None:
        self.edited_text = text
        self.reply_markup = reply_markup


class DummyContext:
    def __init__(self) -> None:
        self.user_data = {}
        self.bot = SimpleNamespace()
        self.application = SimpleNamespace()


def _make_update(query: DummyQuery):
    return SimpleNamespace(
        callback_query=query,
        effective_user=SimpleNamespace(id=runtime.OWNER_ID),
        effective_chat=SimpleNamespace(id=query.message.chat_id),
    )


@pytest.mark.parametrize("callback_data", ["adm:funny:toggle:1", "adm:funny:menu:1"])
def test_funny_callbacks_render_without_error(monkeypatch: pytest.MonkeyPatch, callback_data: str) -> None:
    memory = runtime.default_memory()
    state = default_funny_scan_state()

    monkeypatch.setattr(runtime, "load_memory", lambda: memory)
    monkeypatch.setattr(runtime, "save_memory", lambda _memory: None)
    monkeypatch.setattr(runtime, "_load_funny_scan_state", lambda: state)
    monkeypatch.setattr(runtime, "_save_funny_scan_state", lambda _state: None)

    query = DummyQuery(callback_data)
    update = _make_update(query)
    context = DummyContext()

    asyncio.run(runtime.admin_callback_handler(update, context))

    assert query.edited_text


def test_funny_source_toggle_updates_sources(monkeypatch: pytest.MonkeyPatch) -> None:
    memory = runtime.default_memory()
    memory["chats"]["-1001"] = {"history": [{"message_id": 1, "text": "x", "ts": "2026-04-25T10:00:00"}]}
    state = default_funny_scan_state()

    monkeypatch.setattr(runtime, "load_memory", lambda: memory)
    monkeypatch.setattr(runtime, "save_memory", lambda _memory: None)
    monkeypatch.setattr(runtime, "_load_funny_scan_state", lambda: state)
    monkeypatch.setattr(runtime, "_save_funny_scan_state", lambda _state: None)

    query = DummyQuery("adm:funny:source_toggle:-1001:1")
    update = _make_update(query)
    context = DummyContext()
    asyncio.run(runtime.admin_callback_handler(update, context))

    settings = runtime._get_funny_scan_settings(memory)
    source = next(item for item in settings["sources"] if int(item["chat_id"]) == -1001)
    assert source["enabled"] is True


def test_funny_callbacks_handle_bad_int_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    memory = runtime.default_memory()
    state = default_funny_scan_state()
    monkeypatch.setattr(runtime, "load_memory", lambda: memory)
    monkeypatch.setattr(runtime, "save_memory", lambda _memory: None)
    monkeypatch.setattr(runtime, "_load_funny_scan_state", lambda: state)
    monkeypatch.setattr(runtime, "_save_funny_scan_state", lambda _state: None)

    query = DummyQuery("adm:funny:period_set:not-int:1")
    update = _make_update(query)
    context = DummyContext()

    asyncio.run(runtime.admin_callback_handler(update, context))

    assert any(kwargs.get("show_alert") for _, kwargs in query.answer_calls)
