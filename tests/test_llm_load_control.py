import asyncio
import os
import time
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import pytest

os.environ.setdefault("TELEGRAM_BOT_TOKEN", "test-token")
os.environ.setdefault("OPENAI_API_KEY", "test-api-key")

from timur_bot.services import bot_logic as runtime
from timur_bot.services.llm_load_control import (
    foreground_activity,
    release_background,
    reserve_background,
    reset_for_tests,
)
from timur_bot.services.rolling_memory import enqueue_from_history


@pytest.fixture(autouse=True)
def _reset_load_state():
    reset_for_tests(last_foreground_activity=0.0)
    yield
    reset_for_tests()


def test_background_is_blocked_during_foreground_and_quiet_window() -> None:
    now = time.monotonic()
    with foreground_activity("direct_reply"):
        active = reserve_background("rolling", quiet_seconds=300, now=now + 1000)
        assert active.acquired is False
        assert active.reason == "foreground_active"

    recent = reserve_background("rolling", quiet_seconds=300)
    assert recent.acquired is False
    assert recent.reason == "foreground_recent"


def test_only_one_background_request_can_hold_provider_slot() -> None:
    first = reserve_background("rolling", quiet_seconds=300, now=1000)
    assert first.acquired is True

    second = reserve_background("funny_scan", quiet_seconds=300, now=1000)
    assert second.acquired is False
    assert second.reason == "background_active"

    release_background(first)
    third = reserve_background("funny_scan", quiet_seconds=300, now=1000)
    assert third.acquired is True
    release_background(third)


def test_scheduled_rolling_memory_defers_without_consuming_candidate() -> None:
    memory = runtime.default_memory()
    settings = runtime._rolling_memory_settings(memory)
    chat = runtime.get_chat_mem(memory, 777)
    current = datetime.now(timezone.utc)
    anchor = {
        "message_id": 1,
        "name": "аня",
        "text": "поезд опять отменили прямо перед отправлением",
        "ts": current.isoformat(),
        "is_bot": False,
    }
    chat["history"] = [anchor]
    assert enqueue_from_history(chat, chat_id=777, anchor=anchor, settings=settings, now=current, force=True)
    reset_for_tests()

    with (
        patch.object(runtime, "load_memory", return_value=memory),
        patch.object(runtime, "save_memory") as save_memory,
        patch.object(runtime, "call_openai_metered", new=AsyncMock()) as llm_call,
    ):
        result = asyncio.run(runtime._process_rolling_memory_once(chat_id=777, respect_load_guard=True))

    assert result["attempted"] == 0
    assert len(chat["memory_layers"]["rolling_memory"]["pending"]) == 1
    llm_call.assert_not_awaited()
    save_memory.assert_not_called()
