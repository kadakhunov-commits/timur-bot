import asyncio
import copy
import os
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

os.environ.setdefault("TELEGRAM_BOT_TOKEN", "test-token")
os.environ.setdefault("OPENAI_API_KEY", "test-api-key")

from timur_bot.services import bot_logic as runtime
from timur_bot.services import feature_gate
from timur_bot.services.rolling_memory import enqueue_from_history, ensure_state


NOW = datetime(2026, 7, 20, 12, 0, tzinfo=timezone.utc)


def _message(message_id: int, text: str, *, chat_id: int = 777):
    return SimpleNamespace(
        chat_id=chat_id,
        message_id=message_id,
        text=text,
        caption=None,
        date=NOW + timedelta(minutes=message_id),
        from_user=SimpleNamespace(id=42, first_name="аня", username="anya", is_bot=False),
        sender_chat=None,
        forward_origin=None,
        reply_to_message=None,
    )


def test_new_human_message_enqueues_memory_without_growing_legacy_recent_facts() -> None:
    memory = runtime.default_memory()
    memory["config"]["rolling_memory"]["sample_rate"] = 1.0

    with patch.object(runtime.billing, "register_activity"):
        runtime.update_memory_with_message(memory, _message(1, "поезд опять отменили прямо перед отправлением"))

    chat = runtime.get_chat_mem(memory, 777)
    assert len(chat["memory_layers"]["rolling_memory"]["pending"]) == 1
    assert chat["memory_layers"]["recent_facts"] == []
    assert chat["memory_layers"]["long_facts"] == []


def test_worker_turns_pending_clip_into_llm_memory_without_blocking_ingestion() -> None:
    memory = runtime.default_memory()
    settings = runtime._rolling_memory_settings(memory)
    settings["sample_rate"] = 1.0
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

    response = (
        '{"keep":true,"summary":"аня снова застряла из-за отменённого поезда",'
        '"keywords":["поезд","отмена"],"participants":["аня"]}'
    )
    with (
        patch.object(runtime, "load_memory", return_value=memory),
        patch.object(runtime, "save_memory", return_value=True),
        patch.object(runtime, "call_openai_metered", new=AsyncMock(return_value=(response, 120))),
    ):
        result = asyncio.run(runtime._process_rolling_memory_once(chat_id=777))

    state = chat["memory_layers"]["rolling_memory"]
    assert result == {"attempted": 1, "created": 1, "rejected": 0, "errors": 0}
    assert state["pending"] == []
    assert state["items"][0]["summary"].startswith("аня снова")
    assert state["daily_usage"]["tokens"] == 120


def test_worker_reloads_memory_before_saving_llm_result() -> None:
    initial = runtime.default_memory()
    settings = runtime._rolling_memory_settings(initial)
    chat = runtime.get_chat_mem(initial, 777)
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

    latest = copy.deepcopy(initial)
    runtime.get_chat_mem(latest, 777)["history"].append(
        {
            "message_id": 2,
            "name": "боря",
            "text": "это сообщение пришло пока модель отвечала",
            "ts": current.isoformat(),
            "is_bot": False,
        }
    )
    saved: list[dict] = []
    response = (
        '{"keep":true,"summary":"аня снова застряла из-за отменённого поезда",'
        '"keywords":["поезд","отмена"],"participants":["аня"]}'
    )
    with (
        patch.object(runtime, "load_memory", side_effect=[initial, latest]),
        patch.object(runtime, "save_memory", side_effect=lambda value: saved.append(copy.deepcopy(value)) or True),
        patch.object(runtime, "call_openai_metered", new=AsyncMock(return_value=(response, 120))),
    ):
        result = asyncio.run(runtime._process_rolling_memory_once(chat_id=777))

    saved_chat = runtime.get_chat_mem(saved[0], 777)
    assert result["created"] == 1
    assert [item["message_id"] for item in saved_chat["history"]] == [1, 2]
    assert saved_chat["memory_layers"]["rolling_memory"]["items"][0]["summary"].startswith("аня снова")


def test_free_chat_prompt_can_recall_rolling_memory_but_not_legacy_long_fact() -> None:
    memory = runtime.default_memory()
    settings = runtime._rolling_memory_settings(memory)
    settings["recall_rate"] = 1.0
    chat = runtime.get_chat_mem(memory, 777)
    state = ensure_state(chat, settings, now=NOW)
    state["items"] = [
        {
            "id": "train",
            "summary": "аня снова застряла из-за отменённого поезда",
            "keywords": ["поезд", "отмена"],
            "participants": ["аня"],
            "created_at": NOW.isoformat(),
            "expires_at": (datetime.now(timezone.utc) + timedelta(days=4)).isoformat(),
            "recall_count": 0,
        }
    ]
    chat["memory_layers"]["long_facts"] = [{"text": "старый секретный факт", "strength": 9.0}]
    message = _message(2, "почему поезд опять отменили")

    with (
        patch.object(runtime, "get_chat_features", return_value=dict(feature_gate.FREE_FEATURES)),
        patch("timur_bot.services.rolling_memory.random.random", return_value=0.0),
        patch("timur_bot.services.rolling_memory.random.choices", side_effect=lambda population, weights, k: [population[0]]),
    ):
        prompt = runtime.build_chat_messages(memory, message)[0]["content"]

    assert "живое воспоминание из этого чата" in prompt
    assert "аня снова застряла" in prompt
    assert "старый секретный факт" not in prompt
    assert state["items"][0]["recall_count"] == 1


def test_legacy_long_fact_is_selected_only_for_explicit_full_memory_request() -> None:
    memory = runtime.default_memory()
    chat = runtime.get_chat_mem(memory, 777)
    fact = {"text": "старый факт про вокзал", "strength": 5.0}
    chat["memory_layers"]["long_facts"] = [fact]
    usage = chat["memory_layers"].setdefault("long_fact_usage", {})
    full_features = {**feature_gate.FREE_FEATURES, "memory_depth": "full"}

    with patch.object(runtime, "get_chat_features", return_value=full_features):
        ordinary = runtime.build_chat_messages(memory, _message(2, "как дела"))[0]["content"]
    assert "старый факт про вокзал" not in ordinary
    assert usage == {}

    with (
        patch.object(runtime, "get_chat_features", return_value=full_features),
        patch.object(runtime.random, "choices", side_effect=lambda population, weights, k: [population[0]]),
    ):
        recalled = runtime.build_chat_messages(memory, _message(3, "вспомни что было"))[0]["content"]
    assert "старый факт про вокзал" in recalled
    assert next(iter(usage.values()))["count"] == 1


def test_rolling_memory_is_isolated_between_chats() -> None:
    memory = runtime.default_memory()
    memory["config"]["rolling_memory"]["sample_rate"] = 1.0
    with patch.object(runtime.billing, "register_activity"):
        runtime.update_memory_with_message(memory, _message(1, "у нас отменили поезд", chat_id=100))
        runtime.update_memory_with_message(memory, _message(1, "мы купили лодку", chat_id=200))

    first = runtime.get_chat_mem(memory, 100)["memory_layers"]["rolling_memory"]["pending"][0]
    second = runtime.get_chat_mem(memory, 200)["memory_layers"]["rolling_memory"]["pending"][0]
    assert "поезд" in first["context"][0]["text"]
    assert "лодку" in second["context"][0]["text"]
    assert first["id"] != second["id"]
