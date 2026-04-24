import asyncio
import os
from datetime import datetime
from unittest.mock import patch

os.environ.setdefault("TELEGRAM_BOT_TOKEN", "test-token")
os.environ.setdefault("OPENAI_API_KEY", "test-api-key")

from timur_bot.services import bot_logic as runtime


def test_get_chat_mem_has_memory_layers() -> None:
    memory = runtime.default_memory()
    chat = runtime.get_chat_mem(memory, 1)
    layers = chat.get("memory_layers", {})
    assert isinstance(layers.get("recent_messages", []), list)
    assert isinstance(layers.get("recent_facts", []), list)
    assert isinstance(layers.get("long_facts", []), list)


def test_context_prefers_recent_messages_layer() -> None:
    memory = runtime.default_memory()
    chat = runtime.get_chat_mem(memory, 1)
    chat["history"] = [{"text": "старое", "name": "x", "user_id": 1, "ts": "2024-01-01T00:00:00"}]
    chat["memory_layers"]["recent_messages"] = [
        {"text": "новое", "name": "y", "user_id": 2, "ts": "2024-01-02T00:00:00"}
    ]

    selected = runtime.select_chat_history_for_context(memory, 1)
    assert len(selected) == 1
    assert selected[0]["text"] == "новое"


def test_old_memories_use_long_facts_not_log_random() -> None:
    memory = runtime.default_memory()
    chat = runtime.get_chat_mem(memory, 1)
    chat["log"] = [{"text": "шум"} for _ in range(50)]
    chat["memory_layers"]["long_facts"] = [
        {"text": "A: важный старый факт", "strength": 5.0},
        {"text": "B: менее важный", "strength": 1.0},
    ]

    lines = runtime.select_old_random_memories(memory, 1)
    assert lines
    assert lines[0] in {"A: важный старый факт", "B: менее важный"}


def test_old_memories_penalize_recently_overused_fact() -> None:
    memory = runtime.default_memory()
    chat = runtime.get_chat_mem(memory, 1)
    chat["memory_layers"]["long_facts"] = [
        {"text": "A: заезженный факт", "strength": 5.0},
        {"text": "B: свежий факт", "strength": 2.0},
    ]
    chat["memory_layers"]["long_fact_usage"] = {
        runtime.normalize_token("A: заезженный факт")[:120]: {
            "last_used_ts": datetime.utcnow().isoformat(),
            "count": 12,
        }
    }

    with patch.object(runtime.random, "choices", side_effect=lambda population, weights, k: [population[0]]):
        lines = runtime.select_old_random_memories(memory, 1)

    assert lines == ["B: свежий факт"]


def test_default_memory_contains_life_config() -> None:
    memory = runtime.default_memory()
    life = memory["config"]["life"]
    assert life["enabled"] is True
    assert life["daily_target"] == 3
    assert life["timezone"] == "Europe/Moscow"


def test_toxicity_fallback_uses_persona_default() -> None:
    memory = runtime.default_memory()
    memory["config"].pop("toxicity_level", None)

    heat = runtime.get_toxicity_level(memory)

    assert heat == runtime.APP_CONFIG.default_toxicity_level


def test_reply_guardrail_softens_toxic_personal_attack() -> None:
    toxic = "ты дебил если честно"

    safe = runtime.enforce_reply_guardrails(toxic)

    assert safe == "ок без наездов давай по сути"


def test_effective_toxicity_caps_default_and_chill_modes() -> None:
    memory = runtime.default_memory()
    memory["config"]["toxicity_level"] = 90

    memory["config"]["active_mode"] = "default"
    assert runtime.get_effective_toxicity_level(memory) == 20

    memory["config"]["active_mode"] = "chill"
    assert runtime.get_effective_toxicity_level(memory) == 8


def test_looks_like_memory_request_detection() -> None:
    assert runtime.looks_like_memory_request("тимур высри чето из памяти")
    assert runtime.looks_like_memory_request("вспомни старый прикол")
    assert not runtime.looks_like_memory_request("просто ответь по теме")


def test_story_request_detection() -> None:
    assert runtime._looks_like_story_request("тимур расскажи историю")
    assert runtime._looks_like_story_request("расскажи че было")
    assert not runtime._looks_like_story_request("давай по делу")


def test_generate_daily_slots_uses_daily_target_outside_quiet_hours() -> None:
    life = runtime._default_life_config()
    life["daily_target"] = 3
    slots = runtime._generate_daily_slots(life, day_seed=20260424)
    assert len(slots) == 3
    quiet_start = runtime._parse_hhmm_to_minute("00:00", 0)
    quiet_end = runtime._parse_hhmm_to_minute("10:00", 600)
    assert all(not runtime._is_quiet_minute(s, quiet_start, quiet_end) for s in slots)


def test_processed_event_cache_marks_duplicate() -> None:
    memory = runtime.default_memory()
    chat = runtime.get_chat_mem(memory, 1)
    key = runtime._make_event_key("text", 1, 100)
    assert not runtime._is_processed_event(chat, key)
    runtime._mark_processed_event(chat, key)
    assert runtime._is_processed_event(chat, key)


def test_run_with_typing_sends_chat_action() -> None:
    class DummyBot:
        def __init__(self) -> None:
            self.calls = 0

        async def send_chat_action(self, chat_id: int, action: str) -> None:
            del chat_id, action
            self.calls += 1

    class DummyContext:
        def __init__(self) -> None:
            self.bot = DummyBot()

    async def _task() -> str:
        await asyncio.sleep(0.01)
        return "ok"

    context = DummyContext()
    result = asyncio.run(runtime._run_with_typing(context, 1, _task()))
    assert result == "ok"
    assert context.bot.calls >= 1
