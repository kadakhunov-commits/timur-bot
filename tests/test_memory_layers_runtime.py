import os

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
    assert lines[0] == "A: важный старый факт"


def test_old_memories_skip_blocked_repeated_meme() -> None:
    memory = runtime.default_memory()
    chat = runtime.get_chat_mem(memory, 1)
    chat["memory_layers"]["long_facts"] = [
        {"text": "митя снес сообщения кадыра", "strength": 99.0},
        {"text": "нейтральный старый факт", "strength": 5.0},
    ]

    lines = runtime.select_old_random_memories(memory, 1)

    assert "митя снес сообщения кадыра" not in lines
    assert lines[0] == "нейтральный старый факт"


def test_toxicity_fallback_uses_persona_default() -> None:
    memory = runtime.default_memory()
    memory["config"].pop("toxicity_level", None)

    heat = runtime.get_toxicity_level(memory)

    assert heat == runtime.APP_CONFIG.default_toxicity_level


def test_reply_guardrail_blocks_repeated_deleted_messages_meme() -> None:
    blocked = "митя снес сообщения кадыра и опять умничает"

    safe = runtime.enforce_reply_guardrails(blocked)

    assert safe == "этот старый мем уже помер давай свежак"
