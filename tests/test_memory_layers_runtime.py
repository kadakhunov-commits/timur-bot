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
