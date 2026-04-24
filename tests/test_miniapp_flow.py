import os

os.environ.setdefault("TELEGRAM_BOT_TOKEN", "test-token")
os.environ.setdefault("OPENAI_API_KEY", "test-api-key")

import pytest

from timur_bot.services import bot_logic as runtime


def test_apply_miniapp_admin_config_updates_memory(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(runtime, "save_memory", lambda memory: None)
    memory = runtime.default_memory()

    text = runtime.apply_miniapp_admin_config(
        memory,
        chat_id=123,
        payload={"chat_id": 123, "active_mode": "chill", "heat": 27},
    )

    assert "режим: chill" in text
    assert runtime.get_active_mode(memory) == "chill"
    assert runtime.get_toxicity_level(memory) == 27


def test_apply_miniapp_admin_config_rejects_unknown_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(runtime, "save_memory", lambda memory: None)
    memory = runtime.default_memory()

    try:
        runtime.apply_miniapp_admin_config(
            memory,
            chat_id=123,
            payload={"chat_id": 123, "active_mode": "nope"},
        )
    except ValueError as e:
        assert "unknown mode" in str(e)
    else:
        raise AssertionError("expected ValueError")
