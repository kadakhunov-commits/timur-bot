import os
from urllib.parse import parse_qs, urlsplit

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


def test_build_tts_input_keeps_only_bracket_directives() -> None:
    text = runtime.build_tts_input(
        "брат ща скину голосовое",
        "[slightly raspy] [casual, confident] говори как живой собеседник",
    )

    assert text == "[slightly raspy] [casual, confident]\nбрат ща скину голосовое"


def test_build_miniapp_launch_url_contains_version_and_fact_memory(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(runtime, "MINIAPP_URL", "https://example.com/miniapp")
    monkeypatch.setattr(runtime, "get_build_version", lambda: "abc1234")
    memory = runtime.default_memory()
    chat_mem = runtime.get_chat_mem(memory, 123)
    runtime.upsert_claim_facts(chat_mem, runtime.extract_claim_facts(chat_mem, "где ты родился", "родился в казани"))
    mood = memory["config"]["mood"]
    mood["event_history"] = [
        {
            "id": 11,
            "key": "transport_fail",
            "created_ts": "2026-05-28T10:00:00",
            "privacy_level": 1,
            "seriousness": 1,
            "absurdity": 2,
            "public_text": "сегодня транспорт устроил мне стендап",
        }
    ]
    mood["current_event"] = mood["event_history"][0]

    url = runtime.build_miniapp_launch_url(memory, 123)

    assert "state=" in url
    encoded = parse_qs(urlsplit(url).query)["state"][0]
    payload = runtime.json.loads(runtime.base64.urlsafe_b64decode(encoded + "=" * ((4 - len(encoded) % 4) % 4)).decode("utf-8"))
    assert payload["meta"]["version"] == "abc1234"
    assert payload["memory"]["members"][0]["id"] == "bot:self"
    assert payload["memory"]["members"][0]["facts"]
    assert "mood" in payload["settings"]
    assert "openness" in payload["settings"]["mood"]
    assert payload["settings"]["mood"]["recentEvents"]
    assert payload["settings"]["mood"]["recentEvents"][0]["key"] == "transport_fail"
