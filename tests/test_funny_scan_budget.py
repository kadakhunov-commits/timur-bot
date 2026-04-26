import asyncio
import os
from types import SimpleNamespace

os.environ.setdefault("TELEGRAM_BOT_TOKEN", "test-token")
os.environ.setdefault("OPENAI_API_KEY", "test-api-key")

from timur_bot.services import bot_logic as runtime
from timur_bot.services.funny_scan_storage import (
    add_candidate,
    default_funny_scan_state,
    ensure_budget_day,
    hard_budget_reached,
    has_candidate_signature,
    register_token_usage,
)


def test_hard_budget_stop_triggers() -> None:
    settings = {"daily_token_hard_stop": 1000, "daily_token_budget": 900}
    state = default_funny_scan_state()
    register_token_usage(state, 1200)

    assert hard_budget_reached(settings, state)


def test_adaptive_settings_tighten_after_80_percent_budget() -> None:
    settings = {
        "stage1_min_score": 42,
        "max_llm_candidates_per_scan": 12,
        "llm_max_context_messages": 12,
        "daily_token_budget": 1000,
        "daily_token_hard_stop": 2000,
    }
    state = default_funny_scan_state()
    ensure_budget_day(state)["tokens_used"] = 850

    adapted = runtime._adapt_funny_scan_settings(settings, state)

    assert adapted["stage1_min_score"] > settings["stage1_min_score"]
    assert adapted["max_llm_candidates_per_scan"] < settings["max_llm_candidates_per_scan"]
    assert adapted["llm_max_context_messages"] < settings["llm_max_context_messages"]


def test_scan_dedupes_before_llm_call(monkeypatch) -> None:
    memory = runtime.default_memory()
    settings = runtime._get_funny_scan_settings(memory)
    settings["sources"] = [{"chat_id": -1001, "title": "chat", "enabled": True}]
    memory["chats"]["-1001"] = {
        "history": [{"message_id": 42, "user_id": 1, "name": "u1", "text": "x", "ts": "2026-04-26T10:00:00"}]
    }
    state = default_funny_scan_state()
    existing = {
        "source_chat_id": -1001,
        "source_chat_title": "chat",
        "anchor_message_id": 42,
        "time_start": "2026-04-26T09:59:00",
        "time_end": "2026-04-26T10:01:00",
        "message_ids": [42],
        "pre_score": 50,
        "score": 80,
        "show_to_owner": False,
    }
    add_candidate(state, existing)

    stage1 = {
        "source_chat_id": -1001,
        "source_chat_title": "chat",
        "anchor_message_id": 42,
        "time_start": "2026-04-26T10:00:00",
        "time_end": "2026-04-26T10:00:00",
        "message_ids": [42],
        "cluster_messages": [],
        "pre_score": 55,
        "signals_pos": [],
        "signals_neg": [],
    }

    monkeypatch.setattr(runtime, "load_memory", lambda: memory)
    monkeypatch.setattr(runtime, "_load_funny_scan_state", lambda: state)
    monkeypatch.setattr(runtime, "_save_funny_scan_state", lambda _state: None)
    monkeypatch.setattr(runtime, "build_stage1_candidates", lambda *args, **kwargs: [stage1])

    def _llm_fail(*args, **kwargs):
        raise AssertionError("LLM should not be called for duplicate candidate")

    monkeypatch.setattr(runtime, "evaluate_candidate_with_llm", _llm_fail)

    app = SimpleNamespace(bot=SimpleNamespace())
    summary = asyncio.run(runtime._run_funny_scan_once(app, trigger="manual"))

    assert summary["deduped"] >= 1
    assert summary["llm_calls"] == 0


def test_candidate_signature_stays_stable_after_boundary_change() -> None:
    state = default_funny_scan_state()
    add_candidate(
        state,
        {
            "source_chat_id": -1001,
            "source_chat_title": "chat",
            "anchor_message_id": 42,
            "time_start": "2026-04-26T09:59:00",
            "time_end": "2026-04-26T10:01:00",
            "message_ids": [40, 41, 42, 43],
            "pre_score": 48,
            "score": 79,
            "show_to_owner": False,
        },
    )
    refined_draft = {
        "source_chat_id": -1001,
        "source_chat_title": "chat",
        "anchor_message_id": 42,
        "time_start": "2026-04-26T10:00:10",
        "time_end": "2026-04-26T10:00:20",
        "message_ids": [41, 42],
    }

    assert has_candidate_signature(state, refined_draft) is True


def test_scan_does_not_hold_state_lock_during_llm_call(monkeypatch) -> None:
    memory = runtime.default_memory()
    settings = runtime._get_funny_scan_settings(memory)
    settings["sources"] = [{"chat_id": -1001, "title": "chat", "enabled": True}]
    memory["chats"]["-1001"] = {
        "history": [{"message_id": 42, "user_id": 1, "name": "u1", "text": "x", "ts": "2026-04-26T10:00:00"}]
    }
    state = default_funny_scan_state()
    stage1 = {
        "source_chat_id": -1001,
        "source_chat_title": "chat",
        "anchor_message_id": 42,
        "time_start": "2026-04-26T10:00:00",
        "time_end": "2026-04-26T10:00:00",
        "message_ids": [42],
        "cluster_messages": [],
        "pre_score": 55,
        "signals_pos": [],
        "signals_neg": [],
    }
    lock_seen = {"during_llm": None}

    monkeypatch.setattr(runtime, "load_memory", lambda: memory)
    monkeypatch.setattr(runtime, "_load_funny_scan_state", lambda: state)
    monkeypatch.setattr(runtime, "_save_funny_scan_state", lambda _state: None)
    monkeypatch.setattr(runtime, "build_stage1_candidates", lambda *args, **kwargs: [stage1])

    def _llm_ok(*args, **kwargs):
        del args, kwargs
        lock_seen["during_llm"] = runtime._FUNNY_SCAN_STATE_LOCK.locked()
        return {
            "score": 80,
            "show_to_owner": False,
            "reason_short": "ok",
            "boundary": {},
            "positive_signals": [],
            "negative_signals": [],
        }, 123

    monkeypatch.setattr(runtime, "evaluate_candidate_with_llm", _llm_ok)

    app = SimpleNamespace(bot=SimpleNamespace())
    summary = asyncio.run(runtime._run_funny_scan_once(app, trigger="manual"))

    assert summary["llm_calls"] == 1
    assert lock_seen["during_llm"] is False
