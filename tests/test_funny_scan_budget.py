import asyncio
import os
from datetime import datetime
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
    now_ts = datetime.utcnow().isoformat()
    memory = runtime.default_memory()
    settings = runtime._get_funny_scan_settings(memory)
    settings["sources"] = [{"chat_id": -1001, "title": "chat", "enabled": True}]
    memory["chats"]["-1001"] = {
        "history": [{"message_id": 42, "user_id": 1, "name": "u1", "text": "x", "ts": now_ts}]
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
        "time_start": now_ts,
        "time_end": now_ts,
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
    now_ts = datetime.utcnow().isoformat()
    memory = runtime.default_memory()
    settings = runtime._get_funny_scan_settings(memory)
    settings["sources"] = [{"chat_id": -1001, "title": "chat", "enabled": True}]
    memory["chats"]["-1001"] = {
        "history": [{"message_id": 42, "user_id": 1, "name": "u1", "text": "x", "ts": now_ts}]
    }
    state = default_funny_scan_state()
    stage1 = {
        "source_chat_id": -1001,
        "source_chat_title": "chat",
        "anchor_message_id": 42,
        "time_start": now_ts,
        "time_end": now_ts,
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


def test_scan_uses_main_chat_and_auto_forwards(monkeypatch) -> None:
    now_ts = datetime.utcnow().isoformat()
    memory = runtime.default_memory()
    settings = runtime._get_funny_scan_settings(memory)
    settings["main_chat_id"] = -5001
    settings["owner_delivery_mode"] = "auto_forward"
    settings["review_threshold"] = 70
    memory["chats"]["-5001"] = {
        "history": [{"message_id": 42, "user_id": 1, "name": "u1", "text": "x", "ts": now_ts}]
    }
    memory["chats"]["-7001"] = {
        "history": [{"message_id": 99, "user_id": 2, "name": "u2", "text": "y", "ts": now_ts}]
    }
    state = default_funny_scan_state()
    stage1 = {
        "source_chat_id": -5001,
        "source_chat_title": "chat",
        "anchor_message_id": 42,
        "time_start": now_ts,
        "time_end": now_ts,
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
    monkeypatch.setattr(
        runtime,
        "evaluate_candidate_with_llm",
        lambda *args, **kwargs: (
            {
                "score": 88,
                "show_to_owner": True,
                "reason_short": "ok",
                "boundary": {},
                "positive_signals": [],
                "negative_signals": [],
            },
            111,
        ),
    )

    calls = {"count": 0}

    async def _forward(*, bot, settings, candidate_id, action="approve"):
        del bot, settings, candidate_id, action
        calls["count"] += 1
        return True, "ok"

    monkeypatch.setattr(runtime, "_forward_funny_candidate", _forward)

    app = SimpleNamespace(bot=SimpleNamespace())
    summary = asyncio.run(runtime._run_funny_scan_once(app, trigger="manual"))

    assert summary["sources"] == 1
    assert summary["forwarded"] == 1
    assert calls["count"] == 1


def test_manual_scan_expands_period_to_backfill_start(monkeypatch) -> None:
    now_ts = datetime.utcnow().isoformat()
    memory = runtime.default_memory()
    settings = runtime._get_funny_scan_settings(memory)
    settings["main_chat_id"] = -5001
    settings["backfill_start_date_msk"] = "2024-01-01"
    settings["owner_delivery_mode"] = "preview"
    memory["chats"]["-5001"] = {
        "history": [{"message_id": 42, "user_id": 1, "name": "u1", "text": "x", "ts": now_ts}]
    }
    state = default_funny_scan_state()
    seen = {"hours": 0}
    stage1 = {
        "source_chat_id": -5001,
        "source_chat_title": "chat",
        "anchor_message_id": 42,
        "time_start": now_ts,
        "time_end": now_ts,
        "message_ids": [42],
        "cluster_messages": [],
        "pre_score": 55,
        "signals_pos": [],
        "signals_neg": [],
    }

    monkeypatch.setattr(runtime, "load_memory", lambda: memory)
    monkeypatch.setattr(runtime, "_load_funny_scan_state", lambda: state)
    monkeypatch.setattr(runtime, "_save_funny_scan_state", lambda _state: None)

    def _extract(messages, *, period_hours, backfill_start_date_msk="", now=None):
        del backfill_start_date_msk, now
        seen["hours"] = period_hours
        return list(messages)

    monkeypatch.setattr(runtime, "extract_period_messages", _extract)
    monkeypatch.setattr(runtime, "build_stage1_candidates", lambda *args, **kwargs: [stage1])
    monkeypatch.setattr(
        runtime,
        "evaluate_candidate_with_llm",
        lambda *args, **kwargs: (
            {
                "score": 88,
                "show_to_owner": True,
                "reason_short": "ok",
                "boundary": {},
                "positive_signals": [],
                "negative_signals": [],
            },
            111,
        ),
    )
    monkeypatch.setattr(runtime, "_send_funny_candidate_preview", lambda *args, **kwargs: asyncio.sleep(0, result=True))

    app = SimpleNamespace(bot=SimpleNamespace())
    asyncio.run(runtime._run_funny_scan_once(app, trigger="manual"))

    assert seen["hours"] > 24
