import asyncio
import os
import weakref

import pytest

os.environ.setdefault("TELEGRAM_BOT_TOKEN", "test-token")
os.environ.setdefault("OPENAI_API_KEY", "test-api-key")

from timur_bot.services import bot_logic as runtime
from timur_bot.services.funny_scan_storage import add_candidate, default_funny_scan_state, get_candidate


class OkBot:
    def __init__(self) -> None:
        self.calls = 0

    async def forward_messages(self, **kwargs):
        del kwargs
        self.calls += 1
        return ()


class FailingBot:
    async def forward_messages(self, **kwargs):
        del kwargs
        raise RuntimeError("blocked")


class SlowBot:
    def __init__(self) -> None:
        self.calls = 0

    async def forward_messages(self, **kwargs):
        del kwargs
        self.calls += 1
        await asyncio.sleep(0.03)
        return ()


class SignalingSlowBot:
    def __init__(self) -> None:
        self.calls = 0
        self.started = asyncio.Event()

    async def forward_messages(self, **kwargs):
        del kwargs
        self.calls += 1
        self.started.set()
        await asyncio.sleep(0.03)
        return ()


def _candidate_payload() -> dict:
    return {
        "source_chat_id": -1001,
        "source_chat_title": "chat",
        "message_ids": [11, 12],
        "anchor_message_id": 12,
        "time_start": "2026-04-25T10:00:00",
        "time_end": "2026-04-25T10:00:05",
        "pre_score": 55.0,
        "score": 80,
        "show_to_owner": True,
        "cluster_messages": [],
    }


def test_forward_candidate_success_marks_sent(monkeypatch: pytest.MonkeyPatch) -> None:
    state = default_funny_scan_state()
    candidate_id, _ = add_candidate(state, _candidate_payload())
    settings = {"owner_dm_chat_id": runtime.OWNER_ID, "daily_forward_limit": 20}
    bot = OkBot()
    monkeypatch.setattr(runtime, "_load_funny_scan_state", lambda: state)
    monkeypatch.setattr(runtime, "_save_funny_scan_state", lambda _state: None)

    ok, message = asyncio.run(
        runtime._forward_funny_candidate(bot=bot, settings=settings, candidate_id=candidate_id)
    )

    assert ok is True
    assert "выполнен" in message
    assert bot.calls == 1
    assert get_candidate(state, candidate_id)["status"] == "sent"
    assert state["budget"]["forwards_sent"] == 1
    assert state["budget"]["pending_forwards"] == 0


def test_forward_candidate_failure_keeps_approved_with_error(monkeypatch: pytest.MonkeyPatch) -> None:
    state = default_funny_scan_state()
    candidate_id, _ = add_candidate(state, _candidate_payload())
    settings = {"owner_dm_chat_id": runtime.OWNER_ID, "daily_forward_limit": 20}
    monkeypatch.setattr(runtime, "_load_funny_scan_state", lambda: state)
    monkeypatch.setattr(runtime, "_save_funny_scan_state", lambda _state: None)

    ok, message = asyncio.run(
        runtime._forward_funny_candidate(bot=FailingBot(), settings=settings, candidate_id=candidate_id)
    )

    assert ok is False
    assert "failed" in message
    candidate = get_candidate(state, candidate_id)
    assert candidate["status"] == "approved"
    assert candidate["forward_error"]
    assert state["budget"]["pending_forwards"] == 0


def test_forward_candidate_skips_when_already_sent(monkeypatch: pytest.MonkeyPatch) -> None:
    state = default_funny_scan_state()
    candidate_id, _ = add_candidate(state, _candidate_payload())
    candidate = get_candidate(state, candidate_id)
    assert candidate is not None
    candidate["status"] = "sent"
    settings = {"owner_dm_chat_id": runtime.OWNER_ID, "daily_forward_limit": 20}
    monkeypatch.setattr(runtime, "_load_funny_scan_state", lambda: state)
    monkeypatch.setattr(runtime, "_save_funny_scan_state", lambda _state: None)

    ok, message = asyncio.run(
        runtime._forward_funny_candidate(bot=OkBot(), settings=settings, candidate_id=candidate_id)
    )

    assert ok is False
    assert "уже отправлен" in message


def test_forward_candidate_is_serialized_for_same_candidate(monkeypatch: pytest.MonkeyPatch) -> None:
    state = default_funny_scan_state()
    candidate_id, _ = add_candidate(state, _candidate_payload())
    settings = {"owner_dm_chat_id": runtime.OWNER_ID, "daily_forward_limit": 20}
    bot = SlowBot()
    monkeypatch.setattr(runtime, "_load_funny_scan_state", lambda: state)
    monkeypatch.setattr(runtime, "_save_funny_scan_state", lambda _state: None)

    async def _run_pair():
        return await asyncio.gather(
            runtime._forward_funny_candidate(bot=bot, settings=settings, candidate_id=candidate_id),
            runtime._forward_funny_candidate(bot=bot, settings=settings, candidate_id=candidate_id),
        )

    results = asyncio.run(_run_pair())

    assert bot.calls == 1
    assert sum(1 for ok, _ in results if ok) == 1
    assert sum(1 for ok, msg in results if (not ok) and ("уже отправлен" in msg)) == 1
    assert get_candidate(state, candidate_id)["status"] == "sent"
    assert state["budget"]["forwards_sent"] == 1
    assert state["budget"]["pending_forwards"] == 0


def test_forward_quota_reservation_blocks_parallel_other_candidates(monkeypatch: pytest.MonkeyPatch) -> None:
    state = default_funny_scan_state()
    c1, _ = add_candidate(state, _candidate_payload())
    payload2 = dict(_candidate_payload())
    payload2["anchor_message_id"] = 99
    payload2["message_ids"] = [98, 99]
    c2, _ = add_candidate(state, payload2)
    settings = {"owner_dm_chat_id": runtime.OWNER_ID, "daily_forward_limit": 1}
    bot = SlowBot()
    monkeypatch.setattr(runtime, "_load_funny_scan_state", lambda: state)
    monkeypatch.setattr(runtime, "_save_funny_scan_state", lambda _state: None)

    async def _run_pair():
        return await asyncio.gather(
            runtime._forward_funny_candidate(bot=bot, settings=settings, candidate_id=c1),
            runtime._forward_funny_candidate(bot=bot, settings=settings, candidate_id=c2),
        )

    results = asyncio.run(_run_pair())

    assert bot.calls == 1
    assert sum(1 for ok, _ in results if ok) == 1
    assert sum(1 for ok, msg in results if (not ok) and ("достигнут дневной лимит" in msg)) == 1
    assert state["budget"]["forwards_sent"] == 1
    assert state["budget"]["pending_forwards"] == 0


def test_reject_waits_for_inflight_approve_and_does_not_override_sent(monkeypatch: pytest.MonkeyPatch) -> None:
    state = default_funny_scan_state()
    candidate_id, _ = add_candidate(state, _candidate_payload())
    settings = {"owner_dm_chat_id": runtime.OWNER_ID, "daily_forward_limit": 20}
    bot = SignalingSlowBot()
    monkeypatch.setattr(runtime, "_load_funny_scan_state", lambda: state)
    monkeypatch.setattr(runtime, "_save_funny_scan_state", lambda _state: None)

    async def _run_race():
        approve_task = asyncio.create_task(
            runtime._forward_funny_candidate(bot=bot, settings=settings, candidate_id=candidate_id)
        )
        await bot.started.wait()
        reject_result = await runtime._reject_funny_candidate(candidate_id)
        approve_result = await approve_task
        return approve_result, reject_result

    approve_result, reject_result = asyncio.run(_run_race())

    assert approve_result[0] is True
    assert reject_result[0] is False
    assert "уже отправлен" in reject_result[1]
    assert get_candidate(state, candidate_id)["status"] == "sent"


def test_forward_locks_use_weak_storage() -> None:
    assert isinstance(runtime._FUNNY_FORWARD_LOCKS, weakref.WeakValueDictionary)
