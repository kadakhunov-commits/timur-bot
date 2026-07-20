from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from timur_bot.services.rolling_memory import (
    build_summary_messages,
    complete_candidate,
    enqueue_from_history,
    ensure_state,
    fail_candidate,
    format_recall_prompt,
    next_pending,
    normalize_settings,
    parse_summary,
    select_recall,
    should_sample,
    status_snapshot,
)


NOW = datetime(2026, 7, 20, 12, 0, tzinfo=timezone.utc)


def _settings(**overrides):
    return normalize_settings({**overrides})


def _chat(message_id: int = 10):
    return {
        "history": [
            {"message_id": message_id - 1, "name": "аня", "text": "поезд опять отменили", "ts": NOW.isoformat()},
            {"message_id": message_id, "name": "боря", "text": "зато вокзал уже как родной", "ts": NOW.isoformat()},
        ],
        "memory_layers": {},
    }


def test_sampling_is_stable_and_approximately_one_in_ten() -> None:
    first = [message_id for message_id in range(1, 1001) if should_sample(777, message_id, 0.10)]
    second = [message_id for message_id in range(1, 1001) if should_sample(777, message_id, 0.10)]

    assert first == second
    assert 75 <= len(first) <= 125


def test_enqueue_builds_bounded_context_and_filters_noise() -> None:
    settings = _settings(sample_rate=1.0, context_messages=2)
    chat = _chat()
    anchor = chat["history"][-1]

    assert enqueue_from_history(chat, chat_id=777, anchor=anchor, settings=settings, now=NOW)
    pending = chat["memory_layers"]["rolling_memory"]["pending"]
    assert pending[0]["source_message_ids"] == [9, 10]
    assert len(pending[0]["context"]) == 2

    noise = {"message_id": 11, "name": "боря", "text": "лол", "ts": NOW.isoformat()}
    chat["history"].append(noise)
    assert not enqueue_from_history(chat, chat_id=777, anchor=noise, settings=settings, now=NOW, force=True)


def test_summary_parser_is_strict_and_bounded() -> None:
    settings = _settings(summary_max_chars=40)
    assert parse_summary("not json", settings) is None
    assert parse_summary('{"keep":"yes"}', settings) is None
    assert parse_summary('{"keep":false}', settings) == {
        "keep": False,
        "summary": "",
        "keywords": [],
        "participants": [],
    }
    parsed = parse_summary(
        '{"keep":true,"summary":"очень длинное воспоминание о том как отменили поезд и вокзал стал родным",'
        '"keywords":["поезд","вокзал"],"participants":["аня","боря"]}',
        settings,
    )
    assert parsed and len(parsed["summary"]) <= 40
    assert parsed["keywords"] == ["поезд", "вокзал"]


def test_complete_candidate_expires_after_four_days_and_evicts_oldest() -> None:
    settings = _settings(ttl_days=4, max_items_per_chat=2, sample_rate=1.0)
    chat = _chat()
    state = ensure_state(chat, settings, now=NOW)

    for index in range(3):
        candidate = {
            "id": f"c{index}",
            "anchor_ts": (NOW + timedelta(minutes=index)).isoformat(),
            "source_message_ids": [index + 1],
            "context": [{"name": "аня", "text": f"фрагмент {index}"}],
        }
        state["pending"].append(candidate)
        item = complete_candidate(
            state,
            candidate,
            {"keep": True, "summary": f"память {index}", "keywords": ["память"], "participants": ["аня"]},
            token_usage=10,
            settings=settings,
            now=NOW + timedelta(minutes=index),
        )
        assert item

    assert [item["id"] for item in state["items"]] == ["c1", "c2"]
    snapshot = status_snapshot(state, settings, now=NOW + timedelta(days=4, minutes=3))
    assert snapshot["active"] == 0


def test_recall_requires_relevance_and_marks_only_selected_item() -> None:
    settings = _settings(recall_rate=1.0)
    chat = _chat()
    state = ensure_state(chat, settings, now=NOW)
    state["items"] = [
        {
            "id": "train",
            "summary": "аня и боря застряли из-за отменённого поезда",
            "keywords": ["поезд", "вокзал"],
            "participants": ["аня", "боря"],
            "created_at": NOW.isoformat(),
            "expires_at": (NOW + timedelta(days=4)).isoformat(),
            "recall_count": 0,
        }
    ]
    rng = SimpleNamespace(random=lambda: 0.0, choices=lambda population, weights, k: [population[0]])

    assert select_recall(state, "погода сегодня", settings, now=NOW, rng=rng) is None
    selected = select_recall(state, "опять ехали поездом и задержались", settings, now=NOW, rng=rng)
    assert selected and selected["id"] == "train"
    assert selected["recall_count"] == 1
    assert "естественно" in format_recall_prompt(selected)


def test_budget_and_retry_schedule_are_bounded() -> None:
    settings = _settings(max_summaries_per_chat_per_day=1, daily_token_budget_per_chat=500)
    chat = _chat()
    state = ensure_state(chat, settings, now=NOW)
    candidate = {"id": "x", "anchor_ts": NOW.isoformat(), "next_attempt_at": NOW.isoformat(), "attempts": 0}
    state["pending"] = [candidate]
    assert next_pending(state, settings, now=NOW) is candidate

    assert fail_candidate(state, candidate, "boom", now=NOW)
    assert candidate["next_attempt_at"] == (NOW + timedelta(minutes=5)).isoformat()
    assert fail_candidate(state, candidate, "boom", now=NOW)
    assert fail_candidate(state, candidate, "boom", now=NOW)
    assert not fail_candidate(state, candidate, "boom", now=NOW)
    assert state["pending"] == []


def test_summary_prompt_contains_clip_and_json_contract() -> None:
    settings = _settings()
    candidate = {
        "context": [{"message_id": 1, "name": "аня", "text": "поезд отменили", "is_bot": False}]
    }
    messages = build_summary_messages(candidate, settings)
    assert '"keep":true' in messages[0]["content"]
    assert "поезд отменили" in messages[1]["content"]
