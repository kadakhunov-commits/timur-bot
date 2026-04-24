from timur_bot.tools.export_funny_candidates import build_candidates, is_laugh_response, score_candidate
from timur_bot.tools.import_telegram_html import MessageRecord


def _msg(i: int, author: str, text: str, ts: str | None = None) -> MessageRecord:
    return MessageRecord(message_id=i, from_name=author, text=text, ts=ts or f"2024-01-01T00:00:{i:02d}")


def test_score_candidate_prefers_laugh_after() -> None:
    messages = [
        _msg(1, "A", "ну че"),
        _msg(2, "B", "бля гениальный план"),
        _msg(3, "A", "ахаха"),
    ]

    score, signals = score_candidate(messages, 1)

    assert score >= 4
    assert "laugh_response_after" in signals
    assert "immediate_laugh_after" in signals


def test_laugh_response_is_signal_not_candidate() -> None:
    messages = [
        _msg(1, "A", "контекст"),
        _msg(2, "B", "лол"),
        _msg(3, "A", "ахаха"),
    ]

    score, signals = score_candidate(messages, 1)

    assert is_laugh_response("лол")
    assert score == 0
    assert signals == ["pure_laugh_response"]


def test_build_candidates_includes_context() -> None:
    messages = [
        _msg(1, "A", "контекст"),
        _msg(2, "B", "гениальный план конечно"),
        _msg(3, "A", "ахаха"),
    ]

    candidates = build_candidates(messages, context_size=1, limit=10, seed=1)

    assert candidates
    assert candidates[0]["context"]
    assert "good_reply" in candidates[0]
    assert candidates[0]["good_reply"] == "гениальный план конечно"
    assert candidates[0]["laugh_responses"][0]["text"] == "ахаха"


def test_build_candidates_prefers_newer_when_laughter_is_similar() -> None:
    messages = [
        _msg(1, "A", "старый заход", "2024-01-01T00:00:01"),
        _msg(2, "B", "лол", "2024-01-01T00:00:02"),
        _msg(3, "A", "новый заход", "2024-06-01T00:00:01"),
        _msg(4, "B", "лол", "2024-06-01T00:00:02"),
    ]

    candidates = build_candidates(messages, context_size=1, limit=10, seed=1)

    assert candidates[0]["good_reply"] == "новый заход"
    assert "very_recent" in candidates[0]["signals"]
