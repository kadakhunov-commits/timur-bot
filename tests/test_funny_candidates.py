from timur_bot.tools.export_funny_candidates import build_candidates, score_candidate
from timur_bot.tools.import_telegram_html import MessageRecord


def _msg(i: int, author: str, text: str) -> MessageRecord:
    return MessageRecord(message_id=i, from_name=author, text=text, ts=f"2024-01-01T00:00:{i:02d}")


def test_score_candidate_prefers_laugh_after() -> None:
    messages = [
        _msg(1, "A", "ну че"),
        _msg(2, "B", "бля гениальный план"),
        _msg(3, "A", "ахаха"),
    ]

    score, signals = score_candidate(messages, 1)

    assert score >= 4
    assert "nearby_laugh_after" in signals


def test_build_candidates_includes_context() -> None:
    messages = [
        _msg(1, "A", "контекст"),
        _msg(2, "B", "лол"),
        _msg(3, "A", "ахаха"),
    ]

    candidates = build_candidates(messages, context_size=1, limit=10, seed=1)

    assert candidates
    assert candidates[0]["context"]
    assert "good_reply" in candidates[0]
