from datetime import datetime

from timur_bot.services.funny_scan_pipeline import build_stage1_candidates, extract_period_messages


def _msg(message_id: int, user_id: int, text: str, ts: str) -> dict:
    return {
        "message_id": message_id,
        "user_id": user_id,
        "name": f"u{user_id}",
        "text": text,
        "ts": ts,
    }


def _settings(stage1_min: int = 35) -> dict:
    return {
        "scan_period_hours": 24,
        "stage1_min_score": stage1_min,
        "max_candidates_per_scan": 20,
    }


def _lexicon() -> dict:
    return {
        "laugh_markers": ["лол", "ахах", "ахаха", "хаха", "вынесло"],
        "habitual_laugh_markers": ["лол", "ахах", "ахаха"],
        "sarcasm_markers": ["ага", "ну да", "конечно"],
        "toxicity_markers": ["сук", "бля", "хуй", "пизд", "еб"],
        "reaction_weights": {"total": 0.35, "heart": 1.4, "laugh": 1.2},
        "pure_laugh_pattern": r"^(?:[!?.\s,;:()\-]*)(?:л+о+л+|а?ха(?:ха)+)(?:[!?.\s,;:()\-]*)$",
    }


def test_stage1_detects_laugh_cluster_with_reactions() -> None:
    messages = [
        _msg(1, 1, "контекст", "2026-04-25T10:00:00"),
        _msg(2, 2, "бля это был гениальный план", "2026-04-25T10:00:08"),
        _msg(3, 3, "ахаха вынесло", "2026-04-25T10:00:15"),
        _msg(4, 4, "лол", "2026-04-25T10:00:19"),
    ]
    reaction_index = {"-1001:2": {"total": 6, "heart": 2, "laugh": 1}}

    candidates = build_stage1_candidates(
        messages,
        source_chat_id=-1001,
        source_chat_title="chat",
        reaction_index=reaction_index,
        settings=_settings(stage1_min=20),
        lexicon=_lexicon(),
    )

    assert candidates
    top = candidates[0]
    assert top["anchor_message_id"] == 2
    assert "laugh_after" in top["signals_pos"]
    assert "heart_reactions" in top["signals_pos"]
    assert len(top["message_ids"]) >= 2


def test_stage1_skips_pure_laugh_message_as_anchor() -> None:
    messages = [
        _msg(1, 1, "контекст", "2026-04-25T10:00:00"),
        _msg(2, 2, "лол", "2026-04-25T10:00:10"),
        _msg(3, 3, "ахаха", "2026-04-25T10:00:20"),
    ]

    candidates = build_stage1_candidates(
        messages,
        source_chat_id=-1001,
        source_chat_title="chat",
        reaction_index={},
        settings=_settings(stage1_min=20),
        lexicon=_lexicon(),
    )

    assert all(item["anchor_message_id"] != 2 for item in candidates)


def test_stage1_penalizes_toxic_without_laugh_response() -> None:
    messages = [
        _msg(1, 1, "контекст", "2026-04-25T10:00:00"),
        _msg(2, 2, "ты сук бля", "2026-04-25T10:00:10"),
        _msg(3, 3, "ок", "2026-04-25T10:05:10"),
    ]

    candidates = build_stage1_candidates(
        messages,
        source_chat_id=-1001,
        source_chat_title="chat",
        reaction_index={},
        settings=_settings(stage1_min=35),
        lexicon=_lexicon(),
    )

    assert candidates == []


def test_extract_period_messages_respects_period_without_fallback() -> None:
    messages = [_msg(1, 1, "старое", "2020-01-01T00:00:00")]

    filtered = extract_period_messages(messages, period_hours=24, now=datetime(2026, 4, 26, 12, 0, 0))

    assert filtered == []
