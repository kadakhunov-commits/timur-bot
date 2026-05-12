from datetime import datetime

from timur_bot.services.funny_scan_pipeline import build_learning_profile, build_stage1_candidates, extract_period_messages


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
        "rule_min_hearts": 0,
        "rule_min_laugh_markers": 0,
    }


def _lexicon() -> dict:
    return {
        "laugh_markers": ["лол", "ахах", "ахаха", "хаха", "вынесло"],
        "extra_laugh_markers": ["сука", "мука", "бля", "лол", "лоо"],
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
        settings={**_settings(stage1_min=35), "rule_min_hearts": 1},
        lexicon=_lexicon(),
    )

    assert all(item["anchor_message_id"] != 2 for item in candidates)


def test_extract_period_messages_respects_period_without_fallback() -> None:
    messages = [_msg(1, 1, "старое", "2020-01-01T00:00:00")]

    filtered = extract_period_messages(messages, period_hours=24, now=datetime(2026, 4, 26, 12, 0, 0))

    assert filtered == []


def test_extract_period_messages_applies_backfill_start_date() -> None:
    messages = [
        _msg(1, 1, "раньше", "2026-04-24T20:59:59"),
        _msg(2, 1, "после", "2026-04-24T21:00:01"),
    ]
    filtered = extract_period_messages(
        messages,
        period_hours=24 * 30,
        backfill_start_date_msk="2026-04-25",
        now=datetime(2026, 4, 26, 12, 0, 0),
    )
    assert [item["message_id"] for item in filtered] == [2]


def test_stage1_enforces_hearts_and_laugh_rules() -> None:
    messages = [
        _msg(1, 1, "контекст", "2026-04-25T10:00:00"),
        _msg(2, 2, "бля это лооол", "2026-04-25T10:00:08"),
        _msg(3, 3, "лол", "2026-04-25T10:00:09"),
    ]
    candidates = build_stage1_candidates(
        messages,
        source_chat_id=-1001,
        source_chat_title="chat",
        reaction_index={"-1001:2": {"total": 5, "heart": 3, "laugh": 1}},
        settings={
            "scan_period_hours": 24,
            "stage1_min_score": 15,
            "max_candidates_per_scan": 20,
            "rule_min_hearts": 3,
            "rule_min_laugh_markers": 2,
        },
        lexicon=_lexicon(),
    )
    assert candidates
    assert candidates[0]["anchor_message_id"] == 2


def test_stage1_backfill_without_reactions_skips_hearts_gate() -> None:
    messages = [
        _msg(1, 1, "контекст", "2026-04-25T10:00:00"),
        _msg(2, 2, "бля это лооол", "2026-04-25T10:00:08"),
        _msg(3, 3, "лол", "2026-04-25T10:00:09"),
    ]
    candidates = build_stage1_candidates(
        messages,
        source_chat_id=-1001,
        source_chat_title="chat",
        reaction_index={},
        settings={
            "scan_period_hours": 24,
            "stage1_min_score": 15,
            "max_candidates_per_scan": 20,
            "rule_min_hearts": 3,
            "rule_min_laugh_markers": 2,
        },
        lexicon=_lexicon(),
    )
    assert candidates
    assert "rule_hearts_skipped_no_reactions" in candidates[0]["signals_pos"]


def test_build_learning_profile_produces_examples() -> None:
    messages = [
        _msg(10, 1, "ну это просто сука огонь", "2026-04-25T10:00:00"),
        _msg(11, 2, "ахаха вынесло", "2026-04-25T10:00:10"),
        _msg(12, 3, "лол", "2026-04-25T10:00:20"),
    ]
    profile = build_learning_profile(
        messages,
        source_chat_id=-2002,
        source_chat_title="gluboko",
        lexicon=_lexicon(),
        max_examples=4,
    )
    assert profile["source_stats"]["source_chat_id"] == -2002
    assert profile["source_stats"]["examples_total"] >= 1
    assert profile["examples"]
