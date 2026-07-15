from datetime import datetime, timedelta

from timur_bot.services.conversation_policy import (
    activate_dialogue,
    continue_dialogue,
    interjection_check_allowed,
    mark_interjection_checked,
    mark_reply_sent,
    mark_snipe_sent,
    note_human_message,
    ordinary_reply_allowed,
    snipe_allowed,
)


def test_regular_participation_has_message_gap_and_is_reset_by_reply() -> None:
    chat = {}
    note_human_message(chat)
    assert not ordinary_reply_allowed(chat, min_human_messages=2)

    note_human_message(chat)
    assert ordinary_reply_allowed(chat, min_human_messages=2)

    mark_reply_sent(chat)
    assert not ordinary_reply_allowed(chat, min_human_messages=2)


def test_dialogue_owner_continues_for_ten_minutes_but_expires() -> None:
    chat = {}
    now = datetime(2026, 7, 15, 12, 0, 0)
    activate_dialogue(chat, initiator_id=7, text="давай обсудим пары", now=now)

    assert continue_dialogue(chat, user_id=7, text="а что по расписанию", window_minutes=10, now=now + timedelta(minutes=9))
    assert not continue_dialogue(chat, user_id=7, text="ну ладно", window_minutes=10, now=now + timedelta(minutes=20))


def test_snipe_requires_both_time_and_new_human_messages() -> None:
    chat = {}
    now = datetime(2026, 7, 15, 12, 0, 0)
    mark_snipe_sent(chat, now=now)
    for _ in range(12):
        note_human_message(chat)

    assert not snipe_allowed(chat, cooldown_minutes=30, min_human_messages=12, now=now + timedelta(minutes=29))
    assert snipe_allowed(chat, cooldown_minutes=30, min_human_messages=12, now=now + timedelta(minutes=30))


def test_snipe_and_quality_checks_are_bounded_by_new_messages() -> None:
    chat = {}
    for _ in range(4):
        note_human_message(chat)

    assert not snipe_allowed(chat, cooldown_minutes=30, min_human_messages=12)
    assert not interjection_check_allowed(chat, min_human_messages=5)

    note_human_message(chat)
    assert interjection_check_allowed(chat, min_human_messages=5)
    mark_interjection_checked(chat)
    assert not interjection_check_allowed(chat, min_human_messages=5)
