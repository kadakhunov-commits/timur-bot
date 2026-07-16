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


def test_dialogue_owner_gets_immediate_followup_and_dialogue_expires() -> None:
    chat = {}
    now = datetime(2026, 7, 15, 12, 0, 0)
    activate_dialogue(chat, initiator_id=7, text="давай обсудим пары", now=now)

    assert continue_dialogue(chat, user_id=7, text="купил картошку", window_minutes=10, now=now + timedelta(minutes=1))
    assert continue_dialogue(chat, user_id=7, text="а что по парам", window_minutes=10, now=now + timedelta(minutes=9))
    assert not continue_dialogue(chat, user_id=7, text="заказал новые носки", window_minutes=10, now=now + timedelta(minutes=9))
    assert not continue_dialogue(chat, user_id=7, text="ну ладно", window_minutes=10, now=now + timedelta(minutes=20))


def test_direct_conversation_accepts_natural_topic_shift_from_same_person() -> None:
    chat = {}
    now = datetime(2026, 7, 15, 12, 0, 0)
    activate_dialogue(chat, initiator_id=7, text="тимур как дела", now=now)

    assert continue_dialogue(
        chat,
        user_id=7,
        text="у тебя завтра пары?",
        window_minutes=10,
        now=now + timedelta(minutes=1),
    )


def test_other_chatters_do_not_consume_initiators_open_followup() -> None:
    chat = {}
    now = datetime(2026, 7, 15, 12, 0, 0)
    activate_dialogue(chat, initiator_id=7, text="тимур как дела", now=now)

    for offset, user_id in enumerate((8, 9, 10), start=1):
        assert not continue_dialogue(
            chat,
            user_id=user_id,
            text=f"посторонняя реплика {offset}",
            window_minutes=10,
            now=now + timedelta(minutes=offset),
        )

    assert continue_dialogue(
        chat,
        user_id=7,
        text="у тебя завтра пары?",
        window_minutes=10,
        now=now + timedelta(minutes=4),
    )


def test_related_third_party_and_its_reply_do_not_erase_first_initiator_turn() -> None:
    chat = {}
    now = datetime(2026, 7, 15, 12, 0, 0)
    activate_dialogue(chat, initiator_id=7, text="тимур что по парам", now=now)

    assert continue_dialogue(
        chat,
        user_id=8,
        text="пары завтра отменили",
        window_minutes=10,
        now=now + timedelta(minutes=1),
    )
    # Production answers user 8 and opens their own follow-up too.
    activate_dialogue(chat, initiator_id=8, text="пары завтра отменили", now=now + timedelta(minutes=1))

    assert continue_dialogue(
        chat,
        user_id=7,
        text="а ты когда свободен?",
        window_minutes=10,
        now=now + timedelta(minutes=2),
    )


def test_dialogue_does_not_continue_on_generic_question_words() -> None:
    chat = {}
    now = datetime(2026, 7, 15, 12, 0, 0)
    activate_dialogue(chat, initiator_id=7, text="тимур что думаешь про айфон", now=now)

    assert not continue_dialogue(
        chat,
        user_id=8,
        text="что делать с картошкой",
        window_minutes=10,
        now=now + timedelta(minutes=1),
    )


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
