from datetime import datetime, timezone

from timur_bot.services.summary import (
    SUMMARY_MAX_MESSAGES,
    SUMMARY_MIN_MESSAGES_FOR_GENERAL_TOPIC,
    build_summary_messages,
    parse_summary_request,
    select_summary_window,
)


def test_parse_summary_reply_mode() -> None:
    req, err = parse_summary_request("", reply_message_id=77)
    assert err is None
    assert req is not None
    assert req.mode == "from_message"
    assert req.from_message_id == 77


def test_parse_summary_last_n() -> None:
    req, err = parse_summary_request("25", reply_message_id=None)
    assert err is None
    assert req is not None
    assert req.mode == "last_n"
    assert req.n == 25


def test_parse_summary_since_time_previous_day_if_future() -> None:
    now_utc = datetime(2026, 5, 28, 9, 0, tzinfo=timezone.utc)  # 12:00 msk
    req, err = parse_summary_request("since 14:00", reply_message_id=None, now_utc=now_utc)
    assert err is None
    assert req is not None
    assert req.mode == "since_time"
    assert req.since_utc is not None
    # local 14:00 is in future for 12:00 msk, so parser rolls to previous day 11:00 UTC
    assert req.since_utc == datetime(2026, 5, 27, 11, 0)


def test_parse_summary_bad_format() -> None:
    req, err = parse_summary_request("since soon", reply_message_id=None)
    assert req is None
    assert err is not None
    assert "/summary" in err


def test_select_summary_last_n() -> None:
    history = [
        {"message_id": 1, "text": "a", "name": "u1", "ts": "2026-05-28T10:00:00", "is_bot": False},
        {"message_id": 2, "text": "", "name": "u1", "ts": "2026-05-28T10:01:00", "is_bot": False},
        {"message_id": 3, "text": "c", "name": "u2", "ts": "2026-05-28T10:02:00", "is_bot": True},
    ]
    req, _ = parse_summary_request("2", reply_message_id=None)
    assert req is not None
    window = select_summary_window(history, req)
    assert window.status == "ok"
    assert window.selected_total == 2
    assert len(window.text_messages) == 1
    assert window.text_messages[0]["text"] == "c"


def test_select_summary_since_time() -> None:
    history = [
        {"message_id": 1, "text": "old", "name": "u1", "ts": "2026-05-28T10:00:00", "is_bot": False},
        {"message_id": 2, "text": "new", "name": "u2", "ts": "2026-05-28T13:00:00", "is_bot": False},
    ]
    req, _ = parse_summary_request(
        "since 16:00",
        reply_message_id=None,
        now_utc=datetime(2026, 5, 28, 14, 0, tzinfo=timezone.utc),  # 17:00 msk
    )
    assert req is not None
    window = select_summary_window(history, req)
    assert window.status == "ok"
    assert window.selected_total == 1
    assert window.text_messages[0]["text"] == "new"


def test_select_summary_reply_not_found() -> None:
    history = [{"message_id": 1, "text": "x", "name": "u1", "ts": "2026-05-28T10:00:00", "is_bot": False}]
    req, _ = parse_summary_request("", reply_message_id=999)
    assert req is not None
    window = select_summary_window(history, req)
    assert window.status == "not_found"


def test_select_summary_too_many() -> None:
    history = [
        {"message_id": i, "text": f"msg {i}", "name": "u1", "ts": "2026-05-28T10:00:00", "is_bot": False}
        for i in range(SUMMARY_MAX_MESSAGES + 1)
    ]
    req, _ = parse_summary_request(str(SUMMARY_MAX_MESSAGES + 1), reply_message_id=None)
    assert req is not None
    window = select_summary_window(history, req)
    assert window.status == "too_many"
    assert window.selected_total == SUMMARY_MAX_MESSAGES + 1


async def _fake_llm_general_theme(messages, max_tokens, temperature):  # type: ignore[no-untyped-def]
    user_text = messages[-1]["content"]
    if "сделай анализ куска переписки" in user_text:
        return '{"topics":[],"announcements":[]}'
    if "собери итоговое summary" in user_text:
        return '{"topic_messages":[],"announcements_message":"","fallback_message":""}'
    return '{"message":"в целом тут одна рабочая тема: ковыряли smtp блок и как переждать 24 часа"}'


def test_build_summary_messages_forces_general_topic_when_not_empty_range() -> None:
    text_messages = [
        {
            "name": "u1",
            "text": f"msg {i}",
            "ts": "2026-05-28T14:00:00",
            "is_bot": False,
            "user_id": 1,
            "message_id": i,
        }
        for i in range(SUMMARY_MIN_MESSAGES_FOR_GENERAL_TOPIC)
    ]

    import asyncio

    result = asyncio.run(
        build_summary_messages(
            text_messages=text_messages,
            tz_name="Europe/Moscow",
            system_prompt="sys",
            active_mode="default",
            mode_prompt="mode",
            style_settings="",
            bio_settings="",
            llm_call=_fake_llm_general_theme,
        )
    )
    assert result
    assert "рабочая тема" in result[0]
