from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from timur_bot.services.fact_check import (
    VERDICT_LABELS,
    build_fact_check_messages,
    ensure_fact_check_state,
    extract_fact_check_payload,
    fact_check_request_allowed,
    mark_fact_check_request,
    mention_targets_bot,
    normalize_fact_check_reply,
    note_fact_check_verdict,
    strip_fact_check_markers,
)
import timur_bot.services.bot_logic as runtime


def test_mention_requires_exact_bot_username() -> None:
    assert mention_targets_bot(["@timur_bot"], "timur_bot") is True
    assert mention_targets_bot(["@Timur_Bot"], "timur_bot") is True
    assert mention_targets_bot(["timur", "@друг_бот"], "timur_bot") is False
    assert mention_targets_bot([], "timur_bot") is False
    assert mention_targets_bot(["@timur_bot"], "") is False


def test_strip_markers_removes_trigger_phrases_and_mention() -> None:
    cleaned = strip_fact_check_markers(
        "@timur_bot это правда?", bot_username="timur_bot"
    )
    assert cleaned == ""

    claim = strip_fact_check_markers(
        "@timur_bot земля круглая это правда??", bot_username="timur_bot"
    )
    assert "земля круглая" in claim
    assert "правда" not in claim


def test_claim_priority_is_reply_target_over_own_text() -> None:
    payload = extract_fact_check_payload(
        text="@timur_bot правда или нет",
        mention_texts=["@timur_bot"],
        bot_username="timur_bot",
        reply_message={
            "text": "я в пятницу прилечу",
            "user_id": 5,
            "name": "а",
            "username": "",
            "is_bot": False,
            "message_id": 42,
        },
        author_user_id=7,
        bot_id=999,
    )
    assert payload["triggered"] is True
    assert payload["claim_text"] == "я в пятницу прилечу"
    assert payload["claim_author_id"] == 5
    assert payload["claim_message_id"] == 42
    assert payload["claim_source"] == "reply_target"


def test_claim_falls_back_to_mention_message_itself() -> None:
    payload = extract_fact_check_payload(
        text="@timur_bot казань это столица татарстана, это правда?",
        mention_texts=["@timur_bot"],
        bot_username="timur_bot",
        reply_message=None,
        author_user_id=7,
        bot_id=999,
    )
    assert payload["triggered"] is True
    assert "казань" in payload["claim_text"]
    assert payload["claim_source"] == "mention_message"
    assert payload["claim_author_id"] == 7


def test_bot_rivials_own_bot_claims_and_empty_claims_are_rejected() -> None:
    no_claim = extract_fact_check_payload(
        text="@timur_bot это правда?",
        mention_texts=["@timur_bot"],
        bot_username="timur_bot",
        reply_message={
            "text": "я прилечу",
            "user_id": 7,
            "name": "а",
            "is_bot": False,
            "message_id": 1,
        },
        author_user_id=7,
        bot_id=999,
    )
    assert no_claim["triggered"] is False

    self_bot = extract_fact_check_payload(
        text="@timur_bot правда?",
        mention_texts=["@timur_bot"],
        bot_username="timur_bot",
        reply_message={
            "text": "я бот и я всегда прав",
            "user_id": 0,
            "name": "тимур",
            "is_bot": True,
            "message_id": 1,
        },
        author_user_id=7,
        bot_id=999,
    )
    assert self_bot["triggered"] is False


def test_no_mention_means_no_fact_check() -> None:
    payload = extract_fact_check_payload(
        text="это правда?",
        mention_texts=[],
        bot_username="timur_bot",
        reply_message={"text": "утверждение", "user_id": 5, "is_bot": False, "message_id": 3},
        author_user_id=7,
        bot_id=999,
    )
    assert payload["triggered"] is False


def test_hourly_rate_limit_blocks_seventh_request() -> None:
    chat: dict = {}
    base = datetime(2026, 8, 1, 12, 0, 0)
    for offset in range(6):
        mark_fact_check_request(chat, now=base + timedelta(minutes=offset))
    assert fact_check_request_allowed(chat, max_per_hour=6, now=base + timedelta(minutes=30)) is False
    assert fact_check_request_allowed(chat, max_per_hour=6, now=base + timedelta(minutes=61)) is True
    note_fact_check_verdict(chat, label="правда")
    assert ensure_fact_check_state(chat)["verdicts"]["правда"] == 1


def test_verdict_normalization_requires_known_label() -> None:
    good = normalize_fact_check_reply("скорее нет. он три пятницы переносил", max_chars=160)
    assert good["label"] == "скорее нет"
    assert good["fallback_used"] is False

    unlabeled = normalize_fact_check_reply("нуу, тут сложно сказать наверняка", max_chars=160)
    assert unlabeled["text"] == "не проверяемо"
    assert unlabeled["fallback_used"] is True

    assert normalize_fact_check_reply("враньё", max_chars=160)["label"] == "враньё"
    assert "не проверяемо" in VERDICT_LABELS


def test_fact_check_prompt_pins_labels_and_web_note() -> None:
    messages = build_fact_check_messages(
        "ты тимур",
        claim="земля плоская",
        author_name="а",
        scene="а: земля плоская\nб: ???",
        facts_prompt="- а: верит в плоскую землю",
        dossier="а часто преувеличивает",
        max_chars=160,
        web_search=True,
    )
    system, user = messages[0]["content"], messages[1]["content"]
    assert "правила вердикта" in system
    assert all(label in system for label in VERDICT_LABELS)
    assert "интернет" in system
    assert "земля плоская" in user
    assert "плоскую землю" in user
    assert "преувеличивает" in user

    offline = build_fact_check_messages(
        "ты тимур", claim="x", author_name="", scene="", facts_prompt="", dossier="", max_chars=160, web_search=False,
    )[0]["content"]
    assert "интернет" not in offline


def test_fact_check_extra_body_attaches_polza_web_plugin() -> None:
    settings = {"web_search": True, "web_max_results": 4}
    with patch.object(runtime, "OPENAI_BASE_URL", "https://polza.ai/api/v1"):
        body = runtime._fact_check_extra_body(settings)
    assert body["plugins"] == [{"id": "web", "max_results": 4}]
    assert "provider" in body

    with patch.object(runtime, "OPENAI_BASE_URL", "https://api.openai.com/v1"):
        assert runtime._fact_check_extra_body(settings) == {}

    with patch.object(runtime, "OPENAI_BASE_URL", "https://polza.ai/api/v1"):
        assert runtime._fact_check_extra_body({"web_search": False, "web_max_results": 4}) == {
            "reasoning": {"enabled": False},
            "provider": {
                "ignore": list(runtime.POLZA_IGNORED_TEXT_PROVIDERS),
                "sort": "latency",
                "allow_fallbacks": True,
            },
        }


def _mention_entity(text: str, mention: str) -> SimpleNamespace:
    return SimpleNamespace(type="mention", offset=text.find(mention), length=len(mention))


def _fact_check_update(*, text: str = "@timur_bot это правда?", reply=None, chat_id: int = 300):
    message = SimpleNamespace(
        chat_id=chat_id,
        message_id=20,
        text=text,
        caption=None,
        from_user=SimpleNamespace(id=7, first_name="а", username=None, is_bot=False),
        reply_to_message=reply,
        entities=[_mention_entity(text, "@timur_bot")],
        caption_entities=[],
    )
    if reply is not None:
        author = SimpleNamespace(id=reply.get("user_id", 5), first_name=reply.get("name", "б"), username=None, is_bot=reply.get("is_bot", False))
        message.reply_to_message = SimpleNamespace(
            text=reply.get("text", ""),
            message_id=reply.get("message_id", 50),
            from_user=author,
        )
    else:
        message.reply_to_message = None
    return SimpleNamespace(effective_message=message), SimpleNamespace(
        bot=SimpleNamespace(id=999, username="timur_bot", send_message=AsyncMock(return_value=SimpleNamespace(message_id=77)))
    )


def test_handle_fact_check_sends_reply_to_claim_message() -> None:
    update, context = _fact_check_update(
        reply={"text": "я в пятницу прилечу", "user_id": 5, "name": "б", "message_id": 50, "is_bot": False},
        chat_id=301,
    )
    memory = runtime.default_memory()
    chat = runtime.get_chat_mem(memory, 301)
    chat["history"] = [
        {"message_id": 50, "user_id": 5, "name": "б", "text": "я в пятницу прилечу", "ts": "2026-08-01T12:00:00"},
        {"message_id": 51, "user_id": 7, "name": "а", "text": "ну посмотрим", "ts": "2026-08-01T12:00:10"},
    ]
    runtime.note_human_message(chat)

    with (
        patch.object(runtime, "call_openai_fact_check", AsyncMock(return_value="скорее нет. он три пятницы подряд переносил")),
        patch.object(runtime, "save_memory"),
        patch.object(runtime.billing, "register_bot_reply"),
    ):
        handled = asyncio_run_sync(runtime._handle_fact_check(update, context, memory))

    assert handled is True
    sent = context.bot.send_message.await_args
    assert sent.kwargs["chat_id"] == 301
    assert sent.kwargs["reply_to_message_id"] == 50
    assert sent.kwargs["text"].startswith("скорее нет.")
    layers = chat.get("memory_layers", {})
    assert layers["fact_check"]["requests"], "request counter must be recorded"
    decisions = layers["humor_decisions_v2"]
    assert decisions[-1]["action"] == "FACT_CHECK"
    assert decisions[-1]["sent"] is True
    assert "verdict:скорее нет" in decisions[-1]["reason_codes"]
    scene_rows = [scene.get("output_kind") for scene in layers["humor_scenes_v2"]]
    assert "fact_check" in scene_rows


def test_mention_without_a_claim_stays_silent() -> None:
    update, context = _fact_check_update(chat_id=302)
    memory = runtime.default_memory()
    chat = runtime.get_chat_mem(memory, 302)
    chat["history"] = [
        {"message_id": 20, "user_id": 7, "name": "а", "text": "@timur_bot это правда?", "ts": "2026-08-01T12:00:00"}
    ]
    metered = AsyncMock(return_value="хм, надо подумать")

    with patch.object(runtime, "call_openai_fact_check", metered), patch.object(runtime, "save_memory"):
        handled = asyncio_run_sync(runtime._handle_fact_check(update, context, memory))

    assert handled is False
    metered.assert_not_awaited()
    context.bot.send_message.assert_not_awaited()


def test_handle_fact_check_fallback_verdict_when_label_missing() -> None:
    update, context = _fact_check_update(
        reply={"text": "я в пятницу прилечу", "user_id": 5, "name": "б", "message_id": 50, "is_bot": False},
        chat_id=307,
    )
    memory = runtime.default_memory()
    chat = runtime.get_chat_mem(memory, 307)
    chat["history"] = [
        {"message_id": 50, "user_id": 5, "name": "б", "text": "я в пятницу прилечу", "ts": "2026-08-01T12:00:00"},
        {"message_id": 51, "user_id": 7, "name": "а", "text": "@timur_bot это правда?", "ts": "2026-08-01T12:00:10"},
    ]

    with (
        patch.object(runtime, "call_openai_fact_check", AsyncMock(return_value="хм, надо подумать")),
        patch.object(runtime, "save_memory"),
        patch.object(runtime.billing, "register_bot_reply"),
    ):
        handled = asyncio_run_sync(runtime._handle_fact_check(update, context, memory))

    assert handled is True
    assert context.bot.send_message.await_args.kwargs["text"] == "не проверяемо"
    decisions = chat["memory_layers"]["humor_decisions_v2"]
    assert "fallback_label" in decisions[-1]["reason_codes"]


def test_handle_fact_check_rate_limit_blocks_llm_call() -> None:
    update, context = _fact_check_update(chat_id=303)
    memory = runtime.default_memory()
    chat = runtime.get_chat_mem(memory, 303)
    chat["history"] = [{"message_id": 20, "user_id": 7, "name": "а", "text": "@timur_bot правда?", "ts": "2026-08-01T12:00:00"}]
    state = ensure_fact_check_state(chat)
    now = datetime.utcnow()
    state["requests"] = [(now - timedelta(minutes=offset)).isoformat() for offset in range(6)]
    metered = AsyncMock(return_value="правда.")

    with patch.object(runtime, "call_openai_fact_check", metered), patch.object(runtime, "save_memory"):
        handled = asyncio_run_sync(runtime._handle_fact_check(update, context, memory))

    assert handled is False
    metered.assert_not_awaited()
    context.bot.send_message.assert_not_awaited()


def test_text_handler_routes_mention_to_fact_check_instead_of_reply_decision() -> None:
    update, context = _fact_check_update(chat_id=304)
    message = update.effective_message
    memory = runtime.default_memory()

    with (
        patch.object(runtime, "load_memory", return_value=memory),
        patch.object(runtime, "_handle_admin_pending_text", new=AsyncMock(return_value=False)),
        patch.object(runtime, "_handle_text_feedback", new=AsyncMock(return_value=False)),
        patch.object(runtime, "update_memory_with_message"),
        patch.object(runtime, "_observe_chat_humor"),
        patch.object(runtime, "_apply_message_mood_impact", return_value=False),
        patch.object(runtime, "_sync_mood_state"),
        patch.object(runtime, "_handle_fact_check", new=AsyncMock(return_value=True)) as fact_check,
        patch.object(runtime, "should_reply_decision") as decision,
        patch.object(runtime, "save_memory"),
    ):
        asyncio_run_sync(runtime.text_handler(update, context))

    fact_check.assert_awaited_once()
    decision.assert_not_called()


def test_text_handler_ignores_mention_when_fact_check_disabled() -> None:
    update, context = _fact_check_update(chat_id=305)
    memory = runtime.default_memory()
    memory["config"]["fact_check"] = {"enabled": False}

    with (
        patch.object(runtime, "load_memory", return_value=memory),
        patch.object(runtime, "_handle_admin_pending_text", new=AsyncMock(return_value=False)),
        patch.object(runtime, "_handle_text_feedback", new=AsyncMock(return_value=False)),
        patch.object(runtime, "update_memory_with_message"),
        patch.object(runtime, "_observe_chat_humor"),
        patch.object(runtime, "_apply_message_mood_impact", return_value=False),
        patch.object(runtime, "_sync_mood_state"),
        patch.object(runtime, "_handle_fact_check", new=AsyncMock(return_value=True)) as fact_check,
        patch.object(runtime, "should_reply_decision", return_value=runtime.ReplyDecision(False, "нет")) as decision,
        patch.object(runtime, "_maybe_send_adaptive_snipe", new=AsyncMock(return_value=False)),
        patch.object(runtime, "save_memory"),
    ):
        asyncio_run_sync(runtime.text_handler(update, context))

    fact_check.assert_not_awaited()
    decision.assert_called_once()


def asyncio_run_sync(coro):
    import asyncio

    return asyncio.run(coro)
