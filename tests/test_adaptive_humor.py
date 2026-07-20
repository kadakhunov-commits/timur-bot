import asyncio
import os
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

os.environ.setdefault("TELEGRAM_BOT_TOKEN", "test-token")
os.environ.setdefault("OPENAI_API_KEY", "test-api-key")

from timur_bot.services.adaptive_humor import (
    critic_messages,
    director_writer_messages,
    filter_candidates,
    parse_critic,
    parse_critic_decision,
    parse_director,
    render_scene,
    strip_stale_context_references,
)
from timur_bot.services import bot_logic as runtime
from timur_bot.services.humor import ensure_humor_schema, record_bot_output


def test_scene_is_structured_and_bounded() -> None:
    history = [
        {"message_id": i, "reply_to_message_id": i - 1 if i else None, "name": f"u{i}", "text": "x" * 220}
        for i in range(12)
    ]
    history[-1]["is_bot"] = True
    history[-2]["is_forward"] = True
    history[-2]["forward_origin_chat_title"] = "news"

    scene = render_scene(history)

    assert len(scene) <= 1200
    assert "reply=" in scene
    assert "bot" in scene
    assert "forward" in scene
    assert "u0" not in scene


def test_writer_does_not_judge_itself_and_critic_has_silence() -> None:
    history = [{"name": "рустем", "text": "ну мы же планируем"}]
    writer_prompt = director_writer_messages(history)[0]["content"]
    critic_prompt = critic_messages(history, [{"text": "планируете не приехать", "mechanism": "logic"}])[0]["content"]

    assert "Никаких score" in writer_prompt
    assert '"score"' not in writer_prompt
    assert '"winner_index"' not in writer_prompt
    assert "SILENCE" in critic_prompt
    assert "независимый" in critic_prompt
    assert "latest_message_funny" in writer_prompt
    assert "heart-реакцию" in critic_prompt


def test_parsers_are_strict_and_bounded() -> None:
    director = parse_director(
        '{"should_attempt":true,"setup":"план вместо действия","target":"разрыв",'
        '"scene_type":"contradiction","relation":"pile_on","forbidden_moves":["определение"],'
        '"candidates":['
        '{"text":"планируете не приехать, всё сходится","mechanism":"logic","callback_key":""},'
        '{"text":"главное план не тревожить поездкой","mechanism":"status","callback_key":""},'
        '{"text":"поездка уже почти обсудилась","mechanism":"image","callback_key":""},'
        '{"text":"всё, сомнения отпали","mechanism":"understatement","callback_key":""}]} '
    )
    assert director["should_attempt"] is True
    assert director["scene_type"] == "contradiction"
    assert director["candidates"][0]["mechanism"] == "logic"
    assert parse_director(
        '{"should_attempt":true,"setup":"x","target":"x","scene_type":"x","relation":"x",'
        '"forbidden_moves":[],"candidates":[{"text":"один вариант","mechanism":"logic","callback_key":""}]}'
    )["should_attempt"] is False
    assert parse_director("no json")["candidates"] == []
    assert parse_director('{"should_attempt":true,"score":99}')["should_attempt"] is False

    assert parse_critic('{"winner_index":0,"score":91,"reason_codes":["local"]}', candidate_count=1) == (
        0,
        91,
        ["local"],
    )
    assert parse_critic('{"winner_index":3,"score":99}', candidate_count=1)[0] is None
    assert parse_critic('{"winner_index":"0","score":"99"}', candidate_count=1)[:2] == (None, 0)
    assert parse_critic('prefix {"winner_index":0,"score":99}', candidate_count=1)[0] is None
    assert parse_critic("bad") == (None, 0, ["invalid_json"])
    assert parse_critic_decision(
        '{"winner_index":null,"score":0,"react":true,"reaction_score":94,"reason_codes":["finished"]}',
        candidate_count=0,
    ) == {
        "winner_index": None,
        "score": 0,
        "react": True,
        "reaction_score": 94,
        "reason_codes": ["finished"],
    }


def test_hard_filter_rejects_forced_and_ungrounded_candidates() -> None:
    history = [
        {"name": "а", "text": "да один хуй не приедете"},
        {"name": "б", "text": "ну мы же планируем"},
    ]
    candidates = [
        {"text": "план — это когда есть хоть одна деталь", "mechanism": "definition"},
        {"text": "а то я думал для поделок", "mechanism": "literal"},
        {"text": "митя уже удалил сообщения", "mechanism": "callback"},
        {"text": "гений мысли снова в чате", "mechanism": "roast"},
        {"text": "ну ты клоун", "mechanism": "roast"},
        {"text": "ну ты клоун.", "mechanism": "roast"},
        {"text": "iq 3", "mechanism": "roast"},
        {"text": "митю опять позвали", "mechanism": "callback", "callback_key": "mitya"},
        {"text": "кадыра опять позвали", "mechanism": "callback", "callback_key": "kadyr"},
        {"text": "две строки\nне надо", "mechanism": "chain"},
        {"text": "старый прикол", "mechanism": "callback", "callback_key": ""},
        {"text": "планируем короче", "mechanism": "repeat"},
        {"text": "планируете не приехать, всё сходится", "mechanism": "logic"},
        {"text": "x" * 61, "mechanism": "long"},
    ]

    filtered = filter_candidates(
        candidates,
        history=history,
        known_participant_names=["митя", "кадыр"],
    )

    assert filtered == [
        {"text": "планируете не приехать, всё сходится", "mechanism": "logic", "callback_key": ""}
    ]


def test_filter_rejects_recent_duplicates_and_callback_cooldown() -> None:
    history = [{"name": "а", "text": "приехали почти"}]
    candidates = [
        {"text": "приехали уже, но концептуально", "mechanism": "logic", "callback_key": ""},
        {"text": "главное не тревожить план поездкой", "mechanism": "logic", "callback_key": "trip"},
        {"text": "поездка уже почти обсудилась", "mechanism": "understatement", "callback_key": ""},
    ]

    filtered = filter_candidates(
        candidates,
        history=history,
        recent_outputs=["приехали уже, но концептуально"],
        blocked_callback_keys=["trip"],
    )

    assert [item["text"] for item in filtered] == ["поездка уже почти обсудилась"]


def test_filter_blocks_every_callback_after_recent_callback_budget() -> None:
    filtered = filter_candidates(
        [
            {"text": "обычная логическая добивка", "mechanism": "logic", "callback_key": ""},
            {"text": "снова тот самый прикол", "mechanism": "callback", "callback_key": "old-bit"},
        ],
        history=[{"name": "а", "text": "обсуждают поездку"}],
        blocked_callback_keys=["*"],
    )
    assert [item["text"] for item in filtered] == ["обычная логическая добивка"]


def test_participant_name_is_allowed_when_person_is_author_in_scene() -> None:
    filtered = filter_candidates(
        [{"text": "митя уже почти приехал", "mechanism": "logic", "callback_key": ""}],
        history=[{"name": "митя", "text": "я выхожу"}],
        known_participant_names=["митя", "кадыр"],
    )
    assert filtered == [
        {"text": "митя уже почти приехал", "mechanism": "logic", "callback_key": "person:митя"}
    ]


def test_display_name_words_are_not_treated_as_absent_people() -> None:
    filtered = filter_candidates(
        [{"text": "он вообще прав", "mechanism": "logic", "callback_key": ""}],
        history=[{"name": "а", "text": "да вроде нормально сказал"}],
        known_participant_names=["он чел он миша"],
    )
    assert [item["text"] for item in filtered] == ["он вообще прав"]


def test_person_and_deleted_message_callbacks_get_deterministic_cooldown_keys() -> None:
    history = [{"name": "митя", "text": "я опять тут"}]
    candidate = {"text": "митя опять вышел на смену", "mechanism": "logic", "callback_key": ""}
    first = filter_candidates([candidate], history=history, known_participant_names=["митя"])
    assert first[0]["callback_key"] == "person:митя"
    assert filter_candidates(
        [candidate],
        history=history,
        known_participant_names=["митя"],
        blocked_callback_keys=["person:митя"],
    ) == []

    deleted = filter_candidates(
        [{"text": "сообщение опять удалили", "mechanism": "logic", "callback_key": "wrong-key"}],
        history=[{"name": "а", "text": "кто удалил сообщение"}],
    )
    assert deleted[0]["callback_key"] == "topic:deleted_messages"


def test_direct_guard_keeps_answer_and_drops_absent_name_callback() -> None:
    cleaned = strip_stale_context_references(
        "я на deepseek v4 flash. кадыр опять расследует",
        history=[{"name": "а", "text": "на какой модели работаешь?"}],
        known_participant_names=["кадыр", "митя"],
    )
    assert cleaned == "я на deepseek v4 flash."

    comma_cleaned = strip_stale_context_references(
        "я на deepseek v4 flash, кадыр опять расследует",
        history=[{"name": "а", "text": "на какой модели работаешь?"}],
        known_participant_names=["кадыр", "митя"],
    )
    assert comma_cleaned == "я на deepseek v4 flash"


def _ambient_fixture(chat_id: int = 777) -> tuple[dict, SimpleNamespace, SimpleNamespace]:
    memory = runtime.default_memory()
    memory["config"]["adaptive_humor"]["participation_rate"] = 1.0
    chat = runtime.get_chat_mem(memory, chat_id)
    rows = [
        {"message_id": 1, "user_id": 1, "name": "а", "text": "да один хуй не приедете", "ts": "2026-07-16T12:00:00"},
        {"message_id": 2, "user_id": 2, "name": "б", "text": "зачем", "ts": "2026-07-16T12:00:10"},
        {"message_id": 3, "user_id": 1, "name": "а", "text": "ну мы же планируем", "ts": "2026-07-16T12:00:20"},
    ]
    chat["history"] = rows
    for _ in rows:
        runtime.note_human_message(chat)
    message = SimpleNamespace(
        chat_id=chat_id,
        message_id=3,
        text="ну мы же планируем",
        caption=None,
        from_user=SimpleNamespace(id=1, first_name="а", username=None, is_bot=False),
        reply_to_message=None,
    )
    return memory, SimpleNamespace(effective_message=message), SimpleNamespace()


def test_runtime_ambient_path_uses_writer_then_independent_critic() -> None:
    memory, update, context = _ambient_fixture()
    writer = (
        '{"should_attempt":true,"setup":"план вместо действия","target":"разрыв",'
        '"scene_type":"contradiction","relation":"pile_on","forbidden_moves":["definition"],'
        '"candidates":['
        '{"text":"планируете не приехать, всё сходится","mechanism":"logic","callback_key":""},'
        '{"text":"поездка уже почти обсудилась","mechanism":"status","callback_key":""},'
        '{"text":"главное не тревожить план поездкой","mechanism":"image","callback_key":""},'
        '{"text":"сомнения можно было не будить","mechanism":"understatement","callback_key":""}]}'
    )
    critic = '{"winner_index":0,"score":93,"reason_codes":["local","short"]}'
    metered = AsyncMock(side_effect=[(writer, 100), (critic, 20)])
    sender = AsyncMock(return_value=True)

    with (
        patch.object(runtime, "call_openai_metered", metered),
        patch.object(runtime, "send_reply_with_style", sender),
        patch.object(runtime, "save_memory"),
        patch.object(runtime.random, "random", return_value=0.0),
    ):
        sent = asyncio.run(runtime._maybe_send_adaptive_snipe(update, context, memory))

    assert sent is True
    assert metered.await_count == 2
    assert metered.await_args_list[0].kwargs["max_tokens"] == 180
    assert metered.await_args_list[1].kwargs["max_tokens"] == 40
    assert sender.await_args.args[3] == "планируете не приехать, всё сходится"
    assert sender.await_args.kwargs["is_snipe"] is True
    layers = ensure_humor_schema(runtime.get_chat_mem(memory, 777))
    assert layers["humor_daily_usage_v2"][datetime.utcnow().date().isoformat()] == 120


def test_runtime_writer_receives_one_relevant_hearted_example() -> None:
    memory, update, context = _ambient_fixture(chat_id=776)
    chat = runtime.get_chat_mem(memory, 776)
    runtime.learn_funny_scene(
        chat,
        context=[{"author": "а", "text": "встречу опять перенесли"}],
        punchline="срок чисто декоративный",
        after_context=[],
        signals=["heart"],
        mechanism="understatement",
        source_message_id=90,
    )
    writer = (
        '{"should_attempt":false,"setup":"","target":"","scene_type":"banter",'
        '"relation":"chat","forbidden_moves":[],"candidates":[]}'
    )
    metered = AsyncMock(return_value=(writer, 50))

    with (
        patch.object(runtime, "call_openai_metered", metered),
        patch.object(runtime, "save_memory"),
        patch.object(runtime.random, "random", return_value=0.0),
    ):
        assert asyncio.run(runtime._maybe_send_adaptive_snipe(update, context, memory)) is False

    writer_messages = metered.await_args.args[0]
    assert "один похожий пример с ❤️" in writer_messages[-1]["content"]
    assert "срок чисто декоративный" in writer_messages[-1]["content"]


def test_runtime_writer_can_choose_silence_without_critic_call() -> None:
    memory, update, context = _ambient_fixture()
    writer = (
        '{"should_attempt":false,"setup":"техническое уточнение","target":"","scene_type":"technical",'
        '"relation":"neutral","forbidden_moves":[],"candidates":[]}'
    )
    metered = AsyncMock(return_value=(writer, 55))
    sender = AsyncMock(return_value=True)

    with (
        patch.object(runtime, "call_openai_metered", metered),
        patch.object(runtime, "send_reply_with_style", sender),
        patch.object(runtime.random, "random", return_value=0.0),
    ):
        sent = asyncio.run(runtime._maybe_send_adaptive_snipe(update, context, memory))

    assert sent is False
    assert metered.await_count == 1
    sender.assert_not_awaited()


def test_runtime_finished_joke_gets_heart_without_text_reply() -> None:
    memory, update, context = _ambient_fixture()
    context.bot = SimpleNamespace(set_message_reaction=AsyncMock())
    writer = (
        '{"should_attempt":false,"latest_message_funny":true,"setup":"готовая шутка","target":"",'
        '"scene_type":"banter","relation":"chat","forbidden_moves":[],"candidates":[]}'
    )
    critic = (
        '{"winner_index":null,"score":0,"react":true,"reaction_score":94,'
        '"reason_codes":["finished_joke"]}'
    )
    metered = AsyncMock(side_effect=[(writer, 55), (critic, 20)])
    sender = AsyncMock(return_value=True)

    with (
        patch.object(runtime, "call_openai_metered", metered),
        patch.object(runtime, "send_reply_with_style", sender),
        patch.object(runtime, "save_memory"),
        patch.object(runtime.random, "random", return_value=0.0),
    ):
        acted = asyncio.run(runtime._maybe_send_adaptive_snipe(update, context, memory))

    assert acted is True
    context.bot.set_message_reaction.assert_awaited_once_with(
        chat_id=777,
        message_id=3,
        reaction="❤️",
    )
    sender.assert_not_awaited()
    decision = ensure_humor_schema(runtime.get_chat_mem(memory, 777))["humor_decisions_v2"][-1]
    assert decision["action"] == "REACT"


def test_funny_heart_never_targets_bot_messages() -> None:
    bot = SimpleNamespace(set_message_reaction=AsyncMock())
    context = SimpleNamespace(bot=bot)
    message = SimpleNamespace(
        chat_id=777,
        message_id=3,
        from_user=SimpleNamespace(is_bot=True),
    )

    reacted = asyncio.run(runtime._set_funny_heart_reaction(context, message))

    assert reacted is False
    bot.set_message_reaction.assert_not_awaited()


def test_failed_heart_does_not_suppress_approved_text_reply() -> None:
    memory, update, context = _ambient_fixture()
    context.bot = SimpleNamespace(set_message_reaction=AsyncMock(side_effect=RuntimeError("reactions disabled")))
    writer = (
        '{"should_attempt":true,"latest_message_funny":true,"setup":"готовая шутка","target":"разрыв",'
        '"scene_type":"banter","relation":"chat","forbidden_moves":[],"candidates":['
        '{"text":"добивка раз","mechanism":"logic","callback_key":""},'
        '{"text":"добивка два","mechanism":"status","callback_key":""},'
        '{"text":"добивка три","mechanism":"image","callback_key":""},'
        '{"text":"добивка четыре","mechanism":"understatement","callback_key":""}]}'
    )
    critic = (
        '{"winner_index":0,"score":93,"react":true,"reaction_score":95,'
        '"reason_codes":["local"]}'
    )
    sender = AsyncMock(return_value=True)

    with (
        patch.object(runtime, "call_openai_metered", AsyncMock(side_effect=[(writer, 80), (critic, 20)])),
        patch.object(runtime, "send_reply_with_style", sender),
        patch.object(runtime, "save_memory"),
        patch.object(runtime.random, "random", return_value=0.0),
    ):
        acted = asyncio.run(runtime._maybe_send_adaptive_snipe(update, context, memory))

    assert acted is True
    sender.assert_awaited_once()


def test_runtime_daily_budget_prevents_background_api_calls() -> None:
    memory, update, context = _ambient_fixture()
    chat = runtime.get_chat_mem(memory, 777)
    layers = ensure_humor_schema(chat)
    layers["humor_daily_usage_v2"][datetime.utcnow().date().isoformat()] = 12_000
    metered = AsyncMock()

    with (
        patch.object(runtime, "call_openai_metered", metered),
        patch.object(runtime.random, "random", return_value=0.0),
    ):
        sent = asyncio.run(runtime._maybe_send_adaptive_snipe(update, context, memory))

    assert sent is False
    metered.assert_not_awaited()


def test_direct_api_call_disables_reasoning_uses_proven_providers_and_keeps_sixty_tokens() -> None:
    response = SimpleNamespace(
        choices=[SimpleNamespace(message=SimpleNamespace(content="короткий ответ"))],
    )
    create = AsyncMock(return_value=response)
    with (
        patch.object(runtime, "OPENAI_BASE_URL", "https://polza.ai/api/v1"),
        patch.object(runtime.async_client.chat.completions, "create", create),
    ):
        text = asyncio.run(runtime.call_openai_text([{"role": "user", "content": "тимур?"}]))

    assert text == "короткий ответ"
    assert create.await_count == 1
    assert create.await_args.kwargs["max_tokens"] == 60
    assert "timeout" not in create.await_args.kwargs
    assert create.await_args.kwargs["extra_body"] == {
        "reasoning": {"enabled": False},
        "provider": {
            "only": ["DeepInfra", "Baidu"],
            "order": ["DeepInfra", "Baidu"],
            "allow_fallbacks": True,
        },
    }
    assert runtime.TEXT_TRANSPORT_TIMEOUT_SECONDS > 3.0
    assert runtime.async_client.max_retries == 0


def test_non_polza_api_call_does_not_receive_provider_specific_fields() -> None:
    response = SimpleNamespace(
        choices=[SimpleNamespace(message=SimpleNamespace(content="короткий ответ"))],
    )
    create = AsyncMock(return_value=response)
    with (
        patch.object(runtime, "OPENAI_BASE_URL", "https://api.openai.com/v1"),
        patch.object(runtime.async_client.chat.completions, "create", create),
    ):
        asyncio.run(runtime.call_openai_text([{"role": "user", "content": "тимур?"}]))

    assert create.await_args.kwargs["extra_body"] == {}


def test_token_ceiling_covers_utf8_bytes_and_message_framing() -> None:
    messages = [
        {"role": "system", "content": "кириллица " * 20},
        {"role": "user", "content": "❤️" * 20},
    ]
    utf8_bytes = sum(len(item["content"].encode("utf-8")) for item in messages)
    assert runtime._completion_token_ceiling(messages, 40) >= utf8_bytes + 16 * len(messages) + 40


def test_metered_fallback_is_conservative_without_provider_usage() -> None:
    response = SimpleNamespace(
        choices=[SimpleNamespace(message=SimpleNamespace(content="❤️ смешно"))],
        usage=None,
    )
    create = AsyncMock(return_value=response)
    messages = [{"role": "user", "content": "кириллица " * 20}]
    with (
        patch.object(runtime, "OPENAI_BASE_URL", "https://polza.ai/api/v1"),
        patch.object(runtime.async_client.chat.completions, "create", create),
    ):
        text, tokens = asyncio.run(runtime.call_openai_metered(messages, max_tokens=40, temperature=0.1))

    assert text == "❤️ смешно"
    assert tokens >= len(messages[0]["content"].encode("utf-8"))
    assert create.await_args.kwargs["extra_body"]["reasoning"] == {"enabled": False}
    assert create.await_args.kwargs["extra_body"]["provider"]["only"] == ["DeepInfra", "Baidu"]
    assert create.await_args.kwargs["extra_body"]["provider"]["order"] == ["DeepInfra", "Baidu"]


def test_non_reply_laugh_is_feedback_and_not_an_invitation_to_answer() -> None:
    message = SimpleNamespace(text="лол", caption=None, reply_to_message=None)
    update = SimpleNamespace(effective_message=message)
    assert asyncio.run(runtime._handle_text_feedback(update, runtime.default_memory())) is True


def test_adjacent_laugh_uses_telegram_event_time_not_queue_processing_time() -> None:
    memory = runtime.default_memory()

    def message(message_id: int, user_id: int, text: str, minute: int) -> SimpleNamespace:
        return SimpleNamespace(
            chat_id=780,
            message_id=message_id,
            text=text,
            caption=None,
            date=datetime(2026, 7, 16, 12, minute, tzinfo=timezone.utc),
            from_user=SimpleNamespace(
                id=user_id,
                first_name=f"u{user_id}",
                username=None,
                is_bot=False,
            ),
            sender_chat=None,
            reply_to_message=None,
        )

    setup = message(1, 1, "планируете не приехать", 0)
    delayed_laugh = message(2, 2, "лол", 5)
    with (
        patch.object(runtime, "save_memory") as save_memory,
        patch.object(runtime.billing, "register_activity"),
    ):
        runtime.update_memory_with_message(memory, setup)
        runtime.update_memory_with_message(memory, delayed_laugh)
    save_memory.assert_not_called()
    runtime._observe_chat_humor(memory, delayed_laugh)

    chat = runtime.get_chat_mem(memory, 780)
    assert chat["history"][0]["ts"] == "2026-07-16T12:00:00"
    assert ensure_humor_schema(chat)["humor_scenes_v2"] == []


def test_non_heart_reaction_never_reaches_learning_state() -> None:
    reaction = SimpleNamespace(
        chat=SimpleNamespace(id=777),
        message_id=1,
        user=SimpleNamespace(id=9),
        old_reaction=[],
        new_reaction=[SimpleNamespace(emoji="😂")],
    )
    with (
        patch.object(runtime, "apply_reaction_delta") as apply_delta,
        patch.object(runtime, "load_memory") as load_memory,
    ):
        asyncio.run(runtime.reaction_handler(SimpleNamespace(message_reaction=reaction), SimpleNamespace()))

    apply_delta.assert_not_called()
    load_memory.assert_not_called()


def test_cancelled_background_call_keeps_conservative_budget_reservation() -> None:
    memory, update, context = _ambient_fixture(chat_id=778)

    async def slow_call(*args, **kwargs):
        await asyncio.sleep(1)
        return "", 0

    with (
        patch.object(runtime, "call_openai_metered", side_effect=slow_call),
        patch.object(runtime.random, "random", return_value=0.0),
    ):
        result = asyncio.run(
            runtime._run_with_typing(
                context,
                778,
                runtime._maybe_send_adaptive_snipe(update, context, memory),
                timeout_seconds=0.01,
                show_typing=False,
            )
        )

    assert result == ""
    assert runtime.background_tokens_used_today(runtime.get_chat_mem(memory, 778)) > 0


def test_background_send_error_is_saved_without_counting_sent() -> None:
    memory, update, context = _ambient_fixture(chat_id=779)
    writer = (
        '{"should_attempt":true,"setup":"план вместо действия","target":"разрыв",'
        '"scene_type":"contradiction","relation":"pile_on","forbidden_moves":[],'
        '"candidates":['
        '{"text":"планируете не приехать, всё сходится","mechanism":"logic","callback_key":""},'
        '{"text":"поездка уже почти обсудилась","mechanism":"status","callback_key":""},'
        '{"text":"главное не тревожить план поездкой","mechanism":"image","callback_key":""},'
        '{"text":"сомнения можно было не будить","mechanism":"understatement","callback_key":""}]}'
    )
    critic = '{"winner_index":0,"score":93,"reason_codes":["local"]}'
    saver = patch.object(runtime, "save_memory")
    with (
        patch.object(runtime, "call_openai_metered", AsyncMock(side_effect=[(writer, 100), (critic, 20)])),
        patch.object(runtime, "send_reply_with_style", AsyncMock(side_effect=RuntimeError("telegram down"))),
        saver as save_memory,
        patch.object(runtime.random, "random", return_value=0.0),
    ):
        sent = asyncio.run(runtime._maybe_send_adaptive_snipe(update, context, memory))

    assert sent is False
    save_memory.assert_called()
    chat = runtime.get_chat_mem(memory, 779)
    layers = ensure_humor_schema(chat)
    assert runtime.background_tokens_used_today(chat) == 120
    assert layers["humor_decisions_v2"][-1]["sent"] is False
    assert layers["humor_decisions_v2"][-1]["reason_codes"] == ["send_error"]


def test_runtime_heart_add_and_remove_updates_exact_bot_scene() -> None:
    memory = runtime.default_memory()
    chat = runtime.get_chat_mem(memory, 777)
    chat["history"] = [{"message_id": 10, "user_id": 1, "name": "а", "text": "setup"}]
    scene = record_bot_output(
        chat,
        message_id=11,
        text="короткая добивка",
        plan={"scene_type": "banter", "mechanism": "logic", "context": list(chat["history"])},
        output_kind="ambient",
    )
    assert scene
    heart = SimpleNamespace(emoji="❤️")
    user = SimpleNamespace(id=5)

    async def react(old: list, new: list) -> None:
        reaction = SimpleNamespace(
            chat=SimpleNamespace(id=777),
            message_id=11,
            user=user,
            old_reaction=old,
            new_reaction=new,
        )
        update = SimpleNamespace(message_reaction=reaction)
        with (
            patch.object(runtime, "load_memory", return_value=memory),
            patch.object(runtime, "save_memory"),
            patch.object(runtime, "_load_funny_scan_state", return_value={}),
            patch.object(runtime, "_save_funny_scan_state"),
            patch.object(runtime, "apply_reaction_delta"),
        ):
            await runtime.reaction_handler(update, SimpleNamespace())

    asyncio.run(react([], [heart]))
    assert [(item["source"], item["user_id"]) for item in scene["feedback"]] == [("heart", 5)]

    asyncio.run(react([heart], []))
    assert scene["feedback"] == []


def test_runtime_heart_on_human_message_learns_chat_taste() -> None:
    memory = runtime.default_memory()
    chat = runtime.get_chat_mem(memory, 777)
    chat["history"] = [
        {"message_id": 20, "user_id": 1, "name": "а", "text": "ну мы же планируем", "is_bot": False},
        {"message_id": 21, "user_id": 2, "name": "б", "text": "планируете не приехать", "is_bot": False},
    ]
    reaction = SimpleNamespace(
        chat=SimpleNamespace(id=777),
        message_id=21,
        user=SimpleNamespace(id=9),
        old_reaction=[],
        new_reaction=[SimpleNamespace(emoji="❤️")],
    )
    with (
        patch.object(runtime, "load_memory", return_value=memory),
        patch.object(runtime, "save_memory"),
        patch.object(runtime, "_load_funny_scan_state", return_value={}),
        patch.object(runtime, "_save_funny_scan_state"),
        patch.object(runtime, "apply_reaction_delta"),
    ):
        asyncio.run(runtime.reaction_handler(SimpleNamespace(message_reaction=reaction), SimpleNamespace()))

    scene = next(item for item in ensure_humor_schema(chat)["humor_scenes_v2"] if item["output_message_id"] == 21)
    assert scene["source"] == "human_observed"
    assert scene["feedback"][0]["source"] == "heart"
    assert scene["feedback"][0]["user_id"] == 9
