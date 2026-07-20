import asyncio
import copy
import os
import warnings
from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

os.environ.setdefault("TELEGRAM_BOT_TOKEN", "test-token")
os.environ.setdefault("OPENAI_API_KEY", "test-api-key")

from timur_bot.services import bot_logic as runtime


def test_yaml_persona_is_canonical_unless_admin_set_real_override() -> None:
    memory = runtime.default_memory()
    memory["config"] = {"system_prompt": runtime.DEFAULT_SYSTEM_PROMPT}

    assert runtime.get_system_prompt(memory) == runtime.DEFAULT_SYSTEM_PROMPT
    assert memory["config"]["system_prompt_override"] == ""
    assert "system_prompt" not in memory["config"]

    custom = {"config": {"system_prompt": "мой ручной промпт"}}
    assert runtime.get_system_prompt(custom) == "мой ручной промпт"
    assert custom["config"]["system_prompt_override"] == "мой ручной промпт"


def test_direct_model_question_prompt_contains_truthful_runtime_model() -> None:
    memory = runtime.default_memory()
    message = SimpleNamespace(
        chat_id=991,
        text="на каком ии тимур работает?",
        caption=None,
        from_user=SimpleNamespace(id=7, first_name="кадыр", username=None),
    )

    messages = runtime.build_chat_messages(memory, message, humor_plan=runtime.build_humor_plan(memory, message))

    assert "честный технический ответ — deepseek v4 flash" in messages[0]["content"]
    assert len(messages[0]["content"]) < 4_500


def test_persona_knows_chat_gooning_slang() -> None:
    prompt = runtime.DEFAULT_SYSTEM_PROMPT.lower()

    assert "goon/gooning" in prompt
    assert "гунить" in prompt
    assert "гуннить" in prompt
    assert "длительную дрочку" in prompt


def test_saved_v1_adaptive_defaults_migrate_to_v6_runtime_values() -> None:
    memory = runtime.default_memory()
    memory["config"]["adaptive_humor"] = {
        "participation_rate": 0.30,
        "min_human_messages_between_replies": 2,
        "min_human_messages_between_checks": 5,
        "reply_timeout_seconds": 6,
        "snipe_cooldown_minutes": 30,
        "min_human_messages": 12,
        "candidate_threshold": 91,
        "opportunity_threshold": 85,
        "candidate_count": 3,
    }

    settings = runtime._adaptive_humor_settings(memory)

    assert settings["schema_version"] == 6
    assert settings["participation_rate"] == 0.225
    assert settings["reply_timeout_seconds"] == 10
    assert settings["snipe_cooldown_minutes"] == 10
    assert settings["min_human_messages"] == 3
    assert settings["candidate_threshold"] == 91
    assert settings["legacy_v1_settings"] == {"opportunity_threshold": 85, "candidate_count": 3}


def test_saved_v2_default_reply_timeout_migrates_to_ten_seconds() -> None:
    memory = runtime.default_memory()
    memory["config"]["adaptive_humor"] = {
        "schema_version": 2,
        "reply_timeout_seconds": 3,
    }

    settings = runtime._adaptive_humor_settings(memory)

    assert settings["schema_version"] == 6
    assert settings["reply_timeout_seconds"] == 10


def test_saved_v3_default_participation_rate_is_halved_once() -> None:
    memory = runtime.default_memory()
    memory["config"]["adaptive_humor"] = {
        "schema_version": 3,
        "participation_rate": 0.45,
    }

    settings = runtime._adaptive_humor_settings(memory)

    assert settings["schema_version"] == 6
    assert settings["participation_rate"] == 0.225
    assert runtime._random_photo_reply_chance(memory) == runtime.PHOTO_RANDOM_REPLY_CHANCE / 2


def test_saved_v4_default_reply_lengths_migrate_to_short_limits() -> None:
    memory = runtime.default_memory()
    memory["config"]["adaptive_humor"] = {
        "schema_version": 4,
        "ambient_reply_max_chars": 60,
        "direct_reply_max_chars": 120,
    }

    settings = runtime._adaptive_humor_settings(memory)

    assert settings["schema_version"] == 6
    assert settings["ambient_reply_max_chars"] == 45
    assert settings["direct_reply_max_chars"] == 70


def test_saved_v5_timeout_migrates_above_transport_deadline() -> None:
    memory = runtime.default_memory()
    memory["config"]["adaptive_humor"] = {
        "schema_version": 5,
        "reply_timeout_seconds": 6,
    }

    settings = runtime._adaptive_humor_settings(memory)

    assert settings["schema_version"] == 6
    assert settings["reply_timeout_seconds"] == 10
    assert settings["reply_timeout_seconds"] > runtime.TEXT_TRANSPORT_TIMEOUT_SECONDS


def test_direct_reply_context_contains_timurs_previous_message_and_reply_edge() -> None:
    memory = runtime.default_memory()
    chat = runtime.get_chat_mem(memory, 992)
    chat["history"] = [{"message_id": 1, "user_id": 7, "name": "а", "text": "можно завтра?"}]
    runtime.record_bot_output(
        chat,
        message_id=2,
        text="можно и завтра",
        plan={"mode": "direct", "context": list(chat["history"])},
        output_kind="direct",
        trigger_message_id=1,
        reply_to_message_id=1,
    )
    chat["history"].append(
        {"message_id": 3, "reply_to_message_id": 2, "user_id": 7, "name": "а", "text": "а почему"}
    )
    message = SimpleNamespace(
        chat_id=992,
        message_id=3,
        text="а почему",
        caption=None,
        from_user=SimpleNamespace(id=7, first_name="а", username=None),
    )

    messages = runtime.build_chat_messages(memory, message, humor_plan=runtime.build_humor_plan(memory, message))

    assert "[bot] тимур: можно и завтра" in messages[0]["content"] or "bot] тимур: можно и завтра" in messages[0]["content"]
    assert "reply=#2" in messages[0]["content"]


def test_get_chat_mem_has_memory_layers() -> None:
    memory = runtime.default_memory()
    chat = runtime.get_chat_mem(memory, 1)
    layers = chat.get("memory_layers", {})
    assert isinstance(layers.get("recent_messages", []), list)
    assert isinstance(layers.get("recent_facts", []), list)
    assert isinstance(layers.get("long_facts", []), list)
    assert isinstance(layers.get("rolling_memory", {}), dict)


def test_runtime_premium_chat_gets_all_plus_features_independently_of_scan_role() -> None:
    memory = runtime.default_memory()
    memory["config"]["funny_scan"]["main_chat_id"] = 0

    with (
        patch.object(runtime, "PREMIUM_CHAT_IDS", {-5001}),
        patch.object(runtime.billing, "effective_features") as billing_features,
    ):
        features = runtime.get_chat_features(-5001, memory)

    billing_features.assert_not_called()
    assert features["tier"] == "group_plus"
    assert features["memory_depth"] == "full"
    assert features["voice"] is True
    assert features["friend_dossiers"] is True
    assert features["episodic_memory"] is True
    assert features["watermark"] is False
    assert features["max_daily_replies"] == 3000


def test_context_prefers_recent_messages_layer() -> None:
    memory = runtime.default_memory()
    chat = runtime.get_chat_mem(memory, 1)
    chat["history"] = [{"text": "старое", "name": "x", "user_id": 1, "ts": "2024-01-01T00:00:00"}]
    chat["memory_layers"]["recent_messages"] = [
        {"text": "новое", "name": "y", "user_id": 2, "ts": "2024-01-02T00:00:00"}
    ]

    selected = runtime.select_chat_history_for_context(memory, 1)
    assert len(selected) == 1
    assert selected[0]["text"] == "новое"


def test_old_memories_use_long_facts_not_log_random() -> None:
    memory = runtime.default_memory()
    chat = runtime.get_chat_mem(memory, 1)
    chat["log"] = [{"text": "шум"} for _ in range(50)]
    chat["memory_layers"]["long_facts"] = [
        {"text": "A: важный старый факт", "strength": 5.0},
        {"text": "B: менее важный", "strength": 1.0},
    ]

    lines = runtime.select_old_random_memories(memory, 1)
    assert lines
    assert lines[0] in {"A: важный старый факт", "B: менее важный"}


def test_old_memories_penalize_recently_overused_fact() -> None:
    memory = runtime.default_memory()
    chat = runtime.get_chat_mem(memory, 1)
    chat["memory_layers"]["long_facts"] = [
        {"text": "A: заезженный факт", "strength": 5.0},
        {"text": "B: свежий факт", "strength": 2.0},
    ]
    chat["memory_layers"]["long_fact_usage"] = {
        runtime.normalize_token("A: заезженный факт")[:120]: {
            "last_used_ts": datetime.utcnow().isoformat(),
            "count": 12,
        }
    }

    with patch.object(runtime.random, "choices", side_effect=lambda population, weights, k: [population[0]]):
        lines = runtime.select_old_random_memories(memory, 1)

    assert lines == ["B: свежий факт"]


def test_default_memory_contains_life_config() -> None:
    memory = runtime.default_memory()
    life = memory["config"]["life"]
    assert life["enabled"] is True
    assert life["daily_target"] == 1
    assert life["timezone"] == "Europe/Moscow"
    assert isinstance(life.get("lore_arcs"), list)
    assert int(life.get("last_lore_arc_id", 0)) == 0

    funny_scan = memory["config"]["funny_scan"]
    assert funny_scan["enabled"] is False
    assert funny_scan["intensity"] == "balanced"
    assert funny_scan["scan_period_hours"] == 24

    mood = memory["config"]["mood"]
    assert mood["enabled"] is True
    assert "valence" in mood
    assert "energy" in mood
    assert "guard_level" in mood


def test_sync_mood_state_can_roll_event_when_due() -> None:
    memory = runtime.default_memory()
    mood = memory["config"]["mood"]
    mood["next_event_after_ts"] = "2000-01-01T00:00:00"

    changed, mood_state, rolled = runtime._sync_mood_state(memory, allow_event_roll=True)

    assert changed is True
    assert rolled is not None
    assert mood_state["current_event"]
    assert 0 <= int(mood_state["current_event"]["privacy_level"]) <= 3


def test_apply_message_mood_impact_reacts_to_aggression() -> None:
    memory = runtime.default_memory()
    before_v = float(memory["config"]["mood"]["valence"])
    before_e = float(memory["config"]["mood"]["energy"])

    class DummyMessage:
        chat_id = 1
        text = "ты дебил и чмо"
        caption = None

    changed = runtime._apply_message_mood_impact(memory, DummyMessage())
    assert changed is True
    after_v = float(memory["config"]["mood"]["valence"])
    after_e = float(memory["config"]["mood"]["energy"])
    assert after_v < before_v
    assert after_e >= before_e


def test_toxicity_fallback_uses_persona_default() -> None:
    memory = runtime.default_memory()
    memory["config"].pop("toxicity_level", None)

    heat = runtime.get_toxicity_level(memory)

    assert heat == runtime.APP_CONFIG.default_toxicity_level


def test_reply_guardrail_softens_toxic_personal_attack() -> None:
    toxic = "ты дебил если честно"

    safe = runtime.enforce_reply_guardrails(toxic)

    assert safe == "ок без наездов давай по сути"


def test_effective_toxicity_caps_default_and_chill_modes() -> None:
    memory = runtime.default_memory()
    memory["config"]["toxicity_level"] = 90

    memory["config"]["active_mode"] = "default"
    assert runtime.get_effective_toxicity_level(memory) == 20

    memory["config"]["active_mode"] = "chill"
    assert runtime.get_effective_toxicity_level(memory) == 8


def test_looks_like_memory_request_detection() -> None:
    assert runtime.looks_like_memory_request("тимур высри чето из памяти")
    assert runtime.looks_like_memory_request("вспомни старый прикол")
    assert not runtime.looks_like_memory_request("просто ответь по теме")


def test_story_request_detection() -> None:
    assert runtime._looks_like_story_request("тимур расскажи историю")
    assert runtime._looks_like_story_request("расскажи че было")
    assert not runtime._looks_like_story_request("давай по делу")


def test_generate_daily_slots_uses_daily_target_outside_quiet_hours() -> None:
    life = runtime._default_life_config()
    life["daily_target"] = 1
    slots = runtime._generate_daily_slots(life, day_seed=20260424)
    assert len(slots) == 1
    quiet_start = runtime._parse_hhmm_to_minute("00:00", 0)
    quiet_end = runtime._parse_hhmm_to_minute("10:00", 600)
    assert all(not runtime._is_quiet_minute(s, quiet_start, quiet_end) for s in slots)


def test_ordinary_participation_is_left_to_the_quality_filter_after_gap() -> None:
    memory = runtime.default_memory()
    memory["config"]["adaptive_humor"]["participation_rate"] = 0.3
    chat = runtime.get_chat_mem(memory, 55)
    runtime.note_human_message(chat)
    runtime.note_human_message(chat)

    class DummyUser:
        id = 7

    class DummyMessage:
        chat_id = 55
        text = "вот это у него план"
        caption = None
        from_user = DummyUser()
        reply_to_message = None

    decision = runtime.should_reply_decision(memory, DummyMessage(), bot_id=999)

    assert decision.should_reply is False
    assert "качественному фильтру" in decision.reason


def _rival_message(
    *,
    text: str = "у меня есть план",
    username: str = "sglypa_tg_bot",
    is_bot: bool = True,
    reply_to_user_id: int | None = None,
) -> SimpleNamespace:
    reply_to_message = None
    if reply_to_user_id is not None:
        reply_to_message = SimpleNamespace(
            message_id=90,
            from_user=SimpleNamespace(id=reply_to_user_id, is_bot=True),
        )
    return SimpleNamespace(
        chat_id=58,
        message_id=100,
        text=text,
        caption=None,
        from_user=SimpleNamespace(
            id=77,
            first_name="сглыпа",
            username=username,
            is_bot=is_bot,
        ),
        reply_to_message=reply_to_message,
    )


def test_bot_rival_requires_exact_username_and_bot_flag() -> None:
    assert runtime._bot_rival_settings(_rival_message()) is not None
    assert runtime._bot_rival_settings(_rival_message(username="SGLYPA_TG_BOT")) is not None
    assert runtime._bot_rival_settings(_rival_message(username="other_bot")) is None
    assert runtime._bot_rival_settings(_rival_message(is_bot=False)) is None


def test_bot_rival_reply_uses_configured_probability_boundary() -> None:
    memory = runtime.default_memory()
    message = _rival_message(text="тимур я тут главный")

    with patch.object(runtime.random, "random", return_value=0.149):
        accepted = runtime.should_reply_decision(memory, message, bot_id=999)
    with patch.object(runtime.random, "random", return_value=0.15):
        rejected = runtime.should_reply_decision(memory, message, bot_id=999)

    assert accepted.should_reply is True
    assert accepted.threshold == 0.15
    assert accepted.allow_ambient_fallback is False
    assert rejected.should_reply is False
    assert rejected.allow_ambient_fallback is False


def test_bot_rival_reply_to_timur_is_always_ignored() -> None:
    memory = runtime.default_memory()
    message = _rival_message(text="тимур отвечаю", reply_to_user_id=999)

    with patch.object(runtime.random, "random") as roll:
        decision = runtime.should_reply_decision(memory, message, bot_id=999)

    assert decision.should_reply is False
    assert decision.allow_ambient_fallback is False
    roll.assert_not_called()


def test_bot_rival_prompt_is_scoped_to_current_rival() -> None:
    memory = runtime.default_memory()

    rival_prompt = runtime.build_chat_messages(memory, _rival_message())[0]["content"]
    human_prompt = runtime.build_chat_messages(memory, _rival_message(is_bot=False))[0]["content"]

    assert "отношение к текущему собеседнику" in rival_prompt
    assert "ты считаешь себя заметно умнее сглыпы" in rival_prompt
    assert "отношение к текущему собеседнику" not in human_prompt


def test_text_handler_does_not_run_ambient_snipe_after_rival_abstention() -> None:
    class CachedBot:
        id = 999

    message = _rival_message()
    update = SimpleNamespace(effective_message=message)
    context = SimpleNamespace(bot=CachedBot())
    memory = runtime.default_memory()
    decision = runtime.ReplyDecision(
        False,
        "rival roll missed",
        allow_ambient_fallback=False,
    )

    with (
        patch.object(runtime, "load_memory", return_value=memory),
        patch.object(runtime, "_handle_admin_pending_text", new=AsyncMock(return_value=False)),
        patch.object(runtime, "_handle_text_feedback", new=AsyncMock(return_value=False)),
        patch.object(runtime, "update_memory_with_message"),
        patch.object(runtime, "_observe_chat_humor"),
        patch.object(runtime, "_apply_message_mood_impact", return_value=False),
        patch.object(runtime, "_sync_mood_state"),
        patch.object(runtime, "_handle_mood_probe", new=AsyncMock(return_value=False)),
        patch.object(runtime, "should_reply_decision", return_value=decision),
        patch.object(runtime, "_maybe_send_adaptive_snipe", new=AsyncMock(return_value=False)) as snipe,
        patch.object(runtime, "save_memory"),
    ):
        asyncio.run(runtime.text_handler(update, context))

    snipe.assert_not_awaited()


def test_text_handler_uses_cached_bot_id_without_get_me_request() -> None:
    class CachedBot:
        id = 999

        async def get_me(self):
            raise AssertionError("text handler must not call Telegram get_me")

    message = SimpleNamespace(
        chat_id=57,
        message_id=1001,
        text="обычная реплика",
        caption=None,
        from_user=SimpleNamespace(id=7, first_name="а", username=None, is_bot=False),
        sender_chat=None,
        reply_to_message=None,
    )
    update = SimpleNamespace(effective_message=message)
    context = SimpleNamespace(bot=CachedBot())
    memory = runtime.default_memory()
    decision = runtime.ReplyDecision(False, "обычный случай")

    with (
        patch.object(runtime, "load_memory", return_value=memory),
        patch.object(runtime, "_handle_admin_pending_text", new=AsyncMock(return_value=False)),
        patch.object(runtime, "_handle_text_feedback", new=AsyncMock(return_value=False)),
        patch.object(runtime, "update_memory_with_message"),
        patch.object(runtime, "_observe_chat_humor"),
        patch.object(runtime, "_apply_message_mood_impact", return_value=False),
        patch.object(runtime, "_sync_mood_state"),
        patch.object(runtime, "_handle_mood_probe", new=AsyncMock(return_value=False)),
        patch.object(runtime, "should_reply_decision", return_value=decision) as should_reply,
        patch.object(runtime, "_maybe_send_adaptive_snipe", new=AsyncMock(return_value=False)),
        patch.object(runtime, "save_memory"),
    ):
        asyncio.run(runtime.text_handler(update, context))

    assert should_reply.call_args.args[2] == 999


def test_text_reply_to_photo_uses_vision_instead_of_text_model() -> None:
    class DownloadedFile:
        async def download_as_bytearray(self) -> bytearray:
            return bytearray(b"image-bytes")

    class CachedBot:
        id = 999
        get_file = AsyncMock(return_value=DownloadedFile())

    replied_photo = SimpleNamespace(photo=[SimpleNamespace(file_id="photo-file")])
    message = SimpleNamespace(
        chat_id=59,
        message_id=1003,
        text="тимур глянь",
        caption=None,
        from_user=SimpleNamespace(id=7, first_name="а", username=None, is_bot=False),
        sender_chat=None,
        reply_to_message=replied_photo,
    )
    update = SimpleNamespace(effective_message=message)
    context = SimpleNamespace(bot=CachedBot())
    memory = runtime.default_memory()

    async def run_now(_context, _chat_id, task, **_kwargs):
        return await task

    with (
        patch.object(runtime, "load_memory", return_value=memory),
        patch.object(runtime, "_handle_admin_pending_text", new=AsyncMock(return_value=False)),
        patch.object(runtime, "_handle_text_feedback", new=AsyncMock(return_value=False)),
        patch.object(runtime, "update_memory_with_message"),
        patch.object(runtime, "_observe_chat_humor"),
        patch.object(runtime, "_apply_message_mood_impact", return_value=False),
        patch.object(runtime, "_sync_mood_state"),
        patch.object(runtime, "_handle_mood_probe", new=AsyncMock(return_value=False)),
        patch.object(runtime, "should_reply_decision", return_value=runtime.ReplyDecision(True, "прямое обращение")),
        patch.object(runtime, "can_use_vision", return_value=True),
        patch.object(runtime, "increase_vision_counters") as increase_counters,
        patch.object(runtime, "call_openai_vision", new=AsyncMock(return_value="глянул")) as call_vision,
        patch.object(runtime, "call_openai_text", new=AsyncMock(return_value="не должен вызываться")) as call_text,
        patch.object(runtime, "_run_with_typing", side_effect=run_now),
        patch.object(runtime, "send_reply_with_style", new=AsyncMock(return_value=True)) as send_reply,
        patch.object(runtime, "save_memory"),
    ):
        asyncio.run(runtime.text_handler(update, context))

    context.bot.get_file.assert_awaited_once_with("photo-file")
    increase_counters.assert_called_once_with(memory, 59, 7)
    call_vision.assert_awaited_once()
    call_text.assert_not_awaited()
    send_reply.assert_awaited_once_with(update, context, memory, "глянул", humor_plan=None)


def test_photo_handler_uses_cached_bot_id_without_get_me_request() -> None:
    class CachedBot:
        id = 999

        async def get_me(self):
            raise AssertionError("photo handler must not call Telegram get_me")

    message = SimpleNamespace(
        chat_id=58,
        message_id=1002,
        text=None,
        caption="",
        photo=[],
        from_user=SimpleNamespace(id=7, first_name="а", username=None, is_bot=False),
        sender_chat=None,
        reply_to_message=None,
    )
    update = SimpleNamespace(effective_message=message)
    context = SimpleNamespace(bot=CachedBot())
    memory = runtime.default_memory()

    with (
        patch.object(runtime, "load_memory", return_value=memory),
        patch.object(runtime, "update_memory_with_message"),
        patch.object(runtime, "_apply_message_mood_impact", return_value=False),
        patch.object(runtime, "_sync_mood_state"),
        patch.object(runtime.random, "random", return_value=1.0),
        patch.object(runtime, "save_memory"),
    ):
        asyncio.run(runtime.photo_handler(update, context))


def test_open_followup_does_not_hijack_reply_to_another_human() -> None:
    memory = runtime.default_memory()
    chat = runtime.get_chat_mem(memory, 56)
    runtime.activate_dialogue(chat, initiator_id=7, text="тимур как дела")
    other_human = SimpleNamespace(id=8, is_bot=False)
    message = SimpleNamespace(
        chat_id=56,
        text="я вообще про картошку",
        caption=None,
        from_user=SimpleNamespace(id=7),
        reply_to_message=SimpleNamespace(from_user=other_human),
    )

    decision = runtime.should_reply_decision(memory, message, bot_id=999)

    assert decision.should_reply is False
    assert "другому участнику" in decision.reason
    followup = SimpleNamespace(
        chat_id=56,
        text="у тебя завтра пары?",
        caption=None,
        from_user=SimpleNamespace(id=7),
        reply_to_message=None,
    )
    assert runtime.should_reply_decision(memory, followup, bot_id=999).should_reply is True


def test_processed_event_cache_marks_duplicate() -> None:
    memory = runtime.default_memory()
    chat = runtime.get_chat_mem(memory, 1)
    key = runtime._make_event_key("text", 1, 100)
    assert not runtime._is_processed_event(chat, key)
    runtime._mark_processed_event(chat, key)
    assert runtime._is_processed_event(chat, key)


def test_run_with_typing_sends_chat_action() -> None:
    class DummyBot:
        def __init__(self) -> None:
            self.calls = 0

        async def send_chat_action(self, chat_id: int, action: str) -> None:
            del chat_id, action
            self.calls += 1

    class DummyContext:
        def __init__(self) -> None:
            self.bot = DummyBot()

    async def _task() -> str:
        await asyncio.sleep(0.01)
        return "ok"

    context = DummyContext()
    result = asyncio.run(runtime._run_with_typing(context, 1, _task()))
    assert result == "ok"
    assert context.bot.calls >= 1


def test_run_with_typing_stops_after_timeout() -> None:
    class DummyBot:
        def __init__(self) -> None:
            self.calls = 0

        async def send_chat_action(self, chat_id: int, action: str) -> None:
            del chat_id, action
            self.calls += 1

    class DummyContext:
        def __init__(self) -> None:
            self.bot = DummyBot()

    async def _slow_task() -> str:
        await asyncio.sleep(0.05)
        return "too late"

    context = DummyContext()
    result = asyncio.run(runtime._run_with_typing(context, 1, _slow_task(), timeout_seconds=0.01))

    assert result == ""
    assert context.bot.calls >= 1


def test_run_with_typing_does_not_wait_for_stuck_typing_action() -> None:
    class StuckBot:
        async def send_chat_action(self, chat_id: int, action: str) -> None:
            del chat_id, action
            await asyncio.Event().wait()

    context = type("DummyContext", (), {"bot": StuckBot()})()

    async def _quick_task() -> str:
        return "ok"

    async def _run() -> str:
        return await asyncio.wait_for(
            runtime._run_with_typing(context, 90212, _quick_task(), timeout_seconds=0.1),
            timeout=0.25,
        )

    assert asyncio.run(_run()) == "ok"


def test_run_with_typing_skips_overlapping_generation_in_same_chat() -> None:
    class DummyBot:
        async def send_chat_action(self, chat_id: int, action: str) -> None:
            del chat_id, action

    context = type("DummyContext", (), {"bot": DummyBot()})()

    async def _slow_task() -> str:
        await asyncio.sleep(0.05)
        return "first"

    async def _second_task() -> str:
        return "second"

    async def _run_pair() -> tuple[str, str]:
        first = asyncio.create_task(runtime._run_with_typing(context, 90210, _slow_task(), timeout_seconds=1))
        await asyncio.sleep(0.005)
        second = await runtime._run_with_typing(context, 90210, _second_task(), timeout_seconds=1)
        return await first, second

    assert asyncio.run(_run_pair()) == ("first", "")


def test_background_interjection_does_not_show_typing() -> None:
    class DummyBot:
        def __init__(self) -> None:
            self.calls = 0

        async def send_chat_action(self, chat_id: int, action: str) -> None:
            del chat_id, action
            self.calls += 1

    context = type("DummyContext", (), {"bot": DummyBot()})()

    async def _quick_task() -> str:
        return "ok"

    assert asyncio.run(runtime._run_with_typing(context, 90211, _quick_task(), show_typing=False)) == "ok"
    assert context.bot.calls == 0


def test_store_bot_claim_memory_writes_fact_graph_and_long_fact() -> None:
    memory = runtime.default_memory()
    chat_mem = runtime.get_chat_mem(memory, 77)

    class DummyMessage:
        chat_id = 77

        @staticmethod
        def text() -> str:
            return ""

    message = DummyMessage()
    message.text = "тимур какая у тебя фамилия"
    message.caption = None

    runtime._store_bot_claim_memory(memory, message, "ахметов")

    graph = chat_mem["memory_layers"]["fact_graph"]
    assert graph["facts"]
    assert graph["facts"][0]["attribute"] == "surname"
    assert graph["facts"][0]["value"] == "ахметов"
    assert any("surname" in str(item.get("text", "")) for item in chat_mem["memory_layers"]["long_facts"])


def test_apply_lore_payload_appends_arc_and_persists_facts(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(runtime, "MEMORY_PATH", tmp_path / "memory.json")
    memory = runtime.default_memory()
    cfg = memory["config"]
    mood = runtime._ensure_mood_config(cfg)
    mood["current_event"] = {
        "id": 7,
        "key": "transport_fail",
        "privacy_level": 1,
        "public_text": "транспорт снова устроил цирк",
        "private_text": "опоздал на пару из-за трамвая и выслушал препода",
    }
    life = runtime._ensure_life_config(cfg)
    arc = runtime._get_or_create_active_lore_arc(life, mood["current_event"], datetime.utcnow())

    beat = runtime._apply_lore_payload_to_arc(
        memory=memory,
        chat_id=1,
        arc=arc,
        event=mood["current_event"],
        payload={
            "phase": "build",
            "public_story": "сегодня на входе в универ турникет завис и собрал очередь",
            "private_story": "я полез перепрыгивать турникет и почти уронил рюкзак с тетрадями",
            "hook_question": "у вас тоже турникеты живут своей жизнью",
            "facts": [
                {"attribute": "habit", "value": "в стрессовые дни перескакивает через турникет", "confidence": 0.8, "privacy": 1}
            ],
        },
        proactive=False,
    )

    assert beat["output_text"]
    assert arc["beats"]
    stored_arc = next(item for item in life["lore_arcs"] if item["id"] == arc["id"])
    assert stored_arc is arc
    assert stored_arc["beats"][-1]["id"] == beat["id"]
    chat_mem = runtime.get_chat_mem(memory, 1)
    graph = chat_mem["memory_layers"]["fact_graph"]
    assert any(str(f.get("source")) == "lore_arc" for f in graph.get("facts", []))
    assert any(str(f.get("attribute")) == "habit" for f in graph.get("facts", []))
    assert runtime.save_memory(memory) is True
    reloaded_arc = next(
        item
        for item in runtime.load_memory()["config"]["life"]["lore_arcs"]
        if item["id"] == arc["id"]
    )
    assert reloaded_arc["beats"][-1]["id"] == beat["id"]


def test_lore_fallback_uses_event_hint() -> None:
    memory = runtime.default_memory()
    event = {
        "id": 1,
        "key": "night_no_sleep",
        "privacy_level": 2,
        "public_text": "уникальный намек про ночь без сна",
        "private_text": "сложная личная причина бессонницы",
    }

    text = runtime._fallback_lore_story_text(memory, 1, event, proactive=True)

    assert "уникальный намек" in text or "сложная личная причина" in text


def test_get_or_create_lore_arc_bootstraps_core_arc() -> None:
    memory = runtime.default_memory()
    life = runtime._ensure_life_config(memory["config"])
    arc = runtime._get_or_create_active_lore_arc(life, {}, datetime.utcnow())

    assert arc["arc_kind"] == "core"
    assert "мехмат" in str(arc["title"]).lower()


def test_lore_private_event_can_use_cover_story() -> None:
    memory = runtime.default_memory()
    cfg = memory["config"]
    mood = runtime._ensure_mood_config(cfg)
    mood["current_event"] = {
        "id": 9,
        "key": "friend_conflict",
        "privacy_level": 3,
        "public_text": "день нервный",
        "private_text": "серьезный конфликт с близким",
    }
    life = runtime._ensure_life_config(cfg)
    arc = runtime._get_or_create_active_lore_arc(life, mood["current_event"], datetime.utcnow())

    beat = runtime._apply_lore_payload_to_arc(
        memory=memory,
        chat_id=1,
        arc=arc,
        event=mood["current_event"],
        payload={
            "phase": "build",
            "public_story": "сегодня без деталей",
            "private_story": "в личке разнос",
            "cover_story": "забыл кошелек дома и бегал обратно",
            "facts": [],
        },
        proactive=False,
    )

    assert beat["output_text"]


def test_proactive_story_merge_preserves_newer_feedback_and_dialogue_state() -> None:
    base = runtime.default_memory()
    base_mood = copy.deepcopy(base["config"]["mood"])
    generated = copy.deepcopy(base)
    generated_life = runtime._ensure_life_config(generated["config"])
    generated_life["slots_date"] = "2026-07-16"
    generated_life["daily_slots"] = [780]
    generated_life["sent_slots"] = [780]
    generated_life["last_emit_ts"] = "2026-07-16T13:00:00"
    generated_life["last_emit_chat_id"] = 1
    generated_life["chat_last_emit"] = {"1": "2026-07-16T13:00:00"}
    generated_life["last_lore_arc_id"] = 1
    generated_life["lore_arcs"] = [
        {
            "id": 1,
            "title": "сложный кусок",
            "summary": "закрыл учебную задачу",
            "status": "active",
            "arc_kind": "core",
            "parent_arc_id": 0,
            "seed_event_id": 0,
            "seed_event_key": "",
            "base_privacy": 1,
            "last_beat_id": 1,
            "created_ts": "2026-07-16T12:50:00",
            "updated_ts": "2026-07-16T13:00:00",
            "beats": [
                {
                    "id": 1,
                    "phase": "payoff",
                    "public_story": "сдал сложный кусок и щас легче дышать",
                    "private_story": "сдал сложный кусок и щас легче дышать",
                    "facts": [
                        {
                            "attribute": "study_state",
                            "value": "закрыл сложный учебный кусок",
                            "privacy": 1,
                            "confidence": 0.8,
                        }
                    ],
                    "output_text": "сдал сложный кусок и щас легче дышать\nу вас как с этим обычно",
                }
            ],
            "facts": [],
        }
    ]
    text = "сдал сложный кусок и щас легче дышать\nу вас как с этим обычно"
    runtime._append_story_log(generated, text, source="proactive", chat_id=1)

    latest = copy.deepcopy(base)
    latest["config"]["life"]["enabled"] = False
    latest["config"]["mood"]["valence"] = 77.0
    runtime._append_story_log(latest, "параллельная история", source="on_demand", chat_id=1)
    latest_chat = runtime.get_chat_mem(latest, 1)
    latest_layers = runtime.ensure_humor_schema(latest_chat)
    latest_layers["humor_scenes_v2"].append(
        {
            "id": "fresh-heart-scene",
            "output_message_id": 55,
            "feedback": [{"rating": "funny", "source": "heart"}],
        }
    )
    latest_layers["adaptive_humor"] = {
        "pending_followups": {"42": {"last_activity_ts": "2026-07-16T12:59:59"}}
    }

    merged = runtime._merge_proactive_story_state(
        latest,
        generated,
        base_life=copy.deepcopy(base["config"]["life"]),
        base_mood=base_mood,
        chat_id=1,
        sent_message_id=99,
        text=text,
    )

    merged_chat = runtime.get_chat_mem(merged, 1)
    merged_layers = runtime.ensure_humor_schema(merged_chat)
    assert any(scene.get("id") == "fresh-heart-scene" for scene in merged_layers["humor_scenes_v2"])
    assert "42" in merged_layers["adaptive_humor"]["pending_followups"]
    assert merged["config"]["mood"]["valence"] == 77.0
    assert merged["config"]["life"]["enabled"] is False
    assert merged["config"]["life"]["sent_slots"] == [780]
    assert {item.get("text") for item in merged["config"]["life"]["story_log"]} == {
        "параллельная история",
        text,
    }
    assert any(item.get("message_id") == 99 for item in merged_chat["history"])
    assert not any(scene.get("output_message_id") == 99 for scene in merged_layers["humor_scenes_v2"])
    assert any(
        fact.get("source") == "lore_arc"
        for fact in merged_chat["memory_layers"]["fact_graph"]["facts"]
    )


def test_concurrent_memory_saves_merge_heart_daily_and_direct_state(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(runtime, "MEMORY_PATH", tmp_path / "memory.json")
    initial = runtime.default_memory()
    initial_chat = runtime.get_chat_mem(initial, 1)
    runtime.record_bot_output(
        initial_chat,
        message_id=10,
        text="первая шутка",
        plan={"mode": "ambient", "action": "JOKE", "context": []},
        output_kind="ambient",
    )
    runtime.save_memory(initial)

    heart_writer = runtime.load_memory()
    daily_writer = runtime.load_memory()
    direct_writer = runtime.load_memory()

    assert runtime.set_heart_feedback(
        runtime.get_chat_mem(heart_writer, 1),
        message_id=10,
        user_id=7,
        active=True,
    )
    runtime.save_memory(heart_writer)

    daily_life = runtime._ensure_life_config(daily_writer["config"])
    daily_life["slots_date"] = "2026-07-16"
    daily_life["daily_slots"] = [780]
    daily_life["sent_slots"] = [780]
    daily_life["last_emit_ts"] = "2026-07-16T13:00:00"
    runtime._append_story_log(daily_writer, "дневная история", source="proactive", chat_id=1)
    runtime.record_bot_output(
        runtime.get_chat_mem(daily_writer, 1),
        message_id=12,
        text="дневная история\nу вас как с этим обычно",
        plan=None,
        output_kind="daily_lore",
    )
    runtime.save_memory(daily_writer)

    direct_chat = runtime.get_chat_mem(direct_writer, 1)
    runtime.record_bot_output(
        direct_chat,
        message_id=11,
        text="короткий ответ",
        plan={"mode": "direct", "action": "ANSWER", "context": []},
        output_kind="direct",
    )
    runtime.activate_dialogue(direct_chat, initiator_id=42, text="а почему")
    runtime.save_memory(direct_writer)

    final = runtime.load_memory()
    final_chat = runtime.get_chat_mem(final, 1)
    heart_scene = runtime.find_humor_scene(final_chat, 10)
    assert heart_scene is not None
    assert any(item.get("source") == "heart" for item in heart_scene["feedback"])
    assert runtime.find_humor_scene(final_chat, 11) is not None
    assert "42" in final_chat["memory_layers"]["adaptive_humor"]["pending_followups"]
    assert final["config"]["life"]["sent_slots"] == [780]
    assert any(item.get("text") == "дневная история" for item in final["config"]["life"]["story_log"])
    assert [item.get("message_id") for item in final_chat["history"]] == [10, 11, 12]


def test_voice_budget_check_and_reservation_are_atomic(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(runtime, "MEMORY_PATH", tmp_path / "memory.json")
    monkeypatch.setattr(runtime, "GLOBAL_DAILY_VOICE_LIMIT", 3)
    monkeypatch.setattr(runtime, "CHAT_DAILY_VOICE_LIMIT", 3)
    monkeypatch.setattr(runtime, "get_chat_features", lambda *_: {})
    monkeypatch.setattr(runtime.feature_gate, "voice_allowed", lambda _: True)
    initial = runtime.default_memory()
    initial["config"]["voice_usage"] = {
        runtime._today_str(): {"global": 2, "chats": {"seed": 2}}
    }
    runtime.save_memory(initial)
    first = runtime.load_memory()
    second = runtime.load_memory()

    assert runtime.reserve_voice_attempt(first, 101) is True
    assert runtime.reserve_voice_attempt(second, 202) is False

    final = runtime.load_memory()
    stats = final["config"]["voice_usage"][runtime._today_str()]
    assert stats["global"] == 3
    assert stats["chats"] == {"seed": 2, "101": 1}


def test_concurrent_followup_refresh_wins_over_stale_deletion(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(runtime, "MEMORY_PATH", tmp_path / "memory.json")
    initial = runtime.default_memory()
    runtime.activate_dialogue(
        runtime.get_chat_mem(initial, 1),
        initiator_id=42,
        text="про поезд",
        now=datetime(2026, 7, 16, 12, 0),
    )
    runtime.save_memory(initial)
    stale_consumer = runtime.load_memory()
    refresher = runtime.load_memory()

    assert runtime.continue_dialogue(
        runtime.get_chat_mem(stale_consumer, 1),
        user_id=42,
        text="а почему",
        window_minutes=10,
        now=datetime(2026, 7, 16, 12, 1),
    )
    runtime.activate_dialogue(
        runtime.get_chat_mem(refresher, 1),
        initiator_id=42,
        text="теперь про автобус",
        now=datetime(2026, 7, 16, 12, 2),
    )
    runtime.save_memory(refresher)
    runtime.save_memory(stale_consumer)

    final_state = runtime.get_chat_mem(runtime.load_memory(), 1)["memory_layers"]["adaptive_humor"]
    assert final_state["pending_followups"]["42"]["last_activity_ts"] == "2026-07-16T12:02:00"


def test_proactive_slot_is_durably_reserved_before_send(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(runtime, "MEMORY_PATH", tmp_path / "memory.json")
    memory = runtime.default_memory()
    life = runtime._ensure_life_config(memory["config"])
    life["slots_date"] = "2026-07-16"
    life["daily_slots"] = [780]
    life["sent_slots"] = []
    runtime.save_memory(memory)
    now_local = datetime(2026, 7, 16, 13, 1)

    assert runtime._reserve_proactive_slot(780, now_local) is True
    assert runtime._reserve_proactive_slot(780, now_local) is False
    assert runtime._release_proactive_slot(780, now_local) is True
    assert runtime._reserve_proactive_slot(780, now_local) is True
    assert runtime.load_memory()["config"]["life"]["sent_slots"] == [780]

    failed_reservation = runtime.load_memory()
    failed_reservation["config"]["life"]["daily_slots"] = [780, 781]
    runtime.save_memory(failed_reservation)
    with patch.object(runtime, "save_memory", return_value=False):
        assert runtime._reserve_proactive_slot(781, now_local) is False


def test_concurrent_new_lore_arcs_keep_uid_references_after_numeric_remap(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(runtime, "MEMORY_PATH", tmp_path / "memory.json")
    initial = runtime.default_memory()
    initial_life = runtime._ensure_life_config(initial["config"])
    parent = runtime._start_new_lore_arc(
        initial_life,
        {},
        datetime(2026, 7, 16, 12, 0),
        title="общая арка",
        arc_kind="core",
    )
    parent_uid = parent["uid"]
    runtime.save_memory(initial)
    first = runtime.load_memory()
    second = runtime.load_memory()

    branch_uids: dict[str, str] = {}
    for memory, title, created_at in (
        (first, "первая ветка", datetime(2026, 7, 16, 13, 0, 0, 1)),
        (second, "вторая ветка", datetime(2026, 7, 16, 13, 0, 0, 2)),
    ):
        life = runtime._ensure_life_config(memory["config"])
        current_parent = next(arc for arc in life["lore_arcs"] if arc["uid"] == parent_uid)
        branch = runtime._start_new_lore_arc(
            life,
            {},
            created_at,
            title=title,
            summary=title,
            arc_kind="branch",
            parent_arc_id=current_parent["id"],
            parent_arc_uid=parent_uid,
        )
        branch_uids[title] = branch["uid"]
        current_parent["beats"].append(
            {
                "id": 1,
                "ts": created_at.isoformat(),
                "spawned_branch_arc_id": branch["id"],
                "spawned_branch_arc_uid": branch["uid"],
                "spawned_branch_arc_title": title,
            }
        )
        graph = runtime.ensure_fact_graph(runtime.get_chat_mem(memory, 1))
        graph["facts"].append(
            {
                "id": f"fact-{title}",
                "source": "lore_arc",
                "arc_id": branch["id"],
                "arc_uid": branch["uid"],
            }
        )

    runtime.save_memory(first)
    runtime.save_memory(second)

    saved = runtime.load_memory()
    arcs = saved["config"]["life"]["lore_arcs"]
    assert {arc["title"] for arc in arcs} == {"общая арка", "первая ветка", "вторая ветка"}

    by_uid = {arc["uid"]: arc for arc in arcs}
    saved_parent = by_uid[parent_uid]
    assert len({arc["id"] for arc in arcs}) == len(arcs)
    for title, branch_uid in branch_uids.items():
        saved_branch = by_uid[branch_uid]
        assert saved_branch["parent_arc_uid"] == parent_uid
        assert saved_branch["parent_arc_id"] == saved_parent["id"]
        beat = next(
            item
            for item in saved_parent["beats"]
            if item.get("spawned_branch_arc_title") == title
        )
        assert beat["spawned_branch_arc_uid"] == branch_uid
        assert beat["spawned_branch_arc_id"] == saved_branch["id"]

    facts = runtime.ensure_fact_graph(runtime.get_chat_mem(saved, 1))["facts"]
    for fact in facts:
        assert fact["arc_id"] == by_uid[fact["arc_uid"]]["id"]


def _store_due_daily_memory() -> None:
    memory = runtime.default_memory()
    runtime.get_chat_mem(memory, 1)["history"] = [
        {"message_id": 1, "user_id": 7, "name": "а", "text": "живой чат"}
    ]
    life = runtime._ensure_life_config(memory["config"])
    now_local = datetime.now(runtime._safe_zoneinfo(life["timezone"]))
    life["slots_date"] = now_local.date().isoformat()
    life["daily_slots"] = [0]
    life["sent_slots"] = []
    assert runtime.save_memory(memory) is True


def test_proactive_send_error_releases_slot_for_retry(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(runtime, "MEMORY_PATH", tmp_path / "memory.json")
    _store_due_daily_memory()
    application = SimpleNamespace(bot=SimpleNamespace(send_message=AsyncMock(side_effect=RuntimeError("telegram down"))))

    def stable_mood(memory, **kwargs):
        del kwargs
        return False, runtime._ensure_mood_config(memory["config"]), False

    with (
        patch.object(runtime, "_sync_mood_state", side_effect=stable_mood),
        patch.object(
            runtime,
            "_generate_story_text",
            new=AsyncMock(return_value="сдал сложный кусок\nу вас как с этим обычно"),
        ),
    ):
        try:
            asyncio.run(runtime._emit_proactive_story(application))
        except RuntimeError as exc:
            assert str(exc) == "telegram down"
        else:
            raise AssertionError("Telegram error must propagate to the life loop")

    assert runtime.load_memory()["config"]["life"]["sent_slots"] == []


def test_proactive_ambiguous_network_error_keeps_slot_to_avoid_duplicate(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(runtime, "MEMORY_PATH", tmp_path / "memory.json")
    _store_due_daily_memory()
    application = SimpleNamespace(
        bot=SimpleNamespace(send_message=AsyncMock(side_effect=runtime.TimedOut("outcome unknown")))
    )

    def stable_mood(memory, **kwargs):
        del kwargs
        return False, runtime._ensure_mood_config(memory["config"]), False

    with (
        patch.object(runtime, "_sync_mood_state", side_effect=stable_mood),
        patch.object(
            runtime,
            "_generate_story_text",
            new=AsyncMock(return_value="сдал сложный кусок\nу вас как с этим обычно"),
        ),
    ):
        try:
            asyncio.run(runtime._emit_proactive_story(application))
        except runtime.TimedOut:
            pass
        else:
            raise AssertionError("Telegram timeout must propagate to the life loop")

    assert runtime.load_memory()["config"]["life"]["sent_slots"] == [0]


def test_proactive_bad_request_is_permanent_not_ambiguous_or_retried(tmp_path, monkeypatch, caplog) -> None:
    monkeypatch.setattr(runtime, "MEMORY_PATH", tmp_path / "memory.json")
    _store_due_daily_memory()
    application = SimpleNamespace(
        bot=SimpleNamespace(send_message=AsyncMock(side_effect=runtime.BadRequest("chat not found")))
    )

    def stable_mood(memory, **kwargs):
        del kwargs
        return False, runtime._ensure_mood_config(memory["config"]), False

    with (
        patch.object(runtime, "_sync_mood_state", side_effect=stable_mood),
        patch.object(
            runtime,
            "_generate_story_text",
            new=AsyncMock(return_value="сдал сложный кусок\nу вас как с этим обычно"),
        ),
        caplog.at_level("WARNING", logger="timur-bot"),
    ):
        try:
            asyncio.run(runtime._emit_proactive_story(application))
        except runtime.BadRequest:
            pass
        else:
            raise AssertionError("Telegram bad request must propagate to the life loop")

    assert runtime.load_memory()["config"]["life"]["sent_slots"] == [0]
    assert "слот закрыт без повтора" in caplog.text
    assert "Исход отправки Telegram неизвестен" not in caplog.text


def test_proactive_failure_policy_does_not_retry_obsolete_chat_id() -> None:
    from telegram.error import ChatMigrated, RetryAfter

    assert runtime._telegram_send_failure_policy(ChatMigrated(123)) == "permanent"
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        retry_after = RetryAfter(timedelta(seconds=1))
    assert runtime._telegram_send_failure_policy(retry_after) == "retry"


def test_proactive_final_save_failure_is_logged_not_reported_as_success(tmp_path, monkeypatch, caplog) -> None:
    monkeypatch.setattr(runtime, "MEMORY_PATH", tmp_path / "memory.json")
    _store_due_daily_memory()
    original_save = runtime.save_memory
    message_sent = {"value": False}

    async def send_message(**kwargs):
        del kwargs
        message_sent["value"] = True
        return SimpleNamespace(message_id=99)

    def fail_after_send(memory):
        if message_sent["value"]:
            return False
        return original_save(memory)

    application = SimpleNamespace(bot=SimpleNamespace(send_message=send_message))

    def stable_mood(memory, **kwargs):
        del kwargs
        return False, runtime._ensure_mood_config(memory["config"]), False

    with (
        patch.object(runtime, "_sync_mood_state", side_effect=stable_mood),
        patch.object(
            runtime,
            "_generate_story_text",
            new=AsyncMock(return_value="сдал сложный кусок\nу вас как с этим обычно"),
        ),
        patch.object(runtime, "save_memory", side_effect=fail_after_send),
        caplog.at_level("INFO", logger="timur-bot"),
    ):
        asyncio.run(runtime._emit_proactive_story(application))

    assert "История отправлена, но не сохранена" in caplog.text
    assert "Проактивная история отправлена" not in caplog.text
