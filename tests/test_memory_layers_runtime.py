import asyncio
import os
from datetime import datetime
from unittest.mock import patch

os.environ.setdefault("TELEGRAM_BOT_TOKEN", "test-token")
os.environ.setdefault("OPENAI_API_KEY", "test-api-key")

from timur_bot.services import bot_logic as runtime


def test_get_chat_mem_has_memory_layers() -> None:
    memory = runtime.default_memory()
    chat = runtime.get_chat_mem(memory, 1)
    layers = chat.get("memory_layers", {})
    assert isinstance(layers.get("recent_messages", []), list)
    assert isinstance(layers.get("recent_facts", []), list)
    assert isinstance(layers.get("long_facts", []), list)


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


def test_apply_lore_payload_appends_arc_and_persists_facts() -> None:
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
    chat_mem = runtime.get_chat_mem(memory, 1)
    graph = chat_mem["memory_layers"]["fact_graph"]
    assert any(str(f.get("source")) == "lore_arc" for f in graph.get("facts", []))
    assert any(str(f.get("attribute")) == "habit" for f in graph.get("facts", []))


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
