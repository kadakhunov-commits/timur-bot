"""Layer A quality gate tests over the anonymous scene corpus.

Each scene category gets a deterministic scripted writer/critic so the test
asserts the gate choice (send / silence / filter-dropped) rather than the exact
wording of a joke. No network calls are made.
"""

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from timur_bot.services.humor import apply_feedback, ensure_humor_schema
import timur_bot.services.bot_logic as runtime


FIXTURE = Path(__file__).parent / "fixtures" / "humor_quality_scenes.json"
CORPUS = json.loads(FIXTURE.read_text(encoding="utf-8"))["scenes"]


def load_quality_scenes() -> dict:
    return {scene["id"]: scene for scene in CORPUS}


def _fixture_for_scene(scene: dict, *, chat_id: int = 900):
    memory = runtime.default_memory()
    memory["config"]["adaptive_humor"]["participation_rate"] = 1.0
    chat = runtime.get_chat_mem(memory, chat_id)
    rows = []
    for index, item in enumerate(scene["messages"], start=1):
        rows.append(
            {
                "message_id": index,
                "user_id": 700 + index,
                "name": item.get("name"),
                "text": item["text"],
                "is_bot": bool(item.get("is_bot")),
                "ts": "2026-08-01T12:00:00",
            }
        )
    chat["history"] = rows
    for _ in rows:
        runtime.note_human_message(chat)
    chat["participants"] = {
        "1": {"name": "митя", "username": ""},
        "2": {"name": "кадыр", "username": ""},
    }
    last = rows[-1]
    message = SimpleNamespace(
        chat_id=chat_id,
        message_id=last["message_id"],
        text=last["text"],
        caption=None,
        from_user=SimpleNamespace(id=last["user_id"], first_name=last["name"], username=None, is_bot=False),
        reply_to_message=None,
    )
    return memory, SimpleNamespace(effective_message=message), SimpleNamespace()


def _writer_json(*, should_attempt: bool, candidates, latest_message_funny: bool = False) -> str:
    payload = {
        "should_attempt": should_attempt,
        "latest_message_funny": latest_message_funny,
        "setup": "сцена",
        "scene_type": "banter",
        "relation": "chat",
        "candidates": [
            {"text": text, "mechanism": mechanism, "callback_key": callback}
            for text, mechanism, callback in candidates
        ],
    }
    return json.dumps(payload, ensure_ascii=False)


def _run_snipe(memory, update, context, metered) -> tuple[bool, AsyncMock]:
    sender = AsyncMock(return_value=True)
    with (
        patch.object(runtime, "call_openai_metered", metered),
        patch.object(runtime, "send_reply_with_style", sender),
        patch.object(runtime, "save_memory"),
        patch.object(runtime.random, "random", return_value=0.0),
    ):
        sent = asyncio.run(runtime._maybe_send_adaptive_snipe(update, context, memory))
    return sent, sender


def test_corpus_categories_cover_the_required_cases() -> None:
    scenes = load_quality_scenes()
    categories = {scene["category"] for scene in scenes.values()}
    assert {
        "contextual_twist",
        "serious",
        "technical",
        "finished_joke",
        "no_trigger",
        "repeat_risk",
        "absent_person",
        "template_trap",
    } <= categories
    assert len(scenes) >= 12
    assert set(scenes["quality_absent_mitya"]["forbidden_names"]) >= {"митя", "кадыр"}


def test_writer_silence_produces_no_reply_and_no_critic_call() -> None:
    for scene_id in (
        "quality_technical_ssh",
        "quality_ack_close",
        "quality_greeting",
        "quality_one_word_agreement",
        "quality_serious_illness",
        "quality_grief",
    ):
        scene = load_quality_scenes()[scene_id]
        memory, update, context = _fixture_for_scene(scene, chat_id=hash(scene_id) % 1000 + 100)
        metered = AsyncMock(return_value=(_writer_json(should_attempt=False, candidates=[]), 50))

        sent, sender = _run_snipe(memory, update, context, metered)

        assert sent is False, scene_id
        sender.assert_not_awaited()
        assert metered.await_count == 1, scene_id
        decision = ensure_humor_schema(runtime.get_chat_mem(memory, update.effective_message.chat_id))["humor_decisions_v2"][-1]
        assert decision["action"] == "SILENCE"


def test_contextual_twist_scenes_send_the_critic_winner() -> None:
    cases = [
        (
            "quality_twist_planning",
            [
                ("план не мешает не приехать", "logic", ""),
                ("планирование почти поездка", "status", ""),
                ("сомнения можно было не будить", "understatement", ""),
            ],
        ),
        (
            "quality_twist_deadline",
            [
                ("точно — уже в третий раз", "understatement", ""),
                ("клятвы идут по расписанию", "image", ""),
            ],
        ),
        (
            "quality_status_diploma",
            [
                ("красота требует диплома", "status", ""),
                ("диплом как гирлянда", "image", ""),
            ],
        ),
        (
            "quality_understatement_rent",
            [
                ("немного — почти половина", "understatement", ""),
                ("сорок процентов это же мелочь", "image", ""),
            ],
        ),
    ]
    for scene_id, candidates in cases:
        scene = load_quality_scenes()[scene_id]
        memory, update, context = _fixture_for_scene(scene, chat_id=hash(scene_id) % 1000 + 100)
        writer = _writer_json(should_attempt=True, candidates=candidates)
        critic = '{"winner_index":0,"score":92,"reaction_score":0,"react":false,"reason_codes":["local"]}'
        metered = AsyncMock(side_effect=[(writer, 100), (critic, 20)])

        sent, sender = _run_snipe(memory, update, context, metered)

        assert sent is True, scene_id
        assert sender.await_args.args[3] == candidates[0][0], scene_id
        assert sender.await_args.kwargs["is_snipe"] is True


def test_absent_person_candidates_are_filtered_into_silence() -> None:
    scene = load_quality_scenes()["quality_absent_mitya"]
    memory, update, context = _fixture_for_scene(scene)
    writer = _writer_json(
        should_attempt=True,
        candidates=[
            ("митя бы скинулся за всех", "status", "person:митя"),
            ("кадыр традиционно в доле", "image", "person:кадыр"),
        ],
    )
    metered = AsyncMock(return_value=(writer, 60))

    sent, sender = _run_snipe(memory, update, context, metered)

    assert sent is False
    assert metered.await_count == 1
    sender.assert_not_awaited()
    decision = ensure_humor_schema(runtime.get_chat_mem(memory, 900))["humor_decisions_v2"][-1]
    assert decision["reason_codes"] == ["hard_filter_empty"]


def test_template_candidates_are_filtered_into_silence() -> None:
    scene = load_quality_scenes()["quality_movie_night"]
    memory, update, context = _fixture_for_scene(scene)
    writer = _writer_json(
        should_attempt=True,
        candidates=[
            ("выбор фильма — это когда смотришь описание", "logic", ""),
            ("его iq равен температуре в комнате", "image", ""),
        ],
    )
    metered = AsyncMock(return_value=(writer, 60))

    sent, sender = _run_snipe(memory, update, context, metered)

    assert sent is False
    sender.assert_not_awaited()


def test_repeated_snipe_text_is_never_sent_again() -> None:
    scene = load_quality_scenes()["quality_repeat_risk"]
    memory, update, context = _fixture_for_scene(scene)
    chat = runtime.get_chat_mem(memory, 900)
    runtime.record_bot_output(
        chat,
        message_id=2,
        text="билеты еще не гарантия поездки",
        plan=None,
        output_kind="ambient",
        trigger_message_id=1,
    )
    writer = _writer_json(
        should_attempt=True,
        candidates=[
            ("билеты — еще не гарантия поездки", "logic", ""),
            ("билеты, еще не гарантия поездки", "logic", ""),
        ],
    )
    metered = AsyncMock(return_value=(writer, 60))

    sent, sender = _run_snipe(memory, update, context, metered)

    assert sent is False
    sender.assert_not_awaited()


def test_unfunny_feedback_blocks_mechanism_and_callback_in_next_snipe() -> None:
    scene = load_quality_scenes()["quality_twist_planning"]
    memory, update, context = _fixture_for_scene(scene)
    chat = runtime.get_chat_mem(memory, 900)
    chat_scene = runtime.record_bot_output(
        chat,
        message_id=99,
        text="план сам себя не обсудит",
        plan={"action": "JOKE", "mode": "ambient", "mechanism": "status", "callback_keys": ["trip"]},
        output_kind="ambient",
        trigger_message_id=3,
    )
    apply_feedback(chat, message_id=chat_scene["output_message_id"], rating="unfunny", source="reply_text", user_id=1)

    writer = _writer_json(
        should_attempt=True,
        candidates=[
            ("план снова победил", "status", "trip"),
            ("не приехать, но план есть", "logic", ""),
        ],
    )
    critic = '{"winner_index":0,"score":90,"reaction_score":0,"react":false,"reason_codes":["local"]}'
    metered = AsyncMock(side_effect=[(writer, 100), (critic, 20)])

    sent, sender = _run_snipe(memory, update, context, metered)

    writer_prompt = metered.await_args_list[0].args[0][-1]["content"]
    assert "заблокированные фидбеком" in writer_prompt
    assert "status" in writer_prompt
    assert sent is True
    assert sender.await_args.args[3] == "не приехать, но план есть"
