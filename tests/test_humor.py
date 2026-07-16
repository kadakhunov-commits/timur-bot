from datetime import datetime, timedelta

from timur_bot.services.humor import (
    add_funny_example,
    add_joke_bit,
    apply_feedback,
    background_budget_blocked_today,
    background_tokens_used_today,
    callback_keys_on_cooldown,
    classify_reactions,
    classify_text_feedback,
    ensure_humor_schema,
    ensure_daily_signature,
    humor_metrics,
    learn_funny_scene,
    record_bot_output,
    record_humor_decision,
    reserve_background_tokens,
    select_positive_example,
    set_heart_feedback,
    settle_background_tokens,
)


class DummyReaction:
    def __init__(self, emoji: str) -> None:
        self.emoji = emoji


def test_legacy_memory_moves_to_reversible_quarantine() -> None:
    chat = {
        "memory_layers": {
            "joke_bank": [{"id": "x", "text": "митя опять"}],
            "funny_examples": [
                {
                    "id": "e",
                    "context": [{"author": "а", "text": "обычная сцена"}],
                    "good_reply": "удачная короткая реплика",
                    "signals": ["heart"],
                    "source_message_id": 12,
                    "provisional": False,
                }
            ],
            "bot_outputs": [
                {
                    "message_id": 9,
                    "text": "old but funny",
                    "mode": "deadpan",
                    "feedback": [
                        {"rating": "funny", "source": "reaction", "user_id": 7},
                    ],
                }
            ],
        }
    }

    layers = ensure_humor_schema(chat)

    assert layers["humor_schema_version"] == 2
    assert layers["joke_bank"] == []
    assert layers["legacy_humor_v1"]["joke_bank"][0]["text"] == "митя опять"
    assert layers["legacy_humor_v1"]["migration"]["reversible"] is True
    assert {scene["selected_text"] for scene in layers["humor_scenes_v2"]} == {
        "удачная короткая реплика",
        "old but funny",
    }
    migrated_output = next(scene for scene in layers["humor_scenes_v2"] if scene["selected_text"] == "old but funny")
    assert migrated_output["feedback"][0]["source"] == "heart"
    assert layers["legacy_humor_v1"]["migration"]["confirmed_bot_outputs_migrated"] == 1


def test_legacy_quarantine_is_lossless_above_old_cap() -> None:
    original = [{"id": str(index), "text": f"bit {index}"} for index in range(501)]
    chat = {"memory_layers": {"joke_bank": list(original)}}
    layers = ensure_humor_schema(chat)
    assert layers["legacy_humor_v1"]["joke_bank"] == original


def test_migration_merges_preexisting_legacy_container_without_loss() -> None:
    chat = {
        "memory_layers": {
            "legacy_humor_v1": {"joke_bank": [{"id": "old"}]},
            "joke_bank": [{"id": "new"}],
            "overused_bits": {"new-bit": 3},
            "humor_stats": {"new": {"funny": 2}},
        }
    }
    layers = ensure_humor_schema(chat)
    assert [item["id"] for item in layers["legacy_humor_v1"]["joke_bank"]] == ["old", "new"]

    partial = {
        "memory_layers": {
            "legacy_humor_v1": {
                "overused_bits": {"old-bit": 1},
                "humor_stats": {"old": {"funny": 1}},
            },
            "overused_bits": {"new-bit": 3},
            "humor_stats": {"new": {"funny": 2}},
        }
    }
    partial_layers = ensure_humor_schema(partial)
    legacy = partial_layers["legacy_humor_v1"]
    assert legacy["overused_bits"] == {"old-bit": 1}
    assert legacy["overused_bits_additional_snapshots"] == [{"new-bit": 3}]
    assert legacy["humor_stats_additional_snapshots"] == [{"new": {"funny": 2}}]


def test_intermediate_v2_memory_recovers_confirmed_outputs_from_quarantine() -> None:
    chat = {
        "memory_layers": {
            "humor_schema_version": 2,
            "legacy_humor_v1": {
                "migration": {"reversible": True},
                "bot_outputs": [
                    {
                        "message_id": 91,
                        "text": "коротко и смешно",
                        "mode": "deadpan",
                        "feedback": [{"rating": "funny", "source": "reaction", "user_id": 7}],
                    }
                ],
            },
            "humor_scenes_v2": [],
        }
    }

    layers = ensure_humor_schema(chat)
    assert [scene["selected_text"] for scene in layers["humor_scenes_v2"]] == ["коротко и смешно"]
    assert layers["legacy_humor_v1"]["migration"]["bot_output_migration_revision"] == 1
    assert ensure_humor_schema(chat)["humor_scenes_v2"] == layers["humor_scenes_v2"]


def test_curated_imports_and_bits_stay_quarantined() -> None:
    chat = {}
    bit = add_joke_bit(chat, "кадыр опять", source="manual")
    example = add_funny_example(
        chat,
        context=[{"author": "а", "text": "кадыр спорит"}],
        good_reply="спор на автопилоте",
    )

    layers = ensure_humor_schema(chat)
    assert bit["quarantined"] is True
    assert example and example["quarantined"] is True
    assert layers["humor_scenes_v2"] == []


def test_bot_output_heart_is_attached_to_exact_scene_and_reversible() -> None:
    chat = {"history": [{"message_id": 10, "name": "а", "text": "ну мы же планируем"}]}
    plan = {
        "action": "JOKE",
        "scene_type": "contradiction",
        "relation": "pile_on",
        "setup": "план вместо действия",
        "mechanism": "logical_continuation",
        "context": list(chat["history"]),
        "candidates": [
            {"text": "планируете не приехать", "mechanism": "logical_continuation", "callback_key": ""}
        ],
    }
    scene = record_bot_output(chat, message_id=11, text="планируете не приехать", plan=plan, output_kind="ambient")

    assert scene and scene["output_message_id"] == 11
    assert scene["context"] == plan["context"]
    assert scene["candidates"] == plan["candidates"]
    assert scene["mechanism"] == "logical_continuation"
    assert chat["history"][-1]["is_bot"] is True
    assert set_heart_feedback(chat, message_id=11, user_id=7, active=True)
    assert scene["feedback"][0]["source"] == "heart"
    assert set_heart_feedback(chat, message_id=11, user_id=7, active=False)
    assert scene["feedback"] == []

    duplicate = record_bot_output(chat, message_id=11, text="другой текст", plan=plan, output_kind="ambient")
    assert duplicate is scene
    assert ensure_humor_schema(chat)["humor_stats_v2"]["sent"] == 1
    assert len(ensure_humor_schema(chat)["humor_scenes_v2"]) == 1


def test_other_reactions_are_neutral_and_text_feedback_still_explicit() -> None:
    assert classify_reactions([DummyReaction("❤️")]) == "funny"
    assert classify_reactions([DummyReaction("💩")]) is None
    assert classify_reactions([DummyReaction("😂")]) is None
    assert classify_text_feedback("лол") == "funny"
    assert classify_text_feedback("ахахахах!!!") == "funny"
    assert classify_text_feedback("ЛОООЛ") == "funny"
    assert classify_text_feedback("кринж") == "unfunny"


def test_human_laugh_scene_is_positive_and_retrievable_by_structure() -> None:
    chat = {"participants": {"1": {"name": "митя"}}}
    scene = learn_funny_scene(
        chat,
        context=[{"author": "а", "text": "ну мы же планируем"}],
        punchline="планируете не приехать",
        after_context=[],
        signals=["adjacent_laugh"],
        mechanism="logical_continuation",
        source_message_id=22,
    )
    assert scene and scene["feedback"][0]["rating"] == "funny"

    selected = select_positive_example(
        chat,
        scene_type=scene["scene_type"],
        relation="chat",
        setup="они опять что-то планируют",
        current_text="планируем поездку",
        mechanisms=["logical_continuation"],
    )
    assert selected and selected["output_message_id"] == 22


def test_unconfirmed_signal_never_enters_active_memory() -> None:
    chat = {}
    assert learn_funny_scene(
        chat,
        context=[{"author": "а", "text": "setup"}],
        punchline="reply",
        after_context=[],
        signals=["random_marker"],
        source_message_id=23,
    ) is None
    assert ensure_humor_schema(chat)["humor_scenes_v2"] == []


def test_name_only_overlap_does_not_retrieve_example() -> None:
    chat = {"participants": {"1": {"name": "митя"}}}
    learn_funny_scene(
        chat,
        context=[{"author": "а", "text": "митя проспал пару"}],
        punchline="будильник уволился",
        after_context=[],
        signals=["heart"],
        source_message_id=30,
    )

    assert select_positive_example(
        chat,
        scene_type="question",
        relation="direct",
        setup="где митя",
        current_text="митя",
    ) is None

    assert select_positive_example(
        chat,
        scene_type="banter",
        relation="chat",
        setup="машина опять не заводится",
        current_text="машина встала",
    ) is None


def test_positive_style_transfers_across_topics_by_scene_shape_and_length() -> None:
    chat: dict = {}
    scene = learn_funny_scene(
        chat,
        context=[{"author": "а", "text": "встречу опять перенесли"}],
        punchline="срок чисто декоративный",
        after_context=[],
        signals=["heart"],
        mechanism="understatement",
        source_message_id=31,
    )
    assert scene

    selected = select_positive_example(
        chat,
        scene_type="banter",
        relation="chat",
        setup="машина опять не заводится",
        current_text="машина чисто отдыхает",
    )
    assert selected and selected["output_message_id"] == 31


def test_display_name_chat_words_do_not_block_positive_example_retrieval() -> None:
    chat = {"participants": {"1": {"name": "он чел он миша"}}}
    scene = learn_funny_scene(
        chat,
        context=[{"author": "а", "text": "чел опять перенес встречу"}],
        punchline="срок чисто декоративный",
        after_context=[],
        signals=["heart"],
        mechanism="understatement",
        source_message_id=32,
    )
    assert scene

    selected = select_positive_example(
        chat,
        scene_type="banter",
        relation="chat",
        setup="машина опять не заводится",
        current_text="машина чисто отдыхает",
    )
    assert selected and selected["output_message_id"] == 32
    assert select_positive_example(
        chat,
        scene_type="question",
        relation="direct",
        setup="где митю носит",
        current_text="позовите митю",
    ) is None


def test_snapshot_keeps_old_trigger_and_full_reply_chain() -> None:
    from timur_bot.services.humor import snapshot_scene_context

    history = [
        {
            "message_id": index,
            "reply_to_message_id": index - 1 if index in {2, 3, 4} else 0,
            "name": "а",
            "text": f"message {index}",
        }
        for index in range(1, 13)
    ]
    chat = {"history": history}
    snapshot = snapshot_scene_context(chat, trigger_message_id=4, limit=8, reply_depth=3)
    ids = {item["message_id"] for item in snapshot}
    assert {1, 2, 3, 4}.issubset(ids)
    assert len(snapshot) == 8


def test_feedback_and_callback_cooldown_use_v2_scenes() -> None:
    chat = {"history": [{"message_id": 1, "name": "а", "text": "setup"}]}
    chat.setdefault("memory_layers", {}).setdefault("adaptive_humor", {})["human_messages_total"] = 10
    scene = record_bot_output(
        chat,
        message_id=2,
        text="callback",
        plan={
            "scene_type": "banter",
            "relation": "chat",
            "mechanism": "callback",
            "callback_keys": ["trip"],
            "context": list(chat["history"]),
        },
        output_kind="ambient",
    )
    assert scene
    scene["created_ts"] = (datetime.utcnow() - timedelta(days=1)).isoformat()
    assert "trip" in callback_keys_on_cooldown(chat)
    assert apply_feedback(chat, message_id=2, rating="funny", source="reply_text", user_id=3)
    assert ensure_humor_schema(chat)["humor_stats_v2"]["laughs"] == 1

    scene["created_ts"] = (datetime.utcnow() - timedelta(days=15)).isoformat()
    chat["memory_layers"]["adaptive_humor"]["human_messages_total"] = 111
    assert "trip" not in callback_keys_on_cooldown(chat)


def test_callback_budget_blocks_third_callback_in_last_twenty() -> None:
    chat = {"history": [{"message_id": 1, "name": "а", "text": "setup"}]}
    for index in range(2):
        record_bot_output(
            chat,
            message_id=10 + index,
            text=f"callback {index}",
            plan={"callback_keys": [f"key-{index}"], "context": list(chat["history"])},
            output_kind="ambient",
        )
    assert "*" in callback_keys_on_cooldown(chat)


def test_daily_lore_is_timeline_only_not_training_scene() -> None:
    chat = {}
    result = record_bot_output(
        chat,
        message_id=77,
        text="сдал сложный кусок\nу вас как с этим обычно",
        plan=None,
        output_kind="daily_lore",
    )
    assert result is None
    assert ensure_humor_schema(chat)["humor_scenes_v2"] == []
    assert chat["history"][-1]["text"].endswith("у вас как с этим обычно")


def test_daily_lore_timeline_is_idempotent_for_same_telegram_message() -> None:
    chat: dict = {}
    for _ in range(2):
        record_bot_output(
            chat,
            message_id=77,
            text="сдал сложный кусок\nу вас как с этим обычно",
            plan=None,
            output_kind="daily_lore",
        )

    assert [row["message_id"] for row in chat["history"]] == [77]
    assert [row["message_id"] for row in chat["log"]] == [77]
    assert ensure_humor_schema(chat)["humor_scenes_v2"] == []


def test_daily_signature_is_exact_and_idempotent() -> None:
    body = "сдал сложный кусок и щас легче дышать"
    signed = ensure_daily_signature(body)
    assert signed == f"{body}\nу вас как с этим обычно"
    assert ensure_daily_signature(signed) == signed


def test_background_budget_never_exceeds_reserved_ceiling() -> None:
    chat: dict = {}
    reserved = reserve_background_tokens(chat, 580)
    charged = settle_background_tokens(chat, reserved=reserved, actual=13_400)

    assert charged == 580
    assert background_tokens_used_today(chat) == 580
    assert background_budget_blocked_today(chat) is True
    anomaly = ensure_humor_schema(chat)["humor_budget_anomalies_v2"][-1]
    assert (anomaly["reserved"], anomaly["reported"]) == (580, 13_400)


def test_acceptance_metrics_cover_first_hundred_decisions_and_twenty_jokes() -> None:
    chat = {"history": [{"message_id": 1, "name": "а", "text": "setup"}]}
    for index in range(20):
        scene = record_bot_output(
            chat,
            message_id=100 + index,
            text=f"добивка {index}",
            plan={"context": list(chat["history"]), "latency_ms": 30, "token_usage": 10},
            output_kind="ambient",
        )
        if index < 5:
            assert scene
            set_heart_feedback(chat, message_id=100 + index, user_id=7, active=True)
    for index in range(100):
        record_humor_decision(
            chat,
            action="SILENCE" if index % 2 == 0 else "JOKE",
            sent=index % 2 == 1,
            token_usage=2,
            latency_ms=index,
        )

    metrics = humor_metrics(chat)
    assert metrics["decision_count"] == 100
    assert metrics["silence_rate"] == 0.5
    assert metrics["hearted_in_last_20"] == 5
    assert metrics["tokens_last_100"] == 200
    assert metrics["irrelevant_name_count"] == 0
    assert metrics["ready_for_first_review"] is True
