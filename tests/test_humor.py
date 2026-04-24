from timur_bot.services.humor import (
    add_joke_bit,
    add_funny_example,
    apply_feedback,
    choose_humor_plan,
    classify_reactions,
    classify_text_feedback,
    ensure_humor_schema,
    record_bot_output,
)


class DummyReaction:
    def __init__(self, emoji: str) -> None:
        self.emoji = emoji


def test_choose_humor_plan_uses_serious_fallback() -> None:
    chat = {}
    plan = choose_humor_plan(chat, text="мне очень плохо", user_id=1, user_name="A")
    assert plan["mode"] == "serious"


def test_choose_humor_plan_can_select_callback_bit() -> None:
    chat = {}
    add_joke_bit(chat, "кадыр опять бля", source="test", weight=5)
    plan = choose_humor_plan(chat, text="кадыр пришел", user_id=1, user_name="A")
    assert plan["mode"] == "callback"
    assert plan["bit"]["text"] == "кадыр опять бля"


def test_feedback_updates_mode_and_bit_stats() -> None:
    chat = {}
    bit = add_joke_bit(chat, "локальный прикол", source="test", weight=3)
    plan = {"mode": "callback", "bit_ids": [bit["id"]]}
    record_bot_output(chat, message_id=42, text="ответ", plan=plan)

    assert apply_feedback(chat, message_id=42, rating="funny", source="test", user_id=1)

    layers = ensure_humor_schema(chat)
    assert layers["humor_stats"]["modes"]["callback"]["funny"] == 1
    assert layers["joke_bank"][0]["funny"] == 1


def test_untracked_feedback_is_ignored() -> None:
    chat = {}
    assert not apply_feedback(chat, message_id=999, rating="funny", source="test", user_id=1)


def test_feedback_classifiers() -> None:
    assert classify_text_feedback("лол") == "funny"
    assert classify_text_feedback("Несмешно") == "unfunny"
    assert classify_text_feedback("просто сообщение") is None
    assert classify_reactions([DummyReaction("❤️")]) == "funny"
    assert classify_reactions([DummyReaction("💩")]) == "unfunny"


def test_humor_plan_includes_relevant_funny_example() -> None:
    chat = {}
    add_funny_example(
        chat,
        context=[{"author": "A", "text": "кадыр опять спорит"}],
        good_reply="бля у него спор на автопилоте",
        tags=["deadpan"],
        weight=5,
    )

    plan = choose_humor_plan(chat, text="кадыр спорит", user_id=1, user_name="A")

    assert plan["examples"]
    assert plan["examples"][0]["good_reply"] == "бля у него спор на автопилоте"


def test_humor_plan_skips_blocked_repeated_callback_meme() -> None:
    chat = {}
    add_joke_bit(chat, "митя снес сообщения кадыра", source="test", weight=99)
    add_joke_bit(chat, "обычный локальный бит", source="test", weight=1)

    plan = choose_humor_plan(chat, text="высри чето из памяти", user_id=1, user_name="A")

    blocked_bit = plan.get("bit")
    assert not blocked_bit or blocked_bit.get("text") != "митя снес сообщения кадыра"


def test_humor_plan_avoids_roast_without_explicit_request() -> None:
    chat = {}
    plan = choose_humor_plan(chat, text="тимур что скажешь", user_id=1, user_name="A")
    assert plan["mode"] != "roast_user"
