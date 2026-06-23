import os

os.environ.setdefault("TELEGRAM_BOT_TOKEN", "test-token")
os.environ.setdefault("OPENAI_API_KEY", "test-api-key")

from timur_bot.services.episodes import (
    MAX_EPISODES,
    build_episodes_block,
    maybe_log_episode,
    message_valence,
    recall_episodes,
)


def test_valence_signs() -> None:
    assert message_valence("спасибо красава") > 0
    assert message_valence("ты дебил пошел нахуй") < 0
    assert message_valence("ну ок наверное") == 0.0


def test_only_salient_moments_are_logged() -> None:
    chat_mem: dict = {}
    assert maybe_log_episode(chat_mem, actor="женя", text="ну ок", valence=0.5, ts="t1") is False
    assert maybe_log_episode(chat_mem, actor="женя", text="ты лучший красава!!!", valence=6.0, ts="t2") is True
    assert len(chat_mem["episodes"]) == 1
    assert chat_mem["episodes"][0]["actor"] == "женя"


def test_episode_buffer_is_capped() -> None:
    chat_mem: dict = {}
    for i in range(MAX_EPISODES + 15):
        maybe_log_episode(chat_mem, actor="x", text=f"огонь движ номер {i}", valence=5.0, ts=str(i))
    assert len(chat_mem["episodes"]) == MAX_EPISODES


def test_recall_returns_relevant_episode() -> None:
    chat_mem: dict = {}
    maybe_log_episode(chat_mem, actor="женя", text="мы ходили на рыбалку и поймали щуку", valence=5.0, ts="t1")
    maybe_log_episode(chat_mem, actor="петя", text="psotanyl на экзамене по матану", valence=-5.0, ts="t2")
    lines = recall_episodes(chat_mem, "помнишь ту рыбалку")
    assert lines
    assert "рыбалку" in lines[0]
    assert lines[0].startswith("я помню как женя")


def test_recall_empty_without_overlap() -> None:
    chat_mem: dict = {}
    maybe_log_episode(chat_mem, actor="женя", text="поймали щуку на рыбалке", valence=5.0, ts="t1")
    assert recall_episodes(chat_mem, "какая погода завтра") == []


def test_block_formatting() -> None:
    assert build_episodes_block([]) == ""
    block = build_episodes_block(["я помню как женя писал (тепло): красава"])
    assert block.startswith("из нашей общей истории:")
    assert "- я помню" in block
