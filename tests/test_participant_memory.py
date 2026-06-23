import os

os.environ.setdefault("TELEGRAM_BOT_TOKEN", "test-token")
os.environ.setdefault("OPENAI_API_KEY", "test-api-key")

from timur_bot.services import bot_logic as rt
from timur_bot.services.participant_memory import (
    build_participant_dossier,
    extract_participant_facts,
    learn_participant_facts,
    update_rapport,
)


def _chat_with_participant(uid: int = 555, name: str = "женя") -> dict:
    memory = rt.default_memory()
    chat_mem = rt.get_chat_mem(memory, 7)
    chat_mem.setdefault("participants", {})[str(uid)] = {
        "user_id": uid,
        "name": name,
        "username": "zhenya",
        "keywords": {"аниме": 5.0, "бэтмен": 3.0},
    }
    return chat_mem


def test_extract_self_statements() -> None:
    facts = dict((attr, val) for attr, val, _ in extract_participant_facts(
        "меня зовут женя. мне 25. я живу в питере. я работаю в яндексе. я люблю аниме"
    ))
    assert facts["full_name"] == "женя"
    assert facts["age"] == "25"
    assert facts["residence"] == "питере"
    assert facts["work"] == "яндексе"
    assert facts["likes"] == "аниме"


def test_extract_ignores_empty() -> None:
    assert extract_participant_facts("") == []
    assert extract_participant_facts("ну такое короче") == []


def test_learn_participant_facts_stored_under_user_entity() -> None:
    chat_mem = _chat_with_participant()
    touched = learn_participant_facts(
        chat_mem, user_id=555, name="женя", username="zhenya", text="я живу в питере"
    )
    assert touched
    graph = chat_mem["memory_layers"]["fact_graph"]
    user_facts = [f for f in graph["facts"] if f["entity_id"] == "user:555"]
    assert any(f["attribute"] == "residence" and f["value"] == "питере" for f in user_facts)


def test_rapport_moves_with_tone_and_clamps() -> None:
    chat_mem = _chat_with_participant()
    after_pos = update_rapport(chat_mem, 555, "спасибо красава")
    assert after_pos > 0
    after_neg = update_rapport(chat_mem, 555, "ты дебил заткнись")
    assert after_neg < after_pos  # negative tone pulled it down
    # clamp: a barrage of insults never drops below the floor
    for _ in range(20):
        update_rapport(chat_mem, 555, "идиот мраз")
    assert chat_mem["participants"]["555"]["rapport"] >= -12.0


def test_dossier_is_first_person_with_facts_and_memes() -> None:
    chat_mem = _chat_with_participant()
    learn_participant_facts(chat_mem, user_id=555, name="женя", username="zhenya", text="я живу в питере")
    for _ in range(5):
        update_rapport(chat_mem, 555, "красава спасибо")
    dossier = build_participant_dossier(chat_mem, 555)
    assert dossier.startswith("что я помню про женя")
    assert "питере" in dossier
    assert "мемы" in dossier  # keywords surfaced
    assert "тёплые" in dossier  # strong positive rapport labelled


def test_dossier_empty_for_unknown_user() -> None:
    chat_mem = _chat_with_participant()
    assert build_participant_dossier(chat_mem, 999) == ""
