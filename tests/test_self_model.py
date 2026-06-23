import os

os.environ.setdefault("TELEGRAM_BOT_TOKEN", "test-token")
os.environ.setdefault("OPENAI_API_KEY", "test-api-key")

from timur_bot.services.self_model import (
    CANON_SELF_FACTS,
    build_self_card_prompt,
    ensure_self_profile,
    register_self_claim,
)


def _fresh_memory() -> dict:
    return {}


def test_canon_seeded_and_locked() -> None:
    memory = _fresh_memory()
    profile = ensure_self_profile(memory)
    slots = profile["slots"]
    for attribute, value in CANON_SELF_FACTS.items():
        assert slots[attribute]["value"] == value
        assert slots[attribute]["source"] == "canon"
        assert slots[attribute]["locked"] is True


def test_new_learned_claim_accepted() -> None:
    memory = _fresh_memory()
    result = register_self_claim(memory, "surname", "ахметов", confidence=0.9)
    assert result["status"] == "accepted"
    assert memory["self_profile"]["slots"]["surname"]["value"] == "ахметов"
    assert memory["self_profile"]["slots"]["surname"]["source"] == "learned"


def test_repeated_claim_is_reinforced() -> None:
    memory = _fresh_memory()
    register_self_claim(memory, "surname", "ахметов", confidence=0.7)
    result = register_self_claim(memory, "surname", "Ахметов.", confidence=0.9)
    assert result["status"] == "reinforced"
    slot = memory["self_profile"]["slots"]["surname"]
    assert slot["evidence_count"] == 2
    assert slot["confidence"] == 0.9


def test_contradicting_canon_is_rejected() -> None:
    memory = _fresh_memory()
    result = register_self_claim(memory, "age", "40", confidence=0.99)
    assert result["status"] == "rejected"
    assert result["reason"] == "contradicts_canon"
    assert memory["self_profile"]["slots"]["age"]["value"] == "22"


def test_soft_learned_slot_allows_retcon_then_locks_in() -> None:
    memory = _fresh_memory()
    # soft first claim (low confidence, single sighting) -> retcon allowed
    register_self_claim(memory, "work", "бариста", confidence=0.5)
    retcon = register_self_claim(memory, "work", "курьер", confidence=0.6)
    assert retcon["status"] == "accepted"
    assert retcon["reason"] == "retcon"

    # reinforce to make it established, then a conflict is rejected
    register_self_claim(memory, "work", "курьер", confidence=0.85)
    rejected = register_self_claim(memory, "work", "повар", confidence=0.85)
    assert rejected["status"] == "rejected"
    assert rejected["reason"] == "contradicts_established"
    assert memory["self_profile"]["slots"]["work"]["value"] == "курьер"


def test_empty_claim_ignored() -> None:
    memory = _fresh_memory()
    assert register_self_claim(memory, "surname", "   ")["status"] == "ignored"
    assert register_self_claim(memory, "", "ахметов")["status"] == "ignored"


def test_self_card_is_first_person_and_collapses_duplicate_places() -> None:
    memory = _fresh_memory()
    card = build_self_card_prompt(memory)
    assert card.startswith("кто я")
    assert "меня зовут тимур" in card
    assert "мне 22" in card
    # city/residence/origin all "казань" -> mentioned once, not three times
    assert card.count("казань") == 1


def test_rejection_is_logged() -> None:
    memory = _fresh_memory()
    register_self_claim(memory, "age", "40", confidence=0.99)
    rejected = memory["self_profile"]["rejected"]
    assert rejected and rejected[-1]["attribute"] == "age"
    assert rejected[-1]["kept"] == "22"
