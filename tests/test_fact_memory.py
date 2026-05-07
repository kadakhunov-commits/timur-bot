import os

os.environ.setdefault("TELEGRAM_BOT_TOKEN", "test-token")
os.environ.setdefault("OPENAI_API_KEY", "test-api-key")

from timur_bot.services import bot_logic as runtime
from timur_bot.services.fact_memory import extract_claim_facts, ensure_fact_graph, upsert_claim_facts


def test_extracts_bot_surname_fact() -> None:
    chat_mem = runtime.get_chat_mem(runtime.default_memory(), 1)
    facts = extract_claim_facts(chat_mem, "тимур а какая у тебя фамилия", "ахметов")
    assert len(facts) == 1
    assert facts[0]["entity_id"] == "bot:self"
    assert facts[0]["attribute"] == "surname"
    assert facts[0]["value"] == "ахметов"


def test_upsert_claim_facts_dedupes_and_builds_edges() -> None:
    chat_mem = runtime.get_chat_mem(runtime.default_memory(), 1)
    fact = extract_claim_facts(chat_mem, "где ты родился", "родился в казани")[0]
    upsert_claim_facts(chat_mem, [fact])
    upsert_claim_facts(chat_mem, [fact])
    graph = ensure_fact_graph(chat_mem)
    assert len(graph["facts"]) == 1
    assert graph["facts"][0]["weight"] >= 2.0
    assert any(key.startswith("bot:self|") or key.endswith("|bot:self") for key in graph["edges"])

