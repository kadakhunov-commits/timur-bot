import os

os.environ.setdefault("TELEGRAM_BOT_TOKEN", "test-token")
os.environ.setdefault("OPENAI_API_KEY", "test-api-key")

from timur_bot.services import bot_logic as runtime
from timur_bot.services.fact_memory import extract_claim_facts, upsert_claim_facts
from timur_bot.services.fact_recall import build_fact_recall_bundle, build_miniapp_fact_map


def test_recall_prioritizes_matching_attribute() -> None:
    chat_mem = runtime.get_chat_mem(runtime.default_memory(), 1)
    for question, reply in (
        ("какая у тебя фамилия", "ахметов"),
        ("где ты родился", "родился в казани"),
    ):
        upsert_claim_facts(chat_mem, extract_claim_facts(chat_mem, question, reply))

    bundle = build_fact_recall_bundle(chat_mem, "тимур скажи фамилию")
    assert bundle["facts"]
    assert bundle["facts"][0]["attribute"] == "surname"


def test_miniapp_fact_map_contains_center_and_fact_nodes() -> None:
    chat_mem = runtime.get_chat_mem(runtime.default_memory(), 1)
    upsert_claim_facts(chat_mem, extract_claim_facts(chat_mem, "где ты родился", "родился в казани"))
    graph = build_miniapp_fact_map(chat_mem, "bot:self")
    kinds = {node["kind"] for node in graph["nodes"]}
    assert "center" in kinds
    assert "fact" in kinds
    assert graph["edges"]
    assert {"from", "to", "label", "weight"} <= set(graph["edges"][0].keys())
    assert graph["facts"]
    assert {"text", "subject", "tags", "confidence", "source"} <= set(graph["facts"][0].keys())
