import json
import os
from pathlib import Path

os.environ.setdefault("TELEGRAM_BOT_TOKEN", "test-token")
os.environ.setdefault("OPENAI_API_KEY", "test-api-key")

from timur_bot.services import bot_logic as runtime


def _snapshots() -> dict:
    path = Path(__file__).parent / "fixtures" / "baseline_snapshots.json"
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def test_extract_keywords_snapshot() -> None:
    s = _snapshots()["extract_keywords"]
    got = runtime.extract_keywords(s["text"], limit=s["limit"])
    assert got == s["expected"]


def test_detect_archetype_scores_snapshot() -> None:
    s = _snapshots()["detect_archetype_scores"]
    got = runtime.detect_archetype_scores(s["text"], s["keywords"])
    assert got == s["expected"]


def test_sanitize_reply_text_snapshot() -> None:
    s = _snapshots()["sanitize_reply_text"]
    got = runtime.sanitize_reply_text(s["raw"])
    assert got == s["expected"]


def test_split_into_chain_snapshot() -> None:
    s = _snapshots()["split_into_chain"]
    got = runtime.split_into_chain(s["text"])
    assert got == s["expected"]


def test_top_items_snapshot() -> None:
    s = _snapshots()["top_items"]
    got = runtime._top_items(s["input"], n=s["n"])
    assert [[k, v] for k, v in got] == s["expected"]
