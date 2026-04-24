from __future__ import annotations

from datetime import datetime

from timur_bot.tools.import_telegram_html import (
    MessageRecord,
    ParsedResult,
    TelegramHtmlParser,
    compact_existing_layers,
    import_messages,
)


def test_html_parser_skips_service_and_parses_joined_messages() -> None:
    html = """
    <div class="message service" id="message-1"><div class="body details">Date</div></div>
    <div class="message default clearfix" id="message-10">
      <div class="body">
        <div class="pull_right date details" title="19.01.2024 06:52:31 UTC+12:00">06:52</div>
        <div class="from_name">Mitya</div>
        <div class="text">лол</div>
      </div>
    </div>
    <div class="message default clearfix joined" id="message-11">
      <div class="body">
        <div class="pull_right date details" title="19.01.2024 06:52:35 UTC+12:00">06:52</div>
        <div class="text">еще строка</div>
      </div>
    </div>
    """

    parser = TelegramHtmlParser()
    parser.feed(html)

    assert parser.skipped_service == 1
    assert len(parser.messages) == 2
    assert parser.messages[0].from_name == "Mitya"
    assert parser.messages[1].from_name == "Mitya"
    assert parser.messages[0].ts == "2024-01-18T18:52:31"


def test_import_messages_merge_dedup_and_style_profile() -> None:
    memory = {
        "chats": {
            "1": {
                "history": [
                    {
                        "user_id": 900000000001,
                        "name": "Mitya",
                        "username": "",
                        "text": "лол",
                        "ts": "2024-01-18T18:52:31",
                        "is_bot": False,
                        "message_id": 10,
                    }
                ],
                "log": [],
                "last_meme": None,
                "participants": {},
                "user_relations": {},
                "topic_edges": {},
            }
        },
        "users": {},
        "config": {"style_settings": "мой ручной стиль"},
    }

    parsed = ParsedResult(
        messages=[
            # duplicate of existing record
            MessageRecord(message_id=10, from_name="Mitya", text="лол", ts="2024-01-18T18:52:31"),
            MessageRecord(message_id=12, from_name="Mitya", text="бля ну да?", ts="2024-01-18T18:52:40"),
        ],
        skipped_service=0,
        skipped_empty=0,
    )

    limits = {
        "max_history_per_chat": 100,
        "max_log_per_chat": 1000,
        "max_user_samples": 20,
        "max_quotes_per_user": 6,
        "max_keywords_per_user": 40,
        "max_topic_edges": 300,
        "max_user_relations": 300,
    }

    result = import_messages(
        memory,
        parsed,
        chat_id=1,
        mode="merge",
        limits=limits,
        rus_stopwords={"ну"},
        en_stopwords=set(),
        profanity_markers={"бля"},
        archetypes={},
        apply_style_profile=True,
        recent_days=14,
        max_recent_messages=24,
        max_recent_facts=120,
        max_long_facts=400,
    )

    assert result["deduped"] == 1
    assert result["imported"] == 1
    chat_history = memory["chats"]["1"]["history"]
    assert len(chat_history) == 2
    assert chat_history[-1]["message_id"] == 12

    style_settings = memory["config"]["style_settings"]
    assert "автопрофиль чата (generated):" in style_settings
    assert "=== owner override ===" in style_settings
    assert "мой ручной стиль" in style_settings
    layers = memory["chats"]["1"]["memory_layers"]
    assert isinstance(layers.get("recent_messages", []), list)
    assert isinstance(layers.get("recent_facts", []), list)
    assert isinstance(layers.get("long_facts", []), list)


def test_import_split_recent_and_long_fact_layers() -> None:
    memory = {"chats": {"1": {}}, "users": {}, "config": {}}
    parsed = ParsedResult(
        messages=[
            MessageRecord(message_id=1, from_name="A", text="старый факт", ts="2024-01-01T10:00:00"),
            MessageRecord(message_id=2, from_name="A", text="свежий факт", ts="2024-01-20T10:00:00"),
        ],
        skipped_service=0,
        skipped_empty=0,
    )
    limits = {
        "max_history_per_chat": 100,
        "max_log_per_chat": 1000,
        "max_user_samples": 20,
        "max_quotes_per_user": 6,
        "max_keywords_per_user": 40,
        "max_topic_edges": 300,
        "max_user_relations": 300,
    }
    import_messages(
        memory,
        parsed,
        chat_id=1,
        mode="merge",
        limits=limits,
        rus_stopwords=set(),
        en_stopwords=set(),
        profanity_markers=set(),
        archetypes={},
        apply_style_profile=False,
        recent_days=14,
        max_recent_messages=24,
        max_recent_facts=120,
        max_long_facts=400,
        now_utc=datetime(2024, 1, 25, 12, 0, 0),
    )
    layers = memory["chats"]["1"]["memory_layers"]
    assert len(layers["recent_facts"]) == 1
    assert len(layers["long_facts"]) == 1


def test_compact_existing_layers_moves_old_recent_fact() -> None:
    memory = {
        "chats": {
            "1": {
                "memory_layers": {
                    "recent_messages": [],
                    "recent_facts": [
                        {"text": "A: древний", "ts": "2023-01-01T00:00:00", "weight": 1.0},
                    ],
                    "long_facts": [],
                    "summary": {"chat": "", "updated_at": None},
                }
            }
        },
        "users": {},
        "config": {},
    }
    stats = compact_existing_layers(
        memory,
        chat_id=1,
        recent_days=14,
        max_recent_messages=24,
        max_recent_facts=120,
        max_long_facts=400,
    )
    assert stats["before_recent_facts"] == 1
    assert stats["after_recent_facts"] == 0
    assert stats["after_long_facts"] == 1
