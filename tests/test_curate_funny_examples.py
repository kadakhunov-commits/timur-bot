import json

from timur_bot.tools.curate_funny_examples import (
    _extract_json_object,
    _normalize_selection_payload,
    apply_selections,
    build_curator_messages,
)


def test_extract_json_object_accepts_fenced_json() -> None:
    payload = _extract_json_object('```json\n{"selected": [{"id": "a"}]}\n```')

    assert payload["selected"][0]["id"] == "a"


def test_normalize_selection_payload_clamps_rating_and_tags() -> None:
    selections = _normalize_selection_payload(
        {
            "selected": [
                {"id": "x", "rating": 99, "tags": ["deadpan", "local"], "reason": "good"},
                "y",
            ]
        }
    )

    assert selections[0]["rating"] == 10
    assert selections[0]["tags"] == ["deadpan", "local"]
    assert selections[1]["id"] == "y"
    assert selections[1]["rating"] == 8


def test_apply_selections_marks_only_high_rated_items() -> None:
    candidates = [
        {"id": "a", "good_reply": "лол", "tags": []},
        {"id": "b", "good_reply": "мда", "tags": []},
    ]
    curated = apply_selections(
        candidates,
        [
            {"id": "a", "rating": 9, "tags": ["deadpan"], "reason": "short"},
            {"id": "b", "rating": 6, "tags": ["weak"], "reason": "meh"},
        ],
        min_rating=8,
    )

    by_id = {item["id"]: item for item in curated}
    assert by_id["a"]["selected"] is True
    assert by_id["a"]["curator_rating"] == 9
    assert by_id["a"]["tags"] == ["deadpan"]
    assert by_id["b"]["selected"] is False


def test_build_curator_messages_contains_json_candidates() -> None:
    messages = build_curator_messages(
        [
            {
                "id": "abc",
                "score": 7.2,
                "signals": ["nearby_laugh_after"],
                "author": "A",
                "recency_days": 3,
                "context": [{"author": "B", "text": "ну", "ts": "2024-01-01T00:00:00"}],
                "dialogue_after": [{"author": "B", "text": "ахаха", "ts": "2024-01-01T00:00:01"}],
                "laugh_responses": [{"author": "B", "text": "ахаха", "ts": "2024-01-01T00:00:01"}],
                "good_reply": "классика",
            }
        ],
        select_per_batch=3,
    )

    assert messages[0]["role"] == "system"
    payload_text = messages[1]["content"]
    assert "Выбери максимум 3" in payload_text
    assert json.dumps("abc", ensure_ascii=False) in payload_text
    assert "laugh_responses" in payload_text
    assert "recency_days" in payload_text
