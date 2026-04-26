from timur_bot.services.funny_scan_llm import build_llm_payload, normalize_llm_result


def test_build_llm_payload_truncates_context_and_text() -> None:
    candidate = {
        "message_ids": [1, 2, 3],
        "cluster_messages": [
            {"message_id": 1, "author": "A", "text": "x" * 300, "ts": "2026-04-25T10:00:00"},
            {"message_id": 2, "author": "B", "text": "ok", "ts": "2026-04-25T10:00:01"},
            {"message_id": 3, "author": "C", "text": "fine", "ts": "2026-04-25T10:00:02"},
        ],
        "pre_score": 55.4,
        "signals_pos": ["laugh_after"],
        "signals_neg": [],
    }

    payload = build_llm_payload(candidate, max_context_messages=2, max_chars_per_message=50)

    assert len(payload["messages"]) == 2
    assert len(payload["messages"][0]["text"]) == 50


def test_normalize_llm_result_clamps_and_repairs_boundary() -> None:
    raw = {
        "score": 999,
        "show_to_owner": "invalid",
        "reason_short": "",
        "boundary": {"start_message_id": 3, "end_message_id": 1, "confidence": 5},
        "positive_signals": ["x"] * 20,
        "negative_signals": ["y"] * 20,
    }

    normalized = normalize_llm_result(raw, fallback_message_ids=[1, 2, 3], review_threshold=70)

    assert normalized["score"] == 100
    assert normalized["show_to_owner"] is True
    assert normalized["boundary"]["start_message_id"] == 1
    assert normalized["boundary"]["end_message_id"] == 3
    assert normalized["boundary"]["confidence"] == 1.0
    assert len(normalized["positive_signals"]) == 8
    assert len(normalized["negative_signals"]) == 8
