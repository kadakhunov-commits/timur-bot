from __future__ import annotations

import json
import re
from typing import Any, Dict, List, Sequence, Tuple


def estimate_tokens_from_text(text: str) -> int:
    return max(1, (len(text or "") // 4) + 1)


def _extract_json_object(text: str) -> Dict[str, Any]:
    raw = (text or "").strip()
    if not raw:
        return {}

    fenced = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", raw, flags=re.DOTALL | re.IGNORECASE)
    if fenced:
        raw = fenced.group(1).strip()
    elif not raw.startswith("{"):
        start = raw.find("{")
        end = raw.rfind("}")
        if start >= 0 and end > start:
            raw = raw[start : end + 1]
    parsed = json.loads(raw)
    if not isinstance(parsed, dict):
        return {}
    return parsed


def _clamp_int(value: Any, low: int, high: int, default: int) -> int:
    try:
        parsed = int(float(value))
    except Exception:
        parsed = default
    return max(low, min(high, parsed))


def build_llm_payload(
    candidate: Dict[str, Any],
    *,
    max_context_messages: int,
    max_chars_per_message: int,
) -> Dict[str, Any]:
    messages = list(candidate.get("cluster_messages") or [])
    compact_messages: List[Dict[str, Any]] = []
    for item in messages[: max(1, max_context_messages)]:
        compact_messages.append(
            {
                "message_id": int(item.get("message_id", 0)),
                "author": str(item.get("author") or "unknown")[:80],
                "ts": str(item.get("ts") or ""),
                "text": str(item.get("text") or "")[: max(40, int(max_chars_per_message))],
                "reaction_total": int(item.get("reaction_total", 0)),
                "reaction_heart": int(item.get("reaction_heart", 0)),
                "reaction_laugh": int(item.get("reaction_laugh", 0)),
            }
        )

    return {
        "candidate_id": candidate.get("id") or "",
        "source_chat_id": int(candidate.get("source_chat_id", 0)),
        "pre_score": float(candidate.get("pre_score", 0.0)),
        "signals_pos": list(candidate.get("signals_pos") or []),
        "signals_neg": list(candidate.get("signals_neg") or []),
        "current_boundary": {
            "start_message_id": int((candidate.get("message_ids") or [0])[0]),
            "end_message_id": int((candidate.get("message_ids") or [0])[-1]),
        },
        "messages": compact_messages,
    }


def build_llm_messages(payload: Dict[str, Any]) -> List[Dict[str, str]]:
    system_text = (
        "Ты анализируешь потенциально смешной момент в Telegram-чате.\n"
        "Оценивай только по данным в payload: текст, последовательность реплик и реакции.\n"
        "Ставь итоговый score по единой шкале 0..100.\n"
        "Штрафуй привычные ахах/лол, сарказм без реального эффекта и мат без смешного контекста.\n"
        "Верни только JSON-объект без комментариев."
    )
    user_text = (
        "Оцени кандидата и верни JSON строго по схеме:\n"
        "{\n"
        '  "score": 0..100,\n'
        '  "show_to_owner": true|false,\n'
        '  "reason_short": "кратко до 240 символов",\n'
        '  "boundary": {"start_message_id": int, "end_message_id": int, "confidence": 0..1},\n'
        '  "positive_signals": ["..."],\n'
        '  "negative_signals": ["..."]\n'
        "}\n"
        f"payload: {json.dumps(payload, ensure_ascii=False)}"
    )
    return [{"role": "system", "content": system_text}, {"role": "user", "content": user_text}]


def normalize_llm_result(
    raw: Dict[str, Any],
    *,
    fallback_message_ids: Sequence[int],
    review_threshold: int,
) -> Dict[str, Any]:
    ids = [int(x) for x in fallback_message_ids if int(x) > 0]
    fallback_start = ids[0] if ids else 0
    fallback_end = ids[-1] if ids else 0

    score = _clamp_int(raw.get("score"), 0, 100, 0)
    boundary = raw.get("boundary") if isinstance(raw.get("boundary"), dict) else {}
    start_id = _clamp_int(boundary.get("start_message_id"), fallback_start, fallback_end or fallback_start, fallback_start)
    end_id = _clamp_int(boundary.get("end_message_id"), fallback_start, fallback_end or fallback_start, fallback_end)
    if start_id > end_id:
        start_id, end_id = end_id, start_id

    confidence_raw = boundary.get("confidence", 0.5)
    try:
        confidence = float(confidence_raw)
    except Exception:
        confidence = 0.5
    confidence = max(0.0, min(1.0, confidence))

    positive = raw.get("positive_signals") if isinstance(raw.get("positive_signals"), list) else []
    negative = raw.get("negative_signals") if isinstance(raw.get("negative_signals"), list) else []
    reason = str(raw.get("reason_short") or "").strip()
    if not reason:
        reason = "Контекст и реакция участников указывают на вероятный смешной момент."
    reason = reason[:240]

    show_to_owner = raw.get("show_to_owner")
    if isinstance(show_to_owner, bool):
        show_flag = show_to_owner
    else:
        show_flag = score >= int(review_threshold)

    return {
        "score": score,
        "show_to_owner": bool(show_flag),
        "reason_short": reason,
        "boundary": {
            "start_message_id": start_id,
            "end_message_id": end_id,
            "confidence": confidence,
        },
        "positive_signals": [str(x)[:60] for x in positive][:8],
        "negative_signals": [str(x)[:60] for x in negative][:8],
    }


def evaluate_candidate_with_llm(
    client: Any,
    *,
    model: str,
    candidate: Dict[str, Any],
    max_context_messages: int,
    max_chars_per_message: int,
    review_threshold: int,
) -> Tuple[Dict[str, Any], int]:
    payload = build_llm_payload(
        candidate,
        max_context_messages=max_context_messages,
        max_chars_per_message=max_chars_per_message,
    )
    messages = build_llm_messages(payload)
    request_text = json.dumps(messages, ensure_ascii=False)
    usage_tokens = estimate_tokens_from_text(request_text)

    kwargs: Dict[str, Any] = {
        "model": model,
        "messages": messages,
        "temperature": 0.1,
        "max_tokens": 280,
        "response_format": {"type": "json_object"},
    }
    try:
        response = client.chat.completions.create(**kwargs)
    except Exception:
        kwargs.pop("response_format", None)
        response = client.chat.completions.create(**kwargs)

    content = ""
    if getattr(response, "choices", None):
        content = str(getattr(response.choices[0].message, "content", "") or "")

    raw = _extract_json_object(content)
    normalized = normalize_llm_result(
        raw,
        fallback_message_ids=[int(x) for x in candidate.get("message_ids") or [] if int(x) > 0],
        review_threshold=int(review_threshold),
    )

    usage = getattr(response, "usage", None)
    if usage and getattr(usage, "total_tokens", None) is not None:
        usage_tokens = max(1, int(usage.total_tokens))
    else:
        usage_tokens = max(usage_tokens, estimate_tokens_from_text(content))
    return normalized, usage_tokens
