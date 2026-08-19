"""Fact-check verdicts: bot mention trigger, claim extraction and limits.

The bot is dragged into a chat message with a real @-mention (Grok-style).
Pure helpers here detect the trigger, extract the claim being questioned and
normalize the verdict; the runtime wiring lives in bot_logic.
"""

from __future__ import annotations

import re
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional


FACT_CHECK_SCHEMA_VERSION = 1
FACT_CHECK_MAX_LABELS_PER_STATE = 300

VERDICT_LABELS = (
    "правда",
    "скорее правда",
    "полуправда",
    "скорее нет",
    "враньё",
    "не проверяемо",
)

_TRIGGER_PATTERNS = (
    re.compile(r"это\s+правда\s*\??", re.I),
    re.compile(r"правда\s+или\s+нет\s*\??", re.I),
    re.compile(r"это\s+вранье\s*\??", re.I),
    re.compile(r"фактчек\w*", re.I),
    re.compile(r"factcheck\w*", re.I),
    re.compile(r"\bправда\s*\??$", re.I),
)


def _clean(value: Any, *, limit: int = 500) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()[:limit]


def _now() -> datetime:
    return datetime.utcnow()


def _parse_ts(value: Any) -> datetime | None:
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).replace(tzinfo=None)
    except (TypeError, ValueError):
        return None


def mention_targets_bot(mention_texts: Iterable[str], bot_username: str) -> bool:
    username = str(bot_username or "").strip().lstrip("@").lower()
    if not username:
        return False
    return any(str(item).strip().lstrip("@").lower() == username for item in mention_texts)


def strip_fact_check_markers(text: str, *, bot_username: str = "") -> str:
    clean = _clean(text, limit=2000)
    for pattern in _TRIGGER_PATTERNS:
        clean = pattern.sub(" ", clean)
    if bot_username:
        clean = re.sub(rf"@{re.escape(str(bot_username).strip().lstrip('@'))}\b", " ", clean, flags=re.I)
    return re.sub(r"\s+", " ", clean).strip(" ,.!?:;-")


def extract_fact_check_payload(
    *,
    text: str,
    mention_texts: Iterable[str],
    bot_username: str,
    reply_message: Optional[Dict[str, Any]] = None,
    author_user_id: int = 0,
    bot_id: int = 0,
) -> Dict[str, Any]:
    """Return the claim to verify when the bot was @-mentioned.

    Claim priority: replied-to message from a human, then the mention message
    itself with trigger phrases stripped. Empty claims and self/bot claims are
    rejected so the bot never fact-checks itself.
    """
    result: Dict[str, Any] = {
        "triggered": False,
        "claim_text": "",
        "claim_author_id": 0,
        "claim_author_name": "",
        "claim_message_id": 0,
        "claim_source": "",
    }
    mentions = list(mention_texts)
    if not mention_targets_bot(mentions, bot_username):
        return result
    result["triggered"] = True

    reply = reply_message if isinstance(reply_message, dict) else None
    if reply:
        reply_text = _clean(reply.get("text"), limit=500)
        reply_author_id = int(reply.get("user_id", 0) or 0)
        if (
            reply_text
            and not bool(reply.get("is_bot"))
            and (not bot_id or reply_author_id != int(bot_id))
            and (not author_user_id or reply_author_id != int(author_user_id))
        ):
            result.update(
                {
                    "claim_text": reply_text,
                    "claim_author_id": reply_author_id,
                    "claim_author_name": _clean(reply.get("name") or reply.get("username"), limit=80),
                    "claim_message_id": int(reply.get("message_id", 0) or 0),
                    "claim_source": "reply_target",
                }
            )
            return result

    own_claim = strip_fact_check_markers(text, bot_username=bot_username)
    if len(own_claim) < 4:
        result["triggered"] = False
        return result
    result.update(
        {
            "claim_text": own_claim,
            "claim_author_id": int(author_user_id or 0),
            "claim_author_name": "",
            "claim_message_id": 0,
            "claim_source": "mention_message",
        }
    )
    return result


def ensure_fact_check_state(chat_mem: Dict[str, Any]) -> Dict[str, Any]:
    layers = chat_mem.setdefault("memory_layers", {})
    state = layers.setdefault("fact_check", {})
    state.setdefault("requests", [])
    state.setdefault("verdicts", {})
    return state


def fact_check_request_allowed(chat_mem: Dict[str, Any], *, max_per_hour: int, now: datetime | None = None) -> bool:
    state = ensure_fact_check_state(chat_mem)
    current = now or _now()
    window = timedelta(hours=1)
    recent = [ts for ts in ( _parse_ts(item) for item in state.get("requests", [])) if ts and current - ts < window]
    return len(recent) < max(0, int(max_per_hour))


def mark_fact_check_request(chat_mem: Dict[str, Any], *, now: datetime | None = None) -> None:
    state = ensure_fact_check_state(chat_mem)
    current = now or _now()
    window = timedelta(hours=1)
    kept = [item for item in state.get("requests", []) if _parse_ts(item) and current - _parse_ts(item) < window]
    kept.append(current.isoformat())
    state["requests"] = kept[-100:]


def note_fact_check_verdict(chat_mem: Dict[str, Any], *, label: str) -> None:
    state = ensure_fact_check_state(chat_mem)
    clean = str(label or "").strip().lower().replace("ё", "е")
    if not clean:
        return
    verdicts = state.setdefault("verdicts", {})
    verdicts[clean] = int(verdicts.get(clean, 0)) + 1
    if len(verdicts) > 40:
        for key in sorted(verdicts, key=lambda item: int(verdicts[item]))[: len(verdicts) - 40]:
            verdicts.pop(key, None)


def normalize_fact_check_reply(raw: str, *, max_chars: int, fallback: str = "не проверяемо") -> Dict[str, str]:
    """Ensure the first line carries a known verdict label.

    Replies without a recognized first-line verdict are replaced by the honest
    fallback so the chat never receives an unlabeled guess.
    """
    text = _clean(raw, limit=max(20, int(max_chars)))
    normalized = text.lower().replace("ё", "е")
    first_line = normalized.split("\n", 1)[0]
    for label in VERDICT_LABELS:
        if first_line.startswith(label.lower().replace("ё", "е")):
            return {"text": text[: max(20, int(max_chars))], "label": label, "fallback_used": False}
    return {"text": fallback, "label": "не проверяемо", "fallback_used": True}


def build_fact_check_messages(
    persona: str,
    *,
    claim: str,
    author_name: str,
    scene: str,
    facts_prompt: str,
    dossier: str,
    max_chars: int,
    web_search: bool,
) -> List[Dict[str, str]]:
    bounded_chars = max(20, int(max_chars))
    labels = " / ".join(VERDICT_LABELS)
    system = (
        f"{str(persona or '').strip()}\n\n"
        f"правила вердикта «это правда?». тебя отметили и спрашивают про утверждение человека из чата.\n"
        f"- первая строка ответа — строго один вердикт из списка: {labels}\n"
        f"- дальше максимум одно-два коротких обоснования; весь ответ до {bounded_chars} знаков\n"
        "- если уверенности нет, выбирай «не проверяемо» и не выдумывай факты\n"
        "- оценивай утверждение, не человека; стеб по ситуации допустим, искажать вердикт нельзя\n"
    )
    if web_search:
        system += "- если опираешься на данные из интернета, коротко отметь это\n"
    user_parts = [f"утверждение: {_clean(claim, limit=500)}"]
    if author_name:
        user_parts[0] += f" (автор: {_clean(author_name, limit=80)})"
    if scene.strip():
        user_parts.append(f"сцена вокруг:\n{_clean(scene, limit=1200)}")
    facts = _clean(facts_prompt, limit=800)
    user_parts.append("факты из памяти чата:\n" + (facts or "- пусто"))
    dossier_clean = _clean(dossier, limit=600)
    if dossier_clean:
        user_parts.append(f"заметки про автора:\n{dossier_clean}")
    user_parts.append("вынеси вердикт по утверждению")
    return [
        {"role": "system", "content": system},
        {"role": "user", "content": "\n\n".join(user_parts)},
    ]
