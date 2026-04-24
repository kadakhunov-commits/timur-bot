from __future__ import annotations

import re
from datetime import datetime
from hashlib import sha1
from typing import Any, Dict, Iterable, List, Optional

HUMOR_MODES = (
    "deadpan",
    "callback",
    "roast_user",
    "absurd_literal",
    "parody",
    "serious",
)
FUNNY_REACTIONS = {"❤", "❤️", "💘", "💖", "💗", "💓", "💞", "💕", "🔥"}
UNFUNNY_REACTIONS = {"💩", "👎"}
FUNNY_TEXT = {"лол", "lol", "ахах", "ахаха", "хаха", "пхаха", "ору", "ор"}
UNFUNNY_TEXT = {"несмешно", "не смешно", "кринж", "хуйня", "не смешной"}
SERIOUS_MARKERS = {
    "умер",
    "смерть",
    "болею",
    "болезнь",
    "больница",
    "плохо",
    "депресс",
    "суиц",
    "похорон",
    "тревог",
}
MAX_JOKE_BANK = 180
MAX_FUNNY_EXAMPLES = 240
MAX_BOT_OUTPUTS = 80
MAX_FEEDBACK_LOG = 120
CALLBACK_COOLDOWN_USES = 3


def ensure_humor_schema(chat_mem: Dict[str, Any]) -> Dict[str, Any]:
    layers = chat_mem.setdefault("memory_layers", {})
    layers.setdefault("joke_bank", [])
    layers.setdefault("funny_examples", [])
    layers.setdefault("overused_bits", {})
    layers.setdefault("bot_outputs", [])
    stats = layers.setdefault("humor_stats", {})
    modes = stats.setdefault("modes", {})
    for mode in HUMOR_MODES:
        modes.setdefault(mode, {"uses": 0, "funny": 0, "unfunny": 0})
    stats.setdefault("feedback", [])
    return layers


def make_bit_id(text: str) -> str:
    payload = re.sub(r"\s+", " ", (text or "").strip().lower())
    return sha1(payload.encode("utf-8")).hexdigest()[:12]


def add_joke_bit(
    chat_mem: Dict[str, Any],
    text: str,
    *,
    source: str = "manual",
    tags: Optional[List[str]] = None,
    weight: float = 1.0,
) -> Dict[str, Any]:
    layers = ensure_humor_schema(chat_mem)
    clean = re.sub(r"\s+", " ", (text or "")).strip()
    if not clean:
        raise ValueError("empty joke bit")

    bit_id = make_bit_id(clean)
    bank = layers.setdefault("joke_bank", [])
    for bit in bank:
        if bit.get("id") == bit_id:
            bit["weight"] = float(bit.get("weight", 1.0)) + weight
            bit["last_seen_ts"] = datetime.utcnow().isoformat()
            return bit

    bit = {
        "id": bit_id,
        "text": clean,
        "source": source,
        "tags": tags or [],
        "weight": float(weight),
        "uses": 0,
        "funny": 0,
        "unfunny": 0,
        "last_seen_ts": datetime.utcnow().isoformat(),
    }
    bank.append(bit)
    bank.sort(key=lambda x: (-float(x.get("weight", 0.0)), str(x.get("text", ""))))
    if len(bank) > MAX_JOKE_BANK:
        del bank[MAX_JOKE_BANK:]
    return bit


def add_funny_example(
    chat_mem: Dict[str, Any],
    *,
    context: List[Dict[str, str]],
    good_reply: str,
    tags: Optional[List[str]] = None,
    source: str = "curated",
    weight: float = 2.0,
) -> Optional[Dict[str, Any]]:
    layers = ensure_humor_schema(chat_mem)
    clean_reply = re.sub(r"\s+", " ", (good_reply or "").strip())
    if not clean_reply:
        return None
    clean_context = [
        {
            "author": str(item.get("author", ""))[:80],
            "text": re.sub(r"\s+", " ", str(item.get("text", ""))).strip()[:220],
        }
        for item in context[-8:]
        if str(item.get("text", "")).strip()
    ]
    payload = clean_reply + "|" + "|".join(x["text"] for x in clean_context)
    example_id = sha1(payload.encode("utf-8")).hexdigest()[:12]
    examples = layers.setdefault("funny_examples", [])
    for example in examples:
        if example.get("id") == example_id:
            example["weight"] = float(example.get("weight", 1.0)) + weight
            return example
    example = {
        "id": example_id,
        "context": clean_context,
        "good_reply": clean_reply,
        "tags": tags or [],
        "source": source,
        "weight": float(weight),
        "uses": 0,
    }
    examples.append(example)
    examples.sort(key=lambda x: (-float(x.get("weight", 0.0)), str(x.get("good_reply", ""))))
    if len(examples) > MAX_FUNNY_EXAMPLES:
        del examples[MAX_FUNNY_EXAMPLES:]
    return example


def classify_text_feedback(text: str) -> Optional[str]:
    clean = re.sub(r"\s+", " ", (text or "").strip().lower())
    if not clean:
        return None
    if clean in FUNNY_TEXT:
        return "funny"
    if clean in UNFUNNY_TEXT:
        return "unfunny"
    return None


def classify_reactions(reactions: Iterable[Any]) -> Optional[str]:
    score = 0
    for reaction in reactions or []:
        emoji = getattr(reaction, "emoji", None)
        if not emoji and isinstance(reaction, dict):
            emoji = reaction.get("emoji")
        clean = str(emoji or "").replace("\ufe0f", "")
        if clean in {x.replace("\ufe0f", "") for x in FUNNY_REACTIONS}:
            score += 1
        if clean in {x.replace("\ufe0f", "") for x in UNFUNNY_REACTIONS}:
            score -= 1
    if score > 0:
        return "funny"
    if score < 0:
        return "unfunny"
    return None


def looks_serious(text: str) -> bool:
    low = (text or "").lower()
    return any(marker in low for marker in SERIOUS_MARKERS)


def _tokens(text: str) -> set[str]:
    return {t.lower() for t in re.findall(r"[a-zA-Zа-яА-ЯёЁ0-9]{3,}", text or "")}


def _mode_score(stats: Dict[str, Any], mode: str) -> float:
    data = stats.get("modes", {}).get(mode, {})
    funny = float(data.get("funny", 0))
    unfunny = float(data.get("unfunny", 0))
    uses = float(data.get("uses", 0))
    return (funny - unfunny * 1.2) / (uses + 2.0)


def select_joke_bit(chat_mem: Dict[str, Any], text: str) -> Optional[Dict[str, Any]]:
    layers = ensure_humor_schema(chat_mem)
    bank = layers.get("joke_bank", [])
    if not bank:
        return None

    query = _tokens(text)
    overused = layers.get("overused_bits", {})

    def score(bit: Dict[str, Any]) -> tuple[float, str]:
        bit_tokens = _tokens(str(bit.get("text", "")))
        overlap = len(query.intersection(bit_tokens))
        feedback = float(bit.get("funny", 0)) - float(bit.get("unfunny", 0)) * 1.5
        penalty = float(overused.get(str(bit.get("id")), 0)) * 1.8
        return (float(bit.get("weight", 1.0)) + overlap * 2.0 + feedback - penalty, str(bit.get("text", "")))

    ranked = sorted(bank, key=score, reverse=True)
    best = ranked[0]
    if score(best)[0] <= -1:
        return None
    return best


def select_funny_examples(chat_mem: Dict[str, Any], text: str, limit: int = 2) -> List[Dict[str, Any]]:
    layers = ensure_humor_schema(chat_mem)
    examples = layers.get("funny_examples", [])
    if not examples:
        return []
    query = _tokens(text)

    def score(example: Dict[str, Any]) -> tuple[float, str]:
        payload = str(example.get("good_reply", "")) + " " + " ".join(
            str(item.get("text", "")) for item in example.get("context", [])
        )
        overlap = len(query.intersection(_tokens(payload)))
        return (float(example.get("weight", 1.0)) + overlap * 2.0 - int(example.get("uses", 0)) * 0.2, str(example.get("id", "")))

    ranked = sorted(examples, key=score, reverse=True)
    return ranked[:limit]


def choose_humor_plan(
    chat_mem: Dict[str, Any],
    *,
    text: str,
    user_id: int,
    user_name: str,
) -> Dict[str, Any]:
    layers = ensure_humor_schema(chat_mem)
    stats = layers.get("humor_stats", {})

    if looks_serious(text):
        mode = "serious"
    else:
        bit = select_joke_bit(chat_mem, text)
        candidates = {
            "deadpan": 1.2 + _mode_score(stats, "deadpan"),
            "absurd_literal": 0.9 + _mode_score(stats, "absurd_literal"),
            "roast_user": 0.35 + _mode_score(stats, "roast_user"),
            "parody": 0.4 + _mode_score(stats, "parody"),
            "callback": (0.75 if bit else 0.1) + _mode_score(stats, "callback"),
        }
        if "?" in text:
            candidates["deadpan"] += 0.4
        if len(text or "") < 35:
            candidates["absurd_literal"] += 0.25
        if bit and _tokens(text).intersection(_tokens(str(bit.get("text", "")))):
            candidates["callback"] += 0.6
        mode = max(candidates.items(), key=lambda x: (x[1], x[0]))[0]

    bit = None if mode == "serious" else select_joke_bit(chat_mem, text)
    examples = [] if mode == "serious" else select_funny_examples(chat_mem, text, limit=2)
    instruction_by_mode = {
        "deadpan": "сухая короткая добивка, будто это очевидный провал собеседника",
        "callback": "локальная отсылка к выбранному bit/fact, без объяснения шутки",
        "roast_user": "дружеская прожарка автора через ситуацию, а не прямое оскорбление",
        "absurd_literal": "абсурдно-буквальная трактовка сообщения",
        "parody": "легко передразни вайб автора, не копируя длинно",
        "serious": "без прожарки, ответь по-человечески коротко",
    }
    return {
        "mode": mode,
        "target_user_id": user_id,
        "target_user_name": user_name,
        "bit": bit,
        "bit_ids": [bit["id"]] if bit else [],
        "examples": examples,
        "instruction": instruction_by_mode[mode],
    }


def format_humor_prompt(plan: Dict[str, Any]) -> str:
    lines = [
        "комедийное задание:",
        f"- режим: {plan.get('mode', 'deadpan')}",
        f"- цель: {plan.get('target_user_name') or plan.get('target_user_id')}",
        f"- прием: {plan.get('instruction', '')}",
    ]
    bit = plan.get("bit")
    if bit:
        lines.append(f"- локальный bit: {bit.get('text')}")
    examples = plan.get("examples") or []
    if examples:
        lines.append("- похожие удачные примеры из чата:")
        for example in examples[:2]:
            ctx = " / ".join(
                f"{item.get('author')}: {item.get('text')}" for item in example.get("context", [])[-3:]
            )
            lines.append(f"  контекст: {ctx}")
            lines.append(f"  удачный ответ: {example.get('good_reply')}")
    lines.append("- не объясняй шутку и не делай стендап-монолог")
    lines.append("- не называй человека тупым/дебилом/ничтожным, смеши через ситуацию")
    return "\n".join(lines)


def record_bot_output(
    chat_mem: Dict[str, Any],
    *,
    message_id: int,
    text: str,
    plan: Optional[Dict[str, Any]],
) -> None:
    layers = ensure_humor_schema(chat_mem)
    plan = plan or {"mode": "deadpan", "bit_ids": []}
    outputs = layers.setdefault("bot_outputs", [])
    outputs.append(
        {
            "message_id": int(message_id),
            "text": text,
            "mode": plan.get("mode", "deadpan"),
            "bit_ids": list(plan.get("bit_ids", [])),
            "ts": datetime.utcnow().isoformat(),
            "feedback": [],
        }
    )
    if len(outputs) > MAX_BOT_OUTPUTS:
        del outputs[:-MAX_BOT_OUTPUTS]

    mode = str(plan.get("mode", "deadpan"))
    stats = layers.setdefault("humor_stats", {}).setdefault("modes", {})
    stats.setdefault(mode, {"uses": 0, "funny": 0, "unfunny": 0})
    stats[mode]["uses"] = int(stats[mode].get("uses", 0)) + 1

    overused = layers.setdefault("overused_bits", {})
    for bit_id in plan.get("bit_ids", []):
        overused[str(bit_id)] = int(overused.get(str(bit_id), 0)) + 1
        if overused[str(bit_id)] > CALLBACK_COOLDOWN_USES:
            overused[str(bit_id)] = CALLBACK_COOLDOWN_USES
        for bit in layers.get("joke_bank", []):
            if bit.get("id") == bit_id:
                bit["uses"] = int(bit.get("uses", 0)) + 1
    for example in plan.get("examples", []) or []:
        example_id = str(example.get("id", ""))
        for saved in layers.get("funny_examples", []):
            if str(saved.get("id", "")) == example_id:
                saved["uses"] = int(saved.get("uses", 0)) + 1


def apply_feedback(
    chat_mem: Dict[str, Any],
    *,
    message_id: int,
    rating: str,
    source: str,
    user_id: Optional[int] = None,
) -> bool:
    if rating not in {"funny", "unfunny"}:
        return False
    layers = ensure_humor_schema(chat_mem)
    output = next((o for o in layers.get("bot_outputs", []) if int(o.get("message_id", -1)) == int(message_id)), None)
    if not output:
        return False

    output.setdefault("feedback", []).append(
        {
            "rating": rating,
            "source": source,
            "user_id": user_id,
            "ts": datetime.utcnow().isoformat(),
        }
    )

    mode = str(output.get("mode", "deadpan"))
    modes = layers.setdefault("humor_stats", {}).setdefault("modes", {})
    modes.setdefault(mode, {"uses": 0, "funny": 0, "unfunny": 0})
    modes[mode][rating] = int(modes[mode].get(rating, 0)) + 1

    for bit_id in output.get("bit_ids", []):
        for bit in layers.get("joke_bank", []):
            if bit.get("id") == bit_id:
                bit[rating] = int(bit.get(rating, 0)) + 1

    feedback = layers.setdefault("humor_stats", {}).setdefault("feedback", [])
    feedback.append({"message_id": message_id, "rating": rating, "source": source, "user_id": user_id})
    if len(feedback) > MAX_FEEDBACK_LOG:
        del feedback[:-MAX_FEEDBACK_LOG]
    return True


def format_bits(chat_mem: Dict[str, Any], limit: int = 10) -> str:
    layers = ensure_humor_schema(chat_mem)
    bank = sorted(
        layers.get("joke_bank", []),
        key=lambda b: (-(float(b.get("weight", 0)) + int(b.get("funny", 0)) - int(b.get("unfunny", 0))), str(b.get("text", ""))),
    )
    if not bank:
        return "bits пока пустые"
    lines = []
    for bit in bank[:limit]:
        lines.append(
            f"- {bit.get('text')} | +{bit.get('funny', 0)} -{bit.get('unfunny', 0)} uses={bit.get('uses', 0)}"
        )
    return "\n".join(lines)
