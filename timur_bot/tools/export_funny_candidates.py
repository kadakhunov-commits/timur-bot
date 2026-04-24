from __future__ import annotations

import argparse
import hashlib
import json
import random
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from timur_bot.tools.import_telegram_html import MessageRecord, parse_export_dir

LAUGH_MARKERS = ("лол", "ахах", "ахаха", "пхаха", "хаха", "ору", "ор", "кек")
MEME_MARKERS = ("кринж", "угар", "мем", "прикол", "база", "жесть")
PROFANITY_HINTS = ("бля", "блять", "нах", "хуй", "пизд", "еб", "сука")
PURE_LAUGH_RE = re.compile(r"^(?:[!?.\s,;:()\-]*)(?:л+о+л+|а?ха(?:ха)+|пхаха+|ору+|кек+)(?:[!?.\s,;:()\-]*)$", re.I)


def _norm(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip())


def _parse_ts(ts: str) -> Optional[datetime]:
    try:
        return datetime.fromisoformat((ts or "").replace("Z", "+00:00")).replace(tzinfo=None)
    except Exception:
        return None


def is_laugh_response(text: str) -> bool:
    low = _norm(text).lower()
    if not low:
        return False
    if PURE_LAUGH_RE.match(low):
        return True
    return any(marker in low for marker in ("лол", "ахах", "пхах", "хаха", "ору"))


def _is_pure_laugh(text: str) -> bool:
    return bool(PURE_LAUGH_RE.match(_norm(text).lower()))


def _recency_boost(ts: str, latest_ts: Optional[datetime]) -> tuple[float, List[str], Optional[int]]:
    current = _parse_ts(ts)
    if current is None or latest_ts is None:
        return 0.0, [], None
    age_days = max(0, (latest_ts - current).days)
    if age_days <= 14:
        return 2.0, ["very_recent"], age_days
    if age_days <= 30:
        return 1.5, ["recent"], age_days
    if age_days <= 90:
        return 1.0, ["freshish"], age_days
    if age_days <= 365:
        return 0.45, ["not_too_old"], age_days
    return 0.0, [], age_days


def laugh_responses_after(messages: List[MessageRecord], idx: int, *, window: int = 6) -> List[MessageRecord]:
    return [msg for msg in messages[idx + 1 : idx + 1 + window] if is_laugh_response(msg.text)]


def score_candidate(
    messages: List[MessageRecord],
    idx: int,
    *,
    latest_ts: Optional[datetime] = None,
) -> tuple[float, List[str]]:
    msg = messages[idx]
    text = _norm(msg.text)
    low = text.lower()
    score = 0.0
    signals: List[str] = []

    if _is_pure_laugh(text):
        return 0.0, ["pure_laugh_response"]

    if 2 <= len(text) <= 90:
        score += 1.0
        signals.append("short")
    if any(x in low for x in LAUGH_MARKERS) and not _is_pure_laugh(text):
        score += 0.4
        signals.append("contains_laugh_marker")
    if any(x in low for x in MEME_MARKERS):
        score += 1.2
        signals.append("meme_marker")
    if any(x in low for x in PROFANITY_HINTS):
        score += 0.6
        signals.append("chat_roughness")
    if "?" not in text and len(text) <= 45:
        score += 0.4
        signals.append("punchy_shape")

    after_window = messages[idx + 1 : idx + 7]
    laugh_after = [m for m in after_window if is_laugh_response(m.text)]
    if laugh_after:
        distinct_authors = {m.from_name for m in laugh_after if m.from_name != msg.from_name}
        first_distance = next(
            (offset for offset, after_msg in enumerate(after_window, start=1) if after_msg is laugh_after[0]),
            1,
        )
        score += 4.0
        signals.append("laugh_response_after")
        if first_distance == 1:
            score += 1.2
            signals.append("immediate_laugh_after")
        if any("лол" in m.text.lower() for m in laugh_after):
            score += 0.9
            signals.append("lol_response_after")
        if distinct_authors:
            score += min(2.0, len(distinct_authors) * 0.8)
            signals.append("other_user_laughed")
        if len(laugh_after) >= 2:
            score += 0.8
            signals.append("multiple_laughs_after")

    before = messages[max(0, idx - 6) : idx]
    if before and before[-1].from_name != msg.from_name:
        score += 0.3
        signals.append("reply_like_turn")

    boost, boost_signals, _ = _recency_boost(msg.ts, latest_ts)
    score += boost
    signals.extend(boost_signals)

    return score, signals


def build_candidates(messages: List[MessageRecord], *, context_size: int, limit: int, seed: int) -> List[Dict[str, Any]]:
    candidates: List[Dict[str, Any]] = []
    parsed_timestamps = [ts for ts in (_parse_ts(m.ts) for m in messages) if ts is not None]
    latest_ts = max(parsed_timestamps) if parsed_timestamps else None
    for idx, msg in enumerate(messages):
        score, signals = score_candidate(messages, idx, latest_ts=latest_ts)
        laughs = laugh_responses_after(messages, idx)
        if score < 3.0 or not laughs:
            continue
        context = messages[max(0, idx - context_size) : idx]
        dialogue_after = messages[idx + 1 : idx + 5]
        _, _, recency_days = _recency_boost(msg.ts, latest_ts)
        payload = {
            "id": hashlib.sha1(f"{msg.message_id}|{msg.ts}|{msg.text}".encode("utf-8")).hexdigest()[:16],
            "score": round(score, 2),
            "signals": signals,
            "author": msg.from_name,
            "ts": msg.ts,
            "recency_days": recency_days,
            "context": [{"author": m.from_name, "text": m.text, "ts": m.ts} for m in context],
            "dialogue_after": [{"author": m.from_name, "text": m.text, "ts": m.ts} for m in dialogue_after],
            "laugh_responses": [{"author": m.from_name, "text": m.text, "ts": m.ts} for m in laughs],
            "good_reply": msg.text,
            "tags": [],
            "selected": None,
        }
        candidates.append(payload)

    candidates.sort(key=lambda x: (-float(x["score"]), str(x["id"])))
    if len(candidates) > limit:
        top = candidates[: max(limit // 2, 1)]
        rest = candidates[max(limit // 2, 1) :]
        rng = random.Random(seed)
        rng.shuffle(rest)
        candidates = top + rest[: max(limit - len(top), 0)]
        candidates.sort(key=lambda x: (-float(x["score"]), str(x["id"])))
    return candidates[:limit]


def main() -> int:
    parser = argparse.ArgumentParser(description="Export likely funny Telegram moments for manual/LLM curation")
    parser.add_argument("--src", required=True, help="Path to Telegram export directory")
    parser.add_argument("--out", default="data/funny_candidates.jsonl", help="Output jsonl path")
    parser.add_argument("--limit", type=int, default=500, help="Max candidates")
    parser.add_argument("--context", type=int, default=6, help="Messages before candidate")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for long-tail sampling")
    args = parser.parse_args()

    parsed = parse_export_dir(Path(args.src).expanduser().resolve())
    candidates = build_candidates(
        parsed.messages,
        context_size=max(1, args.context),
        limit=max(1, args.limit),
        seed=args.seed,
    )
    out = Path(args.out).expanduser().resolve()
    out.parent.mkdir(parents=True, exist_ok=True)
    with open(out, "w", encoding="utf-8") as f:
        for item in candidates:
            f.write(json.dumps(item, ensure_ascii=False) + "\n")

    print(f"wrote {len(candidates)} candidates to {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
