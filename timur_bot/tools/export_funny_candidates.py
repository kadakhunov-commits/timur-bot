from __future__ import annotations

import argparse
import hashlib
import json
import random
import re
from pathlib import Path
from typing import Any, Dict, List

from timur_bot.tools.import_telegram_html import MessageRecord, parse_export_dir

LAUGH_MARKERS = ("лол", "ахах", "ахаха", "пхаха", "хаха", "ору", "ор", "кек")
MEME_MARKERS = ("кринж", "угар", "мем", "прикол", "база", "жесть")
PROFANITY_HINTS = ("бля", "блять", "нах", "хуй", "пизд", "еб", "сука")


def _norm(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip())


def score_candidate(messages: List[MessageRecord], idx: int) -> tuple[float, List[str]]:
    msg = messages[idx]
    text = _norm(msg.text)
    low = text.lower()
    score = 0.0
    signals: List[str] = []

    if 2 <= len(text) <= 90:
        score += 1.0
        signals.append("short")
    if any(x in low for x in LAUGH_MARKERS):
        score += 2.0
        signals.append("laugh_marker")
    if any(x in low for x in MEME_MARKERS):
        score += 1.2
        signals.append("meme_marker")
    if any(x in low for x in PROFANITY_HINTS):
        score += 0.6
        signals.append("chat_roughness")
    if "?" not in text and len(text) <= 45:
        score += 0.4
        signals.append("punchy_shape")

    after = messages[idx + 1 : idx + 5]
    if any(any(x in m.text.lower() for x in LAUGH_MARKERS) for m in after):
        score += 3.0
        signals.append("nearby_laugh_after")

    before = messages[max(0, idx - 6) : idx]
    if before and before[-1].from_name != msg.from_name:
        score += 0.3
        signals.append("reply_like_turn")

    return score, signals


def build_candidates(messages: List[MessageRecord], *, context_size: int, limit: int, seed: int) -> List[Dict[str, Any]]:
    candidates: List[Dict[str, Any]] = []
    for idx, msg in enumerate(messages):
        score, signals = score_candidate(messages, idx)
        if score < 2.2:
            continue
        context = messages[max(0, idx - context_size) : idx]
        payload = {
            "id": hashlib.sha1(f"{msg.message_id}|{msg.ts}|{msg.text}".encode("utf-8")).hexdigest()[:16],
            "score": round(score, 2),
            "signals": signals,
            "author": msg.from_name,
            "ts": msg.ts,
            "context": [{"author": m.from_name, "text": m.text, "ts": m.ts} for m in context],
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
