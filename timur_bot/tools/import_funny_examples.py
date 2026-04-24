from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict

from timur_bot.services.humor import add_funny_example, add_joke_bit
from timur_bot.tools.import_telegram_html import _backup_memory, ensure_chat_schema, load_memory, save_memory


def _is_selected(item: Dict[str, Any], min_score: float) -> bool:
    selected = item.get("selected")
    if selected is True:
        return True
    if isinstance(selected, str) and selected.strip().lower() in {"true", "yes", "1", "funny", "good"}:
        return True
    try:
        return float(item.get("score", 0.0)) >= min_score
    except Exception:
        return False


def main() -> int:
    parser = argparse.ArgumentParser(description="Import curated funny examples into Timur memory")
    parser.add_argument("--src", required=True, help="JSONL from export_funny_candidates")
    parser.add_argument("--chat-id", type=int, required=True, help="Target chat id")
    parser.add_argument("--memory-path", default="memory.json", help="Path to memory.json")
    parser.add_argument("--min-score", type=float, default=999.0, help="Fallback score threshold when selected is empty")
    parser.add_argument("--dry-run", action="store_true", help="Report only")
    args = parser.parse_args()

    src = Path(args.src).expanduser().resolve()
    memory_path = Path(args.memory_path).expanduser().resolve()
    memory = load_memory(memory_path)
    chat = ensure_chat_schema(memory.setdefault("chats", {}).setdefault(str(args.chat_id), {}))

    imported = 0
    skipped = 0
    with open(src, "r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            item = json.loads(line)
            if not _is_selected(item, args.min_score):
                skipped += 1
                continue
            example = add_funny_example(
                chat,
                context=item.get("context") or [],
                good_reply=str(item.get("good_reply") or ""),
                tags=[str(x) for x in (item.get("tags") or [])],
                source="curated",
                weight=4.0,
            )
            if example:
                add_joke_bit(chat, str(item.get("good_reply") or ""), source="curated_example", weight=2.0)
                imported += 1

    print(f"imported examples: {imported}")
    print(f"skipped examples: {skipped}")
    if args.dry_run:
        return 0
    if memory_path.exists():
        backup = _backup_memory(memory_path)
        print(f"backup created: {backup}")
    save_memory(memory_path, memory)
    print(f"memory updated: {memory_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
