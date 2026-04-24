from __future__ import annotations

import argparse
import json
import os
import re
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence

from openai import OpenAI

from timur_bot.services.humor import add_funny_example, add_joke_bit
from timur_bot.tools.export_funny_candidates import build_candidates
from timur_bot.tools.import_telegram_html import _backup_memory, ensure_chat_schema, load_memory, parse_export_dir, save_memory


DEFAULT_OUT = "data/funny_curated.jsonl"
DEFAULT_MODEL = "gpt-4.1"


def _chunked(items: Sequence[Dict[str, Any]], size: int) -> Iterable[List[Dict[str, Any]]]:
    for idx in range(0, len(items), size):
        yield list(items[idx : idx + size])


def _extract_json_object(text: str) -> Dict[str, Any]:
    """Accept strict JSON or a fenced JSON blob from OpenAI-compatible APIs."""
    raw = (text or "").strip()
    if not raw:
        return {}

    fence = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", raw, flags=re.DOTALL | re.IGNORECASE)
    if fence:
        raw = fence.group(1).strip()
    elif not raw.startswith("{"):
        start = raw.find("{")
        end = raw.rfind("}")
        if start >= 0 and end > start:
            raw = raw[start : end + 1]

    parsed = json.loads(raw)
    if not isinstance(parsed, dict):
        raise ValueError("curator response must be a JSON object")
    return parsed


def _normalize_selection_payload(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    selected = payload.get("selected") or []
    if not isinstance(selected, list):
        return []

    normalized: List[Dict[str, Any]] = []
    for item in selected:
        if isinstance(item, str):
            normalized.append({"id": item, "rating": 8, "tags": [], "reason": ""})
            continue
        if not isinstance(item, dict):
            continue
        item_id = str(item.get("id") or "").strip()
        if not item_id:
            continue
        try:
            rating = int(float(item.get("rating", 8)))
        except Exception:
            rating = 8
        tags_raw = item.get("tags") or []
        tags = [str(x).strip() for x in tags_raw if str(x).strip()] if isinstance(tags_raw, list) else []
        normalized.append(
            {
                "id": item_id,
                "rating": max(1, min(10, rating)),
                "tags": tags[:6],
                "reason": str(item.get("reason") or item.get("why_funny") or "").strip()[:260],
            }
        )
    return normalized


def _compact_candidate_for_model(item: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": item.get("id"),
        "score": item.get("score"),
        "signals": item.get("signals") or [],
        "author": item.get("author"),
        "ts": item.get("ts"),
        "recency_days": item.get("recency_days"),
        "context": item.get("context") or [],
        "dialogue_after": item.get("dialogue_after") or [],
        "laugh_responses": item.get("laugh_responses") or [],
        "candidate_reply": item.get("good_reply"),
    }


def build_curator_messages(batch: Sequence[Dict[str, Any]], *, select_per_batch: int) -> List[Dict[str, str]]:
    payload = [_compact_candidate_for_model(item) for item in batch]
    system = (
        "Ты отбираешь смешные обучающие примеры для Telegram-бота Тимура. "
        "Цель: бот должен звучать как живой кент из чата: коротко, сухо, локально, абсурдно, иногда с добивкой. "
        "Оценивай реплику только вместе с диалогом вокруг нее: context -> candidate_reply -> dialogue_after/laugh_responses. "
        "Главное доказательство юмора: после candidate_reply другие участники отвечают смехом, 'лол', 'ахаха' или похожей реакцией. "
        "При равном качестве предпочитай более новые примеры (меньше recency_days), потому что они ближе к текущему стилю чата. "
        "Не выбирай реплики, которые смешные только потому что там прямое оскорбление, травля, тупой мат или голая токсичность. "
        "Хороший пример: у него есть контекстный тайминг, неожиданность, локальный вайб, сухая формулировка или удачный callback. "
        "Плохой пример: просто 'ты дебил', длинный стендап, объяснение шутки, слишком личное унижение."
    )
    user = (
        f"Выбери максимум {select_per_batch} лучших примеров из списка. "
        "Если хороших меньше, выбери меньше. Верни строго JSON без комментариев:\n"
        "{\n"
        '  "selected": [\n'
        '    {"id": "candidate-id", "rating": 1-10, "tags": ["deadpan|callback|absurd|local|roast_light"], "reason": "почему это полезно Тимуру"}\n'
        "  ]\n"
        "}\n\n"
        "Кандидаты:\n"
        f"{json.dumps(payload, ensure_ascii=False)}"
    )
    return [{"role": "system", "content": system}, {"role": "user", "content": user}]


def call_curator(
    client: OpenAI,
    *,
    model: str,
    batch: Sequence[Dict[str, Any]],
    select_per_batch: int,
    temperature: float,
    use_response_format: bool = True,
) -> List[Dict[str, Any]]:
    kwargs: Dict[str, Any] = {
        "model": model,
        "messages": build_curator_messages(batch, select_per_batch=select_per_batch),
        "temperature": temperature,
    }
    if use_response_format:
        kwargs["response_format"] = {"type": "json_object"}

    response = client.chat.completions.create(**kwargs)
    content = response.choices[0].message.content or ""
    return _normalize_selection_payload(_extract_json_object(content))


def apply_selections(
    candidates: Sequence[Dict[str, Any]],
    selections: Sequence[Dict[str, Any]],
    *,
    min_rating: int,
) -> List[Dict[str, Any]]:
    by_id = {str(item.get("id")): dict(item) for item in candidates}
    for item in by_id.values():
        item["selected"] = False

    for selection in selections:
        item_id = str(selection.get("id") or "")
        if item_id not in by_id:
            continue
        rating = int(selection.get("rating") or 0)
        by_id[item_id]["curator_rating"] = rating
        by_id[item_id]["curator_reason"] = str(selection.get("reason") or "")
        if selection.get("tags"):
            existing_tags = [str(x) for x in (by_id[item_id].get("tags") or [])]
            merged = list(dict.fromkeys(existing_tags + [str(x) for x in selection.get("tags", [])]))
            by_id[item_id]["tags"] = merged[:8]
        by_id[item_id]["selected"] = rating >= min_rating

    return [by_id[str(item.get("id"))] for item in candidates]


def write_jsonl(path: Path, items: Iterable[Dict[str, Any]]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with open(path, "w", encoding="utf-8") as f:
        for item in items:
            f.write(json.dumps(item, ensure_ascii=False) + "\n")
            count += 1
    return count


def import_selected_examples(
    *,
    items: Sequence[Dict[str, Any]],
    chat_id: int,
    memory_path: Path,
    dry_run: bool,
) -> Dict[str, Any]:
    memory = load_memory(memory_path)
    chat = ensure_chat_schema(memory.setdefault("chats", {}).setdefault(str(chat_id), {}))
    imported = 0
    skipped = 0

    for item in items:
        if item.get("selected") is not True:
            skipped += 1
            continue
        example = add_funny_example(
            chat,
            context=item.get("context") or [],
            good_reply=str(item.get("good_reply") or ""),
            tags=[str(x) for x in (item.get("tags") or [])],
            source="llm_curated",
            weight=5.0,
        )
        if example:
            add_joke_bit(chat, str(item.get("good_reply") or ""), source="llm_curated_example", weight=2.5)
            imported += 1

    backup = None
    if not dry_run:
        if memory_path.exists():
            backup = str(_backup_memory(memory_path))
        save_memory(memory_path, memory)

    return {"imported": imported, "skipped": skipped, "backup": backup}


def curate_candidates(
    *,
    candidates: Sequence[Dict[str, Any]],
    api_key: str,
    base_url: str,
    model: str,
    batch_size: int,
    select_per_batch: int,
    min_rating: int,
    temperature: float,
    sleep_seconds: float,
    use_response_format: bool,
) -> List[Dict[str, Any]]:
    client_kwargs: Dict[str, Any] = {"api_key": api_key}
    if base_url:
        client_kwargs["base_url"] = base_url
    client = OpenAI(**client_kwargs)

    selections: List[Dict[str, Any]] = []
    batches = list(_chunked(list(candidates), max(1, batch_size)))
    for idx, batch in enumerate(batches, start=1):
        print(f"curating batch {idx}/{len(batches)} ({len(batch)} candidates)")
        try:
            selections.extend(
                call_curator(
                    client,
                    model=model,
                    batch=batch,
                    select_per_batch=max(1, select_per_batch),
                    temperature=temperature,
                    use_response_format=use_response_format,
                )
            )
        except Exception as exc:
            if not use_response_format:
                raise
            print(f"response_format failed for batch {idx}: {exc}; retrying without response_format")
            selections.extend(
                call_curator(
                    client,
                    model=model,
                    batch=batch,
                    select_per_batch=max(1, select_per_batch),
                    temperature=temperature,
                    use_response_format=False,
                )
            )
        if sleep_seconds > 0 and idx < len(batches):
            time.sleep(sleep_seconds)

    return apply_selections(candidates, selections, min_rating=min_rating)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Export Telegram funny candidates, curate them with an OpenAI-compatible model, and import them into Timur."
    )
    parser.add_argument("--src", required=True, help="Path to Telegram HTML export directory")
    parser.add_argument("--chat-id", type=int, required=True, help="Target chat id in memory.json")
    parser.add_argument("--api-key", default=os.getenv("OPENAI_API_KEY", ""), help="OpenAI-compatible API key")
    parser.add_argument("--base-url", default=os.getenv("OPENAI_BASE_URL", ""), help="OpenAI-compatible base URL")
    parser.add_argument("--model", default=os.getenv("FUNNY_CURATOR_MODEL", DEFAULT_MODEL), help="Curator model")
    parser.add_argument("--memory-path", default="memory.json", help="Path to memory.json")
    parser.add_argument("--out", default=DEFAULT_OUT, help="Curated JSONL output path")
    parser.add_argument("--limit", type=int, default=600, help="Max heuristic candidates before LLM curation")
    parser.add_argument("--context", type=int, default=8, help="Messages before candidate sent to curator")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for long-tail candidate sampling")
    parser.add_argument("--batch-size", type=int, default=25, help="Candidates per model request")
    parser.add_argument("--select-per-batch", type=int, default=5, help="Max selected items per batch")
    parser.add_argument("--min-rating", type=int, default=8, help="Only import selected items with rating >= this value")
    parser.add_argument("--temperature", type=float, default=0.2, help="Curator model temperature")
    parser.add_argument("--sleep", type=float, default=0.0, help="Seconds to sleep between batches")
    parser.add_argument("--curate-only", action="store_true", help="Write curated JSONL, but do not update memory.json")
    parser.add_argument("--dry-run", action="store_true", help="Run curation and report import counts without writing memory.json")
    parser.add_argument("--no-response-format", action="store_true", help="Do not request JSON response_format from the API")
    args = parser.parse_args()

    if not args.api_key:
        raise SystemExit("missing --api-key or OPENAI_API_KEY")

    parsed = parse_export_dir(Path(args.src).expanduser().resolve())
    candidates = build_candidates(
        parsed.messages,
        context_size=max(1, args.context),
        limit=max(1, args.limit),
        seed=args.seed,
    )
    print(f"parsed messages: {len(parsed.messages)}")
    print(f"heuristic candidates: {len(candidates)}")

    curated = curate_candidates(
        candidates=candidates,
        api_key=args.api_key,
        base_url=args.base_url,
        model=args.model,
        batch_size=args.batch_size,
        select_per_batch=args.select_per_batch,
        min_rating=max(1, min(10, args.min_rating)),
        temperature=args.temperature,
        sleep_seconds=max(0.0, args.sleep),
        use_response_format=not args.no_response_format,
    )

    selected_count = sum(1 for item in curated if item.get("selected") is True)
    out = Path(args.out).expanduser().resolve()
    written = write_jsonl(out, curated)
    print(f"curated file: {out}")
    print(f"written candidates: {written}")
    print(f"selected for Timur: {selected_count}")

    if args.curate_only:
        print("curate-only: memory.json was not changed")
        return 0

    result = import_selected_examples(
        items=curated,
        chat_id=args.chat_id,
        memory_path=Path(args.memory_path).expanduser().resolve(),
        dry_run=args.dry_run,
    )
    if args.dry_run:
        print("dry-run: memory.json was not changed")
    elif result.get("backup"):
        print(f"backup created: {result['backup']}")
    print(f"imported examples: {result['imported']}")
    print(f"skipped examples: {result['skipped']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
