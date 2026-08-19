"""Blind A/B benchmark for Timur system prompts on anonymized real scenes.

Compares persona variants with the production direct-reply pipeline
(`build_chat_messages` + real LLM), scores replies with a blind judge and
deterministic cringe/length checks, then prints a ranked report. The tool
never mutates bot memory.

Usage:
    python3 -m timur_bot.tools.persona_ab [--scopes all|ambient|direct]
        [--variants A,B,C,D] [--no-judge] [--max-scenes N] [--out FILE]
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import random
import re
import statistics
import sys
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, List

try:
    from dotenv import load_dotenv

    load_dotenv(Path(__file__).resolve().parents[2] / ".env")
except Exception:
    pass

from timur_bot.core.config import load_app_config
from timur_bot.services.adaptive_humor import BAD_TEMPLATE_PATTERNS
from timur_bot.services import bot_logic as runtime

ROOT_DIR = Path(__file__).resolve().parents[2]
APP_CONFIG = load_app_config()
MODEL = APP_CONFIG.text_model

PERSONA_BASE = Path(ROOT_DIR / "config" / "persona.yaml").read_text(encoding="utf-8")
_PROD_SYSTEM_PROMPT = PERSONA_BASE.split("default_system_prompt:", 1)[1].split("\nmodes:", 1)[0]
_PROD_SYSTEM_PROMPT = "\n".join(
    line[2:] if line.startswith("  ") else "" for line in _PROD_SYSTEM_PROMPT.splitlines()
).strip()

VARIANTS: Dict[str, str] = {
    "A": _PROD_SYSTEM_PROMPT,
    "B": _PROD_SYSTEM_PROMPT.replace(
        "максимум 2 предложения, очень коротких. обычно это одна мысль в одном сообщении.",
        "максимум 2 предложения, очень коротких. обычно это одна мысль в одном сообщении.\n"
        "твоя цель по длине — одна короткая фраза, обычно 2-8 слов. длинный ответ почти никогда не смешной.",
    ).replace(
        "если шутишь, цепляйся за реальное противоречие или конкретную деталь сцены и добавляй одну новую мысль.",
        "если шутишь, цепляйся за реальное противоречие или конкретную деталь сцены и добавляй одну новую мысль.\n"
        "если точная добивка получается длиннее одной фразы, урезай до сути или молчи.",
    ),
    "C": "",
    "D": "",
}

VARIANTS["C"] = VARIANTS["B"] + (
    "\nкринж-фильтр: не старайся показаться смешным и не сам себя объясняй; не пиши, что это шутка, "
    "не обыгрывай то, что ты бот, не начинай с «ну» и не используй «понимаешь», «кстати», «слушай» как вводные. "
    "пиши как будто просто кинул реплику между делом: факт, подкол или уточнение. "
    "если нечего сказать точно — ответь по делу или промолчи."
)

VARIANTS["D"] = VARIANTS["C"] + (
    "\nрегистр: ты не разговариваешь, ты кидаешь реплику. одна деталь из сцены, без заходов со стороны. "
    "иногда достаточно одного слова: «ага», «ясно», «?», «нет конечно». "
    "не поучай, не давай советов без запроса, не дави иронией. "
    "лучший ответ — тот, после которого хочется продолжить болтать, а не тот, который расписан."
)

CRINGE_MARKERS = (
    "это когда",
    "а то я думал",
    "нейрон",
    "квант",
    "iq",
    "гений мысли",
    "комнатн",
    "ну,",
    "понимаешь",
    "собственно",
    "как говорится",
)

JUDGE_SYSTEM = (
    "ты — редактор, который выбирает, какая реплика человека в тумблере-чате выглядит смешнее и живее.\n"
    "сцена — переписка в групповом чате на русском. даны несколько анонимных ответов на последнюю сцену.\n"
    "оцени каждый ответ по трём шкалам 0-10:\n"
    "- funny: действительно смешно или остроумно, с привязкой к сцене; шаблонный нейрожурнал — 0-2\n"
    "- cringe: натужность, старание казаться смешным, избитые конструкции, длиннота; чистый спокойный ответ — 0\n"
    "- natural: звучит как сообщение реального 22-летнего пацана в чате, а не как послание ИИ\n"
    "длинные ответы (больше 12 слов) почти наверняка проигрывают коротким. чёткий заход со стороны, "
    "объяснение юмора, самоцитирование про бота — штраф по natural и cringe.\n"
    "верни строго json: {\"scores\":[{\"id\":\"A\",\"funny\":N,\"cringe\":N,\"natural\":N},...],\"best\":\"X\"}"
)


def _quality_scenes() -> List[Dict[str, Any]]:
    data = json.loads((ROOT_DIR / "tests" / "fixtures" / "humor_quality_scenes.json").read_text())
    return [scene for scene in data.get("scenes", []) if scene.get("route") == "ambient"]


def _direct_scenes() -> List[Dict[str, Any]]:
    data = json.loads((ROOT_DIR / "tests" / "fixtures" / "humor_replay.json").read_text())
    rows = data if isinstance(data, list) else data.get("scenes", [])
    return [scene for scene in rows if scene.get("route") == "direct"]


def _scene_to_case(scene: Dict[str, Any]) -> Dict[str, Any]:
    messages = [
        {"name": str(item.get("name") or "а"), "text": str(item.get("text") or "")}
        for item in scene.get("messages", [])
        if str(item.get("text") or "").strip()
    ]
    trigger = messages[-1] if messages else {"name": "а", "text": "привет"}
    history = messages[:-1]
    return {
        "id": str(scene.get("id") or "scene"),
        "category": str(scene.get("category") or ""),
        "trigger": trigger,
        "history": history,
    }


def _build_case_state(case: Dict[str, Any]) -> tuple[Dict[str, Any], SimpleNamespace]:
    memory = runtime.default_memory()
    chat = runtime.get_chat_mem(memory, 900)
    rows = []
    for index, item in enumerate(case["history"], start=1):
        rows.append(
            {"message_id": index, "user_id": 1, "name": item["name"], "text": item["text"], "ts": "2026-07-16T12:00:00"}
        )
    trigger_id = len(rows) + 1
    rows.append(
        {"message_id": trigger_id, "user_id": 1, "name": case["trigger"]["name"], "text": case["trigger"]["text"], "ts": "2026-07-16T12:00:20"}
    )
    chat["history"] = rows
    message = SimpleNamespace(
        chat_id=900,
        message_id=trigger_id,
        text=case["trigger"]["text"],
        caption=None,
        from_user=SimpleNamespace(id=1, first_name="а", username=None, is_bot=False),
        reply_to_message=None,
    )
    return memory, message


def objective_metrics(text: str) -> Dict[str, Any]:
    clean = text.strip()
    words = len(clean.split()) if clean else 0
    template_hits = sum(1 for pattern in BAD_TEMPLATE_PATTERNS if pattern.search(clean))
    cringe_hits = sum(1 for marker in CRINGE_MARKERS if marker in clean.lower())
    return {
        "chars": len(clean),
        "words": words,
        "template_hits": template_hits,
        "cringe_hits": cringe_hits,
        "starts_lowercase": bool(clean) and clean[0].islower(),
        "no_emoji": not re.search(r"[\U0001F300-\U0001FAFF\u2600-\u27BF]", clean),
        "suppressed_by_limit": len(clean) > int(runtime._adaptive_humor_settings(runtime.default_memory())["direct_reply_max_chars"]),
    }


async def generate_reply(variant_prompt: str, case: Dict[str, Any]) -> Dict[str, Any]:
    memory, message = _build_case_state(case)
    memory.setdefault("config", {})["system_prompt_override"] = variant_prompt
    humor_plan = runtime.build_humor_plan(memory, message)
    messages = runtime.build_chat_messages(memory, message, humor_plan=humor_plan)
    started = time.perf_counter()
    text = ""
    error = ""
    for attempt in range(3):
        try:
            text = (await runtime.call_openai_text(messages)).strip()
            error = ""
            if text:
                break
            error = "empty_response"
        except Exception as exc:
            error = str(exc)
        await asyncio.sleep(1.5 * (attempt + 1))
    latency_ms = int((time.perf_counter() - started) * 1000)
    return {"text": text, "latency_ms": latency_ms, "error": error}


def judge_scene(client: Any, scene_lines: List[str], entries: List[Dict[str, Any]]) -> Dict[str, Any]:
    replies_block = "\n".join(f"{entry['label']}: {entry['text'] or '(пусто)'}" for entry in entries)
    user_prompt = (
        "сцена (последняя строка — то, на что отвечают):\n"
        + "\n".join(scene_lines[-6:])
        + "\n\nответы:\n"
        + replies_block
        + "\n\nверни только json."
    )
    for attempt in range(3):
        try:
            response = client.chat.completions.create(
                model=MODEL,
                messages=[{"role": "system", "content": JUDGE_SYSTEM}, {"role": "user", "content": user_prompt}],
                max_tokens=260,
                temperature=0.0,
            )
            raw = (response.choices[0].message.content or "").strip()
            parsed = json.loads(raw[raw.find("{") : raw.rfind("}") + 1])
            scores = {str(row.get("id")).upper(): row for row in parsed.get("scores", []) if isinstance(row, dict)}
            if scores:
                return {"scores": scores, "best": str(parsed.get("best", "")).upper()}
        except Exception:
            time.sleep(1.5 * (attempt + 1))
    return {"scores": {}, "best": ""}


async def run_case(case: Dict[str, Any], variants: Dict[str, str]) -> Dict[str, Any]:
    outputs: Dict[str, Dict[str, Any]] = {}
    for label, prompt in variants.items():
        reply = await generate_reply(prompt, case)
        reply.update(objective_metrics(reply["text"]))
        outputs[label] = reply
        await asyncio.sleep(0.6)
    return {"case": case, "outputs": outputs}


def summarize(results: List[Dict[str, Any]], judge_data: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    labels = sorted({label for row in results for label in row["outputs"]})
    summary: Dict[str, Any] = {
        label: {
            "funny": [],
            "cringe": [],
            "natural": [],
            "best_votes": 0,
            "chars": [],
            "words": [],
            "template_hits": 0,
            "cringe_hits": 0,
            "empty": 0,
            "suppressed": 0,
            "latency_ms": [],
        }
        for label in labels
    }
    for row in results:
        for label, out in row["outputs"].items():
            bucket = summary[label]
            bucket["chars"].append(out["chars"])
            bucket["words"].append(out["words"])
            bucket["template_hits"] += out["template_hits"]
            bucket["cringe_hits"] += out["cringe_hits"]
            bucket["empty"] += 1 if not out["text"] else 0
            bucket["suppressed"] += 1 if out.get("suppressed_by_limit") else 0
            bucket["latency_ms"].append(out["latency_ms"])
            judge = judge_data.get(row["case"]["id"], {}) if judge_data else {}
            score = judge.get("scores", {}).get(label, {}) if judge else {}
            if score:
                bucket["funny"].append(float(score.get("funny", 0)))
                bucket["cringe"].append(float(score.get("cringe", 0)))
                bucket["natural"].append(float(score.get("natural", 0)))
            if judge.get("best") == label:
                bucket["best_votes"] += 1
    ranked: List[Dict[str, Any]] = []
    for label, bucket in summary.items():
        funny = statistics.fmean(bucket["funny"]) if bucket["funny"] else None
        cringe = statistics.fmean(bucket["cringe"]) if bucket["cringe"] else None
        natural = statistics.fmean(bucket["natural"]) if bucket["natural"] else None
        judged = all(value is not None for value in (funny, cringe, natural))
        composite = None
        if judged:
            composite = round(
                funny - 0.7 * cringe + 0.5 * natural - 0.15 * statistics.fmean(bucket["words"])
                - 0.6 * bucket["template_hits"],
                2,
            )
        ranked.append(
            {
                "variant": label,
                "composite": composite,
                "funny_avg": round(funny, 2) if funny is not None else None,
                "cringe_avg": round(cringe, 2) if cringe is not None else None,
                "natural_avg": round(natural, 2) if natural is not None else None,
                "best_votes": bucket["best_votes"],
                "chars_avg": round(statistics.fmean(bucket["chars"]), 1),
                "words_avg": round(statistics.fmean(bucket["words"]), 1),
                "template_hits": bucket["template_hits"],
                "cringe_hits": bucket["cringe_hits"],
                "empty": bucket["empty"],
                "suppressed": bucket["suppressed"],
                "latency_ms_avg": round(statistics.fmean(bucket["latency_ms"])),
            }
        )
    ranked.sort(key=lambda item: (item["composite"] if item["composite"] is not None else -999), reverse=True)
    return {"ranked": ranked, "scenes": len(results)}


def print_report(label_map: Dict[str, str], summary: Dict[str, Any]) -> None:
    print(f"\nСЦЕН: {summary['scenes']}")
    header = f"{'#':<3}{'вариант':<9}{'балл':<8}{'funny':<7}{'cringe':<8}{'живо':<7}{'best':<6}{'знак':<7}{'слов':<6}{'темп':<6}{'крж':<5}{'пуст':<6}{'подав':<6}{'мс':<7}"
    print(header)
    print("-" * 88)
    for index, row in enumerate(summary["ranked"], start=1):
        composite = row["composite"] if row["composite"] is not None else "—"
        print(
            f"{index:<3}{row['variant']:<9}{composite:<8}{row['funny_avg'] if row['funny_avg'] is not None else '—':<7}"
            f"{row['cringe_avg'] if row['cringe_avg'] is not None else '—':<8}{row['natural_avg'] if row['natural_avg'] is not None else '—':<7}"
            f"{row['best_votes']:<6}{row['chars_avg']:<7}{row['words_avg']:<6}{row['template_hits']:<6}{row['cringe_hits']:<5}"
            f"{row['empty']:<6}{row['suppressed']:<6}{row['latency_ms_avg']:<7}"
        )
    winner = summary["ranked"][0]["variant"] if summary["ranked"] else "?"
    print(f"\nПОБЕДИТЕЛЬ: {winner} → {label_map.get(winner, winner)}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scopes", default="all", choices=["all", "ambient", "direct"], help="какие сцены гонять")
    parser.add_argument("--variants", default="A,B,C,D", help="какие варианты сравнивать, через запятую")
    parser.add_argument("--no-judge", action="store_true", help="пропустить слепого судью, только объективные метрики")
    parser.add_argument("--max-scenes", type=int, default=0, help="ограничить число сцен (0 — все)")
    parser.add_argument("--out", default="", help="файл json-отчёта")
    args = parser.parse_args()

    random.seed(42)
    variants = {label.upper(): VARIANTS[label.upper()] for label in args.variants.split(",") if label.strip().upper() in VARIANTS}
    if not variants:
        sys.exit("нет выбранных вариантов")

    cases: List[Dict[str, Any]] = []
    if args.scopes in ("all", "ambient"):
        cases.extend(_scene_to_case(scene) for scene in _quality_scenes())
    if args.scopes in ("all", "direct"):
        cases.extend(_scene_to_case(scene) for scene in _direct_scenes())
    if args.max_scenes > 0:
        cases = cases[: args.max_scenes]
    if not cases:
        sys.exit("нет сцен для прогона")

    label_map = {
        "A": "baseline (текущий)",
        "B": "короче",
        "C": "короче + анти-кринж",
        "D": "короче + анти-кринж + живой пацан",
    }
    print(f"MODEL={MODEL}  сцен={len(cases)}  вариантов={','.join(sorted(variants))}  judge={'off' if args.no_judge else 'on'}")

    results: List[Dict[str, Any]] = []
    for index, case in enumerate(cases, start=1):
        row = asyncio.run(_run_case_bound(variants, case))
        results.append(row)
        outputs_preview = "; ".join(f"{label}={out['text'][:40]!r}" for label, out in row["outputs"].items())
        print(f"[{index}/{len(cases)}] {case['id']}: {outputs_preview}")

    judge_data: Dict[str, Dict[str, Any]] = {}
    if not args.no_judge:
        from openai import OpenAI

        client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"), base_url=os.getenv("OPENAI_BASE_URL") or None)
        for row in results:
            scene_lines = [f"{item['name']}: {item['text']}" for item in row["case"]["history"]] + [
                f"{row['case']['trigger']['name']}: {row['case']['trigger']['text']}"
            ]
            entries = [{"label": label, **out} for label, out in row["outputs"].items()]
            random.shuffle(entries)
            judge_data[row["case"]["id"]] = judge_scene(client, scene_lines, entries)

    summary = summarize(results, judge_data)
    print_report(label_map, summary)

    if args.out:
        report = {
            "model": MODEL,
            "scope": args.scopes,
            "variants": {label: label_map.get(label, label) for label in variants},
            "judge": not args.no_judge,
            "summary": summary,
            "results": [
                {
                    "case_id": row["case"]["id"],
                    "category": row["case"]["category"],
                    "trigger": row["case"]["trigger"]["text"],
                    "outputs": {
                        label: {
                            "text": out["text"],
                            "chars": out["chars"],
                            "words": out["words"],
                            "template_hits": out["template_hits"],
                            "cringe_hits": out["cringe_hits"],
                            "suppressed_by_limit": out["suppressed_by_limit"],
                            "latency_ms": out["latency_ms"],
                        }
                        for label, out in row["outputs"].items()
                    },
                    "judge": judge_data.get(row["case"]["id"], {}),
                }
                for row in results
            ],
        }
        Path(args.out).write_text(json.dumps(report, ensure_ascii=False, indent=1), encoding="utf-8")
        print(f"\nотчёт записан: {args.out}")

    if judge_data:
        print("\nСЛЕПОЙ СУДЬЯ по сценам:")
        for row in results:
            judge = judge_data.get(row["case"]["id"], {})
            best = judge.get("best") or "—"
            score_line = "  ".join(
                f"{label}:f{int(score.get('funny', 0))}/c{int(score.get('cringe', 0))}/n{int(score.get('natural', 0))}"
                for label, score in sorted(judge.get("scores", {}).items())
            )
            print(f"  {row['case']['id']:<32} best={best}  {score_line}")


async def _run_case_bound(variants: Dict[str, str], case: Dict[str, Any]) -> Dict[str, Any]:
    return await run_case(case, variants)


if __name__ == "__main__":
    main()
