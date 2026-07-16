"""Live blind replay benchmark for the pinned legacy and current v2 workflows.

Ambient scenes compare the exact old one-shot prompt from the previous runtime
against the current Director/Writer -> hard filters -> Critic pipeline. Direct
and daily-lore scenes are contract checks because they are not ambient jokes.
The command never mutates bot memory.
"""

from __future__ import annotations

import argparse
import json
import random
import re
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence

from openai import OpenAI

from timur_bot.core.config import load_app_config
from timur_bot.services.adaptive_humor import (
    critic_messages,
    director_writer_messages,
    filter_candidates,
    parse_critic,
    parse_director,
)
from timur_bot.services.humor import ensure_daily_signature


ROOT_DIR = Path(__file__).resolve().parents[2]
DEFAULT_FIXTURE = ROOT_DIR / "tests" / "fixtures" / "humor_replay.json"
VALID_ACTIONS = {"ANSWER", "JOKE", "SILENCE", "DAILY_LORE"}
VALID_ROUTES = {"ambient", "direct", "daily_lore"}
ROUTE_ACTIONS = {
    "ambient": {"JOKE", "SILENCE"},
    "direct": {"ANSWER"},
    "daily_lore": {"DAILY_LORE"},
}
VALID_SEMANTIC_CONTRACTS = {
    "model_identity",
    "weather_uncertainty",
    "differential_explanation",
    "dialogue_reason",
    "choice_criteria",
    "bot_identity",
}
DEFAULT_MAX_API_CALLS = 320


class ReplayCallError(RuntimeError):
    pass


class ReplayLLM:
    def __init__(self, client: OpenAI, model: str) -> None:
        self.client = client
        self.model = model
        self.calls = 0
        self.tokens = 0
        self.api_errors = 0

    def complete(
        self,
        messages: List[Dict[str, str]],
        *,
        max_tokens: int,
        temperature: float,
    ) -> str:
        self.calls += 1
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=messages,
                max_tokens=max_tokens,
                temperature=temperature,
            )
            text = (response.choices[0].message.content or "").strip()
            usage = getattr(response, "usage", None)
            total_tokens = max(0, int(getattr(usage, "total_tokens", 0) or 0))
            if total_tokens <= 0:
                input_bytes = sum(len(str(item.get("content", "")).encode("utf-8")) + 16 for item in messages) + 16
                output_ceiling = min(max(1, int(max_tokens)), len(text.encode("utf-8"))) if text else 0
                total_tokens = max(1, input_bytes + output_ceiling)
        except Exception as exc:
            self.api_errors += 1
            raise ReplayCallError(str(exc)) from exc
        self.tokens += total_tokens
        return text


def load_replay_fixture(path: Path = DEFAULT_FIXTURE) -> List[Dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    scenes = payload.get("scenes") if isinstance(payload, dict) else None
    if not isinstance(scenes, list):
        raise ValueError("replay fixture must contain a scenes list")
    validate_replay_scenes(scenes)
    return scenes


def validate_replay_scenes(scenes: Sequence[Dict[str, Any]]) -> None:
    ids: set[str] = set()
    for scene in scenes:
        if not isinstance(scene, dict):
            raise ValueError("every replay scene must be an object")
        scene_id = str(scene.get("id", "")).strip()
        if not scene_id or scene_id in ids:
            raise ValueError(f"invalid or duplicate scene id: {scene_id!r}")
        ids.add(scene_id)
        if "route" not in scene:
            raise ValueError(f"{scene_id}: route is required")
        route = str(scene["route"])
        if route not in VALID_ROUTES:
            raise ValueError(f"{scene_id}: invalid route {route!r}")
        expected_action = scene.get("expected_action")
        if expected_action not in VALID_ACTIONS:
            raise ValueError(f"{scene_id}: invalid expected_action")
        if expected_action not in ROUTE_ACTIONS[route]:
            raise ValueError(f"{scene_id}: expected_action does not match route")
        messages = scene.get("messages")
        if not isinstance(messages, list) or not messages:
            raise ValueError(f"{scene_id}: messages must be non-empty")
        for message in messages:
            if not isinstance(message, dict) or not str(message.get("text", "")).strip():
                raise ValueError(f"{scene_id}: invalid message")
        for field in ("required_all_phrases", "required_any_phrases", "forbidden_names", "forbidden_phrases"):
            value = scene.get(field, [])
            if not isinstance(value, list) or any(not str(item).strip() for item in value):
                raise ValueError(f"{scene_id}: {field} must be a list of non-empty strings")
        if "critical" in scene and not isinstance(scene.get("critical"), bool):
            raise ValueError(f"{scene_id}: critical must be boolean")
        if route == "direct":
            semantic_contract = str(scene.get("semantic_contract", ""))
            if semantic_contract not in VALID_SEMANTIC_CONTRACTS:
                raise ValueError(f"{scene_id}: direct scene needs a known semantic_contract")


def validate_compare(model_names: Sequence[str]) -> None:
    if len(model_names) != 2 or len(set(model_names)) != 2 or set(model_names) != {"legacy", "v2"}:
        raise ValueError("--compare must contain legacy,v2 exactly once")


def estimate_max_api_calls(scenes: Sequence[Dict[str, Any]], runs: int) -> int:
    validate_replay_scenes(scenes)
    per_run = 0
    for scene in scenes:
        route = str(scene.get("route", "ambient"))
        if route == "ambient":
            per_run += 4  # legacy + writer + possible critic + blind judge
        elif route == "direct":
            per_run += 2  # direct answer + independent semantic contract judge
    return per_run * max(1, int(runs))


def _history(scene: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for index, item in enumerate(scene["messages"], start=1):
        row = dict(item)
        row.setdefault("message_id", index)
        rows.append(row)
    return rows[-8:]


def _strict_json(text: str) -> Dict[str, Any]:
    raw = (text or "").strip()
    if not raw.startswith("{") or not raw.endswith("}"):
        return {}
    try:
        value = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    return value if isinstance(value, dict) else {}


def _valid_director_schema(payload: Dict[str, Any]) -> bool:
    required = {"should_attempt", "setup", "target", "scene_type", "relation", "forbidden_moves", "candidates"}
    if set(payload) != required:
        return False
    if (
        not isinstance(payload.get("should_attempt"), bool)
        or not all(isinstance(payload.get(key), str) for key in ("setup", "target", "scene_type", "relation"))
        or not isinstance(payload.get("forbidden_moves"), list)
        or not all(isinstance(item, str) for item in payload.get("forbidden_moves", []))
        or not isinstance(payload.get("candidates"), list)
    ):
        return False
    candidates = payload["candidates"]
    if payload["should_attempt"] is False:
        return candidates == []
    if len(candidates) != 4:
        return False
    return all(
        isinstance(item, dict)
        and set(item) == {"text", "mechanism", "callback_key"}
        and all(isinstance(item.get(key), str) for key in ("text", "mechanism", "callback_key"))
        and bool(str(item.get("text", "")).strip())
        for item in candidates
    )


def _valid_critic_schema(payload: Dict[str, Any], *, candidate_count: int) -> bool:
    if set(payload) != {"winner_index", "score", "reason_codes"}:
        return False
    winner = payload.get("winner_index")
    if winner is not None and (not isinstance(winner, int) or isinstance(winner, bool)):
        return False
    if winner is not None and not 0 <= winner < candidate_count:
        return False
    score = payload.get("score")
    reasons = payload.get("reason_codes")
    return (
        isinstance(score, int)
        and not isinstance(score, bool)
        and 0 <= score <= 100
        and isinstance(reasons, list)
        and all(isinstance(item, str) for item in reasons)
    )


def _legacy_json(text: str) -> Dict[str, Any]:
    """Pinned parser from the previous adaptive_humor.py."""
    raw = (text or "").strip()
    if not raw:
        return {}
    if not raw.startswith("{"):
        start, end = raw.find("{"), raw.rfind("}")
        if start >= 0 and end > start:
            raw = raw[start : end + 1]
    try:
        value = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    return value if isinstance(value, dict) else {}


def _legacy_interjection_messages(history: Iterable[Dict[str, Any]]) -> List[Dict[str, str]]:
    """Exact one-shot system prompt pinned from the previous commit."""
    lines = []
    rows = list(history)[-8:]
    for row in rows:
        text = re.sub(r"\s+", " ", str(row.get("text", ""))).strip()
        if not text:
            continue
        author = str(row.get("name") or row.get("username") or row.get("user_id") or "кто-то")
        lines.append(f"{author}: {text[:220]}")
    target = str(rows[-1].get("name") or rows[-1].get("user_id") or "чат") if rows else "чат"
    humor_hint = "\n".join(
        [
            "ориентир для шутки (не жесткая инструкция):",
            "- режим: deadpan",
            f"- цель: {target}",
            "- если ложится по контексту: сухая короткая добивка, будто это очевидный провал собеседника",
            "- если подсказка мешает, игнорируй ее и ответь естественно по вайбу чата",
            "- без личных наездов и унижения интеллекта",
        ]
    )
    return [
        {
            "role": "system",
            "content": (
                "Ты одновременно автор и строгий редактор короткой реплики для живого русского чата. "
                "Мысленно придумай три варианта и верни только лучший в JSON "
                '{"score":0..100,"reply":"текст или пусто"}. Score выше 85 — только если реплика '
                "точная, контекстная и лучше молчания. Не повторяй последнее слово или оговорку, не "
                "пересказывай контекст, не шути о качестве своего юмора и не упоминай ИИ. Если сильной "
                "реплики нет, reply должен быть пустым."
            ),
        },
        {"role": "user", "content": f"чат:\n{chr(10).join(lines)}\n\nориентиры вкуса:\n{humor_hint}"},
    ]


def _direct_messages(
    persona: str,
    history: Sequence[Dict[str, Any]],
    model_label: str,
) -> List[Dict[str, str]]:
    instruction = (
        "сначала ответь по существу. шутка необязательна. максимум 120 знаков и два коротких предложения. "
        "не вводи отсутствующих людей, не пиши «x — это когда» или «а то я думал». "
        f"если вопрос о модели, честный ответ: {model_label}."
    )
    lines: List[str] = []
    for row in history[-8:]:
        marker = " тимур" if row.get("is_bot") else ""
        reply = f" reply=#{row['reply_to_message_id']}" if row.get("reply_to_message_id") else ""
        row_text = re.sub(r"\s+", " ", str(row.get("text", ""))).strip()
        lines.append(
            f"#{row.get('message_id')} {row.get('name', 'кто-то')}{marker}{reply}: "
            f"{row_text}"
        )
    return [
        {"role": "system", "content": persona + "\n\n" + instruction},
        {"role": "user", "content": "чат:\n" + "\n".join(lines) + "\n\nответь на последнюю реплику"},
    ]


def run_legacy(llm: ReplayLLM, _persona: str, scene: Dict[str, Any]) -> Dict[str, str]:
    history = _history(scene)
    raw = llm.complete(_legacy_interjection_messages(history), max_tokens=90, temperature=0.7)
    payload = _legacy_json(raw)
    if not payload:
        return {"action": "ERROR", "text": "", "error": "legacy_invalid_json"}
    try:
        score = max(0, min(100, int(payload.get("score", 0))))
    except (TypeError, ValueError):
        score = 0
    text = re.sub(r"\s+", " ", str(payload.get("reply", ""))).strip()[:280]
    if score < 85 or not text:
        return {"action": "SILENCE", "text": ""}
    return {"action": "JOKE", "text": text}


def run_v2_ambient(llm: ReplayLLM, _persona: str, scene: Dict[str, Any]) -> Dict[str, Any]:
    history = _history(scene)
    writer_raw = llm.complete(director_writer_messages(history), max_tokens=180, temperature=0.9)
    writer_payload = _strict_json(writer_raw)
    if not writer_payload:
        return {"action": "ERROR", "text": "", "error": "writer_invalid_json", "trace": {}}
    if not _valid_director_schema(writer_payload):
        return {"action": "ERROR", "text": "", "error": "writer_invalid_schema", "trace": {}}
    director = parse_director(writer_raw)
    trace: Dict[str, Any] = {
        "director": {
            key: director.get(key)
            for key in ("should_attempt", "setup", "target", "scene_type", "relation", "forbidden_moves")
        },
        "writer_candidates": list(director.get("candidates", [])),
        "filtered_candidates": [],
        "critic": None,
    }
    if writer_payload["should_attempt"] is False:
        return {"action": "SILENCE", "text": "", "trace": trace}
    if not director.get("should_attempt") or len(director.get("candidates", [])) != 4:
        return {"action": "ERROR", "text": "", "error": "writer_invalid_schema", "trace": trace}
    known_names = {"митя", "кадыр"}
    known_names.update(str(item.get("name", "")) for item in history if item.get("name"))
    candidates = filter_candidates(
        director.get("candidates", []),
        history=history,
        recent_outputs=(),
        known_participant_names=known_names,
        blocked_callback_keys=(),
    )
    trace["filtered_candidates"] = candidates
    if not candidates:
        return {"action": "SILENCE", "text": "", "trace": trace}
    critic_raw = llm.complete(critic_messages(history, candidates), max_tokens=40, temperature=0.1)
    critic_payload = _strict_json(critic_raw)
    if not critic_payload:
        return {"action": "ERROR", "text": "", "error": "critic_invalid_json", "trace": trace}
    if not _valid_critic_schema(critic_payload, candidate_count=len(candidates)):
        return {"action": "ERROR", "text": "", "error": "critic_invalid_schema", "trace": trace}
    winner, score, reasons = parse_critic(critic_raw, candidate_count=len(candidates))
    trace["critic"] = {"winner_index": winner, "score": score, "reason_codes": reasons}
    if winner is None or score < 85:
        return {"action": "SILENCE", "text": "", "trace": trace}
    return {"action": "JOKE", "text": str(candidates[winner]["text"]), "trace": trace}


def run_v2_direct(llm: ReplayLLM, persona: str, scene: Dict[str, Any]) -> Dict[str, str]:
    history = _history(scene)
    text = llm.complete(
        _direct_messages(persona, history, llm.model.rsplit("/", 1)[-1].replace("-", " ")),
        max_tokens=60,
        temperature=0.75,
    ).strip()
    if not text:
        return {"action": "ERROR", "text": "", "error": "direct_empty"}
    return {"action": "ANSWER", "text": text[:120]}


_DIRECT_CONTRACT_RUBRICS = {
    "model_identity": "утверждает, что фактическая модель — expected_model; не отрицает и не отмахивается от этого",
    "weather_uncertainty": "честно говорит, что не видит живую погоду, и не утверждает дождь/снег как установленный факт",
    "differential_explanation": "верно объясняет, что дифференциал позволяет колесам вращаться с разной скоростью, особенно в повороте",
    "dialogue_reason": "логично продолжает предыдущую реплику Тимура и объясняет, почему вариант «завтра» возможен или уместен",
    "choice_criteria": "дает полезный критерий сравнения поезда и автобуса и связывает критерий с выбором, а не советует наугад",
    "bot_identity": "прямо признает, что Тимур — бот, и не отменяет это шуткой или последующим отрицанием",
}


def judge_direct_contract(
    llm: ReplayLLM,
    scene: Dict[str, Any],
    output: Dict[str, Any],
    *,
    expected_model: str,
) -> Dict[str, Any]:
    contract = str(scene.get("semantic_contract", ""))
    rubric = _DIRECT_CONTRACT_RUBRICS.get(contract, "ответ должен быть фактически и логически корректным")
    payload = {
        "conversation": _history(scene),
        "answer": str(output.get("text", "")),
        "contract": contract,
        "rubric": rubric,
        "expected_model": expected_model,
        "instruction": "проверь весь смысл ответа, включая отрицание, оговорки и противоречащий хвост",
    }
    raw = llm.complete(
        [
            {
                "role": "system",
                "content": (
                    "ты независимый проверяющий прямого ответа. не оценивай юмор и стиль. "
                    "ответ проходит только если целиком выполняет rubric без фактического противоречия. "
                    'верни строго json {"passes":true,"reason_code":"ok"}.'
                ),
            },
            {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
        ],
        max_tokens=40,
        temperature=0.0,
    )
    parsed = _strict_json(raw)
    if set(parsed) != {"passes", "reason_code"} or not isinstance(parsed.get("passes"), bool) or not isinstance(
        parsed.get("reason_code"), str
    ):
        return {"valid": False, "passes": False, "reason_code": "invalid_schema"}
    reason_code = str(parsed["reason_code"] or "").strip().lower()
    if (parsed["passes"] and reason_code != "ok") or (not parsed["passes"] and reason_code in {"", "ok"}):
        return {"valid": False, "passes": False, "reason_code": "inconsistent_verdict"}
    return {
        "valid": True,
        "passes": bool(parsed["passes"]),
        "reason_code": reason_code[:80],
    }


def _expected_model_pattern(model_name: str) -> str:
    basename = str(model_name or "deepseek-v4-flash").rsplit("/", 1)[-1].lower()
    tokens = [token for token in re.findall(r"[a-z]+|v?\d+", basename) if token not in {"model", "models", "latest"}]
    if not tokens:
        tokens = ["deepseek", "v4", "flash"]
    return r"[\s/_.-]*".join(re.escape(token) for token in tokens)


def _semantic_contract_errors(contract: str, lowered: str, *, expected_model: str) -> List[str]:
    if contract == "model_identity":
        model_pattern = _expected_model_pattern(expected_model)
        identity = re.search(rf"\b{model_pattern}\b", lowered)
        negated = re.search(
            rf"\bне\s+(?:(?:работаю|сижу|запущен)\s+)?(?:на\s+)?{model_pattern}\b",
            lowered,
        ) or re.search(
            rf"\b{model_pattern}\b.{{0,32}}\b(?:не\s+(?:моя\s+модель|использую|юзаю|работаю)|модель\s+не\s+моя)\b",
            lowered,
        )
        return [] if identity and not negated else ["semantic:model_identity"]
    if contract == "weather_uncertainty":
        honest_limit = re.search(
            r"\b(?:не знаю(?:,?\s+)?(?:какая\s+погода|что\s+там\s+с\s+погодой|идет\s+ли\s+дождь|есть\s+ли\s+дождь)|"
            r"(?:погод\w*\s+не\s+вижу|не\s+вижу\s+погод\w*)|нет\s+(?:доступа|данных)\s+(?:к|по)\s+погод\w*|"
            r"не\s+могу\s+(?:проверить|узнать|посмотреть)\s+(?:погод\w*|дожд\w*))\b",
            lowered,
        )
        honest_limit = honest_limit or (
            "не знаю" in lowered and any(stem in lowered for stem in ("погод", "дожд", "провер", "посмотр"))
        )
        weather_claim = re.search(
            r"\b(?:точно.{0,24}(?:дожд|снег)|(?:дожд|снег).{0,24}точно|"
            r"(?:дожд|снег)\w*.{0,16}(?:идет|льет|начал\w*|пошел|будет)|"
            r"(?:начал\w*|пошел|будет).{0,16}(?:дожд|снег))\b",
            lowered,
        )
        hedged_claim = re.search(r"\b(?:может|возможно|вероятно|похоже).{0,24}(?:дожд|снег)\b", lowered)
        confident_tail = bool(weather_claim and not hedged_claim)
        return [] if honest_limit and not confident_tail else ["semantic:weather_uncertainty"]
    if contract == "differential_explanation":
        negated = re.search(
            r"\b(?:не\s+(?:могут|может|должн\w*).{0,32}разн\w*|не\s+(?:с\s+)?разн\w*|"
            r"(?:обязан\w*|должн\w*).{0,20}одинаков\w*|"
            r"(?:колес|вращ|скорост)\w*.{0,24}(?<!не\s)одинаков\w*)\b",
            lowered,
        )
        valid = (
            "колес" in lowered
            and "разн" in lowered
            and any(stem in lowered for stem in ("вращ", "скорост", "поворот"))
            and not negated
        )
        return [] if valid else ["semantic:differential_explanation"]
    if contract == "dialogue_reason":
        words = re.findall(r"[a-zа-яё0-9]+", lowered, re.I)
        contradiction = re.search(
            r"\bзавтра.{0,24}(?:нельзя|не\s+получится|не\s+смогу|не\s+успею)|"
            r"\b(?:нельзя|не\s+получится).{0,18}завтра\b",
            lowered,
        )
        valid = (
            len(words) >= 3
            and any(stem in lowered for stem in ("завтра", "сроч", "удоб", "успе", "время", "потому"))
            and not contradiction
        )
        return [] if valid else ["semantic:dialogue_reason"]
    if contract == "choice_criteria":
        criterion = any(stem in lowered for stem in ("время", "цен", "комфорт", "быстр", "дешев", "багаж"))
        recommendation = any(stem in lowered for stem in ("если", "выбирай", "бери", "смотри", "сравни"))
        negated = re.search(r"\b(?:время|цен\w*|комфорт|скорост\w*|багаж)\b.{0,16}\b(?:ни при чем|не важ\w*)\b", lowered)
        inverted_price = (
            "цен" in lowered
            and "дорог" in lowered
            and any(stem in lowered for stem in ("бери", "выбирай"))
            and not re.search(r"\bне\s+(?:бери|выбирай)\b", lowered)
        )
        valid = criterion and recommendation and not negated and not inverted_price
        return [] if valid else ["semantic:choice_criteria"]
    if contract == "bot_identity":
        affirmative = bool(
            re.search(r"\bя\s+(?:(?:и есть|реально|же)\s+)?бот\b", lowered)
            or re.search(r"\bда[\s,]+я\s+бот\b", lowered)
            or re.search(r"\bботом\s+являюсь\b", lowered)
        )
        contradiction = bool(
            re.search(r"\bя\s+не\s+бот\b", lowered)
            or re.search(r"\bя\s+человек\b", lowered)
            or re.search(r"\bбот\??\s*(?:нет|неа)\b", lowered)
            or re.search(r"\b(?:но|вообще-то)\s*(?:нет|неа)\b", lowered)
            or re.search(r"\bно\s+(?:вообще\s+)?я\s+не\s+бот\b", lowered)
        )
        return [] if affirmative and not contradiction else ["semantic:bot_identity"]
    return [f"semantic:unknown:{contract}"] if contract else []


def validate_contract(
    scene: Dict[str, Any],
    output: Dict[str, Any],
    *,
    expected_model: str = "deepseek-v4-flash",
) -> List[str]:
    errors: List[str] = []
    if output.get("action") == "ERROR":
        return [str(output.get("error", "generation_error"))]
    expected = str(scene.get("expected_action"))
    if output.get("action") != expected:
        errors.append(f"action:{output.get('action')}!={expected}")
    text = str(output.get("text", ""))
    if expected == "ANSWER" and (not text or len(text) > 120):
        errors.append("direct_length_or_empty")
    if expected == "JOKE" and (not text or len(text) > 60):
        errors.append("ambient_length_or_empty")
    if expected == "DAILY_LORE" and not text.endswith("у вас как с этим обычно"):
        errors.append("daily_signature")
    lowered = text.lower().replace("ё", "е")
    required_all = [str(item).lower().replace("ё", "е") for item in scene.get("required_all_phrases", [])]
    required_any = [str(item).lower().replace("ё", "е") for item in scene.get("required_any_phrases", [])]
    for phrase in required_all:
        if phrase not in lowered:
            errors.append(f"required_phrase:{phrase}")
    if required_any and not any(phrase in lowered for phrase in required_any):
        errors.append("required_any_phrase")
    errors.extend(
        _semantic_contract_errors(
            str(scene.get("semantic_contract", "")),
            lowered,
            expected_model=expected_model,
        )
    )
    for name in scene.get("forbidden_names", []):
        if str(name).lower().replace("ё", "е") in lowered:
            errors.append(f"forbidden_name:{name}")
    for phrase in scene.get("forbidden_phrases", []):
        if str(phrase).lower().replace("ё", "е") in lowered:
            errors.append(f"forbidden_phrase:{phrase}")
    return errors


def _public_output(output: Dict[str, Any]) -> Dict[str, str]:
    return {
        "action": str(output.get("action", "ERROR")),
        "text": str(output.get("text", "")),
        **({"error": str(output.get("error"))} if output.get("error") else {}),
    }


def _map_blind_winner(winner: str, *, swapped: bool) -> str:
    normalized = winner.upper()
    if normalized not in {"A", "B"}:
        return "tie"
    legacy_won = (normalized == "B") if swapped else (normalized == "A")
    return "legacy" if legacy_won else "v2"


def blind_judge(
    llm: ReplayLLM,
    scene: Dict[str, Any],
    legacy: Dict[str, str],
    v2: Dict[str, str],
    *,
    rng: random.Random,
) -> str:
    swapped = bool(rng.getrandbits(1))
    option_a, option_b = (v2, legacy) if swapped else (legacy, v2)
    prompt = {
        "scene": _history(scene),
        "expected_action": scene["expected_action"],
        "notes": scene.get("notes", ""),
        "forbidden_names": scene.get("forbidden_names", []),
        "forbidden_phrases": scene.get("forbidden_phrases", []),
        "A": option_a,
        "B": option_b,
    }
    raw = llm.complete(
        [
            {
                "role": "system",
                "content": (
                    "ты слепой редактор юмора живого чата. выбери A, B или TIE. сначала соблюдение нужного действия "
                    "и фактов сцены, потом точность и естественность, потом краткость. молчание выигрывает у проходной "
                    "или натужной шутки. не знаешь, какой workflow создал варианты. верни только json: "
                    '{"winner":"A"}.'
                ),
            },
            {"role": "user", "content": json.dumps(prompt, ensure_ascii=False)},
        ],
        max_tokens=25,
        temperature=0.0,
    )
    payload = _strict_json(raw)
    if not payload or str(payload.get("winner", "")).upper() not in {"A", "B", "TIE"}:
        return "error"
    return _map_blind_winner(str(payload["winner"]), swapped=swapped)


def run_benchmark(
    scenes: Iterable[Dict[str, Any]],
    *,
    runs: int,
    model_names: Sequence[str],
    llm: ReplayLLM,
    persona: str,
    min_decided_rate: float = 0.70,
) -> Dict[str, Any]:
    validate_compare(model_names)
    scene_list = list(scenes)
    validate_replay_scenes(scene_list)
    rng = random.Random(42)
    totals = {"legacy": 0, "v2": 0, "tie": 0, "error": 0}
    per_scene: Dict[str, Dict[str, int]] = {}
    contract_failures: List[Dict[str, Any]] = []
    critical_failures: List[Dict[str, Any]] = []
    records: List[Dict[str, Any]] = []
    parse_errors = 0
    bounded_runs = max(1, int(runs))
    ambient_comparisons = 0
    interrupted = False
    current_record: Dict[str, Any] | None = None

    try:
        for run_index in range(bounded_runs):
            for scene in scene_list:
                scene_id = str(scene["id"])
                route = str(scene["route"])
                current_record = {"run": run_index + 1, "scene": scene_id, "route": route}
                records.append(current_record)
                if route == "daily_lore":
                    output = {
                        "action": "DAILY_LORE",
                        "text": ensure_daily_signature(str(_history(scene)[-1]["text"])),
                    }
                    current_record["v2"] = output
                    errors = validate_contract(scene, output, expected_model=llm.model)
                    current_record["contract_errors"] = errors
                    if errors:
                        contract_failures.append({"run": run_index + 1, "scene": scene_id, "errors": errors})
                    continue
                if route == "direct":
                    try:
                        output = run_v2_direct(llm, persona, scene)
                    except ReplayCallError:
                        output = {"action": "ERROR", "text": "", "error": "direct_api_error"}
                    current_record["v2"] = output
                    errors = validate_contract(scene, output, expected_model=llm.model)
                    if output.get("action") != "ERROR":
                        try:
                            semantic_judge = judge_direct_contract(
                                llm,
                                scene,
                                output,
                                expected_model=llm.model,
                            )
                        except ReplayCallError:
                            semantic_judge = {"valid": False, "passes": False, "reason_code": "api_error"}
                            errors.append("direct_semantic_judge_api_error")
                        else:
                            if not semantic_judge.get("valid"):
                                parse_errors += 1
                                errors.append("direct_semantic_judge_invalid_schema")
                            elif not semantic_judge.get("passes"):
                                errors.append(f"semantic_judge:{semantic_judge.get('reason_code', 'failed')}")
                        current_record["semantic_judge"] = semantic_judge
                    current_record["contract_errors"] = errors
                    if errors:
                        contract_failures.append({"run": run_index + 1, "scene": scene_id, "errors": errors})
                    continue

                ambient_comparisons += 1
                bucket = per_scene.setdefault(scene_id, {"legacy": 0, "v2": 0, "tie": 0, "error": 0})
                try:
                    legacy = run_legacy(llm, persona, scene)
                except ReplayCallError:
                    current_record["error"] = "legacy_api_error"
                    totals["error"] += 1
                    bucket["error"] += 1
                    continue
                current_record["legacy"] = legacy
                try:
                    v2 = run_v2_ambient(llm, persona, scene)
                except ReplayCallError:
                    current_record["error"] = "v2_api_error"
                    totals["error"] += 1
                    bucket["error"] += 1
                    continue
                current_record["v2"] = v2
                v2_public = _public_output(v2)
                if bool(scene.get("critical", False)):
                    critical_errors = validate_contract(scene, v2_public)
                    current_record["critical_contract_errors"] = critical_errors
                    if critical_errors:
                        critical_failures.append(
                            {"run": run_index + 1, "scene": scene_id, "errors": critical_errors}
                        )
                generation_errors = [
                    str(output.get("error", "generation_error"))
                    for output in (legacy, v2_public)
                    if output.get("action") == "ERROR"
                ]
                if generation_errors:
                    parse_errors += len(generation_errors)
                    current_record["error"] = ",".join(generation_errors)
                    totals["error"] += 1
                    bucket["error"] += 1
                    continue
                try:
                    winner = blind_judge(llm, scene, legacy, v2_public, rng=rng)
                except ReplayCallError:
                    winner = "error"
                    current_record["error"] = "judge_api_error"
                else:
                    if winner == "error":
                        parse_errors += 1
                        current_record["error"] = "judge_invalid_json"
                current_record["judge"] = winner
                totals[winner] += 1
                bucket[winner] += 1
    except KeyboardInterrupt:
        interrupted = True
        if current_record is not None:
            current_record["error"] = "interrupted"

    decided = totals["legacy"] + totals["v2"]
    for scene in scene_list:
        if scene.get("route") != "ambient" or not scene.get("critical"):
            continue
        scene_id = str(scene["id"])
        legacy_wins = int(per_scene.get(scene_id, {}).get("legacy", 0))
        v2_wins = int(per_scene.get(scene_id, {}).get("v2", 0))
        if legacy_wins:
            critical_failures.append({"scene": scene_id, "errors": [f"legacy_wins:{legacy_wins}"]})
        if scene.get("expected_action") == "JOKE" and v2_wins < 1:
            critical_failures.append({"scene": scene_id, "errors": ["no_v2_win"]})
    v2_rate = totals["v2"] / decided if decided else 0.0
    decided_rate = decided / ambient_comparisons if ambient_comparisons else 0.0
    passed = (
        decided > 0
        and v2_rate >= 0.65
        and decided_rate >= max(0.0, min(1.0, float(min_decided_rate)))
        and totals["error"] == 0
        and llm.api_errors == 0
        and not contract_failures
        and not critical_failures
        and not interrupted
    )
    return {
        "benchmark_scope": "ambient pinned legacy vs v2 on isolated scene memory; direct/daily are contracts",
        "scenes": len(scene_list),
        "runs": bounded_runs,
        "ambient_comparisons": ambient_comparisons,
        "totals": totals,
        "decided_rate": round(decided_rate, 4),
        "minimum_decided_rate": round(max(0.0, min(1.0, float(min_decided_rate))), 4),
        "v2_win_rate_without_ties": round(v2_rate, 4),
        "contract_failures": contract_failures,
        "critical_failures": critical_failures,
        "api_calls": llm.calls,
        "api_tokens": llm.tokens,
        "api_errors": llm.api_errors,
        "parse_errors": parse_errors,
        "interrupted": interrupted,
        "passed": passed,
        "per_scene": per_scene,
        "records": records,
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Blind humor workflow replay")
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--compare", default="legacy,v2")
    parser.add_argument("--fixture", type=Path, default=DEFAULT_FIXTURE)
    parser.add_argument("--min-decided-rate", type=float, default=0.70)
    parser.add_argument("--max-api-calls", type=int, default=DEFAULT_MAX_API_CALLS)
    parser.add_argument("--dry-run", action="store_true", help="validate fixtures and cost ceiling without API calls")
    args = parser.parse_args(argv)
    scenes = load_replay_fixture(args.fixture)
    model_names = [item.strip() for item in args.compare.split(",") if item.strip()]
    try:
        validate_compare(model_names)
    except ValueError as exc:
        parser.error(str(exc))
    estimated_calls = estimate_max_api_calls(scenes, args.runs)
    if estimated_calls > max(0, int(args.max_api_calls)):
        parser.error(
            f"estimated API calls {estimated_calls} exceed --max-api-calls={args.max_api_calls}; "
            "raise the ceiling explicitly"
        )
    if args.dry_run:
        print(json.dumps({"valid": True, "scenes": len(scenes), "estimated_max_api_calls": estimated_calls}, ensure_ascii=False))
        return 0

    print(f"humor replay: up to {estimated_calls} API calls", file=sys.stderr)
    config = load_app_config(ROOT_DIR)
    client_kwargs: Dict[str, Any] = {"api_key": config.openai_api_key, "max_retries": 0, "timeout": 10.0}
    if config.openai_base_url:
        client_kwargs["base_url"] = config.openai_base_url
    llm = ReplayLLM(OpenAI(**client_kwargs), config.text_model)
    result = run_benchmark(
        scenes,
        runs=args.runs,
        model_names=model_names,
        llm=llm,
        persona=config.default_system_prompt,
        min_decided_rate=args.min_decided_rate,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
