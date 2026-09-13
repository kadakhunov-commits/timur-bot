"""Общак: трекер вкладов общего котла.

Модуль держит единственное JSON-хранилище (по умолчанию ``data/obshak.json``)
и весь домен: расходы, участники, вишлист, запросы денег, лидерборд и ачивки.

Все функции, которые меняют состояние, работают с переданным словарём и не
пишут на диск сами — вызывающий код оборачивает чтение-изменение-запись в
:func:`lock`. Так хранилище остаётся атомарным, а логика — тестируемой.
"""

from __future__ import annotations

import csv
import io
import json
import os
import random
import re
import threading
import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

SCHEMA_VERSION = 1
ROULETTE_HISTORY_LIMIT = 200
ROULETTE_WINDOW_DAYS = 14

ALLOWED_REACTIONS = ("fire", "goat", "cry", "clap")
PERIODS = ("week", "month", "all")

_LOCK = threading.RLock()
_TITLE_CLEAN_RE = re.compile(r"\s+")

_UTC = timezone.utc


class ObshakError(ValueError):
    """Ошибка предметной области — превращается в 4xx ответ API."""


def lock() -> threading.RLock:
    """Единый лок хранилища (веб-сервер многопоточный)."""
    return _LOCK


# ---------------------------------------------------------------------------
# Время и периоды
# ---------------------------------------------------------------------------


def resolve_timezone(name: str) -> timezone:
    try:
        return ZoneInfo(name)  # type: ignore[return-value]
    except (ZoneInfoNotFoundError, ValueError, KeyError):
        return _UTC


def now_local(tz_name: str = "Europe/Moscow", *, now: Optional[datetime] = None) -> datetime:
    tz = resolve_timezone(tz_name)
    base = now or datetime.now(timezone.utc)
    if base.tzinfo is None:
        base = base.replace(tzinfo=timezone.utc)
    return base.astimezone(tz)


def parse_ts(value: Any) -> Optional[datetime]:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.strip())
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=_UTC)
    return parsed


def period_bounds(
    period: str,
    tz_name: str,
    *,
    now: Optional[datetime] = None,
) -> Tuple[Optional[datetime], Optional[datetime]]:
    """Границы периода в локальном часовом поясе (или (None, None) для «всё»)."""
    local = now_local(tz_name, now=now)
    if period == "week":
        start = (local - timedelta(days=local.weekday())).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
    elif period == "month":
        start = local.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    else:
        return None, None
    return start, local


def week_anchor(tz_name: str, *, now: Optional[datetime] = None) -> Tuple[datetime, datetime]:
    """Начало текущей недели (пн 00:00) и её конец."""
    start, _ = period_bounds("week", tz_name, now=now)
    assert start is not None
    return start, start + timedelta(days=7)


def in_window(
    ts: Optional[datetime],
    since: Optional[datetime],
    until: Optional[datetime],
) -> bool:
    if ts is None:
        return since is None and until is None
    if since is not None and ts < since:
        return False
    if until is not None and ts > until:
        return False
    return True


# ---------------------------------------------------------------------------
# Схема и хранилище
# ---------------------------------------------------------------------------


def _new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:10]}"


def _clean_title(title: Any) -> str:
    text = str(title or "").strip()
    return _TITLE_CLEAN_RE.sub(" ", text)


def title_key(title: Any) -> str:
    return _clean_title(title).lower()


def _members_from_config(config: Dict[str, Any]) -> List[Dict[str, Any]]:
    return [dict(entry) for entry in (config.get("members") or [])]


def default_state(config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    config = config or {}
    members: Dict[str, Dict[str, Any]] = {}
    for entry in _members_from_config(config):
        key = str(entry.get("key", "")).strip()
        if not key:
            continue
        members[key] = {
            "key": key,
            "name": str(entry.get("name", key)),
            "color": str(entry.get("color", "#8b90a3")),
            "avatar": str(entry.get("avatar", key)),
            "telegram_id": None,
            "username": "",
        }
    return {
        "version": SCHEMA_VERSION,
        "members": members,
        "expenses": [],
        "wishlist": [],
        "requests": [],
        "roulette": [],
        "meta": {"created_at": datetime.now(_UTC).isoformat(), "revision": 0},
    }


def ensure_state_schema(state: Dict[str, Any], config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Приводит состояние к актуальной схеме и вливает состав из конфига."""
    config = config or {}
    if not isinstance(state, dict):
        state = default_state(config)
    state["version"] = SCHEMA_VERSION
    for key in ("expenses", "wishlist", "requests", "roulette"):
        if not isinstance(state.get(key), list):
            state[key] = []

    members = state.get("members")
    if not isinstance(members, dict):
        members = {}
    for entry in _members_from_config(config):
        key = str(entry.get("key", "")).strip()
        if not key:
            continue
        current = members.get(key) if isinstance(members.get(key), dict) else {}
        members[key] = {
            "key": key,
            "name": str(entry.get("name", key)),
            "color": str(entry.get("color", "#8b90a3")),
            "avatar": str(entry.get("avatar", key)),
            "telegram_id": current.get("telegram_id"),
            "username": str(current.get("username", "") or ""),
        }
    state["members"] = members

    normalized_expenses: List[Dict[str, Any]] = []
    for item in state["expenses"]:
        normalized = _normalize_expense(item, members)
        if normalized is not None:
            normalized_expenses.append(normalized)
    state["expenses"] = normalized_expenses

    normalized_wishlist: List[Dict[str, Any]] = []
    for item in state["wishlist"]:
        normalized = _normalize_wishlist(item)
        if normalized is not None:
            normalized_wishlist.append(normalized)
    state["wishlist"] = normalized_wishlist

    normalized_requests: List[Dict[str, Any]] = []
    for item in state["requests"]:
        normalized = _normalize_request(item)
        if normalized is not None:
            normalized_requests.append(normalized)
    state["requests"] = normalized_requests

    normalized_roulette: List[Dict[str, Any]] = []
    for item in state["roulette"]:
        normalized = _normalize_roulette(item)
        if normalized is not None:
            normalized_roulette.append(normalized)
    state["roulette"] = normalized_roulette[-ROULETTE_HISTORY_LIMIT:]

    meta = state.get("meta") if isinstance(state.get("meta"), dict) else {}
    meta.setdefault("created_at", datetime.now(_UTC).isoformat())
    meta["revision"] = int(meta.get("revision", 0) or 0)
    state["meta"] = meta
    return state


def _normalize_expense(item: Any, members: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not isinstance(item, dict):
        return None
    member_id = str(item.get("member_id", "")).strip()
    if member_id not in members:
        return None
    title = _clean_title(item.get("title"))
    if not title:
        return None
    try:
        amount = round(float(item.get("amount", 0)), 2)
    except (TypeError, ValueError):
        return None
    if amount <= 0:
        return None
    created_at = item.get("created_at")
    if not isinstance(created_at, str) or not created_at.strip():
        created_at = datetime.now(_UTC).isoformat()
    reactions_raw = item.get("reactions") if isinstance(item.get("reactions"), dict) else {}
    reactions = {
        str(key): str(value)
        for key, value in reactions_raw.items()
        if str(key) in members and str(value) in ALLOWED_REACTIONS
    }
    return {
        "id": str(item.get("id") or _new_id("e")),
        "member_id": member_id,
        "amount": amount,
        "title": title,
        "category": str(item.get("category") or "other"),
        "created_at": created_at,
        "added_by": item.get("added_by"),
        "request_id": item.get("request_id"),
        "source": str(item.get("source") or "miniapp"),
        "reactions": reactions,
    }


def _normalize_wishlist(item: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(item, dict):
        return None
    title = _clean_title(item.get("title"))
    if not title:
        return None
    return {
        "id": str(item.get("id") or _new_id("w")),
        "title": title,
        "note": str(item.get("note") or ""),
        "created_by": item.get("created_by"),
        "claimed_by": item.get("claimed_by"),
        "status": str(item.get("status") or "open"),
        "done_expense_id": item.get("done_expense_id"),
        "created_at": str(item.get("created_at") or datetime.now(_UTC).isoformat()),
        "done_at": item.get("done_at"),
    }


def _normalize_request(item: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(item, dict):
        return None
    title = _clean_title(item.get("title"))
    if not title:
        return None
    try:
        amount = round(float(item.get("amount", 0)), 2)
    except (TypeError, ValueError):
        return None
    if amount <= 0:
        return None
    payments_raw = item.get("payments") if isinstance(item.get("payments"), list) else []
    payments: List[Dict[str, Any]] = []
    for payment in payments_raw:
        if not isinstance(payment, dict):
            continue
        member_id = payment.get("member_id")
        if not member_id:
            continue
        payments.append(
            {
                "member_id": str(member_id),
                "expense_id": payment.get("expense_id"),
                "ts": str(payment.get("ts") or datetime.now(_UTC).isoformat()),
            }
        )
    return {
        "id": str(item.get("id") or _new_id("r")),
        "title": title,
        "created_by": item.get("created_by"),
        "scope": str(item.get("scope") or "all"),
        "amount": amount,
        "per_person": bool(item.get("per_person", True)),
        "status": str(item.get("status") or "open"),
        "created_at": str(item.get("created_at") or datetime.now(_UTC).isoformat()),
        "payments": payments,
    }


def _normalize_roulette(item: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(item, dict):
        return None
    member_id = str(item.get("member_id", "")).strip()
    if not member_id:
        return None
    return {
        "member_id": member_id,
        "ts": str(item.get("ts") or datetime.now(_UTC).isoformat()),
    }


def load_state(path: Path, config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    if not path.exists():
        return default_state(config)
    try:
        with open(path, "r", encoding="utf-8") as handle:
            raw = json.load(handle)
    except (OSError, json.JSONDecodeError):
        return default_state(config)
    return ensure_state_schema(raw, config)


def save_state(path: Path, state: Dict[str, Any]) -> None:
    ensure_state_schema(state)
    state.setdefault("meta", {})["revision"] = int(state.get("meta", {}).get("revision", 0) or 0) + 1
    state["meta"]["updated_at"] = datetime.now(_UTC).isoformat()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    with open(tmp_path, "w", encoding="utf-8") as handle:
        json.dump(state, handle, ensure_ascii=False, indent=2)
    os.replace(tmp_path, path)


# ---------------------------------------------------------------------------
# Участники
# ---------------------------------------------------------------------------


def member_map(state: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    members = state.get("members")
    return members if isinstance(members, dict) else {}


def member_ids(state: Dict[str, Any]) -> List[str]:
    return list(member_map(state).keys())


def member(state: Dict[str, Any], member_id: str) -> Optional[Dict[str, Any]]:
    entry = member_map(state).get(member_id)
    return entry if isinstance(entry, dict) else None


def resolve_member_id(state: Dict[str, Any], telegram_id: Any) -> Optional[str]:
    if telegram_id is None:
        return None
    try:
        target = int(telegram_id)
    except (TypeError, ValueError):
        return None
    for member_id, entry in member_map(state).items():
        stored = entry.get("telegram_id")
        if stored is None:
            continue
        try:
            if int(stored) == target:
                return member_id
        except (TypeError, ValueError):
            continue
    return None


def link_member(
    state: Dict[str, Any],
    member_id: str,
    telegram_id: int,
    *,
    username: str = "",
) -> Tuple[bool, str]:
    """Привязывает Telegram-аккаунт к участнику. Возвращает (успех, код ошибки)."""
    entry = member(state, member_id)
    if entry is None:
        return False, "unknown_member"
    existing = resolve_member_id(state, telegram_id)
    if existing and existing != member_id:
        return False, "already_linked"
    current = entry.get("telegram_id")
    if current is not None:
        try:
            if int(current) != int(telegram_id):
                return False, "slot_taken"
        except (TypeError, ValueError):
            pass
    entry["telegram_id"] = int(telegram_id)
    entry["username"] = username
    return True, "ok"


def release_member(state: Dict[str, Any], telegram_id: int) -> bool:
    member_id = resolve_member_id(state, telegram_id)
    if not member_id:
        return False
    member_map(state)[member_id]["telegram_id"] = None
    member_map(state)[member_id]["username"] = ""
    return True


def unlink_member(state: Dict[str, Any], member_id: str) -> bool:
    entry = member(state, member_id)
    if entry is None:
        return False
    entry["telegram_id"] = None
    entry["username"] = ""
    return True


# ---------------------------------------------------------------------------
# Расходы
# ---------------------------------------------------------------------------


def find_expense(state: Dict[str, Any], expense_id: str) -> Optional[Dict[str, Any]]:
    for item in state.get("expenses", []):
        if item.get("id") == expense_id:
            return item
    return None


def add_expense(
    state: Dict[str, Any],
    *,
    member_id: str,
    amount: float,
    title: str,
    category: str = "other",
    added_by: Any = None,
    request_id: Optional[str] = None,
    source: str = "miniapp",
    created_at: Optional[datetime] = None,
    tz_name: str = "Europe/Moscow",
) -> Dict[str, Any]:
    if member(state, member_id) is None:
        raise ObshakError("unknown_member")
    clean_title = _clean_title(title)
    if not clean_title:
        raise ObshakError("empty_title")
    try:
        value = round(float(amount), 2)
    except (TypeError, ValueError) as exc:
        raise ObshakError("bad_amount") from exc
    if value <= 0:
        raise ObshakError("bad_amount")
    if value > 1_000_000:
        raise ObshakError("amount_too_big")
    stamp = created_at or now_local(tz_name)
    if stamp.tzinfo is None:
        stamp = stamp.replace(tzinfo=resolve_timezone(tz_name))
    expense = {
        "id": _new_id("e"),
        "member_id": member_id,
        "amount": value,
        "title": clean_title,
        "category": str(category or "other"),
        "created_at": stamp.isoformat(),
        "added_by": added_by,
        "request_id": request_id,
        "source": source,
        "reactions": {},
    }
    state.setdefault("expenses", []).append(expense)
    return expense


def update_expense(
    state: Dict[str, Any],
    expense_id: str,
    *,
    amount: Optional[float] = None,
    title: Optional[str] = None,
    category: Optional[str] = None,
) -> Dict[str, Any]:
    expense = find_expense(state, expense_id)
    if expense is None:
        raise ObshakError("unknown_expense")
    if amount is not None:
        try:
            value = round(float(amount), 2)
        except (TypeError, ValueError) as exc:
            raise ObshakError("bad_amount") from exc
        if value <= 0:
            raise ObshakError("bad_amount")
        expense["amount"] = value
    if title is not None:
        clean_title = _clean_title(title)
        if not clean_title:
            raise ObshakError("empty_title")
        expense["title"] = clean_title
    if category is not None:
        expense["category"] = str(category or "other")
    return expense


def delete_expense(state: Dict[str, Any], expense_id: str) -> bool:
    expenses = state.get("expenses", [])
    for index, item in enumerate(expenses):
        if item.get("id") == expense_id:
            del expenses[index]
            return True
    return False


def _expense_ts(item: Dict[str, Any]) -> Optional[datetime]:
    return parse_ts(item.get("created_at"))


def query_expenses(
    state: Dict[str, Any],
    *,
    member_id: Optional[str] = None,
    category: Optional[str] = None,
    since: Optional[datetime] = None,
    until: Optional[datetime] = None,
    limit: Optional[int] = None,
    newest_first: bool = True,
) -> List[Dict[str, Any]]:
    result: List[Dict[str, Any]] = []
    for item in state.get("expenses", []):
        if member_id and item.get("member_id") != member_id:
            continue
        if category and item.get("category") != category:
            continue
        if not in_window(_expense_ts(item), since, until):
            continue
        result.append(item)
    result.sort(key=lambda entry: entry.get("created_at", ""), reverse=newest_first)
    if limit is not None and limit >= 0:
        result = result[:limit]
    return result


def totals(state: Dict[str, Any], expenses: Optional[Iterable[Dict[str, Any]]] = None) -> Dict[str, Any]:
    items = list(expenses if expenses is not None else state.get("expenses", []))
    amount = round(sum(float(item.get("amount", 0)) for item in items), 2)
    return {"total": amount, "count": len(items)}


def leaderboard(
    state: Dict[str, Any],
    *,
    since: Optional[datetime] = None,
    until: Optional[datetime] = None,
    expenses: Optional[Iterable[Dict[str, Any]]] = None,
) -> List[Dict[str, Any]]:
    items = list(expenses) if expenses is not None else query_expenses(
        state, since=since, until=until
    )
    sums: Dict[str, float] = {key: 0.0 for key in member_map(state)}
    counts: Dict[str, int] = {key: 0 for key in member_map(state)}
    for item in items:
        member_id = str(item.get("member_id"))
        if member_id not in sums:
            continue
        sums[member_id] += float(item.get("amount", 0))
        counts[member_id] += 1
    rows: List[Dict[str, Any]] = []
    for order, (member_id, entry) in enumerate(member_map(state).items()):
        rows.append(
            {
                "member_id": member_id,
                "name": entry.get("name", member_id),
                "color": entry.get("color", "#8b90a3"),
                "avatar": entry.get("avatar", member_id),
                "telegram_id": entry.get("telegram_id"),
                "total": round(sums.get(member_id, 0.0), 2),
                "count": counts.get(member_id, 0),
                "order": order,
            }
        )
    # При равных суммах держим порядок из конфига: места в сцене не прыгают.
    rows.sort(key=lambda row: (-row["total"], row["order"]))
    for row in rows:
        row.pop("order", None)
    return rows


def member_stats(state: Dict[str, Any], member_id: str) -> Dict[str, Any]:
    items = [item for item in state.get("expenses", []) if item.get("member_id") == member_id]
    category_counts: Dict[str, int] = {}
    category_sums: Dict[str, float] = {}
    for item in items:
        key = str(item.get("category") or "other")
        category_counts[key] = category_counts.get(key, 0) + 1
        category_sums[key] = round(category_sums.get(key, 0.0) + float(item.get("amount", 0)), 2)
    top_category = None
    if category_counts:
        top_key = max(category_counts, key=lambda key: (category_counts[key], category_sums.get(key, 0)))
        top_category = {"key": top_key, "count": category_counts[top_key], "total": category_sums[top_key]}
    recent = query_expenses(state, member_id=member_id, limit=10)
    biggest = max(items, key=lambda item: float(item.get("amount", 0)), default=None)
    return {
        "member_id": member_id,
        "total": round(sum(float(item.get("amount", 0)) for item in items), 2),
        "count": len(items),
        "average": round(
            sum(float(item.get("amount", 0)) for item in items) / len(items), 2
        )
        if items
        else 0.0,
        "categories": [
            {"key": key, "count": category_counts[key], "total": category_sums.get(key, 0.0)}
            for key in sorted(category_counts, key=lambda key: -category_counts[key])
        ],
        "top_category": top_category,
        "biggest": biggest,
        "recent": recent,
    }


def set_reaction(state: Dict[str, Any], expense_id: str, member_id: str, reaction: str) -> Dict[str, Any]:
    expense = find_expense(state, expense_id)
    if expense is None:
        raise ObshakError("unknown_expense")
    if member(state, member_id) is None:
        raise ObshakError("unknown_member")
    if reaction not in ALLOWED_REACTIONS:
        raise ObshakError("bad_reaction")
    reactions = expense.setdefault("reactions", {})
    if reactions.get(member_id) == reaction:
        reactions.pop(member_id, None)
    else:
        reactions[member_id] = reaction
    return expense


# ---------------------------------------------------------------------------
# Вишлист
# ---------------------------------------------------------------------------


def wishlist_add(
    state: Dict[str, Any],
    *,
    title: str,
    created_by: Optional[str] = None,
    note: str = "",
) -> Dict[str, Any]:
    clean_title = _clean_title(title)
    if not clean_title:
        raise ObshakError("empty_title")
    item = {
        "id": _new_id("w"),
        "title": clean_title,
        "note": str(note or ""),
        "created_by": created_by,
        "claimed_by": None,
        "status": "open",
        "done_expense_id": None,
        "created_at": datetime.now(_UTC).isoformat(),
        "done_at": None,
    }
    state.setdefault("wishlist", []).append(item)
    return item


def find_wishlist(state: Dict[str, Any], item_id: str) -> Optional[Dict[str, Any]]:
    for item in state.get("wishlist", []):
        if item.get("id") == item_id:
            return item
    return None


def wishlist_claim(state: Dict[str, Any], item_id: str, member_id: str) -> Dict[str, Any]:
    item = find_wishlist(state, item_id)
    if item is None:
        raise ObshakError("unknown_wishlist_item")
    if member(state, member_id) is None:
        raise ObshakError("unknown_member")
    if item.get("status") != "open":
        raise ObshakError("item_closed")
    item["claimed_by"] = None if item.get("claimed_by") == member_id else member_id
    return item


def wishlist_complete(
    state: Dict[str, Any],
    item_id: str,
    *,
    member_id: str,
    amount: float,
    category: str = "other",
    tz_name: str = "Europe/Moscow",
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    item = find_wishlist(state, item_id)
    if item is None:
        raise ObshakError("unknown_wishlist_item")
    if item.get("status") != "open":
        raise ObshakError("item_closed")
    expense = add_expense(
        state,
        member_id=member_id,
        amount=amount,
        title=item["title"],
        category=category,
        added_by=member_id,
        source="wishlist",
        tz_name=tz_name,
    )
    item["status"] = "done"
    item["done_expense_id"] = expense["id"]
    item["done_at"] = datetime.now(_UTC).isoformat()
    item["claimed_by"] = member_id
    return item, expense


def wishlist_delete(state: Dict[str, Any], item_id: str) -> bool:
    items = state.get("wishlist", [])
    for index, item in enumerate(items):
        if item.get("id") == item_id:
            del items[index]
            return True
    return False


def wishlist_open(state: Dict[str, Any]) -> List[Dict[str, Any]]:
    return [item for item in state.get("wishlist", []) if item.get("status") == "open"]


def wishlist_done(state: Dict[str, Any], limit: int = 15) -> List[Dict[str, Any]]:
    items = [item for item in state.get("wishlist", []) if item.get("status") == "done"]
    items.sort(key=lambda item: str(item.get("done_at") or ""), reverse=True)
    return items[:limit]


# ---------------------------------------------------------------------------
# Запросы денег (не долги — видимая просьба с прогрессом)
# ---------------------------------------------------------------------------


def find_request(state: Dict[str, Any], request_id: str) -> Optional[Dict[str, Any]]:
    for item in state.get("requests", []):
        if item.get("id") == request_id:
            return item
    return None


def request_targets(request: Dict[str, Any], state: Dict[str, Any]) -> List[str]:
    scope = str(request.get("scope") or "all")
    if scope == "all":
        return [key for key in member_map(state) if key != request.get("created_by")]
    if member(state, scope) is None:
        return []
    return [scope]


def request_expected(request: Dict[str, Any]) -> Optional[float]:
    """Сколько ждут с каждого участника (None — сумма общая, не подушевая)."""
    if request.get("per_person"):
        return round(float(request.get("amount", 0)), 2)
    return None


def request_create(
    state: Dict[str, Any],
    *,
    created_by: str,
    title: str,
    amount: float,
    scope: str = "all",
    per_person: bool = True,
) -> Dict[str, Any]:
    if member(state, created_by) is None:
        raise ObshakError("unknown_member")
    if scope != "all" and member(state, scope) is None:
        raise ObshakError("unknown_member")
    clean_title = _clean_title(title)
    if not clean_title:
        raise ObshakError("empty_title")
    try:
        value = round(float(amount), 2)
    except (TypeError, ValueError) as exc:
        raise ObshakError("bad_amount") from exc
    if value <= 0:
        raise ObshakError("bad_amount")
    item = {
        "id": _new_id("r"),
        "title": clean_title,
        "created_by": created_by,
        "scope": scope,
        "amount": value,
        "per_person": bool(per_person),
        "status": "open",
        "created_at": datetime.now(_UTC).isoformat(),
        "payments": [],
    }
    state.setdefault("requests", []).append(item)
    return item


def request_pay(
    state: Dict[str, Any],
    request_id: str,
    *,
    member_id: str,
    amount: Optional[float] = None,
    tz_name: str = "Europe/Moscow",
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    item = find_request(state, request_id)
    if item is None:
        raise ObshakError("unknown_request")
    if item.get("status") != "open":
        raise ObshakError("request_closed")
    targets = request_targets(item, state)
    if targets and member_id not in targets:
        raise ObshakError("not_a_target")
    if any(payment.get("member_id") == member_id for payment in item.get("payments", [])):
        raise ObshakError("already_paid")
    value = amount
    if value is None:
        value = request_expected(item)
    if value is None:
        value = float(item.get("amount", 0))
    expense = add_expense(
        state,
        member_id=member_id,
        amount=value,
        title=item["title"],
        category="other",
        added_by=member_id,
        request_id=item["id"],
        source="request",
        tz_name=tz_name,
    )
    item.setdefault("payments", []).append(
        {
            "member_id": member_id,
            "expense_id": expense["id"],
            "ts": datetime.now(_UTC).isoformat(),
        }
    )
    return item, expense


def request_close(state: Dict[str, Any], request_id: str, member_id: str) -> Dict[str, Any]:
    item = find_request(state, request_id)
    if item is None:
        raise ObshakError("unknown_request")
    if item.get("created_by") != member_id:
        raise ObshakError("not_author")
    item["status"] = "closed"
    return item


def request_progress(state: Dict[str, Any], request: Dict[str, Any]) -> Dict[str, Any]:
    payments = request.get("payments") or []
    collected = 0.0
    for payment in payments:
        expense = find_expense(state, str(payment.get("expense_id") or ""))
        if expense:
            collected += float(expense.get("amount", 0))
    collected = round(collected, 2)
    targets = request_targets(request, state)
    expected_total = None
    if request.get("per_person"):
        expected_total = round(float(request.get("amount", 0)) * max(1, len(targets)), 2)
    else:
        expected_total = round(float(request.get("amount", 0)), 2)
    return {
        "collected": collected,
        "expected_total": expected_total,
        "paid_count": len(payments),
        "target_count": len(targets),
        "targets": targets,
        "paid_by": [payment.get("member_id") for payment in payments],
    }


def requests_view(state: Dict[str, Any], *, include_closed: bool = False) -> List[Dict[str, Any]]:
    result: List[Dict[str, Any]] = []
    for item in state.get("requests", []):
        if not include_closed and item.get("status") != "open":
            continue
        enriched = dict(item)
        enriched["progress"] = request_progress(state, item)
        result.append(enriched)
    result.sort(key=lambda entry: str(entry.get("created_at") or ""), reverse=True)
    return result


# ---------------------------------------------------------------------------
# Рулетка «кто идёт в магазин» и стрик общака
# ---------------------------------------------------------------------------


def roulette_stats(
    state: Dict[str, Any],
    *,
    days: int = ROULETTE_WINDOW_DAYS,
    now: Optional[datetime] = None,
    tz_name: str = "Europe/Moscow",
) -> Dict[str, Any]:
    tz = resolve_timezone(tz_name)
    since = now_local(tz_name, now=now) - timedelta(days=days)
    counts: Dict[str, int] = {key: 0 for key in member_map(state)}
    total = 0
    last_member: Optional[str] = None
    last_ts: Optional[datetime] = None
    for entry in state.get("roulette", []):
        member_id = str(entry.get("member_id") or "")
        ts = parse_ts(entry.get("ts"))
        if ts is None:
            continue
        if ts.astimezone(tz) < since:
            continue
        if member_id in counts:
            counts[member_id] += 1
        total += 1
        if last_ts is None or ts > last_ts:
            last_ts = ts
            last_member = member_id
    return {
        "counts": counts,
        "total": total,
        "last": {"member_id": last_member, "ts": last_ts.isoformat()} if last_member else None,
        "days": days,
    }


def roulette_weights(
    state: Dict[str, Any],
    *,
    days: int = ROULETTE_WINDOW_DAYS,
    now: Optional[datetime] = None,
    tz_name: str = "Europe/Moscow",
) -> Dict[str, float]:
    """Чем чаще человек уже ходил, тем меньше его шанс — рулетка подравнивает."""
    stats = roulette_stats(state, days=days, now=now, tz_name=tz_name)
    return {
        member_id: 1.0 / (1.0 + stats["counts"].get(member_id, 0))
        for member_id in member_map(state)
    }


def roulette_spin(
    state: Dict[str, Any],
    config: Optional[Dict[str, Any]] = None,
    *,
    now: Optional[datetime] = None,
    rng: Any = None,
    days: int = ROULETTE_WINDOW_DAYS,
) -> Dict[str, Any]:
    config = config or {}
    tz_name = str(config.get("timezone") or "Europe/Moscow")
    members = list(member_map(state))
    if not members:
        raise ObshakError("no_members")
    weights = roulette_weights(state, days=days, now=now, tz_name=tz_name)
    total = sum(weights.values()) or 1.0
    roll = (rng or random).random() * total
    winner = members[-1]
    accumulated = 0.0
    for member_id in members:
        accumulated += weights[member_id]
        if roll <= accumulated:
            winner = member_id
            break
    stamp = now_local(tz_name, now=now)
    history = state.setdefault("roulette", [])
    history.append({"member_id": winner, "ts": stamp.isoformat()})
    del history[:-ROULETTE_HISTORY_LIMIT]
    return {
        "winner": winner,
        "chance": round(weights[winner] / total, 4),
        "weights": {key: round(value / total, 4) for key, value in weights.items()},
        "stats": roulette_stats(state, days=days, now=now, tz_name=tz_name),
    }


def streak(
    state: Dict[str, Any],
    config: Optional[Dict[str, Any]] = None,
    *,
    now: Optional[datetime] = None,
    window_days: int = 14,
) -> Dict[str, Any]:
    """Сколько дней подряд общак не пустует. Сегодняшний день — ещё не провал."""
    config = config or {}
    tz_name = str(config.get("timezone") or "Europe/Moscow")
    tz = resolve_timezone(tz_name)
    today = now_local(tz_name, now=now).date()
    hits = set()
    for item in state.get("expenses", []):
        ts = _expense_ts(item)
        if ts is not None:
            hits.add(ts.astimezone(tz).date())

    cursor = today if today in hits else today - timedelta(days=1)
    current = 0
    while cursor in hits:
        current += 1
        cursor -= timedelta(days=1)

    best = 0
    run = 0
    previous = None
    for day in sorted(hits):
        run = run + 1 if previous is not None and (day - previous).days == 1 else 1
        best = max(best, run)
        previous = day

    return {
        "current": current,
        "best": best,
        "today": today in hits,
        "days": [
            {
                "date": (today - timedelta(days=offset)).isoformat(),
                "hit": (today - timedelta(days=offset)) in hits,
            }
            for offset in range(window_days - 1, -1, -1)
        ],
    }


# ---------------------------------------------------------------------------
# Ачивки
# ---------------------------------------------------------------------------


def format_amount(value: Any) -> str:
    """«1200» вместо «1200.0» — для подписей кубков."""
    try:
        number = float(value)
    except (TypeError, ValueError):
        return str(value)
    if abs(number - round(number)) < 0.005:
        return str(int(round(number)))
    return f"{number:.2f}".rstrip("0").rstrip(".")


def _badge(badge_id: str, title: str, icon: str, member_id: Optional[str], detail: str = "") -> Dict[str, Any]:
    return {
        "id": badge_id,
        "title": title,
        "icon": icon,
        "member_id": member_id,
        "detail": detail,
    }


def achievements(
    state: Dict[str, Any],
    config: Dict[str, Any],
    *,
    now: Optional[datetime] = None,
) -> List[Dict[str, Any]]:
    """Считает пиксельные кубки по данным. Чистая функция от состояния."""
    settings = dict((config or {}).get("achievements") or {})
    tz_name = str((config or {}).get("timezone") or "Europe/Moscow")
    expenses = list(state.get("expenses", []))
    badges: List[Dict[str, Any]] = []
    if not expenses:
        return badges

    by_member: Dict[str, List[Dict[str, Any]]] = {key: [] for key in member_map(state)}
    for item in expenses:
        member_id = str(item.get("member_id"))
        if member_id in by_member:
            by_member[member_id].append(item)

    ordered = sorted(expenses, key=lambda entry: str(entry.get("created_at") or ""))
    first = ordered[0]
    badges.append(
        _badge(
            "first",
            "Первый вклад",
            "coin",
            str(first.get("member_id")),
            f"{first.get('title')} — {format_amount(first.get('amount'))}",
        )
    )

    week_start, week_end = week_anchor(tz_name, now=now)
    month_start, month_end = period_bounds("month", tz_name, now=now)

    def best(member_totals: Dict[str, float]) -> Optional[Tuple[str, float]]:
        if not member_totals:
            return None
        top = max(member_totals, key=lambda key: member_totals[key])
        if member_totals[top] <= 0:
            return None
        return top, round(member_totals[top], 2)

    week_totals: Dict[str, float] = {}
    month_totals: Dict[str, float] = {}
    month_counts: Dict[str, int] = {}
    for item in expenses:
        ts = _expense_ts(item)
        member_id = str(item.get("member_id"))
        if in_window(ts, week_start, week_end):
            week_totals[member_id] = week_totals.get(member_id, 0.0) + float(item.get("amount", 0))
        if in_window(ts, month_start, month_end):
            month_totals[member_id] = month_totals.get(member_id, 0.0) + float(item.get("amount", 0))
            month_counts[member_id] = month_counts.get(member_id, 0) + 1
    week_best = best(week_totals)
    if week_best:
        member_id, total = week_best
        badges.append(_badge("sponsor_week", "Спонсор недели", "star", member_id, f"{format_amount(total)} за неделю"))
    month_best = best(month_totals)
    if month_best:
        member_id, total = month_best
        badges.append(_badge("sponsor_month", "Спонсор месяца", "crown", member_id, f"{format_amount(total)} за месяц"))

    big_threshold = float(settings.get("big_one", 1000))
    big_owners: List[str] = []
    for item in expenses:
        if float(item.get("amount", 0)) >= big_threshold:
            owner = str(item.get("member_id"))
            if owner not in big_owners:
                big_owners.append(owner)
    for owner in big_owners:
        badges.append(_badge("big_one", "Кит", "whale", owner, f"покупка от {format_amount(big_threshold)}"))

    if month_counts:
        patron_threshold = int(settings.get("patron_month_count", 10))
        for member_id, count in month_counts.items():
            if count >= patron_threshold:
                badges.append(_badge("patron", "Меценат", "medal", member_id, f"{count} покупок за месяц"))

    collector_threshold = int(settings.get("collector_categories", 5))
    for member_id, items in by_member.items():
        categories = {str(item.get("category") or "other") for item in items}
        if len(categories) >= collector_threshold:
            badges.append(
                _badge("collector", "Коллекционер", "box", member_id, f"{len(categories)} категорий")
            )

    repeat_threshold = int(settings.get("repeat_title_count", 5))
    for member_id, items in by_member.items():
        counts: Dict[str, Tuple[str, int]] = {}
        for item in items:
            key = title_key(item.get("title"))
            title, count = counts.get(key, (str(item.get("title")), 0))
            counts[key] = (title, count + 1)
        for title, count in counts.values():
            if count >= repeat_threshold:
                badges.append(
                    _badge("repeat", f"{title} ×{count}", "flame", member_id, f"{count} раз")
                )

    night_from = int(settings.get("night_from", 0))
    night_to = int(settings.get("night_to", 5))
    early_from = int(settings.get("early_from", 6))
    early_to = int(settings.get("early_to", 8))
    night_owners: List[str] = []
    early_owners: List[str] = []
    for item in expenses:
        ts = _expense_ts(item)
        if ts is None:
            continue
        hour = ts.astimezone(resolve_timezone(tz_name)).hour
        member_id = str(item.get("member_id"))
        if night_from <= hour < night_to and member_id not in night_owners:
            night_owners.append(member_id)
        if early_from <= hour < early_to and member_id not in early_owners:
            early_owners.append(member_id)
    for owner in night_owners:
        badges.append(_badge("night_owl", "Ночной дозор", "moon", owner))
    for owner in early_owners:
        badges.append(_badge("early_bird", "Ранняя пташка", "sun", owner))

    stability_weeks = int(settings.get("stability_weeks", 4))
    for member_id, items in by_member.items():
        weeks = set()
        for item in items:
            ts = _expense_ts(item)
            if ts is None:
                continue
            local = ts.astimezone(resolve_timezone(tz_name))
            iso = local.isocalendar()
            weeks.add((iso.year, iso.week))
        if not weeks:
            continue
        run = 1
        best_run = 1
        ordered_weeks = sorted(weeks)
        for index in range(1, len(ordered_weeks)):
            previous = ordered_weeks[index - 1]
            current = ordered_weeks[index]
            if _iso_week_distance(previous, current) == 1:
                run += 1
                best_run = max(best_run, run)
            else:
                run = 1
        if best_run >= stability_weeks:
            badges.append(
                _badge("stability", "Стабильность", "calendar", member_id, f"{best_run} недель подряд")
            )

    veteran_days = int(settings.get("veteran_days", 100))
    local_now = now_local(tz_name, now=now)
    for member_id, items in by_member.items():
        stamps = [ts for ts in (_expense_ts(item) for item in items) if ts is not None]
        if not stamps:
            continue
        oldest = min(stamps).astimezone(resolve_timezone(tz_name))
        if (local_now - oldest).days >= veteran_days:
            badges.append(
                _badge("veteran", "Долгожитель общака", "trophy", member_id, f"{veteran_days}+ дней")
            )

    reaction_counts: Dict[str, int] = {}
    for item in expenses:
        for owner in (item.get("reactions") or {}):
            reaction_counts[owner] = reaction_counts.get(owner, 0) + 1
    if reaction_counts:
        top = max(reaction_counts, key=lambda key: reaction_counts[key])
        if reaction_counts[top] > 0:
            badges.append(
                _badge("reaction_magnet", "Реакция-магнит", "heart", top, f"{reaction_counts[top]} реакций")
            )

    return badges


def _iso_week_distance(previous: Tuple[int, int], current: Tuple[int, int]) -> int:
    """Разница в неделях между двумя ISO-неделями (корректно через год)."""
    previous_monday = date.fromisocalendar(previous[0], previous[1], 1)
    current_monday = date.fromisocalendar(current[0], current[1], 1)
    return (current_monday - previous_monday).days // 7


# ---------------------------------------------------------------------------
# Календарь активности и экспорт
# ---------------------------------------------------------------------------


def calendar(
    state: Dict[str, Any],
    config: Dict[str, Any],
    *,
    member_id: Optional[str] = None,
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    tz_name = str(config.get("timezone") or "Europe/Moscow")
    weeks = max(1, min(52, int(config.get("calendar_weeks", 8) or 8)))
    tz = resolve_timezone(tz_name)
    local_now = now_local(tz_name, now=now)
    today = local_now.replace(hour=0, minute=0, second=0, microsecond=0)
    start = today - timedelta(days=today.weekday()) - timedelta(weeks=weeks - 1)
    days: Dict[str, Dict[str, Any]] = {}
    cursor = start
    for _ in range(weeks * 7):
        days[cursor.date().isoformat()] = {"date": cursor.date().isoformat(), "total": 0.0, "count": 0}
        cursor += timedelta(days=1)
    for item in state.get("expenses", []):
        if member_id and item.get("member_id") != member_id:
            continue
        ts = _expense_ts(item)
        if ts is None:
            continue
        local = ts.astimezone(tz)
        key = local.date().isoformat()
        if key not in days:
            continue
        days[key]["total"] = round(days[key]["total"] + float(item.get("amount", 0)), 2)
        days[key]["count"] += 1
    ordered = [days[key] for key in sorted(days)]
    peak = max((day["total"] for day in ordered), default=0.0)
    return {
        "start": start.date().isoformat(),
        "weeks": weeks,
        "peak": peak,
        "days": ordered,
    }


def calendar_day_expenses(state: Dict[str, Any], config: Dict[str, Any], day: str) -> List[Dict[str, Any]]:
    tz = resolve_timezone(str(config.get("timezone") or "Europe/Moscow"))
    result: List[Dict[str, Any]] = []
    for item in state.get("expenses", []):
        ts = _expense_ts(item)
        if ts is None:
            continue
        if ts.astimezone(tz).date().isoformat() == day:
            result.append(item)
    result.sort(key=lambda entry: str(entry.get("created_at") or ""))
    return result


def export_csv(state: Dict[str, Any], config: Dict[str, Any]) -> str:
    names = {key: str(entry.get("name", key)) for key, entry in member_map(state).items()}
    categories = {
        str(entry.get("key")): str(entry.get("name", entry.get("key")))
        for entry in (config.get("categories") or [])
    }
    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow(["id", "date", "member", "title", "category", "amount", "request_id"])
    for item in sorted(state.get("expenses", []), key=lambda entry: str(entry.get("created_at") or "")):
        writer.writerow(
            [
                item.get("id"),
                item.get("created_at"),
                names.get(str(item.get("member_id")), item.get("member_id")),
                item.get("title"),
                categories.get(str(item.get("category")), item.get("category")),
                item.get("amount"),
                item.get("request_id") or "",
            ]
        )
    return buffer.getvalue()


# ---------------------------------------------------------------------------
# Составные вьюхи для API
# ---------------------------------------------------------------------------


def bootstrap(
    state: Dict[str, Any],
    config: Dict[str, Any],
    *,
    period: str = "all",
    telegram_id: Optional[int] = None,
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    tz_name = str(config.get("timezone") or "Europe/Moscow")
    period = period if period in PERIODS else "all"
    since, until = period_bounds(period, tz_name, now=now)
    period_expenses = query_expenses(state, since=since, until=until)
    me = resolve_member_id(state, telegram_id) if telegram_id is not None else None
    return {
        "currency": config.get("currency", "₽"),
        "currency_short": config.get("currency_short", "руб"),
        "period": period,
        "members": [
            {
                "key": entry.get("key"),
                "name": entry.get("name"),
                "color": entry.get("color"),
                "avatar": entry.get("avatar"),
                "linked": entry.get("telegram_id") is not None,
            }
            for entry in member_map(state).values()
        ],
        "categories": [dict(entry) for entry in (config.get("categories") or [])],
        "leaderboard": leaderboard(state, expenses=period_expenses),
        "totals_all": totals(state),
        "totals_period": totals(state, period_expenses),
        "recent": query_expenses(state, limit=40),
        "wishlist_open": wishlist_open(state),
        "wishlist_done": wishlist_done(state),
        "requests": requests_view(state),
        "achievements": achievements(state, config, now=now),
        "me": me,
        "quick_titles": list(config.get("quick_titles") or []),
        "poke_phrases": list(config.get("poke_phrases") or []),
        "light_phrases": list(config.get("light_phrases") or []),
        "streak": streak(state, config, now=now),
        "roulette": {
            "stats": roulette_stats(state, now=now, tz_name=tz_name),
            "history": [dict(entry) for entry in state.get("roulette", [])[-10:]][::-1],
        },
    }
