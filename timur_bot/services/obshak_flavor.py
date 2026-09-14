"""Забавное поверх общака: банка-уровни, кот-тамагочи, гонка недели, газета.

Всё здесь — чистые функции от состояния общака и конфига. Ничего не хранится
отдельно: настроение кота, уровень банки и заголовки новостей пересчитываются
из одних и тех же расходов.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from timur_bot.services import obshak


def _levels(config: Dict[str, Any], key: str) -> List[Dict[str, Any]]:
    section = config.get(key)
    if key == "pet":
        section = (config.get("pet") or {}).get("levels")
    if not isinstance(section, list) or not section:
        return [{"name": "—", "threshold": 0}]
    return sorted(section, key=lambda entry: int(entry.get("threshold", 0)))


def _level_for(value: float, levels: List[Dict[str, Any]]) -> Tuple[Dict[str, Any], Optional[Dict[str, Any]], int]:
    index = 0
    for position, level in enumerate(levels):
        if value >= float(level.get("threshold", 0)):
            index = position
    current = levels[index]
    following = levels[index + 1] if index + 1 < len(levels) else None
    return current, following, index


def jar_state(
    state: Dict[str, Any],
    config: Dict[str, Any],
) -> Dict[str, Any]:
    """Банка общака: накопленный итог, уровень, прогресс до следующего."""
    levels = _levels(config, "jar_levels")
    total = obshak.totals(state)["total"]
    current, following, index = _level_for(total, levels)
    progress = 1.0
    remaining = 0.0
    if following is not None:
        floor = float(current.get("threshold", 0))
        ceiling = float(following.get("threshold", floor + 1))
        span = max(1.0, ceiling - floor)
        progress = max(0.0, min(1.0, (total - floor) / span))
        remaining = round(max(0.0, ceiling - total), 2)
    return {
        "total": total,
        "level": {"name": current.get("name"), "threshold": current.get("threshold"), "index": index},
        "next": (
            {"name": following.get("name"), "threshold": following.get("threshold")}
            if following is not None
            else None
        ),
        "progress": round(progress, 4),
        "remaining": remaining,
        "levels": levels,
    }


def _last_expense_ts(state: Dict[str, Any]) -> Optional[datetime]:
    stamps = [ts for ts in (obshak.parse_ts(item.get("created_at")) for item in state.get("expenses", [])) if ts]
    return max(stamps) if stamps else None


def pet_state(
    state: Dict[str, Any],
    config: Dict[str, Any],
    *,
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    """Кот-тамагочи: уровень от числа покупок, настроение от активности."""
    pet_config = config.get("pet") or {}
    levels = _levels(config, "pet")
    expenses = state.get("expenses", [])
    count = len(expenses)
    current, following, index = _level_for(float(count), levels)
    tz_name = str(config.get("timezone") or "Europe/Moscow")
    local_now = obshak.now_local(tz_name, now=now)

    last_ts = _last_expense_ts(state)
    days_since: Optional[int] = None
    if last_ts is not None:
        days_since = max(0, (local_now - last_ts.astimezone(obshak.resolve_timezone(tz_name))).days)

    week_since, _ = obshak.period_bounds("week", tz_name, now=now)
    recent_categories = {
        str(item.get("category") or "other")
        for item in expenses
        if obshak.in_window(obshak.parse_ts(item.get("created_at")), week_since, None)
    }
    recent_count = sum(
        1
        for item in expenses
        if obshak.in_window(obshak.parse_ts(item.get("created_at")), week_since, None)
    )

    if count == 0 or (days_since is not None and days_since >= 5):
        mood = "hungry"
    elif days_since is not None and days_since >= 3:
        mood = "bored"
    elif recent_count >= 3 and len(recent_categories) >= 3:
        mood = "proud"
    else:
        mood = "happy"

    phrases = ((pet_config.get("phrases") or {}).get(mood)) or ["мяу"]
    seed = count + local_now.timetuple().tm_yday
    phrase = str(phrases[seed % len(phrases)])

    return {
        "name": pet_config.get("name") or "Барсик",
        "count": count,
        "level": {"name": current.get("name"), "threshold": current.get("threshold"), "index": index},
        "next": (
            {"name": following.get("name"), "threshold": following.get("threshold")}
            if following is not None
            else None
        ),
        "mood": mood,
        "phrase": phrase,
        "days_since_last": days_since,
        "sleeping": local_now.hour < 7 or local_now.hour >= 23,
    }


def race(
    state: Dict[str, Any],
    config: Dict[str, Any],
    *,
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    """Гонка недели: позиции участников от 0 до 1 по вкладу за текущую неделю."""
    tz_name = str(config.get("timezone") or "Europe/Moscow")
    since, until = obshak.period_bounds("week", tz_name, now=now)
    week_start, week_end = obshak.week_anchor(tz_name, now=now)
    rows = obshak.leaderboard(state, since=since, until=until)
    top = max((row["total"] for row in rows), default=0.0)
    lanes: List[Dict[str, Any]] = []
    for row in rows:
        position = 0.0 if top <= 0 else min(1.0, row["total"] / top)
        lanes.append({**row, "position": round(position, 4)})
    remaining = week_end - obshak.now_local(tz_name, now=now)
    return {
        "lanes": lanes,
        "leader": lanes[0]["member_id"] if lanes and lanes[0]["total"] > 0 else None,
        "week_start": week_start.isoformat(),
        "reset_at": week_end.isoformat(),
        "seconds_to_reset": max(0, int(remaining.total_seconds())),
        "week_total": round(sum(row["total"] for row in lanes), 2),
    }


def _pick(options: List[str], seed: int) -> str:
    if not options:
        return ""
    return str(options[seed % len(options)])


def format_money(value: Any) -> str:
    """Число в человеческий вид: 1200 → «1 200», 140.5 → «140.50»."""
    try:
        number = float(value)
    except (TypeError, ValueError):
        return str(value)
    if abs(number - round(number)) < 0.005:
        return f"{int(round(number)):,}".replace(",", "\u00a0")
    return f"{number:,.2f}".replace(",", "\u00a0")


def news(
    state: Dict[str, Any],
    config: Dict[str, Any],
    *,
    now: Optional[datetime] = None,
) -> List[Dict[str, str]]:
    """Пиксельная газета: заголовки, собранные из фактов общака."""
    templates = config.get("news") or {}
    currency = config.get("currency", "₽")
    tz_name = str(config.get("timezone") or "Europe/Moscow")
    names = {key: str(entry.get("name", key)) for key, entry in obshak.member_map(state).items()}
    expenses = list(state.get("expenses", []))
    headlines: List[Dict[str, str]] = []

    if not expenses:
        return [
            {
                "kind": "starter",
                "text": "КАССА ПУСТА: первая покупка войдёт в историю",
            }
        ]

    def add(kind: str, **fields: Any) -> None:
        template = _pick(list(templates.get(kind) or []), len(headlines) + len(expenses))
        if not template:
            return
        try:
            text = template.format(currency=currency, **fields)
        except (KeyError, IndexError):
            return
        headlines.append({"kind": kind, "text": text})

    since, until = obshak.period_bounds("week", tz_name, now=now)
    week_rows = obshak.leaderboard(state, since=since, until=until)
    if week_rows and week_rows[0]["total"] > 0:
        add("sponsor_week", name=week_rows[0]["name"], amount=format_money(week_rows[0]["total"]))

    title_counts: Dict[str, Tuple[str, int]] = {}
    for item in expenses:
        key = obshak.title_key(item.get("title"))
        title, count = title_counts.get(key, (str(item.get("title")), 0))
        title_counts[key] = (title, count + 1)
    if title_counts:
        popular_title, popular_count = max(title_counts.values(), key=lambda entry: entry[1])
        if popular_count >= 2:
            add("frequent_item", item=popular_title, count=popular_count)

    biggest = max(expenses, key=lambda item: float(item.get("amount", 0)))
    if float(biggest.get("amount", 0)) >= 500:
        add(
            "big_purchase",
            name=names.get(str(biggest.get("member_id")), "—"),
            amount=format_money(biggest.get("amount")),
        )

    last_ts = _last_expense_ts(state)
    local_now = obshak.now_local(tz_name, now=now)
    if last_ts is not None:
        days_since = max(0, (local_now - last_ts.astimezone(obshak.resolve_timezone(tz_name))).days)
        if days_since >= 3:
            add("idle", days=days_since)

    night_from = int((config.get("achievements") or {}).get("night_from", 0))
    night_to = int((config.get("achievements") or {}).get("night_to", 5))
    night_items = [
        item
        for item in expenses
        if obshak.parse_ts(item.get("created_at")) is not None
        and night_from
        <= obshak.parse_ts(item.get("created_at")).astimezone(obshak.resolve_timezone(tz_name)).hour
        < night_to
    ]
    if night_items:
        latest = max(night_items, key=lambda item: str(item.get("created_at")))
        stamp = obshak.parse_ts(latest.get("created_at"))
        time_label = stamp.astimezone(obshak.resolve_timezone(tz_name)).strftime("%H:%M") if stamp else ""
        add("night_owl", name=names.get(str(latest.get("member_id")), "—"), time=time_label)

    return headlines[:5]


def week_quest(
    state: Dict[str, Any],
    config: Dict[str, Any],
    *,
    now: Optional[datetime] = None,
) -> Optional[Dict[str, Any]]:
    """Квест текущей недели: один на всех, меняется по номеру ISO-недели."""
    quests = config.get("weekly_quests") or []
    if not quests:
        return None
    tz_name = str(config.get("timezone") or "Europe/Moscow")
    local_now = obshak.now_local(tz_name, now=now)
    iso = local_now.isocalendar()
    spec = quests[(iso.year * 53 + iso.week) % len(quests)]
    week_start, week_end = obshak.week_anchor(tz_name, now=now)
    tz = obshak.resolve_timezone(tz_name)

    week_items = [
        item
        for item in state.get("expenses", [])
        if obshak.in_window(obshak.parse_ts(item.get("created_at")), week_start, week_end)
    ]
    key = str(spec.get("key") or "")
    target = int(spec.get("target") or 1)
    kind = "count"

    if key == "days":
        progress = len(
            {
                obshak.parse_ts(item.get("created_at")).astimezone(tz).date()
                for item in week_items
                if obshak.parse_ts(item.get("created_at")) is not None
            }
        )
    elif key == "household":
        progress = 1 if any(str(item.get("category")) == "household" for item in week_items) else 0
    elif key == "everyone":
        progress = len({str(item.get("member_id")) for item in week_items})
    elif key == "amount":
        progress = round(sum(float(item.get("amount", 0)) for item in week_items), 2)
        kind = "money"
    else:
        progress = 0

    return {
        "key": key,
        "title": str(spec.get("title") or key),
        "progress": progress,
        "target": target,
        "kind": kind,
        "done": progress >= target,
        "week_start": week_start.date().isoformat(),
    }


def roast(
    state: Dict[str, Any],
    config: Dict[str, Any],
    member_id: str,
    *,
    now: Optional[datetime] = None,
) -> Optional[Dict[str, str]]:
    """Подкол кота для конкретного человека: самая уместная колкость, что нашлась."""
    entry = obshak.member(state, member_id)
    if entry is None:
        return None
    templates = config.get("roasts") or {}
    if not templates:
        return None
    tz_name = str(config.get("timezone") or "Europe/Moscow")
    tz = obshak.resolve_timezone(tz_name)
    local_now = obshak.now_local(tz_name, now=now)
    name = str(entry.get("name") or member_id)
    expenses = list(state.get("expenses", []))
    mine = [item for item in expenses if str(item.get("member_id")) == member_id]
    rows = obshak.leaderboard(state)
    my_row = next((row for row in rows if row["member_id"] == member_id), None)
    grand_total = obshak.totals(state)["total"]

    key = "default"
    placeholders: Dict[str, Any] = {"name": name, "total": format_money(grand_total)}

    if not mine:
        key = "zero"
    else:
        stamps = [ts for ts in (obshak.parse_ts(item.get("created_at")) for item in mine) if ts]
        last_local = max(stamps).astimezone(tz) if stamps else None
        days_since = max(0, (local_now - last_local).days) if last_local else 0
        top_row = rows[0] if rows else None
        mine_total = float(my_row["total"]) if my_row else 0.0
        top_total = float(top_row["total"]) if top_row else 0.0
        is_last = bool(my_row) and my_row is rows[-1] and len(rows) > 1

        title_counts: Dict[str, Tuple[str, int]] = {}
        for item in mine:
            title_key = obshak.title_key(item.get("title"))
            title, count = title_counts.get(title_key, (str(item.get("title")), 0))
            title_counts[title_key] = (title, count + 1)
        popular = max(title_counts.values(), key=lambda pair: pair[1], default=None)

        achievements = config.get("achievements") or {}
        night_from = int(achievements.get("night_from", 0))
        night_to = int(achievements.get("night_to", 5))
        night_recent_hours = int(achievements.get("night_recent_hours", 6))
        last_hour = last_local.hour if last_local else 12

        if days_since >= 7:
            key = "absent"
            placeholders["days"] = days_since
        elif is_last and top_total >= max(1.0, 2 * max(mine_total, 1.0)):
            key = "last_place"
            placeholders.update(
                {
                    "place": len(rows),
                    "total_members": len(rows),
                    "top_name": str(top_row["name"]) if top_row else "—",
                    "top_total": format_money(top_total),
                    "mine": format_money(mine_total),
                }
            )
        elif popular and popular[1] >= 3:
            key = "same_item"
            placeholders.update({"item": popular[0], "count": popular[1]})
        elif (
            top_total > 0
            and mine_total >= top_total
            and mine_total >= int((config.get("achievements") or {}).get("big_one", 1000))
            and mine_total >= grand_total * 0.4
        ):
            key = "big_spender"
        elif (
            last_local is not None
            and night_from <= last_hour < night_to
            and (local_now - last_local) <= timedelta(hours=night_recent_hours)
        ):
            # «спи» уместно, только если человек правда только что сходил ночью:
            # иначе в десять утра кот всё ещё вспоминал бы 00:18.
            key = "night"
            placeholders["time"] = last_local.strftime("%H:%M")

    options = templates.get(key) or templates.get("default") or []
    if not options:
        return None
    seed = local_now.timetuple().tm_yday + len(mine) + (len(member_id) * 7)
    template = str(options[seed % len(options)])
    try:
        text = template.format(**placeholders)
    except (KeyError, IndexError):
        fallback = (templates.get("default") or ["{name}, соседи ждут твоих подвигов"])[0]
        text = str(fallback).format(name=name, total=format_money(grand_total))
    return {"key": key, "text": text}


def member_card(
    state: Dict[str, Any],
    config: Dict[str, Any],
    member_id: str,
    *,
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    """Данные карточки участника: статистика, место в рейтинге, кубки."""
    entry = obshak.member(state, member_id)
    if entry is None:
        raise obshak.ObshakError("unknown_member")
    rows = obshak.leaderboard(state)
    rank = next((index for index, row in enumerate(rows) if row["member_id"] == member_id), None)
    stats = obshak.member_stats(state, member_id)
    badges = [badge for badge in obshak.achievements(state, config, now=now) if badge.get("member_id") == member_id]
    return {
        "member": {
            "key": entry.get("key"),
            "name": entry.get("name"),
            "color": entry.get("color"),
            "avatar": entry.get("avatar"),
        },
        "rank": None if rank is None else rank + 1,
        "members_total": len(rows),
        "stats": stats,
        "badges": badges,
    }
