"""Тесты домена общака: расходы, лидерборд, вишлист, запросы, ачивки и «вкусовые» фишки."""

import os
import random
from datetime import datetime, timedelta, timezone

os.environ.setdefault("TELEGRAM_BOT_TOKEN", "test-token")
os.environ.setdefault("OPENAI_API_KEY", "test-api-key")

from timur_bot.core.config import load_app_config
from timur_bot.services import obshak, obshak_flavor

CONFIG = load_app_config().obshak_defaults
TZ = "Europe/Moscow"
NOW = datetime(2026, 9, 13, 12, 0, tzinfo=timezone.utc)


def fresh_state():
    return obshak.default_state(CONFIG)


def add(state, member_id, amount, title, category="food", days_ago=0, hour=12):
    stamp = (NOW - timedelta(days=days_ago)).replace(hour=hour)
    return obshak.add_expense(
        state,
        member_id=member_id,
        amount=amount,
        title=title,
        category=category,
        created_at=stamp,
        tz_name=TZ,
    )


# --- участники ---------------------------------------------------------


def test_default_state_has_configured_members_without_telegram_ids():
    state = fresh_state()
    assert list(obshak.member_map(state)) == ["rustem", "kadyr", "dilyara", "amir"]
    assert all(entry["telegram_id"] is None for entry in obshak.member_map(state).values())


def test_link_member_rules():
    state = fresh_state()
    assert obshak.link_member(state, "amir", 111)[0] is True
    assert obshak.link_member(state, "kadyr", 111) == (False, "already_linked")
    assert obshak.link_member(state, "amir", 222) == (False, "slot_taken")
    assert obshak.link_member(state, "nobody", 333) == (False, "unknown_member")
    assert obshak.resolve_member_id(state, 111) == "amir"
    assert obshak.release_member(state, 111) is True
    assert obshak.resolve_member_id(state, 111) is None
    assert obshak.link_member(state, "kadyr", 111)[0] is True


# --- расходы -----------------------------------------------------------


def test_add_expense_validates_input():
    state = fresh_state()
    for kwargs in (
        {"member_id": "ghost", "amount": 10, "title": "x"},
        {"member_id": "amir", "amount": 0, "title": "x"},
        {"member_id": "amir", "amount": -5, "title": "x"},
        {"member_id": "amir", "amount": 10, "title": "   "},
        {"member_id": "amir", "amount": 10_000_000, "title": "x"},
    ):
        try:
            obshak.add_expense(state, **kwargs)
        except obshak.ObshakError:
            continue
        raise AssertionError(f"ожидали ошибку для {kwargs}")


def test_leaderboard_and_totals():
    state = fresh_state()
    add(state, "amir", 140, "майонез")
    add(state, "amir", 90, "майонез")
    add(state, "kadyr", 1000, "пылесос", category="household")
    rows = obshak.leaderboard(state)
    # При равных суммах сохраняется порядок из конфига: kadyr, amir, rustem, dilyara.
    assert [row["member_id"] for row in rows] == ["kadyr", "amir", "rustem", "dilyara"]
    assert rows[0]["total"] == 1000
    assert rows[1]["total"] == 230
    assert rows[1]["count"] == 2
    assert obshak.totals(state) == {"total": 1230.0, "count": 3}


def test_period_filtering():
    state = fresh_state()
    add(state, "amir", 100, "старое", days_ago=40)
    add(state, "amir", 200, "на этой неделе", days_ago=1)
    week_since, week_until = obshak.period_bounds("week", TZ, now=NOW)
    month_since, month_until = obshak.period_bounds("month", TZ, now=NOW)
    assert obshak.totals(state, obshak.query_expenses(state, since=week_since, until=week_until))["total"] == 200
    assert obshak.totals(state, obshak.query_expenses(state, since=month_since, until=month_until))["total"] == 200
    assert obshak.totals(state)["total"] == 300


def test_update_and_delete_expense():
    state = fresh_state()
    expense = add(state, "amir", 100, "чай")
    obshak.update_expense(state, expense["id"], amount=150, title="чай зелёный")
    assert obshak.find_expense(state, expense["id"])["amount"] == 150
    assert obshak.find_expense(state, expense["id"])["title"] == "чай зелёный"
    assert obshak.delete_expense(state, expense["id"]) is True
    assert obshak.find_expense(state, expense["id"]) is None
    assert obshak.delete_expense(state, "nope") is False


def test_reactions_toggle():
    state = fresh_state()
    expense = add(state, "amir", 100, "чай")
    obshak.set_reaction(state, expense["id"], "kadyr", "fire")
    assert expense["reactions"] == {"kadyr": "fire"}
    obshak.set_reaction(state, expense["id"], "kadyr", "fire")
    assert expense["reactions"] == {}
    try:
        obshak.set_reaction(state, expense["id"], "kadyr", "nope")
    except obshak.ObshakError:
        pass
    else:
        raise AssertionError("ожидали ошибку для неизвестной реакции")


# --- вишлист -----------------------------------------------------------


def test_wishlist_lifecycle():
    state = fresh_state()
    item = obshak.wishlist_add(state, title="бумага", created_by="kadyr")
    assert [entry["id"] for entry in obshak.wishlist_open(state)] == [item["id"]]
    obshak.wishlist_claim(state, item["id"], "amir")
    assert obshak.find_wishlist(state, item["id"])["claimed_by"] == "amir"
    obshak.wishlist_claim(state, item["id"], "amir")
    assert obshak.find_wishlist(state, item["id"])["claimed_by"] is None
    done_item, expense = obshak.wishlist_complete(state, item["id"], member_id="amir", amount=200)
    assert done_item["status"] == "done"
    assert expense["member_id"] == "amir"
    assert obshak.totals(state)["total"] == 200
    assert obshak.wishlist_open(state) == []
    try:
        obshak.wishlist_complete(state, item["id"], member_id="amir", amount=10)
    except obshak.ObshakError as exc:
        assert str(exc) == "item_closed"
    else:
        raise AssertionError("закрытый пункт нельзя купить дважды")
    assert obshak.wishlist_delete(state, item["id"]) is True


# --- запросы денег -----------------------------------------------------


def test_request_flow():
    state = fresh_state()
    request = obshak.request_create(state, created_by="rustem", title="шампунь", amount=150)
    assert obshak.request_targets(request, state) == ["kadyr", "dilyara", "amir"]
    try:
        obshak.request_pay(state, request["id"], member_id="rustem")
    except obshak.ObshakError as exc:
        assert str(exc) == "not_a_target"
    else:
        raise AssertionError("автор запроса не должен попадать в цели")
    obshak.request_pay(state, request["id"], member_id="amir")
    try:
        obshak.request_pay(state, request["id"], member_id="amir")
    except obshak.ObshakError as exc:
        assert str(exc) == "already_paid"
    else:
        raise AssertionError("нельзя скинуться дважды")
    progress = obshak.request_progress(state, request)
    assert progress["collected"] == 150
    assert progress["expected_total"] == 450
    assert progress["paid_count"] == 1
    assert obshak.totals(state)["total"] == 150
    try:
        obshak.request_close(state, request["id"], "amir")
    except obshak.ObshakError as exc:
        assert str(exc) == "not_author"
    else:
        raise AssertionError("закрыть может только автор")
    assert obshak.request_close(state, request["id"], "rustem")["status"] == "closed"
    assert obshak.requests_view(state) == []
    assert len(obshak.requests_view(state, include_closed=True)) == 1


def test_request_targeted_at_one_member():
    state = fresh_state()
    request = obshak.request_create(
        state, created_by="rustem", title="долг", amount=300, scope="amir", per_person=True
    )
    assert obshak.request_targets(request, state) == ["amir"]
    progress = obshak.request_progress(state, request)
    assert progress["expected_total"] == 300
    assert progress["target_count"] == 1


# --- ачивки ------------------------------------------------------------


def test_achievements_cover_main_rules():
    state = fresh_state()
    add(state, "kadyr", 1800, "пылесос", category="household")
    # 22:00 UTC — это 01:00 по Москве, то есть ночной дозор.
    for index in range(5):
        add(state, "amir", 100, "майонез", days_ago=index, hour=22)
    ids = {(badge["id"], badge["member_id"]) for badge in obshak.achievements(state, CONFIG, now=NOW)}
    assert ("big_one", "kadyr") in ids
    assert ("repeat", "amir") in ids
    assert ("night_owl", "amir") in ids
    assert ("collector", "kadyr") not in ids
    assert any(badge_id == "sponsor_week" for badge_id, _ in ids)
    assert any(badge_id == "sponsor_month" for badge_id, _ in ids)


def test_achievements_empty_state():
    assert obshak.achievements(fresh_state(), CONFIG, now=NOW) == []


def test_badge_amounts_are_human_readable():
    state = fresh_state()
    add(state, "amir", 300, "майонез")
    badges = obshak.achievements(state, CONFIG, now=NOW)
    first = next(badge for badge in badges if badge["id"] == "first")
    assert first["detail"] == "майонез — 300"


# --- календарь и экспорт ----------------------------------------------


def test_calendar_counts_days_and_peak():
    state = fresh_state()
    add(state, "amir", 100, "чай", days_ago=1)
    add(state, "kadyr", 300, "пылесос", days_ago=1)
    add(state, "dilyara", 50, "хлеб", days_ago=6)
    calendar = obshak.calendar(state, CONFIG, now=NOW)
    assert calendar["weeks"] == CONFIG["calendar_weeks"]
    assert calendar["peak"] == 400
    by_date = {day["date"]: day for day in calendar["days"]}
    target = (NOW - timedelta(days=1)).date().isoformat()
    assert by_date[target]["count"] == 2
    assert by_date[target]["total"] == 400
    day_expenses = obshak.calendar_day_expenses(state, CONFIG, target)
    assert {item["title"] for item in day_expenses} == {"чай", "пылесос"}


def test_export_csv_header_and_rows():
    state = fresh_state()
    add(state, "amir", 140, "майонез")
    csv_text = obshak.export_csv(state, CONFIG)
    lines = csv_text.strip().splitlines()
    assert lines[0] == "id,date,member,title,category,amount,request_id"
    assert "майонез" in lines[1]
    assert "Амир" in lines[1]


# --- хранилище ---------------------------------------------------------


def test_save_and_load_round_trip(tmp_path):
    state = fresh_state()
    obshak.link_member(state, "amir", 111, username="amir")
    add(state, "amir", 140, "майонез")
    path = tmp_path / "obshak.json"
    obshak.save_state(path, state)
    loaded = obshak.load_state(path, CONFIG)
    assert obshak.totals(loaded)["total"] == 140
    assert obshak.resolve_member_id(loaded, 111) == "amir"
    assert loaded["meta"]["revision"] == 1
    assert not list(tmp_path.glob(".*tmp"))


def test_load_state_tolerates_broken_file(tmp_path):
    path = tmp_path / "obshak.json"
    path.write_text("{ not json", encoding="utf-8")
    state = obshak.load_state(path, CONFIG)
    assert state["expenses"] == []
    assert list(obshak.member_map(state)) == ["rustem", "kadyr", "dilyara", "amir"]


def test_ensure_state_schema_drops_orphan_expenses():
    state = fresh_state()
    state["expenses"].append({"id": "e1", "member_id": "ghost", "amount": 10, "title": "x"})
    obshak.ensure_state_schema(state, CONFIG)
    assert state["expenses"] == []


# --- «вкусовые» фишки --------------------------------------------------


def test_jar_levels_and_progress():
    state = fresh_state()
    assert obshak_flavor.jar_state(state, CONFIG)["level"]["name"] == "Пустая банка"
    add(state, "amir", 1000, "закупка")
    jar = obshak_flavor.jar_state(state, CONFIG)
    assert jar["level"]["name"] == "На доширак"
    assert jar["next"]["name"] == "На пельмени"
    assert 0 <= jar["progress"] <= 1
    assert jar["remaining"] == 2000


def test_jar_magnate_at_top_level():
    state = fresh_state()
    add(state, "amir", 30000, "всё сразу")
    jar = obshak_flavor.jar_state(state, CONFIG)
    assert jar["level"]["name"] == "Магнат соседей"
    assert jar["next"] is None
    assert jar["progress"] == 1


def test_pet_moods_follow_activity():
    state = fresh_state()
    hungry = obshak_flavor.pet_state(state, CONFIG, now=NOW)
    assert hungry["mood"] == "hungry"
    assert hungry["phrase"]
    add(state, "amir", 100, "чай")
    assert obshak_flavor.pet_state(state, CONFIG, now=NOW)["mood"] == "happy"
    old = fresh_state()
    add(old, "amir", 100, "чай", days_ago=6)
    assert obshak_flavor.pet_state(old, CONFIG, now=NOW)["mood"] == "hungry"
    bored = fresh_state()
    add(bored, "amir", 100, "чай", days_ago=4)
    assert obshak_flavor.pet_state(bored, CONFIG, now=NOW)["mood"] == "bored"


def test_race_positions_and_reset_window():
    state = fresh_state()
    add(state, "amir", 100, "чай", days_ago=1)
    add(state, "kadyr", 200, "пылесос", days_ago=2)
    add(state, "dilyara", 999, "старое", days_ago=30)
    race = obshak_flavor.race(state, CONFIG, now=NOW)
    assert race["leader"] == "kadyr"
    assert race["week_total"] == 300
    positions = {lane["member_id"]: lane["position"] for lane in race["lanes"]}
    assert positions["kadyr"] == 1
    assert positions["dilyara"] == 0
    assert race["seconds_to_reset"] > 0


def test_news_uses_real_facts():
    state = fresh_state()
    assert obshak_flavor.news(state, CONFIG, now=NOW)[0]["kind"] == "starter"
    add(state, "amir", 140, "майонез", days_ago=1)
    add(state, "amir", 90, "майонез", days_ago=2)
    add(state, "kadyr", 1200, "телек", category="fun", days_ago=1)
    headlines = obshak_flavor.news(state, CONFIG, now=NOW)
    text = " ".join(item["text"] for item in headlines)
    assert "Кадыр" in text
    assert "майонез" in text
    assert len(headlines) <= 5


def test_member_card_stats_and_rank():
    state = fresh_state()
    add(state, "amir", 140, "майонез", category="food")
    add(state, "amir", 90, "майонез", category="food")
    add(state, "kadyr", 1000, "пылесос", category="household")
    card = obshak_flavor.member_card(state, CONFIG, "amir", now=NOW)
    assert card["rank"] == 2
    assert card["stats"]["count"] == 2
    assert card["stats"]["top_category"]["key"] == "food"
    assert card["stats"]["biggest"]["amount"] == 140
    assert card["badges"]


def test_roulette_starts_even_and_records_spin():
    state = fresh_state()
    weights = obshak.roulette_weights(state, now=NOW, tz_name=TZ)
    assert set(weights.values()) == {1.0}
    result = obshak.roulette_spin(state, CONFIG, now=NOW, rng=random.Random(11))
    assert result["winner"] in obshak.member_ids(state)
    assert len(state["roulette"]) == 1
    assert state["roulette"][0]["member_id"] == result["winner"]
    assert abs(sum(result["weights"].values()) - 1) < 0.02


def test_roulette_balances_repeat_walkers():
    state = fresh_state()
    for _ in range(5):
        obshak.roulette_spin(state, CONFIG, now=NOW, rng=_AlwaysFirst())
    stats = obshak.roulette_stats(state, now=NOW, tz_name=TZ)
    assert stats["total"] == 5
    weights = obshak.roulette_weights(state, now=NOW, tz_name=TZ)
    heavy = max(stats["counts"], key=lambda key: stats["counts"][key])
    light = min(stats["counts"], key=lambda key: stats["counts"][key])
    assert stats["counts"][heavy] == 5
    assert weights[heavy] < weights[light]


def test_roulette_ignores_spins_outside_window():
    state = fresh_state()
    obshak.roulette_spin(state, CONFIG, now=NOW - timedelta(days=40), rng=random.Random(1))
    stats = obshak.roulette_stats(state, now=NOW, tz_name=TZ)
    assert stats["total"] == 0
    assert stats["last"] is None
    wider = obshak.roulette_stats(state, days=60, now=NOW, tz_name=TZ)
    assert sum(wider["counts"].values()) == 1
    assert wider["last"]["member_id"] in obshak.member_ids(state)


def test_roulette_history_is_trimmed():
    state = fresh_state()
    for index in range(obshak.ROULETTE_HISTORY_LIMIT + 20):
        obshak.roulette_spin(state, CONFIG, now=NOW, rng=_AlwaysFirst())
    obshak.ensure_state_schema(state, CONFIG)
    assert len(state["roulette"]) == obshak.ROULETTE_HISTORY_LIMIT


class _AlwaysFirst:
    """Стаб random: всегда берёт первый участник — для детерминированных тестов."""

    def random(self) -> float:
        return 0.0


def test_streak_counts_consecutive_days():
    state = fresh_state()
    add(state, "amir", 100, "чай")
    add(state, "amir", 100, "чай", days_ago=1)
    add(state, "kadyr", 100, "чай", days_ago=2)
    add(state, "amir", 100, "чай", days_ago=5)
    result = obshak.streak(state, CONFIG, now=NOW)
    assert result["current"] == 3
    assert result["best"] == 3
    assert result["today"] is True
    assert len(result["days"]) == 14
    assert result["days"][-1]["hit"] is True


def test_streak_survives_empty_today():
    state = fresh_state()
    add(state, "amir", 100, "чай", days_ago=1)
    add(state, "amir", 100, "чай", days_ago=2)
    result = obshak.streak(state, CONFIG, now=NOW)
    assert result["current"] == 2
    assert result["today"] is False


def test_streak_breaks_on_gap():
    state = fresh_state()
    add(state, "amir", 100, "чай", days_ago=3)
    add(state, "amir", 100, "чай", days_ago=2)
    add(state, "amir", 100, "чай", days_ago=1)
    add(state, "amir", 100, "чай", days_ago=10)
    result = obshak.streak(state, CONFIG, now=NOW)
    assert result["current"] == 3
    assert result["best"] == 3


def test_streak_empty_state():
    result = obshak.streak(fresh_state(), CONFIG, now=NOW)
    assert result["current"] == 0
    assert result["best"] == 0
    assert all(day["hit"] is False for day in result["days"])


def test_week_quest_rotates_and_tracks_progress():
    state = fresh_state()
    quest = obshak_flavor.week_quest(state, CONFIG, now=NOW)
    assert quest is not None
    assert quest["key"] in {spec["key"] for spec in CONFIG["weekly_quests"]}
    assert quest["done"] is False

    # Тот же момент времени всегда даёт тот же квест, следующая неделя — другой.
    assert obshak_flavor.week_quest(state, CONFIG, now=NOW)["key"] == quest["key"]
    next_week = obshak_flavor.week_quest(state, CONFIG, now=NOW + timedelta(days=7))
    assert next_week["key"] != quest["key"] or len(CONFIG["weekly_quests"]) == 1


def test_week_quest_days_progress():
    state = fresh_state()
    add(state, "amir", 100, "чай")
    add(state, "amir", 100, "чай", days_ago=1)
    add(state, "kadyr", 100, "чай", days_ago=1)
    add(state, "amir", 100, "чай", days_ago=9)
    quest = _force_quest(state, "days")
    assert quest["key"] == "days"
    assert quest["progress"] == 2
    assert quest["kind"] == "count"


def test_week_quest_household_and_everyone_and_amount():
    state = fresh_state()
    add(state, "amir", 100, "чай", category="household")
    add(state, "kadyr", 300, "пылесос", category="household")
    assert _force_quest(state, "household")["progress"] == 1
    assert _force_quest(state, "everyone")["progress"] == 2
    amount = _force_quest(state, "amount")
    assert amount["progress"] == 400
    assert amount["kind"] == "money"


def _force_quest(state, key):
    """Считает конкретный квест, подменяя конфиг на нужный ключ."""
    config = dict(CONFIG)
    config["weekly_quests"] = [{"key": key, "title": key, "target": 3}]
    return obshak_flavor.week_quest(state, config, now=NOW)


def test_roast_zero_for_newcomer():
    roast = obshak_flavor.roast(fresh_state(), CONFIG, "dilyara", now=NOW)
    assert roast["key"] == "zero"
    assert "Диляра" in roast["text"]


def test_roast_absent_when_idle():
    state = fresh_state()
    add(state, "amir", 100, "чай", days_ago=9)
    roast = obshak_flavor.roast(state, CONFIG, "amir", now=NOW)
    assert roast["key"] == "absent"
    assert "9" in roast["text"]


def test_roast_last_place_and_same_item():
    state = fresh_state()
    add(state, "kadyr", 9000, "пылесос")
    add(state, "rustem", 800, "хлеб")
    add(state, "dilyara", 500, "вода")
    add(state, "amir", 100, "майонез")
    add(state, "amir", 100, "майонез", days_ago=1)
    add(state, "amir", 100, "майонез", days_ago=2)
    last_place = obshak_flavor.roast(state, CONFIG, "amir", now=NOW)
    assert last_place["key"] == "last_place"
    assert "300" in last_place["text"] or "место" in last_place["text"].lower()

    state2 = fresh_state()
    add(state2, "amir", 100, "майонез")
    add(state2, "amir", 100, "майонез", days_ago=1)
    add(state2, "amir", 100, "майонез", days_ago=2)
    same_item = obshak_flavor.roast(state2, CONFIG, "amir", now=NOW)
    assert same_item["key"] == "same_item"
    assert "майонез" in same_item["text"]

def _night_state(hour_utc, day):
    state = fresh_state()
    obshak.add_expense(
        state,
        member_id="amir",
        amount=100,
        title="чай",
        created_at=datetime(2026, 9, day, hour_utc, 0, tzinfo=timezone.utc),
    )
    return state


def test_roast_night_fires_right_after_a_night_purchase():
    # 01:00 МСК, сейчас 01:40 МСК — «спи» уместно.
    state = _night_state(22, 12)
    roast = obshak_flavor.roast(
        state, CONFIG, "amir", now=datetime(2026, 9, 12, 22, 40, tzinfo=timezone.utc)
    )
    assert roast["key"] == "night"
    assert "01:00" in roast["text"]


def test_roast_night_expires_by_morning():
    # Покупка в 00:18 МСК, а сейчас 10:23 МСК: кот не должен вспоминать ночь.
    state = _night_state(21, 13)
    roast = obshak_flavor.roast(
        state, CONFIG, "amir", now=datetime(2026, 9, 14, 7, 23, tzinfo=timezone.utc)
    )
    assert roast["key"] == "default"
    assert "00:18" not in roast["text"]


def test_roast_big_spender():
    state = fresh_state()
    add(state, "amir", 5000, "закупка")
    add(state, "kadyr", 100, "чай")
    roast = obshak_flavor.roast(state, CONFIG, "amir", now=NOW)
    assert roast["key"] in {"big_spender", "last_place", "night", "same_item"}
    assert "Амир" in roast["text"]


def test_roast_none_for_unknown_member():
    assert obshak_flavor.roast(fresh_state(), CONFIG, "ghost", now=NOW) is None


def test_format_money_and_amount():
    assert obshak_flavor.format_money(1200) == "1\u00a0200"
    assert obshak_flavor.format_money(140.5) == "140.50"
    assert obshak.format_amount(300.0) == "300"
    assert obshak.format_amount(140.25) == "140.25"
