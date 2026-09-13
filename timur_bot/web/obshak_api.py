"""REST API миниаппа «Общак» (Flask Blueprint).

Все данные закрыты проверкой Telegram ``initData``: миниапп шлёт её в заголовке
``X-Telegram-Init-Data``. Незалогиненный внутри общака пользователь получает
403 ``needs_link`` и фронт показывает выбор аватара.
"""

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Tuple

from flask import Blueprint, Response, jsonify, request

from timur_bot.core.config import load_app_config
from timur_bot.services import obshak, obshak_flavor, obshak_weather
from timur_bot.web import obshak_auth

INIT_DATA_HEADER = "X-Telegram-Init-Data"

obshak_api = Blueprint("obshak_api", __name__, url_prefix="/api/obshak")


@lru_cache(maxsize=1)
def _app_config():
    return load_app_config()


def _settings() -> Dict[str, Any]:
    return _app_config().obshak_defaults


def _path() -> Path:
    override = os.getenv("OBSHAK_PATH", "").strip()
    return Path(override) if override else _app_config().obshak_path


def reset_for_tests() -> None:
    """Сбрасывает кэш конфига — нужно только тестам."""
    _app_config.cache_clear()
    obshak_auth.reset_member_cache()


def _ok(payload: Any = None, status: int = 200) -> Response:
    body = {"ok": True}
    if payload is not None:
        body.update(payload if isinstance(payload, dict) else {"data": payload})
    response = jsonify(body)
    response.status_code = status
    return response


def _err(code: str, status: int = 400, **extra: Any) -> Response:
    body = {"ok": False, "error": code}
    body.update(extra)
    response = jsonify(body)
    response.status_code = status
    return response


def _read() -> Dict[str, Any]:
    return obshak.load_state(_path(), _settings())


def _mutate(action: Callable[[Dict[str, Any]], Any]) -> Any:
    with obshak.lock():
        state = obshak.load_state(_path(), _settings())
        result = action(state)
        obshak.save_state(_path(), state)
        return result


def _identity() -> Tuple[Optional[Dict[str, Any]], Optional[Response]]:
    token = obshak_auth.get_bot_token() or _app_config().telegram_bot_token
    try:
        verified = obshak_auth.verify_init_data(request.headers.get(INIT_DATA_HEADER, ""), token)
    except obshak_auth.AuthError as exc:
        return None, _err(exc.code, exc.status)
    return obshak_auth.user_identity(verified), None


def _member_of(state: Dict[str, Any], identity: Dict[str, Any]) -> Optional[str]:
    return obshak.resolve_member_id(state, identity["id"])


def _needs_link(state: Dict[str, Any]) -> Response:
    return _err(
        "needs_link",
        403,
        members=[
            {
                "key": entry.get("key"),
                "name": entry.get("name"),
                "color": entry.get("color"),
                "avatar": entry.get("avatar"),
                "linked": entry.get("telegram_id") is not None,
            }
            for entry in obshak.member_map(state).values()
        ],
    )


def _is_owner(identity: Dict[str, Any]) -> bool:
    try:
        return int(identity["id"]) in set(_app_config().owner_ids)
    except (KeyError, TypeError, ValueError):
        return False


def _body() -> Dict[str, Any]:
    data = request.get_json(silent=True)
    return data if isinstance(data, dict) else {}


# ---------------------------------------------------------------------------


@obshak_api.get("/bootstrap")
def bootstrap() -> Response:
    identity, error = _identity()
    if error:
        return error
    state = _read()
    member_id = _member_of(state, identity)
    if not member_id:
        return _needs_link(state)
    period = request.args.get("period", "all")
    config = _settings()
    payload = obshak.bootstrap(state, config, period=period, telegram_id=identity["id"])
    payload.update(
        {
            "jar": obshak_flavor.jar_state(state, config),
            "pet": obshak_flavor.pet_state(state, config),
            "race": obshak_flavor.race(state, config),
            "news": obshak_flavor.news(state, config),
            "quest": obshak_flavor.week_quest(state, config),
            "roast": obshak_flavor.roast(state, config, member_id),
            "is_owner": _is_owner(identity),
        }
    )
    return _ok(payload)


@obshak_api.get("/weather")
def weather() -> Response:
    identity, error = _identity()
    if error:
        return error
    state = _read()
    if not _member_of(state, identity):
        return _needs_link(state)
    settings = _settings().get("weather") or {}
    if not settings.get("enabled", True):
        return _ok({"weather": None})
    ttl_seconds = max(60, int(settings.get("cache_minutes", 30)) * 60)
    try:
        value = obshak_weather.get_weather(
            str(settings.get("city") or "Moscow"), ttl_seconds=ttl_seconds
        )
    except Exception:  # noqa: BLE001 — погода не должна ломать миниапп
        value = None
    return _ok({"weather": value})


@obshak_api.post("/roulette")
def roulette() -> Response:
    identity, error = _identity()
    if error:
        return error
    state = _read()
    if not _member_of(state, identity):
        return _needs_link(state)
    try:
        result = _mutate(lambda current: obshak.roulette_spin(current, _settings()))
    except obshak.ObshakError as exc:
        return _err(str(exc), 400)
    return _ok(result)


@obshak_api.post("/expenses")
def create_expense() -> Response:
    identity, error = _identity()
    if error:
        return error
    state = _read()
    member_id = _member_of(state, identity)
    if not member_id:
        return _needs_link(state)
    data = _body()
    target = str(data.get("member_id") or member_id)
    try:
        expense = _mutate(
            lambda current: obshak.add_expense(
                current,
                member_id=target,
                amount=data.get("amount"),
                title=data.get("title"),
                category=str(data.get("category") or "other"),
                added_by=identity["id"],
                source="miniapp",
                tz_name=str(_settings().get("timezone") or "Europe/Moscow"),
            )
        )
    except obshak.ObshakError as exc:
        return _err(str(exc), 400)
    return _ok({"expense": expense}, 201)


@obshak_api.patch("/expenses/<expense_id>")
def patch_expense(expense_id: str) -> Response:
    identity, error = _identity()
    if error:
        return error
    state = _read()
    member_id = _member_of(state, identity)
    if not member_id:
        return _needs_link(state)
    existing = obshak.find_expense(state, expense_id)
    if existing is None:
        return _err("unknown_expense", 404)
    if existing.get("member_id") != member_id and not _is_owner(identity):
        return _err("not_allowed", 403)
    data = _body()
    try:
        expense = _mutate(
            lambda current: obshak.update_expense(
                current,
                expense_id,
                amount=data.get("amount"),
                title=data.get("title"),
                category=data.get("category"),
            )
        )
    except obshak.ObshakError as exc:
        return _err(str(exc), 400)
    return _ok({"expense": expense})


@obshak_api.delete("/expenses/<expense_id>")
def remove_expense(expense_id: str) -> Response:
    identity, error = _identity()
    if error:
        return error
    state = _read()
    member_id = _member_of(state, identity)
    if not member_id:
        return _needs_link(state)
    existing = obshak.find_expense(state, expense_id)
    if existing is None:
        return _err("unknown_expense", 404)
    if existing.get("member_id") != member_id and not _is_owner(identity):
        return _err("not_allowed", 403)
    _mutate(lambda current: obshak.delete_expense(current, expense_id))
    return _ok({"deleted": expense_id})


@obshak_api.post("/expenses/<expense_id>/reaction")
def react_expense(expense_id: str) -> Response:
    identity, error = _identity()
    if error:
        return error
    state = _read()
    member_id = _member_of(state, identity)
    if not member_id:
        return _needs_link(state)
    reaction = str(_body().get("reaction") or "")
    try:
        expense = _mutate(lambda current: obshak.set_reaction(current, expense_id, member_id, reaction))
    except obshak.ObshakError as exc:
        return _err(str(exc), 400)
    return _ok({"expense": expense, "reactions": obshak.ALLOWED_REACTIONS})


@obshak_api.post("/me")
def link_me() -> Response:
    identity, error = _identity()
    if error:
        return error
    data = _body()
    target = str(data.get("member_id") or "")
    if not target:
        return _err("unknown_member", 400)
    state = _read()
    config = _settings()
    if obshak.member(state, target) is None:
        return _err("unknown_member", 400)
    chat_id = int(config.get("chat_id") or 0)
    if chat_id:
        already_here = obshak.resolve_member_id(state, identity["id"]) == target
        if not already_here:
            check = obshak_auth.is_chat_member(
                obshak_auth.get_bot_token() or _app_config().telegram_bot_token,
                chat_id,
                identity["id"],
            )
            if check is False:
                return _err("not_a_member", 403)
    with obshak.lock():
        state = obshak.load_state(_path(), config)
        success, code = obshak.link_member(
            state, target, identity["id"], username=identity.get("username", "")
        )
        if not success:
            return _err(code, 409 if code == "slot_taken" else 400)
        obshak.save_state(_path(), state)
    return _ok({"me": target})


@obshak_api.post("/me/release")
def release_me() -> Response:
    identity, error = _identity()
    if error:
        return error
    _mutate(lambda current: obshak.release_member(current, identity["id"]))
    return _ok({"released": True})


@obshak_api.get("/members/<member_id>")
def member_card(member_id: str) -> Response:
    identity, error = _identity()
    if error:
        return error
    state = _read()
    if not _member_of(state, identity):
        return _needs_link(state)
    try:
        payload = obshak_flavor.member_card(state, _settings(), member_id)
    except obshak.ObshakError as exc:
        return _err(str(exc), 404)
    return _ok(payload)


@obshak_api.get("/news")
def news() -> Response:
    identity, error = _identity()
    if error:
        return error
    state = _read()
    if not _member_of(state, identity):
        return _needs_link(state)
    return _ok({"headlines": obshak_flavor.news(state, _settings())})


@obshak_api.get("/calendar")
def calendar() -> Response:
    identity, error = _identity()
    if error:
        return error
    state = _read()
    if not _member_of(state, identity):
        return _needs_link(state)
    member_id = request.args.get("member") or None
    return _ok({"calendar": obshak.calendar(state, _settings(), member_id=member_id)})


@obshak_api.get("/calendar/<day>")
def calendar_day(day: str) -> Response:
    identity, error = _identity()
    if error:
        return error
    state = _read()
    if not _member_of(state, identity):
        return _needs_link(state)
    return _ok({"date": day, "expenses": obshak.calendar_day_expenses(state, _settings(), day)})


@obshak_api.post("/wishlist")
def wishlist_create() -> Response:
    identity, error = _identity()
    if error:
        return error
    state = _read()
    member_id = _member_of(state, identity)
    if not member_id:
        return _needs_link(state)
    data = _body()
    try:
        item = _mutate(
            lambda current: obshak.wishlist_add(
                current,
                title=data.get("title"),
                created_by=member_id,
                note=str(data.get("note") or ""),
            )
        )
    except obshak.ObshakError as exc:
        return _err(str(exc), 400)
    return _ok({"item": item}, 201)


@obshak_api.post("/wishlist/<item_id>/claim")
def wishlist_claim(item_id: str) -> Response:
    identity, error = _identity()
    if error:
        return error
    state = _read()
    member_id = _member_of(state, identity)
    if not member_id:
        return _needs_link(state)
    try:
        item = _mutate(lambda current: obshak.wishlist_claim(current, item_id, member_id))
    except obshak.ObshakError as exc:
        return _err(str(exc), 400)
    return _ok({"item": item})


@obshak_api.post("/wishlist/<item_id>/done")
def wishlist_done(item_id: str) -> Response:
    identity, error = _identity()
    if error:
        return error
    state = _read()
    member_id = _member_of(state, identity)
    if not member_id:
        return _needs_link(state)
    data = _body()
    try:
        item, expense = _mutate(
            lambda current: obshak.wishlist_complete(
                current,
                item_id,
                member_id=member_id,
                amount=data.get("amount"),
                category=str(data.get("category") or "other"),
                tz_name=str(_settings().get("timezone") or "Europe/Moscow"),
            )
        )
    except obshak.ObshakError as exc:
        return _err(str(exc), 400)
    return _ok({"item": item, "expense": expense})


@obshak_api.delete("/wishlist/<item_id>")
def wishlist_remove(item_id: str) -> Response:
    identity, error = _identity()
    if error:
        return error
    state = _read()
    member_id = _member_of(state, identity)
    if not member_id:
        return _needs_link(state)
    item = obshak.find_wishlist(state, item_id)
    if item is None:
        return _err("unknown_wishlist_item", 404)
    if item.get("created_by") not in {member_id, None} and not _is_owner(identity):
        return _err("not_allowed", 403)
    _mutate(lambda current: obshak.wishlist_delete(current, item_id))
    return _ok({"deleted": item_id})


@obshak_api.post("/requests")
def request_create() -> Response:
    identity, error = _identity()
    if error:
        return error
    state = _read()
    member_id = _member_of(state, identity)
    if not member_id:
        return _needs_link(state)
    data = _body()
    try:
        item = _mutate(
            lambda current: obshak.request_create(
                current,
                created_by=member_id,
                title=data.get("title"),
                amount=data.get("amount"),
                scope=str(data.get("scope") or "all"),
                per_person=bool(data.get("per_person", True)),
            )
        )
    except obshak.ObshakError as exc:
        return _err(str(exc), 400)
    return _ok({"request": item}, 201)


@obshak_api.post("/requests/<request_id>/pay")
def request_pay(request_id: str) -> Response:
    identity, error = _identity()
    if error:
        return error
    state = _read()
    member_id = _member_of(state, identity)
    if not member_id:
        return _needs_link(state)
    data = _body()
    raw_amount = data.get("amount")
    try:
        item, expense = _mutate(
            lambda current: obshak.request_pay(
                current,
                request_id,
                member_id=member_id,
                amount=raw_amount,
                tz_name=str(_settings().get("timezone") or "Europe/Moscow"),
            )
        )
    except obshak.ObshakError as exc:
        return _err(str(exc), 400)
    return _ok({"request": item, "expense": expense})


@obshak_api.post("/requests/<request_id>/close")
def request_close(request_id: str) -> Response:
    identity, error = _identity()
    if error:
        return error
    state = _read()
    member_id = _member_of(state, identity)
    if not member_id:
        return _needs_link(state)
    try:
        item = _mutate(lambda current: obshak.request_close(current, request_id, member_id))
    except obshak.ObshakError as exc:
        return _err(str(exc), 400)
    return _ok({"request": item})


@obshak_api.get("/export.csv")
def export_csv() -> Response:
    identity, error = _identity()
    if error:
        return error
    state = _read()
    if not _member_of(state, identity):
        return _needs_link(state)
    payload = obshak.export_csv(state, _settings())
    return Response(
        payload,
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment; filename=obshak.csv"},
    )
