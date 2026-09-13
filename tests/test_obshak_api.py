"""Тесты REST API миниаппа общака (Flask test client + реальная проверка initData)."""

import hashlib
import hmac
import json
import os
import time
from urllib.parse import urlencode

import pytest

pytest.importorskip("flask")

os.environ["TELEGRAM_BOT_TOKEN"] = "test-token"
os.environ.setdefault("OPENAI_API_KEY", "test-api-key")

from timur_bot.web import obshak_api  # noqa: E402
from timur_bot.web.admin_panel import app  # noqa: E402

TOKEN = "test-token"
AMIR = 111
KADYR = 222


def init_data(user_id, token=TOKEN, auth_date=None):
    fields = {
        "auth_date": str(int(time.time()) if auth_date is None else auth_date),
        "query_id": "AAA",
        "user": json.dumps({"id": user_id, "username": f"u{user_id}"}, separators=(",", ":")),
    }
    check_string = "\n".join(f"{key}={fields[key]}" for key in sorted(fields))
    secret = hmac.new(b"WebAppData", token.encode("utf-8"), hashlib.sha256).digest()
    fields["hash"] = hmac.new(secret, check_string.encode("utf-8"), hashlib.sha256).hexdigest()
    return urlencode(fields)


def headers(user_id, **kwargs):
    return {"X-Telegram-Init-Data": init_data(user_id, **kwargs)}


@pytest.fixture()
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("OBSHAK_PATH", str(tmp_path / "obshak.json"))
    obshak_api.reset_for_tests()
    with app.test_client() as test_client:
        yield test_client
    obshak_api.reset_for_tests()


def link(client, user_id, member_id):
    response = client.post("/api/obshak/me", json={"member_id": member_id}, headers=headers(user_id))
    assert response.status_code == 200, response.get_json()
    return response


# --- доступ ------------------------------------------------------------


def test_bootstrap_requires_init_data(client):
    assert client.get("/api/obshak/bootstrap").status_code == 401
    assert client.get("/api/obshak/bootstrap", headers={"X-Telegram-Init-Data": "broken"}).status_code == 401


def test_expired_init_data_is_rejected(client):
    old = int(time.time()) - 3 * 24 * 3600
    response = client.get("/api/obshak/bootstrap", headers=headers(AMIR, auth_date=old))
    assert response.status_code == 401
    assert response.get_json()["error"] == "expired"


def test_unlinked_user_gets_needs_link_with_members(client):
    response = client.get("/api/obshak/bootstrap", headers=headers(AMIR))
    assert response.status_code == 403
    payload = response.get_json()
    assert payload["error"] == "needs_link"
    assert [member["key"] for member in payload["members"]] == ["rustem", "kadyr", "dilyara", "amir"]


def test_link_member_and_reject_taken_slot(client):
    link(client, AMIR, "amir")
    response = client.post("/api/obshak/me", json={"member_id": "amir"}, headers=headers(KADYR))
    assert response.status_code == 409
    assert response.get_json()["error"] == "slot_taken"
    response = client.post("/api/obshak/me", json={"member_id": "kadyr"}, headers=headers(KADYR))
    assert response.status_code == 200


def test_link_unknown_member(client):
    response = client.post("/api/obshak/me", json={"member_id": "ghost"}, headers=headers(AMIR))
    assert response.status_code == 400
    assert response.get_json()["error"] == "unknown_member"


def test_release_own_slot(client):
    link(client, AMIR, "amir")
    assert client.post("/api/obshak/me/release", headers=headers(AMIR)).status_code == 200
    assert client.get("/api/obshak/bootstrap", headers=headers(AMIR)).status_code == 403


# --- расходы -----------------------------------------------------------


def test_bootstrap_payload_shape(client):
    link(client, AMIR, "amir")
    client.post(
        "/api/obshak/expenses",
        json={"amount": 140, "title": "Майонез", "category": "food"},
        headers=headers(AMIR),
    )
    payload = client.get("/api/obshak/bootstrap", headers=headers(AMIR)).get_json()
    assert payload["ok"] is True
    assert payload["me"] == "amir"
    assert payload["totals_all"]["total"] == 140
    assert payload["leaderboard"][0]["member_id"] == "amir"
    assert payload["jar"]["level"]["name"] == "Пустая банка"
    assert payload["pet"]["count"] == 1
    assert payload["race"]["lanes"]
    assert payload["news"]
    assert payload["is_owner"] is False
    assert "quick_titles" in payload and "poke_phrases" in payload


def test_expenses_validation(client):
    link(client, AMIR, "amir")
    bad_amount = client.post(
        "/api/obshak/expenses", json={"amount": 0, "title": "x"}, headers=headers(AMIR)
    )
    assert bad_amount.status_code == 400
    assert bad_amount.get_json()["error"] == "bad_amount"
    empty_title = client.post(
        "/api/obshak/expenses", json={"amount": 10, "title": "  "}, headers=headers(AMIR)
    )
    assert empty_title.get_json()["error"] == "empty_title"
    unknown = client.post(
        "/api/obshak/expenses",
        json={"amount": 10, "title": "x", "member_id": "ghost"},
        headers=headers(AMIR),
    )
    assert unknown.get_json()["error"] == "unknown_member"


def test_expense_on_behalf_of_another_member(client):
    link(client, AMIR, "amir")
    response = client.post(
        "/api/obshak/expenses",
        json={"amount": 500, "title": "Пылесос", "member_id": "kadyr"},
        headers=headers(AMIR),
    )
    assert response.status_code == 201
    assert response.get_json()["expense"]["member_id"] == "kadyr"


def test_edit_and_delete_only_own_expenses(client):
    link(client, AMIR, "amir")
    link(client, KADYR, "kadyr")
    created = client.post(
        "/api/obshak/expenses", json={"amount": 140, "title": "Майонез"}, headers=headers(AMIR)
    ).get_json()["expense"]
    assert client.patch(
        f"/api/obshak/expenses/{created['id']}", json={"amount": 150}, headers=headers(AMIR)
    ).status_code == 200
    assert client.patch(
        f"/api/obshak/expenses/{created['id']}", json={"amount": 999}, headers=headers(KADYR)
    ).status_code == 403
    assert client.delete(f"/api/obshak/expenses/{created['id']}", headers=headers(KADYR)).status_code == 403
    assert client.delete(f"/api/obshak/expenses/{created['id']}", headers=headers(AMIR)).status_code == 200
    assert client.delete(f"/api/obshak/expenses/{created['id']}", headers=headers(AMIR)).status_code == 404


def test_reactions(client):
    link(client, AMIR, "amir")
    link(client, KADYR, "kadyr")
    created = client.post(
        "/api/obshak/expenses", json={"amount": 140, "title": "Майонез"}, headers=headers(AMIR)
    ).get_json()["expense"]
    response = client.post(
        f"/api/obshak/expenses/{created['id']}/reaction",
        json={"reaction": "fire"},
        headers=headers(KADYR),
    )
    assert response.status_code == 200
    assert response.get_json()["expense"]["reactions"] == {"kadyr": "fire"}
    bad = client.post(
        f"/api/obshak/expenses/{created['id']}/reaction",
        json={"reaction": "poop"},
        headers=headers(KADYR),
    )
    assert bad.get_json()["error"] == "bad_reaction"


# --- списки и запросы --------------------------------------------------


def test_wishlist_endpoints(client):
    link(client, AMIR, "amir")
    item = client.post("/api/obshak/wishlist", json={"title": "бумага"}, headers=headers(AMIR)).get_json()["item"]
    assert client.post(f"/api/obshak/wishlist/{item['id']}/claim", headers=headers(AMIR)).status_code == 200
    done = client.post(
        f"/api/obshak/wishlist/{item['id']}/done", json={"amount": 200}, headers=headers(AMIR)
    )
    assert done.status_code == 200
    assert done.get_json()["item"]["status"] == "done"
    assert done.get_json()["expense"]["amount"] == 200
    again = client.post(
        f"/api/obshak/wishlist/{item['id']}/done", json={"amount": 10}, headers=headers(AMIR)
    )
    assert again.get_json()["error"] == "item_closed"


def test_requests_endpoints(client):
    link(client, AMIR, "amir")
    link(client, KADYR, "kadyr")
    created = client.post(
        "/api/obshak/requests",
        json={"title": "шампунь", "amount": 150, "scope": "all"},
        headers=headers(AMIR),
    ).get_json()["request"]
    not_target = client.post(f"/api/obshak/requests/{created['id']}/pay", headers=headers(AMIR))
    assert not_target.get_json()["error"] == "not_a_target"
    paid = client.post(f"/api/obshak/requests/{created['id']}/pay", headers=headers(KADYR))
    assert paid.status_code == 200
    assert paid.get_json()["request"]["payments"][0]["member_id"] == "kadyr"
    double = client.post(f"/api/obshak/requests/{created['id']}/pay", headers=headers(KADYR))
    assert double.get_json()["error"] == "already_paid"
    not_author = client.post(f"/api/obshak/requests/{created['id']}/close", headers=headers(KADYR))
    assert not_author.get_json()["error"] == "not_author"
    assert client.post(f"/api/obshak/requests/{created['id']}/close", headers=headers(AMIR)).status_code == 200


def test_export_csv(client):
    link(client, AMIR, "amir")
    client.post("/api/obshak/expenses", json={"amount": 140, "title": "Майонез"}, headers=headers(AMIR))
    response = client.get("/api/obshak/export.csv", headers=headers(AMIR))
    assert response.status_code == 200
    assert response.mimetype == "text/csv"
    body = response.data.decode("utf-8")
    assert body.splitlines()[0] == "id,date,member,title,category,amount,request_id"
    assert "Майонез" in body


# --- вспомогательные вьюхи --------------------------------------------


def test_member_card_and_calendar(client):
    link(client, AMIR, "amir")
    client.post("/api/obshak/expenses", json={"amount": 140, "title": "Майонез"}, headers=headers(AMIR))
    card = client.get("/api/obshak/members/amir", headers=headers(AMIR))
    assert card.status_code == 200
    assert card.get_json()["stats"]["count"] == 1
    assert client.get("/api/obshak/members/ghost", headers=headers(AMIR)).status_code == 404
    calendar = client.get("/api/obshak/calendar", headers=headers(AMIR)).get_json()["calendar"]
    assert calendar["peak"] == 140
    # Берём день с покупкой, а не последний в окне: окно заканчивается воскресеньем
    # текущей недели, то есть в начале недели смотрит в будущее.
    day = next(entry["date"] for entry in calendar["days"] if entry["count"] > 0)
    day_expenses = client.get(f"/api/obshak/calendar/{day}", headers=headers(AMIR)).get_json()
    assert day_expenses["expenses"][0]["title"] == "Майонез"
    assert client.get("/api/obshak/news", headers=headers(AMIR)).status_code == 200


# --- рулетка, стрик, квест и погода -----------------------------------


def test_bootstrap_has_streak_quest_roast_and_roulette(client):
    link(client, AMIR, "amir")
    payload = client.get("/api/obshak/bootstrap", headers=headers(AMIR)).get_json()
    assert payload["streak"]["current"] == 0
    assert len(payload["streak"]["days"]) == 14
    assert payload["quest"] is not None
    assert payload["roast"]["key"] == "zero"
    assert "Амир" in payload["roast"]["text"]
    assert payload["light_phrases"]
    assert payload["roulette"]["stats"]["total"] == 0


def test_roulette_endpoint_records_spin(client):
    link(client, AMIR, "amir")
    link(client, KADYR, "kadyr")
    result = client.post("/api/obshak/roulette", headers=headers(AMIR)).get_json()
    assert result["winner"] in {"rustem", "kadyr", "dilyara", "amir"}
    assert abs(sum(result["weights"].values()) - 1) < 0.02
    assert result["stats"]["total"] == 1
    assert result["stats"]["last"]["member_id"] == result["winner"]
    payload = client.get("/api/obshak/bootstrap", headers=headers(AMIR)).get_json()
    assert payload["roulette"]["stats"]["counts"][result["winner"]] == 1


def test_roulette_requires_link(client):
    assert client.post("/api/obshak/roulette", headers=headers(AMIR)).status_code == 403


def test_weather_endpoint_returns_weather(client, monkeypatch):
    link(client, AMIR, "amir")
    sample = {"kind": "snow", "temp": -4, "desc": "Light snow", "city": "Moscow", "code": 326, "wind": 9}
    monkeypatch.setattr(obshak_api.obshak_weather, "get_weather", lambda *args, **kwargs: sample)
    payload = client.get("/api/obshak/weather", headers=headers(AMIR)).get_json()
    assert payload["weather"] == sample


def test_weather_endpoint_survives_failure(client, monkeypatch):
    link(client, AMIR, "amir")

    def boom(*args, **kwargs):
        raise RuntimeError("сеть умерла")

    monkeypatch.setattr(obshak_api.obshak_weather, "get_weather", boom)
    response = client.get("/api/obshak/weather", headers=headers(AMIR))
    assert response.status_code == 200
    assert response.get_json()["weather"] is None


def test_weather_endpoint_can_be_disabled(client, monkeypatch):
    link(client, AMIR, "amir")
    monkeypatch.setattr(
        obshak_api, "_settings", lambda: {"weather": {"enabled": False, "city": "Moscow"}}
    )
    payload = client.get("/api/obshak/weather", headers=headers(AMIR)).get_json()
    assert payload["weather"] is None


def test_weather_requires_link(client):
    assert client.get("/api/obshak/weather", headers=headers(AMIR)).status_code == 403


# --- страницы ----------------------------------------------------------


def test_pages_are_served(client):
    miniapp = client.get("/miniapp")
    assert miniapp.status_code == 200
    assert "ОБЩАК" in miniapp.data.decode("utf-8")
    assert miniapp.headers["Cache-Control"] == "no-store, max-age=0"
    legacy = client.get("/admin-panel")
    assert legacy.status_code == 200
    assert "personaGrid" in legacy.data.decode("utf-8")
    assert client.get("/").status_code == 302
    assert client.get("/healthz").get_json()["status"] == "ok"


def test_static_assets_are_served(client):
    for name in ("app.css", "app.js", "avatars.js", "scene.js", "sfx.js", "share.js"):
        response = client.get(f"/miniapp/assets/obshak/{name}")
        assert response.status_code == 200, name
        assert response.headers["Cache-Control"] == "no-cache"
    assert client.get("/miniapp/assets/../obshak.html").status_code in (400, 404)
