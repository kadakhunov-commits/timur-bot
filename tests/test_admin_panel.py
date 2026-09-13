import pytest

pytest.importorskip("flask")

from timur_bot.web.admin_panel import app
from timur_bot.web.runtime_meta import get_runtime_meta


def test_healthz_includes_runtime_version(monkeypatch) -> None:
    get_runtime_meta.cache_clear()
    monkeypatch.setenv("TIMUR_VERSION", "abc1234")

    client = app.test_client()
    response = client.get("/healthz")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["status"] == "ok"
    assert payload["version"] == "abc1234"


def test_miniapp_injects_client_meta_script(monkeypatch) -> None:
    get_runtime_meta.cache_clear()
    monkeypatch.setenv("TIMUR_VERSION", "deadbee")
    monkeypatch.setenv("TIMUR_MINIAPP_BUILD", "42")
    monkeypatch.setenv("AMVERA_ENV", "preview")

    client = app.test_client()
    response = client.get("/miniapp")

    assert response.status_code == 200
    html = response.get_data(as_text=True)
    assert "window.__TIMUR_MINIAPP_META__" in html
    assert '"version": "deadbee"' in html
    assert '"buildLabel": "deadbee+42"' in html
    assert '"environment": "preview"' in html


def test_legacy_admin_panel_lives_on_admin_panel_path() -> None:
    client = app.test_client()
    response = client.get("/admin-panel")

    assert response.status_code == 200
    assert "window.__TIMUR_MINIAPP_META__" in response.get_data(as_text=True)
    assert "personaGrid" in response.get_data(as_text=True)


def test_legacy_launch_redirect_preserves_state() -> None:
    client = app.test_client()
    response = client.get("/admin-panel/launch?state=test-state")

    assert response.status_code == 302
    assert response.headers["Location"].endswith("/admin-panel?state=test-state")


def test_miniapp_launch_serves_the_obshak_app_without_redirect() -> None:
    # Старые кнопки «открыть миниапп» били в /miniapp/launch и открывали
    # legacy-панель. Теперь страница отдаётся сразу — закэшированный 302
    # больше не уводит пользователя в старое приложение.
    client = app.test_client()
    response = client.get("/miniapp/launch?state=test-state")

    assert response.status_code == 200
    assert "СОСЕДИ" in response.get_data(as_text=True)
    assert response.headers["Cache-Control"] == "no-store, max-age=0"


def test_old_admin_web_address_serves_the_obshak_app() -> None:
    # На /admin-web смотрел Main Mini App бота, поэтому там теперь кухня,
    # а не прежняя админка: deep-link из карточки не должен открывать старое.
    client = app.test_client()
    for path in ("/admin-web", "/admin-web/launch"):
        response = client.get(path)
        assert response.status_code == 200, path
        body = response.get_data(as_text=True)
        assert "СОСЕДИ" in body, path
        assert "ТЕЛЕГРАМ ADMIN PANEL" not in body, path
