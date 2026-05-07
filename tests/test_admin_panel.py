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


def test_launch_redirect_preserves_state() -> None:
    client = app.test_client()
    response = client.get("/miniapp/launch?state=test-state")

    assert response.status_code == 302
    assert response.headers["Location"].endswith("/miniapp?state=test-state")
