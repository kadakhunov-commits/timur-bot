"""Тесты погоды за окном общака (wttr.in с кэшем, без сети)."""

import pytest

from timur_bot.services import obshak_weather

PAYLOAD = {
    "current_condition": [
        {
            "weatherCode": "296",
            "temp_C": "7",
            "windspeedKmph": "11",
            "weatherDesc": [{"value": "Light rain"}],
        }
    ]
}


@pytest.fixture(autouse=True)
def clear_cache():
    obshak_weather.reset_cache()
    yield
    obshak_weather.reset_cache()


def test_classify_known_codes():
    assert obshak_weather.classify(113) == "clear"
    assert obshak_weather.classify("116") == "clouds"
    assert obshak_weather.classify(296) == "rain"
    assert obshak_weather.classify(332) == "snow"
    assert obshak_weather.classify(143) == "fog"
    assert obshak_weather.classify(389) == "thunder"
    assert obshak_weather.classify("не число") == "clouds"
    assert obshak_weather.classify(None) == "clouds"


def test_get_weather_shapes_payload():
    result = obshak_weather.get_weather("Moscow", fetch=lambda city, timeout: PAYLOAD)
    assert result == {
        "kind": "rain",
        "code": 296,
        "temp": 7,
        "wind": 11,
        "desc": "Light rain",
        "city": "Moscow",
    }


def test_get_weather_caches_result():
    calls = []

    def fetch(city, timeout):
        calls.append(city)
        return PAYLOAD

    first = obshak_weather.get_weather("Moscow", fetch=fetch, now=1000.0, ttl_seconds=1800)
    second = obshak_weather.get_weather("Moscow", fetch=fetch, now=1500.0, ttl_seconds=1800)
    assert first == second
    assert len(calls) == 1
    obshak_weather.get_weather("Moscow", fetch=fetch, now=3000.0, ttl_seconds=1800)
    assert len(calls) == 2


def test_get_weather_caches_failure():
    calls = []

    def failing(city, timeout):
        calls.append(city)
        return None

    assert obshak_weather.get_weather("Moscow", fetch=failing, now=1000.0) is None
    assert obshak_weather.get_weather("Moscow", fetch=failing, now=1100.0) is None
    assert len(calls) == 1


def test_get_weather_handles_malformed_payloads():
    for payload in ({}, {"current_condition": []}, {"current_condition": ["нет"]}, {"current_condition": [None]}):
        obshak_weather.reset_cache()
        assert obshak_weather.get_weather("Moscow", fetch=lambda city, timeout, p=payload: p, now=1.0) is None


def test_get_weather_tolerates_missing_fields():
    payload = {"current_condition": [{"weatherCode": "113"}]}
    result = obshak_weather.get_weather("Moscow", fetch=lambda city, timeout: payload, now=1.0)
    assert result["kind"] == "clear"
    assert result["temp"] is None
    assert result["wind"] is None
    assert result["desc"] == ""


def test_get_weather_falls_back_to_default_city_key():
    payload = {"current_condition": [{"weatherCode": "113", "temp_C": "0"}]}
    assert obshak_weather.get_weather("", fetch=lambda city, timeout: payload, now=1.0)["kind"] == "clear"
