"""Погода за окном кухни: wttr.in без ключа, с кэшем в процессе.

Модуль намеренно никогда не бросает наружу сетевые ошибки: миниапп просто
покажет обычное небо, если погоду получить не удалось.
"""

from __future__ import annotations

import json
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Callable, Dict, Optional

WTTR_URL = "https://wttr.in/{city}?format=j1"
DEFAULT_TIMEOUT = 6.0

THUNDER_CODES = {200, 386, 389, 392, 395}
SNOW_CODES = {
    179, 182, 185, 227, 230, 317, 320, 323, 326, 329, 332, 335, 338, 350,
    362, 365, 368, 371, 374, 377,
}
RAIN_CODES = {176, 263, 266, 281, 284, 293, 296, 299, 302, 305, 308, 311, 314, 353, 356, 359}
FOG_CODES = {143, 248, 260}
CLEAR_CODES = {113}

_CACHE: Dict[str, Dict[str, Any]] = {}
_CACHE_LOCK = threading.Lock()


def classify(code: Any) -> str:
    """Код wttr.in → одно из состояний окна: clear/clouds/rain/snow/fog/thunder."""
    try:
        value = int(code)
    except (TypeError, ValueError):
        return "clouds"
    if value in THUNDER_CODES:
        return "thunder"
    if value in SNOW_CODES:
        return "snow"
    if value in RAIN_CODES:
        return "rain"
    if value in FOG_CODES:
        return "fog"
    if value in CLEAR_CODES:
        return "clear"
    return "clouds"


def _default_fetch(city: str, timeout: float) -> Optional[Dict[str, Any]]:
    url = WTTR_URL.format(city=urllib.parse.quote(city))
    request = urllib.request.Request(url, headers={"User-Agent": "timur-bot-obshak/1.0"})
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except (urllib.error.URLError, TimeoutError, OSError, ValueError):
        return None
    return payload if isinstance(payload, dict) else None


def _shape(payload: Dict[str, Any], city: str) -> Optional[Dict[str, Any]]:
    current = payload.get("current_condition")
    if not isinstance(current, list) or not current or not isinstance(current[0], dict):
        return None
    condition = current[0]
    description = ""
    raw_desc = condition.get("weatherDesc")
    if isinstance(raw_desc, list) and raw_desc and isinstance(raw_desc[0], dict):
        description = str(raw_desc[0].get("value") or "")

    def as_int(value: Any) -> Optional[int]:
        try:
            return int(round(float(value)))
        except (TypeError, ValueError):
            return None

    code = condition.get("weatherCode")
    return {
        "kind": classify(code),
        "code": as_int(code),
        "temp": as_int(condition.get("temp_C")),
        "wind": as_int(condition.get("windspeedKmph")),
        "desc": description,
        "city": city,
    }


def get_weather(
    city: str,
    *,
    ttl_seconds: float = 1800,
    now: Optional[float] = None,
    fetch: Optional[Callable[[str, float], Optional[Dict[str, Any]]]] = None,
    timeout: float = DEFAULT_TIMEOUT,
) -> Optional[Dict[str, Any]]:
    """Погода для города с кэшем. ``None`` — не удалось получить (это нормально)."""
    key = (city or "").strip().lower() or "moscow"
    stamp = time.time() if now is None else float(now)
    with _CACHE_LOCK:
        cached = _CACHE.get(key)
        if cached and (stamp - float(cached.get("at", 0.0))) < ttl_seconds:
            return cached.get("value")
    payload = (fetch or _default_fetch)(city, timeout)
    value = _shape(payload, city) if payload else None
    with _CACHE_LOCK:
        # Кэшируем и неудачу: иначе каждый запрос будет ждать таймаут.
        _CACHE[key] = {"at": stamp, "value": value}
    return value


def reset_cache() -> None:
    with _CACHE_LOCK:
        _CACHE.clear()
