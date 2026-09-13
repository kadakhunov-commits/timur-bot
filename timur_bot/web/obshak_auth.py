"""Проверка Telegram ``initData`` для миниаппа общака.

Правила взяты из документации Telegram:
data-check-string — все полученные поля, кроме ``hash``, отсортированные по
имени, склеенные через перевод строки в формате ``key=value``; секретный ключ —
HMAC-SHA256 от токена бота с константой ``WebAppData`` в роли ключа.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import threading
import time
import urllib.error
import urllib.request
from typing import Any, Dict, Optional, Tuple
from urllib.parse import parse_qsl

INIT_DATA_MAX_AGE_SECONDS = 24 * 3600
MEMBER_CACHE_TTL_SECONDS = 300


class AuthError(Exception):
    """Ошибка авторизации миниаппа."""

    def __init__(self, code: str, status: int = 401, message: str = "") -> None:
        super().__init__(message or code)
        self.code = code
        self.status = status
        self.message = message or code


def get_bot_token() -> str:
    return os.getenv("TELEGRAM_BOT_TOKEN", "").strip()


def verify_init_data(
    init_data: str,
    bot_token: str,
    *,
    max_age_seconds: int = INIT_DATA_MAX_AGE_SECONDS,
    now: Optional[int] = None,
) -> Dict[str, Any]:
    """Проверяет подпись initData и возвращает разобранные поля.

    Бросает :class:`AuthError` с кодом ``missing_init_data``, ``bad_hash``,
    ``expired`` или ``bad_user``.
    """
    raw = (init_data or "").strip()
    if not raw:
        raise AuthError("missing_init_data")
    if not bot_token:
        raise AuthError("server_misconfigured", status=500)

    pairs = dict(parse_qsl(raw, keep_blank_values=True))
    received_hash = pairs.pop("hash", "")
    if not received_hash:
        raise AuthError("missing_init_data")

    data_check_string = "\n".join(f"{key}={pairs[key]}" for key in sorted(pairs))
    secret_key = hmac.new(b"WebAppData", bot_token.encode("utf-8"), hashlib.sha256).digest()
    computed = hmac.new(secret_key, data_check_string.encode("utf-8"), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(computed, received_hash):
        raise AuthError("bad_hash")

    timestamp = now if now is not None else int(time.time())
    try:
        auth_date = int(pairs.get("auth_date", "0"))
    except ValueError as exc:
        raise AuthError("expired") from exc
    if max_age_seconds > 0 and (timestamp - auth_date) > max_age_seconds:
        raise AuthError("expired")

    user: Dict[str, Any] = {}
    if pairs.get("user"):
        try:
            parsed = json.loads(pairs["user"])
            if isinstance(parsed, dict):
                user = parsed
        except json.JSONDecodeError:
            user = {}
    if not user or "id" not in user:
        raise AuthError("bad_user")

    return {
        "auth_date": auth_date,
        "query_id": pairs.get("query_id", ""),
        "start_param": pairs.get("start_param", ""),
        "user": user,
        "raw": pairs,
    }


def user_identity(init_data: Dict[str, Any]) -> Dict[str, Any]:
    """Достаёт из проверенных данных минимальный профиль."""
    user = init_data.get("user") or {}
    name_parts = [user.get("first_name") or "", user.get("last_name") or ""]
    return {
        "id": int(user.get("id")),
        "username": str(user.get("username") or ""),
        "first_name": str(user.get("first_name") or ""),
        "full_name": " ".join(part for part in name_parts if part).strip(),
        "language_code": str(user.get("language_code") or ""),
    }


_MEMBER_CACHE: Dict[Tuple[int, int], Tuple[float, Optional[bool]]] = {}
_MEMBER_CACHE_LOCK = threading.Lock()


def _api_call(bot_token: str, method: str, payload: Dict[str, Any], timeout: float = 6.0) -> Optional[Dict[str, Any]]:
    request = urllib.request.Request(
        f"https://api.telegram.org/bot{bot_token}/{method}",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            body = json.loads(response.read().decode("utf-8"))
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError, OSError, ValueError):
        return None
    if not isinstance(body, dict) or not body.get("ok"):
        return None
    return body


def is_chat_member(
    bot_token: str,
    chat_id: int,
    user_id: int,
    *,
    timeout: float = 6.0,
    use_cache: bool = True,
) -> Optional[bool]:
    """Состоит ли пользователь в беседе. ``None`` — проверить не удалось."""
    if not bot_token or not chat_id or not user_id:
        return None
    key = (int(chat_id), int(user_id))
    if use_cache:
        with _MEMBER_CACHE_LOCK:
            cached = _MEMBER_CACHE.get(key)
            if cached and (time.time() - cached[0]) < MEMBER_CACHE_TTL_SECONDS:
                return cached[1]
    body = _api_call(bot_token, "getChatMember", {"chat_id": chat_id, "user_id": user_id}, timeout=timeout)
    if body is None:
        result: Optional[bool] = None
    else:
        status = str((body.get("result") or {}).get("status") or "")
        result = status in {"creator", "administrator", "member", "restricted"}
    if use_cache:
        with _MEMBER_CACHE_LOCK:
            _MEMBER_CACHE[key] = (time.time(), result)
    return result


def reset_member_cache() -> None:
    with _MEMBER_CACHE_LOCK:
        _MEMBER_CACHE.clear()
