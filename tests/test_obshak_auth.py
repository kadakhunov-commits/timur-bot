"""Тесты проверки Telegram initData для миниаппа общака."""

import hashlib
import hmac
import json
import time
from urllib.parse import urlencode

import pytest

from timur_bot.web import obshak_auth

TOKEN = "123456:test-token"


def build_init_data(token=TOKEN, user_id=111, auth_date=None, extra=None, drop_hash=False):
    fields = {
        "auth_date": str(int(time.time()) if auth_date is None else auth_date),
        "query_id": "AAA",
        "user": json.dumps(
            {"id": user_id, "first_name": "Амир", "last_name": "И.", "username": "amir"},
            ensure_ascii=False,
            separators=(",", ":"),
        ),
    }
    if extra:
        fields.update(extra)
    check_string = "\n".join(f"{key}={fields[key]}" for key in sorted(fields))
    secret = hmac.new(b"WebAppData", token.encode("utf-8"), hashlib.sha256).digest()
    fields["hash"] = hmac.new(secret, check_string.encode("utf-8"), hashlib.sha256).hexdigest()
    if drop_hash:
        fields.pop("hash")
    return urlencode(fields)


def test_valid_init_data_is_accepted():
    verified = obshak_auth.verify_init_data(build_init_data(), TOKEN)
    assert verified["user"]["id"] == 111
    identity = obshak_auth.user_identity(verified)
    assert identity["id"] == 111
    assert identity["full_name"] == "Амир И."
    assert identity["username"] == "amir"


def test_start_param_and_signature_field_do_not_break_validation():
    init_data = build_init_data(extra={"start_param": "add", "signature": "abc123"})
    verified = obshak_auth.verify_init_data(init_data, TOKEN)
    assert verified["start_param"] == "add"


def test_tampered_hash_is_rejected():
    init_data = build_init_data() + "0"
    with pytest.raises(obshak_auth.AuthError) as info:
        obshak_auth.verify_init_data(init_data, TOKEN)
    assert info.value.code == "bad_hash"
    assert info.value.status == 401


def test_other_token_hash_is_rejected():
    with pytest.raises(obshak_auth.AuthError) as info:
        obshak_auth.verify_init_data(build_init_data(token="999:other"), TOKEN)
    assert info.value.code == "bad_hash"


def test_expired_auth_date_is_rejected():
    old = int(time.time()) - 3 * 24 * 3600
    with pytest.raises(obshak_auth.AuthError) as info:
        obshak_auth.verify_init_data(build_init_data(auth_date=old), TOKEN)
    assert info.value.code == "expired"


def test_missing_init_data_is_rejected():
    for value in ("", "   ", None):
        with pytest.raises(obshak_auth.AuthError) as info:
            obshak_auth.verify_init_data(value, TOKEN)
        assert info.value.code == "missing_init_data"


def test_missing_hash_is_rejected():
    with pytest.raises(obshak_auth.AuthError) as info:
        obshak_auth.verify_init_data(build_init_data(drop_hash=True), TOKEN)
    assert info.value.code == "missing_init_data"


def test_broken_user_payload_is_rejected():
    init_data = build_init_data(extra={"user": "not-json"})
    with pytest.raises(obshak_auth.AuthError) as info:
        obshak_auth.verify_init_data(init_data, TOKEN)
    assert info.value.code == "bad_user"
    init_data = build_init_data(extra={"user": json.dumps({"first_name": "нет id"})})
    with pytest.raises(obshak_auth.AuthError) as info:
        obshak_auth.verify_init_data(init_data, TOKEN)
    assert info.value.code == "bad_user"


def test_missing_bot_token_is_server_error():
    with pytest.raises(obshak_auth.AuthError) as info:
        obshak_auth.verify_init_data(build_init_data(), "")
    assert info.value.code == "server_misconfigured"
    assert info.value.status == 500


def test_chat_member_check_returns_none_without_credentials():
    assert obshak_auth.is_chat_member("", 0, 111) is None
    obshak_auth.reset_member_cache()
