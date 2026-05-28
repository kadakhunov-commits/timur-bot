from __future__ import annotations

from types import SimpleNamespace

import pytest

from timur_bot.services.secure_face import (
    _scale_bbox_to_original,
    resolve_secure_source_message,
    warmup_secure_face_model,
)


def test_resolve_secure_source_message_prefers_inline_photo() -> None:
    message = SimpleNamespace(photo=[object()], reply_to_message=None)
    assert resolve_secure_source_message(message) is message


def test_resolve_secure_source_message_uses_reply_photo() -> None:
    reply = SimpleNamespace(photo=[object()])
    message = SimpleNamespace(photo=[], reply_to_message=reply)
    assert resolve_secure_source_message(message) is reply


def test_resolve_secure_source_message_returns_none() -> None:
    message = SimpleNamespace(photo=[], reply_to_message=None)
    assert resolve_secure_source_message(message) is None


def test_warmup_disabled_without_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SECURE_FACE_REF_DIR", raising=False)
    status = warmup_secure_face_model()
    assert status.startswith("disabled:")


def test_scale_bbox_to_original() -> None:
    scaled = _scale_bbox_to_original((20, 10, 50, 40), 0.5, 400, 300)
    assert scaled == (40, 20, 100, 80)

