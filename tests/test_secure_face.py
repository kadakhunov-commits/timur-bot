from __future__ import annotations

from types import SimpleNamespace

import pytest

from timur_bot.services.secure_face import (
    _SecureFaceEngine,
    _expand_bbox_without_overlap,
    _looks_like_face_box,
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


def test_looks_like_face_box_filters_extreme_ratio() -> None:
    assert not _looks_like_face_box((10, 10, 220, 40), (800, 600))
    assert _looks_like_face_box((10, 20, 80, 90), (800, 600))


def test_select_final_matches_prefers_single_best() -> None:
    engine = _SecureFaceEngine.__new__(_SecureFaceEngine)
    engine.settings = SimpleNamespace(max_matches=1, second_best_margin=9.0)
    selected = _SecureFaceEngine._select_final_matches(
        engine,
        [
            (52.0, (1, 1, 10, 10)),
            (73.0, (2, 2, 10, 10)),
        ],
        detected_faces=2,
    )
    assert selected == [(1, 1, 10, 10)]


def test_select_final_matches_rejects_ambiguous() -> None:
    engine = _SecureFaceEngine.__new__(_SecureFaceEngine)
    engine.settings = SimpleNamespace(max_matches=1, second_best_margin=9.0)
    selected = _SecureFaceEngine._select_final_matches(
        engine,
        [
            (64.0, (1, 1, 10, 10)),
            (68.0, (2, 2, 10, 10)),
        ],
        detected_faces=2,
    )
    assert selected == []


def test_expand_bbox_without_overlap_respects_neighbor() -> None:
    # target at x=100..160, neighbor starts at x=170; expansion should stop before 170
    expanded = _expand_bbox_without_overlap(
        (100, 80, 60, 70),
        all_boxes=[(100, 80, 60, 70), (170, 84, 62, 68)],
        image_shape=(480, 640),
        expand_side=0.6,
        expand_top=0.4,
        expand_bottom=0.2,
    )
    x, _, w, _ = expanded
    assert x <= 100
    assert (x + w) <= 169
