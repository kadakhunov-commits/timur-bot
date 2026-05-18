import os
from pathlib import Path

import pytest

from timur_bot.core.config import ConfigError, load_app_config


def _write(path: Path, body: str) -> None:
    path.write_text(body, encoding="utf-8")


def test_load_config_with_fallbacks(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    (tmp_path / "config").mkdir(parents=True, exist_ok=True)
    _write(
        tmp_path / "config" / "persona.yaml",
        """
default_system_prompt: "x"
modes:
  default: "default mode"
defaults:
  active_mode: "missing_mode"
""".strip(),
    )
    _write(tmp_path / "config" / "lexicon.yaml", "archetype_lexicon: {}\n")
    _write(tmp_path / "config" / "runtime.yaml", "models: {}\nlimits: {}\nprobabilities: {}\n")

    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "t")
    monkeypatch.setenv("OPENAI_API_KEY", "k")
    monkeypatch.delenv("OPENAI_BASE_URL", raising=False)
    monkeypatch.delenv("OPENAI_TEXT_MODEL", raising=False)

    cfg = load_app_config(tmp_path)
    assert cfg.default_active_mode == "default"
    assert cfg.text_model == "gpt-4o-mini"
    assert cfg.openai_base_url == ""
    assert cfg.owner_ids
    assert cfg.owner_id in cfg.owner_ids
    assert cfg.funny_scan_defaults["review_threshold"] == 70
    assert cfg.funny_scan_defaults["owner_delivery_mode"] == "auto_forward"
    assert cfg.funny_scan_defaults["rule_min_hearts"] == 3
    assert "laugh_markers" in cfg.funny_scan_lexicon
    assert "extra_laugh_markers" in cfg.funny_scan_lexicon


def test_load_config_fail_fast_on_broken_yaml(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    (tmp_path / "config").mkdir(parents=True, exist_ok=True)
    _write(tmp_path / "config" / "persona.yaml", "default_system_prompt: [bad")
    _write(tmp_path / "config" / "lexicon.yaml", "archetype_lexicon: {}\n")
    _write(tmp_path / "config" / "runtime.yaml", "models: {}\nlimits: {}\nprobabilities: {}\n")

    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "t")
    monkeypatch.setenv("OPENAI_API_KEY", "k")

    with pytest.raises(ConfigError):
        load_app_config(tmp_path)


def test_load_config_parses_owner_ids(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    (tmp_path / "config").mkdir(parents=True, exist_ok=True)
    _write(
        tmp_path / "config" / "persona.yaml",
        """
default_system_prompt: "x"
modes:
  default: "default mode"
defaults:
  active_mode: "default"
""".strip(),
    )
    _write(tmp_path / "config" / "lexicon.yaml", "archetype_lexicon: {}\n")
    _write(
        tmp_path / "config" / "runtime.yaml",
        """
owner_id: 111
owner_ids:
  - 222
  - "333"
models: {}
limits: {}
probabilities: {}
""".strip(),
    )

    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "t")
    monkeypatch.setenv("OPENAI_API_KEY", "k")

    cfg = load_app_config(tmp_path)
    assert cfg.owner_id == 111
    assert cfg.owner_ids == [111, 222, 333]
