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

    cfg = load_app_config(tmp_path)
    assert cfg.default_active_mode == "default"
    assert cfg.text_model == "gpt-4o-mini"
    assert cfg.openai_base_url == ""
    assert cfg.funny_scan_defaults["review_threshold"] == 70
    assert "laugh_markers" in cfg.funny_scan_lexicon


def test_load_config_fail_fast_on_broken_yaml(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    (tmp_path / "config").mkdir(parents=True, exist_ok=True)
    _write(tmp_path / "config" / "persona.yaml", "default_system_prompt: [bad")
    _write(tmp_path / "config" / "lexicon.yaml", "archetype_lexicon: {}\n")
    _write(tmp_path / "config" / "runtime.yaml", "models: {}\nlimits: {}\nprobabilities: {}\n")

    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "t")
    monkeypatch.setenv("OPENAI_API_KEY", "k")

    with pytest.raises(ConfigError):
        load_app_config(tmp_path)
