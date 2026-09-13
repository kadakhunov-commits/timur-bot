"""Тесты ботовой части общака: deep-link, карточка, кнопка меню."""

import asyncio
import os
from datetime import datetime, timezone

os.environ.setdefault("TELEGRAM_BOT_TOKEN", "test-token")
os.environ.setdefault("OPENAI_API_KEY", "test-api-key")

from timur_bot.services import bot_logic, obshak as obshak_service


def build_state():
    state = obshak_service.default_state(bot_logic.OBSHAK_DEFAULTS)
    obshak_service.add_expense(
        state,
        member_id="amir",
        amount=140,
        title="Майонез",
        category="food",
        created_at=datetime(2026, 9, 13, 12, 0, tzinfo=timezone.utc),
    )
    obshak_service.wishlist_add(state, title="бумага", created_by="kadyr")
    return state


def test_deep_link_points_to_main_mini_app():
    link = bot_logic.obshak_deep_link("timur_bot", "add")
    assert link == "https://t.me/timur_bot?startapp=add"


def test_summary_text_for_empty_state():
    state = obshak_service.default_state(bot_logic.OBSHAK_DEFAULTS)
    text = bot_logic._obshak_summary_text(state)
    assert "пуст" in text


def test_summary_text_lists_leader_and_jar():
    text = bot_logic._obshak_summary_text(build_state())
    assert "общак: 140" in text
    assert "Амир" in text
    assert "банка:" in text
    assert "надо купить: 1" in text


def test_summary_text_includes_cat_roast_for_member():
    state = build_state()
    obshak_service.link_member(state, "amir", 111)
    text = bot_logic._obshak_summary_text(state, "amir")
    assert "🐈" in text
    assert "Амир" in text


def test_summary_text_without_member_has_no_roast():
    text = bot_logic._obshak_summary_text(build_state())
    assert "🐈" not in text


def test_keyboard_has_two_entry_points():
    keyboard = bot_logic._obshak_keyboard("timur_bot")
    urls = [button.url for row in keyboard.inline_keyboard for button in row]
    assert urls == [
        "https://t.me/timur_bot?startapp=home",
        "https://t.me/timur_bot?startapp=add",
    ]


def test_links_text_marks_free_and_taken_slots():
    state = build_state()
    obshak_service.link_member(state, "amir", 111, username="amir")
    text = bot_logic._obshak_links_text(state)
    assert "amir (Амир) — id 111" in text
    assert "kadyr (Кадыр) — свободен" in text


class FakeBot:
    def __init__(self):
        self.menu_calls = []
        self.command_calls = []

    async def set_chat_menu_button(self, menu_button=None):
        self.menu_calls.append(menu_button)

    async def set_my_commands(self, commands):
        self.command_calls.append(commands)


class FakeApplication:
    def __init__(self):
        self.bot = FakeBot()


def test_menu_button_and_commands_are_configured_when_url_present(monkeypatch):
    monkeypatch.setattr(bot_logic, "MINIAPP_URL", "https://example.com/miniapp")
    application = FakeApplication()
    asyncio.run(bot_logic.setup_obshak_bot_ui(application))
    assert len(application.bot.menu_calls) == 1
    menu = application.bot.menu_calls[0]
    assert menu.web_app.url == "https://example.com/miniapp"
    assert menu.text == "Общак"
    commands = [command.command for command in application.bot.command_calls[0]]
    assert "obshak" in commands


def test_commands_are_registered_even_without_miniapp_url(monkeypatch):
    monkeypatch.setattr(bot_logic, "MINIAPP_URL", "")
    application = FakeApplication()
    asyncio.run(bot_logic.setup_obshak_bot_ui(application))
    assert application.bot.menu_calls == []
    commands = [command.command for command in application.bot.command_calls[0]]
    assert commands == ["obshak", "start"]
