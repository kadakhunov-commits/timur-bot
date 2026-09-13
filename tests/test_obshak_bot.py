"""Тесты ботовой части общака: deep-link, карточка, кнопка меню."""

import asyncio
import os
from datetime import datetime, timezone

os.environ.setdefault("TELEGRAM_BOT_TOKEN", "test-token")
os.environ.setdefault("OPENAI_API_KEY", "test-api-key")

from telegram.error import TelegramError

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


class FakeCardMessage:
    def __init__(self, chat_id, text, reply_markup, fail_pin=False):
        self.chat_id = chat_id
        self.text = text
        self.reply_markup = reply_markup
        self.pinned = False
        self._fail_pin = fail_pin

    async def pin(self, disable_notification=False):
        if self._fail_pin:
            raise TelegramError("not enough rights")
        self.pinned = True


class FakeCardBot:
    def __init__(self, fail_pin=False):
        self.username = "timur_aibot"
        self.id = 42
        self.sent = []
        self._fail_pin = fail_pin

    async def send_message(self, chat_id, text, **kwargs):
        message = FakeCardMessage(chat_id, text, kwargs.get("reply_markup"), fail_pin=self._fail_pin)
        self.sent.append(message)
        return message


class FakeCardContext:
    def __init__(self, fail_pin=False):
        self.bot = FakeCardBot(fail_pin=fail_pin)


def _prepare_card_env(monkeypatch, tmp_path, *, pin=True):
    config = dict(bot_logic.OBSHAK_DEFAULTS)
    config["pin_group_card"] = pin
    monkeypatch.setattr(bot_logic, "OBSHAK_DEFAULTS", config)
    monkeypatch.setattr(bot_logic, "OBSHAK_PATH", tmp_path / "obshak.json")
    monkeypatch.setattr(bot_logic, "MINIAPP_URL", "https://example.com/miniapp")
    monkeypatch.setattr(bot_logic, "_OBSHAK_PIN_HINTED", set())
    return config


GROUP_ID = -1001234567890


def test_pin_decision_only_for_groups_and_only_once(monkeypatch, tmp_path):
    _prepare_card_env(monkeypatch, tmp_path)
    state = obshak_service.default_state(bot_logic.OBSHAK_DEFAULTS)
    assert bot_logic._obshak_pin_decision(state, GROUP_ID) is True
    assert bot_logic._obshak_pin_decision(state, 444) is False
    state["meta"]["pinned_cards"] = {str(GROUP_ID): "2026-01-01T00:00:00+00:00"}
    assert bot_logic._obshak_pin_decision(state, GROUP_ID) is False
    assert bot_logic._obshak_pin_decision(state, GROUP_ID, force=True) is True
    assert bot_logic._obshak_pin_decision(state, 444, force=True) is False


def test_pin_decision_respects_config_flag(monkeypatch, tmp_path):
    _prepare_card_env(monkeypatch, tmp_path, pin=False)
    state = obshak_service.default_state(bot_logic.OBSHAK_DEFAULTS)
    assert bot_logic._obshak_pin_decision(state, GROUP_ID) is False


def test_card_is_pinned_once_and_force_repins(monkeypatch, tmp_path):
    _prepare_card_env(monkeypatch, tmp_path)
    context = FakeCardContext()

    asyncio.run(bot_logic._send_obshak_card(context, GROUP_ID))
    assert context.bot.sent[0].pinned is True
    assert "открыть общак" in str(context.bot.sent[0].reply_markup.to_dict())

    asyncio.run(bot_logic._send_obshak_card(context, GROUP_ID))
    assert context.bot.sent[1].pinned is False, "повторный /obshak не должен тасовать закреп"

    asyncio.run(bot_logic._send_obshak_card(context, GROUP_ID, force_pin=True))
    assert context.bot.sent[2].pinned is True

    saved = obshak_service.load_state(bot_logic.OBSHAK_PATH, bot_logic.OBSHAK_DEFAULTS)
    assert str(GROUP_ID) in saved["meta"]["pinned_cards"]


def test_card_is_not_pinned_in_private_chat(monkeypatch, tmp_path):
    _prepare_card_env(monkeypatch, tmp_path)
    context = FakeCardContext()
    asyncio.run(bot_logic._send_obshak_card(context, 444))
    assert context.bot.sent[0].pinned is False


def test_pin_failure_explains_what_to_do(monkeypatch, tmp_path):
    _prepare_card_env(monkeypatch, tmp_path)
    context = FakeCardContext(fail_pin=True)

    asyncio.run(bot_logic._send_obshak_card(context, GROUP_ID))
    assert context.bot.sent[0].pinned is False
    hint = context.bot.sent[1].text
    assert "Закреплять сообщения" in hint

    # Подсказка не должна повторяться на каждый /obshak.
    asyncio.run(bot_logic._send_obshak_card(context, GROUP_ID))
    assert len(context.bot.sent) == 3

    saved = obshak_service.load_state(bot_logic.OBSHAK_PATH, bot_logic.OBSHAK_DEFAULTS)
    assert (saved.get("meta") or {}).get("pinned_cards") in (None, {})


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
