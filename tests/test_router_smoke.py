import asyncio
import os
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

os.environ.setdefault("TELEGRAM_BOT_TOKEN", "test-token")
os.environ.setdefault("OPENAI_API_KEY", "test-api-key")

from telegram.ext import CommandHandler, MessageReactionHandler

from timur_bot.app import runner
from timur_bot.app.router import register_handlers


class DummyApp:
    def __init__(self) -> None:
        self.handlers = []

    def add_handler(self, handler, group: int = 0) -> None:
        del group
        self.handlers.append(handler)


def test_register_handlers_smoke() -> None:
    app = DummyApp()
    register_handlers(app)

    assert len(app.handlers) == 51
    assert any(isinstance(h, MessageReactionHandler) for h in app.handlers)

    commands = []
    for h in app.handlers:
        if isinstance(h, CommandHandler):
            commands.extend(sorted(h.commands))

    assert sorted(commands) == sorted(
        [
            "start",
            "story",
            "summary",
            "noire",
            "secure",
            "admin",
            "panel",
            "miniapp",
            "miniappdebug",
            "billhelp",
            "billquote",
            "billsetup",
            "billstatus",
            "billinvoices",
            "billpay",
            "billabuse",
            "billref",
            "subscribe",
            "setprompt",
            "appendprompt",
            "showprompt",
            "resetprompt",
            "setbio",
            "setstyle",
            "setheat",
            "mode",
            "setmode",
            "showmode",
            "bit",
            "bits",
            "funny",
            "unfunny",
            "mood",
            "moodevent",
            "moodset",
            "moodguard",
            "moodopen",
            "moodreset",
            "remember",
            "whois",
            "dump",
            "clearmemory",
        ]
    )


def test_post_init_does_not_eagerly_warm_secure_model() -> None:
    application = SimpleNamespace()
    with (
        patch.object(runner, "start_life_loop", new=AsyncMock()) as start_life,
        patch.object(runner, "start_funny_scan_loop", new=AsyncMock()) as start_funny,
        patch("asyncio.create_task") as create_task,
    ):
        asyncio.run(runner._post_init(application))

    start_life.assert_awaited_once_with(application)
    start_funny.assert_awaited_once_with(application)
    create_task.assert_not_called()
