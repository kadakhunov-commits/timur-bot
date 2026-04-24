import os

os.environ.setdefault("TELEGRAM_BOT_TOKEN", "test-token")
os.environ.setdefault("OPENAI_API_KEY", "test-api-key")

from telegram.ext import CommandHandler

from timur_bot.app.router import register_handlers


class DummyApp:
    def __init__(self) -> None:
        self.handlers = []

    def add_handler(self, handler) -> None:
        self.handlers.append(handler)


def test_register_handlers_smoke() -> None:
    app = DummyApp()
    register_handlers(app)

    assert len(app.handlers) == 25

    commands = []
    for h in app.handlers:
        if isinstance(h, CommandHandler):
            commands.extend(sorted(h.commands))

    assert sorted(commands) == sorted(
        [
            "start",
            "admin",
            "panel",
            "billhelp",
            "billquote",
            "billsetup",
            "billstatus",
            "billinvoices",
            "billpay",
            "billabuse",
            "billref",
            "setprompt",
            "appendprompt",
            "showprompt",
            "resetprompt",
            "setbio",
            "setstyle",
            "setheat",
            "remember",
            "whois",
            "dump",
            "clearmemory",
        ]
    )
