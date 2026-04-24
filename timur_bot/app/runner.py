from telegram.ext import Application

from timur_bot.app.router import register_handlers
from timur_bot.services.bot_logic import TELEGRAM_BOT_TOKEN, logger


def main() -> None:
    application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
    register_handlers(application)
    logger.info("Запускаю Timur Bot...")
    application.run_polling()
