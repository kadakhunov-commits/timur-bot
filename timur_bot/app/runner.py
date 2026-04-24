from telegram import Update
from telegram.ext import Application

from timur_bot.app.router import register_handlers
from timur_bot.services.bot_logic import TELEGRAM_BOT_TOKEN, logger, start_life_loop, stop_life_loop


async def _post_init(application: Application) -> None:
    await start_life_loop(application)


async def _post_shutdown(application: Application) -> None:
    del application
    await stop_life_loop()


def main() -> None:
    application = (
        Application.builder()
        .token(TELEGRAM_BOT_TOKEN)
        .post_init(_post_init)
        .post_shutdown(_post_shutdown)
        .build()
    )
    register_handlers(application)
    logger.info("Запускаю Timur Bot...")
    # Explicitly request all update types so reaction updates are always delivered.
    application.run_polling(allowed_updates=Update.ALL_TYPES)
