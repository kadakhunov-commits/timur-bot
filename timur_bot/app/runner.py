import asyncio

from telegram import Update
from telegram.ext import Application

from timur_bot.app.router import register_handlers
from timur_bot.services.secure_face import warmup_secure_face_model
from timur_bot.services.bot_logic import (
    TELEGRAM_BOT_TOKEN,
    logger,
    start_funny_scan_loop,
    start_life_loop,
    stop_funny_scan_loop,
    stop_life_loop,
)


async def _post_init(application: Application) -> None:
    await start_life_loop(application)
    await start_funny_scan_loop(application)
    application.create_task(_warmup_secure_model())


async def _post_shutdown(application: Application) -> None:
    del application
    await stop_funny_scan_loop()
    await stop_life_loop()


async def _warmup_secure_model() -> None:
    try:
        status = await asyncio.to_thread(warmup_secure_face_model)
        logger.info("Secure model warmup: %s", status)
    except Exception:
        logger.exception("Secure model warmup failed")


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
