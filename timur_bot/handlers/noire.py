import asyncio
import io
import logging
from concurrent.futures import ProcessPoolExecutor

from telegram import InputFile, Update
from telegram.constants import ChatAction
from telegram.ext import ContextTypes

from timur_bot.services.noire import convert_to_noire_png, resolve_noire_source_message

logger = logging.getLogger("timur-bot.noire")
_NOIRE_POOL: ProcessPoolExecutor | None = None


def _get_noire_pool() -> ProcessPoolExecutor:
    global _NOIRE_POOL
    if _NOIRE_POOL is None:
        _NOIRE_POOL = ProcessPoolExecutor(max_workers=1)
    return _NOIRE_POOL


async def _run_noire_task(
    context: ContextTypes.DEFAULT_TYPE,
    *,
    chat_id: int,
    file_id: str,
    reply_to_message_id: int,
) -> None:
    try:
        await context.bot.send_chat_action(chat_id=chat_id, action=ChatAction.UPLOAD_PHOTO)
        file = await context.bot.get_file(file_id)
        file_bytes = await file.download_as_bytearray()
        loop = asyncio.get_running_loop()
        noir_bytes = await loop.run_in_executor(_get_noire_pool(), convert_to_noire_png, bytes(file_bytes))
        await context.bot.send_photo(
            chat_id=chat_id,
            photo=InputFile(io.BytesIO(noir_bytes), filename="noire.png"),
            caption="Noire",
            reply_to_message_id=reply_to_message_id,
        )
    except Exception:
        logger.exception("Не удалось применить noir-фильтр")
        await context.bot.send_message(
            chat_id=chat_id,
            text="Не получилось сделать нуарный фильтр. Попробуй ещё раз с другой фоткой.",
            reply_to_message_id=reply_to_message_id,
        )


def _log_task_failure(task: asyncio.Task) -> None:
    try:
        task.result()
    except Exception:
        logger.exception("Фоновая задача /noire завершилась с ошибкой")


async def _acknowledge_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    if not message:
        return
    try:
        await context.bot.set_message_reaction(
            chat_id=message.chat_id,
            message_id=message.message_id,
            reaction="👌",
        )
    except Exception:
        logger.debug("Не удалось поставить реакцию на /noire", exc_info=True)


async def noire_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    source = resolve_noire_source_message(message)

    if not message or not source or not source.photo:
        if message:
            await message.reply_text("Используй /noire вместе с фото или ответь /noire на сообщение с фото.")
        return

    await _acknowledge_command(update, context)

    task_factory = getattr(context, "application", None)
    coro = _run_noire_task(
        context,
        chat_id=source.chat_id,
        file_id=source.photo[-1].file_id,
        reply_to_message_id=source.message_id,
    )
    task = task_factory.create_task(coro) if task_factory else asyncio.create_task(coro)
    task.add_done_callback(_log_task_failure)
