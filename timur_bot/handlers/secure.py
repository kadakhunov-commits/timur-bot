from __future__ import annotations

import asyncio
import html
import io
import logging

from telegram import InputFile, Update
from telegram.constants import ChatAction
from telegram.ext import ContextTypes

from timur_bot.services.secure_face import process_secure_photo, resolve_secure_source_message

logger = logging.getLogger("timur-bot.secure")
_SECURE_SEMAPHORE = asyncio.Semaphore(1)


def _build_warning_text(source_message) -> tuple[str, str | None]:
    sender = getattr(source_message, "from_user", None)
    if not sender:
        return "Осторожнее!", None
    if sender.username:
        return f"@{sender.username}, Осторожнее!", None
    if sender.id:
        name = html.escape(sender.full_name or "пользователь")
        return f'<a href="tg://user?id={sender.id}">{name}</a>, Осторожнее!', "HTML"
    return "Осторожнее!", None


async def _run_secure_task(
    context: ContextTypes.DEFAULT_TYPE,
    *,
    source_message,
    trigger_message_id: int,
) -> None:
    chat_id = source_message.chat_id
    try:
        async with _SECURE_SEMAPHORE:
            await context.bot.send_chat_action(chat_id=chat_id, action=ChatAction.UPLOAD_PHOTO)
            file = await context.bot.get_file(source_message.photo[-1].file_id)
            file_bytes = await file.download_as_bytearray()
            result = await asyncio.to_thread(process_secure_photo, bytes(file_bytes))

            if result.matched_faces <= 0:
                if result.detected_faces <= 0:
                    text = "Лицо на фото не обнаружено. Попробуй кадр, где лицо крупнее и без сильного наклона."
                else:
                    best = f"{result.best_distance:.1f}" if result.best_distance is not None else "n/a"
                    text = (
                        "Лицо найдено, но не прошло порог совпадения. "
                        f"(best_distance={best})"
                    )
                logger.info(
                    "/secure no match: detected_faces=%s best_distance=%s",
                    result.detected_faces,
                    f"{result.best_distance:.2f}" if result.best_distance is not None else "n/a",
                )
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=text,
                    reply_to_message_id=trigger_message_id,
                )
                return

            caption, parse_mode = _build_warning_text(source_message)
            logger.info(
                "/secure success: matched_faces=%s used_emoji=%s detected_faces=%s best_distance=%s",
                result.matched_faces,
                result.used_emoji,
                result.detected_faces,
                f"{result.best_distance:.2f}" if result.best_distance is not None else "n/a",
            )
            await context.bot.send_photo(
                chat_id=chat_id,
                photo=InputFile(io.BytesIO(result.image_bytes), filename="secure.png"),
                caption=caption,
                parse_mode=parse_mode,
                reply_to_message_id=source_message.message_id,
            )
    except Exception:
        logger.exception("Не удалось обработать /secure")
        await context.bot.send_message(
            chat_id=chat_id,
            text="Не получилось обработать /secure. Проверь референсы и попробуй еще раз.",
            reply_to_message_id=trigger_message_id,
        )


def _log_task_failure(task: asyncio.Task) -> None:
    try:
        task.result()
    except Exception:
        logger.exception("Фоновая задача /secure завершилась с ошибкой")


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
        logger.debug("Не удалось поставить реакцию на /secure", exc_info=True)


async def secure_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    source = resolve_secure_source_message(message)

    if not message or not source or not source.photo:
        if message:
            await message.reply_text("Используй /secure вместе с фото или ответь /secure на сообщение с фото.")
        return

    await _acknowledge_command(update, context)
    logger.info(
        "/secure accepted: chat_id=%s trigger_message_id=%s source_message_id=%s source_user_id=%s",
        source.chat_id,
        message.message_id,
        source.message_id,
        source.from_user.id if source.from_user else None,
    )
    task_factory = getattr(context, "application", None)
    coro = _run_secure_task(
        context,
        source_message=source,
        trigger_message_id=message.message_id,
    )
    task = task_factory.create_task(coro) if task_factory else asyncio.create_task(coro)
    task.add_done_callback(_log_task_failure)
