from __future__ import annotations

import asyncio
import html
import io
import json
import logging
from pathlib import Path

from telegram import InputFile, Update
from telegram.constants import ChatAction
from telegram.ext import ContextTypes

from timur_bot.services.secure_face import process_secure_photo, resolve_secure_source_message

logger = logging.getLogger("timur-bot.secure")
_SECURE_SEMAPHORE = asyncio.Semaphore(1)
_ROOT_DIR = Path(__file__).resolve().parents[2]
_SECURE_AUTO_STATE_PATH = _ROOT_DIR / "data" / "secure_auto_state.json"


def _load_secure_auto_state() -> dict[str, bool]:
    if not _SECURE_AUTO_STATE_PATH.exists():
        return {}
    try:
        with _SECURE_AUTO_STATE_PATH.open("r", encoding="utf-8") as fh:
            raw = json.load(fh)
    except Exception:
        return {}
    if not isinstance(raw, dict):
        return {}
    out: dict[str, bool] = {}
    for key, value in raw.items():
        if str(key).strip():
            out[str(key)] = bool(value)
    return out


def _save_secure_auto_state(state: dict[str, bool]) -> None:
    _SECURE_AUTO_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with _SECURE_AUTO_STATE_PATH.open("w", encoding="utf-8") as fh:
        json.dump(state, fh, ensure_ascii=False, indent=2)


def _is_secure_auto_enabled(chat_id: int) -> bool:
    state = _load_secure_auto_state()
    return bool(state.get(str(chat_id), False))


def _set_secure_auto_enabled(chat_id: int, enabled: bool) -> None:
    state = _load_secure_auto_state()
    state[str(chat_id)] = bool(enabled)
    _save_secure_auto_state(state)


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
    silent_on_no_match: bool = False,
    require_other_face: bool = False,
) -> None:
    chat_id = source_message.chat_id
    try:
        async with _SECURE_SEMAPHORE:
            await context.bot.send_chat_action(chat_id=chat_id, action=ChatAction.UPLOAD_PHOTO)
            file = await context.bot.get_file(source_message.photo[-1].file_id)
            file_bytes = await file.download_as_bytearray()
            result = await asyncio.to_thread(process_secure_photo, bytes(file_bytes))

            other_faces_count = max(0, int(result.detected_faces) - int(result.matched_faces))
            has_match = result.matched_faces > 0
            has_other_face = other_faces_count >= 1
            if require_other_face and has_match and not has_other_face:
                logger.info(
                    "/secure skipped: target present but no other face (detected=%s matched=%s)",
                    result.detected_faces,
                    result.matched_faces,
                )
                return

            if not has_match:
                if result.detected_faces <= 0:
                    text = "Лицо на фото не обнаружено. Попробуй кадр, где лицо крупнее и без сильного наклона."
                else:
                    text = "а че secure то?"
                logger.info(
                    "/secure no match: detected_faces=%s best_distance=%s",
                    result.detected_faces,
                    f"{result.best_distance:.2f}" if result.best_distance is not None else "n/a",
                )
                if silent_on_no_match:
                    return
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
    if message and context.args and context.args[0].lower() == "auto":
        if len(context.args) < 2 or context.args[1].lower() not in {"on", "off"}:
            await message.reply_text("использование: /secure auto on|off")
            return
        enabled = context.args[1].lower() == "on"
        _set_secure_auto_enabled(message.chat_id, enabled)
        status = "включен" if enabled else "выключен"
        await message.reply_text(f"secure auto {status}")
        logger.info("/secure auto toggled: chat_id=%s enabled=%s", message.chat_id, enabled)
        return

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


async def secure_auto_photo_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    if not message or not message.photo:
        return
    if not _is_secure_auto_enabled(message.chat_id):
        return

    task_factory = getattr(context, "application", None)
    coro = _run_secure_task(
        context,
        source_message=message,
        trigger_message_id=message.message_id,
        silent_on_no_match=True,
        require_other_face=True,
    )
    task = task_factory.create_task(coro) if task_factory else asyncio.create_task(coro)
    task.add_done_callback(_log_task_failure)
