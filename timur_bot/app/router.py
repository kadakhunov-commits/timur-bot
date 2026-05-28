from telegram.ext import CallbackQueryHandler, CommandHandler, MessageHandler, MessageReactionHandler, filters

from timur_bot.handlers.admin import (
    admin_callback_handler,
    admin_cmd,
    miniapp_cmd,
    miniappdebug_cmd,
    web_app_data_handler,
)
from timur_bot.handlers.billing import (
    billabuse_cmd,
    billhelp_cmd,
    billinvoices_cmd,
    billpay_cmd,
    billquote_cmd,
    billref_cmd,
    billsetup_cmd,
    billstatus_cmd,
)
from timur_bot.handlers.chat import command_memory_tap, photo_handler, reaction_handler, start_cmd, story_cmd, summary_cmd, text_handler
from timur_bot.handlers.noire import noire_cmd
from timur_bot.handlers.secure import secure_auto_photo_handler, secure_cmd
from timur_bot.handlers.owner import (
    appendprompt_cmd,
    bit_cmd,
    bits_cmd,
    clearmemory_cmd,
    dump_cmd,
    funny_cmd,
    mood_cmd,
    moodguard_cmd,
    moodopen_cmd,
    moodreset_cmd,
    moodset_cmd,
    moodevent_cmd,
    mode_cmd,
    remember_cmd,
    resetprompt_cmd,
    setbio_cmd,
    setheat_cmd,
    setmode_cmd,
    setprompt_cmd,
    setstyle_cmd,
    showmode_cmd,
    showprompt_cmd,
    unfunny_cmd,
    whois_cmd,
)
from timur_bot.services.noire import NOIRE_COMMAND_PATTERN
from timur_bot.services.secure_face import SECURE_COMMAND_PATTERN


def register_handlers(application) -> None:
    application.add_handler(CommandHandler("start", start_cmd))
    application.add_handler(CommandHandler("story", story_cmd))
    application.add_handler(CommandHandler("summary", summary_cmd))
    application.add_handler(CommandHandler("noire", noire_cmd))
    application.add_handler(CommandHandler("secure", secure_cmd))
    application.add_handler(CommandHandler("admin", admin_cmd))
    application.add_handler(CommandHandler("panel", admin_cmd))
    application.add_handler(CommandHandler("miniapp", miniapp_cmd))
    application.add_handler(CommandHandler("miniappdebug", miniappdebug_cmd))
    application.add_handler(CommandHandler("billhelp", billhelp_cmd))
    application.add_handler(CommandHandler("billquote", billquote_cmd))
    application.add_handler(CommandHandler("billsetup", billsetup_cmd))
    application.add_handler(CommandHandler("billstatus", billstatus_cmd))
    application.add_handler(CommandHandler("billinvoices", billinvoices_cmd))
    application.add_handler(CommandHandler("billpay", billpay_cmd))
    application.add_handler(CommandHandler("billabuse", billabuse_cmd))
    application.add_handler(CommandHandler("billref", billref_cmd))
    application.add_handler(CommandHandler("setprompt", setprompt_cmd))
    application.add_handler(CommandHandler("appendprompt", appendprompt_cmd))
    application.add_handler(CommandHandler("showprompt", showprompt_cmd))
    application.add_handler(CommandHandler("resetprompt", resetprompt_cmd))
    application.add_handler(CommandHandler("setbio", setbio_cmd))
    application.add_handler(CommandHandler("setstyle", setstyle_cmd))
    application.add_handler(CommandHandler("setheat", setheat_cmd))
    application.add_handler(CommandHandler("mode", mode_cmd))
    application.add_handler(CommandHandler("setmode", setmode_cmd))
    application.add_handler(CommandHandler("showmode", showmode_cmd))
    application.add_handler(CommandHandler("bit", bit_cmd))
    application.add_handler(CommandHandler("bits", bits_cmd))
    application.add_handler(CommandHandler("funny", funny_cmd))
    application.add_handler(CommandHandler("unfunny", unfunny_cmd))
    application.add_handler(CommandHandler("mood", mood_cmd))
    application.add_handler(CommandHandler("moodevent", moodevent_cmd))
    application.add_handler(CommandHandler("moodset", moodset_cmd))
    application.add_handler(CommandHandler("moodguard", moodguard_cmd))
    application.add_handler(CommandHandler("moodopen", moodopen_cmd))
    application.add_handler(CommandHandler("moodreset", moodreset_cmd))
    application.add_handler(CommandHandler("remember", remember_cmd))
    application.add_handler(CommandHandler("whois", whois_cmd))
    application.add_handler(CommandHandler("dump", dump_cmd))
    application.add_handler(CommandHandler("clearmemory", clearmemory_cmd))
    application.add_handler(MessageHandler(filters.COMMAND, command_memory_tap), group=1)
    application.add_handler(
        MessageHandler(
            filters.PHOTO & ~filters.CaptionRegex(NOIRE_COMMAND_PATTERN) & ~filters.CaptionRegex(SECURE_COMMAND_PATTERN),
            secure_auto_photo_handler,
        ),
        group=-1,
    )
    application.add_handler(CallbackQueryHandler(admin_callback_handler, pattern=r"^adm:"))
    application.add_handler(MessageHandler(filters.StatusUpdate.WEB_APP_DATA, web_app_data_handler))
    application.add_handler(MessageReactionHandler(reaction_handler))
    application.add_handler(MessageHandler(filters.PHOTO & filters.CaptionRegex(NOIRE_COMMAND_PATTERN), noire_cmd))
    application.add_handler(MessageHandler(filters.PHOTO & filters.CaptionRegex(SECURE_COMMAND_PATTERN), secure_cmd))
    application.add_handler(
        MessageHandler(
            filters.PHOTO
            & ~filters.COMMAND
            & ~filters.CaptionRegex(NOIRE_COMMAND_PATTERN)
            & ~filters.CaptionRegex(SECURE_COMMAND_PATTERN),
            photo_handler,
        )
    )
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler))
