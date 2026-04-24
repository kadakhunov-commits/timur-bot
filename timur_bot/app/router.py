from telegram.ext import CallbackQueryHandler, CommandHandler, MessageHandler, MessageReactionHandler, filters

from timur_bot.handlers.admin import admin_callback_handler, admin_cmd
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
from timur_bot.handlers.chat import photo_handler, reaction_handler, start_cmd, text_handler
from timur_bot.handlers.owner import (
    appendprompt_cmd,
    bit_cmd,
    bits_cmd,
    clearmemory_cmd,
    dump_cmd,
    funny_cmd,
    remember_cmd,
    resetprompt_cmd,
    setbio_cmd,
    setheat_cmd,
    setprompt_cmd,
    setstyle_cmd,
    showprompt_cmd,
    unfunny_cmd,
    whois_cmd,
)


def register_handlers(application) -> None:
    application.add_handler(CommandHandler("start", start_cmd))
    application.add_handler(CommandHandler("admin", admin_cmd))
    application.add_handler(CommandHandler("panel", admin_cmd))
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
    application.add_handler(CommandHandler("bit", bit_cmd))
    application.add_handler(CommandHandler("bits", bits_cmd))
    application.add_handler(CommandHandler("funny", funny_cmd))
    application.add_handler(CommandHandler("unfunny", unfunny_cmd))
    application.add_handler(CommandHandler("remember", remember_cmd))
    application.add_handler(CommandHandler("whois", whois_cmd))
    application.add_handler(CommandHandler("dump", dump_cmd))
    application.add_handler(CommandHandler("clearmemory", clearmemory_cmd))
    application.add_handler(CallbackQueryHandler(admin_callback_handler, pattern=r"^adm:"))
    application.add_handler(MessageReactionHandler(reaction_handler))
    application.add_handler(MessageHandler(filters.PHOTO & ~filters.COMMAND, photo_handler))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler))
