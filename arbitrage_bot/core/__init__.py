def run_bot():
    from config import TELEGRAM_BOT_TOKEN
    from telegram.ext import Updater, CommandHandler

    updater = Updater(TELEGRAM_BOT_TOKEN, use_context=True)
    dp = updater.dispatcher

    dp.add_handler(CommandHandler("start", start))
    dp.add_handler(CommandHandler("stake", set_stake))

    print("🤖 Telegram Bot running. Use Ctrl+C to stop.")
    updater.start_polling()
    updater.idle()


if __name__ == "__main__":
    run_bot()
