# core/bot_commands.py

import os
from dotenv import load_dotenv
from telegram.ext import Updater, CommandHandler
from telegram import Update
from telegram.ext import CallbackContext
from core.settings import load_stake, set_stake

# Load environment variables
load_dotenv()
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
AUTHORIZED_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# ⚙️ Handle /stake command
def stake(update: Update, context: CallbackContext):
    chat_id = str(update.effective_chat.id)
    if chat_id != AUTHORIZED_CHAT_ID:
        update.message.reply_text("❌ You're not authorized to change the stake.")
        return

    try:
        new_stake = int(context.args[0])
        if new_stake < 100:
            raise ValueError
        set_stake(new_stake)
        update.message.reply_text(f"✅ Stake updated to {new_stake} KES.")
    except (IndexError, ValueError):
        update.message.reply_text("❗ Usage: /stake 15000")

# ⚙️ Handle /start command
def start(update: Update, context: CallbackContext):
    current_stake = load_stake()
    update.message.reply_text(
        f"🤖 Hello! Welcome to Njagua Arb Bot.\n" 
        f"Your current stake is {current_stake} KES.\n"
        f"Use /stake <amount> to change it."
    )

# 🟢 Start the Telegram bot
def run_bot():
    if not TOKEN:
        print("❌ TELEGRAM_BOT_TOKEN is missing.")
        return

    updater = Updater(TOKEN, use_context=True)
    dp = updater.dispatcher

    dp.add_handler(CommandHandler("start", start))
    dp.add_handler(CommandHandler("stake", stake))

    print("🤖 Telegram Bot is running. Press Ctrl+C to stop.")
    updater.start_polling()
    updater.idle()

# 🏁 Run bot if executed directly
if __name__ == "__main__":
    run_bot()
