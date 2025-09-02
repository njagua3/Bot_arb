# core/telegram.py
import os
import requests
from dotenv import load_dotenv
from telegram.ext import Updater, CommandHandler, CallbackContext
from telegram import Update

from core.logger import get_logger
from core.settings import load_stake, set_stake

# Load environment variables
load_dotenv()
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# Use central logger
logger = get_logger(__name__)


# =============== ALERTS ===============
def send_telegram_alert(message: str):
    """Send a Telegram alert using bot token + chat ID from .env"""
    if not TOKEN or not CHAT_ID:
        logger.error("❌ Missing Telegram credentials. Check .env file.")
        return

    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    data = {
        "chat_id": CHAT_ID,
        "text": message,          # ✅ no escaping, we want HTML markup to render
        "parse_mode": "HTML"
    }

    try:
        response = requests.post(url, data=data, timeout=10)
        if response.status_code != 200:
            logger.error(f"❌ Telegram API error: {response.text}")
        else:
            logger.info("📩 Telegram alert sent successfully.")
    except requests.exceptions.RequestException as e:
        logger.exception(f"❌ Network/Request error while sending Telegram alert: {e}")
    except Exception as e:
        logger.exception(f"❌ Unexpected error while sending Telegram alert: {e}")


# =============== BOT COMMANDS ===============
def stake(update: Update, context: CallbackContext):
    chat_id = str(update.effective_chat.id)
    if chat_id != CHAT_ID:
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


def start(update: Update, context: CallbackContext):
    current_stake = load_stake()
    update.message.reply_text(
        f"🤖 Hello! Welcome to Njagua Arb Bot.\n"
        f"Your current stake is {current_stake} KES.\n"
        f"Use /stake <amount> to change it."
    )


def run_bot():
    """Start Telegram bot command listener (/start, /stake)"""
    if not TOKEN:
        logger.error("❌ TELEGRAM_BOT_TOKEN is missing.")
        return

    updater = Updater(TOKEN, use_context=True)
    dp = updater.dispatcher

    dp.add_handler(CommandHandler("start", start))
    dp.add_handler(CommandHandler("stake", stake))

    logger.info("🤖 Telegram Bot is running in background thread.")
    updater.start_polling()   # ✅ no idle()


# Run directly
if __name__ == "__main__":
    run_bot()
