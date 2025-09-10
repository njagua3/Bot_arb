# core/telegram.py
import os
import time
import requests
from dotenv import load_dotenv
from telegram.ext import Updater, CommandHandler, CallbackContext
from telegram import Update

from core.logger import get_logger
from core.settings import load_stake, set_stake, SCAN_INTERVAL
from scrapers.scraper_loader import discover_scrapers

# Load environment variables
load_dotenv()
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")

# Support multiple authorized users (comma separated list in .env)
CHAT_IDS = set(
    chat_id.strip()
    for chat_id in os.getenv("TELEGRAM_CHAT_IDS", os.getenv("TELEGRAM_CHAT_ID", "")).split(",")
    if chat_id.strip()
)

# Use central logger
logger = get_logger(__name__)


# ===============================
# ALERTS
# ===============================
def send_telegram_alert(message: str, retries: int = 3, backoff: float = 2.0):
    """
    Send a Telegram alert to all authorized users.
    Retries on transient network errors with exponential backoff.
    """
    if not TOKEN or not CHAT_IDS:
        logger.error("❌ Missing Telegram credentials. Check .env file.")
        return

    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"

    for chat_id in CHAT_IDS:
        data = {
            "chat_id": chat_id,
            "text": message,      # ✅ allow HTML markup
            "parse_mode": "HTML"
        }

        for attempt in range(1, retries + 1):
            try:
                response = requests.post(url, data=data, timeout=10)
                if response.status_code == 200:
                    logger.info(f"📩 Telegram alert sent successfully to {chat_id}.")
                    break
                else:
                    logger.error(f"❌ Telegram API error for {chat_id}: {response.text}")
            except requests.exceptions.RequestException as e:
                logger.warning(f"⚠️ Network issue (attempt {attempt}/{retries}) for {chat_id}: {e}")
            except Exception as e:
                logger.exception(f"❌ Unexpected error for {chat_id}: {e}")

            if attempt < retries:
                time.sleep(backoff * attempt)
        else:
            logger.error(f"❌ Failed to send Telegram alert to {chat_id} after retries.")


# ===============================
# BOT COMMANDS
# ===============================
def stake(update: Update, context: CallbackContext):
    """Update stake via /stake <amount>"""
    chat_id = str(update.effective_chat.id)
    if chat_id not in CHAT_IDS:
        update.message.reply_text("❌ You're not authorized to change the stake.")
        return

    try:
        new_stake = int(context.args[0])
        if new_stake < 100:
            raise ValueError
        set_stake(new_stake)
        update.message.reply_text(f"✅ Stake updated to {new_stake} KES.")
        logger.info(f"Stake updated to {new_stake} by user {chat_id}")
    except (IndexError, ValueError):
        update.message.reply_text("❗ Usage: /stake 15000")


def start(update: Update, context: CallbackContext):
    """Respond to /start with current settings"""
    current_stake = load_stake()
    update.message.reply_text(
        f"🤖 Hello! Welcome to Njagua Arb Bot.\n"
        f"Your current stake is {current_stake} KES.\n"
        f"Use /stake <amount> to change it.\n"
        f"Type /help to see all available commands."
    )


def help_command(update: Update, context: CallbackContext):
    """Show available commands"""
    update.message.reply_text(
        "📖 Available commands:\n"
        "/start – Show welcome message + current stake\n"
        "/stake <amount> – Update stake (authorized users only)\n"
        "/status – Show bot status (authorized users only)\n"
        "/help – Show this help message"
    )


def status_command(update: Update, context: CallbackContext):
    """Show bot status (stake, scan interval, scrapers)"""
    chat_id = str(update.effective_chat.id)
    if chat_id not in CHAT_IDS:
        update.message.reply_text("❌ You're not authorized to view status.")
        logger.warning(f"🚨 Unauthorized /status attempt by chat_id={chat_id}")
        return

    current_stake = load_stake()
    try:
        scrapers = discover_scrapers()
        scraper_count = len(scrapers)
    except Exception as e:
        scraper_count = 0
        logger.error(f"❌ Failed to discover scrapers in /status: {e}")

    update.message.reply_text(
        f"📊 Bot Status:\n"
        f"• Stake: {current_stake} KES\n"
        f"• Scan interval: {SCAN_INTERVAL} seconds\n"
        f"• Active scrapers: {scraper_count}"
    )


# ===============================
# RUN BOT
# ===============================
def run_bot():
    """
    Start Telegram bot command listener (/start, /stake, /status, /help).
    Runs in a background thread (see main.py).
    """
    if not TOKEN:
        logger.error("❌ TELEGRAM_BOT_TOKEN is missing.")
        return

    try:
        updater = Updater(TOKEN, use_context=True)
    except Exception as e:
        logger.error(f"❌ Failed to initialize Telegram Updater: {e}")
        return

    dp = updater.dispatcher
    dp.add_handler(CommandHandler("start", start))
    dp.add_handler(CommandHandler("stake", stake))
    dp.add_handler(CommandHandler("status", status_command))
    dp.add_handler(CommandHandler("help", help_command))

    logger.info(f"🤖 Telegram Bot is running for {len(CHAT_IDS)} authorized users.")
    updater.start_polling()   # ✅ non-blocking


# Run directly (debug mode)
if __name__ == "__main__":
    run_bot()
