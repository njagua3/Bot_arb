import os
import requests
from dotenv import load_dotenv

load_dotenv()

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

def send_telegram_alert(message: str):
    if not TOKEN or not CHAT_ID:
        print("❌ Missing Telegram credentials.")
        return

    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    data = {
        "chat_id": CHAT_ID,
        "text": message,
        "parse_mode": "HTML"
    }

    try:
        response = requests.post(url, data=data)
        if response.status_code != 200:
            print("❌ Telegram error:", response.text)
        else:
            print("📩 Telegram alert sent!")
    except Exception as e:
        print("❌ Error sending Telegram alert:", e)
