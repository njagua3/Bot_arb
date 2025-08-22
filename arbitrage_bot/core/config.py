# core/config.py
import os
from dotenv import load_dotenv

# Load .env variables into environment
load_dotenv()

# Telegram configs
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

# Database configs (future-proofing)
DB_URL = os.getenv("DB_URL", "sqlite:///data/arbitrage.db")

# Environment mode
ENV = os.getenv("ENV", "development")  # or "production"
