# core/logger.py

import os
from datetime import datetime

LOG_FILE = os.path.join("data", "arb_log.txt")

def log_to_file(message: str, filename: str = LOG_FILE):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        with open(filename, "a", encoding="utf-8") as f:
            f.write(f"[{timestamp}] {message.strip()}\n")
    except Exception as e:
        print(f"❌ Failed to write to log file: {e}")
