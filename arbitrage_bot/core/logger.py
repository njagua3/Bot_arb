# core/logger.py

import os
from datetime import datetime
import json

# Use the same settings.json as core/settings.py
SETTINGS_FILE = os.path.join("data", "settings.json")
DEFAULT_LOG_DIR = "data"
DEFAULT_LOG_FILE = "arb_log.txt"


def load_log_config():
    """Load log configuration from settings.json if available."""
    try:
        with open(SETTINGS_FILE, "r", encoding="utf-8") as f:
            config = json.load(f)
        log_dir = config.get("log_dir", DEFAULT_LOG_DIR)
        log_file = config.get("log_file", DEFAULT_LOG_FILE)
        console = config.get("log_console", True)
    except (FileNotFoundError, json.JSONDecodeError):
        # fallback defaults
        log_dir = DEFAULT_LOG_DIR
        log_file = DEFAULT_LOG_FILE
        console = True
    
    return log_dir, os.path.join(log_dir, log_file), console


LOG_DIR, LOG_FILE, LOG_TO_CONSOLE = load_log_config()

# Ensure log directory exists
os.makedirs(LOG_DIR, exist_ok=True)


def _log(message: str, level: str = "INFO"):
    """Internal logger function with levels + console output."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    formatted = f"[{timestamp}] [{level}] {message.strip()}"
    
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(formatted + "\n")
    except Exception as e:
        print(f"❌ Failed to write to log file: {e}")

    # Console output (only if enabled)
    if LOG_TO_CONSOLE:
        print(formatted)


# Public helper functions
def log_info(message: str):
    _log(message, "INFO")

def log_success(message: str):
    _log(message, "SUCCESS")

def log_warning(message: str):
    _log(message, "WARNING")

def log_error(message: str):
    _log(message, "ERROR")


# ✅ Add get_logger() so existing imports still work
class SimpleLogger:
    def info(self, msg): log_info(msg)
    def success(self, msg): log_success(msg)
    def warning(self, msg): log_warning(msg)
    def error(self, msg): log_error(msg)

def get_logger(name=None):
    """Return a simple logger object with .info(), .error(), etc."""
    return SimpleLogger()
