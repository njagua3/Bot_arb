# core/settings.py
import json
import os
from pathlib import Path
from typing import Any, Dict

# Allow dynamic config file path via environment variable
SETTINGS_FILE = Path(os.getenv("SETTINGS_FILE", "data/settings.json"))
SETTINGS_FILE.parent.mkdir(parents=True, exist_ok=True)

DEFAULT_SETTINGS: Dict[str, Any] = {
    "stake": 10000,                # Default stake in local currency
    "min_profit_percent": 3.0,     # Ignore arbitrage opportunities < 2%
    "min_profit_absolute": 300.0,    # Ignore arbitrage opportunities < 1 KES
    "cache_expiry_minutes": 5,     # Cached results expiry
    "scan_interval": 180,          # Interval between scans (seconds)
    "log_dir": "data",             # Log directory
    "log_file": "arb_log.txt",     # Log filename
    "log_console": True            # Print logs to console
}


def validate_settings(settings: Dict[str, Any]) -> Dict[str, Any]:
    """
    Ensure settings have correct types and fill in missing defaults.
    """
    validated = {**DEFAULT_SETTINGS, **settings}
    validated["stake"] = float(validated.get("stake", DEFAULT_SETTINGS["stake"]))
    validated["min_profit_percent"] = float(validated.get("min_profit_percent", DEFAULT_SETTINGS["min_profit_percent"]))
    validated["min_profit_absolute"] = float(validated.get("min_profit_absolute", DEFAULT_SETTINGS["min_profit_absolute"]))
    validated["cache_expiry_minutes"] = int(validated.get("cache_expiry_minutes", DEFAULT_SETTINGS["cache_expiry_minutes"]))
    validated["scan_interval"] = int(validated.get("scan_interval", DEFAULT_SETTINGS["scan_interval"]))
    validated["log_console"] = bool(validated.get("log_console", DEFAULT_SETTINGS["log_console"]))
    return validated


def load_settings() -> Dict[str, Any]:
    """
    Loads settings from JSON file. If file doesn't exist,
    defaults are written and returned.
    """
    if SETTINGS_FILE.exists():
        with open(SETTINGS_FILE, "r") as f:
            try:
                data = json.load(f)
                return validate_settings(data)
            except json.JSONDecodeError:
                return DEFAULT_SETTINGS.copy()
    else:
        save_settings(DEFAULT_SETTINGS)
        return DEFAULT_SETTINGS.copy()


def save_settings(settings: Dict[str, Any]) -> None:
    """Saves settings to file."""
    with open(SETTINGS_FILE, "w") as f:
        json.dump(validate_settings(settings), f, indent=2)


def get_setting(key: str, default: Any = None) -> Any:
    """
    Fetch a single setting with fallback.
    Priority: Environment variable -> JSON file -> Default.
    """
    # 1. Check environment variable
    env_val = os.getenv(key.upper())
    if env_val is not None:
        try:
            return type(default)(env_val) if default is not None else env_val
        except Exception:
            return env_val

    # 2. Check settings.json
    settings = load_settings()
    return settings.get(key, default)


def set_setting(key: str, value: Any) -> None:
    """Update a single setting in JSON file."""
    settings = load_settings()
    settings[key] = value
    save_settings(settings)


# Shortcut helpers
def load_stake() -> float:
    """Returns the current stake amount."""
    return float(get_setting("stake", DEFAULT_SETTINGS["stake"]))


def set_stake(value: float) -> None:
    """Updates stake value."""
    set_setting("stake", float(value))


# ✅ Export scan interval constant for main.py
SCAN_INTERVAL: int = get_setting("scan_interval", DEFAULT_SETTINGS["scan_interval"])
