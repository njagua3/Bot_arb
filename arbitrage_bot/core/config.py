# core/config.py
from __future__ import annotations
import os
from dataclasses import dataclass
from pathlib import Path
from dotenv import load_dotenv

# Load .env early
load_dotenv()

def _env(name: str, default: str = "") -> str:
    v = os.getenv(name, default)
    return v.strip() if isinstance(v, str) else v

def _bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None: return default
    return str(v).lower() in {"1","true","yes","y","on"}

def _int(name: str, default: int) -> int:
    v = os.getenv(name)
    try:
        return int(v) if v is not None else default
    except Exception:
        return default

@dataclass(frozen=True)
class EnvConfig:
    # App
    ENV: str = _env("ENV", "development")

    # Telegram
    TELEGRAM_BOT_TOKEN: str = _env("TELEGRAM_BOT_TOKEN")
    TELEGRAM_CHAT_ID: str = _env("TELEGRAM_CHAT_ID")

    # DB
    DB_URL: str = _env("DB_URL")
    DB_HOST: str = _env("DB_HOST", "localhost")
    DB_PORT: int = _int("DB_PORT", 3306)
    DB_NAME: str = _env("DB_NAME", "arbitrage_db")
    DB_USER: str = _env("DB_USER", "arb_user")
    DB_PASSWORD: str = _env("DB_PASSWORD", "strong_password")

    # Redis/Celery
    REDIS_URL: str = _env("REDIS_URL", "redis://localhost:6379/0")
    CELERY_BROKER_URL: str = _env("CELERY_BROKER_URL", "redis://localhost:6379/0")
    CELERY_RESULT_EXPIRES: int = _int("CELERY_RESULT_EXPIRES", 3600)
    CELERY_TASK_TIME_LIMIT: int = _int("CELERY_TASK_TIME_LIMIT", 90)
    CELERY_TASK_SOFT_TIME_LIMIT: int = _int("CELERY_TASK_SOFT_TIME_LIMIT", 75)

    # Scraper & metrics
    SCRAPER_LOG_LEVEL: str = _env("SCRAPER_LOG_LEVEL", "INFO")
    BLACKLIST_ZSET: str = _env("BLACKLIST_ZSET", "proxy:blacklist")
    PROXY_BLACKLIST_COOLDOWN: int = _int("PROXY_BLACKLIST_COOLDOWN", 1800)
    METRICS_PREFIX: str = _env("METRICS_PREFIX", "scraper:metrics")

    # Settings file path (used by core.settings)
    SETTINGS_FILE: Path = Path(_env("SETTINGS_FILE", "data/settings.json"))

    def effective_db_url(self) -> str:
        if self.DB_URL:
            return self.DB_URL
        # Build from pieces if DB_URL not provided
        return f"mysql+pymysql://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"

ENVCFG = EnvConfig()

# Optional: terse debug (no secrets)
def describe_env() -> str:
    return (
        f"[env={ENVCFG.ENV}] DB={'URL' if bool(ENVCFG.DB_URL) else 'BUILT'} | "
        f"Redis='{ENVCFG.REDIS_URL}' | Broker='{ENVCFG.CELERY_BROKER_URL}' | "
        f"Settings='{ENVCFG.SETTINGS_FILE}'"
    )
