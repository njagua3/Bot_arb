# scrapers/tasks.py (Production-Ready with Non-blocking Fallback)

import asyncio
import importlib
import inspect
import logging
import os
import random
import time
from typing import Optional, Dict, Any
from datetime import datetime, timezone

from bs4 import BeautifulSoup
import redis
from celery import Celery, chain
from celery.exceptions import MaxRetriesExceededError
from playwright.sync_api import sync_playwright
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

# ---------- Logging ----------
logger = logging.getLogger("scrapers.tasks")
logger.setLevel(os.environ.get("SCRAPER_LOG_LEVEL", "INFO"))
_handler = logging.StreamHandler()
_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
logger.addHandler(_handler)

# ---------- Redis ----------
REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
redis_client = redis.Redis.from_url(REDIS_URL, decode_responses=True)
BLACKLIST_ZSET = os.environ.get("BLACKLIST_ZSET", "proxy:blacklist")
BLACKLIST_COOLDOWN_SEC = int(os.environ.get("PROXY_BLACKLIST_COOLDOWN", 1800))
METRICS_PREFIX = os.environ.get("METRICS_PREFIX", "scraper:metrics")

# ---------- Celery ----------
CELERY_BROKER_URL = os.environ.get("CELERY_BROKER_URL", REDIS_URL)
celery_app = Celery("scrapers", broker=CELERY_BROKER_URL, backend=CELERY_BROKER_URL)
celery_app.conf.update(
    result_expires=int(os.environ.get("CELERY_RESULT_EXPIRES", 3600)),
    task_time_limit=int(os.environ.get("CELERY_TASK_TIME_LIMIT", 90)),
    task_soft_time_limit=int(os.environ.get("CELERY_TASK_SOFT_TIME_LIMIT", 75)),
    broker_transport_options={"visibility_timeout": 3600},
)

# ---------- Blacklist Helpers ----------
def prune_expired_blacklist():
    now_ts = int(time.time())
    redis_client.zremrangebyscore(BLACKLIST_ZSET, 0, now_ts)

def blacklist_proxy(proxy: str, cooldown: Optional[int] = None):
    if not proxy:
        return
    expire_ts = int(time.time()) + (cooldown or BLACKLIST_COOLDOWN_SEC)
    redis_client.zadd(BLACKLIST_ZSET, {proxy: expire_ts})
    logger.info(f"Blacklisted proxy {proxy} until {datetime.fromtimestamp(expire_ts, tz=timezone.utc)}")

def is_blacklisted(proxy: str) -> bool:
    prune_expired_blacklist()
    return redis_client.zscore(BLACKLIST_ZSET, proxy) is not None

def available_proxies(pool: list) -> list:
    prune_expired_blacklist()
    return [p for p in pool if redis_client.zscore(BLACKLIST_ZSET, p) is None]

# ---------- Browser Workers ----------
@celery_app.task(queue="high_priority")
def run_selenium_task(url: str, user_agent: str, proxy: Optional[str] = None, timeout: int = 30):
    try:
        opts = Options()
        opts.add_argument("--headless=new")
        opts.add_argument(f"user-agent={user_agent}")
        if proxy:
            opts.add_argument(f"--proxy-server={proxy}")
        driver = webdriver.Chrome(options=opts)
        driver.set_page_load_timeout(timeout)
        driver.get(url)
        html = driver.page_source
        driver.quit()
        return html
    except Exception:
        logger.exception("Selenium task failed")
        return None

@celery_app.task(queue="high_priority")
def run_playwright_task(url: str, user_agent: str, proxy: Optional[str] = None, timeout: int = 30):
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context_kwargs = {"user_agent": user_agent}
            if proxy:
                context_kwargs["proxy"] = {"server": proxy}
            context = browser.new_context(**context_kwargs)
            page = context.new_page()
            page.goto(url, timeout=timeout * 1000)
            html = page.content()
            browser.close()
            return html
    except Exception:
        logger.exception("Playwright task failed")
        return None

# ---------- Fallback Processor ----------
@celery_app.task(queue="default")
def process_fallback_html(html: str, scraper_module: str, scraper_class: str, proxy: Optional[str] = None):
    if not html:
        return {"status": "NO_HTML"}
    try:
        module = importlib.import_module(scraper_module)
        cls = getattr(module, scraper_class)
        scraper = cls()
        soup = BeautifulSoup(html, "html.parser")
        parsed = scraper.parse_html(soup) if not inspect.iscoroutinefunction(scraper.parse_html) \
            else asyncio.run(scraper.parse_html(soup))
        if parsed:
            result = {
                "bookmaker": getattr(scraper, "bookmaker", scraper_class),
                "matches": parsed,
                "proxy_used": proxy,
                "fallback_used": True,
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
            redis_client.setex(f"scraper:fallback:{scraper_class}", 3600, str(result))
            return {"status": "OK", "matches": len(parsed)}
        return {"status": "NO_MATCHES"}
    except Exception:
        logger.exception("Fallback processing failed")
        return {"status": "ERROR"}

# ---------- Main Scraper ----------
@celery_app.task(bind=True, max_retries=3, default_retry_delay=5, queue="default")
def run_scraper_task(self, scraper_module: str, scraper_class: str, proxy_pool: Optional[list] = None,
                     max_retries: int = 3, jitter: int = 5, fallback_timeout: int = 60):
    attempt = self.request.retries + 1
    proxy = None
    try:
        # Select proxy
        if proxy_pool:
            avail = available_proxies(proxy_pool)
            if not avail:
                raise RuntimeError("No available proxies")
            proxy = avail[self.request.retries % len(avail)]

        # Load scraper
        module = importlib.import_module(scraper_module)
        cls = getattr(module, scraper_class)
        scraper = cls(proxy=proxy) if "proxy" in inspect.signature(cls).parameters else cls()
        bookmaker = getattr(scraper, "bookmaker", scraper_class)

        logger.info(f"[{bookmaker}] Attempt {attempt}/{max_retries} (proxy={proxy})")

        # Execute
        result = asyncio.run(scraper.get_odds()) if getattr(scraper, "is_async", False) else scraper.get_odds()
        if not isinstance(result, list):
            raise ValueError("Invalid scraper result")

        return {"bookmaker": bookmaker, "matches": result, "proxy_used": proxy,
                "timestamp": datetime.now(timezone.utc).isoformat()}

    except Exception as exc:
        logger.exception(f"[{scraper_class}] Attempt {attempt} failed")

        if proxy:
            blacklist_proxy(proxy)

        # Retry with exponential backoff
        if attempt < max_retries:
            delay = max(1, int((2 ** attempt) + random.uniform(0, jitter)))
            raise self.retry(exc=exc, countdown=delay)

        # Final attempt → schedule non-blocking fallback chain
        module = importlib.import_module(scraper_module)
        cls = getattr(module, scraper_class)
        if getattr(cls, "supports_browser_fallback", True):
            fallback_url = getattr(cls, "fallback_url", None) or getattr(scraper, "base_url", None)
            user_agent = getattr(cls, "user_agent", getattr(scraper, "ua", "Mozilla/5.0"))
            if fallback_url:
                logger.warning(f"[{scraper_class}] Scheduling non-blocking fallback chain")
                chain(
                    run_playwright_task.s(fallback_url, user_agent, proxy),
                    process_fallback_html.s(scraper_module, scraper_class, proxy)
                ).apply_async()
        return {"status": "FALLBACK_SCHEDULED", "bookmaker": scraper_class}
