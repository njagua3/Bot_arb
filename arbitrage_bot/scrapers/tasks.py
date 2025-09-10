# scrapers/tasks.py

import asyncio
import importlib
import inspect
import json
import logging
import os
import random
import time
from typing import Optional, List, Dict, Any
from datetime import datetime, timezone

from bs4 import BeautifulSoup
import redis
from celery import Celery, chain
from playwright.sync_api import sync_playwright
from selenium import webdriver
from selenium.webdriver.chrome.options import Options


# ---------- Logging (JSON Structured) ----------
class JsonFormatter(logging.Formatter):
    def format(self, record):
        payload = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload)


logger = logging.getLogger("scrapers.tasks")
if not logger.handlers:
    _handler = logging.StreamHandler()
    _handler.setFormatter(JsonFormatter())
    logger.addHandler(_handler)
logger.setLevel(os.environ.get("SCRAPER_LOG_LEVEL", "INFO"))


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


# ---------- Helpers ----------
def safe_async_run(coro):
    """Safely run async code inside Celery worker (avoid asyncio.run conflicts)."""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    if loop and loop.is_running():
        return loop.run_until_complete(coro)
    else:
        return asyncio.run(coro)


def _metric_key(bookmaker: str, metric: str) -> str:
    return f"{METRICS_PREFIX}:{bookmaker}:{metric}"


def _record_metric(metric: str, bookmaker: str, value: int = 1):
    try:
        redis_client.incrby(_metric_key(bookmaker, metric), value)
        logger.info(json.dumps({
            "event": "metric_recorded",
            "bookmaker": bookmaker,
            "metric": metric,
            "value": value
        }))
    except Exception as e:
        logger.warning(json.dumps({
            "event": "metric_failed",
            "bookmaker": bookmaker,
            "metric": metric,
            "error": str(e)
        }))


# ---------- Blacklist ----------
def prune_expired_blacklist():
    now_ts = int(time.time())
    redis_client.zremrangebyscore(BLACKLIST_ZSET, 0, now_ts)


def blacklist_proxy(proxy: str, cooldown: Optional[int] = None):
    if not proxy:
        return
    expire_ts = int(time.time()) + (cooldown or BLACKLIST_COOLDOWN_SEC)
    redis_client.zadd(BLACKLIST_ZSET, {proxy: expire_ts})
    logger.info(json.dumps({
        "event": "proxy_blacklisted",
        "proxy": proxy,
        "expires_at": datetime.fromtimestamp(expire_ts, tz=timezone.utc).isoformat()
    }))
    _record_metric("proxy_blacklisted", "global")


def is_blacklisted(proxy: str) -> bool:
    prune_expired_blacklist()
    return redis_client.zscore(BLACKLIST_ZSET, proxy) is not None


def available_proxies(pool: list) -> list:
    prune_expired_blacklist()
    return [p for p in pool if redis_client.zscore(BLACKLIST_ZSET, p) is None]


# ---------- Browser Workers ----------
def _wrap_browser_result(status: str, html: Optional[str], proxy: Optional[str] = None) -> Dict[str, Any]:
    return {
        "status": status,
        "bookmaker": "BROWSER_TASK",
        "matches": [],
        "proxy_used": proxy,
        "html": html,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@celery_app.task(queue="high_priority")
def run_selenium_task(url: str, user_agent: str, proxy: Optional[str] = None, timeout: int = 30):
    driver = None
    try:
        opts = Options()
        opts.add_argument("--headless=new")
        opts.add_argument(f"user-agent={user_agent}")
        if proxy:
            opts.add_argument(f"--proxy-server={proxy}")
        driver = webdriver.Chrome(options=opts)
        driver.set_page_load_timeout(timeout)
        driver.get(url)
        return _wrap_browser_result("OK", driver.page_source, proxy)
    except Exception:
        logger.exception("Selenium task failed")
        return _wrap_browser_result("ERROR", None, proxy)
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass


@celery_app.task(queue="high_priority")
def run_playwright_task(url: str, user_agent: str, proxy: Optional[str] = None, timeout: int = 30):
    browser = None
    try:
        with sync_playwright() as p:
            launch_kwargs = {"headless": True}
            if proxy:
                launch_kwargs["proxy"] = {"server": proxy}
            browser = p.chromium.launch(**launch_kwargs)

            context_kwargs = {"user_agent": user_agent}
            context = browser.new_context(**context_kwargs)

            page = context.new_page()
            page.goto(url, timeout=timeout * 1000)

            html = page.content()
            return _wrap_browser_result("OK", html, proxy)

    except Exception:
        logger.exception("Playwright task failed")
        return _wrap_browser_result("ERROR", None, proxy)

    finally:
        if browser:
            try:
                browser.close()
            except Exception:
                pass


# ---------- Fallback Processor ----------
@celery_app.task(queue="default")
def process_fallback_html(result: dict, scraper_module: str, scraper_class: str, proxy: Optional[str] = None):
    html = result.get("html") if isinstance(result, dict) else None
    if not html:
        return {
            "status": "NO_HTML",
            "bookmaker": scraper_class,
            "matches": [],
            "proxy_used": proxy,
            "fallback_used": True,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    try:
        module = importlib.import_module(scraper_module)
        cls = getattr(module, scraper_class)

        # ---- Smart init logic ----
        params = inspect.signature(cls).parameters
        if "proxy_list" in params:
            scraper = cls(proxy_list=[proxy] if proxy else [])
        elif "proxy" in params:
            scraper = cls(proxy=proxy)
        else:
            scraper = cls()
        # --------------------------

        soup = BeautifulSoup(html, "html.parser")

        parsed = scraper.parse_html(soup) if not inspect.iscoroutinefunction(scraper.parse_html) \
            else safe_async_run(scraper.parse_html(soup))

        matches = parsed or []
        status = "OK" if matches else "NO_MATCHES"

        out = {
            "status": status,
            "bookmaker": getattr(scraper, "bookmaker", scraper_class),
            "matches": matches,
            "proxy_used": proxy,
            "fallback_used": True,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        redis_client.setex(f"scraper:fallback:{scraper_class}", 3600, json.dumps(out))
        _record_metric("fallback", scraper_class)
        return out
    except Exception:
        logger.exception("Fallback processing failed")
        return {
            "status": "ERROR",
            "bookmaker": scraper_class,
            "matches": [],
            "proxy_used": proxy,
            "fallback_used": True,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }


# ---------- Main Scraper ----------
@celery_app.task(bind=True, max_retries=3, default_retry_delay=5, queue="default")
def run_scraper_task(self, scraper_module: str, scraper_class: str, proxy_pool: Optional[list] = None,
                     max_retries: int = 3, jitter: int = 5, fallback_timeout: int = 60):
    attempt = self.request.retries + 1
    proxy = None
    start_time = time.time()

    try:
        # Select proxy
        if proxy_pool:
            avail = available_proxies(proxy_pool)
            if not avail:
                raise RuntimeError("No available proxies")
            proxy = avail[self.request.retries % len(avail)]
            logger.info(json.dumps({"event": "proxy_selected", "proxy": proxy}))

        # Load scraper
        module = importlib.import_module(scraper_module)
        cls = getattr(module, scraper_class)

        # ---- Smart init logic (prefer proxy_list > proxy > none) ----
        params = inspect.signature(cls).parameters
        if "proxy_list" in params:
            scraper = cls(proxy_list=[proxy] if proxy else [])
        elif "proxy" in params:
            scraper = cls(proxy=proxy)
        else:
            scraper = cls()
        # ------------------------------------------------------------

        bookmaker = getattr(scraper, "bookmaker", scraper_class)

        logger.info(json.dumps({
            "event": "scraper_attempt",
            "bookmaker": bookmaker,
            "attempt": attempt,
            "max_retries": max_retries,
            "proxy": proxy,
        }))

        # Execute scraper (supports get_multiple_odds + async)
        if hasattr(scraper, "get_multiple_odds"):
            result = safe_async_run(scraper.get_multiple_odds()) if getattr(scraper, "is_async", False) \
                else scraper.get_multiple_odds()
        else:
            result = safe_async_run(scraper.get_odds()) if getattr(scraper, "is_async", False) \
                else scraper.get_odds()

        if not isinstance(result, list):
            raise ValueError("Invalid scraper result (expected list)")

        latency = time.time() - start_time
        _record_metric("success", bookmaker)
        _record_metric("latency_ms", bookmaker, int(latency * 1000))

        if not result:
            logger.warning(json.dumps({"event": "zero_matches", "bookmaker": bookmaker}))

        # --- Push detailed scraper metrics if available ---
        try:
            if hasattr(scraper, "metrics_snapshot"):
                metrics = scraper.metrics_snapshot()
                for k, v in metrics.items():
                    if isinstance(v, (int, float)):
                        _record_metric(k, bookmaker, int(v))
        except Exception:
            logger.warning(json.dumps({"event": "metrics_snapshot_failed", "bookmaker": bookmaker}))

        return {
            "status": "OK",
            "bookmaker": bookmaker,
            "matches": result,
            "proxy_used": proxy,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    except Exception as exc:
        logger.exception(f"[{scraper_class}] Attempt {attempt} failed")

        if proxy:
            blacklist_proxy(proxy)

        # Retry with exponential backoff
        if attempt < max_retries:
            delay = max(1, int((2 ** attempt) + random.uniform(0, jitter)))
            raise self.retry(exc=exc, countdown=delay)

        # Final attempt → fallback chain
        module = importlib.import_module(scraper_module)
        cls = getattr(module, scraper_class)
        if getattr(cls, "supports_browser_fallback", True):
            fallback_url = getattr(cls, "fallback_url", None) or getattr(scraper, "base_url", None)
            user_agent = getattr(cls, "user_agent", getattr(scraper, "ua", "Mozilla/5.0"))
            if fallback_url:
                logger.warning(json.dumps({
                    "event": "fallback_scheduled",
                    "scraper_class": scraper_class,
                    "fallback_url": fallback_url,
                    "proxy": proxy,
                }))
                chain(
                    run_playwright_task.s(fallback_url, user_agent, proxy, fallback_timeout),
                    process_fallback_html.s(scraper_module, scraper_class, proxy),
                ).apply_async()

        _record_metric("failure", scraper_class)
        return {
            "status": "FALLBACK_SCHEDULED",
            "bookmaker": scraper_class,
            "matches": [],
            "proxy_used": proxy,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
