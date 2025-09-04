import time
import json
import random
import logging
from typing import Optional, Dict, List

import requests
from bs4 import BeautifulSoup

from utils.match_utils import build_match_dict
from .proxy_pool import ProxyPool

logger = logging.getLogger("scraper")
logger.setLevel(logging.INFO)
_handler = logging.StreamHandler()
_handler.setFormatter(logging.Formatter("%(message)s"))
logger.addHandler(_handler)


class BaseScraper:
    bookmaker: str = "GenericBookmaker"
    supports_browser_fallback: bool = True  # default for all scrapers

    def __init__(
        self,
        base_url: str,
        proxy_list: Optional[List[str]] = None,
        max_retries: int = 3,
        sleep_between_requests: float = 1.0,
        cache_ttl: int = 60,
        bookmaker: Optional[str] = None,
        request_timeout: float = 20.0,
    ):
        self.bookmaker = bookmaker or self.bookmaker
        self.base_url = base_url.rstrip("/")
        self.max_retries = max_retries
        self.sleep_between_requests = sleep_between_requests
        self.cache_ttl = cache_ttl
        self.request_timeout = request_timeout

        self.proxy_pool = ProxyPool(proxy_list or [], max_failures=3)

        # Session is kept, but UA will be rotated per attempt via per-request headers.
        self.session = requests.Session()
        self.session.headers.update({"Accept-Language": "en-US,en;q=0.9"})

        # Simple in-process cache: {key: (value, ts)}
        self.cache: Dict[str, tuple] = {}

        # basic metrics
        self.metrics = {
            "requests_made": 0,
            "failed_requests": 0,
            "successful_requests": 0,
            "matches_collected": 0,
        }

    # -----------------------------
    # Logging
    # -----------------------------
    def log(self, event: str, level: str = "info", **kwargs):
        message = json.dumps({"event": event, "bookmaker": self.bookmaker, **kwargs})
        getattr(logger, level, logger.info)(message)

    # -----------------------------
    # User-Agent rotation
    # -----------------------------
    def get_random_user_agent(self) -> str:
        return random.choice([
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/114.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Safari/605.1.15",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/114.0",
            "Mozilla/5.0 (X11; Linux x86_64) Chrome/113.0",
        ])

    # -----------------------------
    # Simple cache helpers
    # -----------------------------
    def _cache_get(self, key: str):
        entry = self.cache.get(key)
        if not entry:
            return None
        value, ts = entry
        if time.time() - ts < self.cache_ttl:
            # lightweight touch: refresh timestamp to keep hot entries warm
            self.cache[key] = (value, time.time())
            return value
        # expired
        self.cache.pop(key, None)
        return None

    def _cache_set(self, key: str, value):
        self.cache[key] = (value, time.time())

    # -----------------------------
    # Retry wrapper
    #   1) Proxy handling per attempt (mark failures immediately, rotate next)
    #   2) Mark proxy success on good requests
    #   3) Better logging/metrics
    #   4) Rotate UA per attempt
    # -----------------------------
    def with_retries(self, func):
        def wrapper(*args, **kwargs):
            # choose initial proxy, then rotate with .next() on each failure
            proxy = self.proxy_pool.get()
            last_exc = None

            for attempt in range(1, self.max_retries + 1):
                # 4) rotate UA per attempt (per-request headers)
                ua = self.get_random_user_agent()
                headers = kwargs.pop("headers", None) or {}
                headers = {"User-Agent": ua, **headers}
                kwargs["headers"] = headers

                try:
                    if self.sleep_between_requests > 0:
                        time.sleep(self.sleep_between_requests)

                    start = time.perf_counter()
                    # pass proxies to func (requests expects dict or None)
                    result = func(*args, proxies=proxy, **kwargs)
                    duration = time.perf_counter() - start

                    if result is not None:
                        # 2) success: mark proxy healthy, record latency & metrics
                        if proxy:
                            self.proxy_pool.mark_success(proxy)
                            self.proxy_pool.mark_latency(proxy, duration)
                        self.metrics["successful_requests"] += 1
                        self.log(
                            "request_success",
                            url_or_endpoint=args[0] if args else None,
                            attempt=attempt,
                            duration_ms=int(duration * 1000),
                            proxy=proxy["http"] if proxy else None,
                        )
                        return result

                    # Treat empty result as soft failure
                    raise RuntimeError("Empty result")

                except Exception as e:
                    last_exc = e
                    self.metrics["failed_requests"] += 1
                    # 1) mark failed immediately on each failed attempt
                    if proxy:
                        self.proxy_pool.mark_failed(proxy)

                    self.log(
                        "request_attempt_failed",
                        level="warning",
                        attempt=attempt,
                        error=str(e),
                        proxy=proxy["http"] if proxy else None,
                    )

                    # backoff with jitter
                    sleep_for = (2 ** (attempt - 1)) + random.uniform(0, 0.75)
                    time.sleep(sleep_for)

                    # rotate proxy for next attempt
                    proxy = self.proxy_pool.next()

            # All retries exhausted
            self.log(
                "request_retries_exhausted",
                level="error",
                error=str(last_exc) if last_exc else None,
            )
            return None

        return wrapper

    # -----------------------------
    # HTTP helpers
    # -----------------------------
    @property
    def try_api(self):
        @self.with_retries
        def _impl(endpoint: str, proxies: Optional[Dict[str, str]] = None, headers: Optional[Dict[str, str]] = None):
            cache_key = f"api:{endpoint}"
            cached = self._cache_get(cache_key)
            if cached is not None:
                return cached

            self.metrics["requests_made"] += 1
            resp = self.session.get(endpoint, proxies=proxies, headers=headers, timeout=self.request_timeout)
            resp.raise_for_status()

            ctype = (resp.headers.get("content-type") or "").lower()
            if "application/json" in ctype or ctype.endswith("+json"):
                data = resp.json()
                self._cache_set(cache_key, data)
                return data
            return None
        return _impl

    @property
    def try_static_html(self):
        @self.with_retries
        def _impl(url: str, proxies: Optional[Dict[str, str]] = None, headers: Optional[Dict[str, str]] = None):
            cache_key = f"html:{url}"
            cached = self._cache_get(cache_key)
            if cached is not None:
                return cached

            self.metrics["requests_made"] += 1
            resp = self.session.get(url, proxies=proxies, headers=headers, timeout=self.request_timeout)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")
            self._cache_set(cache_key, soup)
            return soup
        return _impl

    # -----------------------------
    # Browser fallback (Selenium) with lazy import + safe cleanup
    # -----------------------------
    def try_browser(self, url: str, proxies: Optional[Dict[str, str]] = None, use_queue: bool = False) -> Optional[str]:
        if use_queue:
            # Placeholder: integrate with Celery task queue in tasks.py
            self.log("browser_task_queued", url=url)
            return None

        # Lazy import so environments without selenium don’t break
        try:
            from selenium import webdriver
            from selenium.webdriver.chrome.options import Options
        except Exception as e:
            self.log("selenium_import_failed", level="error", error=str(e))
            return None

        driver = None
        try:
            opts = Options()
            opts.add_argument("--headless=new")
            opts.add_argument(f"user-agent={self.get_random_user_agent()}")
            if proxies and "http" in proxies:
                opts.add_argument(f"--proxy-server={proxies['http']}")

            driver = webdriver.Chrome(options=opts)
            driver.set_page_load_timeout(int(self.request_timeout))
            driver.get(url)
            # small human-like delay to let dynamic content settle
            time.sleep(random.uniform(1.5, 3.0))
            html = driver.page_source
            self.log("browser_success", url=url, proxy=proxies["http"] if proxies else None)
            return html

        except Exception as e:
            self.metrics["failed_requests"] += 1
            self.log("browser_failed", level="error", url=url, error=str(e))
            return None

        finally:
            try:
                if driver:
                    driver.quit()
            except Exception:
                # swallow cleanup errors
                pass

    # -----------------------------
    # Hooks (override in subclasses)
    # -----------------------------
    def solve_captcha(self, page_content):  # placeholder
        return page_content

    def handle_cloudflare(self, url):  # placeholder
        return None

    def paginate_hook(self, soup_or_page):
        return soup_or_page

    # -----------------------------
    # Normalization helper
    # -----------------------------
    def normalize_match(self, home, away, start_time, market, odds):
        self.metrics["matches_collected"] += 1
        return build_match_dict(home, away, start_time, market, odds, self.bookmaker)

    # -----------------------------
    # Orchestration: API -> Static HTML -> Browser
    # -----------------------------
    def get_odds(self, api_endpoint: Optional[str] = None, sport_path: str = "", use_browser: bool = False):
        matches = []

        # 1) Try API
        if api_endpoint:
            data = self.try_api(api_endpoint)
            if data:
                try:
                    matches.extend(self.parse_api(data))
                except Exception as e:
                    self.log("parse_api_failed", level="error", error=str(e))

        # 2) Try static HTML
        if not matches:
            url = f"{self.base_url}{sport_path}"
            soup = self.try_static_html(url)
            if soup:
                soup = self.paginate_hook(soup)
                try:
                    matches.extend(self.parse_html(soup))
                except Exception as e:
                    self.log("parse_html_failed", level="error", error=str(e))

        # 3) Browser fallback
        if not matches and use_browser:
            url = f"{self.base_url}{sport_path}"
            html = self.try_browser(url)
            if html:
                soup = self.paginate_hook(BeautifulSoup(html, "html.parser"))
                try:
                    matches.extend(self.parse_html(soup))
                except Exception as e:
                    self.log("parse_html_failed", level="error", error=str(e))

        return matches

    def get_multiple_odds(
        self,
        api_endpoints: Optional[List[str]] = None,
        sport_paths: Optional[List[str]] = None
    ):
        matches: List[dict] = []

        if api_endpoints:
            for ep in api_endpoints:
                try:
                    matches.extend(self.get_odds(api_endpoint=ep))
                except Exception as e:
                    self.log("multi_api_failed", level="error", endpoint=ep, error=str(e))

        if sport_paths:
            for sp in sport_paths:
                try:
                    matches.extend(self.get_odds(sport_path=sp))
                except Exception as e:
                    self.log("multi_path_failed", level="error", sport_path=sp, error=str(e))

        return matches

    # To be implemented by subclasses
    def parse_api(self, data): return []
    def parse_html(self, soup): return []
