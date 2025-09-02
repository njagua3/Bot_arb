#scrapers/mvp_scraper.py
import time
import json
import random
import logging
import asyncio
import itertools
from contextlib import asynccontextmanager, contextmanager
from statistics import mean

import requests
import httpx
from requests.adapters import HTTPAdapter, Retry
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.chrome.options import Options

from playwright.async_api import async_playwright

from utils.match_utils import build_match_dict

import asyncio
import inspect
from typing import List, Union


class ScraperOrchestrator:
    def __init__(self, scrapers: List[Union["BaseScraper", "AsyncBaseScraper"]]):
        self.scrapers = scrapers

    def run(self):
        """Runs all scrapers, mixing sync and async seamlessly."""
        sync_scrapers = [s for s in self.scrapers if not inspect.iscoroutinefunction(s.get_odds)]
        async_scrapers = [s for s in self.scrapers if inspect.iscoroutinefunction(s.get_odds)]

        results = []

        # --- run sync scrapers sequentially ---
        for scraper in sync_scrapers:
            try:
                matches = scraper.get_odds()
                results.extend(matches)
            except Exception as e:
                scraper.log("scraper_failed", error=str(e))

        # --- run async scrapers concurrently ---
        if async_scrapers:
            async def run_async_scrapers():
                all_results = []
                tasks = []
                for scraper in async_scrapers:
                    tasks.append(self._run_async_scraper(scraper))
                grouped = await asyncio.gather(*tasks, return_exceptions=True)
                for r in grouped:
                    if isinstance(r, list):
                        all_results.extend(r)
                return all_results

            async_results = asyncio.run(run_async_scrapers())
            results.extend(async_results)

        return results

    async def _run_async_scraper(self, scraper: "AsyncBaseScraper"):
        try:
            async with scraper:
                return await scraper.get_odds()
        except Exception as e:
            scraper.log("scraper_failed", error=str(e))
            return []

# -------------------
# LOGGING CONFIG
# -------------------
logger = logging.getLogger("scraper")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(message)s"))
logger.addHandler(handler)


# -------------------
# PROXY POOL W/ LATENCY
# -------------------
class ProxyPool:
    def __init__(self, proxies: list[str], max_failures: int = 3):
        self.proxies = itertools.cycle(proxies) if proxies else None
        self.fail_counts = {p: 0 for p in proxies} if proxies else {}
        self.latencies = {p: [] for p in proxies} if proxies else {}
        self.max_failures = max_failures

    def get(self) -> dict | None:
        if not self.proxies:
            return None
        # simple bias: sort by avg latency (low = preferred)
        ranked = sorted(self.latencies.items(), key=lambda kv: mean(kv[1]) if kv[1] else float("inf"))
        for proxy, _ in ranked:
            if self.fail_counts[proxy] < self.max_failures:
                return {"http": proxy, "https": proxy}
        return None

    def mark_failed(self, proxy: dict):
        if not proxy:
            return
        url = proxy["http"]
        self.fail_counts[url] += 1

    def mark_latency(self, proxy: dict, duration: float):
        if not proxy:
            return
        url = proxy["http"]
        self.latencies[url].append(duration)
        if len(self.latencies[url]) > 20:  # keep sliding window
            self.latencies[url].pop(0)


# -------------------
# BASE (SYNC)
# -------------------
class BaseScraper:
    def __init__(self, bookmaker: str, base_url: str, proxy_list=None,
                 max_retries: int = 3, sleep_between_requests: float = 1.0):
        self.bookmaker = bookmaker
        self.base_url = base_url
        self.max_retries = max_retries
        self.sleep_between_requests = sleep_between_requests

        self.proxy_pool = ProxyPool(proxy_list or [], max_failures=3)
        self.ua = self.get_random_user_agent()

        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": self.ua,
            "Accept-Language": "en-US,en;q=0.9",
        })

        self.metrics = {"requests_made": 0, "failed_requests": 0, "matches_collected": 0}

    # -------------------
    # LOGGING
    # -------------------
    def log(self, event: str, **kwargs):
        logger.info(json.dumps({"event": event, "bookmaker": self.bookmaker, **kwargs}))

    # -------------------
    # UTILS
    # -------------------
    def get_random_user_agent(self):
        return random.choice([
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/114.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Safari/605.1.15",
            "Mozilla/5.0 (X11; Linux x86_64) Chrome/113.0",
        ])

    def with_retries(self, func):
        def wrapper(*args, **kwargs):
            proxy = kwargs.get("proxies") or self.proxy_pool.get()
            for attempt in range(self.max_retries):
                try:
                    start = time.time()
                    result = func(*args, proxies=proxy, **kwargs)
                    if result:
                        if proxy:
                            self.proxy_pool.mark_latency(proxy, time.time() - start)
                        return result
                except Exception as e:
                    self.metrics["failed_requests"] += 1
                    self.log("retry_failed", attempt=attempt+1, error=str(e))
                    time.sleep(2 ** attempt + random.random())

            # Retries exhausted → mark proxy failed
            if proxy:
                self.proxy_pool.mark_failed(proxy)
            return None
        return wrapper

    # -------------------
    # SYNC SCRAPING
    # -------------------
    @property
    def try_api(self):
        @self.with_retries
        def _impl(endpoint: str, proxies=None):
            self.metrics["requests_made"] += 1
            resp = self.session.get(endpoint, proxies=proxies, timeout=15)
            resp.raise_for_status()
            return resp.json() if "json" in resp.headers.get("content-type", "") else None
        return _impl

    @property
    def try_static_html(self):
        @self.with_retries
        def _impl(url: str, proxies=None):
            self.metrics["requests_made"] += 1
            resp = self.session.get(url, proxies=proxies, timeout=15)
            resp.raise_for_status()
            return BeautifulSoup(resp.text, "html.parser")
        return _impl

    @property
    def try_browser(self):
        @self.with_retries
        def _impl(url: str, proxies=None):
            opts = Options()
            opts.add_argument("--headless=new")
            opts.add_argument(f"user-agent={self.ua}")
            if proxies and "http" in proxies:
                opts.add_argument(f"--proxy-server={proxies['http']}")

            driver = webdriver.Chrome(options=opts)
            driver.get(url)
            time.sleep(random.uniform(2, 5))  # anti-bot wait
            html = driver.page_source
            driver.quit()
            return html
        return _impl

    # -------------------
    # PAGINATION HOOK
    # -------------------
    def paginate_hook(self, soup_or_page):
        """Subclasses can override to scroll/click if needed."""
        return soup_or_page

    # -------------------
    # NORMALIZATION
    # -------------------
    def normalize_match(self, home, away, start_time, market, odds):
        self.metrics["matches_collected"] += 1
        return build_match_dict(home, away, start_time, market, odds, self.bookmaker)

    # -------------------
    # MAIN SCRAPING ENTRY
    # -------------------
    def get_odds(self, api_endpoint=None, sport_path=""):
        matches = []

        if api_endpoint:
            data = self.try_api(api_endpoint)
            if data:
                try:
                    matches.extend(self.parse_api(data))
                except Exception as e:
                    self.log("parse_api_failed", error=str(e))

        if not matches:
            soup = self.try_static_html(self.base_url + sport_path)
            if soup:
                soup = self.paginate_hook(soup)
                try:
                    matches.extend(self.parse_html(soup))
                except Exception as e:
                    self.log("parse_html_failed", error=str(e))

        if not matches:
            page = self.try_browser(self.base_url + sport_path)
            if page:
                soup = self.paginate_hook(BeautifulSoup(page, "html.parser"))
                try:
                    matches.extend(self.parse_html(soup))
                except Exception as e:
                    self.log("parse_html_failed", error=str(e))

        return matches

    # -------------------
    # MULTIPLE ENDPOINTS (SYNC)
    # -------------------
    def get_multiple_odds(self, api_endpoints: list[str] = None, sport_paths: list[str] = None):
        """Scrape multiple endpoints sequentially for consistency with async version."""
        matches = []

        if api_endpoints:
            for ep in api_endpoints:
                try:
                    matches.extend(self.get_odds(api_endpoint=ep))
                except Exception as e:
                    self.log("multi_api_failed", endpoint=ep, error=str(e))

        if sport_paths:
            for sp in sport_paths:
                try:
                    matches.extend(self.get_odds(sport_path=sp))
                except Exception as e:
                    self.log("multi_path_failed", sport_path=sp, error=str(e))

        return matches

    # -------------------
    # Hooks to override
    # -------------------
    def parse_api(self, data): return []
    def parse_html(self, soup): return []

class AsyncBaseScraper:
    """
    Async scraper with:
      - rate limiting (requests_per_minute)
      - per-endpoint circuit breaker (failure_threshold, recovery_timeout)
      - retries with exponential backoff
      - proxy rotation & latency tracking (via self.proxy_pool)
      - Playwright lifecycle (use `async with scraper:`)
    """

    def __init__(
        self,
        bookmaker: str,
        base_url: str,
        proxy_list: Optional[List[str]] = None,
        max_retries: int = 3,
        requests_per_minute: int = 60,
        failure_threshold: int = 5,
        recovery_timeout: int = 60,
    ):
        self.bookmaker = bookmaker
        self.base_url = base_url
        self.max_retries = max_retries

        # proxy_pool expected to be defined in your module (same interface as earlier)
        self.proxy_pool = ProxyPool(proxy_list or [], max_failures=3)

        # user-agent
        self.ua = random.choice([
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/114.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Safari/605.1.15",
        ])

        # httpx async client
        self.client = httpx.AsyncClient(
            headers={"User-Agent": self.ua, "Accept-Language": "en-US,en;q=0.9"},
            timeout=15,
        )

        # Playwright lifecycle placeholders
        self._playwright = None
        self._browser = None

        # metrics
        self.metrics = {"requests_made": 0, "failed_requests": 0, "matches_collected": 0}

        # rate limiter: minimum interval between requests for this instance
        self.requests_per_minute = requests_per_minute
        self._min_interval = 60.0 / max(1, requests_per_minute)
        self._rate_lock = asyncio.Lock()
        self._last_request_ts = 0.0

        # circuit breaker store: key -> {fail_count, state, opened_at}
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self._cb_store: Dict[str, Dict[str, float]] = {}  # e.g. {"endpoint": {"fail_count":0,"state":"closed","opened_at":0.0}}

    # --------------------
    # async context manager for Playwright
    # --------------------
    async def __aenter__(self):
        self._playwright = await async_playwright().start()
        self._browser = await self._playwright.chromium.launch(headless=True)
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.cleanup()

    async def cleanup(self):
        try:
            await self.client.aclose()
        except Exception:
            pass
        if self._browser:
            try:
                await self._browser.close()
            except Exception:
                pass
        if self._playwright:
            try:
                await self._playwright.stop()
            except Exception:
                pass

    # --------------------
    # logging helper
    # --------------------
    def log(self, event: str, **kwargs):
        logger.info(json.dumps({"event": event, "bookmaker": self.bookmaker, **kwargs}))

    # --------------------
    # rate limiting helper
    # --------------------
    async def _acquire_rate_slot(self):
        async with self._rate_lock:
            now = asyncio.get_event_loop().time()
            elapsed = now - self._last_request_ts
            wait_for = self._min_interval - elapsed
            if wait_for > 0:
                await asyncio.sleep(wait_for)
            self._last_request_ts = asyncio.get_event_loop().time()

    # --------------------
    # circuit breaker helpers
    # --------------------
    def _cb_get(self, key: str) -> Dict[str, float]:
        r = self._cb_store.get(key)
        if not r:
            r = {"fail_count": 0, "state": "closed", "opened_at": 0.0}
            self._cb_store[key] = r
        return r

    def _cb_allow(self, key: str) -> bool:
        r = self._cb_get(key)
        state = r["state"]
        if state == "closed":
            return True
        if state == "open":
            # if timeout passed -> half-open trial
            if asyncio.get_event_loop().time() - r["opened_at"] >= self.recovery_timeout:
                r["state"] = "half-open"
                return True
            return False
        if state == "half-open":
            return True
        return True

    def _cb_record_success(self, key: str):
        r = self._cb_get(key)
        r["fail_count"] = 0
        r["state"] = "closed"
        r["opened_at"] = 0.0

    def _cb_record_failure(self, key: str):
        r = self._cb_get(key)
        r["fail_count"] += 1
        if r["fail_count"] >= self.failure_threshold:
            r["state"] = "open"
            r["opened_at"] = asyncio.get_event_loop().time()
            self.log("circuit_opened", endpoint=key, fail_count=r["fail_count"])

    # --------------------
    # retry wrapper (async)
    # --------------------
    def with_retries(self, coro_fn):
        async def wrapped(*args, **kwargs):
            # identify cb_key: allow kwargs override, else use first arg (endpoint/url)
            cb_key = kwargs.get("cb_key") or (args[0] if args else "default")
            if cb_key is None:
                cb_key = "default"

            # circuit breaker check
            if not self._cb_allow(cb_key):
                self.log("circuit_blocked", endpoint=cb_key)
                return None

            proxy = kwargs.get("proxies") or self.proxy_pool.get()
            last_exc = None
            for attempt in range(1, self.max_retries + 1):
                try:
                    await self._acquire_rate_slot()
                    start = asyncio.get_event_loop().time()
                    result = await coro_fn(*args, proxies=proxy, **kwargs)
                    duration = asyncio.get_event_loop().time() - start
                    if proxy:
                        self.proxy_pool.mark_latency(proxy, duration)
                    if result is not None:
                        # success -> reset circuit
                        self._cb_record_success(cb_key)
                        return result
                except Exception as e:
                    last_exc = e
                    self.metrics["failed_requests"] += 1
                    self.log("retry_failed", attempt=attempt, error=str(e), endpoint=cb_key)
                    await asyncio.sleep(2 ** (attempt - 1) + random.random())

            # all retries exhausted
            if proxy:
                self.proxy_pool.mark_failed(proxy)
            self._cb_record_failure(cb_key)
            self.log("retries_exhausted", endpoint=cb_key, error=str(last_exc) if last_exc else None)
            return None
        return wrapped

    # --------------------
    # async scraping primitives (wrapped)
    # --------------------
    @property
    def try_api(self):
        @self.with_retries
        async def _impl(endpoint: str, proxies=None, **kwargs):
            self.metrics["requests_made"] += 1
            resp = await self.client.get(endpoint, proxies=proxies)
            resp.raise_for_status()
            return resp.json() if "json" in resp.headers.get("content-type", "") else None
        return _impl

    @property
    def try_static_html(self):
        @self.with_retries
        async def _impl(url: str, proxies=None, **kwargs):
            self.metrics["requests_made"] += 1
            resp = await self.client.get(url, proxies=proxies)
            resp.raise_for_status()
            html = resp.text
            # optional: detect captcha and try solver (override hooks)
            if self.captcha_detect_hook(mode="http", html=html, url=url):
                self.log("captcha_detected", mode="http", url=url)
                solved = await self.captcha_solve_hook(mode="http", html=html, url=url, proxy=proxies)
                if solved:
                    return BeautifulSoup(solved, "html.parser")
                else:
                    # return None so wrapper will retry another proxy
                    return None
            return BeautifulSoup(html, "html.parser")
        return _impl

    @property
    def try_browser(self):
        @self.with_retries
        async def _impl(url: str, proxies=None, **kwargs):
            if not self._browser:
                raise RuntimeError("Browser not initialized. Use `async with scraper:`")

            pw_proxy = {"server": proxies["http"]} if proxies else None
            context = await self._browser.new_context(user_agent=self.ua, proxy=pw_proxy)
            page = await context.new_page()
            await page.goto(url, timeout=15000)
            await asyncio.sleep(random.uniform(1.5, 3.5))
            html = await page.content()

            # captcha handling in browser mode
            if self.captcha_detect_hook(mode="browser", html=html, url=url, page=page):
                self.log("captcha_detected", mode="browser", url=url)
                solved = await self.captcha_solve_hook(mode="browser", page=page, url=url, proxy=proxies)
                if not solved:
                    await context.close()
                    return None
                # if solved, re-read page
                await asyncio.sleep(1.0)
                html = await page.content()

            await context.close()
            return html
        return _impl

    # --------------------
    # pagination hook (override)
    # --------------------
    async def paginate_hook(self, soup_or_page):
        return soup_or_page

    # --------------------
    # captcha hooks (override)
    # --------------------
    def captcha_detect_hook(self, mode: str, **ctx) -> bool:
        html = ctx.get("html", "") or ""
        if not html:
            return False
        needles = ("captcha", "recaptcha", "hcaptcha", "g-recaptcha", "cf-challenge", "are you a robot")
        return any(n in html.lower() for n in needles)

    async def captcha_solve_hook(self, mode: str, **ctx) -> Optional[bool]:
        """
        Default: no-op. Override:
         - mode == "http": return solved HTML string or None
         - mode == "browser": perform actions on `page` and return True/False
        """
        return None if mode == "http" else False

    # --------------------
    # normalization / parse stubs
    # --------------------
    def normalize_match(self, home, away, start_time, market, odds):
        self.metrics["matches_collected"] += 1
        return build_match_dict(home, away, start_time, market, odds, self.bookmaker)

    async def parse_api(self, data): return []
    async def parse_html(self, soup): return []

    # --------------------
    # orchestrator-friendly get_odds
    # --------------------
    async def get_odds(self, api_endpoint=None, sport_path=""):
        matches = []

        # 1) API
        if api_endpoint:
            data = await self.try_api(api_endpoint)
            if data:
                try:
                    parsed = await self.parse_api(data)
                    if parsed:
                        matches.extend(parsed)
                except Exception as e:
                    self.log("parse_api_failed", error=str(e))

        # 2) static html
        if not matches:
            url = f"{self.base_url}{sport_path}"
            soup = await self.try_static_html(url)
            if soup:
                try:
                    paged = await self.paginate_hook(soup)
                    parsed = await self.parse_html(paged)
                    if parsed:
                        matches.extend(parsed)
                except Exception as e:
                    self.log("parse_html_failed", error=str(e))

        # 3) browser fallback
        if not matches:
            url = f"{self.base_url}{sport_path}"
            html = await self.try_browser(url)
            if html:
                try:
                    paged = await self.paginate_hook(BeautifulSoup(html, "html.parser"))
                    parsed = await self.parse_html(paged)
                    if parsed:
                        matches.extend(parsed)
                except Exception as e:
                    self.log("parse_html_failed", error=str(e))

        return matches