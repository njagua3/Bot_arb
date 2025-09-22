# scrapers/async_base_scraper.py
import asyncio
import json
import logging
import random
import time
from collections import OrderedDict, defaultdict
from typing import List, Dict, Optional, Callable, Tuple, Any
from core.db import resolve_bookmaker_id
import httpx
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, Browser, Error as PWError

from scrapers.proxy_pool import ProxyPool
from utils.match_utils import build_match_dict

# --- Structured JSON Logger ---
logger = logging.getLogger("async_base_scraper")
handler = logging.StreamHandler()
formatter = logging.Formatter(
    json.dumps({
        "time": "%(asctime)s",
        "level": "%(levelname)s",
        "message": "%(message)s",
        "name": "%(name)s",
    })
)
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)


class AsyncBaseScraper:
    CONTEXT_TTL_SEC: int = 180
    CONTEXT_CACHE_MAX: int = 6

    USER_AGENTS = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Safari/605.1.15",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0 Safari/537.36",
        "Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Mobile/15E148 Safari/604.1",
        "Mozilla/5.0 (Linux; Android 11; Pixel 5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0 Mobile Safari/537.36",
    ]

    DEFAULT_DIRECT_UA = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/114.0 Safari/537.36"
    )

    BROWSER_PROFILES: List[Dict[str, Any]] = [
        {"locale": "en-US", "timezone_id": "America/New_York", "viewport": {"width": 1366, "height": 768}},
        {"locale": "en-US", "timezone_id": "America/Los_Angeles", "viewport": {"width": 1440, "height": 900}},
        {"locale": "en-GB", "timezone_id": "Europe/London", "viewport": {"width": 1920, "height": 1080}},
        {"locale": "de-DE", "timezone_id": "Europe/Berlin", "viewport": {"width": 1536, "height": 864}},
        {"locale": "en-KE", "timezone_id": "Africa/Nairobi", "viewport": {"width": 1600, "height": 900}},
        {"locale": "en-US", "timezone_id": "America/Chicago", "viewport": {"width": 390, "height": 844}, "mobile": True},
        {"locale": "en-US", "timezone_id": "America/New_York", "viewport": {"width": 412, "height": 915}, "mobile": True},
    ]

    TRANSIENT_STATUSES = {429, 500, 502, 503, 504, 520, 521, 522, 524}

    def __init__(self,
                 bookmaker: str,
                 base_url: str,
                 proxy_list: Optional[List[str]] = None,
                 max_retries: int = 3,
                 requests_per_minute: int = 60,
                 failure_threshold: int = 5,
                 recovery_timeout: int = 60,
                 rate_limiter: Optional[asyncio.Semaphore] = None,
                 http2: bool = False,
                 request_timeout: float = 20.0):
        self.bookmaker = bookmaker
        self.base_url = base_url.rstrip("/")
        self.default_max_retries = max_retries

        # ✅ NEW: resolve and cache bookmaker_id once (uses optional subclass attr `bookmaker_url`)
        try:
            self.bookmaker_id = resolve_bookmaker_id(self.bookmaker, getattr(self, "bookmaker_url", None))
            logger.info(json.dumps({
                "time": time.strftime("%Y-%m-%d %H:%M:%S"),
                "level": "INFO",
                "name": "async_base_scraper",
                "message": json.dumps({
                    "event": "bookmaker_resolved",
                    "bookmaker": self.bookmaker,
                    "bookmaker_id": self.bookmaker_id
                })
            }))
        except Exception as e:
            raise RuntimeError(f"Failed to resolve bookmaker_id for {self.bookmaker}: {e}")

        self.proxy_pool = ProxyPool(proxy_list or [], max_failures=3)
        self._direct_mode = not bool(proxy_list)
        self.ua = self.DEFAULT_DIRECT_UA if self._direct_mode else random.choice(self.USER_AGENTS)

        self.client: Optional[httpx.AsyncClient] = None
        self._http2 = http2
        self._request_timeout = request_timeout

        self._playwright = None
        self._browser: Optional[Browser] = None

        self._context_cache: "OrderedDict[str, Dict[str, Any]]" = OrderedDict()
        self._context_profiles: Dict[str, Dict[str, Any]] = {}
        self._context_ua: Dict[str, str] = {}

        self.metrics = {
            "requests_made": 0,
            "successful_requests": 0,
            "failed_requests": 0,
            "matches_collected": 0,
            "latency_histogram": [],
            "proxy_success": 0,
            "proxy_fail": 0,
            "endpoint_errors": defaultdict(int),
        }

        self.requests_per_minute = requests_per_minute
        self._min_interval = 60.0 / max(1, requests_per_minute)
        self._rate_lock = asyncio.Lock()
        self._shared_rate_limiter = rate_limiter
        self._last_request_ts = 0.0

        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self._cb_store: Dict[str, Dict[str, float]] = {}

    # ... rest of the class stays unchanged ...

    # --------------------
    # lifecycle
    # --------------------
    async def __aenter__(self):
        if not self.client:
            proxy = self.proxy_pool.get()
            if not proxy:
                self._direct_mode = True
                self.ua = self.DEFAULT_DIRECT_UA
                self.log("no_proxies_configured", level="warning",
                         message="Running without proxies — direct connection mode enabled")

            limits = httpx.Limits(max_connections=100, max_keepalive_connections=20, keepalive_expiry=30.0)

            headers = {
                "User-Agent": self.ua,
                "Accept": "application/json, text/html;q=0.9, */*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "Connection": "keep-alive",
            }

            self.client = httpx.AsyncClient(
                headers=headers,
                timeout=self._request_timeout,
                trust_env=False,
                http2=self._http2,
                proxies=proxy["http"] if proxy else None,
                follow_redirects=True,
                limits=limits,
            )
            self.log("httpx_client_created")

        # ✅ Only launch Playwright if this scraper actually needs it
        if getattr(self, "supports_browser_fallback", True):
            self._playwright = await async_playwright().start()
            self._browser = await self._playwright.chromium.launch(headless=True)
            self.log("browser_started")

        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.cleanup()

    async def cleanup(self):
        if self.client:
            try:
                await self.client.aclose()
                self.log("httpx_client_closed")
            except Exception as e:
                self.log("httpx_client_close_failed", level="warning", error=str(e))
            self.client = None

        for key, cached in list(self._context_cache.items()):
            try:
                await cached["context"].close()
                self.log("context_closed", context_key=key)
            except Exception as e:
                self.log("context_close_failed", level="warning", context_key=key, error=str(e))
        self._context_cache.clear()
        self._context_profiles.clear()
        self._context_ua.clear()

        if self._browser:
            try:
                await self._browser.close()
                self.log("browser_closed")
            except Exception as e:
                self.log("browser_close_failed", level="warning", error=str(e))
            self._browser = None

        if self._playwright:
            try:
                await self._playwright.stop()
                self.log("playwright_stopped")
            except Exception as e:
                self.log("playwright_stop_failed", level="warning", error=str(e))
            self._playwright = None

        self.log("cleanup_complete")

    # --------------------
    # logging helper
    # --------------------
    def log(self, event: str, level: str = "info", **kwargs):
        msg = {"event": event, "bookmaker": self.bookmaker, **kwargs}
        getattr(logger, level)(json.dumps(msg))

    # --------------------
    # rate limit helper
    # --------------------
    async def _rate_limit(self):
        if self._shared_rate_limiter is not None:
            async with self._shared_rate_limiter:
                await asyncio.sleep(0)
        async with self._rate_lock:
            now = time.perf_counter()
            wait = self._min_interval - (now - self._last_request_ts)
            if wait > 0:
                await asyncio.sleep(wait)
            self._last_request_ts = time.perf_counter()

    # --------------------
    # retry wrapper (respects Retry-After via _cb_store)
    # --------------------
    def with_retries(
        self,
        coro_fn: Callable[..., Any],
        retry_on: Tuple[type, ...] = (httpx.RequestError, PWError),
        max_retries: Optional[int] = None,
        backoff_base: float = 1.0,
        jitter: Tuple[float, float] = (0.5, 1.5),
        on_retry: Optional[Callable[[int, Exception, str], Any]] = None,
    ):
        async def wrapped(*args, **kwargs):
            cb_key = kwargs.get("cb_key") or (args[0] if args else "default")
            local_max_retries = max_retries or self.default_max_retries
            last_exc: Optional[Exception] = None

            for attempt in range(1, local_max_retries + 1):
                try:
                    if not self._direct_mode:
                        self.ua = random.choice(self.USER_AGENTS)
                        if self.client:
                            self.client.headers.update({"User-Agent": self.ua})

                    result = await coro_fn(*args, **kwargs)
                    if result is not None:
                        self.metrics["successful_requests"] += 1
                        return result
                    last_exc = last_exc or Exception("Transient empty result")
                    self.metrics["failed_requests"] += 1
                except retry_on as e:
                    last_exc = e
                    self.metrics["failed_requests"] += 1
                    if on_retry:
                        try:
                            on_retry(attempt, e, cb_key)
                        except Exception:
                            pass
                except Exception as e:
                    last_exc = e
                    break

                ra = None
                if cb_key in self._cb_store:
                    ra = self._cb_store[cb_key].pop("retry_after", None)
                    if not self._cb_store[cb_key]:
                        self._cb_store.pop(cb_key, None)

                if ra is not None:
                    delay = float(ra)
                else:
                    delay = (backoff_base * (2 ** (attempt - 1))) * random.uniform(*jitter)
                await asyncio.sleep(delay)

            self.log("retries_exhausted", level="error", endpoint=cb_key, error=str(last_exc) if last_exc else None)
            return None
        return wrapped

    # --------------------
    # try_api / try_static_html
    # --------------------
    @property
    def try_api(self):
        @self.with_retries
        async def _impl(endpoint: str, **kwargs):
            await self._rate_limit()
            self.metrics["requests_made"] += 1
            t0 = time.perf_counter()

            resp = await self.client.get(endpoint, timeout=self._request_timeout)
            dt = time.perf_counter() - t0
            self.metrics["latency_histogram"].append(dt)

            status = resp.status_code
            if status in self.TRANSIENT_STATUSES:
                ra = resp.headers.get("Retry-After")
                if ra and ra.isdigit():
                    self._cb_store.setdefault(kwargs.get("cb_key", f"api:{endpoint}"), {})["retry_after"] = float(ra)
                self.metrics["endpoint_errors"][f"{status}"] += 1
                return None

            try:
                resp.raise_for_status()
            except httpx.HTTPStatusError:
                self.metrics["endpoint_errors"][f"{status}"] += 1
                raise

            ctype = (resp.headers.get("content-type") or "").lower()
            if "json" in ctype:
                try:
                    return resp.json()
                except Exception as e:
                    self.log("json_decode_failed", level="error", endpoint=endpoint, error=str(e))
                    return None
            return None
        return _impl

    @property
    def try_static_html(self):
        @self.with_retries
        async def _impl(url: str, **kwargs):
            await self._rate_limit()
            self.metrics["requests_made"] += 1
            t0 = time.perf_counter()

            resp = await self.client.get(url, timeout=self._request_timeout)
            dt = time.perf_counter() - t0
            self.metrics["latency_histogram"].append(dt)

            status = resp.status_code
            if status in self.TRANSIENT_STATUSES:
                ra = resp.headers.get("Retry-After")
                if ra and ra.isdigit():
                    self._cb_store.setdefault(kwargs.get("cb_key", f"http:{url}"), {})["retry_after"] = float(ra)
                self.metrics["endpoint_errors"][f"{status}"] += 1
                return None

            resp.raise_for_status()
            html = resp.text
            return BeautifulSoup(html, "html.parser")
        return _impl

    # --------------------
    # Browser fallback (self-contained; no Celery import at module import time)
    # --------------------
    async def try_browser(self, url: str, **kwargs) -> Optional[str]:
        """
        Minimal Playwright fetch to get page HTML. Returns HTML string or None.
        """
        if not getattr(self, "supports_browser_fallback", True):
            return None
        if not self._browser or not self._playwright:
            # If someone toggled fallback on mid-flight, bring up the browser lazily
            self._playwright = await async_playwright().start()
            self._browser = await self._playwright.chromium.launch(headless=True)
            self.log("browser_started_late")

        page = await self._browser.new_page()
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=int(self._request_timeout * 1000))
            try:
                await page.wait_for_load_state("networkidle", timeout=5000)
            except Exception:
                pass
            content = await page.content()
            return content
        except Exception as e:
            self.log("browser_fetch_failed", level="warning", error=str(e), url=url)
            return None
        finally:
            try:
                await page.close()
            except Exception:
                pass

    # If you really need Celery-driven Playwright, lazy-load it:
    def run_playwright_task(self, *args, **kwargs):
        from scrapers.tasks import run_playwright_task as _rp
        return _rp(*args, **kwargs)

    # --------------------
    # hooks to override in subclasses
    # --------------------
    async def paginate_hook(self, soup_or_page):
        """
        Override in subclasses.
        - If called from static HTML path: receives BeautifulSoup, return soup (optionally transformed).
        - If called from browser path: receives Playwright Page; you can infinite-scroll or click-to-load and return None.
        """
        return soup_or_page

    def captcha_detect_hook(self, mode: str, **ctx) -> bool:
        html = ctx.get("html", "") or ""
        if not html:
            return False
        needles = ("captcha", "recaptcha", "hcaptcha", "g-recaptcha", "cf-challenge", "are you a robot")
        return any(n in html.lower() for n in needles)

    async def captcha_solve_hook(self, mode: str, **ctx) -> Optional[bool]:
        return None if mode == "http" else False

    # --------------------
    # normalize / parse stubs
    # --------------------
    def normalize_match(self, home, away, start_time, market, odds):
        self.metrics["matches_collected"] += 1
        return build_match_dict(home, away, start_time, market, odds, self.bookmaker)

    async def parse_api(self, data): return []
    async def parse_html(self, soup): return []

    # --------------------
    # orchestrator-friendly get_odds (API -> HTML -> Browser)
    # --------------------
    async def get_odds(self, api_endpoint: Optional[str] = None, sport_path: str = ""):
        matches = []

        # 1) API
        if api_endpoint:
            data = await self.try_api(api_endpoint, cb_key=f"api:{api_endpoint}")
            if data:
                try:
                    parsed = await self.parse_api(data)
                    if parsed:
                        matches.extend(parsed)
                except Exception as e:
                    self.log("parse_api_failed", level="error", error=str(e), endpoint=api_endpoint)

        # 2) static HTML
        if not matches:
            url = f"{self.base_url}{sport_path}"
            soup = await self.try_static_html(url, cb_key=f"http:{url}")
            if soup:
                try:
                    paged = await self.paginate_hook(soup)
                    parsed = await self.parse_html(paged)
                    if parsed:
                        matches.extend(parsed)
                except Exception as e:
                    self.log("parse_html_failed", level="error", error=str(e), endpoint=url)

        # 3) browser fallback
        if not matches:
            url = f"{self.base_url}{sport_path}"
            html = await self.try_browser(url, cb_key=f"browser:{url}")
            if html:
                try:
                    paged = await self.paginate_hook(BeautifulSoup(html, "html.parser"))
                    parsed = await self.parse_html(paged)
                    if parsed:
                        matches.extend(parsed)
                except Exception as e:
                    self.log("parse_html_failed", level="error", error=str(e), endpoint=url)

        return matches

    # --------------------
    # observability
    # --------------------
    def metrics_snapshot(self) -> Dict[str, Any]:
        try:
            pool_stats = self.proxy_pool.stats()
        except Exception:
            pool_stats = {}

        buckets = {"lt_0_2s": 0, "0_2_0_5s": 0, "0_5_1_0s": 0, "gt_1_0s": 0}
        for d in self.metrics["latency_histogram"]:
            if d < 0.2:
                buckets["lt_0_2s"] += 1
            elif d < 0.5:
                buckets["0_2_0_5s"] += 1
            elif d < 1.0:
                buckets["0_5_1_0s"] += 1
            else:
                buckets["gt_1_0s"] += 1

        return {
            "bookmaker": self.bookmaker,
            "requests_made": self.metrics["requests_made"],
            "successful_requests": self.metrics["successful_requests"],
            "failed_requests": self.metrics["failed_requests"],
            "matches_collected": self.metrics["matches_collected"],
            "proxy_success": self.metrics["proxy_success"],
            "proxy_fail": self.metrics["proxy_fail"],
            "endpoint_errors": dict(self.metrics["endpoint_errors"]),
            "latency_buckets": buckets,
            "proxy_pool": pool_stats,
        }

    # --------------------
    # contract compatibility with BaseScraper
    # --------------------
    supports_browser_fallback: bool = True

    async def get_multiple_odds(
        self,
        api_endpoints: Optional[List[str]] = None,
        sport_paths: Optional[List[str]] = None,
    ) -> List[dict]:
        """
        Batch wrapper around get_odds().
        Returns a flat list of matches across all endpoints/paths.
        """
        results: List[dict] = []
        api_endpoints = api_endpoints or []
        sport_paths = sport_paths or []

        tasks = []
        for api in api_endpoints:
            tasks.append(self.get_odds(api_endpoint=api))
        for path in sport_paths:
            tasks.append(self.get_odds(sport_path=path))

        gathered = await asyncio.gather(*tasks, return_exceptions=True)
        for res in gathered:
            if isinstance(res, Exception):
                self.log("get_multiple_odds_failed", level="error", error=str(res))
                continue
            if res:
                results.extend(res)

        return results
