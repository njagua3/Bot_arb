# scrapers/async_base_scraper.py
import asyncio
import json
import logging
import random
import time
from collections import OrderedDict, defaultdict
from typing import List, Dict, Optional, Callable, Tuple, Any

import httpx
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, Browser, Error as PWError

from scrapers.proxy_pool import ProxyPool
from utils.match_utils import build_match_dict
from scrapers.tasks import run_playwright_task  # Celery task


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
    """
    Async scraper with:
      - User-Agent rotation on every retry
      - rate limiting (shared semaphore or per-instance)
      - per-endpoint circuit breaker
      - retries with exponential backoff + jitter
      - proxy pool with latency/success tracking
      - httpx AsyncClient (lazy)
      - Playwright lifecycle with proxy-bound context cache
      - structured logging + metrics snapshot
    """

    CONTEXT_TTL_SEC: int = 180
    CONTEXT_CACHE_MAX: int = 6

    USER_AGENTS = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Safari/605.1.15",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0 Safari/537.36",
    ]

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

        # retry defaults
        self.default_max_retries = max_retries

        # proxy pool
        self.proxy_pool = ProxyPool(proxy_list or [], max_failures=3)

        # initial UA
        self.ua = random.choice(self.USER_AGENTS)

        # httpx client (lazy)
        self.client: Optional[httpx.AsyncClient] = None
        self._http2 = http2
        self._request_timeout = request_timeout

        # Playwright lifecycle
        self._playwright = None
        self._browser: Optional[Browser] = None

        # context cache (per-proxy)
        self._context_cache: "OrderedDict[str, Dict[str, Any]]" = OrderedDict()

        # metrics
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

        # rate limiting
        self.requests_per_minute = requests_per_minute
        self._min_interval = 60.0 / max(1, requests_per_minute)
        self._rate_lock = asyncio.Lock()
        self._shared_rate_limiter = rate_limiter
        self._last_request_ts = 0.0

        # circuit breaker
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self._cb_store: Dict[str, Dict[str, float]] = {}

    # --------------------
    # lifecycle
    # --------------------
    async def __aenter__(self):
        if not self.client:
            self.client = httpx.AsyncClient(
                headers={"User-Agent": self.ua, "Accept-Language": "en-US,en;q=0.9"},
                timeout=self._request_timeout,
                trust_env=False,
                http2=self._http2,
            )
            self.log("httpx_client_created")
        self._playwright = await async_playwright().start()
        self._browser = await self._playwright.chromium.launch(headless=True)
        self.log("browser_started")
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.cleanup()

    async def cleanup(self):
        # close httpx client
        if self.client:
            try:
                await self.client.aclose()
                self.log("httpx_client_closed")
            except Exception as e:
                self.log("httpx_client_close_failed", level="warning", error=str(e))
            self.client = None

        # close cached contexts
        for key, cached in list(self._context_cache.items()):
            try:
                await cached["context"].close()
                self.log("context_closed", context_key=key)
            except Exception as e:
                self.log("context_close_failed", level="warning", context_key=key, error=str(e))
        self._context_cache.clear()

        # close browser
        if self._browser:
            try:
                await self._browser.close()
                self.log("browser_closed")
            except Exception as e:
                self.log("browser_close_failed", level="warning", error=str(e))
            self._browser = None

        # stop playwright
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

    # (rest of your code stays identical: retries, circuit breaker, try_api, try_html, try_browser, hooks, metrics…)


    # --------------------
    # rate limiting helper
    # --------------------
    async def _acquire_rate_slot(self):
        """
        If a shared semaphore is provided, that coordinates across multiple scrapers.
        Otherwise enforce a simple per-instance minimal interval.
        """
        if self._shared_rate_limiter:
            async with self._shared_rate_limiter:
                await asyncio.sleep(self._min_interval)
        else:
            async with self._rate_lock:
                now = asyncio.get_running_loop().time()
                elapsed = now - self._last_request_ts
                wait_for = self._min_interval - elapsed
                if wait_for > 0:
                    await asyncio.sleep(wait_for)
                self._last_request_ts = asyncio.get_running_loop().time()

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
        if r["state"] == "closed":
            return True
        if r["state"] == "open":
            if time.time() - r["opened_at"] >= self.recovery_timeout:
                r["state"] = "half-open"
                return True
            return False
        # half-open: allow a test call
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
            r["opened_at"] = time.time()
            self.log("circuit_opened", level="warning", endpoint=key, fail_count=r["fail_count"])

    # --------------------
    # retry wrapper (with UA rotation)
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
        """
        Decorator for async coroutine functions.
        - retry_on: exception classes to retry
        - max_retries: override (falls back to self.default_max_retries)
        - backoff_base: base seconds for exponential backoff
        - jitter: multiplicative jitter range
        - on_retry: optional callback (attempt, exc, cb_key)
        """

        async def wrapped(*args, **kwargs):
            cb_key = kwargs.get("cb_key") or (args[0] if args else "default") or "default"
            if not self._cb_allow(cb_key):
                self.log("circuit_blocked", level="warning", endpoint=cb_key)
                return None

            local_max_retries = kwargs.pop("max_retries", None) or max_retries or self.default_max_retries
            local_retry_on = kwargs.pop("retry_on", None) or retry_on

            # pick initial proxy (can pass proxies=... to override)
            proxy = kwargs.get("proxies") or self.proxy_pool.get()
            last_exc: Optional[Exception] = None

            for attempt in range(1, local_max_retries + 1):
                try:
                    # --- rotate UA for this attempt ---
                    self.ua = random.choice(self.USER_AGENTS)
                    if self.client:
                        # update client header for this attempt
                        self.client.headers.update({"User-Agent": self.ua})

                    await self._acquire_rate_slot()
                    start = time.perf_counter()
                    result = await coro_fn(*args, proxies=proxy, **kwargs)
                    duration = time.perf_counter() - start

                    # metrics & proxy latency
                    self.metrics["latency_histogram"].append(duration)
                    if proxy:
                        try:
                            self.proxy_pool.mark_latency(proxy, duration)
                        except Exception:
                            pass

                    if result is not None:
                        # success
                        self.metrics["successful_requests"] += 1
                        if proxy:
                            try:
                                self.proxy_pool.mark_success(proxy)
                                self.metrics["proxy_success"] += 1
                            except Exception:
                                pass
                        self._cb_record_success(cb_key)
                        self.log(
                            "request_success",
                            level="info",
                            endpoint=cb_key,
                            attempt=attempt,
                            duration_ms=int(duration * 1000),
                            proxy=proxy["http"] if proxy else None,
                        )
                        return result

                    # treat empty result as a soft failure
                    raise RuntimeError("Empty result")

                except local_retry_on as e:
                    last_exc = e
                    self.metrics["failed_requests"] += 1
                    if proxy:
                        try:
                            self.proxy_pool.mark_failed(proxy)
                            self.metrics["proxy_fail"] += 1
                        except Exception:
                            pass

                    self.log(
                        "retry_failed",
                        level="warning",
                        attempt=attempt,
                        error=str(e),
                        endpoint=cb_key,
                        proxy=proxy["http"] if proxy else None,
                    )
                    if on_retry:
                        try:
                            on_retry(attempt, e, cb_key)
                        except Exception:
                            pass

                    # rotate to a new proxy for the next attempt
                    proxy = self.proxy_pool.next()

                    # exponential backoff with jitter
                    sleep_for = (backoff_base * (2 ** (attempt - 1))) * random.uniform(*jitter)
                    await asyncio.sleep(sleep_for)

                except Exception as e:
                    # non-retryable error: break and record
                    last_exc = e
                    self.metrics["failed_requests"] += 1
                    self.log("request_error", level="error", error=str(e), endpoint=cb_key)
                    break

            # exhausted retries
            self._cb_record_failure(cb_key)
            self.metrics["endpoint_errors"][cb_key] += 1
            self.log("retries_exhausted", level="error", endpoint=cb_key, error=str(last_exc) if last_exc else None)
            return None

        return wrapped

    # --------------------
    # Playwright context cache (per-proxy contexts)
    # --------------------
    async def _get_browser_context(self, proxy: Optional[Dict[str, str]]):
        """
        Reuse a context per proxy for speed; respects TTL and LRU eviction.
        Key is proxy["http"] or "direct".
        """
        key = proxy["http"] if proxy else "direct"
        now = time.time()

        # prune expired entries
        to_delete = []
        for k, cached in self._context_cache.items():
            if now - cached["created_at"] >= self.CONTEXT_TTL_SEC:
                to_delete.append(k)
        for k in to_delete:
            try:
                await self._context_cache[k]["context"].close()
            except Exception:
                pass
            self._context_cache.pop(k, None)

        # return cached (and mark as recently used)
        cached = self._context_cache.get(key)
        if cached:
            self._context_cache.move_to_end(key)
            return cached["context"]

        # enforce LRU limit
        while len(self._context_cache) >= self.CONTEXT_CACHE_MAX:
            old_key, old_val = self._context_cache.popitem(last=False)
            try:
                await old_val["context"].close()
            except Exception:
                pass

        if not self._browser:
            raise RuntimeError("Browser not initialized. Use `async with scraper:`")

        # Use current UA when creating context
        context = await self._browser.new_context(
            user_agent=self.ua,
            proxy={"server": proxy["http"]} if proxy else None,
        )
        self._context_cache[key] = {"context": context, "created_at": now}
        return context

    # --------------------
    # async scraping primitives (decorated with retry wrapper)
    # --------------------
    @property
    def try_api(self):
        @self.with_retries
        async def _impl(endpoint: str, proxies=None, **kwargs):
            headers = kwargs.pop("headers", None) or {}
            timeout = kwargs.pop("timeout", self._request_timeout)

            self.metrics["requests_made"] += 1
            resp = await self.client.get(endpoint, proxies=proxies, headers=headers, timeout=timeout)
            resp.raise_for_status()

            ctype = (resp.headers.get("content-type") or "").lower()
            if "application/json" in ctype or ctype.endswith("+json"):
                return resp.json()
            return None

        return _impl

    @property
    def try_static_html(self):
        @self.with_retries
        async def _impl(url: str, proxies=None, **kwargs):
            headers = kwargs.pop("headers", None) or {}
            timeout = kwargs.pop("timeout", self._request_timeout)

            self.metrics["requests_made"] += 1
            resp = await self.client.get(url, proxies=proxies, headers=headers, timeout=timeout)
            resp.raise_for_status()
            html = resp.text

            if self.captcha_detect_hook(mode="http", html=html, url=url):
                self.log("captcha_detected", level="warning", mode="http", url=url)
                solved = await self.captcha_solve_hook(mode="http", html=html, url=url, proxy=proxies)
                return BeautifulSoup(solved, "html.parser") if solved else None

            return BeautifulSoup(html, "html.parser")

        return _impl

    @property
    def try_browser(self):
        @self.with_retries
        async def _impl(url: str, proxies=None, **kwargs):
            """
            Browser fetch. If `self.offload_browser` is truthy we will schedule a Celery
            task (run_playwright_task) and await its result using asyncio.to_thread
            so we don't block the event loop.
            """
            offload = getattr(self, "offload_browser", False)
            proxy_server = None
            if proxies and isinstance(proxies, dict):
                proxy_server = proxies.get("http") or proxies.get("https")

            if offload:
                # dispatch Celery task and wait in a thread
                try:
                    async_result = run_playwright_task.delay(url, self.ua, proxy_server, kwargs.get("timeout", 20))
                    def wait_result():
                        return async_result.get(timeout=kwargs.get("celery_result_timeout", 30))
                    html = await asyncio.to_thread(wait_result)
                    if not html:
                        return None
                    return html
                except Exception as e:
                    self.log("playwright_offload_failed", level="error", error=str(e), url=url)
                    return None

            # fallback to local Playwright path
            if not self._browser:
                raise RuntimeError("Browser not initialized. Use `async with scraper:`")

            context = await self._get_browser_context(proxies)
            page = await context.new_page()
            await page.goto(url, timeout=int(self._request_timeout * 1000), wait_until="domcontentloaded")
            await asyncio.sleep(random.uniform(1.0, 2.5))
            html = await page.content()

            if self.captcha_detect_hook(mode="browser", html=html, url=url, page=page):
                self.log("captcha_detected", level="warning", mode="browser", url=url)
                solved = await self.captcha_solve_hook(mode="browser", page=page, url=url, proxy=proxies)
                if not solved:
                    return None
                await asyncio.sleep(0.8)
                html = await page.content()

            return html

        return _impl

    # --------------------
    # hooks to override in subclasses
    # --------------------
    async def paginate_hook(self, soup_or_page):
        return soup_or_page

    def captcha_detect_hook(self, mode: str, **ctx) -> bool:
        html = ctx.get("html", "") or ""
        if not html:
            return False
        needles = ("captcha", "recaptcha", "hcaptcha", "g-recaptcha", "cf-challenge", "are you a robot")
        return any(n in html.lower() for n in needles)

    async def captcha_solve_hook(self, mode: str, **ctx) -> Optional[bool]:
        """
        Override to integrate a solver. For http mode return solved HTML (str) or None.
        For browser mode return True if solved, False otherwise.
        """
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

        # 1) try API
        if api_endpoint:
            data = await self.try_api(api_endpoint, cb_key=f"api:{api_endpoint}")
            if data:
                try:
                    parsed = await self.parse_api(data)
                    if parsed:
                        matches.extend(parsed)
                except Exception as e:
                    self.log("parse_api_failed", level="error", error=str(e), endpoint=api_endpoint)

        # 2) try static HTML
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
        pool_stats = {}
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
