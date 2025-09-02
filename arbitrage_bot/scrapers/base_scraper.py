import time
import json
import random
import logging
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from functools import lru_cache

from utils.match_utils import build_match_dict
from .proxy_pool import ProxyPool

logger = logging.getLogger("scraper")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(message)s"))
logger.addHandler(handler)

class BaseScraper:
    bookmaker: str = "GenericBookmaker"
    supports_browser_fallback: bool = True  # default for all scrapers

    def __init__(self, base_url: str, proxy_list=None,
                 max_retries: int = 3, sleep_between_requests: float = 1.0,
                 cache_ttl: int = 60, bookmaker: str = None):
        self.bookmaker = bookmaker or self.bookmaker
        self.base_url = base_url
        self.max_retries = max_retries
        self.sleep_between_requests = sleep_between_requests
        self.cache_ttl = cache_ttl

        self.proxy_pool = ProxyPool(proxy_list or [], max_failures=3)
        self.ua = self.get_random_user_agent()

        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": self.ua,
            "Accept-Language": "en-US,en;q=0.9",
        })

        self.cache = {}
        self.metrics = {
            "requests_made": 0,
            "failed_requests": 0,
            "matches_collected": 0
        }


    def log(self, event: str, level: str = "info", **kwargs):
        message = json.dumps({"event": event, "bookmaker": self.bookmaker, **kwargs})
        getattr(logger, level, logger.info)(message)

    def get_random_user_agent(self):
        return random.choice([
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/114.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Safari/605.1.15",
            "Mozilla/5.0 (X11; Linux x86_64) Chrome/113.0",
        ])

    def _cache_get(self, key):
        if key in self.cache:
            data, ts = self.cache[key]
            if time.time() - ts < self.cache_ttl:
                return data
            else:
                del self.cache[key]
        return None

    def _cache_set(self, key, value):
        self.cache[key] = (value, time.time())

    def with_retries(self, func):
        def wrapper(*args, **kwargs):
            for attempt in range(self.max_retries):
                proxy = self.proxy_pool.get()  # Rotate proxy every attempt
                try:
                    start = time.time()
                    result = func(*args, proxies=proxy, **kwargs)
                    if result:
                        if proxy:
                            self.proxy_pool.mark_latency(proxy, time.time() - start)
                        return result
                except Exception as e:
                    self.metrics["failed_requests"] += 1
                    self.log("retry_failed", level="warning",
                             attempt=attempt + 1, error=str(e))
                    time.sleep(2 ** attempt + random.random())
            if proxy:
                self.proxy_pool.mark_failed(proxy)
            return None
        return wrapper

    @property
    def try_api(self):
        @self.with_retries
        def _impl(endpoint: str, proxies=None):
            cache_key = f"api:{endpoint}"
            cached = self._cache_get(cache_key)
            if cached:
                return cached
            self.metrics["requests_made"] += 1
            resp = self.session.get(endpoint, proxies=proxies, timeout=15)
            resp.raise_for_status()
            if "json" in resp.headers.get("content-type", ""):
                data = resp.json()
                self._cache_set(cache_key, data)
                return data
            return None
        return _impl

    @property
    def try_static_html(self):
        @self.with_retries
        def _impl(url: str, proxies=None):
            cache_key = f"html:{url}"
            cached = self._cache_get(cache_key)
            if cached:
                return cached
            self.metrics["requests_made"] += 1
            resp = self.session.get(url, proxies=proxies, timeout=15)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")
            self._cache_set(cache_key, soup)
            return soup
        return _impl

    def try_browser(self, url: str, proxies=None, use_queue=False):
        if use_queue:
            # Placeholder: push to Selenium task queue for async processing
            self.log("browser_task_queued", level="info", url=url)
            return None
        opts = Options()
        opts.add_argument("--headless=new")
        opts.add_argument(f"user-agent={self.ua}")
        if proxies and "http" in proxies:
            opts.add_argument(f"--proxy-server={proxies['http']}")
        driver = webdriver.Chrome(options=opts)
        try:
            driver.get(url)
            time.sleep(random.uniform(2, 5))
            html = driver.page_source
            return html
        finally:
            driver.quit()

    def solve_captcha(self, page_content):
        """Hook: integrate external captcha solver (e.g., 2Captcha, hCaptcha)."""
        return page_content

    def handle_cloudflare(self, url):
        """Hook: integrate Cloudflare bypass (e.g., using cloudscraper)."""
        return None

    def paginate_hook(self, soup_or_page):
        return soup_or_page

    def normalize_match(self, home, away, start_time, market, odds):
        self.metrics["matches_collected"] += 1
        return build_match_dict(home, away, start_time, market, odds, self.bookmaker)

    def get_odds(self, api_endpoint=None, sport_path="", use_browser=False):
        matches = []

        if api_endpoint:
            data = self.try_api(api_endpoint)
            if data:
                try:
                    matches.extend(self.parse_api(data))
                except Exception as e:
                    self.log("parse_api_failed", level="error", error=str(e))

        if not matches:
            soup = self.try_static_html(self.base_url + sport_path)
            if soup:
                soup = self.paginate_hook(soup)
                try:
                    matches.extend(self.parse_html(soup))
                except Exception as e:
                    self.log("parse_html_failed", level="error", error=str(e))

        if not matches and use_browser:
            page = self.try_browser(self.base_url + sport_path)
            if page:
                soup = self.paginate_hook(BeautifulSoup(page, "html.parser"))
                try:
                    matches.extend(self.parse_html(soup))
                except Exception as e:
                    self.log("parse_html_failed", level="error", error=str(e))

        return matches

    def get_multiple_odds(self, api_endpoints: list[str] = None, sport_paths: list[str] = None):
        matches = []
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

    def parse_api(self, data): return []
    def parse_html(self, soup): return []
