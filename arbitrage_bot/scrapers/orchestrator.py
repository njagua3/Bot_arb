# scrapers/orchestrator.py
import asyncio
import json
import logging
import os
from datetime import datetime
from typing import List, Union, Dict, Any, Optional

import redis
from celery.result import AsyncResult

from .base_scraper import BaseScraper
from .async_base_scraper import AsyncBaseScraper
from .tasks import run_scraper_task
from .scraper_loader import discover_scrapers


# ---------- JSON logger ----------
class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": getattr(record, "asctime", None) or self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        # include any extra attributes on the record (avoid overwriting keys above)
        for k, v in getattr(record, "__dict__", {}).items():
            if k not in payload and k not in ("args", "msg", "exc_info", "stack_info"):
                try:
                    json.dumps(v)  # ensure serializable
                    payload[k] = v
                except Exception:
                    payload[k] = str(v)
        return json.dumps(payload)


logger = logging.getLogger("scraper_orchestrator")
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    logger.addHandler(handler)
logger.setLevel(os.getenv("SCRAPER_LOG_LEVEL", "INFO"))


class ScraperOrchestrator:
    """
    Orchestrates distributed scraping using Celery.
    - Auto-discovers scrapers if not provided
    - Optional persistent Redis-backed cache
    - Concurrency control for waiting on Celery tasks
    - Structured JSON logging
    - Metrics integration
    """

    def __init__(
        self,
        scrapers: List[Union[BaseScraper, AsyncBaseScraper]] = None,
        cache_enabled: bool = True,
        max_concurrent: int = 5,
        task_timeout: int = 40,
        high_priority_bookmakers: List[str] = None,
        redis_url: Optional[str] = None,
        task_retries: int = 3,
    ):
        self.scrapers = scrapers or discover_scrapers()
        self.cache_enabled = cache_enabled
        self._in_memory_cache: Dict[str, Any] = {}
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.task_timeout = task_timeout
        self.high_priority_bookmakers = set(high_priority_bookmakers or [])
        self.task_retries = task_retries

        # Redis persistent cache + metrics (optional)
        self.redis: Optional[redis.Redis] = None
        redis_conn_url = redis_url or os.getenv("REDIS_URL")
        if redis_conn_url:
            try:
                self.redis = redis.Redis.from_url(redis_conn_url, decode_responses=True)
                self.redis.ping()
                logger.info(json.dumps({"event": "redis_connected", "url": redis_conn_url}))
            except Exception as e:
                logger.warning(json.dumps({"event": "redis_unavailable", "error": str(e)}))
                self.redis = None

    # --------------------
    # cache helpers (JSON-safe)
    # --------------------
    def _cache_key(self, bookmaker: str, suffix: str = "odds", extra: str = "") -> str:
        key = f"scraper:{bookmaker}:{suffix}"
        if extra:
            key = f"{key}:{extra}"
        return key

    def _cache_set(self, key: str, value: Any, ttl: int = 300) -> None:
        try:
            if self.redis:
                self.redis.setex(key, ttl, json.dumps(value))
            else:
                self._in_memory_cache[key] = value
            logger.info(json.dumps({"event": "cache_set", "key": key, "ttl": ttl}))
        except Exception as e:
            logger.error(json.dumps({"event": "cache_set_failed", "key": key, "error": str(e)}))

    def _cache_get(self, key: str) -> Any:
        try:
            if self.redis:
                raw = self.redis.get(key)
                return json.loads(raw) if raw else None
            return self._in_memory_cache.get(key)
        except Exception as e:
            logger.error(json.dumps({"event": "cache_get_failed", "key": key, "error": str(e)}))
            return None

    def clear_cache(self, bookmaker: str, extra: str = "") -> None:
        """
        Manually invalidate cache for a bookmaker.
        """
        key = self._cache_key(bookmaker, "odds", extra)
        try:
            if self.redis:
                self.redis.delete(key)
            else:
                self._in_memory_cache.pop(key, None)
            logger.info(json.dumps({"event": "cache_cleared", "bookmaker": bookmaker, "key": key}))
        except Exception as e:
            logger.error(json.dumps({"event": "cache_clear_failed", "bookmaker": bookmaker, "error": str(e)}))

    # --------------------
    # orchestration
    # --------------------
    def run(self) -> Dict[str, Any]:
        """
        Submit all scrapers as Celery tasks and collect results.
        Returns structured dict with status, matches, bookmakers_run, timestamp.
        """
        if not self.scrapers:
            logger.warning(json.dumps({"event": "no_scrapers_found"}))
            return {
                "status": "NO_SCRAPERS",
                "matches": [],
                "bookmakers_run": [],
                "timestamp": datetime.utcnow().isoformat(),
            }

        aggregated: List[Dict[str, Any]] = []
        bookmakers_run: List[str] = []

        async def orchestrate():
            tasks: List[tuple] = []
            for scraper in self.scrapers:
                extra = getattr(scraper, "cache_scope", "")
                cache_key = self._cache_key(scraper.bookmaker, "odds", extra)

                if self.cache_enabled:
                    cached = self._cache_get(cache_key)
                    if cached:
                        try:
                            scraper.log("cache_hit", bookmaker=scraper.bookmaker)
                        except Exception:
                            logger.info(json.dumps({"event": "cache_hit", "bookmaker": scraper.bookmaker}))
                        aggregated.extend(cached)
                        bookmakers_run.append(scraper.bookmaker)
                        continue

                queue = "high_priority" if (
                    scraper.bookmaker in self.high_priority_bookmakers
                    or getattr(scraper, "priority", False)
                ) else "default"

                task_kwargs = {
                    "scraper_module": scraper.__class__.__module__,
                    "scraper_class": scraper.__class__.__name__,
                    "proxy_pool": getattr(scraper, "proxy_pool", None),
                }

                try:
                    task = run_scraper_task.apply_async(
                        kwargs=task_kwargs,
                        queue=queue,
                        retry=True,
                        retry_policy={
                            "max_retries": self.task_retries,
                            "interval_start": 5,
                            "interval_step": 10,
                            "interval_max": 30,
                        },
                    )
                    logger.info(json.dumps({"event": "task_submitted", "bookmaker": scraper.bookmaker, "queue": queue}))
                    tasks.append((scraper, task, cache_key))
                except Exception as e:
                    logger.error(json.dumps({"event": "task_submit_failed", "bookmaker": scraper.bookmaker, "error": str(e)}))
                    if self.redis:
                        self.redis.incr("scraper:metrics:failure", 1)

            gathered = await self._gather_results(tasks, bookmakers_run)
            aggregated.extend(gathered)
            return aggregated

        matches = asyncio.run(orchestrate())
        return {
            "status": "OK",
            "matches": matches,
            "bookmakers_run": list(set(bookmakers_run)),
            "timestamp": datetime.utcnow().isoformat(),
        }

    async def _gather_results(self, tasks: List[tuple], bookmakers_run: List[str]) -> List[Dict[str, Any]]:
        all_matches: List[Dict[str, Any]] = []

        async def fetch_result(scraper, async_result: AsyncResult, cache_key: str):
            async with self.semaphore:
                try:
                    result = await asyncio.to_thread(async_result.get, timeout=self.task_timeout)
                except Exception as e:
                    scraper.log("task_failed", error=str(e))
                    logger.error(json.dumps({"event": "task_failed", "bookmaker": scraper.bookmaker, "error": str(e)}))
                    if self.redis:
                        self.redis.incr("scraper:metrics:failure", 1)
                    return

                if not result:
                    scraper.log("empty_task_result", bookmaker=scraper.bookmaker)
                    return

                matches = result.get("matches")
                if isinstance(matches, list):
                    all_matches.extend(matches)
                    bookmakers_run.append(scraper.bookmaker)
                    if self.cache_enabled:
                        try:
                            self._cache_set(cache_key, matches)
                        except Exception as e:
                            logger.error(json.dumps({"event": "cache_set_failed", "bookmaker": scraper.bookmaker, "error": str(e)}))
                    scraper.log("task_success", bookmaker=scraper.bookmaker, matches=len(matches))
                    if self.redis:
                        self.redis.incr("scraper:metrics:success", 1)
                else:
                    scraper.log("invalid_task_result", bookmaker=scraper.bookmaker, result_type=type(matches).__name__)
                    logger.warning(json.dumps({"event": "invalid_task_result", "bookmaker": scraper.bookmaker, "result": str(matches)}))
                    if self.redis:
                        self.redis.incr("scraper:metrics:failure", 1)

        await asyncio.gather(*(fetch_result(s, t, k) for s, t, k in tasks))
        return all_matches

    # --------------------
    # clean up resources
    # --------------------
    async def close(self) -> None:
        if self.redis:
            try:
                try:
                    self.redis.close()
                except Exception:
                    pass
                try:
                    self.redis.connection_pool.disconnect()
                except Exception:
                    pass
                logger.info(json.dumps({"event": "redis_closed"}))
            except Exception as e:
                logger.warning(json.dumps({"event": "redis_close_failed", "error": str(e)}))
