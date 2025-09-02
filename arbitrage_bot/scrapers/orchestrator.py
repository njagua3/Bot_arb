# scrapers/orchestrator.py
import asyncio
from typing import List, Union, Dict, Any
from celery.result import AsyncResult

from .base_scraper import BaseScraper
from .async_base_scraper import AsyncBaseScraper
from .tasks import run_scraper_task
from .scraper_loader import discover_scrapers  # NEW import for auto-discovery


class ScraperOrchestrator:
    def __init__(self,
                 scrapers: List[Union[BaseScraper, AsyncBaseScraper]] = None,
                 cache_enabled: bool = True,
                 max_concurrent: int = 5,
                 task_timeout: int = 40,
                 high_priority_bookmakers: List[str] = None):
        """
        Orchestrates distributed scraping using Celery.
        If no scrapers list is passed, auto-discovers them from the scrapers/ folder.
        """
        self.scrapers = scrapers or discover_scrapers()
        self.cache_enabled = cache_enabled
        self._cache: Dict[str, Any] = {}
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.task_timeout = task_timeout
        self.high_priority_bookmakers = set(high_priority_bookmakers or [])

    def run(self) -> List[Dict[str, Any]]:
        """
        Submit all scrapers as Celery tasks and collect results.
        Returns a list of aggregated match dicts.
        """
        results = []

        async def orchestrate():
            tasks = []
            for scraper in self.scrapers:
                cache_key = f"{scraper.bookmaker}_odds"
                if self.cache_enabled and cache_key in self._cache:
                    scraper.log("cache_hit", bookmaker=scraper.bookmaker)
                    results.extend(self._cache[cache_key])
                    continue

                queue = "high_priority" if scraper.bookmaker in self.high_priority_bookmakers \
                    or getattr(scraper, "priority", False) else "default"

                task_kwargs = {
                    "scraper_module": scraper.__class__.__module__,
                    "scraper_class": scraper.__class__.__name__,
                    "proxy_pool": getattr(scraper, "proxy_pool", None)
                }

                task = run_scraper_task.apply_async(kwargs=task_kwargs, queue=queue)
                tasks.append((scraper, task))

            gathered_results = await self._gather_results(tasks)
            results.extend(gathered_results)
            return results

        return asyncio.run(orchestrate())

    async def _gather_results(self, tasks: List) -> List[Dict[str, Any]]:
        """
        Wait for Celery task results with concurrency and timeout.
        """
        all_results = []

        async def fetch_result(scraper, task: AsyncResult):
            async with self.semaphore:
                try:
                    result = await asyncio.to_thread(task.get, timeout=self.task_timeout)
                    if result and isinstance(result.get("matches", []), list):
                        all_results.extend(result["matches"])
                        if self.cache_enabled:
                            self._cache[f"{scraper.bookmaker}_odds"] = result["matches"]
                    else:
                        scraper.log("empty_result", bookmaker=scraper.bookmaker)
                except Exception as e:
                    scraper.log("task_failed", error=str(e))

        await asyncio.gather(*(fetch_result(s, t) for s, t in tasks))
        return all_results
