# scrapers/__init__.py
"""
Lightweight, lazy exports for scrapers package.
Avoid eager imports that pull legacy BaseScraper (removed).
"""

from __future__ import annotations
import importlib

__all__ = ["ProxyPool", "AsyncBaseScraper", "ScraperOrchestrator"]

def __getattr__(name: str):
    if name == "ProxyPool":
        return getattr(importlib.import_module(".proxy_pool", __name__), "ProxyPool")
    if name == "AsyncBaseScraper":
        return getattr(importlib.import_module(".async_base_scraper", __name__), "AsyncBaseScraper")
    if name == "ScraperOrchestrator":
        return getattr(importlib.import_module(".orchestrator", __name__), "ScraperOrchestrator")
    if name == "BaseScraper":
        # Explicitly block legacy import paths
        raise AttributeError(
            "scrapers.BaseScraper was removed. Use scrapers.AsyncBaseScraper instead."
        )
    raise AttributeError(name)
