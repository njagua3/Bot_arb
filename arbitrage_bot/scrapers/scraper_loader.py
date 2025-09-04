# scrapers/scraper_loader.py
import importlib
import inspect
import logging
import os
import pkgutil
import json
from pathlib import Path
from typing import List, Union

from .base_scraper import BaseScraper
from .async_base_scraper import AsyncBaseScraper

SCRAPERS_PACKAGE = __name__.rsplit(".", 1)[0]  # "scrapers"

# ---------- JSON logger ----------
class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload)


logger = logging.getLogger("scraper_loader")
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    logger.addHandler(handler)
logger.setLevel(os.getenv("SCRAPER_LOG_LEVEL", "INFO"))


def discover_scrapers(scrapers_dir: Path = None) -> List[Union[BaseScraper, AsyncBaseScraper]]:
    """
    Auto-discovers scraper classes in the scrapers/ folder.
    - Only loads classes inheriting BaseScraper or AsyncBaseScraper
    - Ignores base/infra modules
    """
    scrapers: List[Union[BaseScraper, AsyncBaseScraper]] = []
    scrapers_dir = scrapers_dir or Path(__file__).parent

    skipped_modules = {"base_scraper", "async_base_scraper", "tasks", "orchestrator", "scraper_loader"}
    disabled = set((os.getenv("DISABLED_SCRAPERS", "")).split(","))  # optional: disable via env

    discovered_count, failed_imports, failed_inits = 0, 0, 0

    for _, module_name, is_pkg in pkgutil.iter_modules([str(scrapers_dir)]):
        if is_pkg or module_name in skipped_modules or module_name in disabled:
            continue

        module_fullname = f"{SCRAPERS_PACKAGE}.{module_name}"
        try:
            module = importlib.import_module(module_fullname)
            logger.info(json.dumps({"event": "module_imported", "module": module_fullname}))
        except Exception as e:
            logger.error(json.dumps({"event": "module_import_failed", "module": module_fullname, "error": str(e)}))
            failed_imports += 1
            continue

        for name, obj in inspect.getmembers(module, inspect.isclass):
            if obj.__module__ == module_fullname and (
                issubclass(obj, BaseScraper) or issubclass(obj, AsyncBaseScraper)
            ):
                try:
                    scrapers.append(obj())
                    discovered_count += 1
                    logger.info(json.dumps({"event": "scraper_discovered", "class": name, "module": module_fullname}))
                except Exception as e:
                    logger.error(json.dumps({"event": "scraper_init_failed", "class": name, "module": module_fullname, "error": str(e)}))
                    failed_inits += 1

    if not scrapers:
        logger.warning(json.dumps({"event": "no_scrapers_found"}))

    logger.info(json.dumps({
        "event": "discovery_summary",
        "discovered": discovered_count,
        "failed_imports": failed_imports,
        "failed_inits": failed_inits,
    }))

    return scrapers
