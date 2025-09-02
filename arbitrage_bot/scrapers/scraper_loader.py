# scrapers/scraper_loader.py
import importlib
import inspect
import pkgutil
from pathlib import Path
from typing import List, Union

from .base_scraper import BaseScraper
from .async_base_scraper import AsyncBaseScraper

SCRAPERS_PACKAGE = __name__.rsplit(".", 1)[0]  # "scrapers"

def discover_scrapers(scrapers_dir: Path = None) -> List[Union[BaseScraper, AsyncBaseScraper]]:
    """
    Automatically discovers all scraper classes in the scrapers/ folder.
    - Only loads classes inheriting BaseScraper or AsyncBaseScraper
    - Ignores base modules and non-scraper files
    """
    scrapers = []
    scrapers_dir = scrapers_dir or Path(__file__).parent

    for _, module_name, is_pkg in pkgutil.iter_modules([str(scrapers_dir)]):
        if is_pkg:
            continue
        if module_name in ("base_scraper", "async_base_scraper", "tasks", "orchestrator", "scraper_loader"):
            continue

        module_fullname = f"{SCRAPERS_PACKAGE}.{module_name}"
        try:
            module = importlib.import_module(module_fullname)
        except Exception as e:
            print(f"[scraper_loader] Failed to import {module_fullname}: {e}")
            continue

        for name, obj in inspect.getmembers(module, inspect.isclass):
            if obj.__module__ == module_fullname and (
                issubclass(obj, BaseScraper) or issubclass(obj, AsyncBaseScraper)
            ):
                try:
                    scrapers.append(obj())  # Instantiate with default __init__
                except Exception as e:
                    print(f"[scraper_loader] Failed to init {name} from {module_fullname}: {e}")

    return scrapers
