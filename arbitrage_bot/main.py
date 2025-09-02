# main.py
import time, signal, sys, threading, concurrent.futures
from core.settings import load_stake, SCAN_INTERVAL, SCRAPERS
from core.logger import log_error, log_info
from core.cache import Cache
from core.arbitrage import ArbitrageFinder
from core.telegram import run_bot
from core.db import init_db
from importlib import import_module


cache = Cache()
arb_finder = ArbitrageFinder()


def load_scrapers():
    """Dynamically import scrapers defined in settings.json with validation."""
    scrapers = []
    for name, path in SCRAPERS:
        try:
            module_name, func_name = path.rsplit(".", 1)
            module = import_module(module_name)

            if not hasattr(module, func_name):
                log_error(f"⚠️ Scraper '{name}' missing function '{func_name}' in {module_name}")
                continue

            scraper_func = getattr(module, func_name)
            scrapers.append(scraper_func)
            log_info(f"✅ Loaded scraper: {name} ({path})")

        except ModuleNotFoundError:
            log_error(f"⚠️ Scraper '{name}' skipped: module '{module_name}' not found")
        except Exception as e:
            log_error(f"⚠️ Scraper '{name}' skipped due to error: {e}")
    return scrapers


def run_check():
    total_stake = load_stake()
    log_info("🔍 Scanning for arbitrage opportunities...")

    all_entries = []
    scrapers = load_scrapers()

    if not scrapers:
        log_error("❌ No valid scrapers loaded! Check your settings.json")
        return

    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = {executor.submit(scraper): scraper.__name__ for scraper in scrapers}
        for future in concurrent.futures.as_completed(futures):
            scraper_name = futures[future]
            try:
                all_entries.extend(future.result())
            except Exception as e:
                log_error(f"❌ Error in scraper {scraper_name}: {e}")

    alerts_sent = arb_finder.scan_and_alert(all_entries)
    log_info(f"📨 Alerts sent: {alerts_sent}")

    cache.cleanup()


def shutdown_handler(sig, frame):
    log_info("🛑 Shutting down arbitrage bot gracefully...")
    cache.cleanup()
    sys.exit(0)


def start_scanner():
    while True:
        start = time.perf_counter()
        run_check()
        elapsed = time.perf_counter() - start
        log_info(f"⏳ Waiting {SCAN_INTERVAL}s before next scan (last took {elapsed:.2f}s)...")
        for _ in range(SCAN_INTERVAL):
            time.sleep(1)


def start_telegram_bot():
    while True:
        try:
            run_bot()
        except Exception as e:
            log_error(f"⚠️ Telegram bot crashed: {e}, restarting in 5s...")
            time.sleep(5)


if __name__ == "__main__":
    log_info("🚀 Arbitrage Bot Started!")
    init_db()
    cache.load()

    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    bot_thread = threading.Thread(target=start_telegram_bot, daemon=True)
    bot_thread.start()

    start_scanner()
