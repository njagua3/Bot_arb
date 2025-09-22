import time, signal, sys, threading, traceback
from core.settings import load_stake, SCAN_INTERVAL
from core.logger import log_error, log_info
from core.cache import Cache
from core.arbitrage import ArbitrageFinder
from core.telegram import run_bot
from core.db import init_db

# 👇 scrapers
from scrapers.scraper_loader import discover_scrapers
from scrapers.orchestrator import ScraperOrchestrator
from scrapers.base_scraper import BaseScraper
from scrapers.async_base_scraper import AsyncBaseScraper
import asyncio

cache = Cache()
arb_finder = ArbitrageFinder()


def run_scrapers_fallback(scrapers):
    """
    Run scrapers directly (synchronously/asynchronously) without Celery.
    """
    all_entries = []
    for scraper in scrapers:
        try:
            if isinstance(scraper, BaseScraper):
                entries = scraper.get_odds()
            elif isinstance(scraper, AsyncBaseScraper):
                entries = asyncio.run(scraper.get_odds())
            else:
                log_error(f"❌ Unknown scraper type: {type(scraper)}")
                continue

            if entries:
                all_entries.extend(entries)
                log_info(f"✅ Fallback: {scraper.bookmaker} returned {len(entries)} entries.")
            else:
                log_info(f"⚠️ Fallback: {scraper.bookmaker} returned no entries.")
        except Exception as e:
            log_error(f"❌ Fallback scraper {scraper.bookmaker} failed: {e}")
            traceback.print_exc()
    return all_entries


def run_check():
    total_stake = load_stake()
    log_info("🔍 Scanning for arbitrage opportunities...")

    scrapers = discover_scrapers()
    if not scrapers:
        log_error("❌ No valid scrapers discovered!")
        return

    all_entries = []
    try:
        # 🎯 Primary: Celery orchestrator
        orch = ScraperOrchestrator(scrapers)
        result = orch.run()
        all_entries = result.get("matches", [])
        log_info("✅ Scrapers executed via Celery Orchestrator.")
    except Exception as e:
        # 🚨 Fallback mode
        log_error(f"⚠️ Orchestrator failed ({e}), falling back to direct execution.")
        all_entries = run_scrapers_fallback(scrapers)

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
    """
    Start Telegram bot (single instance).
    """
    try:
        log_info("🤖 Starting Telegram bot...")
        run_bot()  # blocking call (polling loop inside)
    except Exception as e:
        log_error(f"⚠️ Telegram bot crashed: {e}")


        # 💓 heartbeat log
        if time.time() - last_heartbeat >= 600:  # every 10 minutes
            log_info("💓 Telegram bot thread alive")
            last_heartbeat = time.time()


if __name__ == "__main__":
    log_info("🚀 Arbitrage Bot Started!")
    init_db()
    cache.load()

    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    bot_thread = threading.Thread(target=start_telegram_bot, daemon=True)
    bot_thread.start()

    start_scanner()
