# main.py
import time, signal, sys, threading
from core.settings import load_stake, SCAN_INTERVAL
from core.logger import log_error, log_info
from core.cache import Cache
from core.arbitrage import ArbitrageFinder
from core.telegram import run_bot
from core.db import init_db

# 👇 new imports
from scrapers.scraper_loader import discover_scrapers
from scrapers.orchestrator import Orchestrator

cache = Cache()
arb_finder = ArbitrageFinder()


def run_check():
    total_stake = load_stake()
    log_info("🔍 Scanning for arbitrage opportunities...")

    # 🔎 auto-discover scrapers in scrapers/
    scrapers = discover_scrapers()
    if not scrapers:
        log_error("❌ No valid scrapers discovered!")
        return

    # 🎯 orchestrator runs all scrapers (async/sync handled internally)
    orch = Orchestrator(scrapers)
    all_entries = orch.run_cycle()

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
