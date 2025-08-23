# main.py
import time
import concurrent.futures
import signal
import sys
import threading

from core.settings import load_stake, SCAN_INTERVAL
from core.logger import log_error, log_info
from core.cache import Cache
from core.arbitrage import ArbitrageFinder   # ✅ use the class, not a missing function
from core.telegram import send_telegram_alert, run_bot


# Import scrapers
from scrapers.odibets import get_odds as get_odibets
from scrapers.betika import get_odds as get_betika
from scrapers.sportpesa import get_odds as get_sportpesa
from core.db import init_db 

# Ensure DB schema is created
init_db()
# Global cache
cache = Cache()

# Global arbitrage finder
arb_finder = ArbitrageFinder()


def run_check():
    total_stake = load_stake()  # ✅ dynamic stake reload
    log_info("🔍 Scanning for arbitrage opportunities...")

    scrapers = [get_odibets, get_betika, get_sportpesa]
    all_entries = []

    # ✅ parallel scraping
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = {executor.submit(scraper): scraper.__name__ for scraper in scrapers}
        for future in concurrent.futures.as_completed(futures):
            scraper_name = futures[future]
            try:
                all_entries.extend(future.result())
            except Exception as e:
                log_error(f"❌ Error in scraper {scraper_name}: {e}")

    # Run arbitrage detection pipeline
    alerts_sent = arb_finder.scan_and_alert(all_entries)

    log_info(f"📨 Alerts sent: {alerts_sent}")

    # Clean old cache
    cache.cleanup()


def shutdown_handler(sig, frame):
    """ ✅ Graceful shutdown on CTRL+C """
    log_info("🛑 Shutting down arbitrage bot gracefully...")
    cache.cleanup()
    sys.exit(0)


def start_scanner():
    """Loop that continuously runs arbitrage scans"""
    while True:
        run_check()
        log_info(f"⏳ Waiting {SCAN_INTERVAL} seconds before next scan...")
        time.sleep(SCAN_INTERVAL)


if __name__ == "__main__":
    log_info("🚀 Arbitrage Bot Started!")
    cache.load()

    # ✅ catch CTRL+C / kill
    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    # Start Telegram bot in a separate thread
    bot_thread = threading.Thread(target=run_bot, daemon=True)
    bot_thread.start()

    # Start scanner loop in main thread
    start_scanner()
