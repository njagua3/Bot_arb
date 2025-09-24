# main.py
from __future__ import annotations
import os
import sys
import time
import signal
import atexit
import threading
import traceback
from pathlib import Path
from typing import List, Optional

from core.logger import get_logger, log_error, log_info, log_success
from core.settings import load_settings, get_scan_interval, get_target_markets
from core.db import init_db, resolve_sport_id
from core.arbitrage import scan_and_alert_db
from core.telegram import run_bot

# Optional: use your scraper orchestrator per cycle (so fresh odds land in DB)
from scrapers.scraper_loader import discover_scrapers
from scrapers.orchestrator import ScraperOrchestrator
from scrapers.base_scraper import BaseScraper
from scrapers.async_base_scraper import AsyncBaseScraper
import asyncio

LOG = get_logger(__name__)

# -------------------------
# single-instance lock
# -------------------------
_LOCKFILE = Path("data/.arb_scanner.lock")

def _write_lock():
    _LOCKFILE.parent.mkdir(parents=True, exist_ok=True)
    if _LOCKFILE.exists():
        existing = _LOCKFILE.read_text().strip()
        LOG.warning(f"🔒 Lock exists (PID={existing}). If this is stale, delete {_LOCKFILE}.")
        raise SystemExit(1)
    _LOCKFILE.write_text(str(os.getpid()))
    atexit.register(_cleanup_lock)

def _cleanup_lock():
    try:
        if _LOCKFILE.exists():
            _LOCKFILE.unlink()
    except Exception:
        pass

# -------------------------
# graceful shutdown
# -------------------------
_STOP = False
def _handle_sig(signum, frame):
    global _STOP
    _STOP = True
    LOG.info(f"👋 Received signal {signum}; will stop after this cycle.")

for sig in (signal.SIGINT, signal.SIGTERM):
    signal.signal(sig, _handle_sig)

# -------------------------
# CLI
# -------------------------
def _parse_args():
    import argparse
    ap = argparse.ArgumentParser(description="Arbitrage scanner (DB-backed) + Telegram bot")
    ap.add_argument("--sport", type=int, default=None, help="Internal sport_id (DB id). If omitted, resolves from --sport-name or 'Soccer'.")
    ap.add_argument("--sport-name", type=str, default=None, help="Canonical sport name (e.g., 'Soccer').")
    ap.add_argument("--hours", type=int, default=48, help="Look-ahead window (hours).")
    ap.add_argument("--markets", nargs="*", help="DB market labels (e.g., 1X2 'Match Winner' 'Double Chance' 'Over/Under' 'Handicap 0').")
    ap.add_argument("--limit", type=int, default=10, help="Max Telegram alerts to send per scan.")
    ap.add_argument("--interval", type=int, default=None, help="Scan interval seconds (default: settings.scan_interval).")
    ap.add_argument("--loop", action="store_true", help="Run continuously.")
    ap.add_argument("--no-bot", action="store_true", help="Do not start Telegram bot thread.")
    ap.add_argument("--scrape-each-cycle", action="store_true", help="Run scrapers before each scan (writes fresh odds to DB).")
    return ap.parse_args()

# -------------------------
# Optional: scrapers runner
# -------------------------
def _run_scrapers_once() -> int:
    """Run all discovered scrapers; return total entries pushed to DB."""
    try:
        scrapers = discover_scrapers()
    except Exception as e:
        log_error(f"❌ discover_scrapers failed: {e}")
        return 0

    if not scrapers:
        log_error("❌ No valid scrapers discovered.")
        return 0

    # Try orchestrator first
    try:
        orch = ScraperOrchestrator(scrapers)
        result = orch.run()
        n = len(result.get("matches", [])) if isinstance(result, dict) else 0
        log_success(f"✅ Orchestrator executed. Entries: {n}")
        return n
    except Exception as e:
        log_error(f"⚠️ Orchestrator failed ({e}); falling back to direct execution.")

    # Fallback direct exec (sync/async)
    total = 0
    for s in scrapers:
        try:
            if isinstance(s, BaseScraper):
                entries = s.get_odds()
            elif isinstance(s, AsyncBaseScraper):
                entries = asyncio.run(s.get_odds())
            else:
                log_error(f"❌ Unknown scraper type: {type(s)}")
                continue
            n = len(entries or [])
            total += n
            if n:
                log_info(f"✅ {s.bookmaker} produced {n} entries (fallback).")
            else:
                log_info(f"⚠️ {s.bookmaker} produced no entries.")
        except Exception as ex:
            log_error(f"❌ Fallback scraper {getattr(s, 'bookmaker', '<?>')} failed: {ex}")
            traceback.print_exc()
    return total

# -------------------------
# Telegram bot thread
# -------------------------
def _start_bot_thread():
    def _runner():
        try:
            log_info("🤖 Starting Telegram bot…")
            run_bot()  # blocking
        except Exception as e:
            log_error(f"⚠️ Telegram bot crashed: {e}")
    t = threading.Thread(target=_runner, name="telegram-bot", daemon=True)
    t.start()
    return t

# -------------------------
# Resolve sport
# -------------------------
def _resolve_sport_id(sport_id: Optional[int], sport_name: Optional[str]) -> int:
    if isinstance(sport_id, int) and sport_id > 0:
        return sport_id
    if sport_name:
        return resolve_sport_id(sport_name)
    return resolve_sport_id("Soccer")

# -------------------------
# One scan cycle
# -------------------------
def _scan_once(
    sport_id: int,
    hours: int,
    markets: List[str],
    limit: int,
    scrape_before: bool,
) -> int:
    if scrape_before:
        _ = _run_scrapers_once()

    try:
        sent = scan_and_alert_db(
            sport_id=sport_id,
            hours=hours,
            market_names=markets,
            max_send=limit,
            sport_name=None,  # already resolved id
        )
        return int(sent or 0)
    except Exception as e:
        log_error(f"❌ scan_and_alert_db failed: {e}")
        return 0

# -------------------------
# Main
# -------------------------
def main():
    args = _parse_args()
    s = load_settings()
    init_db()

    if args.loop:
        _write_lock()

    # config
    interval = int(args.interval) if args.interval is not None else int(get_scan_interval())
    markets = args.markets or list(get_target_markets())
    resolved_sport_id = _resolve_sport_id(args.sport, args.sport_name)

    log_info(
        f"🚀 Arbitrage Bot up.\n"
        f"   sport={args.sport or args.sport_name or 'Soccer'} (id={resolved_sport_id})\n"
        f"   hours={args.hours}, markets={markets}, per-scan limit={args.limit}\n"
        f"   loop={bool(args.loop)}, interval={interval}s, scrape_each_cycle={bool(args.scrape_each_cycle)}"
    )

    # Start Telegram bot thread unless disabled
    if not args.no_bot:
        _start_bot_thread()

    if not args.loop:
        sent = _scan_once(resolved_sport_id, args.hours, markets, args.limit, args.scrape_each_cycle)
        log_success(f"✅ One-shot scan complete. Alerts sent: {sent}")
        return

    # Loop mode
    total_sent = 0
    try:
        while not _STOP:
            cycle_start = time.time()
            sent = _scan_once(resolved_sport_id, args.hours, markets, args.limit, args.scrape_each_cycle)
            total_sent += sent
            log_success(f"✅ Scan cycle done. Sent {sent} (total {total_sent}).")

            # sleep to next tick, but remain responsive to signals
            remaining = max(1.0, interval - (time.time() - cycle_start))
            end_at = time.time() + remaining
            while time.time() < end_at:
                if _STOP:
                    break
                time.sleep(1)
            if _STOP:
                break
    finally:
        _cleanup_lock()
        log_info("👋 Stopped. Bye!")

if __name__ == "__main__":
    main()
