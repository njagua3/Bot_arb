# core/arbitrage.py
from core import db   # ✅ import database helpers

from typing import Dict, List, Tuple, Any, Optional, Callable
from collections import defaultdict
from datetime import datetime

from core.logger import log_info, log_success, log_error, log_warning
from core.cache import Cache   # ✅ class-based cache w/ smarter duplicate detection
from core.telegram import send_telegram_alert
from core.calculator import calculate_3way_arbitrage, calculate_2way_arbitrage
from utils.match_utils import normalize_market_name
from core.settings import load_settings  # ✅ dynamic config


# ------------------------------
# Data Structures
# ------------------------------

class ArbitrageOpportunity:
    """
    Represents a single arbitrage opportunity.
    """

    def __init__(
        self,
        match_label: str,
        market: str,
        sport: str,
        start_time: str,
        best_odds: Dict[str, float],
        best_books: Dict[str, str],
        best_urls: Dict[str, str],
        result: Dict[str, Any],
    ):
        self.match_label = match_label
        self.market = market
        self.sport = sport
        self.start_time = start_time
        self.best_odds = best_odds
        self.best_books = best_books
        self.best_urls = best_urls
        self.result = result

    def format_alert(self) -> str:
        """
        Builds a Telegram-ready HTML alert message.
        """
        best_lines = []
        for opt, odd in self.best_odds.items():
            book = self.best_books.get(opt, "")
            url = self.best_urls.get(opt, "")
            if url:
                best_lines.append(f"{opt} ➤ <a href='{url}'>{odd} ({book})</a>")
            else:
                best_lines.append(f"{opt} ➤ {odd} ({book})")

        stake_lines = []
        for opt in self.best_odds.keys():
            stake_val = self.result["stakes"].get(opt)
            if stake_val is not None:
                stake_lines.append(f"{opt} = {round(stake_val, 2)}")

        return (
            f"📣 <b>Arbitrage Opportunity</b>\n"
            f"🏟️ <b>{self.match_label}</b>\n"
            f"🎯 Market: <b>{self.market}</b>\n"
            f"📅 Match Time: <b>{self.start_time}</b>\n"
            f"⚽ Sport: {self.sport}\n\n"
            f"💰 Best Odds:\n" + "\n".join(best_lines) + "\n\n"
            f"📊 Stake Split (KES):\n" + "\n".join(stake_lines) + "\n\n"
            f"🟢 Profit: {round(self.result['profit'], 2)} KES\n"
            f"📈 Profit %: {round(self.result['roi'], 2)}%"
        )


# ------------------------------
# Helpers
# ------------------------------

def _safe_strptime(dt: str) -> str:
    if not dt:
        return ""
    try:
        return datetime.fromisoformat(dt.replace("Z", "")).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return dt


def _entry_to_common_shape(entry: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize odds entry from scrapers into consistent dict shape.
    """
    out = {
        "market": normalize_market_name(entry.get("market", "")),
        "odds": entry.get("odds") or {},
        "bookmaker": entry.get("bookmaker", ""),
        "url": entry.get("url") or entry.get("offer_url", ""),
        "sport": entry.get("sport", "Football"),
        "start_time": _safe_strptime(entry.get("match_time") or entry.get("start_time") or ""),
    }

    if "match" in entry and entry["match"]:
        out["match_label"] = entry["match"]
    else:
        home, away = (entry.get("home_team", "").strip(), entry.get("away_team", "").strip())
        out["match_label"] = f"{home} vs {away}" if home and away else home or away or "Unknown vs Unknown"

    return out


def _group_by_match_market(all_entries: List[Dict[str, Any]]) -> Dict[Tuple[str, str], List[Dict[str, Any]]]:
    grouped = defaultdict(list)
    for e in all_entries:
        ce = _entry_to_common_shape(e)
        grouped[(ce["match_label"], ce["market"])].append(ce)
    return grouped


def _best_prices(entries: List[Dict[str, Any]]):
    if not entries:
        return {}, {}, {}, "", "", ""

    base_opts = list(entries[0]["odds"].keys())
    best_odds, best_books, best_urls = {}, {}, {}

    for opt in base_opts:
        best_odds[opt], best_books[opt], best_urls[opt] = 0.0, "", ""

    for e in entries:
        for opt in base_opts:
            try:
                fv = float(e["odds"].get(opt, 0))
            except Exception:
                continue
            if fv > best_odds[opt] and fv > 1.0:
                best_odds[opt], best_books[opt], best_urls[opt] = fv, e["bookmaker"], e.get("url", "")

    return (
        best_odds,
        best_books,
        best_urls,
        entries[0].get("sport", "Football"),
        entries[0]["match_label"],
        entries[0].get("start_time", ""),
    )


# ------------------------------
# Arbitrage Finder
# ------------------------------

class ArbitrageFinder:
    """
    Detects arbitrage opportunities from normalized odds entries.
    """

    def __init__(self):
        settings = load_settings()
        self.total_stake = settings.get("stake", 10000)
        self.min_profit_percent = settings.get("min_profit_percent", 0.0)
        self.min_profit_absolute = settings.get("min_profit_absolute", 0.0)   # ✅ NEW
        expiry_minutes = settings.get("cache_expiry_minutes", 5)

        # ✅ smarter cache
        self.cache = Cache(expiry_seconds=expiry_minutes * 60)

    def _calc_arbitrage(self, best_odds: Dict[str, float]) -> Optional[Dict[str, Any]]:
        """
        Generic arbitrage calculator: supports 2-way and 3-way markets.
        """
        keys = list(best_odds.keys())
        num_outcomes = len(keys)

        if num_outcomes == 2:
            result = calculate_2way_arbitrage(*list(best_odds.values()), self.total_stake)
            if result and "stakes" in result:
                # Remap generic A/B back to real labels
                result["stakes"] = {
                    keys[0]: result["stakes"]["A"],
                    keys[1]: result["stakes"]["B"],
                }
            return result

        if num_outcomes == 3:
            try:
                return calculate_3way_arbitrage(*list(best_odds.values()), self.total_stake)
            except Exception as e:
                log_warning(f"Failed 3-way arb calc: {e} | Odds={best_odds}")
                return None

        log_info(f"Skipping {num_outcomes}-way market: {best_odds}")
        return None

    def scan_and_alert(self, all_entries: List[Dict[str, Any]], alert_sender=send_telegram_alert) -> int:
        grouped = _group_by_match_market(all_entries)
        alerts_sent = 0
        now_dt = datetime.utcnow()

        for (match_label, market), entries in grouped.items():
            if len(entries) < 2:
                continue

            best_odds, best_books, best_urls, sport, _, start_time_str = _best_prices(entries)
            if not best_odds or all(v <= 1.0 for v in best_odds.values()):
                continue

            # --- PREMATCH FILTER ---
            if start_time_str:
                try:
                    match_start_dt = datetime.fromisoformat(start_time_str.replace("Z", ""))
                    if match_start_dt <= now_dt:
                        log_info(f"⏭ Skipping past match: {match_label} ({start_time_str})")
                        continue
                except Exception:
                    pass  # if date parsing fails, we still proceed

            result = self._calc_arbitrage(best_odds)
            if not result:
                continue

            if result["roi"] < self.min_profit_percent:
                continue

            if result["profit"] < self.min_profit_absolute:
                continue

            # Persist into DB always
            try:
                db.upsert_match(
                    match_uid=f"{sport}:{match_label}:{market}",
                    home_team=match_label.split(" vs ")[0],
                    away_team=match_label.split(" vs ")[1] if " vs " in match_label else "",
                    market_name=market,
                    bookmaker=best_books.get(next(iter(best_books)), "Unknown"),
                    odds_dict=best_odds,
                    start_time_iso=start_time_str,
                    offer_url=best_urls.get(next(iter(best_urls)), "")
                )
            except Exception as e:
                log_error(f"⚠️ DB persistence failed for {match_label} {market}: {e}")

            # Send alert using updated cache
            cache_key = f"{sport}::{match_label}::{market}::{start_time_str}"
            result_cache = {
                "match": match_label,
                "market": market,
                "match_time": start_time_str,
                "profit": result["profit"],
                "roi": result["roi"],
                "odds": best_odds,
            }
            if not self.cache.is_duplicate_alert(cache_key, result_cache):
                log_success(
                    f"📣 ALERT: {match_label} | {market} | Profit: {round(result['profit'], 2)} | ROI: {round(result['roi'], 2)}%"
                )
                opportunity = ArbitrageOpportunity(
                    match_label, market, sport, start_time_str, best_odds, best_books, best_urls, result
                )
                try:
                    alert_sender(opportunity.format_alert())
                except Exception as e:
                    log_error(f"⚠️ Failed to send alert: {e}")
                else:
                    alerts_sent += 1
                    # Update last_sent in cache after sending
                    self.cache.store_alert(
                        match_label, market, start_time_str, result["profit"], result["roi"], best_odds
                    )
            else:
                log_info(f"⏭ Duplicate skipped: {cache_key}")

        # Cleanup expired cache entries after each scan
        self.cache.cleanup()
        return alerts_sent
