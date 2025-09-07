# core/arbitrage.py

from typing import Dict, List, Tuple, Any, Optional
from collections import defaultdict
from datetime import datetime
import time

from core import db
from core.logger import log_info, log_success, log_error, log_warning
from core.cache import Cache
from core.telegram import send_telegram_alert
from core.calculator import calculate_3way_arbitrage, calculate_2way_arbitrage
from utils.match_utils import normalize_market_name
from core.settings import load_settings
from utils.market_aliases import normalize_outcomes
from core.markets import Market


# ------------------------------
# Data Structures
# ------------------------------

class ArbitrageOpportunity:
    """Represents a single arbitrage opportunity."""

    def __init__(self, match_label: str, market_obj: Market, sport: str, start_time: str,
                 best_odds: Dict[str, float], best_books: Dict[str, str], best_urls: Dict[str, str],
                 result: Dict[str, Any], status: str = "new"):
        self.match_label = match_label
        self.market_obj = market_obj
        self.market = market_obj.name  # for backwards compatibility
        self.sport = sport
        self.start_time = start_time
        self.best_odds = best_odds
        self.best_books = best_books
        self.best_urls = best_urls
        self.result = result
        self.status = status  # "new" | "repeat"

    def format_alert(self) -> str:
        """Builds a Telegram-ready HTML alert message."""
        tag = "⚡ NEW" if self.status == "new" else "🔄 Update"

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
            f"{tag} <b>Arbitrage Opportunity</b>\n"
            f"🏟️ <b>{self.match_label}</b>\n"
            f"🎯 Market: <b>{self.market_obj.name}</b>\n"
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
    """Normalize odds entry from scrapers into consistent dict shape."""
    market_obj: Market = normalize_market_name(entry.get("market", ""))
    odds = normalize_outcomes(entry.get("odds") or {})

    out = {
        "market_obj": market_obj,
        "market": market_obj.name,  
        "odds": odds,
        "bookmaker": entry.get("bookmaker", ""),
        "url": entry.get("url") or entry.get("offer_url", ""),
        "sport": entry.get("sport", "Football"),
        "start_time": _safe_strptime(entry.get("match_time") or entry.get("start_time") or ""),
    }

    if entry.get("match"):
        out["match_label"] = entry["match"]
    else:
        home, away = (entry.get("home_team", "").strip(), entry.get("away_team", "").strip())
        out["match_label"] = f"{home} vs {away}" if home and away else home or away or "Unknown vs Unknown"

    return out


def _group_by_match_market(all_entries: List[Dict[str, Any]]) -> Dict[Tuple[str, str], List[Dict[str, Any]]]:
    grouped = defaultdict(list)
    for e in all_entries:
        ce = _entry_to_common_shape(e)
        grouped[(ce["match_label"], ce["market_obj"].name)].append(ce)
    return grouped


def _best_prices(entries: List[Dict[str, Any]]):
    """Pick best odds across bookmakers for a given market."""
    if not entries:
        return {}, {}, {}, "", "", "", None

    market_obj: Market = entries[0].get("market_obj")
    if not market_obj:
        return {}, {}, {}, "", "", "", None

    base_opts = market_obj.outcomes
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
        market_obj,
    )


# ------------------------------
# Arbitrage Finder
# ------------------------------

class ArbitrageFinder:
    """Detects arbitrage opportunities from normalized odds entries."""

    def __init__(self):
        settings = load_settings()
        self.total_stake = settings.get("stake", 10000)
        self.min_profit_percent = settings.get("min_profit_percent", 0.0)
        self.min_profit_absolute = settings.get("min_profit_absolute", 0.0)
        expiry_minutes = settings.get("cache_expiry_minutes", 5)
        self.cache = Cache(expiry_seconds=expiry_minutes * 60)

    def _calc_arbitrage(self, best_odds: Dict[str, float]) -> Optional[Dict[str, Any]]:
        keys = list(best_odds.keys())
        num_outcomes = len(keys)

        if num_outcomes == 2:
            result = calculate_2way_arbitrage(*list(best_odds.values()), self.total_stake)
            if result and "stakes" in result:
                result["stakes"] = {keys[0]: result["stakes"]["A"], keys[1]: result["stakes"]["B"]}
            return result

        if num_outcomes == 3:
            try:
                return calculate_3way_arbitrage(*list(best_odds.values()), self.total_stake)
            except Exception as e:
                log_warning(f"Failed 3-way arb calc: {e} | Odds={best_odds}")
                return None

        return None

    def scan_and_alert(self, all_entries: List[Dict[str, Any]], alert_sender=send_telegram_alert) -> int:
        """Scan all entries and send alerts for arbitrage opportunities."""
        start_time = time.perf_counter()
        alerts_sent, opportunities = 0, 0
        now_dt = datetime.utcnow()

        grouped = _group_by_match_market(all_entries)
        for (match_label, market), entries in grouped.items():
            if len(entries) < 2:
                continue
            best_odds, best_books, best_urls, sport, _, start_time_str, market_obj = _best_prices(entries)
            opportunities += 1
            alerts_sent += self._process_arb(match_label, market_obj, sport, start_time_str,
                                             best_odds, best_books, best_urls, now_dt, alert_sender)

        self.cache.cleanup()
        elapsed = time.perf_counter() - start_time
        log_info(f"🔎 Scan completed: {opportunities} markets checked | {alerts_sent} alerts sent | {elapsed:.2f}s")
        return alerts_sent

    def _process_arb(self, match_label: str, market_obj: Market, sport: str, start_time_str: str,
                     best_odds: Dict[str, float], best_books: Dict[str, str],
                     best_urls: Dict[str, str], now_dt: datetime, alert_sender) -> int:
        """Evaluate arbitrage and possibly send alert."""
        if not best_odds or all(v <= 1.0 for v in best_odds.values()):
            return 0

        if start_time_str:
            try:
                match_start_dt = datetime.fromisoformat(start_time_str.replace("Z", ""))
                if match_start_dt <= now_dt:
                    return 0
            except Exception:
                pass

        result = self._calc_arbitrage(best_odds)
        if not result:
            return 0
        if result["roi"] < self.min_profit_percent or result["profit"] < self.min_profit_absolute:
            return 0

        status = self.cache.check_alert_status(
            match_label, market_obj.name, start_time_str,
            result["profit"], result["roi"], best_odds
        )

        if status == "duplicate":
            return 0

        try:
            db.upsert_match(
                match_uid=f"{sport}:{match_label}:{market_obj.name}",
                home_team=match_label.split(" vs ")[0],
                away_team=match_label.split(" vs ")[1] if " vs " in match_label else "",
                market_name=market_obj.name,
                bookmaker=best_books.get(next(iter(best_books)), "Unknown"),
                odds_dict=best_odds,
                start_time_iso=start_time_str,
                offer_url=best_urls.get(next(iter(best_urls)), "")
            )
        except Exception as e:
            log_error(f"⚠️ DB persistence failed for {match_label} {market_obj.name}: {e}")

        opportunity = ArbitrageOpportunity(
            match_label, market_obj, sport, start_time_str,
            best_odds, best_books, best_urls, result, status=status
        )
        try:
            alert_sender(opportunity.format_alert())
        except Exception as e:
            log_error(f"⚠️ Failed to send alert: {e}")
            return 0
        else:
            log_success(
                f"📣 ALERT [{status.upper()}]: {match_label} | {market_obj.name} | "
                f"Profit: {round(result['profit'], 2)} | ROI: {round(result['roi'], 2)}%"
            )
            self.cache.store_alert(match_label, market_obj.name, start_time_str,
                                   result["profit"], result["roi"], best_odds)
            return 1
