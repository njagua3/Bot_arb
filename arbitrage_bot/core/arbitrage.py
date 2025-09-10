# core/arbitrage.py

from typing import Dict, List, Tuple, Any, Optional, NamedTuple
from collections import defaultdict
from datetime import datetime, timezone
import time

from core import db
from core.db import reset_db   # ✅ new import for safe testing
from core.logger import log_info, log_success, log_error, log_warning
from core.cache import Cache
from core.telegram import send_telegram_alert
from core.calculator import calculate_3way_arbitrage, calculate_2way_arbitrage
from utils.match_utils import normalize_market_name
from core.settings import load_settings
from utils.market_aliases import normalize_outcomes
from core.markets import Market


# ------------------------------
# Fallback SimpleMarket
# ------------------------------
class SimpleMarket:
    """Minimal fallback Market-like object used when normalize_market_name fails."""
    def __init__(self, name: str, outcomes: Optional[List[str]] = None):
        self.name = name or "Unknown"
        self.outcomes = outcomes or []
    def __repr__(self):
        return f"<SimpleMarket name={self.name} outcomes={self.outcomes}>"


# ------------------------------
# Data Structures
# ------------------------------
class ArbitrageOpportunity:
    """Represents a single arbitrage opportunity."""

    def __init__(self, match_label: str, market_obj: Market, sport: str, start_time: Optional[datetime],
                 best_odds: Dict[str, float], best_books: Dict[str, str], best_urls: Dict[str, str],
                 result: Dict[str, Any], status: str = "new"):
        self.match_label = match_label
        self.market_obj = market_obj
        self.market = getattr(market_obj, "name", str(market_obj))  # backwards compat
        self.sport = sport
        self.start_time_dt = start_time
        self.start_time = start_time.astimezone(timezone.utc).isoformat() if start_time else ""
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
        stakes = self.result.get("stakes", {}) or {}
        for opt in self.best_odds.keys():
            stake_val = stakes.get(opt)
            if stake_val is not None:
                stake_lines.append(f"{opt} = {round(stake_val, 2)}")

        return (
            f"{tag} <b>Arbitrage Opportunity</b>\n"
            f"🏟️ <b>{self.match_label}</b>\n"
            f"🎯 Market: <b>{self.market}</b>\n"
            f"📅 Match Time: <b>{self.start_time or 'Unknown'}</b>\n"
            f"⚽ Sport: {self.sport}\n\n"
            f"💰 Best Odds:\n" + "\n".join(best_lines) + "\n\n"
            f"📊 Stake Split (KES):\n" + "\n".join(stake_lines) + "\n\n"
            f"🟢 Profit: {round(self.result.get('profit', 0.0), 2)} KES\n"
            f"📈 Profit %: {round(self.result.get('roi', 0.0), 2)}%"
        )


# ------------------------------
# Helpers
# ------------------------------
def _parse_iso_to_dt(dt_str: Optional[str]) -> Optional[datetime]:
    """Parse ISO datetime into UTC datetime."""
    if not dt_str:
        return None
    try:
        s = dt_str.strip()
        if s.endswith("Z"):
            s = s[:-1]
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return dt
    except Exception:
        return None


def _entry_to_common_shape(entry: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize odds entry from scrapers into consistent dict shape."""
    raw_market = entry.get("market") or ""
    raw_odds = entry.get("odds") or {}

    try:
        market_obj = normalize_market_name(raw_market or "")
        if not getattr(market_obj, "outcomes", None):
            inferred = list(raw_odds.keys()) if isinstance(raw_odds, dict) else []
            market_obj = SimpleMarket(getattr(market_obj, "name", str(raw_market or "Unknown")), inferred)
    except Exception as e:
        log_warning(f"Market normalization failed for '{raw_market}': {e}")
        inferred = list(raw_odds.keys()) if isinstance(raw_odds, dict) else []
        market_obj = SimpleMarket(raw_market or "Unknown", inferred)

    try:
        odds = normalize_outcomes(raw_odds or {})
    except Exception as e:
        log_warning(f"Outcome normalization failed for market '{raw_market}': {e}")
        odds = {}
        if isinstance(raw_odds, dict):
            for k, v in raw_odds.items():
                try:
                    odds[str(k).strip()] = float(v)
                except Exception:
                    continue

    start_time_raw = entry.get("match_time") or entry.get("start_time") or ""
    start_time_dt = _parse_iso_to_dt(start_time_raw)

    out = {
        "market_obj": market_obj,
        "market": getattr(market_obj, "name", str(raw_market)),
        "odds": odds,
        "bookmaker": entry.get("bookmaker", "") or entry.get("bookie", ""),
        "url": entry.get("url") or entry.get("offer_url") or entry.get("link") or "",
        "sport": entry.get("sport", "Football"),
        "start_time_dt": start_time_dt,
    }

    if entry.get("match"):
        out["match_label"] = entry["match"]
    else:
        home = (entry.get("home_team") or entry.get("home") or "").strip()
        away = (entry.get("away_team") or entry.get("away") or "").strip()
        out["match_label"] = f"{home} vs {away}" if home and away else home or away or "Unknown vs Unknown"

    return out


def _group_by_match_market(all_entries: List[Dict[str, Any]]) -> Dict[Tuple[str, str], List[Dict[str, Any]]]:
    grouped = defaultdict(list)
    for e in all_entries:
        try:
            ce = _entry_to_common_shape(e)
            key = (ce["match_label"], getattr(ce["market_obj"], "name", ce.get("market", "Unknown")))
            grouped[key].append(ce)
        except Exception as ex:
            log_warning(f"Skipping malformed entry during grouping: {ex}")
    return grouped


# ------------------------------
# Best Prices
# ------------------------------
class BestPrices(NamedTuple):
    best_odds: Dict[str, float]
    best_books: Dict[str, str]
    best_urls: Dict[str, str]
    sport: str
    match_label: str
    start_time_dt: Optional[datetime]
    market_obj: Any
    offer_url: str


def _best_prices(entries: List[Dict[str, Any]]) -> BestPrices:
    """Pick best odds across bookmakers for a given market."""
    if not entries:
        return BestPrices({}, {}, {}, "Football", "Unknown vs Unknown", None, None, "")

    market_obj = entries[0].get("market_obj")
    if not market_obj:
        return BestPrices({}, {}, {}, "Football", "Unknown vs Unknown", None, None, "")

    base_opts = getattr(market_obj, "outcomes", []) or list(entries[0].get("odds", {}).keys())

    best_odds: Dict[str, float] = {opt: 0.0 for opt in base_opts}
    best_books: Dict[str, str] = {opt: "" for opt in base_opts}
    best_urls: Dict[str, str] = {opt: "" for opt in base_opts}

    for e in entries:
        for opt in base_opts:
            try:
                fv = float(e["odds"].get(opt, 0))
            except Exception:
                continue
            if fv > best_odds[opt] and fv > 1.0:
                best_odds[opt] = fv
                best_books[opt] = e.get("bookmaker", "")
                best_urls[opt] = e.get("url", "")

    offer_url = next((u for u in best_urls.values() if u), "")

    return BestPrices(
        best_odds=best_odds,
        best_books=best_books,
        best_urls=best_urls,
        sport=entries[0].get("sport", "Football"),
        match_label=entries[0].get("match_label", "Unknown vs Unknown"),
        start_time_dt=entries[0].get("start_time_dt"),
        market_obj=market_obj,
        offer_url=offer_url,
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
        if len(keys) == 2:
            result = calculate_2way_arbitrage(*list(best_odds.values()), self.total_stake)
            if result and "stakes" in result:
                try:
                    result["stakes"] = {keys[0]: result["stakes"]["A"], keys[1]: result["stakes"]["B"]}
                except Exception:
                    vals = list(result["stakes"].values())
                    result["stakes"] = {keys[i]: vals[i] for i in range(min(len(keys), len(vals)))}
            return result
        if len(keys) == 3:
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
        now_dt = datetime.now(timezone.utc)

        grouped = _group_by_match_market(all_entries)
        for (match_label, market), entries in grouped.items():
            if len(entries) < 2:
                continue
            prices = _best_prices(entries)
            opportunities += 1
            alerts_sent += self._process_arb(
                prices.match_label, prices.market_obj, prices.sport, prices.start_time_dt,
                prices.best_odds, prices.best_books, prices.best_urls,
                prices.offer_url, now_dt, alert_sender
            )

        self.cache.cleanup()
        elapsed = time.perf_counter() - start_time
        log_info(f"🔎 Scan completed: {opportunities} markets checked | {alerts_sent} alerts sent | {elapsed:.2f}s")
        return alerts_sent

    def _process_arb(self, match_label: str, market_obj: Market, sport: str, start_time_dt: Optional[datetime],
                     best_odds: Dict[str, float], best_books: Dict[str, str],
                     best_urls: Dict[str, str], offer_url: str, now_dt: datetime,
                     alert_sender, persist_to_db: bool = True) -> int:
        if not best_odds or all(v <= 1.0 for v in best_odds.values()):
            return 0
        if start_time_dt and start_time_dt <= now_dt:
            return 0

        result = self._calc_arbitrage(best_odds)
        if not result:
            return 0
        if result.get("roi", 0.0) < self.min_profit_percent or result.get("profit", 0.0) < self.min_profit_absolute:
            log_info(
                f"Arb found but below thresholds: {match_label} | {market_obj.name} | "
                f"profit={result.get('profit', 0.0)} ROI={result.get('roi', 0.0)}%"
            )
            return 0

        start_time_iso = start_time_dt.astimezone(timezone.utc).isoformat() if start_time_dt else ""
        status = self.cache.check_alert_status(
            match_label, getattr(market_obj, "name", str(market_obj)), start_time_iso,
            result["profit"], result["roi"], best_odds
        )
        if status == "duplicate":
            return 0

        if persist_to_db:
            try:
                db.upsert_match(
                    match_uid=f"{sport}:{match_label}:{getattr(market_obj, 'name', str(market_obj))}",
                    home_team=match_label.split(" vs ")[0],
                    away_team=match_label.split(" vs ")[1] if " vs " in match_label else "",
                    market_name=getattr(market_obj, "name", str(market_obj)),
                    bookmaker=best_books.get(next(iter(best_books)), "Unknown"),
                    odds_dict=best_odds,
                    start_time_iso=start_time_iso,
                    offer_url=offer_url or ""
                )
            except Exception as e:
                log_error(f"⚠️ DB persistence failed for {match_label} {getattr(market_obj, 'name', '')}: {e}")

        opportunity = ArbitrageOpportunity(
            match_label, market_obj, sport, start_time_dt,
            best_odds, best_books, best_urls, result, status=status
        )
        try:
            alert_sender(opportunity.format_alert())
        except Exception as e:
            log_error(f"⚠️ Failed to send alert: {e}")
            return 0
        else:
            log_success(
                f"📣 ALERT [{status.upper()}]: {match_label} | {getattr(market_obj, 'name', '')} | "
                f"Profit: {round(result.get('profit', 0.0), 2)} | ROI: {round(result.get('roi', 0.0), 2)}%"
            )
            self.cache.store_alert(match_label, getattr(market_obj, "name", str(market_obj)), start_time_iso,
                                   result["profit"], result["roi"], best_odds)
            return 1
