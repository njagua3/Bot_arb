"""
Arbitrage Calculator Module (DB-backed scanner)

- Keeps your pure math helpers
- Adds a window scanner that:
  * pulls latest odds snapshots from DB
  * groups by (arb_event, market_name, line)
  * picks best odds per outcome across bookmakers (canonical 1/X/2)
  * computes arbitrage & stake split
  * returns opportunities; optional persistence or alerting can be added later
"""

from __future__ import annotations
from typing import Dict, Tuple, Optional, List, Any, Iterable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from collections import defaultdict

from core.db import get_latest_odds_for_window
from core.settings import load_settings

# --------------------------
# Pure math (your original)
# --------------------------

def calculate_implied_probabilities(odds: Dict[str, float]) -> Dict[str, float]:
    return {outcome: 1.0 / odd for outcome, odd in odds.items() if odd > 0}


def is_arbitrage(odds: Dict[str, float]) -> Tuple[bool, float]:
    implied = calculate_implied_probabilities(odds)
    total_prob = sum(implied.values())
    arbitrage_exists = total_prob < 1.0
    margin = (1.0 - total_prob) * 100.0 if arbitrage_exists else 0.0
    return arbitrage_exists, round(margin, 4)


def calculate_stakes(odds: Dict[str, float], total_stake: float) -> Optional[Dict[str, float]]:
    arbitrage, _ = is_arbitrage(odds)
    if not arbitrage:
        return None
    implied = calculate_implied_probabilities(odds)
    total_prob = sum(implied.values())
    stakes = {o: round((total_stake * (1.0 / odd)) / total_prob, 2) for o, odd in odds.items()}
    return stakes


def calculate_expected_profit(odds: Dict[str, float], stakes: Dict[str, float]) -> float:
    if not stakes:
        return 0.0
    any_outcome = next(iter(stakes))
    payout = odds[any_outcome] * stakes[any_outcome]
    total = sum(stakes.values())
    return round(payout - total, 2)


def calculate_arbitrage(
    odds: Dict[str, float],
    total_stake: float,
    min_profit: float = 1.0
) -> Optional[Dict[str, Any]]:
    if len(odds) not in (2, 3):
        return None

    arbitrage, margin = is_arbitrage(odds)
    if not arbitrage:
        return None

    stakes = calculate_stakes(odds, total_stake)
    if not stakes:
        return None

    payouts = {o: round(stakes[o] * odds[o], 2) for o in stakes}
    total_investment = sum(stakes.values())
    profit = round(next(iter(payouts.values())) - total_investment, 2)

    if profit < float(min_profit):
        return None

    roi = round((profit / total_investment) * 100.0, 2) if total_investment > 0 else 0.0
    payout = profit + total_investment

    return {
        "stakes": stakes,
        "payouts": payouts,
        "profit": profit,
        "roi": roi,
        "payout": round(payout, 2),
        "margin": margin,
    }

# ----------------------------------
# DB-backed window scanner for MVP
# ----------------------------------

@dataclass
class Leg:
    bookmaker_id: int
    outcome: str
    odds: float

@dataclass
class MarketBookBest:
    # best odds per outcome across books (canonical '1','x','2')
    best: Dict[str, Leg]  # outcome -> Leg

@dataclass
class Opportunity:
    arb_event_id: int
    market_name: str           # e.g. "1X2" or "Over/Under 2.5"
    line: Optional[str]
    start_time: datetime
    odds: Dict[str, float]     # best odds per outcome (decimal)
    legs: Dict[str, Dict[str, Any]]  # outcome -> {bookmaker_id, odds}
    profit: float
    roi: float
    margin: float
    stakes: Optional[Dict[str, float]] = None  # attach for Telegram formatting

def _now_utc() -> datetime:
    return datetime.now(tz=timezone.utc)


def _group_rows_by_event_market(rows: List[Dict[str, Any]]) -> Dict[tuple[int, str, Optional[str]], List[Dict[str, Any]]]:
    """
    Group odds rows by (arb_event_id, market_name, line).
    rows contain: arb_event_id, start_time, market_id, market_name, line,
                  bookmaker_id, outcome, value, home_team, away_team
    """
    groups: Dict[tuple[int, str, Optional[str]], List[Dict[str, Any]]] = defaultdict(list)
    for r in rows:
        key = (int(r["arb_event_id"]), str(r["market_name"]), r["line"] if r["line"] is not None else None)
        groups[key].append(r)
    return groups

# --- Outcome canonicalization: map raw (team names / aliases) -> {'1','x','2'} ---
def _canon_outcome(raw: str, home: Optional[str], away: Optional[str]) -> Optional[str]:
    s = (raw or "").strip().casefold()
    if s in {"x", "draw"}:
        return "x"

    # Exact, case-insensitive team matches
    hs = (home or "").strip().casefold()
    as_ = (away or "").strip().casefold()
    if hs and s == hs:
        return "1"
    if as_ and s == as_:
        return "2"

    # Fallback aliases often seen in scrapers
    if s in {"1", "home", "1 (home)"}:
        return "1"
    if s in {"2", "away", "2 (away)"}:
        return "2"

    return None  # unknown for this market/event


def _best_per_outcome(rows: List[Dict[str, Any]]) -> MarketBookBest:
    """
    Build best odds per canonical outcome {'1','x','2'} across all bookmakers.

    rows contain: arb_event_id, start_time, market_id, market_name, line,
                  bookmaker_id, outcome, value, home_team, away_team
    """
    best: Dict[str, Leg] = {}

    # assume all rows in this bucket are same event; pull names from the first row
    home = (rows[0].get("home_team") or "") if rows else ""
    away = (rows[0].get("away_team") or "") if rows else ""

    for r in rows:
        raw_outcome = str(r["outcome"])
        key = _canon_outcome(raw_outcome, home, away)
        if key is None:
            # skip outcomes not relevant/canonical for this event
            continue
        val = float(r["value"])
        bm = int(r["bookmaker_id"])
        cur = best.get(key)
        if (cur is None) or (val > cur.odds):
            best[key] = Leg(bookmaker_id=bm, outcome=key, odds=val)

    return MarketBookBest(best=best)


def _is_supported_outcome_set(outcomes: Iterable[str]) -> bool:
    outs = set(outcomes)
    return outs.issubset({"1", "x", "2"}) and (2 <= len(outs) <= 3)


def _calc_from_best(
    arb_event_id: int,
    market_name: str,
    line: Optional[str],
    start_time: datetime,
    best: MarketBookBest,
    total_stake: float,
    min_profit_abs: float
) -> Optional[Opportunity]:
    odds = {o: leg.odds for o, leg in best.best.items()}
    if not _is_supported_outcome_set(odds.keys()):
        return None

    ar = calculate_arbitrage(odds, total_stake=total_stake, min_profit=min_profit_abs)
    if not ar:
        return None

    legs = {
        o: {"bookmaker_id": best.best[o].bookmaker_id, "odds": best.best[o].odds}
        for o in odds
    }

    return Opportunity(
        arb_event_id=arb_event_id,
        market_name=market_name,
        line=line,
        start_time=start_time,
        odds=odds,
        legs=legs,
        profit=ar["profit"],
        roi=ar["roi"],
        margin=ar["margin"],
        stakes=ar.get("stakes"),
    )


def run_calc_window(
    sport_id: int,
    hours: int = 48,
    market_names: Optional[List[str]] = None,
    min_profit_percent: Optional[float] = None,
    min_profit_absolute: Optional[float] = None,
    stake: Optional[float] = None,
) -> List[Opportunity]:
    """
    Scan next `hours` for arbitrage opportunities using latest DB odds.

    Groups by canonical arb_event_id (shared across bookmakers).
    """
    s = load_settings()
    stake = float(stake if stake is not None else s.stake)
    min_profit_percent = float(min_profit_percent if min_profit_percent is not None else s.min_profit_percent)
    min_profit_absolute = float(min_profit_absolute if min_profit_absolute is not None else s.min_profit_absolute)

    market_names = market_names or ["1X2"]

    now = _now_utc()
    end = now + timedelta(hours=hours)

    rows = get_latest_odds_for_window(
        sport_id=sport_id,
        start_from=now,
        start_to=end,
        market_names=market_names,
        include_lines=True,
    )

    groups = _group_rows_by_event_market(rows)

    opps: List[Opportunity] = []
    for (arb_event_id, market_name, line), bucket in groups.items():
        if not bucket:
            continue

        start_time = bucket[0]["start_time"]
        if isinstance(start_time, str):
            try:
                start_time = datetime.fromisoformat(start_time.replace("Z", "+00:00"))
            except Exception:
                start_time = now

        best = _best_per_outcome(bucket)
        opp = _calc_from_best(
            arb_event_id=arb_event_id,
            market_name=market_name,
            line=line,
            start_time=start_time,
            best=best,
            total_stake=stake,
            min_profit_abs=min_profit_absolute,
        )
        if not opp:
            continue

        if opp.margin < float(min_profit_percent):
            continue

        opps.append(opp)

    opps.sort(key=lambda o: (-o.roi, o.start_time))
    return opps


# --------------------------
# CLI for quick dry runs
# --------------------------
if __name__ == "__main__":
    import argparse, json

    ap = argparse.ArgumentParser(description="Arbitrage calculator window scan")
    ap.add_argument("--sport", type=int, default=14, help="Sport ID (e.g., 14=Soccer)")
    ap.add_argument("--hours", type=int, default=48)
    ap.add_argument("--markets", nargs="*", help="DB market names (e.g., 1X2 'Over/Under 2.5')")
    ap.add_argument("--min-profit-pct", type=float, help="Minimum margin percent")
    ap.add_argument("--min-profit-abs", type=float, help="Minimum absolute profit (KES)")
    ap.add_argument("--stake", type=float, help="Stake amount")
    ap.add_argument("--limit", type=int, default=20, help="Max results to print")
    args = ap.parse_args()

    opps = run_calc_window(
        sport_id=args.sport,
        hours=args.hours,
        market_names=args.markets,
        min_profit_percent=args.min_profit_pct,
        min_profit_absolute=args.min_profit_abs,
        stake=args.stake,
    )

    for i, o in enumerate(opps[: args.limit], 1):
        print(f"{i:02d}. {o.market_name}{(' ' + str(o.line)) if o.line else ''} | "
              f"arb_event_id={o.arb_event_id} | KO={o.start_time} | "
              f"profit={o.profit} | roi={o.roi}% | margin={o.margin}%")
        for outcome, leg in o.legs.items():
            print(f"    - {outcome}: {leg['odds']} (bookmaker_id={leg['bookmaker_id']})")
