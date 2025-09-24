"""
Arbitrage Calculator Module (DB-backed scanner)

- Keeps pure math helpers
- Adds a window scanner that:
  * pulls latest odds snapshots from DB
  * normalizes market keys (core.markets.normalize_market)
  * groups by (arb_event, market_key, line)
  * picks best odds per canonical outcome across bookmakers
  * computes arbitrage & stake split
  * supports cross-market 2-leg (e.g., AH0+X2) and 3-leg (AH0+X+2) combos
"""

from __future__ import annotations
from typing import Dict, Tuple, Optional, List, Any, Iterable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from collections import defaultdict

from core.db import get_latest_odds_for_window
from core.settings import load_settings

# NEW: market normalizer/specs
from core.markets import normalize_market, market_label_from_key, MarketSpec

# --------------------------
# Pure math (original)
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
# DB-backed / market-normalized scan
# ----------------------------------

@dataclass
class Leg:
    bookmaker_id: int
    outcome: str
    odds: float

@dataclass
class MarketBookBest:
    best: Dict[str, Leg]  # outcome -> Leg

@dataclass
class Opportunity:
    arb_event_id: int
    market_name: str           # e.g. "1X2", "Over/Under 2.5", "Handicap 0 + Double Chance"
    line: Optional[str]
    start_time: datetime
    odds: Dict[str, float]     # outcome label -> decimal odds
    legs: Dict[str, Dict[str, Any]]  # label -> {bookmaker_id, odds}
    profit: float
    roi: float
    margin: float
    stakes: Optional[Dict[str, float]] = None

def _now_utc() -> datetime:
    return datetime.now(tz=timezone.utc)

# ------------- helpers: grouping & best per market ----------------

def _group_rows_by_event_marketkey(rows: List[Dict[str, Any]]):
    """
    Buckets by (arb_event_id, market_key, line).
    market_key/line come from core.markets.normalize_market().
    """
    buckets = defaultdict(list)
    for r in rows:
        ms: MarketSpec = normalize_market(str(r["market_name"]))
        line = ms.line
        if r.get("line") is not None:
            line = str(r["line"])
        buckets[(int(r["arb_event_id"]), ms.market_key, line)].append(r)
    return buckets

def _best_per_market(rows: List[Dict[str, Any]], ms: MarketSpec) -> Dict[str, Leg]:
    """
    Pick best odds per canonical outcome for the normalized market.
    Handles 1x2, ml, dc, ah:0, ou:<line>.
    """
    home = (rows[0].get("home_team") or "").strip().casefold() if rows else ""
    away = (rows[0].get("away_team") or "").strip().casefold() if rows else ""

    def map_outcome(raw: str) -> Optional[str]:
        s = (raw or "").strip()
        sl = s.casefold()

        # 1X2
        if ms.market_key == "1x2":
            if sl in {"x", "draw"}: return "X"
            if home and sl == home: return "1"
            if away and sl == away: return "2"
            if sl in {"1","home","1 (home)"}: return "1"
            if sl in {"2","away","2 (away)"}: return "2"
            return None

        # ML (two-way)
        if ms.market_key == "ml":
            if home and sl == home: return "1"
            if away and sl == away: return "2"
            if sl in {"1","home","1 (home)"}: return "1"
            if sl in {"2","away","2 (away)"}: return "2"
            return None

        # Double Chance
        if ms.market_key == "dc":
            if sl in {"1x","1-x","1 or x","home or draw"}: return "1X"
            if sl in {"x2","x-2","draw or away"}: return "X2"
            if sl in {"12","1-2","home or away","no draw"}: return "12"
            if sl == "double chance 1x": return "1X"
            if sl == "double chance x2": return "X2"
            if sl == "double chance 12": return "12"
            return None

        # AH 0.0 = Draw No Bet
        if ms.market_key == "ah:0":
            if home and sl == home: return "Home"
            if away and sl == away: return "Away"
            if sl in {"home","1","home (0)","ah0 home","dnb home"}: return "Home"
            if sl in {"away","2","away (0)","ah0 away","dnb away"}: return "Away"
            return None

        # OU L
        if ms.market_key.startswith("ou:"):
            line = ms.line or ""
            if sl.startswith("over") and line in s:  return f"Over {line}"
            if sl.startswith("under") and line in s: return f"Under {line}"
            if sl in {"o","over"}:   return f"Over {line}"
            if sl in {"u","under"}:  return f"Under {line}"
            return None

        return None

    best: Dict[str, Leg] = {}
    for r in rows:
        lab = map_outcome(str(r["outcome"]))
        if lab is None:
            continue
        if ms.outcomes and lab not in ms.outcomes:
            continue
        val = float(r["value"])
        bm  = int(r["bookmaker_id"])
        cur = best.get(lab)
        if (cur is None) or (val > cur.odds):
            best[lab] = Leg(bookmaker_id=bm, outcome=lab, odds=val)
    return best

# ------------- 2-leg arb math (generic) ----------------

def _two_way_arb(odd_a: float, odd_b: float, total_stake: float):
    if odd_a <= 1.0 or odd_b <= 1.0:
        return None
    inv = (1.0/odd_a) + (1.0/odd_b)
    if inv >= 1.0:
        return None
    s_a = round(total_stake * (odd_b/(odd_a+odd_b)), 2)
    s_b = round(total_stake * (odd_a/(odd_a+odd_b)), 2)
    payout = round(s_a*odd_a, 2)  # = s_b*odd_b
    invested = s_a + s_b
    profit = round(payout - invested, 2)
    margin = round((1.0 - inv) * 100.0, 4)
    roi = round((profit / invested) * 100.0, 2) if invested>0 else 0.0
    return {"stakes": (s_a, s_b), "payout": payout, "profit": profit, "margin": margin, "roi": roi}

# ------------- 3-leg closed-form combos ----------------
# AH0(Home) + X + 2  (states H,D,A)
# Let O1 = AH0(Home), OX = X(1x2), O2 = '2'(1x2)
# Equalize payouts P across states:
#   H: s1*O1 = P
#   D: s1 + sX*OX = P  (AH0 refund on D)
#   A: s2*O2 = P
# Total S = s1 + sX + s2.
# Solve -> P = S / K, where K = 1/O1 + 1/O2 + (1 - 1/O1)/OX
# Arb if K < 1. Stakes:
#   s1 = P/O1; s2 = P/O2; sX = P*(1 - 1/O1)/OX

def _three_leg_ah0h_x_2(O1: float, OX: float, O2: float, total_stake: float):
    if min(O1, OX, O2) <= 1.0:
        return None
    K = (1.0/O1) + (1.0/O2) + ((1.0 - 1.0/O1) / OX)
    if K >= 1.0:
        return None
    P = total_stake / K
    s1 = P / O1
    s2 = P / O2
    sX = P * (1.0 - 1.0/O1) / OX
    # rounding to cents (or smallest currency unit used in settings layer)
    s1r, sXr, s2r = round(s1, 2), round(sX, 2), round(s2, 2)
    payout = round(P, 2)  # equal across states
    invested = round(s1r + sXr + s2r, 2)
    profit = round(payout - invested, 2)
    margin = round((1.0 - K) * 100.0, 4)
    roi = round((profit / invested) * 100.0, 2) if invested>0 else 0.0
    return {"stakes": (s1r, sXr, s2r), "payout": payout, "profit": profit, "margin": margin, "roi": roi}

# AH0(Away) + X + 1 symmetry:
# Oa = AH0(Away), O1 = '1'(1x2), OX = X(1x2)
# P = S / K, K = 1/O1 + 1/Oa + (1 - 1/Oa)/OX
# sA = P/Oa; s1 = P/O1; sX = P*(1 - 1/Oa)/OX

def _three_leg_ah0a_x_1(Oa: float, OX: float, O1: float, total_stake: float):
    if min(Oa, OX, O1) <= 1.0:
        return None
    K = (1.0/O1) + (1.0/Oa) + ((1.0 - 1.0/Oa) / OX)
    if K >= 1.0:
        return None
    P = total_stake / K
    sA = P / Oa
    s1 = P / O1
    sX = P * (1.0 - 1.0/Oa) / OX
    sAr, sXr, s1r = round(sA, 2), round(sX, 2), round(s1, 2)
    payout = round(P, 2)
    invested = round(sAr + sXr + s1r, 2)
    profit = round(payout - invested, 2)
    margin = round((1.0 - K) * 100.0, 4)
    roi = round((profit / invested) * 100.0, 2) if invested>0 else 0.0
    return {"stakes": (sAr, sXr, s1r), "payout": payout, "profit": profit, "margin": margin, "roi": roi}

# ------------- enumerate opps per event ----------------

def _enumerate_opportunities_for_event(
    event_id: int,
    start_time: datetime,
    sibling_bests: Dict[tuple, Dict[str, Leg]],  # (market_key,line)->best_map
    total_stake: float,
    min_profit_abs: float,
    min_margin_pct: float,
    enabled_cross_pairs: List[List[str]],
    enable_three_leg: bool,
) -> List[Opportunity]:
    opps: List[Opportunity] = []

    def add_two_way(label: str, line: Optional[str], a_lab: str, a_leg: Leg, b_lab: str, b_leg: Leg):
        res = _two_way_arb(a_leg.odds, b_leg.odds, total_stake)
        if not res or res["profit"] < min_profit_abs or res["margin"] < min_margin_pct:
            return
        odds = {a_lab: a_leg.odds, b_lab: b_leg.odds}
        stakes = {a_lab: res["stakes"][0], b_lab: res["stakes"][1]}
        legs = {
            a_lab: {"bookmaker_id": a_leg.bookmaker_id, "odds": a_leg.odds},
            b_lab: {"bookmaker_id": b_leg.bookmaker_id, "odds": b_leg.odds},
        }
        opps.append(Opportunity(
            arb_event_id=event_id, market_name=label, line=line, start_time=start_time,
            odds=odds, legs=legs, profit=res["profit"], roi=res["roi"], margin=res["margin"], stakes=stakes
        ))

    # Single-market opportunities
    # 1x2 (3-way)
    bm_1x2 = sibling_bests.get(("1x2", None))
    if bm_1x2 and all(o in bm_1x2 for o in ("1","X","2")):
        odds = {"1": bm_1x2["1"].odds, "x": bm_1x2["X"].odds, "2": bm_1x2["2"].odds}
        ar = calculate_arbitrage(odds, total_stake, min_profit=min_profit_abs)
        if ar and ar["margin"] >= min_margin_pct:
            legs = {
                "1": {"bookmaker_id": bm_1x2["1"].bookmaker_id, "odds": bm_1x2["1"].odds},
                "x": {"bookmaker_id": bm_1x2["X"].bookmaker_id, "odds": bm_1x2["X"].odds},
                "2": {"bookmaker_id": bm_1x2["2"].bookmaker_id, "odds": bm_1x2["2"].odds},
            }
            opps.append(Opportunity(
                arb_event_id=event_id, market_name=market_label_from_key("1x2", None),
                line=None, start_time=start_time, odds=odds, legs=legs,
                profit=ar["profit"], roi=ar["roi"], margin=ar["margin"], stakes=ar["stakes"]
            ))

    # ML
    bm_ml = sibling_bests.get(("ml", None))
    if bm_ml and "1" in bm_ml and "2" in bm_ml:
        add_two_way(market_label_from_key("ml", None), None, "1", bm_ml["1"], "2", bm_ml["2"])

    # AH0
    bm_ah0 = sibling_bests.get(("ah:0", None))
    if bm_ah0 and "Home" in bm_ah0 and "Away" in bm_ah0:
        add_two_way(market_label_from_key("ah:0", None), None, "Home", bm_ah0["Home"], "Away", bm_ah0["Away"])

    # OU (each line separately)
    for (mk, line), bm in list(sibling_bests.items()):
        if mk.startswith("ou:"):
            over = f"Over {line}"; under = f"Under {line}"
            if over in bm and under in bm:
                add_two_way(market_label_from_key(mk, line), line, over, bm[over], under, bm[under])

    # Cross-market 2-leg pairs from settings
    for pair in enabled_cross_pairs or []:
        try:
            (mk1, o1) = pair[0].split("|", 1)
            (mk2, o2) = pair[1].split("|", 1)
        except Exception:
            continue
        bm1 = sibling_bests.get((mk1, None))
        bm2 = sibling_bests.get((mk2, None))
        if not bm1 or not bm2: 
            continue
        if o1 not in bm1 or o2 not in bm2:
            continue
        add_two_way(f"{market_label_from_key(mk1, None)} + {market_label_from_key(mk2, None)}",
                    None, f"{mk1}:{o1}", bm1[o1], f"{mk2}:{o2}", bm2[o2])

    # Cross-market 3-leg closed-form combos
    if enable_three_leg and bm_ah0 and bm_1x2 and ("X" in bm_1x2):
        # AH0(Home) + X + 2
        if "Home" in bm_ah0 and "2" in bm_1x2:
            res = _three_leg_ah0h_x_2(bm_ah0["Home"].odds, bm_1x2["X"].odds, bm_1x2["2"].odds, total_stake)
            if res and res["profit"] >= min_profit_abs and res["margin"] >= min_margin_pct:
                odds = {
                    "AH0 Home": bm_ah0["Home"].odds,
                    "X": bm_1x2["X"].odds,
                    "2": bm_1x2["2"].odds,
                }
                stakes = {"AH0 Home": res["stakes"][0], "X": res["stakes"][1], "2": res["stakes"][2]}
                legs = {
                    "AH0 Home": {"bookmaker_id": bm_ah0["Home"].bookmaker_id, "odds": bm_ah0["Home"].odds},
                    "X": {"bookmaker_id": bm_1x2["X"].bookmaker_id, "odds": bm_1x2["X"].odds},
                    "2": {"bookmaker_id": bm_1x2["2"].bookmaker_id, "odds": bm_1x2["2"].odds},
                }
                opps.append(Opportunity(
                    arb_event_id=event_id,
                    market_name=f"{market_label_from_key('ah:0', None)} + Draw + Away",
                    line=None, start_time=start_time, odds=odds, legs=legs,
                    profit=res["profit"], roi=res["roi"], margin=res["margin"], stakes=stakes
                ))

        # AH0(Away) + X + 1
        if "Away" in bm_ah0 and "1" in bm_1x2:
            res = _three_leg_ah0a_x_1(bm_ah0["Away"].odds, bm_1x2["X"].odds, bm_1x2["1"].odds, total_stake)
            if res and res["profit"] >= min_profit_abs and res["margin"] >= min_margin_pct:
                odds = {
                    "AH0 Away": bm_ah0["Away"].odds,
                    "X": bm_1x2["X"].odds,
                    "1": bm_1x2["1"].odds,
                }
                stakes = {"AH0 Away": res["stakes"][0], "X": res["stakes"][1], "1": res["stakes"][2]}
                legs = {
                    "AH0 Away": {"bookmaker_id": bm_ah0["Away"].bookmaker_id, "odds": bm_ah0["Away"].odds},
                    "X": {"bookmaker_id": bm_1x2["X"].bookmaker_id, "odds": bm_1x2["X"].odds},
                    "1": {"bookmaker_id": bm_1x2["1"].bookmaker_id, "odds": bm_1x2["1"].odds},
                }
                opps.append(Opportunity(
                    arb_event_id=event_id,
                    market_name=f"{market_label_from_key('ah:0', None)} + Draw + Home",
                    line=None, start_time=start_time, odds=odds, legs=legs,
                    profit=res["profit"], roi=res["roi"], margin=res["margin"], stakes=stakes
                ))

    return opps

# ---------------- main window scan ----------------

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
    Uses normalized market keys. Cross-market combos are controlled by settings.
    """
    s = load_settings()
    stake = float(stake if stake is not None else getattr(s, "stake", 10000.0))
    min_profit_percent = float(min_profit_percent if min_profit_percent is not None else getattr(s, "min_profit_percent", 0.5))
    min_profit_absolute = float(min_profit_absolute if min_profit_absolute is not None else getattr(s, "min_profit_absolute", 50.0))

    # Which markets to pull (DB labels), fallback to 1X2/OU/AH0 common labels via normalizer
    markets_cfg = list(getattr(s, "markets", [])) or ["1X2", "Match Winner", "Double Chance", "Over/Under", "Handicap 0"]

    # Cross 2-leg pairs like [["ah:0|Home","dc|X2"], ["ah:0|Away","dc|1X"]]
    cross_pairs = list(getattr(s, "cross_bundles", [])) or [["ah:0|Home","dc|X2"], ["ah:0|Away","dc|1X"]]

    # Enable 3-leg closed-form (AH0+X+2 and symmetric)
    enable_three_leg = bool(getattr(s, "cross_three_leg_enable", True))

    now = _now_utc()
    end = now + timedelta(hours=hours)

    rows = get_latest_odds_for_window(
        sport_id=sport_id,
        start_from=now,
        start_to=end,
        market_names=markets_cfg,
        include_lines=True,
    )

    # group by event then by normalized market key
    by_event = defaultdict(list)
    for r in rows:
        by_event[int(r["arb_event_id"])].append(r)

    opps: List[Opportunity] = []

    for event_id, ev_rows in by_event.items():
        # build sibling bests for this event: (market_key,line)->best_map
        norm_buckets = _group_rows_by_event_marketkey(ev_rows)
        sibling_bests: Dict[tuple, Dict[str, Leg]] = {}
        start_time = ev_rows[0]["start_time"]
        if isinstance(start_time, str):
            try:
                start_time = datetime.fromisoformat(start_time.replace("Z", "+00:00"))
            except Exception:
                start_time = now

        for (eid, mkey, line), bucket in norm_buckets.items():
            if eid != event_id:
                continue
            ms = normalize_market(str(bucket[0]["market_name"]))
            # prefer DB line for OU/AH if present
            if line is None and bucket[0].get("line") is not None:
                ms = MarketSpec(ms.market_key, str(bucket[0]["line"]), ms.outcomes)
            best_map = _best_per_market(bucket, ms)
            if best_map:
                sibling_bests[(ms.market_key, ms.line)] = best_map

        if not sibling_bests:
            continue

        opps.extend(_enumerate_opportunities_for_event(
            event_id=event_id,
            start_time=start_time,
            sibling_bests=sibling_bests,
            total_stake=stake,
            min_profit_abs=min_profit_absolute,
            min_margin_pct=min_profit_percent,
            enabled_cross_pairs=cross_pairs,
            enable_three_leg=enable_three_leg,
        ))

    # sort: highest ROI first, then earliest KO
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
    ap.add_argument("--markets", nargs="*", help="DB market names (e.g., 1X2 'Over/Under' 'Match Winner')")
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
