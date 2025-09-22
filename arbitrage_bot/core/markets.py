# core/markets.py
from __future__ import annotations
import re
from dataclasses import dataclass
from typing import List, Optional

# A small, DB-agnostic spec used everywhere in the pipeline
@dataclass(frozen=True)
class MarketSpec:
    market_key: str          # canonical key, e.g. "1x2", "ml", "btts", "dc", "ou:2.5", "ah:-1"
    line: Optional[str]      # the numeric line for OU/AH, else None
    outcomes: List[str]      # canonical outcomes for this market

# Canonical outcome sets
OUT_1X2  = ["1", "X", "2"]
OUT_ML   = ["1", "2"]
OUT_BTTS = ["Yes", "No"]
OUT_DC   = ["1X", "12", "X2"]

def _mk_ou(line: str) -> MarketSpec:
    return MarketSpec(market_key=f"ou:{line}", line=line, outcomes=[f"Over {line}", f"Under {line}"])

def _mk_ah(line: str) -> MarketSpec:
    return MarketSpec(market_key=f"ah:{line}", line=line, outcomes=["Home", "Away"])

def normalize_market(raw: str) -> MarketSpec:
    """
    PURE normalizer: maps a bookmaker's raw market label to a MarketSpec.
    - No DB access here.
    - Stable canonical keys used by calculator & storage.
    """
    if not raw:
        return MarketSpec("unknown", None, [])

    s = raw.strip().lower()

    # 1X2 / Result
    if s in {"1x2", "full time result", "result", "ft result", "match odds", "ft", "fulltime"}:
        return MarketSpec("1x2", None, OUT_1X2)

    # Match Winner / Moneyline
    if s in {"match winner", "moneyline", "to win", "ml"}:
        return MarketSpec("ml", None, OUT_ML)

    # BTTS
    if s in {"btts", "both teams to score", "gg/ng", "gg-ng", "goal goal", "btts yes/no"}:
        return MarketSpec("btts", None, OUT_BTTS)

    # Double Chance
    if s in {"double chance", "1x", "12", "x2"}:
        return MarketSpec("dc", None, OUT_DC)

    # Draw No Bet (treat as AH 0 line)
    if s in {"dnb", "draw no bet", "ah(0)", "asian handicap 0", "handicap 0"}:
        return _mk_ah("0")

    # Asian Handicap with explicit line
    m = re.search(r"(?:ahc|asian|handicap)[^\d+-]*([+-]?\d+(?:\.\d+)?)", s)
    if m:
        return _mk_ah(m.group(1))

    # Totals / Over–Under with line
    m = re.search(r"(?:total\s*goals\s*)?(?:o|over|u|under)[^\d]*(\d+(?:\.\d+)?)", s)
    if m:
        line = m.group(1)
        return _mk_ou(line)

    # Exact goals (kept as “special”; not in MVP calc)
    m = re.search(r"(?:exact|exactly)\s*(\d+)", s)
    if m:
        n = m.group(1)
        return MarketSpec(f"exact:{n}", n, [f"Exactly {n}"])

    # Fallback: passthrough label as key
    return MarketSpec(s, None, [])

def market_label_from_key(market_key: str, line: Optional[str]) -> str:
    """
    Human label for storing in DB (markets.name) and for Telegram messages.
    """
    if market_key == "1x2": return "1X2"
    if market_key == "ml":  return "Match Winner"
    if market_key == "btts": return "Both Teams to Score"
    if market_key == "dc":   return "Double Chance"
    if market_key.startswith("ou:"): return f"Over/Under {line}"
    if market_key.startswith("ah:"): return f"Handicap {line}"
    return market_key
