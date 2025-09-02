# core/markets.py

import re
from typing import List, Optional, Dict, Union


class Market:
    """
    Represents a betting market with its outcomes and structure.
    """
    def __init__(self, name: str, outcomes: List[str], mtype: str, param: Optional[Union[int, float]] = None):
        """
        :param name: Standardized market name (e.g. "1X2", "Over/Under 2.5")
        :param outcomes: List of possible outcomes (e.g. ["1","X","2"])
        :param mtype: "2-way", "3-way", or "special"
        :param param: Optional market parameter (e.g. 2.5 for totals/handicap)
        """
        self.name = name
        self.outcomes = outcomes
        self.mtype = mtype
        self.param = param

    def __repr__(self):
        return f"<Market {self.name} ({self.mtype})>"


# ================================================================
# STATIC MARKET DEFINITIONS
# ================================================================
BASE_MARKETS: Dict[str, Market] = {
    "1X2": Market("1X2", ["1", "X", "2"], "3-way"),
    "MATCH WINNER": Market("Match Winner", ["1", "2"], "2-way"),
    "BTTS": Market("Both Teams to Score", ["Yes", "No"], "2-way"),
    "DOUBLE CHANCE": Market("Double Chance", ["1X", "12", "X2"], "3-way"),
}


# ================================================================
# NORMALIZATION FUNCTION
# ================================================================
def normalize_market_name(raw: str) -> Market:
    """
    Normalize raw bookmaker market name into a structured Market object.
    Handles dynamic markets like Over/Under and Handicap.
    Returns a fallback Market if unknown.
    """
    if not raw:
        return Market("Unknown", [], "special")

    market = raw.strip().lower()

    # -----------------------------
    # 1X2 / Full time result
    # -----------------------------
    if market in ["1x2", "full time result", "result", "ft result", "match odds"]:
        return BASE_MARKETS["1X2"]

    # -----------------------------
    # Match Winner (2-way, no draw)
    # -----------------------------
    if market in ["match winner", "moneyline", "to win"]:
        return BASE_MARKETS["MATCH WINNER"]

    # -----------------------------
    # DNB / AH(0) → same as Match Winner
    # -----------------------------
    if market in ["dnb", "draw no bet", "ah(0)", "asian handicap 0", "handicap 0"]:
        return Market("Draw No Bet", ["1", "2"], "2-way", 0)

    # -----------------------------
    # BTTS
    # -----------------------------
    if market in ["btts", "both teams to score", "gg/ng", "gg-ng", "goal goal", "btts yes/no"]:
        return BASE_MARKETS["BTTS"]

    # -----------------------------
    # Double Chance
    # -----------------------------
    if market in ["double chance", "1x", "12", "x2"]:
        return BASE_MARKETS["DOUBLE CHANCE"]

    # -----------------------------
    # Handicap / Asian Handicap
    # e.g. "AHC +1.5", "asian handicap -1", "handicap +2"
    # -----------------------------
    hc_match = re.search(r"(ahc|handicap|asian)[^\d+-]*([+-]?\d+(?:\.\d)?)", market)
    if hc_match:
        param = float(hc_match.group(2))
        return Market(f"Handicap {param}", ["Home", "Away"], "2-way", param)

    # -----------------------------
    # Over/Under
    # e.g. "Over 2.5", "O2.5", "Under1.5"
    # -----------------------------
    ou_match = re.search(r"\b(o|over|u|under)[^\d]*(\d+(?:\.\d)?)", market)
    if ou_match:
        param = float(ou_match.group(2))
        return Market(
            f"Over/Under {param}",
            [f"Over {param}", f"Under {param}"],
            "2-way",
            param,
        )

    # -----------------------------
    # Exact Goals
    # -----------------------------
    eg_match = re.search(r"(exact|exactly)\s*(\d+)", market)
    if eg_match:
        param = int(eg_match.group(2))
        return Market(f"Exact Goals {param}", [f"Exactly {param}"], "special", param)

    # -----------------------------
    # Unknown → return fallback Market
    # -----------------------------
    return Market(raw.strip(), [], "special")
