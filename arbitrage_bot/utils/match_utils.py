# utils/match_utils.py

import re
from datetime import datetime
from typing import Any, Dict, Optional

# -----------------------------
# Team Name Normalization
# -----------------------------
def normalize_team_name(name: str) -> str:
    """
    Normalize team names by removing extra spaces, 
    common suffixes, and unifying capitalization.
    """
    if not name:
        return ""
    name = name.strip().lower()

    # Remove "fc", "cf", "sc" at end
    name = re.sub(r"\b(fc|cf|sc|afc|cfc)$", "", name)

    # Collapse multiple spaces
    name = re.sub(r"\s+", " ", name)

    return name.strip().title()


# -----------------------------
# Market Name Normalization
# -----------------------------
def normalize_market(market: str) -> str:
    """
    Normalize bookmaker market names to standard ones.
    E.g. '1X2', 'Match Odds', 'Full Time Result' -> '1x2'
    """
    if not market:
        return ""

    mapping = {
        "1x2": "1x2",
        "match odds": "1x2",
        "full time result": "1x2",
        "ou": "over_under",
        "over/under": "over_under",
        "totals": "over_under",
        "handicap": "handicap",
        "asian handicap": "handicap",
    }

    key = market.strip().lower()
    return mapping.get(key, key)


# -----------------------------
# Odds Normalization
# -----------------------------
def normalize_odds(value: Any) -> Optional[float]:
    """
    Convert odds into float decimals.
    Accepts strings like '2.5', '2/1', or numbers.
    Returns None if invalid.
    """
    if value is None:
        return None

    if isinstance(value, (int, float)):
        return float(value)

    val = str(value).strip()

    # Handle fractional odds like '2/1'
    if "/" in val:
        try:
            num, den = val.split("/")
            return round(float(num) / float(den) + 1, 2)
        except (ValueError, ZeroDivisionError):
            return None

    try:
        return float(val)
    except ValueError:
        return None


# -----------------------------
# Match Dictionary Builder
# -----------------------------
def build_match_dict(
    home_team: str,
    away_team: str,
    start_time: str,
    market: str,
    odds: Dict[str, Any],
    bookmaker: str,
) -> Dict[str, Any]:
    """
    Build a clean, normalized match dictionary for storage or arbitrage.
    """

    return {
        "home_team": normalize_team_name(home_team),
        "away_team": normalize_team_name(away_team),
        "start_time": parse_datetime(start_time),
        "market": normalize_market(market),
        "odds": {
            outcome: normalize_odds(odd) for outcome, odd in odds.items()
        },
        "bookmaker": bookmaker,
    }


# -----------------------------
# Date Parsing
# -----------------------------
def parse_datetime(dt_str: str) -> Optional[str]:
    """
    Parse datetime strings into ISO format (UTC).
    Accepts multiple common formats.
    """
    if not dt_str:
        return None

    dt_str = dt_str.strip()
    formats = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%d/%m/%Y %H:%M",
        "%d-%m-%Y %H:%M",
        "%Y/%m/%d %H:%M",
        "%Y-%m-%dT%H:%M:%SZ",  # ISO8601 UTC
    ]

    for fmt in formats:
        try:
            dt = datetime.strptime(dt_str, fmt)
            return dt.isoformat()
        except ValueError:
            continue

    return None
