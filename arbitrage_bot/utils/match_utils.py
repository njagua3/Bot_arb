# utils/match_utils.py

import re
from typing import Any, Dict, Optional
from dateutil import parser, tz

# Import the new structured Market system
from core.markets import normalize_market_name, Market

# ✅ Import team normalization (centralized in utils/team_utils)
from utils.team_utils import normalize_team


# ================================================================
# ODDS NORMALIZATION
# ================================================================
def normalize_odds(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    val = str(value).strip()
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


def normalize_odds_keys(odds: dict) -> dict:
    mapping = {
        "home": "1", "draw": "X", "away": "2",
        "team a": "1", "team b": "2",
        "yes": "Yes", "no": "No",
    }
    return {
        mapping.get(k.strip().lower(), k.strip()): normalize_odds(v)
        for k, v in odds.items()
    }


# ================================================================
# MATCH BUILDER
# ================================================================
def build_match_dict(home_team: str, away_team: str, start_time: str,
                     market: str, odds: Dict[str, Any], bookmaker: str) -> Dict[str, Any]:
    normalized_market: Market = normalize_market_name(market)

    return {
        "home_team": normalize_team(home_team),
        "away_team": normalize_team(away_team),
        "start_time": parse_datetime(start_time),
        "market": normalized_market.name,   # always safe, fallback handled
        "market_obj": normalized_market,   # structured Market object
        "odds": normalize_odds_keys(odds),
        "bookmaker": bookmaker,
    }


# ================================================================
# DATETIME PARSER
# ================================================================
def parse_datetime(dt_str: str) -> Optional[str]:
    if not dt_str:
        return None
    try:
        dt = parser.parse(dt_str)
        if not dt.tzinfo:
            dt = dt.replace(tzinfo=tz.UTC)
        return dt.astimezone(tz.UTC).isoformat()
    except Exception:
        return None


# ================================================================
# UTILS
# ================================================================
def truncate_label(label: str, max_len: int = 50) -> str:
    return label if len(label) <= max_len else label[:47] + "..."
