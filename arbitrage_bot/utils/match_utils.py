# utils/match_utils.py

import re
from typing import Any, Dict, Optional
from datetime import datetime
from dateutil import parser, tz
from rapidfuzz import process

# Import the new structured Market system
from core.markets import normalize_market_name, Market

# ================================================================
# TEAM NAME NORMALIZATION
# ================================================================
TEAM_ALIASES = {
    "man utd": "Manchester United",
    "man united": "Manchester United",
    "man city": "Manchester City",
    "bayern": "Bayern Munich",
    "psg": "Paris Saint-Germain",
    "inter": "Inter Milan",
    "spurs": "Tottenham Hotspur",
}

def normalize_team_name(name: str) -> str:
    if not name:
        return ""
    raw_name = name.strip().lower()
    raw_name = re.sub(r"\b(fc|cf|sc|afc|cfc)$", "", raw_name).strip()
    
    # Direct alias match
    if raw_name in TEAM_ALIASES:
        return TEAM_ALIASES[raw_name]

    # Fuzzy match
    result = process.extractOne(raw_name, TEAM_ALIASES.keys(), score_cutoff=85)
    if result:
        match, score, _ = result
        return TEAM_ALIASES.get(match, raw_name.title())
    
    # Fallback
    return raw_name.title()


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
    normalized_market: Optional[Market] = normalize_market_name(market)

    return {
        "home_team": normalize_team_name(home_team),
        "away_team": normalize_team_name(away_team),
        "start_time": parse_datetime(start_time),
        "market": normalized_market.name if normalized_market else market,   # fallback to raw
        "market_obj": normalized_market,  # keep the full Market object for advanced use
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
