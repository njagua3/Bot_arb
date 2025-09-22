# utils/match_utils.py
from __future__ import annotations
from typing import Any, Dict, Optional
from dateutil import parser, tz

# Market normalization (pure, DB-agnostic)
from core.markets import normalize_market, market_label_from_key
# Team normalization
from utils.team_utils import normalize_team


# ================================================================
# ODDS NORMALIZATION
# ================================================================
def normalize_odds(value: Any) -> Optional[float]:
    """Convert odds-like values into decimal float."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        try:
            return float(value)
        except Exception:
            return None

    s = str(value).strip()
    # Fractional (e.g., "5/2")
    if "/" in s:
        try:
            num, den = s.split("/")
            return round(float(num) / float(den) + 1.0, 4)
        except Exception:
            return None
    try:
        return float(s)
    except Exception:
        return None


def normalize_odds_keys(odds: Dict[str, Any]) -> Dict[str, float]:
    """
    Standardize odds keys (home/draw/away → 1/X/2, yes/no → Yes/No).
    Note: does not enforce market-specific outcome sets here.
    """
    mapping = {
        "home": "1", "draw": "X", "away": "2",
        "team a": "1", "team b": "2",
        "yes": "Yes", "no": "No",
    }
    out: Dict[str, float] = {}
    for k, v in odds.items():
        key = mapping.get(k.strip().lower(), k.strip())
        val = normalize_odds(v)
        if val is not None:
            out[key] = val
    return out


# ================================================================
# DATETIME PARSER
# ================================================================
def parse_datetime_iso_utc(dt_str: str) -> Optional[str]:
    """Parse datetime strings and always return ISO8601 in UTC (string)."""
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
# MATCH BUILDER (JSON-SAFE, PRIMITIVES ONLY)
# ================================================================
def build_match_dict(
    home_team: str,
    away_team: str,
    start_time: str,
    market_key: str,                 # raw bookmaker market string
    odds: Dict[str, Any],
    bookmaker: str,
    market_name: Optional[str] = None,
    sport_name: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Build a JSON-safe dict of normalized match data.
    - Returns ONLY primitives (str, float, int, bool, None).
    - NO DB lookups or IDs here.
    - Market normalization is pure (MarketSpec).
    """
    spec = normalize_market(market_name or market_key)
    human_label = market_label_from_key(spec.market_key, spec.line)

    return {
        # Teams & meta
        "home_team": normalize_team(home_team),
        "away_team": normalize_team(away_team),
        "start_time": parse_datetime_iso_utc(start_time),   # ISO UTC string
        "sport_name": sport_name or "Unknown",
        "bookmaker": str(bookmaker),

        # Market (pure normalized)
        "market_key": spec.market_key,      # e.g., "1x2", "ou:2.5", "btts"
        "market_line": spec.line,           # e.g., "2.5" or None
        "market_label": human_label,        # e.g., "Over/Under 2.5" (for UI/DB name)
        "outcomes_norm": list(spec.outcomes),

        # Odds (keys normalized; numeric floats)
        "odds": normalize_odds_keys(odds),
    }


# ================================================================
# UTILS
# ================================================================
def truncate_label(label: str, max_len: int = 50) -> str:
    return label if len(label) <= max_len else label[: max_len - 3] + "..."
