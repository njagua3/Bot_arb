# core/save.py
from __future__ import annotations
from typing import Dict, Optional, Any
from datetime import datetime, timezone

from dateutil import parser

from core.db import (
    upsert_event,
    upsert_market,
    upsert_odds,
    resolve_bookmaker_id,
)

# ----------------------------
# Helpers
# ----------------------------

def _parse_start_time_utc(val: Any) -> datetime:
    """
    Accepts datetime | ISO string | epoch (sec/ms) and returns tz-aware UTC datetime.
    """
    if isinstance(val, datetime):
        return val.astimezone(timezone.utc) if val.tzinfo else val.replace(tzinfo=timezone.utc)

    s = str(val).strip()
    if not s:
        raise ValueError("start_time is empty")

    # epoch?
    if s.isdigit():
        v = float(s)
        if v > 1e12:  # ms
            v /= 1000.0
        return datetime.fromtimestamp(v, tz=timezone.utc)

    # ISO string
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    dt = parser.parse(s)
    if not dt.tzinfo:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _db_market_from_key(market_key: str, line: Optional[Any]) -> tuple[str, Optional[str]]:
    """
    Map canonical keys from normalize_market to DB market names.
    line is passed through (stringified) when relevant (OU/AH).
      canonical: 1x2, ml, btts, dc, ou:<line>, ah:<line>
    """
    base = (market_key or "").strip().lower()
    if ":" in base:
        base, _ = base.split(":", 1)

    if base in ("1x2", "ml"):
        return ("1X2", None)
    if base == "btts":
        return ("Both Teams To Score", None)
    if base == "dc":
        return ("Double Chance", None)
    if base == "ou":
        return ("Over/Under", None if line is None else str(line))
    if base == "ah":
        return ("Asian Handicap", None if line is None else str(line))
    # Fallback: store as-is
    return (market_key, None if line is None else str(line))


def _require(cond: bool, msg: str):
    if not cond:
        raise ValueError(msg)


# ----------------------------
# Main entry
# ----------------------------

def save_match_odds(norm: Dict) -> None:
    """
    Persist a normalized match dict produced by utils.match_utils.build_match_dict(...) and
    augmented by the scraper. Expected fields:

      Required:
        - home_team, away_team: str
        - start_time: datetime|ISO|epoch
        - sport_name: str
        - bookmaker: str   (used only if bookmaker_id not provided)
        - market_key: str  (canonical)
        - odds: Dict[outcome -> float]
        - match_id: int|str  (bookmaker's event id)

      Optional (recommended):
        - bookmaker_id: int
        - bookmaker_url: str
        - competition_name: str
        - category: str
        - line: str|float (for OU/AH)
        - outcomes: any (ignored here but fine to pass through)
    """
    # --- minimal validation ---
    for key in ("home_team", "away_team", "start_time", "sport_name", "bookmaker", "market_key", "odds"):
        _require(key in norm, f"Missing field '{key}' in normalized payload")

    _require(norm.get("match_id") is not None, "match_id is required for event mapping")

    # --- normalize inputs ---
    start_dt = _parse_start_time_utc(norm["start_time"])
    bm_event_id = str(norm["match_id"])  # ensure string for DB consistency
    sport_name = norm.get("sport_name") or "Unknown"

    # Prefer cached id from scraper; fallback to resolving by name/url
    bookmaker_id = norm.get("bookmaker_id")
    if not bookmaker_id:
        bookmaker_id = resolve_bookmaker_id(norm["bookmaker"], norm.get("bookmaker_url"))

    # --- upsert canonical event (creates/returns arb_event_id and maps bookmaker_event_id) ---
    arb_event_id = upsert_event({
        "bookmaker_id": int(bookmaker_id),
        "bookmaker_event_id": bm_event_id,
        "sport_name": sport_name,
        "competition_name": norm.get("competition_name"),
        "category": norm.get("category"),
        "start_time": start_dt,                   # aware UTC
        "home_team": norm["home_team"],
        "away_team": norm["away_team"],
    })

    # --- market name + line mapping ---
    # accept either `line` (preferred) or legacy `market_line`
    line = norm.get("line", norm.get("market_line"))
    db_market_name, db_line = _db_market_from_key(norm["market_key"], line)

    market_id = upsert_market(arb_event_id, db_market_name, db_line)

    # --- odds snapshot + history ---
    odds: Dict[str, float] = norm.get("odds") or {}
    for outcome, price in odds.items():
        try:
            v = float(price)
        except Exception:
            continue
        upsert_odds(market_id, int(bookmaker_id), str(outcome), v)


# Bulk save helper (optional)
def save_batch(items) -> int:
    ok = 0
    for norm in items:
        try:
            save_match_odds(norm)
            ok += 1
        except Exception as e:
            mid = norm.get("match_id")
            mk = norm.get("market_key")
            print(f"[WARN] save_match_odds failed (match_id={mid}, market={mk}): {e}")
    return ok
