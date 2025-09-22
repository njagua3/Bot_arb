# core/opps.py
from __future__ import annotations
import hashlib, json
from datetime import datetime, timezone
from typing import Dict, Any, Optional
from core.db import get_cursor, _ph

# Normalize tiny price jitters to avoid spammy duplicates (e.g., 2.0001 vs 2.0)
_ODDS_DP = 3

def _legs_hash(legs: Dict[str, Dict[str, Any]]) -> str:
    # deterministic signature of outcome -> {bookmaker_id, odds}
    norm = {
        outcome: {
            "bookmaker_id": int(leg["bookmaker_id"]),
            "odds": round(float(leg["odds"]), _ODDS_DP),
        }
        for outcome, leg in sorted(legs.items())
    }
    s = json.dumps(norm, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(s.encode("utf-8")).hexdigest()[:40]

def persist_opportunity(
    *,
    arb_event_id: int,
    sport_id: int,
    event_fingerprint: str,
    market_key: str,              # we’ll pass market_name as key for now
    line: Optional[str],
    profit_pct: float,            # you can use margin or roi; pick one consistently
    legs: Dict[str, Dict[str, Any]],
) -> Optional[int]:
    ph = _ph()
    legs_json = json.dumps(
        {k: {"bookmaker_id": int(v["bookmaker_id"]), "odds": float(v["odds"])} for k, v in legs.items()},
        sort_keys=True, separators=(",", ":"), ensure_ascii=False,
    )
    legs_sig = _legs_hash(legs)
    now = datetime.now(timezone.utc)

    sql = (
        f"INSERT INTO opportunities(arb_event_id, sport_id, event_fingerprint, market_key, line, "
        f"profit_pct, legs_json, legs_hash, created_at) "
        f"VALUES({ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph}) "
        f"ON DUPLICATE KEY UPDATE id = id"
    )

    with get_cursor() as cur:
        cur.execute(sql, (
            arb_event_id, sport_id, event_fingerprint, market_key, line,
            float(profit_pct), legs_json, legs_sig, now
        ))
        # If DUPLICATE, lastrowid is usually 0; treat that as "already existed"
        return int(cur.lastrowid) or None

def legs_signature_for_telegram(legs: Dict[str, Dict[str, Any]]) -> str:
    """Expose the same, rounded signature for bot de-dup."""
    return _legs_hash(legs)
