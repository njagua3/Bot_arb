# tools/cleanup.py
from __future__ import annotations
from datetime import timezone
from core.db import get_cursor
from core.config import ENVCFG

# Keep recent data; tweak as you prefer
RETAIN_EVENTS_DAYS  = int(getattr(ENVCFG, "RETAIN_EVENTS_DAYS", 7))   # deletes old events (cascade wipes markets/odds/history)
RETAIN_HISTORY_DAYS = int(getattr(ENVCFG, "RETAIN_HISTORY_DAYS", 14)) # trims long odds_history
RETAIN_OPPS_DAYS    = int(getattr(ENVCFG, "RETAIN_OPPS_DAYS", 30))    # trims old opportunities

def cleanup_db(retain_events_days=RETAIN_EVENTS_DAYS,
               retain_history_days=RETAIN_HISTORY_DAYS,
               retain_opps_days=RETAIN_OPPS_DAYS) -> None:
    with get_cursor() as cur:
        # 1) Old opportunities → free space & keep recent analytics relevant
        cur.execute(
            "DELETE FROM opportunities "
            "WHERE created_at < UTC_TIMESTAMP() - INTERVAL %s DAY",
            (retain_opps_days,),
        )

        # 2) Trim odds_history (if you keep long-running events)
        cur.execute(
            "DELETE FROM odds_history "
            "WHERE recorded_at < UTC_TIMESTAMP() - INTERVAL %s DAY",
            (retain_history_days,),
        )

        # 3) Old events (cascade wipes markets, odds, odds_history)
        cur.execute(
            "DELETE FROM arb_events "
            "WHERE start_time < UTC_TIMESTAMP() - INTERVAL %s DAY",
            (retain_events_days,),
        )

if __name__ == "__main__":
    cleanup_db()
    print("✅ Cleanup complete.")
