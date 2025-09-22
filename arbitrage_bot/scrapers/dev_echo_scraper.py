# scrapers/dev_echo_scraper.py
from __future__ import annotations
import argparse
from datetime import datetime, timedelta, timezone
from collections import defaultdict
from typing import Dict, List, Any, Tuple

from core.db import get_latest_odds_for_window, get_cursor, resolve_bookmaker_id, upsert_sport
from utils.match_utils import build_match_dict
from core.save import save_match_odds

def _now_utc() -> datetime:
    return datetime.now(tz=timezone.utc)

def _to_iso_utc(dt_val) -> str | None:
    """Return ISO-8601 Z string or None."""
    if dt_val is None:
        return None
    if isinstance(dt_val, str):
        # assume already ISO-ish from DB
        return dt_val
    if isinstance(dt_val, datetime):
        if dt_val.tzinfo is None:
            dt_val = dt_val.replace(tzinfo=timezone.utc)
        else:
            dt_val = dt_val.astimezone(timezone.utc)
        return dt_val.isoformat().replace("+00:00", "Z")
    return None

def _event_meta(arb_event_id: int) -> Tuple[str, str | None, str | None, str, str]:
    sql = """
        SELECT s.name AS sport, ae.competition_name, ae.category,
               t1.name AS home, t2.name AS away
        FROM arb_events ae
        JOIN sports s ON s.id = ae.sport_id
        JOIN teams  t1 ON t1.id = ae.home_team_id
        JOIN teams  t2 ON t2.id = ae.away_team_id
        WHERE ae.id = %s
    """
    try:
        with get_cursor(commit=False) as cur:
            try:
                cur.execute(sql, (arb_event_id,))
            except Exception:
                cur.execute(sql.replace("%s","?"), (arb_event_id,))
            r = cur.fetchone()
    except Exception:
        r = None

    if not r:
        return ("Soccer", None, None, "Home", "Away")

    if isinstance(r, dict):
        return (
            r.get("sport") or "Soccer",
            r.get("competition_name"),
            r.get("category"),
            r.get("home") or "Home",
            r.get("away") or "Away",
        )
    return (r[0] or "Soccer", r[1], r[2], r[3] or "Home", r[4] or "Away")

def _to_market_key(name: str, line: str | None) -> Tuple[str, str | None]:
    n = (name or "").strip().lower()
    if n in ("1x2", "1x2 full time", "match result"):
        return ("1x2", None)
    if n.startswith("over/under"):
        return ("ou", line)
    if n.startswith("asian handicap") or n.startswith("ah"):
        return ("ah", line)
    return (n, line)

def _synthetic_match_id(arb_event_id: int) -> int:
    return 10_000_000 + int(arb_event_id)

def _group_by_event_market(rows: List[Dict[str, Any]]) -> Dict[Tuple[int,str,str|None], List[Dict[str,Any]]]:
    buckets: Dict[Tuple[int,str,str|None], List[Dict[str,Any]]] = defaultdict(list)
    for r in rows:
        buckets[(int(r["arb_event_id"]), str(r["market_name"]), r.get("line"))].append(r)
    return buckets

def main():
    ap = argparse.ArgumentParser(description="Dev echo scraper to seed a 2nd bookmaker for testing")
    ap.add_argument("--hours", type=int, default=24, help="Look-ahead window")
    ap.add_argument("--market", default="1X2", help="DB market name (e.g., 1X2 or 'Over/Under 2.5')")
    ap.add_argument("--boost", type=float, default=0.12, help="Outcome odds boost factor (0.12 = +12%)")
    ap.add_argument("--boost-outcome", default="draw", help="Outcome to boost (e.g., home/draw/away or exact label)")
    ap.add_argument("--book", default="EchoBook", help="Name of synthetic bookmaker")
    ap.add_argument("--book-url", default=None, help="Optional bookmaker URL")
    ap.add_argument("--sport", default="Soccer", help="Sport name in DB (default: Soccer)")
    args = ap.parse_args()

    # Resolve sport by NAME so it matches your DB
    sport_id = upsert_sport(args.sport)

    now = _now_utc()
    end = now + timedelta(hours=args.hours)
    market_label = args.market.strip()

    rows = get_latest_odds_for_window(
        sport_id=sport_id,
        start_from=now,
        start_to=end,
        market_names=[market_label],
        include_lines=True,
    )

    if not rows:
        print(f"No rows found to echo for sport='{args.sport}' (id={sport_id}), market='{market_label}', window={args.hours}h.")
        return

    print(f"Found {len(rows)} odds rows to echo for market='{market_label}'.")

    bm_id = resolve_bookmaker_id(args.book, args.book_url)
    grouped = _group_by_event_market(rows)

    total_saved = 0
    for (arb_event_id, mname, line), bucket in grouped.items():
        # best per outcome
        base_odds: Dict[str,float] = {}
        for r in bucket:
            o = str(r["outcome"])
            v = float(r["value"])
            if (o not in base_odds) or (v > base_odds[o]):
                base_odds[o] = v
        if not base_odds:
            continue

        # apply boost to chosen outcome (case-insensitive)
        target = args.boost_outcome.strip().lower()
        for k in list(base_odds.keys()):
            if k.strip().lower() == target:
                base_odds[k] = round(base_odds[k] * (1.0 + args.boost), 3)

        sport_name, comp, cat, home, away = _event_meta(arb_event_id)
        mkey, mline = _to_market_key(mname, line)

        start_time_iso = _to_iso_utc(bucket[0].get("start_time"))
        if not start_time_iso:
            # shouldn’t happen, but avoid crashing if DB row is weird
            continue

        norm = build_match_dict(
            home_team=home,
            away_team=away,
            start_time=start_time_iso,        # ✅ ISO string for core/save.py
            market_key=mkey,
            odds=base_odds,
            bookmaker=args.book,
            sport_name=sport_name,
        )
        norm["match_id"] = _synthetic_match_id(arb_event_id)
        norm["competition_name"] = comp or ""
        norm["category"] = cat or ""
        norm["bookmaker_url"] = args.book_url
        norm["market_line"] = mline

        save_match_odds(norm)
        total_saved += 1

    print(f"Echo saved {total_saved} markets for bookmaker '{args.book}'. ✅")

if __name__ == "__main__":
    main()
