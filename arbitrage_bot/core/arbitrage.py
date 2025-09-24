from __future__ import annotations
from typing import List, Optional
from datetime import datetime, timezone

from core.calculator import run_calc_window, Opportunity
from core.settings import load_settings
from core.logger import log_info, log_success, log_warning, log_error
from core.telegram import send_opportunity
from core.opps import persist_opportunity, legs_signature_for_telegram
from core.db import resolve_sport_id


def _event_fp(opp: Opportunity) -> str:
    """
    Stable fingerprint per canonical event (arb_event_id + kickoff UTC to seconds).
    If you also want league in the FP, append competition/category.
    """
    ko: datetime = opp.start_time
    # Ensure UTC, include seconds, suffix 'Z' for clarity
    if ko.tzinfo is None:
        ko = ko.replace(tzinfo=timezone.utc)
    else:
        ko = ko.astimezone(timezone.utc)
    return f"{opp.arb_event_id}:{ko.strftime('%Y%m%dT%H%M%SZ')}"


def _resolve_sport_id(sport_id: Optional[int], sport_name: Optional[str]) -> int:
    """
    Prefer explicit sport_id. Otherwise resolve from name.
    Defaults to canonical 'Soccer'.
    """
    if isinstance(sport_id, int) and sport_id > 0:
        return sport_id
    if sport_name:
        return resolve_sport_id(sport_name)
    return resolve_sport_id("Soccer")


def scan_and_alert_db(
    sport_id: Optional[int] = None,
    hours: int = 48,
    market_names: Optional[List[str]] = None,
    max_send: int = 20,
    sport_name: Optional[str] = None,
) -> int:
    """
    DB-backed scan: compute opportunities, persist new legs-combos,
    send alerts for NEW ONLY (unique by legs signature).
    - sport_id: your internal DB id (preferred if known).
    - sport_name: canonical name in your sports table (e.g., "Soccer").
    """
    _ = load_settings()  # thresholds & stake used inside run_calc_window

    # Canonicalize sport
    resolved_sport_id = _resolve_sport_id(sport_id, sport_name)

    try:
        opps = run_calc_window(
            sport_id=resolved_sport_id,
            hours=hours,
            market_names=market_names or ["1X2"],
        )
    except Exception as e:
        log_error(f"arbitrage.scan_and_alert_db: calculator failed: {e}")
        return 0

    if not opps:
        log_info("arbitrage.scan_and_alert_db: no opportunities found.")
        return 0

    sent = 0
    for opp in opps:
        try:
            # Persist with DB uniqueness (event_fingerprint, market_key, line, legs_hash)
            fp = _event_fp(opp)
            row_id = persist_opportunity(
                arb_event_id=opp.arb_event_id,
                sport_id=resolved_sport_id,
                event_fingerprint=fp,
                market_key=str(opp.market_name),  # keep consistent with markets.name
                line=str(opp.line) if opp.line is not None else None,
                profit_pct=float(opp.margin),     # storing margin as "profit_pct"
                legs=opp.legs,
            )

            # Only alert if this exact legs combo was NEWLY inserted
            if row_id:  # expect int new id; falsy if duplicate/no-op
                # Hand the same legs signature to Telegram de-dup for cross-run throttling
                try:
                    opp._legs_sig = legs_signature_for_telegram(opp.legs)
                except Exception:
                    # non-fatal: in-memory dedup will fall back to local hash
                    pass

                if send_opportunity(opp):
                    sent += 1

            if sent >= max_send:
                break

        except Exception as e:
            log_warning(
                f"send_opportunity failed for arb_event_id={getattr(opp, 'arb_event_id', '?')}: {e}"
            )

    log_success(f"arbitrage.scan_and_alert_db: sent {sent} alert(s).")
    return sent


if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser(description="Scan DB for arbs and send Telegram alerts")
    ap.add_argument("--sport", type=int, default=None, help="Internal sport_id (our DB id). If omitted, resolves from --sport-name or defaults to 'Soccer'.")
    ap.add_argument("--sport-name", type=str, default=None, help='Canonical sport name (e.g., "Soccer"). Used if --sport is not provided.')
    ap.add_argument("--hours", type=int, default=48, help="Look-ahead window")
    ap.add_argument("--markets", nargs="*", help="DB market labels, e.g., 1X2 'Over/Under 2.5'")
    ap.add_argument("--limit", type=int, default=10, help="Max alerts to send")
    args = ap.parse_args()

    scan_and_alert_db(
        sport_id=args.sport,
        sport_name=args.sport_name,
        hours=args.hours,
        market_names=args.markets,
        max_send=args.limit,
    )
