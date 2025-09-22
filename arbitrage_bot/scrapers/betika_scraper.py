# scrapers/betika_scraper.py
import os
import asyncio
import random
from pathlib import Path
from datetime import datetime, timedelta, timezone
from tqdm import tqdm

from utils.match_utils import build_match_dict
from core.markets import normalize_market
from core.save import save_match_odds
from .async_base_scraper import AsyncBaseScraper

# 🎯 Canonical market keys (from core.markets.normalize_market)
#   1x2, ml, btts, dc, ou:<line>, ah:<line>
TARGET_KEYS = {"1x2", "ml", "btts", "dc", "ah"}     # include all AH lines; OU allowed via prefix below
PRIORITY_KEYS = {"1x2", "btts", "ah", "ou"}         # include all OU/AH lines

# ⚡ Speed tuning (phase-specific)
PRIORITY_CONCURRENCY = 50       # 0–48h — more aggressive
FULL_CONCURRENCY = 15           # >48h — avoid throttling
MAX_RETRIES = 2
RETRY_DELAY = 0.5               # base; exponential backoff applied
PRIORITY_PAGES_PER_BATCH = 10
FULL_PAGES_PER_BATCH = 5

LAST_PAGE_FILE = Path("data/last_page.txt")
MODE = os.getenv("BETIKA_MODE", "all")  # all|24|48|gt48


def _ts_to_dt(ts) -> datetime | None:
    """Convert Betika start_time (epoch sec/ms or ISO) → aware UTC datetime."""
    try:
        if ts is None:
            return None
        if isinstance(ts, (int, float)) or (isinstance(ts, str) and ts.isdigit()):
            val = float(ts)
            if val > 1e12:  # epoch ms
                val /= 1000.0
            return datetime.fromtimestamp(val, tz=timezone.utc)
        s = str(ts)
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        return datetime.fromisoformat(s).astimezone(timezone.utc)
    except Exception:
        return None


def _in_window(start_ts, start_dt: datetime | None, end_dt: datetime | None) -> bool:
    """True if start_ts ∈ [start_dt, end_dt)."""
    dt = _ts_to_dt(start_ts)
    if dt is None:
        return False
    if start_dt and dt < start_dt:
        return False
    if end_dt and dt >= end_dt:
        return False
    return True


def _is_selected_market(spec_key: str, only_priority: bool) -> bool:
    """Filter by canonical key; supports wildcard prefixes for ou:/ah:."""
    base = spec_key.split(":", 1)[0]  # 'ou:2.5' -> 'ou'
    if only_priority:
        return base in PRIORITY_KEYS
    return (base in TARGET_KEYS) or (base == "ou")  # allow all OU lines in full mode


class BetikaScraper(AsyncBaseScraper):
    bookmaker = "Betika"
    bookmaker_url = "https://www.betika.com"
    list_api_url = "https://api.betika.com/v1/uo/matches"
    match_api_url = "https://api.betika.com/v1/uo/match?parent_match_id={}"

    # 🚫 Don’t spin up Playwright for this scraper
    supports_browser_fallback = False

    def __init__(self, *args, **kwargs):
        # ensure persistent httpx client from base
        super().__init__(bookmaker=self.bookmaker, base_url=self.list_api_url, *args, **kwargs)
        self.soccer_sport_id = None
        self._seen = set()
        self.semaphore = None
        self._failed_details: set[str] = set()

    # ----------------------------
    async def discover_soccer_sport_id(self):
        url = f"{self.list_api_url}?page=1&limit=100"
        data = await self.try_api(url, cb_key="api:firstpage")
        if not data or not data.get("data"):
            raise RuntimeError("⚠️ Could not load matches to detect soccer sport_id")

        seen = set()
        for m in data["data"]:
            sid = m.get("sport_id")
            sname = (m.get("sport_name") or "").lower()
            seen.add((sid, m.get("sport_name")))
            if sname == "soccer":
                self.soccer_sport_id = str(sid)
                break

        print("📋 Sports found on first matches page:")
        for sid, name in seen:
            print(f"   - {sid}: {name}")

        if not self.soccer_sport_id:
            raise RuntimeError("⚠️ Could not detect soccer sport_id from matches page")

    # ----------------------------
    async def parse_and_store(self, match_stub: dict, detail_data: dict, only_priority=False):
        markets = detail_data.get("data") or []
        if not markets:
            return 0

        match_id = match_stub.get("match_id") or match_stub.get("parent_match_id")
        if not match_id:
            return 0

        stored = 0

        for raw_market in markets:
            try:
                nm = (raw_market.get("name") or "").strip()
                spec = normalize_market(nm)  # MarketSpec: market_key, line, outcomes

                if not _is_selected_market(spec.market_key, only_priority):
                    continue

                # Build odds dict from Betika market payload
                raw_odds_list = raw_market.get("odds") or []
                odds_dict = {}
                for odd in raw_odds_list:
                    sel = odd.get("odd_key") or odd.get("display")
                    val = odd.get("odd_value")
                    if sel is None or val is None:
                        continue
                    try:
                        odds_dict[str(sel).strip()] = float(val)
                    except Exception:
                        continue

                if not odds_dict:
                    continue

                # Build normalized, JSON-safe payload (no DB work here)
                norm = build_match_dict(
                    home_team=match_stub.get("home_team", ""),
                    away_team=match_stub.get("away_team", ""),
                    start_time=match_stub.get("start_time"),
                    market_key=spec.market_key,
                    odds=odds_dict,
                    bookmaker=self.bookmaker,
                    sport_name=match_stub.get("sport_name") or "Soccer",
                )

                # Add fields the saver expects
                norm["match_id"] = int(match_id)
                norm["competition_name"] = match_stub.get("competition_name", "")
                norm["category"] = match_stub.get("category", "")
                norm["bookmaker_url"] = self.bookmaker_url  # no trailing comma

                # Add extra fields for DB layer
                norm["line"] = spec.line             # e.g., 2.5 for OU, -1 for AH
                norm["outcomes"] = spec.outcomes     # structured outcomes from normalize_market
                # ✅ use cached id from AsyncBaseScraper.__init__()
                norm["bookmaker_id"] = self.bookmaker_id

                # Persist: upsert_event → upsert_market(line) → upsert_odds
                save_match_odds(norm)
                stored += 1

            except Exception as e:
                self.log("parse_market_failed", level="error", error=str(e), match_id=match_id)

        return stored

    # ----------------------------
    async def fetch_match_details(
        self,
        match_stub: dict,
        only_priority: bool = False,
        start_dt: datetime | None = None,
        end_dt: datetime | None = None
    ):
        # ⚽ Only soccer
        if str(match_stub.get("sport_id")) != str(self.soccer_sport_id):
            return 0

        # ⏱ window gate — avoid calling detail if the match is outside this phase
        if not _in_window(match_stub.get("start_time"), start_dt, end_dt):
            return 0

        # Skip if no markets available
        sb = match_stub.get("side_bets")
        if (not sb) or (isinstance(sb, (list, dict)) and not sb):
            return 0

        match_id = (
            match_stub.get("parent_match_id")
            or match_stub.get("match_id")
            or match_stub.get("id")
        )
        if not match_id:
            return 0

        async with self.semaphore:
            url = self.match_api_url.format(match_id)
            for attempt in range(1, MAX_RETRIES + 1):
                data = await self.try_api(url, cb_key=f"api:{url}")
                if data and data.get("data"):
                    return await self.parse_and_store(match_stub, data, only_priority=only_priority)
                if attempt < MAX_RETRIES:
                    # ⤴️ Exponential backoff + jitter to reduce hammering when throttled
                    delay = (RETRY_DELAY * (2 ** (attempt - 1))) + random.random() * 0.4
                    await asyncio.sleep(delay)

        # record failed for tail-retry
        self._failed_details.add(str(match_id))
        return 0

    # ----------------------------
    async def scrape_phase(
        self, *,
        period_id: int,
        only_priority: bool,
        concurrency: int,
        pages_per_batch: int,
        start_dt: datetime | None = None,
        end_dt: datetime | None = None
    ) -> int:
        # label window
        def _fmt(dt):
            return dt.strftime("%Y-%m-%d %H:%M") if dt else "∞"

        if start_dt or end_dt:
            label_str = f"{_fmt(start_dt)} → {_fmt(end_dt)}"
        else:
            label_str = "All upcoming"

        phase_name = f"PRIORITY ({label_str})" if only_priority else f"FULL ({label_str})"
        print(f"\n🚀 Starting phase: {phase_name}")

        # Set phase-specific concurrency
        self.semaphore = asyncio.Semaphore(concurrency)

        page = 1
        total_stored = 0
        pbar = tqdm(desc="Scraping soccer matches", unit="match")

        while True:
            batch_pages = [page + i for i in range(pages_per_batch)]
            list_urls = [
                f"{self.list_api_url}?page={p}&limit=200&sport_id={self.soccer_sport_id}&period_id={period_id}"
                for p in batch_pages
            ]
            results = await asyncio.gather(*[
                self.try_api(url, cb_key=f"api:list:{p}")
                for p, url in zip(batch_pages, list_urls)
            ])

            if all(not r or not r.get("data") for r in results):
                break

            tasks = []
            for r in results:
                if r and r.get("data"):
                    for m in r["data"]:
                        # List-level window filter BEFORE scheduling details
                        if not _in_window(m.get("start_time"), start_dt, end_dt):
                            continue
                        mid = str(m.get("match_id") or m.get("parent_match_id") or m.get("id"))
                        if mid in self._seen:
                            continue
                        self._seen.add(mid)
                        tasks.append(self.fetch_match_details(
                            m, only_priority=only_priority,
                            start_dt=start_dt, end_dt=end_dt
                        ))

            # ⚡ Stream completions to keep memory low and UI responsive
            stored = 0
            for fut in asyncio.as_completed(tasks):
                try:
                    res = await fut
                except Exception as e:
                    self.log("detail_task_failed", level="error", error=str(e))
                    res = 0
                if res:
                    stored += res
                    pbar.update(1)

            total_stored += stored
            page += pages_per_batch

            # tiny inter-batch jitter to avoid synchronized bursts
            await asyncio.sleep(0.2 + random.random() * 0.3)

        pbar.close()
        print(f"✅ Phase complete — stored {total_stored} matches")

        # 🔁 Gentle tail-retry for failed details in this phase
        await self.retry_failed_details(only_priority=only_priority, start_dt=start_dt, end_dt=end_dt)
        return total_stored

    # ----------------------------
    async def retry_failed_details(
        self, *,
        only_priority: bool,
        start_dt: datetime | None,
        end_dt: datetime | None
    ):
        if not self._failed_details:
            return
        ids = list(self._failed_details)
        self._failed_details.clear()
        print(f"🔁 Tail retry for {len(ids)} failed details (slow mode)...")

        old_sem = self.semaphore
        self.semaphore = asyncio.Semaphore(5)  # slow & gentle

        recovered = 0
        for mid in ids:
            # build a minimal stub so parse_and_store can work
            stub = {
                "match_id": mid,
                "sport_id": self.soccer_sport_id,
                "side_bets": True,  # assume yes on retry; parse_and_store will still validate content
                "start_time": None,  # unknown; retry anyway since we already window-filtered earlier
            }
            url = self.match_api_url.format(mid)
            for attempt in range(1, MAX_RETRIES + 2):
                data = await self.try_api(url, cb_key=f"retry:{mid}")
                if data and data.get("data"):
                    ok = await self.parse_and_store(stub, data, only_priority=only_priority)
                    recovered += ok
                    break
                if attempt < (MAX_RETRIES + 1):
                    await asyncio.sleep(0.8 * (2 ** (attempt - 1)) + random.random() * 0.5)
        print(f"🔁 Tail retry complete — recovered {recovered}")

        self.semaphore = old_sem

    # ----------------------------
    async def run(self):
        await self.discover_soccer_sport_id()

        now_utc = datetime.now(timezone.utc)
        in_24h = now_utc + timedelta(hours=24)
        in_48h = now_utc + timedelta(hours=48)

        # Auto-tune when running without proxies (base sets _direct_mode in __aenter__)
        if getattr(self, "_direct_mode", False):
            priority_conc = min(30, PRIORITY_CONCURRENCY)
            priority_pages = min(6, PRIORITY_PAGES_PER_BATCH)
        else:
            priority_conc = PRIORITY_CONCURRENCY
            priority_pages = PRIORITY_PAGES_PER_BATCH

        stored_0_24 = stored_24_48 = stored_gt_48 = 0

        # Phase A — 0–24h, priority markets (fast)
        if MODE in ("all", "24"):
            stored_0_24 = await self.scrape_phase(
                period_id=-2,  # "Next 48h" list; we filter to 0–24h window
                only_priority=True,
                concurrency=priority_conc,
                pages_per_batch=priority_pages,
                start_dt=now_utc,
                end_dt=in_24h,
            )

        # Phase B — 24–48h, priority markets (fast)
        if MODE in ("all", "48"):
            stored_24_48 = await self.scrape_phase(
                period_id=-2,  # same list; filter to 24–48h window
                only_priority=True,
                concurrency=priority_conc,
                pages_per_batch=priority_pages,
                start_dt=in_24h,
                end_dt=in_48h,
            )

        # Phase C — >48h, full market set (optional)
        if MODE in ("all", "gt48"):
            stored_gt_48 = await self.scrape_phase(
                period_id=9,  # "All upcoming"
                only_priority=False,
                concurrency=FULL_CONCURRENCY,
                pages_per_batch=FULL_PAGES_PER_BATCH,
                start_dt=in_48h,
                end_dt=None,
            )

        # 📊 Summary
        print("📊 Summary:")
        print(f"  0–24h stored: {stored_0_24}")
        print(f"  24–48h stored: {stored_24_48}")
        print(f"  >48h stored: {stored_gt_48}")


if __name__ == "__main__":
    async def main():
        async with BetikaScraper() as s:
            await s.run()
    asyncio.run(main())
