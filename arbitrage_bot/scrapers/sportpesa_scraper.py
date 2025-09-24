# scrapers/sportpesa_scraper.py
import os
import asyncio
import random
import re
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Optional, Any, Dict, List
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
PRIORITY_CONCURRENCY = 40
FULL_CONCURRENCY = 12
MAX_RETRIES = 3
RETRY_DELAY = 0.8
PRIORITY_PAGES_PER_BATCH = 6
FULL_PAGES_PER_BATCH = 4

PAGE_SIZE = int(os.getenv("SPORTPESA_PAGE_SIZE", "50"))
ORDER = os.getenv("SPORTPESA_ORDER", "leagues")  # "leagues" or "games"

MODE = os.getenv("SPORTPESA_MODE", "all")  # all|24|48|gt48
DEBUG_LIST = bool(int(os.getenv("SPORTPESA_DEBUG_LIST", "0")))

def _ts_to_dt(ts) -> Optional[datetime]:
    try:
        if ts is None:
            return None
        if isinstance(ts, (int, float)) or (isinstance(ts, str) and str(ts).isdigit()):
            val = float(ts)
            if val > 1e12:
                val /= 1000.0
            return datetime.fromtimestamp(val, tz=timezone.utc)
        s = str(ts)
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        return datetime.fromisoformat(s).astimezone(timezone.utc)
    except Exception:
        return None

def _kickoff_from_match(m: dict):
    return m.get("date") or m.get("dateTimestamp") or m.get("startTime") or m.get("start") or m.get("kickoff")

def _teams_from_match(m: dict):
    comp = m.get("competitors")
    if isinstance(comp, list) and len(comp) >= 2:
        return (comp[0].get("name", ""), comp[1].get("name", ""))
    return ((m.get("homeTeam") or {}).get("name", ""), (m.get("awayTeam") or {}).get("name", ""))

def _in_window(start_ts, start_dt: Optional[datetime], end_dt: Optional[datetime]) -> bool:
    dt = _ts_to_dt(start_ts)
    if dt is None:
        return False
    if start_dt and dt < start_dt:
        return False
    if end_dt and dt >= end_dt:
        return False
    return True

def _flatten_games_from_payload(payload: Any) -> List[dict]:
    if not payload:
        return []
    if isinstance(payload, dict):
        if isinstance(payload.get("games"), list):
            return payload["games"]
        if isinstance(payload.get("data"), list):
            return payload["data"]
        return []
    if isinstance(payload, list):
        if not payload:
            return []
        out: List[dict] = []
        for block in payload:
            if not isinstance(block, dict):
                continue
            if isinstance(block.get("games"), list):
                out.extend(block["games"])
            elif "id" in block:
                out.append(block)
        return out
    return []

# —— market name/selection helpers to align with your core/markets.py ——
def _resolve_market_name_for_core(raw_name: str, market_id: Optional[int]) -> str:
    s = (raw_name or "").strip().lower()
    if s in {"3 way", "3-way", "match result", "full time result"} or market_id == 10:
        return "1x2"
    if s in {"both teams to score", "btts", "btts yes/no"}:
        return "both teams to score"
    if s in {"double chance", "1x2 double chance", "double chance (regular time)"}:
        return "double chance"
    if s.startswith("draw no bet"):
        return "draw no bet"
    if s.startswith("over/under") or "handicap" in s:
        return raw_name  # let your regex pick up the numeric line
    return raw_name

def _is_selected_market_key(market_key: Optional[str], only_priority: bool) -> bool:
    if not market_key:
        return False
    base = market_key.split(":", 1)[0]
    if only_priority:
        return base in PRIORITY_KEYS
    return (base in TARGET_KEYS) or (base == "ou")

def _canon_sel_name(market_key: str, odd: dict, home: str, away: str) -> Optional[str]:
    base = market_key.split(":", 1)[0]
    name = (odd.get("name") or "").strip()
    short = (odd.get("shortName") or "").strip()

    if base == "1x2":
        if short in {"1", "X", "2"}: return short
        if name.lower() == "draw":   return "X"
        if home and name == home:    return "1"
        if away and name == away:    return "2"
        return None
    if base == "ml":
        if short in {"1", "2"}:      return short
        if home and name == home:    return "1"
        if away and name == away:    return "2"
        return None
    if base == "btts":
        if name.lower() in {"yes", "no"}:  return name.capitalize()
        if short.lower() in {"yes", "no"}: return short.capitalize()
        return None
    if base == "dc":
        if short in {"1X", "12", "X2"}: return short
        n = name.replace(" ", "").upper()
        if n in {"1X", "12", "X2"}:     return n
        return None
    if base == "ou":
        if name.lower().startswith(("over", "under")):  return name
        if short.lower().startswith(("over", "under")): return short
        return None
    if base == "ah":
        if home and home.lower() in name.lower(): return "Home"
        if away and away.lower() in name.lower(): return "Away"
        if name in {"Home", "Away"}:             return name
        return None
    return None

class SportPesaScraper(AsyncBaseScraper):
    bookmaker = "SportPesa"
    bookmaker_url = "https://www.ke.sportpesa.com"
    list_api_tpl = (
        "https://www.ke.sportpesa.com/api/upcoming/games"
        "?type=prematch&sportId=1&section=upcoming"
        f"&markets_layout=multiple&o={ORDER}&filterDay=-1"
        "&pag_count={pag_count}&pag_min={pag_min}"
    )
    match_api_tpl = "https://www.ke.sportpesa.com/api/games/markets?games={game_id}&markets=all"
    supports_browser_fallback = False

    def _headers(self) -> Dict[str, str]:
        cookie = os.getenv("SPORTPESA_COOKIE")
        return {
            "Accept": "application/json, text/plain, */*",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Accept-Language": "en-US,en;q=0.9",
            "Connection": "keep-alive",
            "User-Agent": os.getenv(
                "SPORTPESA_UA",
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36",
            ),
            "X-App-Timezone": os.getenv("SPORTPESA_TZ", "Africa/Nairobi"),
            "X-Requested-With": "XMLHttpRequest",
            "Referer": os.getenv(
                "SPORTPESA_REFERER",
                "https://www.ke.sportpesa.com/en/sports-betting/football-1/upcoming-games/",
            ),
            "Origin": os.getenv("SPORTPESA_ORIGIN", "https://www.ke.sportpesa.com"),
            **({"Cookie": cookie} if cookie else {}),
        }

    def __init__(self, *args, **kwargs):
        super().__init__(bookmaker=self.bookmaker, base_url=self.bookmaker_url, *args, **kwargs)
        self.soccer_sport_id = "1"
        self._seen = set()
        self.semaphore: Optional[asyncio.Semaphore] = None
        self._failed_details: set[str] = set()

    async def _parse_markets_payload(self, match_stub: dict, markets_payload: List[dict], only_priority: bool) -> int:
        match_id = match_stub.get("id") or match_stub.get("match_id")
        if not match_id:
            return 0

        home, away = _teams_from_match(match_stub)
        comp = (match_stub.get("competition") or {}).get("name", "")
        start_ts = _kickoff_from_match(match_stub)

        stored = 0
        for raw_market in (markets_payload or []):
            try:
                raw_name = raw_market.get("name") or ""
                fixed_name = _resolve_market_name_for_core(raw_name, raw_market.get("id"))
                spec = normalize_market(fixed_name)

                if not _is_selected_market_key(spec.market_key, only_priority):
                    continue

                odds_dict: Dict[str, float] = {}
                for odd in (raw_market.get("selections") or raw_market.get("odds") or []):
                    sel = _canon_sel_name(spec.market_key, odd, home, away)
                    if not sel:
                        continue
                    val = odd.get("odds") if "odds" in odd else odd.get("odd_value")
                    try:
                        odds_dict[sel] = float(val)
                    except Exception:
                        try:
                            odds_dict[sel] = float(str(val))
                        except Exception:
                            continue

                if not odds_dict:
                    continue

                norm = build_match_dict(
                    home_team=home,
                    away_team=away,
                    start_time=start_ts,
                    market_key=spec.market_key,
                    odds=odds_dict,
                    bookmaker=self.bookmaker,
                    sport_name="Soccer",
                )

                norm["match_id"] = int(match_id)
                norm["competition_name"] = comp
                norm["category"] = ""
                norm["bookmaker_url"] = self.bookmaker_url
                norm["bookmaker_id"] = self.bookmaker_id
                if spec.line is not None:
                    norm["line"] = spec.line
                if spec.outcomes:
                    norm["outcomes"] = spec.outcomes

                save_match_odds(norm)
                stored += 1
            except Exception as e:
                self.log("parse_market_failed", level="error", error=str(e), match_id=match_id)
        return stored

    async def parse_and_store(self, match_stub: dict, detail_data: dict, only_priority=False) -> int:
        markets: Optional[List[dict]] = None
        if isinstance(detail_data, dict):
            mbg = detail_data.get("marketsByGame")
            if isinstance(mbg, dict):
                markets = mbg.get(str(match_stub.get("id")))
            if not markets and isinstance(detail_data.get("markets"), list):
                markets = detail_data.get("markets")
            if not markets and isinstance(detail_data.get(str(match_stub.get("id"))), list):
                markets = detail_data.get(str(match_stub.get("id")))
        if not markets:
            return 0
        return await self._parse_markets_payload(match_stub, markets, only_priority)

    async def fetch_match_details(
        self,
        match_stub: dict,
        only_priority: bool = False,
        start_dt: Optional[datetime] = None,
        end_dt: Optional[datetime] = None,
    ) -> int:
        sport_id = match_stub.get("sportId") or (match_stub.get("sport") or {}).get("id")
        if str(sport_id or "1") != str(self.soccer_sport_id):
            return 0

        start_ts = _kickoff_from_match(match_stub)
        if start_ts is not None and not _in_window(start_ts, start_dt, end_dt):
            return 0

        match_id = match_stub.get("id")
        if not match_id:
            return 0

        async with self.semaphore:
            url = self.match_api_tpl.format(game_id=match_id)
            for attempt in range(1, MAX_RETRIES + 1):
                data = await self.try_api(url, cb_key=f"api:{url}", headers=self._headers(), timeout=12.0)
                if data:
                    return await self.parse_and_store(match_stub, data, only_priority=only_priority)
                if attempt < MAX_RETRIES:
                    delay = (RETRY_DELAY * (2 ** (attempt - 1))) + random.random() * 0.4
                    await asyncio.sleep(delay)

        self._failed_details.add(str(match_id))
        return 0

    async def scrape_phase(
        self,
        *,
        only_priority: bool,
        concurrency: int,
        pages_per_batch: int,
        start_dt: Optional[datetime] = None,
        end_dt: Optional[datetime] = None,
    ) -> int:
        def _fmt(dt):
            return dt.strftime("%Y-%m-%d %H:%M") if dt else "∞"
        label_str = f"{_fmt(start_dt)} → {_fmt(end_dt)}" if (start_dt or end_dt) else "All upcoming"
        phase_name = f"PRIORITY ({label_str})" if only_priority else f"FULL ({label_str})"
        print(f"\n🚀 Starting phase: {phase_name}")

        self.semaphore = asyncio.Semaphore(concurrency)

        pag_min = 0
        total_stored = 0
        pbar = tqdm(desc="Scraping soccer matches", unit="match")
        print(f"Using PAGE_SIZE={PAGE_SIZE}, pages_per_batch={pages_per_batch}, concurrency={concurrency}")
        print(f"List URL template: {self.list_api_tpl}")

        while True:
            batch_offsets = [pag_min + (i * PAGE_SIZE) for i in range(pages_per_batch)]
            list_urls = [self.list_api_tpl.format(pag_count=PAGE_SIZE, pag_min=o) for o in batch_offsets]

            results = await asyncio.gather(*[
                self.try_api(url, cb_key=f"api:list:{o}", headers=self._headers(), timeout=12.0)
                for o, url in zip(batch_offsets, list_urls)
            ])

            if DEBUG_LIST:
                for idx, r in enumerate(results[:2]):
                    if isinstance(r, dict):
                        print(f"[debug:list] result[{idx}] keys=", list(r.keys())[:8])
                    elif isinstance(r, list):
                        head = r[0] if r else None
                        head_keys = list(head.keys())[:8] if isinstance(head, dict) else type(head).__name__ if head is not None else None
                        print(f"[debug:list] result[{idx}] type=list len={len(r)} head={head_keys}")
                    else:
                        print(f"[debug:list] result[{idx}] type=", type(r).__name__)

            page_games: List[dict] = []
            for r in results:
                page_games.extend(_flatten_games_from_payload(r))

            if not page_games:
                break

            tasks: List[asyncio.Future] = []
            for m in page_games:
                start_field = _kickoff_from_match(m)
                if start_field is not None and not _in_window(start_field, start_dt, end_dt):
                    continue
                mid = str(m.get("id"))
                if not mid or mid in self._seen:
                    continue
                self._seen.add(mid)

                if isinstance(m.get("markets"), list) and m["markets"]:
                    tasks.append(self._parse_markets_payload(m, m["markets"], only_priority))
                else:
                    tasks.append(self.fetch_match_details(m, only_priority=only_priority, start_dt=start_dt, end_dt=end_dt))

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

            # 👈 FIX: advance to the next *new* window of pages (avoid overlapping offsets)
            pag_min += PAGE_SIZE * pages_per_batch

            await asyncio.sleep(0.2 + random.random() * 0.3)

        pbar.close()
        print(f"✅ Phase complete — stored {total_stored} matches")

        await self.retry_failed_details(only_priority=only_priority, start_dt=start_dt, end_dt=end_dt)
        return total_stored

    async def retry_failed_details(
        self,
        *,
        only_priority: bool,
        start_dt: Optional[datetime],
        end_dt: Optional[datetime],
    ):
        if not self._failed_details:
            return
        ids = list(self._failed_details)
        self._failed_details.clear()
        print(f"🔁 Tail retry for {len(ids)} failed details (slow mode)...")

        old_sem = self.semaphore
        self.semaphore = asyncio.Semaphore(5)

        recovered = 0
        for mid in ids:
            stub = {"id": int(mid), "sport": {"id": 1}, "hasMarkets": True, "date": None}
            url = self.match_api_tpl.format(game_id=mid)
            for attempt in range(1, MAX_RETRIES + 2):
                data = await self.try_api(url, cb_key=f"retry:{mid}", headers=self._headers(), timeout=12.0)
                if data:
                    ok = await self.parse_and_store(stub, data, only_priority=only_priority)
                    recovered += ok
                    break
                if attempt < (MAX_RETRIES + 1):
                    await asyncio.sleep(0.8 * (2 ** (attempt - 1)) + random.random() * 0.5)
        print(f"🔁 Tail retry complete — recovered {recovered}")

        self.semaphore = old_sem

    async def run(self):
        now_utc = datetime.now(timezone.utc)
        in_24h = now_utc + timedelta(hours=24)
        in_48h = now_utc + timedelta(hours=48)

        # Be a bit gentler in direct mode to avoid transient empties
        if getattr(self, "_direct_mode", False):
            priority_conc = min(24, PRIORITY_CONCURRENCY)
            priority_pages = min(3, PRIORITY_PAGES_PER_BATCH)
        else:
            priority_conc = PRIORITY_CONCURRENCY
            priority_pages = PRIORITY_PAGES_PER_BATCH

        stored_0_24 = stored_24_48 = stored_gt_48 = 0

        if MODE in ("all", "24"):
            stored_0_24 = await self.scrape_phase(
                only_priority=True,
                concurrency=priority_conc,
                pages_per_batch=priority_pages,
                start_dt=now_utc,
                end_dt=in_24h,
            )

        if MODE in ("all", "48"):
            stored_24_48 = await self.scrape_phase(
                only_priority=True,
                concurrency=priority_conc,
                pages_per_batch=priority_pages,
                start_dt=in_24h,
                end_dt=in_48h,
            )

        if MODE in ("all", "gt48"):
            stored_gt_48 = await self.scrape_phase(
                only_priority=False,
                concurrency=FULL_CONCURRENCY,
                pages_per_batch=FULL_PAGES_PER_BATCH,
                start_dt=in_48h,
                end_dt=None,
            )

        print("📊 Summary:")
        print(f"  0–24h stored: {stored_0_24}")
        print(f"  24–48h stored: {stored_24_48}")
        print(f"  >48h stored: {stored_gt_48}")

if __name__ == "__main__":
    async def main():
        async with SportPesaScraper() as s:
            await s.run()
    asyncio.run(main())
