
import argparse
import asyncio
import json
import random
import time
from collections import Counter
from pathlib import Path
from typing import Dict, List, Optional

from core.db import (
    resolve_bookmaker_id,
    upsert_event,
    upsert_market,
    upsert_odds,
)
from core.settings import get_setting
from core.markets import normalize_market_name
from .async_base_scraper import AsyncBaseScraper


# =============================================================================
# Tunables (overridable in settings.json)
# =============================================================================
CONCURRENCY_LIMIT = int(get_setting("sportpesa.concurrency_limit", 8))
MAX_RETRIES = int(get_setting("sportpesa.max_retries", 4))
RETRY_BASE_DELAY = float(get_setting("sportpesa.retry_base_delay", 1.25))   # seconds (with jitter)
BATCH_SIZE = int(get_setting("sportpesa.batch_size", 20))
PAG_COUNT = int(get_setting("sportpesa.pag_count", 50))
REQUEST_TIMEOUT = float(get_setting("sportpesa.request_timeout", 12.0))     # seconds
SLEEP_MS_BETWEEN_BATCHES = int(get_setting("sportpesa.sleep_ms_between_batches", 300))
COOLDOWN_AFTER_FAILURES = int(get_setting("sportpesa.cooldown_after_failures", 10))
COOLDOWN_SECONDS = int(get_setting("sportpesa.cooldown_seconds", 300))      # 5 minutes
MARKET_CACHE_FILE = Path(get_setting("sportpesa.market_id_cache_file", "data/sportpesa_market_ids.json"))
MARKET_CACHE_TTL_SECS = int(get_setting("sportpesa.market_id_cache_ttl_secs", 12 * 3600))  # 12 hours

FAILED_RESPONSES_FILE = Path(get_setting("sportpesa.debug_failed_path", "data/debug_failed_responses_sportpesa.jsonl"))
IGNORED_MARKETS_FILE = Path(get_setting("sportpesa.debug_ignored_path", "data/debug_ignored_markets_sportpesa.jsonl"))
for p in (FAILED_RESPONSES_FILE, IGNORED_MARKETS_FILE, MARKET_CACHE_FILE):
    p.parent.mkdir(exist_ok=True, parents=True)


# =============================================================================
# Market groups (normalized human names, lowercase)
# NOTE: Correct Score is explicitly not included.
# =============================================================================
CORE_MARKETS = {
    "1x2",
    "double chance",
    "draw no bet",
    "both teams to score",
    "over/under 2.5",   # keep a single pinned total for cross-book consistency
}

VOLATILE_MARKETS = {
    "handicap",         # requires dynamic specValue (+/- lines)
    "first half 1x2",
    "first team to score",
}

ALL_ALLOWED_MARKETS = CORE_MARKETS | VOLATILE_MARKETS


# =============================================================================
# Scraper
# =============================================================================
class SportpesaScraper(AsyncBaseScraper):
    bookmaker = "SportPesa"
    bookmaker_url = "https://www.ke.sportpesa.com"
    list_api_url = "https://www.ke.sportpesa.com/api/upcoming/games"
    markets_json_url = "https://www.ke.sportpesa.com/i18n/markets/en.json?v3.20.0.71"
    default_markets_url = "https://www.ke.sportpesa.com/api/default/markets"
    odds_api_url = "https://www.ke.sportpesa.com/api/games/markets?games={}&markets={}"

    def __init__(self, mode: str = "core", *args, **kwargs):
        """
        mode: "core" or "volatile"
        """
        super().__init__(bookmaker=self.bookmaker, base_url=self.list_api_url, *args, **kwargs)
        if mode not in ("core", "volatile"):
            raise ValueError("mode must be 'core' or 'volatile'")
        self.mode = mode
        self.target_markets = CORE_MARKETS if self.mode == "core" else VOLATILE_MARKETS

        self.bookmaker_id = resolve_bookmaker_id(self.bookmaker, self.bookmaker_url)

        self._seen: set = set()
        self.semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)

        # Market request params (e.g. "10","46","43","56-2.5","47-1","47--1")
        self.market_ids: List[str] = []

        # DevOps tracking
        self.ignored_markets_set = set()
        self.ignored_markets_counter: Counter = Counter()
        self.consecutive_failures = 0

        # Optional proxy knob if AsyncBaseScraper supports it via try_api kwargs
        self.proxy: Optional[str] = get_setting("sportpesa.proxy") or None

    # -------------------------------------------------------------------------
    # HTTP headers (parity with browser-ish clients)
    # -------------------------------------------------------------------------
    def _headers(self) -> Dict[str, str]:
        return {
            "Accept": "application/json, text/plain, */*",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Accept-Language": "en-US,en;q=0.9",
            "Connection": "keep-alive",
            "User-Agent": get_setting(
                "sportpesa.user_agent",
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36"
            ),
            "X-App-Timezone": get_setting("sportpesa.tz", "Africa/Nairobi"),
            "X-Requested-With": "XMLHttpRequest",
        }

    # -------------------------------------------------------------------------
    # Jittered exponential backoff
    # -------------------------------------------------------------------------
    async def _sleep_backoff(self, attempt: int):
        base = RETRY_BASE_DELAY * (2 ** (attempt - 1))
        delay = base * (0.7 + 0.6 * random.random())
        await asyncio.sleep(delay)

    # -------------------------------------------------------------------------
    # try_api wrapper to pass headers/timeout/proxy
    # -------------------------------------------------------------------------
    async def _try_api_json(self, url: str, cb_key: str):
        extra = {
            "headers": self._headers(),
            "timeout": REQUEST_TIMEOUT,
        }
        if self.proxy:
            extra["proxy"] = self.proxy
        return await self.try_api(url, cb_key=cb_key, **extra)

    # -------------------------------------------------------------------------
    # Market-ID discovery with local cache
    # -------------------------------------------------------------------------
    def _load_market_ids_from_cache(self) -> bool:
        try:
            if MARKET_CACHE_FILE.exists():
                mtime = MARKET_CACHE_FILE.stat().st_mtime
                if (time.time() - mtime) <= MARKET_CACHE_TTL_SECS:
                    data = json.loads(MARKET_CACHE_FILE.read_text())
                    if isinstance(data, dict) and "core" in data and "volatile" in data:
                        cached = data.get(self.mode, [])
                        if cached:
                            self.market_ids = sorted(set(map(str, cached)))
                            self.log("market_ids_loaded_from_cache",
                                     mode=self.mode, count=len(self.market_ids))
                            return True
        except Exception as e:
            self.log("market_cache_load_failed", level="warning", error=str(e))
        return False

    def _save_market_ids_to_cache(self, core_ids: List[str], volatile_ids: List[str]):
        try:
            payload = {
                "core": sorted(set(core_ids)),
                "volatile": sorted(set(volatile_ids)),
                "cached_at": int(time.time()),
                "ttl_secs": MARKET_CACHE_TTL_SECS,
            }
            MARKET_CACHE_FILE.write_text(json.dumps(payload, ensure_ascii=False))
            self.log("market_ids_saved_to_cache", core=len(core_ids), volatile=len(volatile_ids))
        except Exception as e:
            self.log("market_cache_save_failed", level="warning", error=str(e))

    async def _discover_ids_for_allowed(self, allowed_names: set) -> List[str]:
        """
        Discover market IDs (with specValue when applicable) for given allowed normalized names.
        Combines i18n JSON + /default/markets.
        """
        ids = set()

        # Primary: i18n map
        data = await self._try_api_json(self.markets_json_url, cb_key="sportpesa:markets_i18n")
        if data and "markets" in data and "1" in data["markets"]:
            football = data["markets"]["1"]
            for market_id, spec in football.items():
                if not isinstance(spec, dict):
                    continue
                raw_name = spec.get("name") or ""
                if not raw_name:
                    continue
                norm = normalize_market_name(raw_name)
                if norm.name.strip().lower() in allowed_names:
                    # i18n sometimes shows a demonstrative [[specValue]]; if present, add one
                    if spec.get("specValue") not in (None, ""):
                        ids.add(f"{market_id}-{spec['specValue']}")
                    else:
                        ids.add(str(market_id))

        # Fallback: default/markets
        default_data = await self._try_api_json(self.default_markets_url, cb_key="sportpesa:default_markets")
        if isinstance(default_data, list):
            for block in default_data:
                if block.get("sportId") != 1:
                    continue
                for m in block.get("markets", []):
                    raw = m.get("name", "")
                    norm = normalize_market_name(raw)
                    if norm.name.strip().lower() in allowed_names:
                        spec = m.get("specValue")
                        if spec not in (None, "", " "):
                            ids.add(f"{m['id']}-{spec}")
                        else:
                            ids.add(str(m["id"]))

        # Ensure BTTS is present if requested
        if "both teams to score" in allowed_names:
            ids.add("43")

        return sorted(ids)

    async def discover_markets(self):
        """
        Determine self.market_ids depending on mode (core|volatile), with cache.
        Cache file stores both sets; we load only the requested mode.
        """
        # Try cache first
        if self._load_market_ids_from_cache():
            return

        # Recompute for both sets, cache them, then pick current mode
        core_ids = await self._discover_ids_for_allowed(CORE_MARKETS)
        volatile_ids = await self._discover_ids_for_allowed(VOLATILE_MARKETS)
        self._save_market_ids_to_cache(core_ids, volatile_ids)
        self.market_ids = core_ids if self.mode == "core" else volatile_ids

        self.log("discovered_target_market_ids",
                 mode=self.mode, ids=self.market_ids, count=len(self.market_ids))

    # -------------------------------------------------------------------------
    # Expand dynamic spec markets by sampling one match (Handicap/extra OU lines)
    # -------------------------------------------------------------------------
    async def expand_dynamic_specs(self, sample_match_id: int):
        """
        Some match-specific markets expose multiple specValue lines (e.g., Handicap +/-N).
        Probe one live match with markets=all, then merge any lines that match our target markets.
        """
        base = set(self.market_ids)
        url = f"https://www.ke.sportpesa.com/api/games/markets?games={sample_match_id}&markets=all"
        data = await self._try_api_json(url, cb_key=f"sportpesa:probe:{sample_match_id}")
        if not data:
            return

        markets = data.get("markets") or []
        for mk in markets:
            try:
                raw_name = mk.get("name", "") or ""
                norm = normalize_market_name(raw_name)
                if norm.name.strip().lower() not in self.target_markets:
                    continue
                mid = mk.get("id")
                spec_val = mk.get("specValue")
                if mid is None:
                    continue
                if spec_val not in (None, "", " "):
                    base.add(f"{mid}-{spec_val}")
                else:
                    base.add(str(mid))
            except Exception:
                continue

        self.market_ids = sorted(base)
        self.log("expanded_dynamic_specs",
                 mode=self.mode, count=len(self.market_ids), ids=self.market_ids)

    # -------------------------------------------------------------------------
    # Parse + Upsert (batch)
    # odds payload can be {"marketsByGame": {"<id>":[...]}} or {"<id>":[...]}
    # -------------------------------------------------------------------------
    async def parse_and_store(self, match_stubs: List[dict], odds_data: dict) -> int:
        stored = 0
        markets_by_game = odds_data.get("marketsByGame", {}) or odds_data

        for stub in match_stubs:
            mid = stub.get("id")
            if not mid:
                continue
            key = str(mid)
            match_markets = markets_by_game.get(key)
            if not match_markets:
                continue
            if mid in self._seen:
                continue
            self._seen.add(mid)

            home = stub.get("homeTeam", {}).get("name")
            away = stub.get("awayTeam", {}).get("name")
            comp = stub.get("competition", {}).get("name", "")

            try:
                event_id = upsert_event({
                    "match_id": mid,
                    "sport_name": "Soccer",
                    "competition_name": comp,
                    "category": "",
                    "start_time": stub.get("startTime"),
                    "home_team": home,
                    "away_team": away,
                })

                for raw_market in match_markets:
                    try:
                        raw_name = raw_market.get("name", "") or ""
                        norm = normalize_market_name(raw_name)
                        if norm.name.strip().lower() not in self.target_markets:
                            # Track ignored markets for visibility later
                            self.ignored_markets_set.add(raw_name)
                            self.ignored_markets_counter[raw_name] += 1
                            with IGNORED_MARKETS_FILE.open("a") as f:
                                f.write(json.dumps({"match_id": mid, "market": raw_name}) + "\n")
                            continue

                        # Upsert market
                        market_id = upsert_market(event_id, norm.name)

                        # Selections (SportPesa typically uses "selections")
                        odds_list = raw_market.get("selections") or raw_market.get("odds") or []
                        for odd in odds_list:
                            try:
                                selection = odd.get("name") or odd.get("odd_key") or odd.get("display")
                                val = odd.get("odds") if "odds" in odd else odd.get("odd_value")
                                value = float(val)
                                upsert_odds(market_id, self.bookmaker_id, selection, value)
                                # If your upsert_odds internally writes odds_history, you're done.
                                # If not, you can extend upsert_odds to do so, or add a dedicated history call here.
                            except Exception:
                                continue

                    except Exception as e:
                        self.log("parse_market_failed", level="error",
                                 error=str(e), match_id=mid, home_team=home, away_team=away)

                stored += 1

            except Exception as e:
                self.log("parse_match_failed", level="error",
                         error=str(e), match_id=mid, home_team=home, away_team=away)

        return stored

    # -------------------------------------------------------------------------
    # Fetch odds for a batch of matches (robust retries + cool-down)
    # -------------------------------------------------------------------------
    async def fetch_odds_batch(self, match_batch: List[dict]) -> int:
        game_ids = ",".join(str(m.get("id")) for m in match_batch if m.get("id"))
        market_ids = ",".join(self.market_ids)
        url = self.odds_api_url.format(game_ids, market_ids)

        for attempt in range(1, MAX_RETRIES + 1):
            data = await self._try_api_json(url, cb_key=f"api:{url}")
            if data:
                self.consecutive_failures = 0
                return await self.parse_and_store(match_batch, data)

            self.log("retry_batch_failed", level="warning", attempt=attempt, url=url)
            if attempt < MAX_RETRIES:
                await self._sleep_backoff(attempt)

        # Final failure for this batch
        self.consecutive_failures += 1
        self.log("batch_failed_giveup", level="error", url=url, consecutive_failures=self.consecutive_failures)
        with FAILED_RESPONSES_FILE.open("a") as f:
            f.write(json.dumps({"url": url, "note": "batch giveup"}) + "\n")

        # Cool-down after too many consecutive failures across batches
        if self.consecutive_failures >= COOLDOWN_AFTER_FAILURES:
            self.log("cooling_down_after_failures", level="warning", seconds=COOLDOWN_SECONDS)
            await asyncio.sleep(COOLDOWN_SECONDS)
            self.consecutive_failures = 0

        return 0

    # -------------------------------------------------------------------------
    # Main orchestration
    # -------------------------------------------------------------------------
    async def run(self) -> int:
        # 1) Discover market IDs (cached by mode); exit early if none
        await self.discover_markets()
        if not self.market_ids:
            self.log("no_market_ids_found", level="error", mode=self.mode)
            return 0

        # 2) Pull first page to pick a sample match and expand dynamic specs
        first_list_url = (
            f"{self.list_api_url}?type=prematch&sportId=1&section=upcoming"
            f"&markets_layout=multiple&o=leagues&pag_count={PAG_COUNT}&pag_min=0"
        )
        first_page = await self._try_api_json(first_list_url, cb_key=f"api:{first_list_url}")
        if first_page and first_page.get("games"):
            sample_id = first_page["games"][0].get("id")
            if sample_id:
                await self.expand_dynamic_specs(sample_id)

        total_stored = 0
        pag_min = 0

        while True:
            list_url = (
                f"{self.list_api_url}?type=prematch&sportId=1&section=upcoming"
                f"&markets_layout=multiple&o=leagues&pag_count={PAG_COUNT}&pag_min={pag_min}"
            )

            # Robust list fetch with retries
            match_list = None
            for attempt in range(1, MAX_RETRIES + 1):
                match_list = await self._try_api_json(list_url, cb_key=f"api:{list_url}")
                if match_list:
                    break
                self.log("retry_match_list_failed", level="warning", attempt=attempt, url=list_url)
                if attempt < MAX_RETRIES:
                    await self._sleep_backoff(attempt)

            if not match_list or not match_list.get("games"):
                self.log("empty_match_list_page", pag_min=pag_min, mode=self.mode)
                break

            matches = match_list["games"]

            # Process in polite batches (semaphore gates overlap; we do one batch at a time)
            for i in range(0, len(matches), BATCH_SIZE):
                batch = matches[i:i + BATCH_SIZE]
                async with self.semaphore:
                    total_stored += await self.fetch_odds_batch(batch)

                # Optional inter-batch sleep to avoid hammering
                if SLEEP_MS_BETWEEN_BATCHES > 0:
                    await asyncio.sleep(SLEEP_MS_BETWEEN_BATCHES / 1000.0)

            # Pagination exit
            if len(matches) < PAG_COUNT:
                break
            pag_min += PAG_COUNT

        # Final summary
        ignored_sorted = sorted(self.ignored_markets_counter.items(), key=lambda kv: (-kv[1], kv[0]))
        summary = {
            "mode": self.mode,
            "stored_events": total_stored,
            "unique_ignored_markets": len(self.ignored_markets_set),
            "top_ignored_markets": ignored_sorted[:25],
        }
        self.log("done", **summary)

        # Pretty print ignored-market summary to stdout
        if ignored_sorted:
            print("\n=== SportPesa: Ignored Markets Summary ===")
            print(f"{'Market Name':60} | Count")
            print("-" * 75)
            for name, cnt in ignored_sorted:
                print(f"{(name or '')[:60]:60} | {cnt}")
            print("-" * 75)

        return total_stored


# =============================================================================
# CLI Entrypoint
# =============================================================================
def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="SportPesa batch scraper (core|volatile)")
    parser.add_argument(
        "--mode",
        choices=["core", "volatile"],
        default="core",
        help="Select which market group to scrape: core (every 15–30m) or volatile (every 3–5m).",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()

    async def main():
        async with SportpesaScraper(mode=args.mode) as scraper:
            total = await scraper.run()
            print(f"✅ {args.mode.upper()} run stored {total} matches")

    asyncio.run(main())
