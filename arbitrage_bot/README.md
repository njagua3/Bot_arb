

┌─────────────────────────────────────────────────────────────────────────┐
│ 1) SCRAPER LAYER                                                        │
│                                                                         │
│  BetikaScraper / SportPesaScraper (scrapers/*)                          │
│   ├─ inherits AsyncBaseScraper                                          │
│   │   ├─ resolves self.bookmaker_id once (core.db.resolve_bookmaker_id) │
│   │   ├─ httpx client + rate limits + retries                           │
│   │   └─ (optionally) Playwright fallback                               │
│   ├─ list_api → match_api (per match)                                   │
│   ├─ normalize_market() → MarketSpec (core.markets)                     │
│   ├─ build_match_dict() (utils.match_utils)                             │
│   └─ save_match_odds(norm) (core.save)                                  │
└─────────────────────────────────────────────────────────────────────────┘
                                   │ normalized payload per (match, market)
                                   ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ 2) PERSISTENCE – SAVE LAYER (core/save.py)                              │
│                                                                         │
│  save_match_odds(norm):                                                 │
│   1) Parse norm["start_time"] → aware UTC                               │
│   2) Resolve bookmaker_id (again safe)                                   │
│   3) upsert_event({...})                                                │
│      • sports, teams (+ aliases), arb_events                            │
│      • bookmaker_event_map(bookmaker_id ↔ bookmaker_event_id)           │
│        → establishes canonical arb_event_id across books                │
│   4) upsert_market(arb_event_id, market_label, line)                    │
│   5) upsert_odds(market_id, bookmaker_id, outcome, value)               │
│      • odds (unique per market/bookmaker/outcome)                       │
│      • odds_history append (new schema)                                 │
└─────────────────────────────────────────────────────────────────────────┘
                                   │ canonicalized & historized odds
                                   ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ 3) DATABASE (core/db.py)                                                │
│                                                                         │
│  Tables of interest:                                                    │
│   • sports, teams, team_aliases                                         │
│   • bookmakers, bookmaker_event_map                                     │
│   • arb_events (canonical event id)                                     │
│   • markets (unique by arb_event_id, name, line)                        │
│   • odds (latest snapshot) + odds_history (timeline)                    │
│   • opportunities (for persisted arbs; has legs_hash, line, etc.)       │
│                                                                         │
│  Query for calculator:                                                  │
│   get_latest_odds_for_window(sport_id, start_from, start_to, markets)   │
│    → returns rows [arb_event_id, start_time, market_id, market_name,    │
│                   line, bookmaker_id, outcome, value]                   │
└─────────────────────────────────────────────────────────────────────────┘
                                   │ windowed latest odds rows
                                   ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ 4) ARB CALCULATOR (core/calculator.py)                                  │
│                                                                         │
│  run_calc_window(...):                                                  │
│   a) group rows by (arb_event_id, market_name, line)                    │
│   b) pick best odds per outcome across bookmakers                       │
│   c) calculate_arbitrage():                                             │
│       • implied probs / margin                                          │
│       • stake split (for total stake)                                   │
│       • profit, ROI, payouts                                            │
│   d) produce Opportunity objects:                                       │
│       {arb_event_id, market_name, line, start_time,                     │
│        odds, legs{ outcome → {bookmaker_id, odds} },                    │
│        stakes?, profit, roi, margin}                                    │
│   e) filter by thresholds (min margin %, min profit abs)                │
│   f) sort (ROI desc, KO asc)                                            │
└─────────────────────────────────────────────────────────────────────────┘
                                   │ in-memory Opportunity list
                                   ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ 5) ARB PIPELINE / PERSIST (core/arbitrage.py + core/opps.py)            │
│                                                                         │
│  scan_and_alert_db(...):                                                │
│   • calls run_calc_window                                               │
│   • for each Opportunity:                                               │
│       – compute legs_signature (core.opps.legs_signature_for_db)        │
│         → stable across runs, insensitive to outcome order              │
│       – persist in opportunities with UNIQUE                            │
│         (event_fingerprint, market_key, line, legs_hash)                │
│       – attach _legs_sig to Opportunity for Telegram de-dup             │
│       – only “new” rows (not already in UNIQUE set) proceed to alert    │
└─────────────────────────────────────────────────────────────────────────┘
                                   │ new opportunities only
                                   ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ 6) ALERTING (core/telegram.py)                                          │
│                                                                         │
│  • _opp_hash(opp): prefers opp._legs_sig (DB-consistent).               │
│    Fallback = SHA1 of {outcome, bookmaker_id, odds rounded to 3 dp}     │
│    + (arb_event_id|market|line) → in-memory de-dup for ~30 min          │
│  • format_opp():                                                         │
│      – pretty header (3-way/2-way), teams/market/KO                     │
│      – “Best Odds” with bookmaker names (from DB cache)                 │
│      – optional stake split + total payout                              │
│      – Profit/ROI                                                       │
│  • send_telegram_alert(): chunking, retries, handles 429                │
└─────────────────────────────────────────────────────────────────────────┘


## Architecture Overview

<p align="center">
  <img src="file:///C:/Users/Administrator/Downloads/arb_pipeline.svg" alt="Arbitrage Bot Data Flow" width="820">
</p>

**Pipeline at a glance**

- **Scrapers** (`scrapers/*`): Pull raw match + market JSON from each bookmaker API; normalize market keys/lines and attach `bookmaker_id`.
- **Normalization** (`utils.match_utils`, `core.markets`): Canonicalizes teams, market names/lines (e.g., `1x2`, `ou:2.5`, `ah:-1`), and outcomes.
- **Persist Odds** (`core/save.py`): `upsert_event → upsert_market → upsert_odds` (+ `odds_history`), mapping each book’s `match_id` to a canonical `arb_event_id`.
- **Database** (`core/db.py`): MySQL schema for `arb_events`, `bookmaker_event_map`, `markets`, `odds`, `odds_history`, and `opportunities` (with `legs_hash` for uniqueness).
- **Calculator** (`core/calculator.py`): Scans a time window, picks **best odds per outcome** across books, computes **margin/ROI/stakes**, yields `Opportunity`.
- **Opportunity Store** (`core/opps.py`): Derives `legs_hash`/`legs_sig`, persists an entry **only when legs combo is new** (per `event_fingerprint + market + line + legs_hash`).
- **Alerting** (`core/arbitrage.py` → `core/telegram.py`): Sends formatted Telegram alerts with market, KO time, best odds by bookmaker, **stake split**, ROI & profit. In-memory de-dup matches DB `legs_sig`.
- **Settings** (`core/settings.py`): Central thresholds (stake, min profit/ROI, scan window) used by calculator and bot.


