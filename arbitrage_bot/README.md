📖 AsyncBaseScraper – Features & Improvements

The AsyncBaseScraper is a high-performance asynchronous scraper base class designed for bookmaker odds collection. It provides built-in support for proxies, retries, rate-limiting, Playwright, and structured logging, making it suitable for production-scale arbitrage scraping.

✅ Features
1. Asynchronous Architecture

Uses httpx.AsyncClient for fast non-blocking API and HTML requests.

Integrates Playwright (Chromium) for dynamic content.

Supports Celery offload for Playwright tasks (run_playwright_task) to distribute browser automation across workers.

2. Request Management

Retries with exponential backoff + jitter.

Circuit breaker per endpoint (closes after repeated failures, half-open recovery).

User-Agent rotation per retry (randomly picks from USER_AGENTS).

Timeout handling with configurable per-request timeouts.

Rate limiting:

Global per-instance limit (requests_per_minute).

Optional shared semaphore for multi-scraper coordination.

3. Proxy Support

Integrated proxy pool (ProxyPool) with:

Success/failure tracking.

Latency measurement.

Rotation on failures.

Per-proxy Playwright contexts (cached for speed, with TTL + LRU eviction).

4. Anti-Ban & Stealth

Rotating User-Agents (desktop Chrome, Safari, Linux).

Randomized wait times when using Playwright.

Circuit breaker to reduce hammering endpoints.

Hooks for captcha detection and solving:

captcha_detect_hook (looks for captcha markers in HTML).

captcha_solve_hook (stub to integrate with solver services).

5. Data Parsing & Normalization

Normalizes matches with normalize_match() → produces consistent dicts via build_match_dict.

Extensible parsing:

parse_api(data) → bookmaker API parsers.

parse_html(soup) → bookmaker HTML parsers.

paginate_hook(soup_or_page) stub for handling:

Multi-page results.

Infinite scroll.

Multiple XHR requests.

6. Scraping Orchestration

Unified odds collection flow (get_odds):

Try bookmaker API.

Fallback to static HTML.

Fallback to Playwright browser.

Automatically logs parsing errors and continues gracefully.

7. Observability

Structured JSON logging for every event:

Request success/failure.

Retry attempts.

Circuit breaker state changes.

Browser lifecycle.

Metrics snapshot:

Requests made / success / fail.

Matches collected.

Proxy pool stats.

Endpoint error counts.

Latency histogram.

⚠️ Improvements Needed

Logging

Avoid double JSON encoding (formatter + log method). Use plain '%(message)s' formatter and let log() output structured JSON.

Playwright Resource Cleanup

Pages are opened but not always closed (page.close() missing). Add cleanup after scraping HTML.

Captcha Solving

Detection is present, but solving is a stub. Integrate with services like 2Captcha, hCaptcha solver, or internal ML models.

Stealth Mode

Chromium is launched headless but doesn’t mask automation fingerprints. Integrate playwright-stealth or patch navigator.webdriver, permissions, etc.

Rate Limiting Granularity

Current limits are per-scraper. Some bookmakers may enforce per-endpoint quotas. Extend circuit breaker or add endpoint-specific throttling.

Pagination

paginate_hook is a no-op. Implement helpers for:

Next-page clicks (Playwright).

Infinite scroll (page.evaluate).

XHR-based pagination.

Timeout Hardening

Wrap calls in asyncio.wait_for() to avoid hanging beyond configured timeout.

Metrics Granularity

Current latency buckets are coarse. Replace with Prometheus-style buckets (<0.1s, <0.3s, <1s, <3s, >3s).

Testing

No test fixtures for parsing. Add bookmaker HTML/API snapshots to test parse_html and parse_api offline.


📖 BaseScraper – Features & Improvements

The BaseScraper is a synchronous base class for scraping bookmaker odds. It’s a simpler counterpart to your async version, designed to support APIs, static HTML, and Selenium browser fallback.

✅ Features
1. Request Handling

Built on requests.Session for persistent connections.

Retries with exponential backoff + jitter.

Proxy rotation via ProxyPool:

Marks failures immediately.

Records latency.

Rotates on next attempt.

User-Agent rotation per attempt.

Per-request timeouts (request_timeout).

2. Caching

Simple in-process cache (dict):

Stores responses by key.

TTL-based expiration.

Touches hot entries to refresh timestamp.

Applied to both API and static HTML requests.

3. Browser Fallback

Uses Selenium (Chrome headless) when API/HTML fail.

Features:

Random User-Agent.

Optional proxy support.

Small randomized wait (1.5s–3.0s) for dynamic content.

Supports queue-based orchestration (use_queue=True, placeholder for Celery integration).

4. Hooks (extensible)

solve_captcha(page_content) → stub for captcha solving.

handle_cloudflare(url) → stub for CF bypass logic.

paginate_hook(soup_or_page) → allows subclassing for pagination / scrolling.

5. Normalization

Unified normalize_match() → returns consistent dict via build_match_dict().

Tracks metrics (matches_collected).

6. Scraping Orchestration

Unified odds collection (get_odds):

API first.

Static HTML fallback.

Selenium browser fallback.

Supports multiple endpoints/paths (get_multiple_odds).

7. Observability

Structured JSON logging for each event:

Request success/failure.

Retry attempts.

Browser lifecycle.

Basic metrics:

Requests made.

Success/failure counts.

Matches collected.

⚠️ Improvements Needed

Logging

Same issue as async version: risk of double JSON encoding.

Solution: configure logger with %(message)s and let self.log() output JSON.

Cache Management

Currently never evicts old keys → risk of memory bloat.

Solution: use LRU cache (e.g. functools.lru_cache) or add a max cache size.

Selenium Cleanup

driver.quit() is inside finally, but no guarantee page objects are closed.

Consider try: driver.close() before quit().

Captcha/Cloudflare Handling

Hooks exist but are stubs.

Suggest:

Integrate with captcha solving APIs.

For Cloudflare, add support for cloudscraper (requests wrapper).

Pagination

paginate_hook is empty.

Needs real logic for:

Next-page links.

Infinite scroll.

Multiple XHR requests.

Metrics Granularity

Currently flat counters only.

Suggest adding per-endpoint metrics + latency distribution.

Thread Safety

Cache and metrics are not thread-safe. If you run this in multi-threaded context, need threading.Lock.

Browser Stealth

Headless Chrome without stealth → detectable (navigator.webdriver).

Suggest: use undetected-chromedriver or patch Selenium caps.


📖 ScraperOrchestrator – Features & Improvements

This module is the central coordinator of your scraping framework.
It manages task scheduling, caching, concurrency, and metrics for distributed scrapers.

✅ Features
1. Structured Logging

Custom JsonFormatter ensures all logs are JSON for easy parsing/ELK/Datadog ingestion.

Captures:

Timestamps

Level & logger name

Exception stack traces

Extra attributes from log records

2. Scraper Management

Auto-discovers available scrapers via discover_scrapers().

Accepts both BaseScraper (sync) and AsyncBaseScraper (async) subclasses.

3. Task Scheduling

Submits scrapers as Celery tasks with run_scraper_task.apply_async().

Supports:

Queues → "high_priority" vs "default".

Retry policy → exponential backoff with configurable max retries.

Async result collection (AsyncResult).

4. Caching

Hybrid cache:

Redis-backed (persistent, JSON-encoded).

In-memory fallback when Redis is unavailable.

Per-bookmaker cache keys (scraper:{bookmaker}:odds).

clear_cache(bookmaker) for manual invalidation.

TTL support for freshness.

5. Concurrency Control

Uses asyncio.Semaphore to cap concurrent Celery result fetches.

Prevents overwhelming broker/worker.

6. Metrics

Tracks via Redis counters:

Successes (scraper:metrics:success).

Failures (scraper:metrics:failure).

JSON logs for all cache and task lifecycle events.

7. Result Aggregation

Collects matches from all scrapers.

Returns unified response with:

Status.

Matches list.

Bookmakers run.

Timestamp.

8. Resource Management

close() properly shuts down Redis connections and pools.

⚠️ Improvements Needed

Cache Scope

Cache key includes only bookmaker + optional cache_scope.

Risk: cache collisions across different sports/markets.

✅ Suggest: add sport/market name to _cache_key.

Error Handling

When a Celery task result is invalid, you only log → no fallback.

✅ Suggest: re-run scraper locally (sync fallback).

Serialization

Passing proxy_pool in task_kwargs → won’t survive Celery serialization.

✅ Suggest: pass proxy list only, re-initialize ProxyPool inside worker.

Async Pattern

Calls asyncio.run(orchestrate()) inside .run().

If .run() is called inside another event loop (FastAPI, Jupyter) → crash.

✅ Suggest: make .run() fully async, or detect existing loop (nest_asyncio).

Logging Duplication

Both scraper.log() and orchestrator log JSON separately.

✅ Suggest: unify log context via structlog or propagate same logger.

Scalability

asyncio.gather waits for all tasks → if one is stuck, it blocks.

✅ Suggest: use asyncio.wait(..., return_when=FIRST_COMPLETED) with timeout.

Metrics

Currently success/failure counts only.

✅ Suggest: track latency per scraper + cache hit ratio.

Testing/Observability

No way to dry-run orchestration without Celery/Redis.

✅ Suggest: add dry_run=True mode → runs scrapers locally.


✅ Features

Proxy rotation

Uses itertools.cycle to continuously rotate proxies.

Provides next() to move to the next available proxy in the cycle.

Latency-based ranking

Keeps track of proxy response times (mark_latency).

get() returns the proxy with the lowest average latency.

Failure tracking & blacklisting

Counts failures (mark_failed) per proxy.

Blacklists proxies that exceed max_failures.

Blacklisted proxies are retried after a cooldown (_prune_blacklist).

Health recovery

mark_success gradually reduces failure counts on successful requests.

Removes a proxy from blacklist if it succeeds.

Monitoring

stats() returns failure counts, average latencies, and blacklist state.

⚠️ Corrections & Improvements

Blacklisting logic edge-case

Currently, _is_available() allows a proxy with failures just below max_failures.

Suggestion: When fail_counts >= max_failures, mark as blacklisted immediately, not just after mark_failed.

Example fix:

def _is_available(self, proxy: str) -> bool:
    self._prune_blacklist()
    if proxy in self.blacklist:
        return False
    return self.fail_counts.get(proxy, 0) < self.max_failures


Memory growth from latency tracking

Right now you keep a sliding window of 20 latencies, which is fine.

But if proxies churn a lot (new ones added), dicts could grow indefinitely.

Suggestion: Add a cleanup mechanism to remove proxies that haven’t been used in a while.

Better fallback in get()

If all proxies are blacklisted or have no latency data, get() falls back to next().

But if all proxies are unavailable, it still returns None.

Suggestion: Optionally retry a random proxy instead of None.

Concurrency safety

If multiple async tasks use the pool, dict modifications (fail_counts, latencies, blacklist) could race.

Suggestion: If you’ll use it in async scrapers, wrap state modifications with asyncio.Lock() or threading locks.

Proxy schema assumption

You assume "http" and "https" in proxy dicts are the same.

Works fine for most, but SOCKS proxies or split endpoints will fail.

Suggestion: Let caller decide or allow schema customization.



✅ Features

Auto-discovery of scrapers

Iterates over all modules in scrapers/.

Instantiates any class inheriting BaseScraper or AsyncBaseScraper.

Selective loading

Skips infra modules: base_scraper, async_base_scraper, tasks, orchestrator, scraper_loader.

Allows disabling specific scrapers dynamically via DISABLED_SCRAPERS env var.

Resilient error handling

Tracks failed imports and failed instantiations separately.

Logs all events in structured JSON (module_imported, scraper_discovered, etc.).

Discovery summary

At the end, logs a summary with counts of discovered scrapers, failed imports, and failed initializations.

⚠️ Corrections & Improvements

Instantiation assumption

Right now, it assumes all scraper classes have parameterless constructors (obj()).

If one of your scrapers requires init params (like ProxyPool or headers), this will fail.

Suggestion:

Add a factory pattern (load class references, then instantiate later in Orchestrator).

Example tweak:

scrapers.append(obj)  # keep class reference


Then instantiate them later with args.

Duplicate discovery

If a module defines multiple scraper classes, all of them get instantiated.

This may or may not be what you want (sometimes a file has helpers).

Suggestion: Add a naming convention filter (e.g., class name ends with Scraper).

Disabled scrapers parsing

disabled = set((os.getenv("DISABLED_SCRAPERS", "")).split(","))
→ If env var is empty, you get {""} which can cause false skips.

Fix:

disabled = set(filter(None, os.getenv("DISABLED_SCRAPERS", "").split(",")))


Module reload issues

When scrapers are modified, Python may keep old modules cached.

Could use importlib.reload(module) in dev mode to force fresh import.

Logging consistency

You’re using a JSON logger here, but in orchestrator.py you also defined another JsonFormatter.

They differ slightly (one includes extra attrs, one doesn’t).

Suggestion: Centralize logging in a shared util (e.g., utils/logging.py) to avoid drift.


This is the task engine for your scrapers — and it’s already quite advanced. Let’s break it down.

✅ Features

Logging

Structured JSON logging for every task event (proxy_selected, scraper_attempt, fallback_scheduled, etc.).

Redis Integration

Blacklist management with sorted sets (zset for proxy expiration).

Metric tracking per bookmaker (success, failure, latency, fallback count).

Stores fallback results (scraper:fallback:{scraper_class}).

Celery Tasks

run_scraper_task: main task to run a scraper with retry + proxy rotation.

run_selenium_task and run_playwright_task: browser automation fallbacks.

process_fallback_html: parses HTML when browser fallback succeeds.

Error Handling / Retries

Exponential backoff with jitter for retries.

Blacklists failing proxies.

Schedules browser fallback chain after max retries.

Flexibility

Supports both sync and async scrapers (is_async flag).

Proxies can be passed in and rotated.

Scrapers can define supports_browser_fallback and fallback_url.

⚠️ Corrections & Improvements

Mixing sync + async execution

safe_async_run tries to run coroutines inside Celery workers.

But Celery is not async-native, so you risk blocking workers if scrapers are heavy.

Suggestion: run async scrapers in a dedicated asyncio worker pool (e.g., celery-asyncio).

Redis blacklist granularity

You blacklist a proxy globally after a single task failure.

This might over-penalize proxies (could work on another bookmaker).

Suggestion: Namespace blacklist per bookmaker, e.g.,

f"proxy:blacklist:{bookmaker}"


Resource cleanup

In run_selenium_task, you call driver.quit() in finally, but driver.get() may hang on some sites.

Add --disable-dev-shm-usage, --no-sandbox for better container performance.

Playwright context leak

You close browser, but not always the context.

Should ensure context.close() inside finally.

Metrics inflation

You record latency_ms as incrby → this makes it a counter, not a true latency metric.

Suggestion: Store in a Redis TS (time-series) or push to Prometheus.

Fallback parsing assumptions

process_fallback_html assumes scraper.parse_html exists and is async-safe.

If a scraper doesn’t implement it, this will break.

Add:

if not hasattr(scraper, "parse_html"):
    raise NotImplementedError("Scraper does not support fallback parsing")


Logging consistency

You defined a JsonFormatter here again, but you already have it in scraper_loader.

Suggestion: move to a single utils/logging.py.

Scalability bottleneck

All retries and fallbacks are chained in one Celery worker.

Better: split queues (scraper, fallback, browser) with separate worker pools, so heavy Playwright jobs don’t block scrapers.

🔑 Overall

This is a very production-ready Celery task system:

Retries ✅

Proxies ✅

Redis blacklist ✅

Browser fallback ✅

Async support ✅

The main things to polish are centralized logging, per-bookmaker proxy health, and separating heavy browser jobs from lightweight HTTP scrapers.


⚠️ Missing / incomplete:

Playwright stealth plugin / undetected-chromedriver.

True browser fingerprint rotation.

Cookie/session replay from browser → API.

Full navigation automation (pagination, infinite scroll, click-to-load odds).




Quick notes on the diagram:

Numbered solid arrows show the main happy-path (discover → submit tasks → worker runs scraper → record metrics → orchestrator collects results).

Dashed arrows show the fallback path (after retries the worker chains a browser fetch → fallback HTML parsing → store fallback result).

Redis is used for caching, metrics, and the proxy blacklist; ProxyPool supplies and tracks proxies used by scrapers.


https://prnt.sc/9tOpJr-tUZwH