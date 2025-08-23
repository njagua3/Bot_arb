"""
Cache system for storing and detecting duplicate arbitrage alerts.

Features:
- Prevents spamming identical alerts.
- Resends alerts if ROI/profit/odds change immediately.
- Resends unchanged alerts only after 30 minutes.
- Persists cache to disk for continuity across restarts.
- Adds detailed logging.
"""

import hashlib
import json
import logging
import threading
import time
from pathlib import Path
from collections import deque
from typing import Dict, Any

RESEND_INTERVAL = 30 * 60  # 30 minutes

class Cache:
    def __init__(self, cache_dir: str = "data", max_size: int = 10000, expiry_seconds: int = 3600):
        self.cache_dir = Path(cache_dir)
        self.cache_file = self.cache_dir / "alert_cache.jsonl"
        self.max_size = max_size
        self.expiry_seconds = expiry_seconds

        # In-memory state
        self._cache_order = deque(maxlen=max_size)
        self._cache_map: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.Lock()

        # Ensure dir exists
        self.cache_dir.mkdir(parents=True, exist_ok=True)

        # Setup logging
        logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
        self.logger = logging.getLogger(__name__)

        # Load previous cache
        self.load()

    # ---------- Helpers ----------
    def _make_key(self, match: str, market: str, match_time: str) -> str:
        raw = f"{(match or '').strip().lower()}_{(market or '').strip().lower()}_{(match_time or '').strip()}"
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    # ---------- Persistence ----------
    def load(self) -> None:
        """Load alerts from disk into memory."""
        if not self.cache_file.exists():
            return
        try:
            with self._lock, self.cache_file.open("r", encoding="utf-8") as f:
                for line in f:
                    try:
                        record = json.loads(line.strip())
                        k = record["key"]
                        self._cache_order.append(k)
                        self._cache_map[k] = record
                    except Exception:
                        continue
            self.logger.info(f"✅ Loaded {len(self._cache_order)} alerts into cache")
        except Exception as e:
            self.logger.error(f"⚠️ Failed to load alert cache: {e}")

    # ---------- Main API ----------
    def is_duplicate_alert(self, key: str, result: Dict[str, Any]) -> bool:
        """
        Check if an alert is a duplicate using the result dict and cache key.
        - Returns False if profit/roi/odds have changed OR TTL expired.
        - Returns True if identical and still within RESEND_INTERVAL.
        """
        now = time.time()
        match = result.get("match")
        market = result.get("market_name") or result.get("market")
        match_time = result.get("match_time")
        profit = result.get("profit")
        roi = result.get("roi")
        odds_snapshot = result.get("odds") or result.get("odds_snapshot", {})

        with self._lock:
            record = self._cache_map.get(key)
            if not record:
                self.logger.info(f"[CACHE] New opportunity → {match} ({market})")
                return False

            age_since_last_sent = now - record.get("last_sent", 0)
            age_since_created = now - record["timestamp"]

            # Expired TTL
            if age_since_created > self.expiry_seconds:
                self.logger.info(f"[CACHE] Resent → expired TTL ({age_since_created:.0f}s) for {match} ({market})")
                return False

            # Profit changed
            if round(profit, 2) != round(record['profit'], 2):
                self.logger.info(
                    f"[CACHE] Resent → profit changed {record['profit']:.2f} → {profit:.2f} KES for {match} ({market})"
                )
                return False

            # ROI changed
            if round(roi, 2) != round(record.get('roi', 0.0), 2):
                self.logger.info(
                    f"[CACHE] Resent → ROI changed {record.get('roi', 0.0):.2f}% → {roi:.2f}% for {match} ({market})"
                )
                return False

            # Odds changed
            old_odds = record.get("odds", {})
            if odds_snapshot != old_odds:
                self.logger.info(
                    f"[CACHE] Resent → odds changed {old_odds} → {odds_snapshot} for {match} ({market})"
                )
                return False

            # Duplicate, unchanged → resend only after RESEND_INTERVAL
            if age_since_last_sent < RESEND_INTERVAL:
                self.logger.info(
                    f"[CACHE] Skipped duplicate → {match} ({market}), sent {age_since_last_sent/60:.1f} min ago"
                )
                return True

            # Ready to resend
            self.logger.info(f"[CACHE] Ready to resend after interval → {match} ({market})")
            return False

    def store_alert(self, match: str, market: str, match_time: str,
                    profit: float, roi: float, odds_snapshot: Dict[str, Any]) -> None:
        """Store/update an alert in memory + disk."""
        now = time.time()
        key = self._make_key(match, market, match_time)

        record = {
            "key": key,
            "match": match,
            "market": market,
            "match_time": match_time,
            "profit": round(float(profit), 2),
            "roi": round(float(roi), 2),
            "odds": odds_snapshot,
            "timestamp": now,
            "last_sent": now,  # track last alert sent
        }

        with self._lock:
            if key not in self._cache_map:
                self._cache_order.append(key)
            self._cache_map[key] = record

            # Trim
            while len(self._cache_order) > self.max_size:
                evicted = self._cache_order.popleft()
                self._cache_map.pop(evicted, None)

            # Append to disk
            try:
                with self.cache_file.open("a", encoding="utf-8") as f:
                    f.write(json.dumps(record) + "\n")
            except Exception as e:
                self.logger.error(f"⚠️ Failed to write alert cache: {e}")

    def clear(self) -> None:
        """Clear in-memory and on-disk cache completely."""
        with self._lock:
            self._cache_order.clear()
            self._cache_map.clear()
            try:
                if self.cache_file.exists():
                    self.cache_file.unlink()
                self.logger.info("🗑️ Alert cache cleared")
            except Exception as e:
                self.logger.error(f"⚠️ Failed to clear cache: {e}")

    def size(self) -> int:
        """Return current cache size."""
        with self._lock:
            return len(self._cache_order)

    def cleanup(self) -> None:
        """Remove expired entries from cache (optional)."""
        now = time.time()
        expired_keys = []
        with self._lock:
            for k, record in list(self._cache_map.items()):
                if now - record["timestamp"] > self.expiry_seconds:
                    expired_keys.append(k)
            for k in expired_keys:
                self._cache_map.pop(k, None)
                if k in self._cache_order:
                    self._cache_order.remove(k)
        if expired_keys:
            self.logger.info(f"[CACHE] Cleaned up {len(expired_keys)} expired entries")
