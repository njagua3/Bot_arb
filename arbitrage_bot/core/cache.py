"""
Cache for arbitrage alerts.

- Avoids spamming duplicates.
- Resends if profit/ROI/odds change.
- Resends unchanged only after 30 min.
- Persists to disk across restarts.
"""

import hashlib
import json
import logging
import threading
import time
from pathlib import Path
from collections import deque
from typing import Dict, Any

RESEND_INTERVAL = 30 * 60  # 30 min


class Cache:
    def __init__(self, cache_dir: str = "data", max_size: int = 10000, expiry_seconds: int = 3600):
        self.cache_dir = Path(cache_dir)
        self.cache_file = self.cache_dir / "alert_cache.jsonl"
        self.max_size = max_size
        self.expiry_seconds = expiry_seconds

        self._cache_order = deque(maxlen=max_size)
        self._cache_map: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.Lock()

        self.cache_dir.mkdir(parents=True, exist_ok=True)

        logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
        self.logger = logging.getLogger(__name__)

        self.load()

    def _make_key(self, match: str, market: str, match_time: str) -> str:
        raw = f"{(match or '').strip().lower()}_{(market or '').strip().lower()}_{(match_time or '').strip()}"
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    def load(self) -> None:
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

    def check_alert_status(self, match: str, market: str, match_time: str,
                           profit: float, roi: float, odds_snapshot: Dict[str, Any]) -> str:
        """
        Decide if alert is new/update/duplicate.
        Returns: "new", "update", or "duplicate".
        """
        now = time.time()
        key = self._make_key(match, market, match_time)

        with self._lock:
            record = self._cache_map.get(key)
            if not record:
                return "new"

            age_last = now - record.get("last_sent", 0)
            age_created = now - record["timestamp"]

            if age_created > self.expiry_seconds:
                return "update"

            if round(profit, 2) != round(record["profit"], 2):
                return "update"

            if round(roi, 2) != round(record.get("roi", 0.0), 2):
                return "update"

            if odds_snapshot != record.get("odds", {}):
                return "update"

            if age_last < RESEND_INTERVAL:
                return "duplicate"

            return "update"

    def store_alert(self, match: str, market: str, match_time: str,
                    profit: float, roi: float, odds_snapshot: Dict[str, Any]) -> None:
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
            "last_sent": now,
        }

        with self._lock:
            if key not in self._cache_map:
                self._cache_order.append(key)
            self._cache_map[key] = record

            while len(self._cache_order) > self.max_size:
                evicted = self._cache_order.popleft()
                self._cache_map.pop(evicted, None)

            try:
                with self.cache_file.open("a", encoding="utf-8") as f:
                    f.write(json.dumps(record) + "\n")
            except Exception as e:
                self.logger.error(f"⚠️ Failed to write alert cache: {e}")

    def clear(self) -> None:
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
        with self._lock:
            return len(self._cache_order)

    def cleanup(self) -> None:
        now = time.time()
        expired = []
        with self._lock:
            for k, record in list(self._cache_map.items()):
                if now - record["timestamp"] > self.expiry_seconds:
                    expired.append(k)
            for k in expired:
                self._cache_map.pop(k, None)
                if k in self._cache_order:
                    self._cache_order.remove(k)
        if expired:
            self.logger.info(f"[CACHE] Cleaned {len(expired)} expired entries")
