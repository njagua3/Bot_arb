import itertools
import time
from statistics import mean
from typing import Optional, Dict

class ProxyPool:
    def __init__(self, proxies: list[str], max_failures: int = 3, cooldown: int = 300):
        self.proxies = list(dict.fromkeys(proxies)) if proxies else []
        self._cycle = itertools.cycle(self.proxies) if self.proxies else None

        self.fail_counts = {p: 0 for p in self.proxies}
        self.latencies = {p: [] for p in self.proxies}
        self.blacklist = {}  # proxy -> retry_after (timestamp)
        self.max_failures = max_failures
        self.cooldown = cooldown  # seconds

    def _is_available(self, proxy: str) -> bool:
        """Check if proxy is usable (not blacklisted, not failed too much)."""
        retry_after = self.blacklist.get(proxy)
        if retry_after and retry_after > time.time():
            return False
        return self.fail_counts.get(proxy, 0) < self.max_failures

    def get(self) -> Optional[Dict[str, str]]:
        """Get best available proxy (lowest latency, not blacklisted)."""
        if not self._cycle:
            return None

        ranked = sorted(
            self.latencies.items(),
            key=lambda kv: mean(kv[1]) if kv[1] else float("inf"),
        )
        for proxy, _ in ranked:
            if self._is_available(proxy):
                return {"http": proxy, "https": proxy}
        return None

    def next(self) -> Optional[Dict[str, str]]:
        """Force rotate to the next proxy in the cycle."""
        if not self._cycle:
            return None

        for _ in range(len(self.proxies)):
            proxy = next(self._cycle)
            if self._is_available(proxy):
                return {"http": proxy, "https": proxy}
        return None

    def mark_failed(self, proxy: Dict[str, str]):
        """Increment failure count and temporarily blacklist if limit exceeded."""
        if not proxy:
            return
        url = proxy["http"]
        self.fail_counts[url] = self.fail_counts.get(url, 0) + 1
        if self.fail_counts[url] >= self.max_failures:
            self.blacklist[url] = time.time() + self.cooldown

    def mark_success(self, proxy: Dict[str, str]):
        """Recover proxy health when a request succeeds."""
        if not proxy:
            return
        url = proxy["http"]
        # Gradual recovery: reduce failure score instead of hard reset
        self.fail_counts[url] = max(0, self.fail_counts.get(url, 0) - 1)
        if url in self.blacklist:
            del self.blacklist[url]

    def mark_latency(self, proxy: Dict[str, str], duration: float):
        """Track proxy latency to rank best ones first."""
        if not proxy:
            return
        url = proxy["http"]
        if url not in self.latencies:
            self.latencies[url] = []
        self.latencies[url].append(duration)
        if len(self.latencies[url]) > 20:  # keep sliding window of 20
            self.latencies[url].pop(0)

    def stats(self) -> dict:
        """Export current health metrics for monitoring/logging."""
        return {
            "fail_counts": dict(self.fail_counts),
            "avg_latencies": {
                p: mean(l) if l else None for p, l in self.latencies.items()
            },
            "blacklist": dict(self.blacklist),
        }
