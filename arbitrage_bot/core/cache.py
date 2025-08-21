# core/cache.py

import hashlib
import os

CACHE_FILE = "data/alert_cache.txt"
alert_cache = set()

def _hash_alert_key(match, market, match_time, profit):
    key = f"{match.lower()}_{market.lower()}_{match_time}_{round(profit, 2)}"
    return hashlib.sha256(key.encode()).hexdigest()

def is_duplicate_alert(match, market, match_time, profit):
    alert_key = _hash_alert_key(match, market, match_time, profit)
    return alert_key in alert_cache

def store_alert(match, market, match_time, profit):
    alert_key = _hash_alert_key(match, market, match_time, profit)
    alert_cache.add(alert_key)
    with open(CACHE_FILE, "a") as f:
        f.write(alert_key + "\n")

def load_alert_cache():
    if not os.path.exists(CACHE_FILE):
        return
    with open(CACHE_FILE, "r") as f:
        for line in f:
            alert_cache.add(line.strip())
