# core/settings.py
from __future__ import annotations
import json
from dataclasses import dataclass, asdict
from typing import Any, Dict, List
from pathlib import Path
from .config import ENVCFG

SETTINGS_FILE: Path = ENVCFG.SETTINGS_FILE
SETTINGS_FILE.parent.mkdir(parents=True, exist_ok=True)

DEFAULTS: Dict[str, Any] = {
    "stake": 10000.0,
    "min_profit_percent": 0,      # % threshold for alerts
    "min_profit_absolute": 1,   # absolute KES threshold
    "cache_expiry_minutes": 5,
    "scan_interval": 180,           # used by schedulers / loops
    "log_dir": "data",
    "log_file": "arb_log.txt",
    "log_console": True,
    "target_markets": ["1X2", "Match Winner", "Over/Under", "Both Teams To Score", "Double Chance"],
}

def _norm_markets(markets: List[str]) -> List[str]:
    out: List[str] = []
    for m in markets or []:
        k = str(m).strip()
        if not k:
            continue
        # basic normalization (calculator will map these further)
        out.append(k.replace(" ", " ").title())  # stable casing
    # preserve order, remove dups
    seen = set(); dedup = []
    for k in out:
        if k not in seen:
            dedup.append(k); seen.add(k)
    return dedup or DEFAULTS["target_markets"]

@dataclass
class Settings:
    stake: float
    min_profit_percent: float
    min_profit_absolute: float
    cache_expiry_minutes: int
    scan_interval: int
    log_dir: str
    log_file: str
    log_console: bool
    target_markets: List[str]

    @staticmethod
    def validate(d: Dict[str, Any]) -> "Settings":
        merged = {**DEFAULTS, **(d or {})}
        return Settings(
            stake=float(merged["stake"]),
            min_profit_percent=float(merged["min_profit_percent"]),
            min_profit_absolute=float(merged["min_profit_absolute"]),
            cache_expiry_minutes=int(merged["cache_expiry_minutes"]),
            scan_interval=int(merged["scan_interval"]),
            log_dir=str(merged["log_dir"]),
            log_file=str(merged["log_file"]),
            log_console=bool(merged["log_console"]),
            target_markets=_norm_markets(list(merged.get("target_markets", []))),
        )

def load_settings() -> Settings:
    if SETTINGS_FILE.exists():
        try:
            raw = json.loads(SETTINGS_FILE.read_text())
            return Settings.validate(raw)
        except Exception:
            # fallback to defaults if corrupted
            return Settings.validate({})
    else:
        s = Settings.validate({})
        save_settings(s)
        return s

def save_settings(s: Settings) -> None:
    SETTINGS_FILE.write_text(json.dumps(asdict(s), indent=2))

# Convenience helpers
def get_scan_interval() -> int:
    return load_settings().scan_interval

def get_target_markets() -> List[str]:
    return load_settings().target_markets
