# core/settings.py
from __future__ import annotations
import json
from dataclasses import dataclass, asdict, field
from typing import Any, Dict, List, Optional
from pathlib import Path
from .config import ENVCFG

SETTINGS_FILE: Path = ENVCFG.SETTINGS_FILE
SETTINGS_FILE.parent.mkdir(parents=True, exist_ok=True)

# Defaults
DEFAULTS: Dict[str, Any] = {
    "stake": 10000.0,
    "min_profit_percent": 0.0,      # % threshold for alerts (calculator 'margin' floor)
    "min_profit_absolute": 1.0,     # absolute KES threshold
    "cache_expiry_minutes": 5,
    "scan_interval": 180,           # used by schedulers / loops
    "log_dir": "data",
    "log_file": "arb_log.txt",
    "log_console": True,

    # (Legacy, human labels; kept for backward compatibility)
    "target_markets": ["1X2", "Match Winner", "Over/Under", "Both Teams To Score", "Double Chance"],

    # New normalized market keys (see core/markets.normalize_market)
    # "ou:*" means all OU lines, "ah:0" is DNB/AH0
    "markets": ["1x2", "ml", "dc", "ou:*", "ah:0"],

    # Cross-market bundles (2-leg) to attempt; entries are "market_key|Outcome"
    # Example pairs: AH0(Home)+X2 and AH0(Away)+1X
    "cross_bundles": [
        ["ah:0|Home", "dc|X2"],
        ["ah:0|Away", "dc|1X"]
    ],

    # Enable 3-leg closed-form combos like AH0(Home)+X+2 (and symmetric)
    "cross_three_leg_enable": True,
}

# ----------------------
# Normalization helpers
# ----------------------
def _norm_list_str(values: Optional[List[str]]) -> List[str]:
    out: List[str] = []
    for v in values or []:
        s = str(v).strip()
        if s:
            out.append(s)
    # preserve order, drop dups
    seen = set(); dedup: List[str] = []
    for s in out:
        if s not in seen:
            dedup.append(s); seen.add(s)
    return dedup

def _norm_markets_human(markets: List[str]) -> List[str]:
    """Legacy: keep human labels (title-ish casing)."""
    out: List[str] = []
    for m in markets or []:
        k = str(m).strip()
        if not k:
            continue
        out.append(" ".join(k.split()))  # collapse whitespace; keep original casing
    # de-dup
    seen = set(); dedup = []
    for k in out:
        if k not in seen:
            dedup.append(k); seen.add(k)
    return dedup or DEFAULTS["target_markets"]

def _norm_market_keys(markets: List[str]) -> List[str]:
    """
    Normalized market keys expected by calculator/cross logic:
    examples: "1x2","ml","dc","ou:*","ou:2.5","ah:0","ah:-1"
    """
    out: List[str] = []
    for m in markets or []:
        k = str(m).strip().lower()
        if not k:
            continue
        out.append(k)
    # de-dup preserve order
    seen = set(); dedup = []
    for k in out:
        if k not in seen:
            dedup.append(k); seen.add(k)
    return dedup or DEFAULTS["markets"]

def _norm_bundles(bundles: Any) -> List[List[str]]:
    """
    Expect a list of pairs like: [["ah:0|Home","dc|X2"], ["ah:0|Away","dc|1X"]]
    Silently drop malformed items.
    """
    out: List[List[str]] = []
    if isinstance(bundles, list):
        for item in bundles:
            if isinstance(item, list) and len(item) == 2:
                a, b = (str(item[0]).strip(), str(item[1]).strip())
                if a and b:
                    out.append([a, b])
    return out or DEFAULTS["cross_bundles"]

# -------------
# Settings type
# -------------
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

    # Legacy human labels (still loaded/saved for compatibility)
    target_markets: List[str] = field(default_factory=list)

    # New normalized switches
    markets: List[str] = field(default_factory=list)
    cross_bundles: List[List[str]] = field(default_factory=list)
    cross_three_leg_enable: bool = True

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
            target_markets=_norm_markets_human(list(merged.get("target_markets", []))),
            markets=_norm_market_keys(list(merged.get("markets", []))),
            cross_bundles=_norm_bundles(merged.get("cross_bundles")),
            cross_three_leg_enable=bool(merged.get("cross_three_leg_enable", DEFAULTS["cross_three_leg_enable"])),
        )

# -----------------
# Load / Save API
# -----------------
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

# -----------------
# Convenience APIs
# -----------------
def get_scan_interval() -> int:
    return load_settings().scan_interval

def get_target_markets() -> List[str]:
    """Legacy getter (human labels)."""
    return load_settings().target_markets

def get_markets_keys() -> List[str]:
    """New normalized keys used by calculator & market normalizer."""
    return load_settings().markets

def get_cross_bundles() -> List[List[str]]:
    return load_settings().cross_bundles

def cross_three_leg_enabled() -> bool:
    return bool(load_settings().cross_three_leg_enable)
