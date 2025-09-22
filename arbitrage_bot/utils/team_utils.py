# utils/team_utils.py
import os
import re
import json
import difflib
import time
import logging
from functools import lru_cache
from typing import Dict, List

# ------------------------------------------------------------
# Logging (inherits level from SCRAPER_LOG_LEVEL if configured)
# ------------------------------------------------------------
logger = logging.getLogger("scrapers.teams")  # no handler here; use app-wide config

# ================================================================
# CONFIG
# ================================================================
CLUB_ALIASES_FILE = os.path.join("data", "team_aliases.json")
NATIONAL_ALIASES_FILE = os.path.join("data", "national_team_aliases.json")

# Default seed master list (clubs + some common nationals)
MASTER_TEAMS: List[str] = [
    # Clubs
    "Manchester United", "Chelsea", "Liverpool", "Arsenal", "Barcelona",
    "Real Madrid", "Bayern Munich", "PSG", "Juventus",
    # Nationals (seed, will expand from JSON)
    "Brazil", "Argentina", "Germany", "France", "Spain",
    "Italy", "England", "Kenya"
]

TEAM_ALIASES: Dict[str, List[str]] = {}       # canonical → aliases
TEAM_ALIASES_CLEAN: Dict[str, List[str]] = {} # canonical → cleaned aliases
NATIONAL_KEYS: set = set()                    # cache of national team canonicals

# Fallback burst guard (rate-limited warning)
_FALLBACK_CNT = 0
_FALLBACK_WINDOW_START = time.time()
_FALLBACK_WARN_THRESHOLD_PER_MIN = 200  # tune as you like


# ================================================================
# HELPERS
# ================================================================
def _clean_text(text: str) -> str:
    """Lowercase and strip punctuation/extra spaces."""
    return re.sub(r"[^a-z0-9\s]", "", text.lower()).strip()


def _load_json(path: str, default: Dict[str, List[str]] = None) -> Dict[str, List[str]]:
    """Load JSON safely, bootstrap with default if missing/broken."""
    if default is None:
        default = {}

    try:
        if not os.path.exists(path):
            # Bootstrap with default
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "w", encoding="utf-8") as f:
                json.dump(default, f, indent=2, ensure_ascii=False)
            return default

        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
            return {
                canonical.strip(): [a.strip() for a in aliases]
                for canonical, aliases in data.items()
            }
    except Exception as e:
        logger.warning("Could not load %s: %s", path, e)
        return default


def _fuzzy_match(clean_name: str, candidates: List[str], cutoff: float):
    """Helper for difflib fuzzy match."""
    return difflib.get_close_matches(clean_name, candidates, n=1, cutoff=cutoff)


def _fallback_burst_guard():
    """If fallback normalizations are exploding, emit a single WARN per minute."""
    global _FALLBACK_CNT, _FALLBACK_WINDOW_START
    now = time.time()
    if now - _FALLBACK_WINDOW_START >= 60:
        if _FALLBACK_CNT > _FALLBACK_WARN_THRESHOLD_PER_MIN:
            logger.warning("High rate of team fallback normalizations: %s/min", _FALLBACK_CNT)
        _FALLBACK_CNT = 0
        _FALLBACK_WINDOW_START = now


# ================================================================
# NORMALIZER
# ================================================================
@lru_cache(maxsize=512)
def normalize_team(name: str, cutoff: float = 0.75) -> str:
    """
    Normalize team names across sportsbooks using:
    1. Alias dictionary (nationals prioritized over clubs)
    2. Exact canonical match
    3. Substring alias match (only if alias length > 3)
    4. Fuzzy match (nationals prioritized, stricter cutoff)
    5. Fallback → return original input (DEBUG-log only)
    """
    if not name:
        return ""

    clean_name = _clean_text(name)

    # 1. Exact alias or canonical match (national first)
    for canonical in NATIONAL_KEYS:
        aliases_cleaned = TEAM_ALIASES_CLEAN.get(canonical, [])
        if clean_name == canonical.lower() or clean_name in aliases_cleaned:
            return canonical

    for canonical, aliases_cleaned in TEAM_ALIASES_CLEAN.items():
        if clean_name == canonical.lower() or clean_name in aliases_cleaned:
            return canonical

    # 2. Substring alias match (only if alias length > 3)
    for canonical in NATIONAL_KEYS:
        for alias_clean in TEAM_ALIASES_CLEAN.get(canonical, []):
            if len(alias_clean) > 3 and re.search(rf"\b{re.escape(alias_clean)}\b", clean_name):
                return canonical

    for canonical, aliases_cleaned in TEAM_ALIASES_CLEAN.items():
        for alias_clean in aliases_cleaned:
            if len(alias_clean) > 3 and re.search(rf"\b{re.escape(alias_clean)}\b", clean_name):
                return canonical

    # 3. Fuzzy match (nationals first at higher cutoff, then clubs)
    nat_candidates = [t.lower() for t in NATIONAL_KEYS]
    club_candidates = [t.lower() for t in MASTER_TEAMS if t not in NATIONAL_KEYS]

    best_match = _fuzzy_match(clean_name, nat_candidates, 0.85)
    if not best_match:
        best_match = _fuzzy_match(clean_name, club_candidates, 0.7)

    if best_match:
        match = best_match[0]

        # check nationals first
        for canonical in NATIONAL_KEYS:
            if match == canonical.lower() or match in TEAM_ALIASES_CLEAN.get(canonical, []):
                return canonical

        # fallback to any team
        for canonical in MASTER_TEAMS:
            if match == canonical.lower() or match in TEAM_ALIASES_CLEAN.get(canonical, []):
                return canonical

    # 4. Fallback → return original, log at DEBUG, and rate-limit WARNs if too many
    logger.debug("Fallback normalization used for: %s", name)
    try:
        global _FALLBACK_CNT
        _FALLBACK_CNT += 1
        _fallback_burst_guard()
    except Exception:
        pass

    return name.strip()


# ================================================================
# LOAD & RELOAD
# ================================================================
def reload_team_aliases() -> None:
    """
    Reload aliases for both clubs and national teams.
    Expands MASTER_TEAMS accordingly and clears cache.
    """
    global TEAM_ALIASES, TEAM_ALIASES_CLEAN, MASTER_TEAMS, NATIONAL_KEYS

    # Provide sensible defaults if files are missing
    default_clubs = {
        "Manchester United": ["Man Utd", "Man United"],
        "Chelsea": ["Blues", "CFC"]
    }
    default_nationals = {
        "Kenya": ["Harambee Stars"],
        "Brazil": ["Brasil", "Seleção"]
    }

    club_aliases = _load_json(CLUB_ALIASES_FILE, default_clubs)
    nat_aliases = _load_json(NATIONAL_ALIASES_FILE, default_nationals)

    TEAM_ALIASES = {**club_aliases, **nat_aliases}
    TEAM_ALIASES_CLEAN = {
        canonical: [_clean_text(alias) for alias in aliases]
        for canonical, aliases in TEAM_ALIASES.items()
    }

    # Track which canonicals are nationals
    NATIONAL_KEYS = set(nat_aliases.keys())

    # Expand MASTER_TEAMS with all canonical names from alias files
    MASTER_TEAMS = sorted(set(MASTER_TEAMS) | set(TEAM_ALIASES.keys()))

    normalize_team.cache_clear()
    logger.info("Team + National team aliases reloaded.")


# Load once at import (after normalize_team is defined)
reload_team_aliases()


# ================================================================
# BULK ALIAS SCRAPER (Wikipedia / Transfermarkt stub)
# ================================================================
def scrape_aliases_and_update_json():
    """
    ⚡️ Experimental stub:
    Fetches nicknames/aliases for teams from Wikipedia/Transfermarkt
    and appends them to team_aliases.json.
    """
    logger.info("Alias scraping automation not implemented yet.")
