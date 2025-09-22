# utils/market_aliases.py
import re


def normalize_outcomes(outcome: str) -> str:
    """
    Normalize betting outcomes to canonical values.

    Examples:
      "1"       -> "HOME"
      "2"       -> "AWAY"
      "X"       -> "DRAW"
      "Yes"     -> "YES"
      "No"      -> "NO"
      "Over"    -> "OVER"
      "Under"   -> "UNDER"
      "1X"      -> "HOME_OR_DRAW"
      "12"      -> "HOME_OR_AWAY"
      "X2"      -> "DRAW_OR_AWAY"
    """
    if not outcome:
        return ""

    key = re.sub(r"[^a-z0-9]", "", outcome.lower())

    mapping = {
        # Basic 1X2
        "1": "HOME",
        "home": "HOME",
        "team1": "HOME",

        "2": "AWAY",
        "away": "AWAY",
        "team2": "AWAY",

        "x": "DRAW",
        "draw": "DRAW",
        "d": "DRAW",

        # BTTS
        "yes": "YES",
        "y": "YES",
        "no": "NO",
        "n": "NO",

        # Totals
        "over": "OVER",
        "o": "OVER",
        "under": "UNDER",
        "u": "UNDER",

        # Double chance
        "1x": "HOME_OR_DRAW",
        "12": "HOME_OR_AWAY",
        "x2": "DRAW_OR_AWAY",
    }

    return mapping.get(key, outcome.upper())
