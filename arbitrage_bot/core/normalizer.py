import re

def normalize_market_name(raw_market):
    """
    Normalize different market naming styles to standard keys.
    """

    market = raw_market.strip().lower()

    # 1X2 market normalization
    if market in ["1x2", "home/draw/away", "match winner", "result"]:
        return "1X2"
    
    # Both Teams To Score
    if market in ["btts", "both teams to score", "gg/ng", "gg-ng"]:
        return "BTTS"

    # Over/Under goals market
    ou_match = re.search(r"(over|under|o|u)[^\d]*(\d+(\.\d)?)", market)
    if ou_match:
        direction = ou_match.group(1)
        value = ou_match.group(2)

        # Normalize to Over/Under X.X
        return f"Over/Under {value}"

    # Capitalize fallback for consistency
    return raw_market.upper()


def normalize_odds_keys(odds: dict) -> dict:
    """
    Normalize odds option keys to a standard format.

    Example:
    - "Home" -> "1"
    - "Draw" -> "X"
    - "Away" -> "2"
    - "Yes"/"No" stay the same
    """
    mapping = {
        "home": "1",
        "draw": "X",
        "away": "2",
        "team a": "1",
        "team b": "2",
        "yes": "Yes",
        "no": "No"
    }

    normalized = {}
    for key, value in odds.items():
        norm_key = mapping.get(key.strip().lower(), key.strip())
        normalized[norm_key] = value
    return normalized
