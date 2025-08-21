# scrapers/odibets_scraper.py

from utils.match_utils import normalize_match_name, generate_match_id

def get_odds():
    raw_matches = [
        {
            "sport": "Football",
            "match": "Manchester United vs Chelsea",
            "market": "1X2",
            "bookmaker": "Odibets",
            "odds": {"1": 2.4, "X": 3.1, "2": 6.0},
            "match_time": "2025-07-16 19:30",
            "url": "https://odibets.com/game/123"
        },
        {
            "sport": "Football",
            "match": "Barcelona vs Real Madrid",
            "market": "Over 2.5",
            "bookmaker": "Odibets",
            "odds": {"Over": 2.80, "Under": 5.98},
            "match_time": "2025-07-18 21:00",
            "url": "https://odibets.com/game/124"
        }
    ]

    normalized_matches = []
    for m in raw_matches:
        normalized_name = normalize_match_name(m["match"])
        normalized_matches.append({
            **m,
            "match": normalized_name,
            "match_id": generate_match_id(normalized_name, m["match_time"])
        })

    return normalized_matches
