# scrapers/sportpesa_scraper.py

from utils.match_utils import normalize_market, normalize_odds, normalize_match_name

def get_odds():
    raw_data = [
        {
            "sport": "Football",
            "match": "Manchester United vs Chelsea",
            "market": "Match Winner",  # will normalize to 1X2
            "bookmaker": "SportPesa",
            "odds": {"1": 4.7, "X": 7.0, "2": 4.9},
            "match_time": "2025-07-16 19:30",
            "url": "https://ke.sportpesa.com/game/789"
        },
        {
            "sport": "Football",
            "match": "Arsenal v Liverpool",
            "market": "Both Teams To Score",  # will normalize to BTTS
            "bookmaker": "SportPesa",
            "odds": {"Yes": 1.9, "No": 3.0},
            "match_time": "2025-07-17 21:00",
            "url": "https://ke.sportpesa.com/game/790"
        },
        {
            "sport": "Football",
            "match": "Barcelona - Real Madrid",
            "market": "Over 2.5",  # Over/Under market
            "bookmaker": "SportPesa",
            "odds": {"Over": 1.85, "Under": 2.40},
            "match_time": "2025-07-18 21:00",
            "url": "https://ke.sportpesa.com/game/791"
        }
    ]

    normalized_data = []
    for item in raw_data:
        normalized_data.append({
            "sport": item["sport"],
            "match": normalize_match_name(item["match"]),
            "market": normalize_market(item["market"]),
            "bookmaker": item["bookmaker"],
            "odds": normalize_odds(item["odds"], item["market"]),
            "match_time": item["match_time"],
            "url": item["url"]
        })

    return normalized_data
