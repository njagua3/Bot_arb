# scrapers/sportpesa.py

from utils.match_utils import build_match_dict

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
        # Split match into home and away teams
        if " vs " in item["match"]:
            home_team, away_team = item["match"].split(" vs ")
        elif " - " in item["match"]:
            home_team, away_team = item["match"].split(" - ")
        elif " v " in item["match"]:
            home_team, away_team = item["match"].split(" v ")
        else:
            home_team, away_team = item["match"], ""

        normalized_data.append(
            build_match_dict(
                home_team=home_team,
                away_team=away_team,
                start_time=item["match_time"],
                market=item["market"],
                odds=item["odds"],
                bookmaker=item["bookmaker"]
            )
        )

    return normalized_data

if __name__ == "__main__":
    data = get_odds()
    print(data)