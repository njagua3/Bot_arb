# scrapers/odibets.py

from utils.match_utils import build_match_dict

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
        # Split match into home and away teams
        if " vs " in m["match"]:
            home_team, away_team = m["match"].split(" vs ")
        elif " - " in m["match"]:
            home_team, away_team = m["match"].split(" - ")
        else:
            home_team, away_team = m["match"], ""

        normalized_matches.append(
            build_match_dict(
                home_team=home_team,
                away_team=away_team,
                start_time=m["match_time"],
                market=m["market"],
                odds=m["odds"],
                bookmaker=m["bookmaker"]
            )
        )

    return normalized_matches

if __name__ == "__main__":
    data = get_odds()
    print(data)