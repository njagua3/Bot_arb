import time
from core.settings import load_stake
from core.logger import log_to_file
from core.cache import is_duplicate_alert, store_alert, load_alert_cache
from core.normalizer import normalize_market_name
from bots.odibets import get_odds as get_odibets
from bots.betika import get_odds as get_betika
from bots.sportpesa import get_odds as get_sportpesa
from core.calculator import calculate_3way_arbitrage, calculate_2way_arbitrage
from core.telegram import send_telegram_alert

TOTAL_STAKE = load_stake()

def run_check():
    print("🔍 Scanning for arbitrage opportunities...\n")

    all_odds = get_odibets() + get_betika() + get_sportpesa()

    match_groups = {}
    for entry in all_odds:
        key = (entry["match"], normalize_market_name(entry["market"]))
        match_groups.setdefault(key, []).append(entry)

    for (match, market), entries in match_groups.items():
        if len(entries) < 2:
            continue

        odds_sample = list(entries[0]["odds"].keys())
        match_time = entries[0]["match_time"]
        is_3way = set(odds_sample) == {"1", "X", "2"}
        is_2way = set(odds_sample) == {"Yes", "No"} or len(odds_sample) == 2

        if is_3way:
            best = {"1": 0, "X": 0, "2": 0}
            best_sources = {"1": "", "X": "", "2": ""}
            best_urls = {"1": "", "X": "", "2": ""}

            for entry in entries:
                for opt in ["1", "X", "2"]:
                    if entry["odds"][opt] > best[opt]:
                        best[opt] = entry["odds"][opt]
                        best_sources[opt] = entry["bookmaker"]
                        best_urls[opt] = entry["url"]

            result = calculate_3way_arbitrage(best["1"], best["X"], best["2"], TOTAL_STAKE)

            if result:
                if is_duplicate_alert(match, market, match_time, result["profit"]):
                    print(f"🔁 Skipping duplicate alert for {match} ({market}) [{match_time}]\n")
                    log_to_file(f"🔁 Skipped duplicate: {match} | {market} | Profit: {result['profit']}")
                    continue

                store_alert(match, market, match_time, result["profit"])
                log_to_file(f"✅ New Arbitrage: {match} | {market} | Profit: {result['profit']}")

                print(f"✅ 3-WAY Arbitrage for {match} ({market})")
                print(f"Profit: {result['profit']} KES | ROI: {result['roi']}%\n")

                msg = (
                    f"📣 <b>3-WAY Arbitrage</b>\n"
                    f"🏟️ <b>{match}</b>\n"
                    f"🎯 Market: <b>{market}</b>\n"
                    f"📅 Match Time: <b>{match_time}</b>\n"
                    f"⚽ Sport: {entries[0]['sport']}\n\n"
                    f"💰 Best Odds:\n"
                    f"1 ➤ <a href='{best_urls['1']}'>{best['1']} ({best_sources['1']})</a>\n"
                    f"X ➤ <a href='{best_urls['X']}'>{best['X']} ({best_sources['X']})</a>\n"
                    f"2 ➤ <a href='{best_urls['2']}'>{best['2']} ({best_sources['2']})</a>\n\n"
                    f"📊 Stake Split (KES):\n"
                    f"1 = {result['stake1']}\n"
                    f"X = {result['stakeX']}\n"
                    f"2 = {result['stake2']}\n\n"
                    f"🟢 Profit: {result['profit']} KES\n"
                    f"📈 ROI: {result['roi']}%"
                )

                send_telegram_alert(msg)

        elif is_2way:
            opts = odds_sample
            best = {opt: 0 for opt in opts}
            best_sources = {opt: "" for opt in opts}
            best_urls = {opt: "" for opt in opts}

            for entry in entries:
                for opt in opts:
                    if entry["odds"][opt] > best[opt]:
                        best[opt] = entry["odds"][opt]
                        best_sources[opt] = entry["bookmaker"]
                        best_urls[opt] = entry["url"]

            result = calculate_2way_arbitrage(best[opts[0]], best[opts[1]], TOTAL_STAKE)

            if result:
                if is_duplicate_alert(match, market, match_time, result["profit"]):
                    print(f"🔁 Skipping duplicate alert for {match} ({market}) [{match_time}]\n")
                    log_to_file(f"🔁 Skipped duplicate: {match} | {market} | Profit: {result['profit']}")
                    continue

                store_alert(match, market, match_time, result["profit"])
                log_to_file(f"✅ New Arbitrage: {match} | {market} | Profit: {result['profit']}")

                print(f"✅ 2-WAY Arbitrage for {match} ({market})")
                print(f"Profit: {result['profit']} KES | ROI: {result['roi']}%\n")

                msg = (
                    f"📣 <b>2-WAY Arbitrage</b>\n"
                    f"🏟️ <b>{match}</b>\n"
                    f"🎯 Market: <b>{market}</b>\n"
                    f"📅 Match Time: <b>{match_time}</b>\n"
                    f"⚽ Sport: {entries[0]['sport']}\n\n"
                    f"💰 Best Odds:\n"
                    f"{opts[0]} ➤ <a href='{best_urls[opts[0]]}'>{best[opts[0]]} ({best_sources[opts[0]]})</a>\n"
                    f"{opts[1]} ➤ <a href='{best_urls[opts[1]]}'>{best[opts[1]]} ({best_sources[opts[1]]})</a>\n\n"
                    f"📊 Stake Split (KES):\n"
                    f"{opts[0]} = {result['stake_a']}\n"
                    f"{opts[1]} = {result['stake_b']}\n\n"
                    f"🟢 Profit: {result['profit']} KES\n"
                    f"📈 ROI: {result['roi']}%"
                )

                send_telegram_alert(msg)

        else:
            print(f"⚠️ Unknown market structure: {market} ({match})")

# 🚀 Start bot loop
if __name__ == "__main__":
    print("🚀 Arbitrage Bot Started!")
    load_alert_cache()

    while True:
        run_check()
        print("⏳ Waiting 180 seconds before next scan...\n")
        time.sleep(180)
