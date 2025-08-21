# core/calculator.py

def calculate_3way_arbitrage(odds1, oddsX, odds2, total_stake):
    """
    Takes three odds (1, X, 2) and total stake,
    returns arbitrage stake distribution and profit if arb exists.
    """
    arb_percent = (1 / odds1) + (1 / oddsX) + (1 / odds2)

    if arb_percent >= 1:
        return None  # No arbitrage opportunity

    # Calculate payout
    total_payout = total_stake / arb_percent
    stake1 = total_payout / odds1
    stakeX = total_payout / oddsX
    stake2 = total_payout / odds2
    profit = total_payout - total_stake
    roi = (profit / total_stake) * 100

    return {
        "stake1": round(stake1, 2),
        "stakeX": round(stakeX, 2),
        "stake2": round(stake2, 2),
        "payout": round(total_payout, 2),
        "profit": round(profit, 2),
        "roi": round(roi, 2),
        "arb_percent": round(arb_percent, 4)
    }
def calculate_2way_arbitrage(odds_a, odds_b, total_stake):
    arb_percent = (1 / odds_a) + (1 / odds_b)
    
    if arb_percent >= 1:
        return None  # No arbitrage

    total_payout = total_stake / arb_percent
    stake_a = total_payout / odds_a
    stake_b = total_payout / odds_b
    profit = total_payout - total_stake
    roi = (profit / total_stake) * 100

    return {
        "stake_a": round(stake_a, 2),
        "stake_b": round(stake_b, 2),
        "payout": round(total_payout, 2),
        "profit": round(profit, 2),
        "roi": round(roi, 2),
        "arb_percent": round(arb_percent, 4)
    }
