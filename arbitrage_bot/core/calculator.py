"""
Arbitrage Calculator Module

Provides functions to detect and calculate arbitrage opportunities
for both 2-way and 3-way markets dynamically.
"""

from typing import Dict, Tuple, Optional


def calculate_implied_probabilities(odds: Dict[str, float]) -> Dict[str, float]:
    """
    Convert odds into implied probabilities.
    Formula: implied probability = 1 / odd
    """
    return {outcome: 1 / odd for outcome, odd in odds.items() if odd > 0}


def is_arbitrage(odds: Dict[str, float]) -> Tuple[bool, float]:
    """
    Check if a given set of odds creates an arbitrage opportunity.
    
    Args:
        odds: Dict of outcome -> odd (e.g., {"1": 2.1, "X": 3.5, "2": 3.0})
    
    Returns:
        (bool, float): True/False if arbitrage exists, and arbitrage margin (%)
    """
    implied_probs = calculate_implied_probabilities(odds)
    total_prob = sum(implied_probs.values())

    arbitrage_exists = total_prob < 1
    profit_margin = (1 - total_prob) * 100 if arbitrage_exists else 0.0

    return arbitrage_exists, round(profit_margin, 2)


def calculate_stakes(odds: Dict[str, float], total_stake: float) -> Optional[Dict[str, float]]:
    """
    Calculate the optimal stake distribution for an arbitrage opportunity.
    
    Args:
        odds: Dict of outcome -> odd
        total_stake: total amount to stake
    
    Returns:
        Dict of outcome -> stake, or None if no arbitrage
    """
    arbitrage, _ = is_arbitrage(odds)
    if not arbitrage:
        return None

    implied_probs = calculate_implied_probabilities(odds)
    total_prob = sum(implied_probs.values())

    stakes = {}
    for outcome, odd in odds.items():
        stakes[outcome] = round((total_stake * (1 / odd)) / total_prob, 2)

    return stakes


def calculate_expected_profit(odds: Dict[str, float], stakes: Dict[str, float]) -> float:
    """
    Calculate guaranteed profit from an arbitrage stake distribution.
    """
    if not stakes:
        return 0.0

    # Pick any outcome – payouts should be equal in true arbitrage
    outcome = next(iter(stakes))
    payout = odds[outcome] * stakes[outcome]
    total_investment = sum(stakes.values())
    profit = payout - total_investment

    return round(profit, 2)


def calculate_arbitrage(
    odds: Dict[str, float],
    total_stake: float,
    min_profit: float = 1.0  # safeguard in absolute currency (KES, USD, etc.)
) -> Optional[Dict]:
    """
    Unified arbitrage calculator for both 2-way and 3-way markets.

    Works for any market with exactly 2 or 3 outcomes.

    Args:
        odds: Dict of outcome -> odd (e.g., {"Over 2.5": 1.95, "Under 2.5": 1.95})
        total_stake: total stake available
        min_profit: minimum absolute profit required to count as arbitrage
    
    Returns:
        Dict containing:
            - stakes (Dict[str, float])
            - payouts (Dict[str, float])
            - profit (float)
            - roi (float)
            - payout (float)
            - margin (float)
        or None if no arbitrage
    """
    if len(odds) not in (2, 3):
        return None  # only support 2-way & 3-way for now

    arbitrage, margin = is_arbitrage(odds)
    if not arbitrage:
        return None

    stakes = calculate_stakes(odds, total_stake)
    if not stakes:
        return None

    payouts = {o: round(stakes[o] * odds[o], 2) for o in stakes}
    total_investment = sum(stakes.values())
    profit = round(next(iter(payouts.values())) - total_investment, 2)

    if profit < min_profit:  # enforce absolute profit threshold
        return None

    roi = round((profit / total_investment) * 100, 2) if total_investment > 0 else 0.0
    payout = profit + total_investment

    return {
        "stakes": stakes,
        "payouts": payouts,
        "profit": profit,
        "roi": roi,
        "payout": round(payout, 2),
        "margin": margin,
    }


# Convenience wrappers for backwards compatibility

def calculate_3way_arbitrage(o1: float, oX: float, o2: float, total_stake: float):
    return calculate_arbitrage({"1": o1, "X": oX, "2": o2}, total_stake)


def calculate_2way_arbitrage(oA: float, oB: float, total_stake: float):
    return calculate_arbitrage({"A": oA, "B": oB}, total_stake)
