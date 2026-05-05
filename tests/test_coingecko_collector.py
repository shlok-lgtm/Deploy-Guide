"""Tests for CoinGecko collector trust-score parsing."""

from app.collectors.coingecko import _is_high_trust


def _make_tickers(specs):
    """Build minimal ticker dicts from a list of (trust_score, trust_score_rank) tuples."""
    tickers = []
    for trust_score, trust_score_rank in specs:
        t = {}
        if trust_score is not None:
            t["trust_score"] = trust_score
        if trust_score_rank is not None:
            t["trust_score_rank"] = trust_score_rank
        tickers.append(t)
    return tickers


def _compute_trust_ratio(tickers):
    """Replicate the trust-ratio calculation from collect_market_activity_components."""
    if not tickers:
        return 0.0
    high_trust = sum(1 for t in tickers if _is_high_trust(t))
    return high_trust / len(tickers)


# ---- test_exchange_trust_ratio_handles_string_trust_score ----
def test_exchange_trust_ratio_handles_string_trust_score():
    """Legacy format: trust_score is 'green'/'yellow'/'red'. Only green counts."""
    tickers = _make_tickers([
        ("green", None),   # high trust
        ("green", None),   # high trust
        ("red", None),     # not high trust
    ])
    ratio = _compute_trust_ratio(tickers)
    assert ratio == 2 / 3, f"Expected 2/3, got {ratio}"


# ---- test_exchange_trust_ratio_handles_numeric_rank ----
def test_exchange_trust_ratio_handles_numeric_rank():
    """Modern format: trust_score is null, trust_score_rank is numeric."""
    tickers = _make_tickers([
        (None, 3),    # rank <= 10 -> high trust
        (None, 10),   # rank <= 10 -> high trust
        (None, 11),   # rank > 10  -> not high trust
        (None, 250),  # rank > 10  -> not high trust
    ])
    ratio = _compute_trust_ratio(tickers)
    assert ratio == 2 / 4, f"Expected 2/4, got {ratio}"


# ---- test_exchange_trust_ratio_mixed_formats ----
def test_exchange_trust_ratio_mixed_formats():
    """Mix of legacy string and modern numeric formats."""
    tickers = _make_tickers([
        ("green", None),  # high trust via string
        (None, 5),        # high trust via rank
        ("red", 50),      # neither qualifies
    ])
    ratio = _compute_trust_ratio(tickers)
    assert ratio == 2 / 3, f"Expected 2/3, got {ratio}"


# ---- edge cases ----
def test_is_high_trust_empty_ticker():
    """Empty ticker dict should not be high trust."""
    assert not _is_high_trust({})


def test_is_high_trust_yellow_not_high():
    """Yellow trust_score is NOT high trust (only green is)."""
    assert not _is_high_trust({"trust_score": "yellow"})


def test_is_high_trust_rank_boundary():
    """Rank exactly 10 is high trust; 11 is not."""
    assert _is_high_trust({"trust_score_rank": 10})
    assert not _is_high_trust({"trust_score_rank": 11})
