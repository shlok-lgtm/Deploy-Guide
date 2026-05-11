"""Tests for CoinGecko collector trust-score parsing."""

from app.collectors.coingecko import _is_high_trust, _has_trust_signal


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


def _compute_trust_ratio_signal_aware(tickers):
    """Replicate the signal-aware trust-ratio calculation.

    Mirrors the production logic in collect_market_activity_components:
    divide high_trust by tickers-with-signal, not all tickers. Returns
    (ratio, signaled_count) so callers can detect the "no signal at all"
    case where the production code now emits is_stale=True.
    """
    rated = [t for t in tickers if _has_trust_signal(t)]
    if not rated:
        return None, 0
    high_trust = sum(1 for t in rated if _is_high_trust(t))
    return high_trust / len(rated), len(rated)


def _compute_trust_ratio(tickers):
    """Back-compat helper: same as signal-aware, but flattens to ratio only."""
    ratio, _ = _compute_trust_ratio_signal_aware(tickers)
    return ratio if ratio is not None else 0.0


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


# ---- _has_trust_signal: distinguishes "field absent" from "field is bad" ----
def test_has_trust_signal_red_is_signal():
    """A 'red' trust_score IS a signal (low trust, but provided)."""
    assert _has_trust_signal({"trust_score": "red"})


def test_has_trust_signal_yellow_is_signal():
    """A 'yellow' trust_score IS a signal."""
    assert _has_trust_signal({"trust_score": "yellow"})


def test_has_trust_signal_high_rank_is_signal():
    """A numeric rank, even 500, IS a signal (CoinGecko did rank the exchange)."""
    assert _has_trust_signal({"trust_score_rank": 500})


def test_has_trust_signal_both_null_is_no_signal():
    """Both fields explicitly null is NOT a signal — CoinGecko told us nothing."""
    assert not _has_trust_signal({"trust_score": None, "trust_score_rank": None})


def test_has_trust_signal_empty_ticker_is_no_signal():
    """Empty ticker (fields missing entirely) is NOT a signal."""
    assert not _has_trust_signal({})


# ---- signal-aware trust-ratio: regression for USDC raw=0/normalized=0 ----
def test_trust_ratio_signal_aware_all_null_returns_none():
    """When every ticker is missing trust signal, ratio is None (not 0).

    This is the USDC bug from 2026-05-11: production wrote raw=0,
    normalized=0 to component_readings even though CoinGecko simply
    hadn't populated the trust_score* fields. Production now records
    this case as is_stale=True with raw=None.
    """
    tickers = _make_tickers([
        (None, None),
        (None, None),
        (None, None),
    ])
    ratio, signaled = _compute_trust_ratio_signal_aware(tickers)
    assert ratio is None
    assert signaled == 0


def test_trust_ratio_signal_aware_partial_signal_divides_by_signaled():
    """If only some tickers have a signal, divide by signaled-count, not total.

    3 tickers; 2 carry signal (one green, one red); 1 is silent. Old
    code would have returned 1/3 ≈ 0.33 (wrongly penalising the absent
    ticker). Signal-aware returns 1/2 = 0.5 — high_trust / rated.
    """
    tickers = _make_tickers([
        ("green", None),   # signal: high trust
        ("red", None),     # signal: low trust
        (None, None),      # NO signal — must not count
    ])
    ratio, signaled = _compute_trust_ratio_signal_aware(tickers)
    assert signaled == 2
    assert ratio == 0.5


def test_trust_ratio_signal_aware_all_red_still_returns_zero():
    """All tickers signal low trust → ratio=0 (correct). Not the bug case."""
    tickers = _make_tickers([
        ("red", None),
        ("red", None),
    ])
    ratio, signaled = _compute_trust_ratio_signal_aware(tickers)
    assert signaled == 2
    assert ratio == 0.0
