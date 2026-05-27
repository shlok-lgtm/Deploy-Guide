"""Per-stream freshness check for discovery_signals.

Regression: 2026-05-21 04:52 — entity_discovery went silent across all
five Circle 7 generic-index domains (bri, cxri, lsti, tti, vsri). The
aggregate `MAX(detected_at) FROM discovery_signals` stayed fresh because
large_mint_burn / micro_depeg kept firing every ~5 min from
run_discovery_cycle. check_discovery_freshness reported 'healthy' for
5+ days while a covered output stream was dead.

These tests pin the fix: check_discovery_freshness must verify each
declared (signal_type, domain) stream against its own cadence.
"""
from datetime import datetime, timedelta, timezone
from unittest.mock import patch


def _ts(hours_ago):
    return datetime.now(timezone.utc) - timedelta(hours=hours_ago)


def test_aggregate_fresh_hides_stale_entity_discovery():
    """The 2026-05-21 fingerprint: large_mint_burn / micro_depeg fresh
    (~5min), entity_discovery 200h old (cadence 168h). Must NOT be
    healthy."""
    def fake_safe_fetch_one(sql, params):
        signal_type, _ = params
        if signal_type in ("large_mint_burn", "micro_depeg"):
            return {"ts": _ts(0.08)}
        if signal_type == "concentration_topology":
            return {"ts": _ts(1)}
        if signal_type == "entity_discovery":
            return {"ts": _ts(200)}
        return {"ts": None}

    with patch("app.ops.tools.health_checker._safe_fetch_one",
               side_effect=fake_safe_fetch_one):
        from app.ops.tools.health_checker import check_discovery_freshness
        result = check_discovery_freshness()

    assert result["system"] == "discovery"
    assert result["status"] != "healthy"
    streams = {s["stream"]: s["status"] for s in result["details"]["streams"]}
    assert streams["large_mint_burn:sii"] == "healthy"
    assert streams["micro_depeg:sii"] == "healthy"
    for idx in ("bri", "cxri", "lsti", "tti", "vsri"):
        assert streams[f"entity_discovery:{idx}"] in ("degraded", "down")


def test_all_streams_within_cadence_is_healthy():
    def fake(sql, params):
        return {"ts": _ts(1)}
    with patch("app.ops.tools.health_checker._safe_fetch_one", side_effect=fake):
        from app.ops.tools.health_checker import check_discovery_freshness
        result = check_discovery_freshness()
    assert result["status"] == "healthy"
    assert result["details"]["healthy"] == result["details"]["stream_count"]


def test_stream_past_2x_cadence_escalates_to_down():
    """Beyond 2× cadence the overall status escalates to 'down', even
    if the affected stream is just one of many."""
    def fake(sql, params):
        signal_type, _ = params
        if signal_type == "large_mint_burn":
            return {"ts": _ts(10)}  # 2.5× the 4h cadence
        return {"ts": _ts(0.1)}
    with patch("app.ops.tools.health_checker._safe_fetch_one", side_effect=fake):
        from app.ops.tools.health_checker import check_discovery_freshness
        result = check_discovery_freshness()
    assert result["status"] == "down"


def test_never_fired_stream_is_down():
    def fake(sql, params):
        signal_type, domain = params
        if signal_type == "entity_discovery" and domain == "bri":
            return {"ts": None}
        return {"ts": _ts(0.1)}
    with patch("app.ops.tools.health_checker._safe_fetch_one", side_effect=fake):
        from app.ops.tools.health_checker import check_discovery_freshness
        result = check_discovery_freshness()
    assert result["status"] == "down"
    bri = next(s for s in result["details"]["streams"]
               if s["stream"] == "entity_discovery:bri")
    assert bri["status"] == "never_fired"


def test_coherence_output_stream_check_flags_silent_stream():
    """Coherence sweep's _check_output_streams returns a per-stream issue
    when MAX(detected_at) exceeds the declared cadence, regardless of
    whether state_attestations for the domain are fresh."""

    def fake_fetch_one(query, params=None):
        if "entity_discovery" in query:
            return {"ts": _ts(200)}  # > 168h cadence
        if "mempool_observations" in query:
            return {"ts": _ts(0.01)}
        return {"ts": _ts(0.01)}

    with patch("app.coherence.fetch_one", side_effect=fake_fetch_one):
        from app.coherence import _check_output_streams
        issues = _check_output_streams()

    silent = [i for i in issues if "entity_discovery" in i["stream"]]
    assert len(silent) == 5, f"expected 5 entity_discovery streams flagged, got {silent}"
    for issue in silent:
        assert issue["check"] == "output_stream"
        assert issue["domain"] == "discovery_signals"
        assert issue["severity"] in ("warning", "alert")


def test_coherence_output_stream_check_flags_dead_mempool_capture():
    """mempool_observations.seen_at age > 2h cadence — heartbeat may say
    fresh because _attest_observations_summary fires constantly with
    {status: no_rows_yet}; the OUTPUT-TABLE check sees through it."""

    def fake_fetch_one(query, params=None):
        if "mempool_observations" in query:
            return {"ts": _ts(24 * 19)}  # ~19 days dark
        return {"ts": _ts(0.01)}

    with patch("app.coherence.fetch_one", side_effect=fake_fetch_one):
        from app.coherence import _check_output_streams
        issues = _check_output_streams()

    mempool = [i for i in issues if i["stream"] == "mempool_observations:seen_at"]
    assert len(mempool) == 1
    assert mempool[0]["severity"] == "alert"
