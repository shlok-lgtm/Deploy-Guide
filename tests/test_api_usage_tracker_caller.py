"""
Tests for caller auto-resolution in app.api_usage_tracker.track_api_call.

These tests inspect the in-memory _buffer directly (without flushing to DB),
so they can run without a database connection.
"""

import sys
import pytest

from app import api_usage_tracker
from app.api_usage_tracker import _resolve_caller, track_api_call


@pytest.fixture(autouse=True)
def _reset_buffer():
    """Clear the buffer (and counters) before and after every test."""
    with api_usage_tracker._buffer_lock:
        api_usage_tracker._buffer.clear()
    with api_usage_tracker._counters_lock:
        api_usage_tracker._counters.clear()
    yield
    with api_usage_tracker._buffer_lock:
        api_usage_tracker._buffer.clear()
    with api_usage_tracker._counters_lock:
        api_usage_tracker._counters.clear()


def _last_entry() -> dict:
    """Pop the most recent buffered tracking entry."""
    with api_usage_tracker._buffer_lock:
        assert api_usage_tracker._buffer, "no entries were buffered"
        return api_usage_tracker._buffer[-1]


def test_explicit_caller_preserved():
    """An explicit caller= argument must be passed through unchanged."""
    track_api_call("test_provider", "/test", caller="explicit_caller_value")
    entry = _last_entry()
    assert entry["caller"] == "explicit_caller_value"
    assert entry["provider"] == "test_provider"
    assert entry["endpoint"] == "/test"


def _helper_that_calls_track():
    """Local helper that calls track_api_call without caller=.

    The auto-resolved caller should reflect this test module, not "unknown".
    """
    track_api_call("test_provider", "/auto")


def test_auto_caller_from_module():
    """When caller is omitted, it should be resolved from the calling frame."""
    _helper_that_calls_track()
    entry = _last_entry()
    caller = entry["caller"]
    # The caller may be the test module name (e.g. "tests.test_api_usage_tracker_caller"
    # or "test_api_usage_tracker_caller"), or the file basename — but never
    # "unknown" for a normal in-process call.
    assert caller != "unknown", f"auto caller resolved to 'unknown' (got {caller!r})"
    assert isinstance(caller, str) and caller, "caller should be a non-empty string"
    # We don't pin the exact module path because pytest may run this either
    # as 'tests.test_api_usage_tracker_caller' or 'test_api_usage_tracker_caller'
    # depending on rootdir and configuration. Just assert it mentions this file.
    assert "test_api_usage_tracker_caller" in caller, (
        f"expected caller to reference this test module, got {caller!r}"
    )


def test_auto_caller_strips_app_prefix(monkeypatch):
    """_resolve_caller() must strip the leading 'app.' prefix."""

    class _FakeFrame:
        def __init__(self):
            self.f_globals = {
                "__name__": "app.data_layer.peg_monitor",
                "__file__": "/some/path/app/data_layer/peg_monitor.py",
            }

    fake_frame = _FakeFrame()

    def fake_getframe(depth):
        # _resolve_caller calls sys._getframe(2); accept any depth here.
        return fake_frame

    monkeypatch.setattr(sys, "_getframe", fake_getframe)
    assert _resolve_caller() == "data_layer.peg_monitor"


def test_auto_caller_falls_back_on_failure(monkeypatch):
    """If frame inspection raises, _resolve_caller must return 'unknown'."""

    def boom(depth):
        raise RuntimeError("simulated frame access failure")

    monkeypatch.setattr(sys, "_getframe", boom)
    # Must not propagate — must return the sentinel.
    assert _resolve_caller() == "unknown"
