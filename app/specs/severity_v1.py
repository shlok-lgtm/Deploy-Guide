"""
BASIS-SEVERITY-v1 Specification
================================
Published taxonomy for classifying assessment event significance.
Version: 1.0.0
Status: Canonical

Severity levels (ordered by significance):
  silent   -- No action required. Background monitoring data.
  notable  -- Interesting but not urgent. Worth logging.
  alert    -- Requires attention. Potential risk change.
  critical -- Immediate action recommended. Material risk event.
"""

SEVERITY_V1 = {
    "version": "1.0.0",
    "levels": [
        {
            "name": "silent",
            "ordinal": 0,
            "description": "No action required. Background monitoring data.",
            "broadcast": False,
            "example_triggers": ["daily_cycle with no score change", "minor peg deviation <0.1%"]
        },
        {
            "name": "notable",
            "ordinal": 1,
            "description": "Interesting but not urgent. Worth logging.",
            "broadcast": False,
            "example_triggers": ["score change >2 points", "new asset auto-promoted", "concentration shift"]
        },
        {
            "name": "alert",
            "ordinal": 2,
            "description": "Requires attention. Potential risk change.",
            "broadcast": True,
            "example_triggers": ["score change >5 points", "score decrease", "large movement >$10M"]
        },
        {
            "name": "critical",
            "ordinal": 3,
            "description": "Immediate action recommended. Material risk event.",
            "broadcast": True,
            "example_triggers": ["depeg >1%", "score drop >10 points", "coverage below 50%"]
        }
    ]
}

SEVERITY_ORDER = {s["name"]: s["ordinal"] for s in SEVERITY_V1["levels"]}


def severity_gte(severity, threshold):
    """Check if a severity level meets or exceeds a threshold."""
    return SEVERITY_ORDER.get(severity, 0) >= SEVERITY_ORDER.get(threshold, 0)
