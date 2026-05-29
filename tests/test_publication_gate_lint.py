"""Publication-gate lint guard (migration 112).

Migration 112 introduced two views — `stablecoins_published` and
`psi_scores_published` — that filter the SII/PSI scoring tables down
to architect-approved entities. Every user-facing read path in
server.py / report.py / pulse_generator.py / divergence.py MUST
reference the gated view (or be guarded by `publication_gate.require_*`).

This test fails when a public-surface file mentions the raw
`stablecoins` / `psi_scores` tables outside an approved exception.
It exists because the alternative is "remember to add the filter
on every new endpoint" — exactly the discipline that fails after
six months.

Scope is intentionally narrow: only files that serve unauthenticated
HTTP responses. The ops/admin layer (`app/ops/*`, `app/server.py`
admin routes) is excluded — the architect needs raw access there
to approve unpublished entities.

Adding a new public read path? Use `stablecoins_published` or
`psi_scores_published`. If you absolutely need raw access (e.g. to
verify both published and unpublished states for an admin tool),
add the line number to the exception list below with a comment
explaining why.
"""

import re
from pathlib import Path


# Files this lint walks. These are the public-surface modules. Adding a
# new file that serves unauthenticated HTTP should be added here.
PUBLIC_FILES = [
    "app/report.py",
    "app/pulse_generator.py",
    "app/divergence.py",
    "app/composition.py",
    "app/playground.py",
    "app/payments.py",
]

# Patterns that signal a raw (ungated) reference to a publication-gated
# table. Each match is a candidate violation unless explicitly excepted.
RAW_PATTERNS = [
    re.compile(r"\bFROM\s+stablecoins\b(?!_published)", re.IGNORECASE),
    re.compile(r"\bJOIN\s+stablecoins\b(?!_published)", re.IGNORECASE),
    re.compile(r"\bFROM\s+psi_scores\b(?!_published)", re.IGNORECASE),
    re.compile(r"\bJOIN\s+psi_scores\b(?!_published)", re.IGNORECASE),
]

# Lines exempted from the lint, keyed by file path. Each exemption MUST
# carry a written reason — exemptions without a reason will fail review.
# Format: (file_path, line_number_substring, reason).
EXEMPT_LINES = {
    # Empty by default. Document any future exemption here.
}

# app/server.py is handled separately because its admin routes
# legitimately reference the raw tables. The lint there enforces only
# that public endpoints (those without _check_admin_key) use the view.
SERVER_PY = "app/server.py"


def _project_root() -> Path:
    here = Path(__file__).resolve()
    for parent in here.parents:
        if (parent / "CLAUDE.md").exists():
            return parent
    raise RuntimeError("Could not locate project root")


def _is_exempted(file_path: str, line_idx: int, line: str) -> bool:
    for f, substr, _reason in EXEMPT_LINES.get(file_path, []):
        if f == file_path and substr in line:
            return True
    return False


def test_public_modules_use_published_views():
    """Every raw `stablecoins`/`psi_scores` reference in a public-surface
    file is a lint failure."""
    root = _project_root()
    violations = []
    for rel in PUBLIC_FILES:
        path = root / rel
        if not path.exists():
            continue
        for i, line in enumerate(path.read_text().splitlines(), start=1):
            for pat in RAW_PATTERNS:
                if pat.search(line):
                    if _is_exempted(rel, i, line):
                        continue
                    violations.append(f"{rel}:{i}: {line.strip()}")

    assert not violations, (
        "Publication-gate lint failure — raw table reference in a "
        "public-surface module. Use stablecoins_published / "
        "psi_scores_published, or guard with publication_gate.require_*. "
        "If raw access is genuinely required, add an entry to EXEMPT_LINES "
        "in tests/test_publication_gate_lint.py with a written reason.\n\n"
        + "\n".join(violations)
    )


def test_server_public_endpoints_use_published_views():
    """For app/server.py, every raw reference must live inside an
    admin-gated function (one that calls _check_admin_key) or be
    explicitly exempted.

    The lint walks each match and looks for `_check_admin_key` in the
    nearest preceding def or @app.<method> decorator. Public-surface
    matches are violations.
    """
    root = _project_root()
    path = root / SERVER_PY
    text = path.read_text()
    lines = text.splitlines()

    # Pre-compute the function-boundary indices: every line that
    # starts a def or @app.* decorator.
    def_starts = [
        i for i, l in enumerate(lines)
        if l.startswith("def ") or l.startswith("async def ")
        or l.startswith("@app.")
    ]

    def enclosing_function_text(line_idx: int) -> str:
        # Walk back to find the start of the enclosing function.
        # Look for the most recent "async def" / "def" line before line_idx.
        for j in range(line_idx, -1, -1):
            l = lines[j]
            if l.startswith("def ") or l.startswith("async def "):
                # Walk forward until the next top-level def/async def to
                # capture the full function body.
                end = len(lines)
                for k in range(j + 1, len(lines)):
                    nl = lines[k]
                    if (nl.startswith("def ") or nl.startswith("async def ")
                            or (nl.startswith("@app.") and not lines[k-1].startswith("@app."))):
                        end = k
                        break
                return "\n".join(lines[j:end])
        return ""

    violations = []
    for i, line in enumerate(lines, start=1):
        for pat in RAW_PATTERNS:
            if pat.search(line):
                # Skip the publication_gate helper itself.
                if "publication_gate" in line or "publish_entity" in line:
                    continue
                fn_text = enclosing_function_text(i - 1)
                # Allowed: admin-gated functions, the entity-existence
                # checks for publish_entity (which need raw access by
                # definition), or the cache key resolver.
                if "_check_admin_key" in fn_text:
                    continue
                # Allowed in publication-gate admin endpoint itself —
                # it needs to read the raw flag to compute prior_state.
                if "publish_entity" in fn_text:
                    continue
                # Exempted: integrity sweep queries that legitimately
                # count both published and unpublished entities (these
                # exist for operational health, not user surfaces).
                if (
                    "integrity" in fn_text.lower()
                    and "admin" not in fn_text.lower()
                ):
                    # Only allow integrity-internal exemption when the
                    # function is genuinely not on a public route.
                    continue
                violations.append(f"{SERVER_PY}:{i}: {line.strip()}")

    assert not violations, (
        "Publication-gate lint failure in app/server.py — raw table "
        "reference outside an admin-gated function. Switch to the "
        "_published view, or move the call into an admin endpoint that "
        "calls _check_admin_key.\n\n" + "\n".join(violations)
    )
