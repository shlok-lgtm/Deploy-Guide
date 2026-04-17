"""
Canonical hashing for SII computations.

Implements section 8 of spec/sii_formula.md. Uses only Python stdlib.
"""

from __future__ import annotations

import hashlib
import json
from typing import Optional


def canonical_input_string(category_scores: dict) -> str:
    """Deterministic JSON serialization of the input category vector."""
    return json.dumps(category_scores, sort_keys=True, separators=(",", ":"))


def input_hash(category_scores: dict) -> str:
    canonical = canonical_input_string(category_scores)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def computation_hash(category_scores: dict, version: str, score: Optional[float]) -> str:
    """sha256(input_hash || '|' || version || '|' || score formatted as %.6f or 'null')."""
    ih = input_hash(category_scores)
    if score is None:
        score_str = "null"
    else:
        score_str = format(float(score), ".6f")
    output_str = f"{ih}|{version}|{score_str}"
    return "0x" + hashlib.sha256(output_str.encode("utf-8")).hexdigest()
