"""
basis-methodology reference implementation.

This package is a deliberately minimal, dependency-free translation of
the SII specification in `spec/sii_formula.md`. It exists so that a
third party can audit the formula end-to-end without touching the
production basis-hub codebase.
"""

from basis_reference.sii import compute as compute_sii
from basis_reference.hashing import input_hash, computation_hash

__all__ = ["compute_sii", "input_hash", "computation_hash"]
__version__ = "v1.0.0"
