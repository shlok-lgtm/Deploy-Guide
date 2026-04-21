"""
Confidence Tier Codes
======================
Maps confidence_tag strings to two-character codes for on-chain
publishing via the bytes2 grade slot on the BasisOracle contract.

These are NOT credit ratings. They represent the methodological
confidence level at which the underlying score was computed.
"""

import logging

logger = logging.getLogger(__name__)

CONFIDENCE_TIER_CODES_VERSION = "v1"

CONFIDENCE_TIER_CODES: dict[str, str] = {
    None: "HI",
    "STANDARD": "ST",
    "LIMITED DATA": "LD",
}

_FALLBACK_CODE = "XX"


def tag_to_code(confidence_tag: str | None) -> str:
    """Return two-character tier code for a confidence_tag value."""
    code = CONFIDENCE_TIER_CODES.get(confidence_tag)
    if code is None and confidence_tag not in CONFIDENCE_TIER_CODES:
        logger.warning(f"Unknown confidence_tag '{confidence_tag}' — using fallback code '{_FALLBACK_CODE}'")
        return _FALLBACK_CODE
    return code
