# Confidence Tier Code Field

## What the `grade` field carries

The `grade` field in `/api/scores` and `/api/psi/scores` carries a **confidence tier code**, not a credit rating.

This is **not** an NRSRO rating, a CRA rating, or any assessment of creditworthiness. It is a two-character code representing the methodological confidence level at which the underlying score was computed.

## Mapping table

| confidence_tag | Code | Meaning |
|---|---|---|
| *(null/high)* | `HI` | High confidence — ≥80% component coverage |
| `STANDARD` | `ST` | Standard confidence — ≥60% component coverage |
| `LIMITED DATA` | `LD` | Limited data — <60% component coverage |
| *(unknown)* | `XX` | Fallback for unrecognized tags |

Version: `confidence_tier_codes_v1`

## On-chain storage

The `bytes2 grade` slot on the BasisOracle contract (BasisSIIOracle.sol) is a legacy schema field name. As of this commit, the semantic meaning is "confidence tier code."

The keeper's `gradeToBytes2()` function packs any two ASCII characters into bytes2. The field is encoding-agnostic — it simply stores two bytes. The interpretation of those bytes changed from letter grades to confidence tier codes.

Examples:
- `"HI"` → `0x4849` (H=0x48, I=0x49)
- `"ST"` → `0x5354` (S=0x53, T=0x54)
- `"LD"` → `0x4c44` (L=0x4c, D=0x44)

## Future methodology hash

The confidence tier code mapping will be registered as `confidence_tier_codes_v1` in the methodology_hashes registry (PR 4) and anchored on-chain via `publishReportHash` with lens `0x00000300`.
