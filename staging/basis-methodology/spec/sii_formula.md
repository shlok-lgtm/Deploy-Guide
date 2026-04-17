# Stablecoin Integrity Index (SII) — v1.0.0 Formula Specification

This is the formal specification of the SII v1.0.0 formula. It is the
ground truth that both `basis-hub/app/scoring.py` and
`basis-methodology/reference/basis_reference/sii.py` must match.

No marketing. No narrative. Only what a deterministic implementation
needs.

---

## 1. Inputs

An SII computation takes a **category score vector**:

```
C = { peg_stability:             s_peg     ∈ [0, 100] ∪ {None},
      liquidity_depth:           s_liq     ∈ [0, 100] ∪ {None},
      mint_burn_dynamics:        s_mb      ∈ [0, 100] ∪ {None},
      holder_distribution:       s_hd      ∈ [0, 100] ∪ {None},
      structural_risk_composite: s_struct  ∈ [0, 100] ∪ {None} }
```

Each category score is itself the weighted average of normalized
component readings. Component-level specification is in section 4.

`None` is permitted for any category and represents "no data at this
cycle." `None` values are excluded from the weighted sum and the weights
are renormalized (section 3).

## 2. Category weights

```
W = { peg_stability:             0.30,
      liquidity_depth:           0.25,
      mint_burn_dynamics:        0.15,
      holder_distribution:       0.10,
      structural_risk_composite: 0.20 }
```

The weights sum to 1.00.

## 3. Aggregation

```
usable    = { c : C[c] is not None }
total     = Σ_{c ∈ usable}  C[c] × W[c]
used_wt   = Σ_{c ∈ usable}  W[c]

SII       = None                  if used_wt = 0
            total                  if used_wt = 1.0
            total / used_wt        otherwise   # renormalize
```

SII ∈ [0, 100] or None.

## 4. Structural composite

The `structural_risk_composite` category is itself the weighted average
of five subcategories:

```
Ws = { reserves_collateral:    0.30,
       smart_contract_risk:    0.20,
       oracle_integrity:       0.15,
       governance_operations:  0.20,
       network_chain_risk:     0.15 }
```

with identical aggregation semantics (renormalize on missing subscores;
return None if all subscores are None).

## 5. Normalization functions

Component readings are raw metrics; they become 0–100 subscores via one
of six pure functions. Every implementation MUST produce byte-identical
outputs for these.

### 5.1 inverse_linear(value, perfect, threshold)
Lower is better.
```
if value ≤ perfect:    return 100.0
if value ≥ threshold:  return 0.0
return 100.0 − ((value − perfect) / (threshold − perfect)) × 100.0
```

### 5.2 linear(value, min_val, max_val)
Higher is better.
```
if value ≤ min_val:    return 0.0
if value ≥ max_val:    return 100.0
return ((value − min_val) / (max_val − min_val)) × 100.0
```

### 5.3 log(value, thresholds)
`thresholds` is a dict `{ upper_bound: score }`. Sort by upper_bound
ascending. Return the score of the first bucket whose upper_bound
exceeds `value`. If `value ≥` all upper_bounds, return the largest
bucket's score. If `value ≤ 0`, return 0.

### 5.4 centered(value, center, tolerance, extreme)
Deviation either side of center is bad.
```
d = |value − center|
if d ≤ tolerance:  return 100.0
if d ≥ extreme:    return 0.0
return 100.0 − ((d − tolerance) / (extreme − tolerance)) × 100.0
```

### 5.5 exponential_penalty(value, ideal, decay_rate=200)
```
d = |value − ideal|
return 100.0 × exp(−d × decay_rate)
```

### 5.6 direct(value)
```
return max(0.0, min(100.0, float(value)))
```

## 6. Floating-point determinism

All arithmetic is IEEE-754 double-precision. All implementations MUST
use Python `float` (or a language equivalent with identical IEEE-754
semantics). The order of operations in the weighted sum is the order in
which the category keys are listed in section 2 above — implementations
MUST iterate `peg_stability, liquidity_depth, mint_burn_dynamics,
holder_distribution, structural_risk_composite` in that exact order,
summing `C[c] × W[c]` left-to-right. The structural subcategory sum
iterates `Ws` in the order listed in section 4.

The renormalization divide (`total / used_wt`) happens **after** the
summation, not during.

## 7. Output

An SII computation returns:

```
{ "score": SII (float in [0, 100]) or None,
  "version": "v1.0.0",
  "used_weight": used_wt,
  "missing_categories": [ c : C[c] is None ] }
```

No grade is returned. Grades are a display-layer transform applied
elsewhere and are not part of the formula.

## 8. Hashing

For attestation purposes, the canonical hash of an SII computation is:

```
canonical_input = json.dumps(C, sort_keys=True, separators=(",", ":"))
input_hash      = sha256(canonical_input).hexdigest()
output_str      = f"{input_hash}|v1.0.0|{score:.6f}"
computation_hash = "0x" + sha256(output_str).hexdigest()
```

Implementations MUST produce these exact strings to be considered
conformant. Note `{score:.6f}` means six digits after the decimal point,
zero-padded, using Python's `%f` / `format()` rules. `None` scores are
formatted as the literal string `"null"`.

## 9. Version identifier

`v1.0.0`. Any change to sections 2, 3, 4, 5, 6, or 8 MUST bump this
identifier. Section 1 (input vector shape) is frozen for v1.x.

## 10. Conformance

An implementation is **conformant** iff, for every `(inputs,
expected_score, expected_hash)` triple in
`basis-methodology/test_vectors/`, it produces the expected score and
expected hash byte-for-byte.
