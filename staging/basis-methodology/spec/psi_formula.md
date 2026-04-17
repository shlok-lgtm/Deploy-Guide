# Protocol Safety Index (PSI) — v0.2.0 Formula Specification

This is a minimal spec of the PSI formula suitable for third-party
reproducibility. Only sections 1–3 and 7–8 are complete; section 4–6
(component normalizations) will be extracted from
`basis-hub/app/index_definitions/psi_v01.py` in a follow-up.

## 1. Inputs

```
C = { operations_maturity:        s_ops    ∈ [0, 100] ∪ {None},
      smart_contract_safety:      s_sc     ∈ [0, 100] ∪ {None},
      economic_safety:            s_econ   ∈ [0, 100] ∪ {None},
      governance_integrity:       s_gov    ∈ [0, 100] ∪ {None},
      user_protection:            s_user   ∈ [0, 100] ∪ {None} }
```

## 2. Category weights

```
W = { operations_maturity:   0.20,
      smart_contract_safety: 0.30,
      economic_safety:       0.20,
      governance_integrity:  0.15,
      user_protection:       0.15 }
```

Source: `app/index_definitions/psi_v01.py::PSI_V01_DEFINITION["categories"]`.

## 3. Aggregation

Identical to SII section 3 — weighted sum with renormalization on
missing categories, `None` if everything is missing.

## 4. Component normalizations

TBD. Must match `psi_v01.py`.

## 5. Floating-point determinism

Same rules as SII section 6. Iteration order is the key order in
section 2 above.

## 6. Hashing

```
canonical_input  = json.dumps(C, sort_keys=True, separators=(",", ":"))
input_hash       = sha256(canonical_input).hexdigest()
output_str       = f"{input_hash}|v0.2.0|{score:.6f}"
computation_hash = "0x" + sha256(output_str).hexdigest()
```

## 7. Version identifier

`v0.2.0`.

## 8. Status

**This spec is a skeleton.** SII v1.0.0 is the first index with a
full-width spec in `basis-methodology`. PSI will follow once ≥5 SII
vectors are passing reproducibility CI.
