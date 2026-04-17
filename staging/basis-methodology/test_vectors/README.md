# test_vectors/

Captured production SII input vectors + expected outputs.

**This directory is empty in the initial scaffold.** Populate it by
running `verify/capture_vectors.py` against a live basis-hub:

```bash
python verify/capture_vectors.py \
    --api https://basisprotocol.xyz \
    --out test_vectors/ \
    --limit 10
```

Each file is a single SII vector:

```json
{
  "coin": "usdc",
  "version": "v1.0.0",
  "inputs": {
    "peg_stability": 98.1,
    "liquidity_depth": 92.4,
    "mint_burn_dynamics": 78.0,
    "holder_distribution": 71.3,
    "structural_risk_composite": 85.2
  },
  "expected_score": 88.45,
  "expected_input_hash": "...",
  "expected_computation_hash": "0x...",
  "captured_from": "https://basisprotocol.xyz"
}
```

The reproducibility test requires **≥5 vectors** before it will run
assertions. Fewer vectors trigger a `pytest.skip` so CI fails cleanly
on an empty repo rather than passing vacuously.

## Acceptance

CI passes iff, for every vector here, the reference implementation in
`reference/basis_reference/sii.py` produces `score`, `input_hash`, and
`computation_hash` byte-identical to the `expected_*` fields captured
from the hub.
