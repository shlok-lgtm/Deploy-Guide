# basis-methodology

> **Note:** this tree is staged inside `basis-protocol/basis-hub` because
> the current MCP toolchain cannot create a new GitHub repository outside
> of the `basis-hub` scope. The intended home for this code is a
> **separate** repository `basis-protocol/basis-methodology`. See
> `EXTRACT.md` in this directory for the one-shot extraction steps.

## Purpose

Basis publishes the **Stablecoin Integrity Index** (and related indices)
from a production codebase — `basis-protocol/basis-hub`. That codebase
is large, carries runtime concerns (database, workers, API), and reads
heterogeneous inputs from the open internet. A skeptical reader cannot
audit it in a weekend.

`basis-methodology` is a **second, much smaller codebase** that:

1. Specifies the SII formula as prose + tables (`spec/`).
2. Implements that specification in ~300 lines of dependency-free
   Python (`reference/basis_reference/`).
3. Runs the reference implementation against ≥5 production input
   vectors captured from the hub, asserting byte-identical score and
   hash output (`verify/`).
4. Fails CI if the two codebases diverge.

The reproducibility claim is: if the two independent implementations
agree on ≥5 real production vectors, the formula is what the spec says
it is.

## Repo layout

```
.
├── paper/             # Discussion-format whitepaper draft (narrative)
├── spec/              # Formal specification (mechanical, no marketing)
│   ├── sii_formula.md
│   └── psi_formula.md
├── reference/
│   └── basis_reference/
│       ├── __init__.py
│       ├── sii.py     # ~200 lines; stdlib only
│       └── hashing.py
├── test_vectors/      # Captured production input vectors + expected outputs
│   └── README.md
├── verify/
│   ├── __init__.py
│   ├── test_reproducibility.py   # runs reference impl; asserts equality
│   └── capture_vectors.py        # pulls vectors from basis-hub (one-off)
├── .github/workflows/
│   └── reproducibility.yml
└── README.md
```

## What counts as passing

The single acceptance test is:

```
for vector in test_vectors/*.json:
    spec_score, spec_hash = basis_reference.sii.compute(vector.inputs)
    assert spec_score == vector.expected_score
    assert spec_hash  == vector.expected_hash
```

where `vector.expected_score` and `vector.expected_hash` are the values
published by `basis-hub` in its `scores` table and
`computation_attestation` log for that input vector. A mismatch on a
single vector fails CI.

## Current honest status

- `spec/sii_formula.md` — drafted in this PR; needs third-party review.
- `reference/basis_reference/sii.py` — drafted in this PR; not yet run
  against any vectors (no vectors have been captured).
- `test_vectors/` — empty. `verify/capture_vectors.py` is the tool that
  will fill it, but it requires network access to the live hub API.
- `.github/workflows/reproducibility.yml` — defined but not running
  (will activate when this tree is extracted to its own repo).

The reproducibility claim in the README of `basis-hub` must NOT be made
until `test_vectors/` contains ≥5 real vectors and the CI job has
passed at least once on the extracted repo.
