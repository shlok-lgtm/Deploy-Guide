# Composition Spec

**Status:** v1 (V9.13 §N publication-ready)
**Last updated:** 2026-05-10

This document is a specification, not a tutorial. It is precise enough to
let a third party reproduce a published composition output_hash without
reading Basis source code.

The serialization rules below are also documented in
`app/composition_serialization.py` (the canonical implementation). If
those two ever drift, this document is authoritative for spec questions
and the source is authoritative for behavior — and the drift itself is
a constitutional break (V9.13 §N).

## Overview

The composition layer derives two indices from upstream attested state:

- **CQI — Collateral Quality Index.** Per (stablecoin, protocol) pair.
  Geometric mean of the stablecoin's SII and the protocol's PSI. Penalises
  weakness in either input — a stablecoin held as collateral by a
  fragile protocol gets a lower CQI than a strong stablecoin in a
  strong protocol.
- **RQS — Reserve Quality Score.** Per protocol. Weighted-average SII
  across the stablecoins held in the protocol's treasury, weighted by
  USD value.

Both are computed on demand from FastAPI handlers. They are not
scheduled. They consume two upstream attested domains:

- `sii_scores` — see `app/scoring.py` (SII v1.0.0; weights canonical).
- `psi_scores` — see `app/index_definitions/psi_v01.py` (PSI v0.2.0).

Composition outputs are **publication-ready** (V9.13 §N). The four
invariants — deterministic compute, stable serialization, attestation at
compute time, documented spec — apply uniformly. On-chain publication is
a future buyer-triggered state; the architecture is bounded already.

## cqi_compositions

Computed by `app.composition.compute_cqi_matrix()`.

### Formula

For each (stablecoin `s`, protocol `p`) with both an SII and a PSI score:

```
CQI(s, p) = round( sqrt( SII(s) * PSI(p) ), 2 )
```

i.e. the geometric mean of the two inputs, rounded to 2 decimal places
(banker's rounding via Python's `round`).

A pair is excluded from the matrix iff either:

- `SII(s)` is `None`, `0`, or negative (geometric mean undefined or
  destructive), or
- `PSI(p)` is `None`, `0`, or negative.

### Inputs

- `SII(s)`: latest `scores.overall_score` for stablecoin `s` keyed by
  `UPPER(stablecoins.symbol)`. Joined to `scores` via `stablecoin_id`.
- `PSI(p)`: latest `psi_scores.overall_score` for protocol `p` keyed by
  `protocol_slug`. Latest is `DISTINCT ON (protocol_slug) ORDER BY
  protocol_slug, computed_at DESC`.

The full input set is hashed at compute time. The hashes recorded in the
attestation payload are computed over this projection:

```
input_sii_hash = canonical_hash(sorted([
  {symbol, overall_score, component_count}
  for each row in scores joined to stablecoins
  where overall_score IS NOT NULL
], by symbol ASC))

input_psi_hash = canonical_hash(sorted([
  {protocol_slug, overall_score}
  for latest row per protocol_slug in psi_scores
  where overall_score IS NOT NULL
], by protocol_slug ASC))
```

### Output shape

```
{
  "matrix": [
    {
      "asset":          str,         // stablecoin symbol (uppercase)
      "protocol":       str,         // protocol display name
      "protocol_slug":  str,
      "cqi_score":      float,       // 2 decimals
      "confidence":     str,         // "limited" | "standard" | "high"
      "sii_score":      float,
      "psi_score":      float
    },
    ...
  ],
  "count": int    // == len(matrix)
}
```

Order is `cqi_score DESC, asset ASC, protocol_slug ASC` (stable
tiebreakers required for determinism).

### Precision

CQI scores: 2 decimal places, banker's rounding (`round` in Python 3).

Serialization precision (when hashing): 8 decimal places. See
`canonical_serialize` below for why 8 was chosen and what the wider
serializer precision means for verifiers.

### Serialization

Per `app.composition_serialization.canonical_serialize`. Rules,
authoritative for verifiers:

1. Wrap the output in `{"v": SERIALIZER_VERSION, "d": <coerced>}`. The
   wrapper records the format version. As of v1, `SERIALIZER_VERSION =
   "composition-serializer-v1"`.
2. Sort all dict keys lexicographically at every nesting level.
3. Coerce values:
   - `None`, `bool`, `int`, `str` → unchanged.
   - `float` → `Decimal(str(x))` → fixed 8-decimal string representation
     with banker's rounding (`ROUND_HALF_EVEN`). Plain decimal notation
     (no scientific form), no trailing zeros stripped.
   - `Decimal` → same as float (8-decimal banker's-rounded string).
   - `datetime` → ISO-8601, normalised to UTC (naive treated as UTC).
   - `date` → ISO-8601 date.
   - `dict` → recursively coerced; non-string keys stringified
     deterministically (`int`/`bool`/`float`/`Decimal` → canonical
     string form, anything else → `str(k)`); raises if two keys collapse
     to the same string.
   - `list` / `tuple` → recursively coerced; order preserved.
   - `set` / `frozenset` → list, sorted by canonical bytes of each
     element.
4. Encode with `json.dumps(sort_keys=True, separators=(",", ":"))` then
   `.encode("utf-8")`. No whitespace at any structural point.

### Attestation domain

`cqi_compositions`. Single record per call:

```
{
  "domain":         "cqi_compositions",
  "computed_at":    str,    // ISO-8601 UTC, hour-truncated
  "input_sii_hash": str,    // hex
  "input_psi_hash": str,    // hex
  "output_hash":    str,    // canonical_hash(output) — see Output shape
  "row_count":      int     // len(matrix)
}
```

Hour-truncation: `datetime.now(UTC).replace(minute=0, second=0,
microsecond=0).isoformat()`. This is what makes determinism testable —
two compute calls within the same hour produce identical attestation
payloads.

## rqs_composition

Computed by `app.composition.compute_rqs_for_protocol(slug)`.

### Formula

```
weighted_sum  = sum(weight_i * SII(symbol_i))   over scored holdings
scored_weight = sum(weight_i)                   over scored holdings
RQS = round( weighted_sum / scored_weight, 2 )  if scored_weight > 0
                                                else None
```

`weight_i` for symbol `i` is `usd_value_i / sum_j(usd_value_j)`, where
each `usd_value` is aggregated over all chains for that symbol from the
protocol's latest snapshot (see Inputs).

A holding is "scored" iff `scores.overall_score` exists for the symbol;
unscored holdings remain in `breakdown` with `scored: false` but do not
contribute to the weighted sum. `RQS` is renormalised over the scored
weight only.

If `coverage_threshold > 0` and `scored_coverage < coverage_threshold`,
`rqs_score = None` and `withheld = true` (caller-provided gate).

### Inputs

- Per-symbol SII: `scores.overall_score` joined to `stablecoins` by
  `UPPER(symbol)`.
- Treasury holdings: `protocol_treasury_holdings` rows with
  `is_stablecoin = TRUE` and `snapshot_date = MAX(snapshot_date)` for
  the protocol. Aggregated by `UPPER(token_symbol)` summing `usd_value`.
- PSI metadata: latest `psi_scores` row per `protocol_slug`.

`input_sii_hash` and `input_psi_hash` are computed identically to
`cqi_compositions` (they are global inventories, not per-protocol).

### Output shape

```
{
  "composite_id":      "rqs",
  "name":              "Reserve Quality Score",
  "rqs_score":         float | None,    // None if withheld
  "scored_coverage":   float,            // 4 decimals
  "coverage_threshold": float,
  "withheld":          bool,
  "confidence":        str,
  "confidence_tag":    str,
  "breakdown":         [ {symbol, weight, sii_score, contribution,
                          scored, usd_value}, ...],
  "warnings":          [str, ...],
  "method":            "weighted_average",
  "formula_version":   "composition-v1.0.0",
  "protocol":          str,             // display name
  "protocol_slug":     str,
  "psi_score":         float | None,
  "treasury_total_usd": float,
  "holdings_as_of":    str,             // ISO date
  "data_as_of":        str,             // older of holdings/SII (date)
  "sii_scored_at":     str              // ISO datetime, oldest input
}
```

`breakdown` is ordered by `contribution DESC, symbol ASC`.

### Precision

`rqs_score`: 2 decimals. `weight`, `scored_coverage`, `contribution`: 4
decimals. `treasury_total_usd`, `usd_value`: 2 decimals.

Serialization precision: 8 decimals, per the canonical serializer.

### Serialization

Per `canonical_serialize` (rules above).

### Attestation domain

`rqs_composition`. Single record per call, with `entity_id =
protocol_slug`:

```
{
  "domain":         "rqs_composition",
  "computed_at":    str,
  "input_sii_hash": str,
  "input_psi_hash": str,
  "output_hash":    str,
  "row_count":      1
}
```

## rqs_compositions

Computed by `app.composition.compute_rqs_all()`.

### Formula

For each `slug` in `app.index_definitions.psi_v01.TARGET_PROTOCOLS`,
compute `compute_rqs_for_protocol(slug)`. Collect successes; record
errors separately.

### Inputs

`TARGET_PROTOCOLS` (snapshot of the registry at compute time). Per-
protocol inputs are as for `rqs_composition`.

`input_sii_hash` and `input_psi_hash` are computed once for the batch,
identically to `cqi_compositions`.

### Output shape

```
{
  "protocols":       [ <result of rqs_composition>, ...],
  "count":           int,
  "skipped":         [ {protocol_slug, error}, ...],
  "formula_version": "composition-v1.0.0"
}
```

`protocols` is ordered by `rqs_score DESC, protocol_slug ASC`. Items
without an `rqs_score` use `0` as the sort key.

### Precision

Same as `rqs_composition` per protocol; 8 decimals at serialization
boundary.

### Serialization

Per `canonical_serialize`.

### Attestation domain

`rqs_compositions`. Single record per call:

```
{
  "domain":         "rqs_compositions",
  "computed_at":    str,
  "input_sii_hash": str,
  "input_psi_hash": str,
  "output_hash":    str,
  "row_count":      int    // == len(protocols)
}
```

## Reproducing a published hash

Given an attestation payload `P` for any of the three domains:

1. **Fetch the SII snapshot at `P.computed_at`.** Take the projection
   `(symbol, overall_score, component_count)` for every row with
   `overall_score IS NOT NULL`, sort by symbol ASC. Compute
   `canonical_hash` of that list and confirm it matches
   `P.input_sii_hash`.
2. **Fetch the PSI snapshot at `P.computed_at`.** Take the projection
   `(protocol_slug, overall_score)` for the latest row per
   `protocol_slug` with `overall_score IS NOT NULL`, sort by
   `protocol_slug` ASC. Compute `canonical_hash` and confirm it matches
   `P.input_psi_hash`.
3. **Apply the formula** for the relevant domain (above).
4. **Order the output** per the "Output shape" rules.
5. **Serialize** per `canonical_serialize`.
6. **Hash with SHA-256** (`canonical_hash`).
7. **Compare** to `P.output_hash`. If equal, the composition was
   produced as specified at the recorded inputs.

If step 1 or step 2 fails, the upstream snapshot has drifted (a row
changed between attestation and verification — or the extraction is
out-of-spec). If step 6 fails, the composition formula has drifted, the
precision has drifted, or the serialization has drifted. Each is a
constitutional break; the snapshot test in
`tests/test_composition_serialization.py` catches the serialization
class loudly, the determinism tests in
`tests/test_composition_determinism.py` catch the formula class.

## Versioning

This document and the canonical serializer are at **v1**.

- Serializer version: `SERIALIZER_VERSION = "composition-serializer-v1"`,
  recorded in the wrapper of every `canonical_serialize` output. Bumping
  this version invalidates every previously-published composition
  `output_hash`.
- Formula version: `formula_version = "composition-v1.0.0"`, recorded
  in the result body of every composition output.

Bumping rules:

- Any change to the formula bumps `formula_version`.
- Any change to precision, the wrapper, the coercion rules, or any
  other rule under "Serialization" above bumps `SERIALIZER_VERSION`.
- Both bumps require a constitution amendment (V9.13 §N), recorded in
  the V9.13 amendment changelog.
