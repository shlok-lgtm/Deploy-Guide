# Parameter History Numeric Overflow — Investigation

Date: 2026-05-14
Author: Claude (Opus 4.7)
Status: PATH B (systemic) — no fix PR, design call required
Scope: 1,564 `numeric field overflow` errors / 7d in `cycle_phase = parameter_history`

---

## TL;DR

The Aave V3 entry in `PROTOCOL_PARAMETER_REGISTRY`
(`app/collectors/parameter_history.py`) reads the wrong fields from
`getReserveData(address)`. Field indices 1, 2, 3, 11, 12 — used as proxies
for LTV / liquidation threshold / liquidation bonus / supply cap / borrow
cap — actually correspond to `liquidityIndex` (a Ray-scaled rate, ~10²⁷),
`currentLiquidityRate`, `variableBorrowIndex`, and various `address`-typed
fields cast to uint256 (~10⁴⁸). The values requested are not at flat
indices; they are **bit-packed inside `configuration` (field 0)** of the
`ReserveDataLegacy` struct.

This is not a `normalization_factor` mistake — it is a struct-decoding
mismatch. Five distinct Aave parameters across five assets are affected
(25 spec entries). Compound's `getAssetInfoByAddress` writer also appears
broken (`protocol_parameters` has zero compound-finance rows, despite the
spec being in the registry). Per halt rule "more than 5 distinct
parameters affected — propose Option B (systemic), surface for design
call," no fix PR is shipped.

The columns (`numeric(30, 8)` for value, `numeric(10, 4)` for
change_magnitude) are correctly sized for legitimately-normalized values.
Per operator: do not widen the columns.

---

## Substrate findings

### Q1 — Error pattern + frequency (verbatim)

```sql
SELECT cycle_phase, COUNT(*) AS occurrences_7d,
       LEFT(error_message, 250) AS pattern,
       MIN(occurred_at), MAX(occurred_at)
FROM cycle_errors
WHERE error_message ILIKE '%numeric%overflow%'
  AND occurred_at > NOW() - INTERVAL '7 days'
GROUP BY cycle_phase, LEFT(error_message, 250)
ORDER BY occurrences_7d DESC;
```

| cycle_phase | occurrences_7d | pattern | first | last |
|---|---|---|---|---|
| parameter_history | 1252 | `numeric field overflow / DETAIL: A field with precision 30, scale 8 must round to an absolute value less than 10^22.` | 2026-05-11T11:21:53Z | 2026-05-14T02:02:32Z |
| parameter_history | 312 | `numeric field overflow / DETAIL: A field with precision 10, scale 4 must round to an absolute value less than 10^6.` | 2026-05-11T11:26:53Z | 2026-05-14T02:02:27Z |

Total: 1,564 errors / 7d (matches the operator-reported figure of 1,516
within rounding for the timing of the query). Both classes started at
~11:21 UTC on 2026-05-11 — coincides with the `parameter_history`
collector going live.

### Q2 — Full error / traceback

The `cycle_errors` table does not have an `error_traceback` column;
attempted query failed. Error message body confirms the column-precision
limits:

```
numeric field overflow
DETAIL:  A field with precision 30, scale 8 must round to an absolute value less than 10^22.
```

```
numeric field overflow
DETAIL:  A field with precision 10, scale 4 must round to an absolute value less than 10^6.
```

### Q3 — Top current_value rows in `protocol_parameters`

```sql
SELECT protocol_slug, parameter_key, asset_symbol, value_unit,
       current_value, current_value_raw, last_updated_at
FROM protocol_parameters
ORDER BY current_value DESC NULLS LAST
LIMIT 40;
```

| protocol | parameter_key | asset | value_unit | current_value_raw | current_value (normalized) |
|---|---|---|---|---|---|
| aave | aave_dai_borrow_cap | DAI | token_units | `3219087706602324282882` | `3.219e21` |
| aave | aave_wbtc_liquidation_threshold | WBTC | percent | `59568040747111840071708` | `5.957e20` |
| aave | aave_weth_borrow_cap | WETH | token_units | `15799427751052757873` | `1.58e19` |
| aave | aave_usdc_borrow_cap | USDC | token_units | `21704313023` | `2.17e10` |
| aave | aave_usdt_borrow_cap | USDT | token_units | `19624847663` | `1.96e10` |
| aave | aave_wbtc_borrow_cap | WBTC | token_units | `3057648` | `3.06e6` |

Compound: `protocol_parameters` contains **zero** rows for
`compound-finance`. The Compound writer is silently failing (likely
selector or return-layout mismatch), distinct from the overflow class.

The DAI row (`3.219e21`) is the textbook overflow trigger — it lands
right between numeric(30,8)'s 10²² limit and bare token units. The WBTC
liquidation_threshold (`5.957e20`) is even more obviously wrong: a
"percent" can't be in 10²⁰.

Also revealing: `aave_*_borrow_cap` (all field_idx=12, same spec) returns
wildly different magnitudes per asset — DAI=10²¹, WETH=10¹⁹, USDC=10¹⁰,
WBTC=10⁶. A single field index across reserves yielding 15 orders of
magnitude of variance is conclusive evidence that **the field index does
not correspond to a single semantic concept** in Aave's return layout.

### Recent `protocol_parameter_changes` rows

Only `aave_wbtc_borrow_cap` is making it through to the changes table at
all (WBTC's value at 3.06M just happens to fit). Other parameters fail at
INSERT time due to overflow — which is why we see 1,252+312 errors but
the table is mostly empty.

---

## Code-read findings

### The registry (lines 178–195 of `app/collectors/parameter_history.py`)

```python
AAVE_PARAM_SPECS = [
    ("ltv", "LTV", 1, "percent", 100),
    ("liquidation_threshold", "Liquidation Threshold", 2, "percent", 100),
    ("liquidation_bonus", "Liquidation Bonus", 3, "percent", 100),
    ("supply_cap", "Supply Cap", 11, "token_units", 1),
    ("borrow_cap", "Borrow Cap", 12, "token_units", 1),
]
```

Each tuple is `(key, name, field_index_in_return, value_unit, normalization_factor)`.

### How it's applied (lines 593–599 and 149–155)

```python
raw_int = _decode_uint256(result, field_idx)
...
norm = spec["normalization_factor"]
normalized = raw_int / norm if norm else raw_int
```

```python
def _decode_uint256(hex_str: str, offset: int = 0) -> int:
    start = 2 + (offset * 64)  # skip 0x prefix
    end = start + 64
    return int(hex_str[start:end], 16)
```

The decoder treats `getReserveData`'s return data as a **flat array of
32-byte uint256 words at fixed offsets**. It then divides each word by a
constant `normalization_factor`.

### Why it can go wrong

Aave V3's actual `getReserveData` return type is `ReserveDataLegacy`
(or `ReserveData` on newer pools), a packed struct whose first field is
`ReserveConfigurationMap configuration` (a `uint256` storing LTV,
threshold, bonus, decimals, active/frozen flags, supply cap, borrow cap,
etc. **as bit-packed sub-fields**, per Aave docs):

| Word offset | Field | What it holds |
|---|---|---|
| 0 | `configuration.data` (uint256) | **bit-packed: LTV (0–15), liq threshold (16–31), liq bonus (32–47), decimals (48–55), active/frozen/borrowing/stable/paused/borrowable-in-isolation/siloed/flash-loanable flags, reserveFactor (64–79), borrow cap (80–115), supply cap (116–151), liquidation protocol fee, eMode category, unbacked mint cap, debt ceiling…** |
| 1 | `liquidityIndex` (uint128) | Ray-scaled, ~10²⁷ |
| 2 | `currentLiquidityRate` (uint128) | Ray-scaled APY, ~10²⁷ |
| 3 | `variableBorrowIndex` (uint128) | Ray-scaled, ~10²⁷ |
| 4 | `currentVariableBorrowRate` | Ray |
| 5 | `currentStableBorrowRate` | Ray |
| 6 | `lastUpdateTimestamp` + `id` (packed) | ~10⁹ unix |
| 7–12 | `aTokenAddress`, `stableDebtTokenAddress`, `variableDebtTokenAddress`, `interestRateStrategyAddress`, `accruedToTreasury`, `unbacked` | addresses cast to uint256 ≈ 10⁴⁸; counters in ray |

So:

- `field_idx=1 / 100` (claimed LTV) → `liquidityIndex / 100` ≈ 10²⁵
- `field_idx=2 / 100` (claimed liq threshold) → `currentLiquidityRate / 100` ≈ 10²⁵ (matches WBTC's 5.96×10²² after some interim collisions)
- `field_idx=3 / 100` (claimed liq bonus) → `variableBorrowIndex / 100` ≈ 10²⁵
- `field_idx=11 / 1` (claimed supply cap) → an address or `accruedToTreasury` (varies wildly per asset)
- `field_idx=12 / 1` (claimed borrow cap) → an address or `unbacked` counter (varies wildly per asset)

This matches the substrate exactly: a single registry "borrow cap" spec
producing 15 orders of magnitude of variation across assets because each
asset has different address bytes / different unbacked counters at that
slot.

Correct decoding requires reading **field 0** and then unpacking bit
ranges (e.g. `(config >> 0) & 0xFFFF` for LTV in basis points, `(config
>> 80) & ((1<<36)-1)` for borrow cap in whole tokens, etc.). Aave also
exposes helper views on PoolDataProvider (`getReserveConfigurationData`,
`getReserveCaps`) that return the fields already unpacked — using those
would be the cleaner fix.

### Why `change_magnitude` (numeric(10,4)) also overflows

Once the value column starts accepting bad data (e.g., WBTC borrow_cap
which legitimately fits), and a subsequent read returns a different bogus
slot (e.g., a different address byte), `abs(new_value - prev_value)`
can exceed 10⁶ even when both endpoints individually fit. That's the
source of the 312/7d secondary errors.

---

## Path classification

- Not Path A — five distinct parameters and a sixth (Compound) silently
  failing. Editing `normalization_factor` cannot fix struct-decode errors.
- **Path B** — registry's decode strategy (flat-uint256-at-index) is
  semantically wrong for Aave V3's return type. Needs replacement, not a
  one-line tweak.
- Not Path C — the values are demonstrably bogus (a "percent" of 10²⁰ is
  not a real on-chain quantity). Per operator, do not widen columns.

---

## Recommended next investigation step

Pick one of:

1. **Switch to Aave PoolDataProvider helper views.** Replace
   `getReserveData` with explicit `getReserveConfigurationData(address)`
   (returns `decimals, ltv, liquidationThreshold, liquidationBonus,
   reserveFactor, usageAsCollateralEnabled, borrowingEnabled,
   stableBorrowRateEnabled, isActive, isFrozen`) and
   `getReserveCaps(address)` (returns `borrowCap, supplyCap` in whole
   token units). One spec per (asset, parameter); each spec's
   `field_index` and `normalization_factor` then makes sense.
2. **Bit-unpack the `configuration` word.** Keep `getReserveData` but
   change the decoder so specs declare `(bit_offset, bit_width,
   normalization_factor)` against field 0. More work; matches Aave's
   on-chain layout exactly.

Either path requires:

- Rewriting `AAVE_PARAM_SPECS` and `_read_parameter_value` for Aave.
- Auditing `COMPOUND_PARAM_SPECS` similarly — the empty
  `protocol_parameters` rows for `compound-finance` suggest
  `getAssetInfoByAddress` is also mis-decoded; selectors/return layouts
  need verification on mainnet.
- Backfilling: existing `protocol_parameters` rows for `aave` are bogus
  and should be deleted before relaunch so the change-detector doesn't
  fire 1,564 "changes" the moment correct values land.
- Until then, consider a kill switch on the registry (empty
  `PROTOCOL_PARAMETER_REGISTRY["aave"]`) to silence the error stream
  without rewriting the decoder.

---

## Normalization summary

`normalization_factor` is applied as `normalized = raw_int / norm`
(line 598). It runs once on the uint256 already extracted from a chosen
struct offset. It can go wrong in four ways:

1. **Wrong offset** — current bug. `raw_int` is not what the spec claims.
2. **Wrong direction** — for percent-in-bps you want `/10000`, not `/100`.
3. **Wrong magnitude** — wei→eth needs `/1e18`, not `/1`.
4. **Width mismatch** — Aave packs liq fields as uint128 inside a uint256
   slot; reading the full 32-byte word will pull adjacent packed fields
   into the high bits and corrupt the value even when offset and factor
   are both "right" by intent.

For this incident, the failure is class 1 (wrong offset) plus class 4
(field 0 is bit-packed, not a single integer). Fixing the
`normalization_factor` constants will not help.
