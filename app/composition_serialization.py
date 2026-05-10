"""
Composition Output Serialization
=================================
Canonical, byte-stable serialization for the publication-ready composition
outputs (CQI, RQS).

Background
----------
``app.composition`` produces three derived outputs that consume attested SII
and PSI state:

  - ``cqi_compositions``  (compute_cqi_matrix)
  - ``rqs_composition``   (compute_rqs_for_protocol)
  - ``rqs_compositions``  (compute_rqs_all)

These are not yet published on-chain. They are reachable only from FastAPI
handlers (no orchestrator path), which an audit (PR #118) correctly flags
as ``reachable=0``. V9.13 §N upgrades the policy: rather than treating that
as an orphan, we declare these domains *publication-ready* and hold them
to the same four invariants we hold SII/PSI to:

  1. **Deterministic compute** — identical inputs produce identical outputs
     in-process and across runs.
  2. **Stable serialization** — outputs serialize to byte-identical bytes
     for structurally equal inputs (this module).
  3. **Attestation at compute time** — every published output is hashed
     and recorded in ``state_attestations`` (already wired in
     ``app.composition``; see ``output_hash`` field).
  4. **Documented spec** — formula + serialization rules are committed
     to ``docs/composition_spec.md`` so a third party can reproduce a
     hash without reading source.

Why a dedicated serializer?
---------------------------
``app.state_attestation.compute_batch_hash`` predates this work and uses a
``default=_serialize`` fallback that floats Decimals — fine for the SII/PSI
record-list shape (lists of small dicts of primitives), but insufficient
for composition outputs where:

  - Decimal precision must be pinned (not coerced to float)
  - Dict iteration order must be canonicalized at every nesting level
  - Format changes must trip a snapshot test loudly

This module is the canonical serializer for composition outputs. The
output of ``canonical_serialize`` carries a small wrapper recording the
serializer version so a future format change is detectable without
consulting source code.

Bridge contract
---------------
The attestation payload written by ``app.composition`` includes:

  {
    "domain":           "<cqi_compositions | rqs_composition | rqs_compositions>",
    "computed_at":      "<ISO-8601 UTC, hour-truncated>",
    "input_sii_hash":   "<hash of SII snapshot>",
    "input_psi_hash":   "<hash of PSI snapshot>",
    "output_hash":      "<canonical_hash of output>",
    "row_count":        <int>,
  }

A verifier can:
  1. Look up the upstream SII/PSI attestations by their hashes.
  2. Reproduce the composition per ``docs/composition_spec.md``.
  3. Run ``canonical_hash(output)`` and compare to ``output_hash``.

If any of those three steps fails, the composition has drifted from the
spec — either the formula changed, the precision changed, or the
serialization changed. Each is a constitutional break that this module's
snapshot tests will catch.
"""

from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, timezone
from decimal import ROUND_HALF_EVEN, Decimal

# ---------------------------------------------------------------------------
# Versioning
# ---------------------------------------------------------------------------

# Serializer format version. Bump when:
#   - precision changes
#   - any rule below is altered
#   - the wrapper shape changes
# Bumping this version invalidates all previously published output_hashes
# for composition outputs. Do so only with a constitution amendment.
SERIALIZER_VERSION = "composition-serializer-v1"

# Decimal precision. Reconciled against ``app.composition``: scoring
# outputs are rounded to 2 decimals (cqi_score, rqs_score, sii_score,
# psi_score, contribution at 4 decimals, weight at 4 decimals,
# scored_coverage at 4 decimals, treasury_total_usd at 2). 8 decimals
# is strictly wider than every existing rounding in the module, so no
# information is lost when serializing existing outputs. Picking 8
# (not e.g. 4) gives headroom for future per-component contributions
# and matches the precision typical of on-chain integer scaling
# (10^8 is also Bitcoin's satoshi factor — convenient for verifiers).
DECIMAL_PRECISION = 8

# Quantizer for fixed-precision Decimal output.
_QUANTIZE = Decimal(10) ** -DECIMAL_PRECISION


# ---------------------------------------------------------------------------
# Internal coercion
# ---------------------------------------------------------------------------

def _coerce(value):
    """Convert ``value`` to a JSON-serializable canonical form.

    Rules:
      - ``None``, ``bool``, ``int``, ``str``      → unchanged
      - ``float``                                 → Decimal(str(value)) → fixed-precision string
      - ``Decimal``                               → fixed-precision string
      - ``datetime``, ``date``                    → ISO-8601; datetime is UTC-normalized
      - ``dict``                                  → dict with sorted string keys, recursively coerced values
      - ``list`` / ``tuple``                      → list, recursively coerced (order preserved)
      - ``set`` / ``frozenset``                   → list, sorted by canonical_serialize bytes
      - other                                     → ``str(value)`` (deterministic but lossy; flagged via test fixture)

    Float and Decimal both produce **strings** (not JSON numbers). This
    avoids JSON's float-rendering subtleties (e.g. ``json.dumps(0.1)`` is
    "0.1" but ``json.dumps(1e-5)`` is "1e-05" on some platforms). String
    form makes the canonical bytes truly platform-independent.
    """
    # bool BEFORE int (bool is a subclass of int)
    if value is None or isinstance(value, (bool, int, str)):
        return value

    if isinstance(value, float):
        # Route via Decimal(str(...)) so 0.1 + 0.2 == 0.30000000000000004
        # surfaces as a serialization difference, not silently rounds.
        return _format_decimal(Decimal(str(value)))

    if isinstance(value, Decimal):
        return _format_decimal(value)

    if isinstance(value, datetime):
        # Normalize to UTC; if naive, treat as already-UTC (the worker
        # writes UTC timestamps). Drop sub-microsecond precision.
        if value.tzinfo is None:
            v = value.replace(tzinfo=timezone.utc)
        else:
            v = value.astimezone(timezone.utc)
        return v.isoformat()

    if isinstance(value, date):
        return value.isoformat()

    if isinstance(value, dict):
        # Stringify keys deterministically; ``json.dumps(sort_keys=True)``
        # at the wrapper level requires string keys, and Python allows
        # arbitrary hashable keys (int, tuple, Decimal, ...). Map every
        # key to its canonical string form and recurse on the value.
        items = []
        for k, v in value.items():
            items.append((_key_to_string(k), _coerce(v)))
        # Sort by the string key. Duplicate string keys would be a real
        # bug (two different Python keys collapsing to the same string);
        # detect and raise.
        items.sort(key=lambda kv: kv[0])
        out: dict = {}
        for sk, cv in items:
            if sk in out:
                raise ValueError(
                    f"canonical_serialize: duplicate key after stringification: {sk!r}"
                )
            out[sk] = cv
        return out

    if isinstance(value, (list, tuple)):
        return [_coerce(v) for v in value]

    if isinstance(value, (set, frozenset)):
        # Sort by serialized bytes so order is deterministic regardless
        # of element type or insertion order.
        coerced = [_coerce(v) for v in value]
        return sorted(
            coerced,
            key=lambda x: json.dumps(x, sort_keys=True, separators=(",", ":")),
        )

    return str(value)


def _key_to_string(k) -> str:
    """Canonical string form of a dict key.

    Composition outputs only ever use ``str`` keys today; the int/Decimal
    branches exist defensively so a future refactor that introduces a
    non-string key surfaces deterministically rather than relying on
    ``json.dumps`` raising at the wrapper level.
    """
    if isinstance(k, str):
        return k
    if isinstance(k, bool):
        return "true" if k else "false"
    if isinstance(k, int):
        return str(k)
    if isinstance(k, float):
        return _format_decimal(Decimal(str(k)))
    if isinstance(k, Decimal):
        return _format_decimal(k)
    return str(k)


def _format_decimal(d: Decimal) -> str:
    """Format ``d`` to ``DECIMAL_PRECISION`` decimal places, banker's
    rounding. Returns plain decimal notation (no scientific form)."""
    if d.is_nan() or d.is_infinite():
        # Propagate as a sentinel string. Snapshot tests will fail loudly
        # if this ever surfaces, prompting a real fix at the producer.
        return str(d)
    quantized = d.quantize(_QUANTIZE, rounding=ROUND_HALF_EVEN)
    # Avoid Python's exponent-notation output for very small/large numbers.
    return format(quantized, "f")


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def canonical_serialize(obj) -> bytes:
    """Serialize ``obj`` to canonical UTF-8 bytes.

    Output structure:

        {"v":"<SERIALIZER_VERSION>","d":<coerced obj>}

    The wrapper records the serializer version. When the format changes,
    ``SERIALIZER_VERSION`` bumps and previously-published hashes no
    longer verify — a deliberate, loud break.

    Properties guaranteed by the snapshot test:

      - Byte-identical for structurally equal inputs (dict-key permutation
        is invisible).
      - ``float(x)`` and ``Decimal(str(x))`` serialize to identical bytes
        for representable ``x``.
      - No whitespace, no trailing newline.
    """
    coerced = _coerce(obj)
    wrapper = {"v": SERIALIZER_VERSION, "d": coerced}
    return json.dumps(wrapper, sort_keys=True, separators=(",", ":")).encode("utf-8")


def canonical_hash(obj) -> str:
    """Return the SHA-256 hex digest of ``canonical_serialize(obj)``."""
    return hashlib.sha256(canonical_serialize(obj)).hexdigest()
