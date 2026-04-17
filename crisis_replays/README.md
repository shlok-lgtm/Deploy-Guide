# Crisis Replay Library

Reference replays of historical financial crises through every applicable
Basis index. Each replay is a self-contained directory with:

  - `inputs.json`              — canonical input vector (the data we believe was
                                 available at the moment of the event)
  - `result.json`              — final scores + grades + computation hash
  - `replay.py`                — re-derivation harness; deterministic, no
                                 network calls
  - `README.md`                — context, sources, and what the score was
                                 trying to capture at the time

## What this library is (and is not)

**It is:** a deterministic consistency check. Given the `inputs.json`
stored for each crisis, the scoring engine produces the same score, grade,
and computation hash on every machine, every time. The hashes pinned in
`result.json` are reproducible byte-for-byte from the stored inputs.

**It is not:** a re-derivation from primary historical data. The component
values in each `inputs.json` are plausible synthetic approximations of
what the SII/PSI/RPI inputs looked like at the moment of the crisis. They
were written by `scripts/generate_crisis_replays.py`, not extracted from
archived chain snapshots, historical price feeds, or raw governance
records. A future upgrade to "Claim B" (full historical re-derivation
from primary sources) is out of scope for Bucket A.

## Re-derivation harness

Every replay can be re-derived from this repo with:

```bash
python -m crisis_replays.run <crisis_slug>
# or run every replay
python -m crisis_replays.run --all
```

The harness loads `inputs.json`, runs the canonical scoring engine for the
declared `index_kind` (sii/psi/rpi/cqi) at the methodology version pinned in
`result.json`, and verifies that:

    sha256(canonical(inputs.json))                    == input_vector_hash
    sha256(input_vector_hash || version || scores)    == computation_hash

The same hashes are persisted in the `crisis_replays` Postgres table and
surfaced at `/crisis-replays/{slug}` for public verification.

## Reference implementation

`run.py` in this directory is the reference implementation. It is intentionally
small (no I/O beyond reading the local JSON files and printing results) so a
third party can audit it end-to-end in a few minutes.

## Crises covered (15)

  - terra-luna       (May 2022)
  - ftx              (Nov 2022)
  - usdc-svb         (Mar 2023)
  - euler            (Mar 2023)
  - nomad            (Aug 2022)
  - ronin            (Mar 2022)
  - celsius          (Jul 2022)
  - iron-finance     (Jun 2021)
  - mango            (Oct 2022)
  - wormhole         (Feb 2022)
  - curve            (Jul 2023)
  - bzx              (Feb 2020)
  - harmony-horizon  (Jun 2022)
  - voyager          (Jul 2022)
  - basis-cash       (Jan 2021)
