# `edges` — design questions (2026-05-12)

Holdout from the v9.12 P2 main.py-legacy bundle. The standard hoist
doesn't fix the substrate problem this audit surfaces.

See `docs/audits/2026-05-12-p2-main-py-legacy-investigation.md` for
the bundle-scope investigation.

## Substrate

```sql
SELECT COUNT(*) FROM state_attestations WHERE domain = 'edges';
-- 0
```

Paired underlying table:

```sql
SELECT COUNT(*) AS total,
       COUNT(*) FILTER (WHERE updated_at > NOW() - INTERVAL '7 days') AS updated_7d,
       MAX(updated_at) AS last_updated
FROM wallet_graph.wallet_edges;
-- total=1,011,156, updated_7d=348,852, last_updated=2026-05-12T09:23:25.863Z
```

1M+ edges total, 350K updated in the last 7 days — but **zero
`state_attestations` rows for domain `edges` ever**.

## The dual-writer pair (both broken in the same way)

`main.py:476-486`:

```python
edge_result = asyncio.run(run_edge_builder(...))
# Attest edges for this chain
try:
    from app.state_attestation import attest_state
    if edge_result.get('total_edges_created', 0) > 0:
        attest_state("edges", [{"chain": edge_chain,
                                "wallets": ..., "edges": ...}])
except Exception as ae:
    logger.debug(f"Edge attestation skipped for {edge_chain}: {ae}")
```

`app/indexer/edges.py:447-464`:

```python
try:
    from app.state_attestation import attest_state
    if total_edges > 0:
        await asyncio.to_thread(attest_state, "edges",
            [{"chain": chain, "wallets": wallets_processed, "edges": total_edges}],
            chain)
except asyncio.CancelledError:
    raise
except Exception as ae:
    logger.warning(f"Edge attestation skipped for {chain}: {ae}")
    ...
```

Identical gate (`> 0`), identical domain key, identical payload shape.

## Why both writers miss

`total_edges` accumulates `result["edges_upserted"]` per wallet
(`edges.py:419`). The "upserted" count includes both inserts and
updates — there's no separate `inserted` counter. In steady state
with 1M edges already present, almost every per-wallet upsert hits the
`ON CONFLICT DO UPDATE` branch at `edges.py:291-298`.

Specifically:

```sql
INSERT INTO wallet_graph.wallet_edges (...)
VALUES (...)
ON CONFLICT (from_address, to_address, chain, edge_type) DO UPDATE SET
    transfer_count = wallet_graph.wallet_edges.transfer_count + EXCLUDED.transfer_count,
    ...
```

Every conflict counts as `+1` on `edges_upserted` but the row already
existed. So `total_edges_created` is misleading; nothing was actually
*created*. The `> 0` gate therefore opens only when at least one wallet
in the batch has a transfer to a counterparty never seen before in the
edge graph — which is rare given coverage saturation.

## Secondary issue: entity_id leak

`edges.py:451` calls `attest_state("edges", [...], chain)` — the third
positional arg is `entity_id` (`app/state_attestation.py:61`).

When `entity_id` is set, the consumer query at `state_attestation.py:84`
filters `WHERE domain = %s AND entity_id IS NULL` — meaning chain-scoped
attests don't show up in the default `get_latest_attestation` lookup.
This compounds the gate problem: even if the gate did open, the
attest row would be stamped with `entity_id = 'ethereum'` (or `'base'`
etc.) and consumers calling `get_latest_attestation('edges')` would
miss it.

`main.py:482` does NOT pass `chain` as `entity_id`, so the caller-side
attest writes `entity_id = NULL` — but the caller-side gate is the
same `> 0` so it never fires either.

## Why hoisting doesn't help

Removing `main.py:482` deletes one of two DUAL_WRITERs, but the
remaining `edges.py:451` writer:
1. Has the same `> 0` gate that doesn't open in steady state.
2. Writes with `entity_id = chain`, which fails the consumer's
   `entity_id IS NULL` lookup.

Coherence sweep with 12h cadence (`coherence.py:72`) will continue to
show `edges` as stale or missing.

## Design questions

**Q1.** Should `run_edge_builder` attest **always at end-of-run**,
regardless of edges-created count? Payload like:

```python
{
    "chain": chain,
    "wallets_processed": N,
    "edges_upserted": E,  # current "total_edges"
    "transfers_processed": T,
    "status": "ran" | "skipped_fresh" | "error",
}
```

Matches #198 / #193 wrapper shape. Pro: substrate cadence becomes
measurable. Con: floods `state_attestations` with no-op rows (~6
chains x ~hourly).

**Q2.** Or should the gate switch from `total_edges > 0` to
`wallets_processed > 0`? That captures "we tried" rather than
"something new emerged". Pro: smallest diff. Con: still misses
the genuinely-skipped case.

**Q3.** Should the `entity_id = chain` parameter at `edges.py:451`
move into the payload instead, so the row stays `entity_id = NULL`
and consumers can find it? Or should consumers be updated to also
look up by `entity_id = chain`?

**Q4.** The coherence cadence (12h) is tighter than the main.py gate
(10h between `run_edge_builder` invocations). Should these be
coupled? Currently they're independent.

**Q5.** Should we add an actual `inserted` count by checking pg's
`xact_commit_lsn` or by doing `INSERT ... RETURNING (xmax = 0)` to
distinguish new vs updated edges? That would let the `> 0` gate
have its original intended meaning.

## Recommendation

Q1 + Q3: build `run_edge_builder_scheduled(chain)` wrapper that
attests always with status payload, AND fix the `entity_id` plumbing
so consumers can find the rows. Q5 is nice-to-have but expensive;
the always-attest pattern from Q1 makes Q5 optional.

Estimated diff: ~50-80 lines (wrapper + entity_id refactor). Substrate
gate verification: post-deploy, the consumer query

```sql
SELECT COUNT(*) FROM state_attestations
WHERE domain = 'edges' AND cycle_timestamp > NOW() - INTERVAL '12 hours';
```

should return > 0 within 12 hours of next edge build cycle.

## Halt condition for the eventual refactor PR

If `edges` rows still don't appear within 24h of deploy, halt — the
wrapper is not on the live path (lesson 6 family) or the `entity_id`
shape is still mismatched.

## References

- `app/coherence.py:33,72` (consumer)
- `app/report.py:327` (consumer — wallet-report state hashes)
- `app/pulse_generator.py:250` (consumer — state-root list)
- `app/integrity.py:409` (consumer — edges integrity check)
- `app/state_attestation.py:61,84` (the `entity_id` plumbing)
- `app/indexer/edges.py:291-298` (the ON CONFLICT path that masks
  insert vs update)
- `docs/audits/2026-05-12-p2-main-py-legacy-investigation.md` (parent)
