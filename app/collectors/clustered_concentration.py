"""
Graph-Clustered Holder Concentration Collector (Pipeline 14)
===============================================================
Computes true holder concentration by merging wallets that share
common graph counterparties into single economic entities using
union-find on the relationship graph.

Nominal holder concentration is reconstructable. Graph-clustered
concentration requires edges that existed at a specific point in
time — those edges are pruned on a 180-day rolling basis.

Runs daily in the slow cycle.  Never raises — all errors logged and skipped.
"""

import hashlib
import json
import logging
import time
from datetime import date, datetime, timezone

from app.database import fetch_all, fetch_one, execute

logger = logging.getLogger(__name__)

# Minimum edge weight to merge two wallets into a cluster.
# Without this, every wallet that ever sent dust to an exchange
# gets merged into the exchange cluster, destroying the signal.
MIN_EDGE_WEIGHT = 0.1

# Max edges to load per stablecoin to avoid memory issues
MAX_EDGES = 50000

# Max holders to analyze
MAX_HOLDERS = 500


# ---------------------------------------------------------------------------
# Union-Find with path compression
# ---------------------------------------------------------------------------

class UnionFind:
    def __init__(self):
        self.parent = {}
        self.rank = {}

    def find(self, x):
        if x not in self.parent:
            self.parent[x] = x
            self.rank[x] = 0
        if self.parent[x] != x:
            self.parent[x] = self.find(self.parent[x])  # path compression
        return self.parent[x]

    def union(self, x, y):
        rx, ry = self.find(x), self.find(y)
        if rx == ry:
            return
        # union by rank
        if self.rank[rx] < self.rank[ry]:
            self.parent[rx] = ry
        elif self.rank[rx] > self.rank[ry]:
            self.parent[ry] = rx
        else:
            self.parent[ry] = rx
            self.rank[rx] += 1

    def groups(self, keys):
        """Return dict mapping root -> list of members."""
        clusters = {}
        for k in keys:
            root = self.find(k)
            clusters.setdefault(root, []).append(k)
        return clusters


# ---------------------------------------------------------------------------
# Gini coefficient
# ---------------------------------------------------------------------------

def _compute_gini(values: list[float]) -> float:
    """Compute Gini coefficient. Returns 0-1 float."""
    if not values or len(values) < 2:
        return 0.0
    vals = sorted(values)
    n = len(vals)
    total = sum(vals)
    if total <= 0:
        return 0.0
    numerator = sum((2 * i - n - 1) * x for i, x in enumerate(vals, 1))
    return numerator / (n * total)


def _top_n_pct(values: list[float], n: int) -> float:
    """Return percentage of total held by top N entries."""
    if not values:
        return 0.0
    sorted_desc = sorted(values, reverse=True)
    total = sum(sorted_desc)
    if total <= 0:
        return 0.0
    top = sum(sorted_desc[:n])
    return round(top / total * 100, 4)


# ---------------------------------------------------------------------------
# Core clustering logic
# ---------------------------------------------------------------------------

def _compute_clusters(stablecoin_symbol: str, stablecoin_id: int) -> tuple[list[dict], dict]:
    """
    Run the full clustering pipeline for one stablecoin.
    Returns (clusters_list, metrics_dict).
    """
    symbol_upper = stablecoin_symbol.upper()

    # Step 1: Load top holders
    holders = fetch_all(
        """SELECT DISTINCT ON (wh.wallet_address)
                  wh.wallet_address, wh.value_usd,
                  COALESCE(ac.actor_type, 'unknown') AS actor_type
           FROM wallet_graph.wallet_holdings wh
           LEFT JOIN wallet_graph.actor_classifications ac
                  ON wh.wallet_address = ac.wallet_address
           WHERE UPPER(wh.symbol) = %s
             AND wh.value_usd > 0
           ORDER BY wh.wallet_address, wh.indexed_at DESC
           LIMIT %s""",
        (symbol_upper, MAX_HOLDERS),
    )

    if not holders or len(holders) < 10:
        logger.warning(
            f"Clustered concentration: {symbol_upper} has {len(holders or [])} holders, "
            f"skipping (need >= 10)"
        )
        return [], {}

    holder_addrs = [h["wallet_address"] for h in holders]
    balance_map = {h["wallet_address"]: float(h["value_usd"]) for h in holders}
    actor_map = {h["wallet_address"]: h.get("actor_type", "unknown") for h in holders}

    # Step 2: Load relevant graph edges (respect 180-day pruning)
    edges = fetch_all(
        """SELECT from_address, to_address, weight
           FROM wallet_graph.wallet_edges
           WHERE (from_address = ANY(%s) OR to_address = ANY(%s))
             AND last_transfer_at > NOW() - INTERVAL '180 days'
             AND weight >= %s
           LIMIT %s""",
        (holder_addrs, holder_addrs, MIN_EDGE_WEIGHT, MAX_EDGES),
    )

    # Step 3: Build union-find structure
    uf = UnionFind()
    holder_set = set(holder_addrs)
    for e in (edges or []):
        from_addr = e["from_address"]
        to_addr = e["to_address"]
        # Only merge if BOTH endpoints are in the holder set
        if from_addr in holder_set and to_addr in holder_set:
            uf.union(from_addr, to_addr)

    # Step 4: Build clusters
    groups = uf.groups(holder_addrs)
    total_supply = sum(balance_map.values())

    raw_clusters = []
    for root, members in groups.items():
        cluster_balance = sum(balance_map.get(m, 0) for m in members)
        seed_wallet = max(members, key=lambda m: balance_map.get(m, 0))

        # Determine cluster type from actor classifications
        actor_types = [actor_map.get(m, "unknown") for m in members]
        actor_labels_set = list(set(actor_types))

        if "exchange" in actor_types:
            cluster_type = "exchange"
        elif "contract_vault" in actor_types:
            cluster_type = "contract"
        elif all(a == "unknown" for a in actor_types):
            cluster_type = "whale" if cluster_balance > 1_000_000 else "unknown"
        else:
            cluster_type = "mixed" if len(set(actor_types)) > 1 else actor_types[0]

        raw_clusters.append({
            "seed_wallet": seed_wallet,
            "member_wallets": sorted(members),
            "member_count": len(members),
            "total_balance_usd": round(cluster_balance, 2),
            "pct_of_supply": round(cluster_balance / total_supply * 100, 4) if total_supply > 0 else 0,
            "cluster_type": cluster_type,
            "actor_labels": actor_labels_set,
        })

    # Sort by balance descending and assign cluster_id
    raw_clusters.sort(key=lambda c: c["total_balance_usd"], reverse=True)
    for i, c in enumerate(raw_clusters):
        c["cluster_id"] = i + 1

    # Step 5: Compute concentration metrics
    nominal_balances = sorted([balance_map[a] for a in holder_addrs if balance_map.get(a, 0) > 0])
    clustered_balances = sorted([c["total_balance_usd"] for c in raw_clusters if c["total_balance_usd"] > 0])

    nominal_gini = _compute_gini(nominal_balances)
    clustered_gini = _compute_gini(clustered_balances)

    singleton_count = sum(1 for c in raw_clusters if c["member_count"] == 1)

    exchange_supply = sum(c["total_balance_usd"] for c in raw_clusters if c["cluster_type"] == "exchange")
    whale_supply = sum(c["total_balance_usd"] for c in raw_clusters if c["cluster_type"] == "whale")

    metrics = {
        "nominal_gini": round(nominal_gini, 6),
        "clustered_gini": round(clustered_gini, 6),
        "nominal_top10_pct": _top_n_pct(nominal_balances, 10),
        "clustered_top10_pct": _top_n_pct(clustered_balances, 10),
        "nominal_top50_pct": _top_n_pct(nominal_balances, 50),
        "clustered_top50_pct": _top_n_pct(clustered_balances, 50),
        "cluster_count": len(raw_clusters),
        "singleton_count": singleton_count,
        "exchange_cluster_pct": round(exchange_supply / total_supply * 100, 4) if total_supply > 0 else 0,
        "whale_cluster_pct": round(whale_supply / total_supply * 100, 4) if total_supply > 0 else 0,
        "divergence_score": round(abs(clustered_gini - nominal_gini), 4),
        "total_wallets_analyzed": len(holder_addrs),
        "total_supply_tracked_usd": round(total_supply, 2),
    }

    return raw_clusters, metrics


# ---------------------------------------------------------------------------
# Main collector
# ---------------------------------------------------------------------------

async def collect_clustered_concentration() -> dict:
    """
    Compute graph-clustered holder concentration for all scored stablecoins.
    Returns summary dict.
    """
    results = {
        "stablecoins_analyzed": 0,
        "clusters_computed": 0,
        "snapshots_stored": 0,
        "errors": [],
    }

    today = date.today()

    # Load scored stablecoins
    stablecoins = fetch_all(
        "SELECT id, symbol FROM stablecoins WHERE scoring_enabled = TRUE"
    )
    if not stablecoins:
        # Fallback: try without scoring_enabled filter
        stablecoins = fetch_all("SELECT id, symbol FROM stablecoins")

    if not stablecoins:
        logger.info("Clustered concentration: no stablecoins found")
        return results

    for coin in stablecoins:
        symbol = coin["symbol"]
        coin_id = coin["id"]
        t0 = time.time()

        try:
            # Check if today's snapshot already exists
            existing = fetch_one(
                """SELECT id FROM concentration_snapshots
                   WHERE stablecoin_symbol = %s AND snapshot_date = %s""",
                (symbol.upper(), today),
            )
            if existing:
                continue

            clusters, metrics = _compute_clusters(symbol, coin_id)
            if not clusters or not metrics:
                continue

            # Store clusters
            for cluster in clusters:
                execute(
                    """INSERT INTO holder_clusters
                        (stablecoin_symbol, stablecoin_id, snapshot_date, cluster_id,
                         seed_wallet, member_wallets, member_count,
                         total_balance_usd, pct_of_supply, cluster_type, actor_labels)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                       ON CONFLICT (stablecoin_symbol, snapshot_date, cluster_id) DO NOTHING""",
                    (
                        symbol.upper(), coin_id, today, cluster["cluster_id"],
                        cluster["seed_wallet"],
                        json.dumps(cluster["member_wallets"]),
                        cluster["member_count"],
                        cluster["total_balance_usd"],
                        cluster["pct_of_supply"],
                        cluster["cluster_type"],
                        json.dumps(cluster["actor_labels"]),
                    ),
                )
            results["clusters_computed"] += len(clusters)

            # Store concentration snapshot
            content_data = (
                f"{symbol.upper()}{today.isoformat()}"
                f"{metrics['clustered_gini']}{metrics['cluster_count']}"
            )
            content_hash = "0x" + hashlib.sha256(content_data.encode()).hexdigest()

            execute(
                """INSERT INTO concentration_snapshots
                    (stablecoin_symbol, stablecoin_id, snapshot_date,
                     nominal_gini, clustered_gini,
                     nominal_top10_pct, clustered_top10_pct,
                     nominal_top50_pct, clustered_top50_pct,
                     cluster_count, singleton_count,
                     exchange_cluster_pct, whale_cluster_pct,
                     divergence_score, total_wallets_analyzed,
                     total_supply_tracked_usd, content_hash, attested_at)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                   ON CONFLICT (stablecoin_symbol, snapshot_date) DO NOTHING""",
                (
                    symbol.upper(), coin_id, today,
                    metrics["nominal_gini"], metrics["clustered_gini"],
                    metrics["nominal_top10_pct"], metrics["clustered_top10_pct"],
                    metrics["nominal_top50_pct"], metrics["clustered_top50_pct"],
                    metrics["cluster_count"], metrics["singleton_count"],
                    metrics["exchange_cluster_pct"], metrics["whale_cluster_pct"],
                    metrics["divergence_score"], metrics["total_wallets_analyzed"],
                    metrics["total_supply_tracked_usd"], content_hash,
                ),
            )
            results["snapshots_stored"] += 1

            # Attest
            try:
                from app.state_attestation import attest_state
                attest_state("clustered_concentration", [{
                    "stablecoin": symbol.upper(),
                    "snapshot_date": today.isoformat(),
                    "clustered_gini": metrics["clustered_gini"],
                    "divergence_score": metrics["divergence_score"],
                }], str(coin_id))
            except Exception:
                pass

            elapsed = time.time() - t0
            logger.info(
                f"Clustered concentration: {symbol.upper()} "
                f"gini={metrics['clustered_gini']:.4f} "
                f"divergence={metrics['divergence_score']:.4f} "
                f"clusters={metrics['cluster_count']} "
                f"({elapsed:.1f}s)"
            )

            if metrics["divergence_score"] > 0.1:
                logger.warning(
                    f"HIGH CONCENTRATION DIVERGENCE: {symbol.upper()} "
                    f"divergence={metrics['divergence_score']:.4f} — "
                    f"graph merging significantly changes concentration picture"
                )

            results["stablecoins_analyzed"] += 1

        except Exception as e:
            results["errors"].append(f"{symbol}: {e}")
            logger.error(f"Clustered concentration failed for {symbol}: {e}")

    logger.info(
        f"Clustered concentration: analyzed={results['stablecoins_analyzed']} "
        f"clusters={results['clusters_computed']} "
        f"snapshots={results['snapshots_stored']} "
        f"errors={len(results['errors'])}"
    )
    return results
