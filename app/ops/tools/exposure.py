"""
Exposure report generator — direct DB queries joining wallet holdings with SII scores.
"""
import json
import logging
from datetime import datetime
from app.database import fetch_one, fetch_all, execute

logger = logging.getLogger(__name__)


def generate_exposure(target_id: int) -> dict:
    """
    Generate an exposure report for a target based on their known wallet addresses.
    Queries wallet_holdings + scores tables directly.
    Returns the report dict or None.
    """
    target = fetch_one("SELECT * FROM ops_targets WHERE id = %s", (target_id,))
    if not target:
        return None

    wallet_addresses = target.get("wallet_addresses") or []
    if not wallet_addresses:
        return {"error": "No wallet addresses configured for this target"}

    # Query holdings for these wallets
    holdings = []
    for addr in wallet_addresses:
        rows = fetch_all(
            """SELECT wh.token_symbol, wh.balance_usd, wh.balance_raw,
                      wrs.risk_score, wrs.risk_grade
               FROM wallet_graph.wallet_holdings wh
               LEFT JOIN wallet_graph.wallet_risk_scores wrs
                   ON wh.wallet_address = wrs.wallet_address AND wh.chain = wrs.chain
               WHERE LOWER(wh.wallet_address) = LOWER(%s)
               ORDER BY wh.balance_usd DESC NULLS LAST""",
            (addr,),
        )
        if rows:
            holdings.extend([{**dict(r), "wallet": addr} for r in rows])

    # Get current SII scores for stablecoins found in holdings
    stablecoin_symbols = list(set(
        h["token_symbol"] for h in holdings
        if h.get("token_symbol")
    ))

    sii_scores = {}
    if stablecoin_symbols:
        for sym in stablecoin_symbols:
            score = fetch_one(
                "SELECT symbol, overall_score, grade FROM scores WHERE UPPER(symbol) = UPPER(%s)",
                (sym,),
            )
            if score:
                sii_scores[score["symbol"].upper()] = {
                    "score": float(score["overall_score"]) if score["overall_score"] else None,
                }

    # Build report data
    total_usd = sum(float(h.get("balance_usd") or 0) for h in holdings)
    stablecoin_holdings = []
    for h in holdings:
        sym = (h.get("token_symbol") or "").upper()
        sii = sii_scores.get(sym, {})
        stablecoin_holdings.append({
            "symbol": sym,
            "wallet": h["wallet"],
            "balance_usd": float(h.get("balance_usd") or 0),
            "sii_score": sii.get("score"),
            "sii_grade": sii.get("grade"),
        })

    # Calculate weighted SII
    weighted_sum = 0
    weighted_total = 0
    for sh in stablecoin_holdings:
        if sh["sii_score"] is not None and sh["balance_usd"] > 0:
            weighted_sum += sh["sii_score"] * sh["balance_usd"]
            weighted_total += sh["balance_usd"]

    weighted_sii = round(weighted_sum / weighted_total, 1) if weighted_total > 0 else None

    report_data = {
        "target": target["name"],
        "wallet_addresses": wallet_addresses,
        "total_usd": total_usd,
        "stablecoin_count": len(set(sh["symbol"] for sh in stablecoin_holdings if sh["sii_score"] is not None)),
        "weighted_sii": weighted_sii,
        "holdings": stablecoin_holdings,
        "generated_at": datetime.utcnow().isoformat(),
    }

    # Generate markdown report
    md_lines = [
        f"# Exposure Report: {target['name']}",
        f"Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}",
        "",
        f"**Wallets analyzed:** {len(wallet_addresses)}",
        f"**Total stablecoin value:** ${total_usd:,.0f}",
        f"**Weighted SII score:** {weighted_sii or 'N/A'}",
        "",
        "| Symbol | Balance (USD) | SII Score | Grade |",
        "|--------|--------------|-----------|-------|",
    ]

    for sh in sorted(stablecoin_holdings, key=lambda x: x["balance_usd"], reverse=True):
        if sh["balance_usd"] > 0:
            md_lines.append(
                f"| {sh['symbol']} | ${sh['balance_usd']:,.0f} | "
                f"{sh['sii_score'] or 'N/A'} | {sh['sii_grade'] or 'N/A'} |"
            )

    report_markdown = "\n".join(md_lines)

    # Store the report
    execute(
        """INSERT INTO ops_target_exposure_reports (target_id, wallet_addresses, report_data, report_markdown)
           VALUES (%s, %s, %s, %s)""",
        (target_id, wallet_addresses, json.dumps(report_data), report_markdown),
    )

    return {"data": report_data, "markdown": report_markdown}
