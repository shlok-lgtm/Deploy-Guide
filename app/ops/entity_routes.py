"""
Entity View Routes — ops-only, auth-gated.

JSON API endpoints:
    GET /api/ops/entity/stablecoin/{symbol}
    GET /api/ops/entity/protocol/{slug}
    GET /api/ops/entity/wallet/{address}

HTML rendered pages:
    GET /api/ops/entity/stablecoin/{symbol}/page
    GET /api/ops/entity/protocol/{slug}/page
    GET /api/ops/entity/wallet/{address}/page
"""

import asyncio
import os
import json
import hmac
import logging
import traceback as _traceback_mod

from fastapi import APIRouter, Request, HTTPException
from fastapi.responses import JSONResponse, HTMLResponse

from app.database import fetch_one, fetch_all
from app.ops.entity_views import (
    get_stablecoin_entity,
    get_protocol_entity,
    get_wallet_entity,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/ops/entity", tags=["ops-entity"])


def _check_admin_key(request: Request):
    admin_key = os.environ.get("ADMIN_KEY", "")
    provided = (
        request.query_params.get("key", "")
        or request.headers.get("x-admin-key", "")
    )
    if not admin_key or not provided or not hmac.compare_digest(provided, admin_key):
        raise HTTPException(status_code=401, detail="Unauthorized")


# ---------------------------------------------------------------------------
# JSON API endpoints
# ---------------------------------------------------------------------------

@router.get("/stablecoin/{symbol}")
async def stablecoin_entity_json(symbol: str, request: Request):
    """Full entity view for a stablecoin — JSON."""
    _check_admin_key(request)
    try:
        data = await asyncio.to_thread(get_stablecoin_entity, symbol)
        if not data:
            raise HTTPException(status_code=404, detail=f"Stablecoin '{symbol}' not found")
        return data
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Stablecoin entity view error for {symbol}: {e}")
        return JSONResponse(status_code=500, content={
            "error": str(e), "traceback": _traceback_mod.format_exc()
        })


@router.get("/protocol/{slug}")
async def protocol_entity_json(slug: str, request: Request):
    """Full entity view for a protocol — JSON."""
    _check_admin_key(request)
    try:
        data = await asyncio.to_thread(get_protocol_entity, slug)
        if not data:
            raise HTTPException(status_code=404, detail=f"Protocol '{slug}' not found")
        return data
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Protocol entity view error for {slug}: {e}")
        return JSONResponse(status_code=500, content={
            "error": str(e), "traceback": _traceback_mod.format_exc()
        })


@router.get("/wallet/{address}")
async def wallet_entity_json(address: str, request: Request):
    """Full entity view for a wallet — JSON."""
    _check_admin_key(request)
    try:
        data = await asyncio.to_thread(get_wallet_entity, address)
        if not data:
            raise HTTPException(status_code=404, detail=f"Wallet '{address}' not found")
        return data
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Wallet entity view error for {address}: {e}")
        return JSONResponse(status_code=500, content={
            "error": str(e), "traceback": _traceback_mod.format_exc()
        })


# ---------------------------------------------------------------------------
# HTML rendered pages
# ---------------------------------------------------------------------------

@router.get("/stablecoin/{symbol}/page")
async def stablecoin_entity_page(symbol: str, request: Request):
    """Full entity view for a stablecoin — HTML."""
    _check_admin_key(request)
    try:
        data = await asyncio.to_thread(get_stablecoin_entity, symbol)
        if not data:
            raise HTTPException(status_code=404, detail=f"Stablecoin '{symbol}' not found")
        html = _render_stablecoin_page(data)
        return HTMLResponse(content=html)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Stablecoin entity page error for {symbol}: {e}")
        return HTMLResponse(content=f"<pre>Error: {e}\n{_traceback_mod.format_exc()}</pre>", status_code=500)


@router.get("/protocol/{slug}/page")
async def protocol_entity_page(slug: str, request: Request):
    """Full entity view for a protocol — HTML."""
    _check_admin_key(request)
    try:
        data = await asyncio.to_thread(get_protocol_entity, slug)
        if not data:
            raise HTTPException(status_code=404, detail=f"Protocol '{slug}' not found")
        html = _render_protocol_page(data)
        return HTMLResponse(content=html)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Protocol entity page error for {slug}: {e}")
        return HTMLResponse(content=f"<pre>Error: {e}\n{_traceback_mod.format_exc()}</pre>", status_code=500)


@router.get("/wallet/{address}/page")
async def wallet_entity_page(address: str, request: Request):
    """Full entity view for a wallet — HTML."""
    _check_admin_key(request)
    try:
        data = await asyncio.to_thread(get_wallet_entity, address)
        if not data:
            raise HTTPException(status_code=404, detail=f"Wallet '{address}' not found")
        html = _render_wallet_page(data)
        return HTMLResponse(content=html)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Wallet entity page error for {address}: {e}")
        return HTMLResponse(content=f"<pre>Error: {e}\n{_traceback_mod.format_exc()}</pre>", status_code=500)


# ---------------------------------------------------------------------------
# HTML rendering helpers
# ---------------------------------------------------------------------------

def _entity_page(title: str, body: str, form_id: str = "", stats: list = None) -> str:
    """Wrap body in full HTML page using the project design language."""
    from app.templates._html import page
    return page(title, body, description=f"Entity view: {title}",
                form_id=form_id, stats=stats)


def _section(title: str, content: str) -> str:
    from app.templates._html import section
    return section(title, content)


def _table(headers: list, rows: list, num_cols: list = None) -> str:
    from app.templates._html import table
    return table(headers, rows, num_cols)


def _fmt_score(val) -> str:
    if val is None:
        return "—"
    try:
        return f"{float(val):.1f}"
    except (TypeError, ValueError):
        return "—"


def _fmt_usd(val) -> str:
    from app.templates._html import fmt_usd
    return fmt_usd(val)


def _fmt_pct(val) -> str:
    if val is None:
        return "—"
    try:
        return f"{float(val):.1f}%"
    except (TypeError, ValueError):
        return "—"


def _fmt_ts(val) -> str:
    if not val:
        return "—"
    s = str(val)
    return s[:19].replace("T", " ") if len(s) > 10 else s


def _error_or_empty(data, key) -> bool:
    """Check if a section has an error or is empty."""
    section = data.get(key)
    if not section:
        return True
    if isinstance(section, dict) and section.get("error"):
        return True
    return False


def _section_error_msg(data, key) -> str:
    section = data.get(key)
    if isinstance(section, dict) and section.get("error"):
        return f'<p class="meta">Section unavailable: {section["error"]}</p>'
    return '<p class="meta">No data available.</p>'


# ---------------------------------------------------------------------------
# Stablecoin HTML
# ---------------------------------------------------------------------------

def _render_stablecoin_page(data: dict) -> str:
    sym = data.get("symbol", "?")
    name = data.get("name", sym)
    scores = data.get("scores", {})
    overall = scores.get("overall_score")

    body = ""

    # Header
    from app.templates._html import score_header
    body += score_header(
        f"{name} ({sym})",
        overall,
        subtitle=f"Issuer: {data.get('issuer') or '—'} · {data.get('token_contract') or '—'}",
    )

    # Scores — Category breakdown
    if not _error_or_empty(data, "scores"):
        cats = scores.get("categories") or {}
        struct = scores.get("structural_breakdown") or {}
        rows = []
        for cat, val in cats.items():
            rows.append([cat.replace("_", " ").title(), _fmt_score(val),
                         f'<span class="bar" style="width:{int(float(val or 0) * 1.5)}px"></span>'])
        body += _section("SII Category Breakdown",
                         _table(["Category", "Score", ""], rows, [1])
                         + f'<p class="meta" style="margin-top:8px">Formula: {scores.get("formula_version")} · '
                           f'Components: {scores.get("component_count")} · '
                           f'Freshness: {_fmt_pct(scores.get("data_freshness_pct"))} · '
                           f'Computed: {_fmt_ts(scores.get("computed_at"))}</p>')

        if any(v is not None for v in struct.values()):
            s_rows = [[k.title(), _fmt_score(v),
                        f'<span class="bar" style="width:{int(float(v or 0) * 1.5)}px"></span>']
                       for k, v in struct.items()]
            body += _section("Structural Breakdown", _table(["Sub-category", "Score", ""], s_rows, [1]))
    else:
        body += _section("Scores", _section_error_msg(data, "scores"))

    # Market snapshot
    if not _error_or_empty(data, "scores"):
        price = scores.get("current_price")
        mcap = scores.get("market_cap")
        vol = scores.get("volume_24h")
        dc = scores.get("daily_change")
        wc = scores.get("weekly_change")
        body += _section("Market Snapshot", _table(
            ["Metric", "Value"],
            [
                ["Price", f"${float(price):.4f}" if price else "—"],
                ["Market Cap", _fmt_usd(mcap)],
                ["Volume 24h", _fmt_usd(vol)],
                ["Daily Change", _fmt_pct(dc)],
                ["Weekly Change", _fmt_pct(wc)],
            ], [1]))

    # Score history
    hist = data.get("score_history", {})
    if not _error_or_empty(data, "score_history"):
        points = hist.get("points") or []
        if points:
            rows = [[p.get("date", "—"), _fmt_score(p.get("score")),
                      _fmt_pct(p.get("daily_change"))]
                     for p in points[:20]]
            body += _section("Score History (last 20)", _table(["Date", "Score", "Daily Change"], rows, [1, 2]))

    # CQI pairs
    cqi = data.get("cqi_pairs", {})
    if not _error_or_empty(data, "cqi_pairs"):
        pairs = cqi.get("pairs") or []
        if pairs:
            rows = [[p.get("protocol", "—"), _fmt_score(p.get("sii_score")),
                      _fmt_score(p.get("psi_score")), _fmt_score(p.get("cqi_score")),
                      _fmt_ts(p.get("computed_at"))]
                     for p in pairs]
            body += _section("CQI Pairs", _table(
                ["Protocol", "SII", "PSI", "CQI", "Computed"], rows, [1, 2, 3]))

    # Evidence
    ev = data.get("evidence", {})
    if not _error_or_empty(data, "evidence"):
        cda_list = ev.get("cda_extractions") or []
        if cda_list:
            rows = [[e.get("extraction_vendor", "—"), e.get("source_type", "—"),
                      _fmt_score(e.get("confidence_score")), _fmt_ts(e.get("extracted_at"))]
                     for e in cda_list[:10]]
            body += _section("CDA Extractions", _table(
                ["Vendor", "Source Type", "Confidence", "Extracted"], rows, [2]))

        static_list = ev.get("static_evidence") or []
        if static_list:
            rows = [[e.get("component_slug", "—"), e.get("capture_method", "—"),
                      '<span class="pill pill-fail">stale</span>' if e.get("is_stale") else '<span class="pill pill-pass">fresh</span>',
                      _fmt_ts(e.get("captured_at"))]
                     for e in static_list[:10]]
            body += _section("Static Evidence", _table(
                ["Component", "Method", "Status", "Captured"], rows))

    # Signals
    sig = data.get("signals", {})
    if not _error_or_empty(data, "signals"):
        div_list = sig.get("divergence") or []
        if div_list:
            rows = [[d.get("detector_name", "—"), d.get("signal_direction", "—"),
                      _fmt_score(d.get("magnitude")), d.get("severity", "—"),
                      _fmt_ts(d.get("cycle_timestamp"))]
                     for d in div_list[:10]]
            body += _section("Divergence Signals", _table(
                ["Detector", "Direction", "Magnitude", "Severity", "Timestamp"], rows, [2]))

        disc_list = sig.get("discovery") or []
        if disc_list:
            rows = [[d.get("signal_type", "—"), d.get("domain", "—"),
                      d.get("title", "—"), _fmt_ts(d.get("detected_at"))]
                     for d in disc_list[:10]]
            body += _section("Discovery Signals", _table(
                ["Type", "Domain", "Title", "Detected"], rows))

    # Distribution
    dist = data.get("distribution", {})
    if not _error_or_empty(data, "distribution"):
        proto_exp = dist.get("protocol_exposure") or []
        if proto_exp:
            rows = [[p.get("protocol_slug", "—"), _fmt_usd(p.get("tvl_usd")),
                      str(p.get("pool_count", "—")), p.get("pool_type", "—")]
                     for p in proto_exp[:15]]
            body += _section("Protocol Exposure", _table(
                ["Protocol", "TVL", "Pools", "Type"], rows, [1, 2]))

    # Graph — top holders
    graph = data.get("graph", {})
    if not _error_or_empty(data, "graph"):
        holders = graph.get("top_holders") or []
        if holders:
            rows = [[f'<code>{h.get("wallet_address", "—")[:12]}…</code>',
                      _fmt_usd(h.get("value_usd")),
                      _fmt_pct(h.get("pct_of_wallet")),
                      _fmt_score(h.get("sii_score"))]
                     for h in holders[:15]]
            body += _section("Top Holders", _table(
                ["Wallet", "Value", "% of Wallet", "SII"], rows, [1, 2, 3]))

    # Timeline
    tl = data.get("timeline", {})
    if not _error_or_empty(data, "timeline"):
        evts = tl.get("events") or []
        if evts:
            rows = [[_fmt_ts(e.get("timestamp")),
                      f'<span class="pill pill-{"pass" if e.get("event_type") == "score_change" else "fail"}">'
                      f'{e.get("event_type")}</span>',
                      e.get("detail", "—")]
                     for e in evts[:30]]
            body += _section("Timeline", _table(["Timestamp", "Type", "Detail"], rows))

    return _entity_page(
        f"{name} — Stablecoin Entity View",
        body,
        form_id="ENTITY · STABLECOIN",
        stats=[f"SII {_fmt_score(overall)}", f"{sym}"],
    )


# ---------------------------------------------------------------------------
# Protocol HTML
# ---------------------------------------------------------------------------

def _render_protocol_page(data: dict) -> str:
    slug = data.get("entity_id", "?")
    name = data.get("name", slug)
    scores = data.get("scores", {})
    overall = scores.get("overall_score")

    body = ""

    from app.templates._html import score_header
    body += score_header(name, overall,
                         subtitle=f"Protocol slug: {slug}")

    # PSI Scores
    if not _error_or_empty(data, "scores"):
        cats = scores.get("categories") or {}
        rows = [[cat.replace("_", " ").title(), _fmt_score(val),
                 f'<span class="bar" style="width:{int(float(val or 0) * 1.5)}px"></span>']
                for cat, val in cats.items()]
        body += _section("PSI Category Breakdown",
                         _table(["Category", "Score", ""], rows, [1])
                         + f'<p class="meta" style="margin-top:8px">Formula: {scores.get("formula_version")} · '
                           f'Confidence: {scores.get("confidence_tag") or "—"} · '
                           f'Components: {scores.get("components_populated")} · '
                           f'Computed: {_fmt_ts(scores.get("computed_at"))}</p>')
    else:
        body += _section("Scores", _section_error_msg(data, "scores"))

    # RPI
    rpi = data.get("rpi", {})
    if not _error_or_empty(data, "rpi") and rpi.get("available"):
        comps = rpi.get("component_scores") or {}
        if comps:
            rows = [[k.replace("_", " ").title(), _fmt_score(v)]
                    for k, v in sorted(comps.items())]
            body += _section("RPI Score",
                             f'<p>Overall: <strong>{_fmt_score(rpi.get("overall_score"))}</strong> '
                             f'(v{rpi.get("methodology_version") or "—"}, '
                             f'{_fmt_ts(rpi.get("computed_at"))})</p>'
                             + _table(["Component", "Score"], rows, [1]))

    # CQI Matrix
    cqi = data.get("cqi_matrix", {})
    if not _error_or_empty(data, "cqi_matrix"):
        pairs = cqi.get("pairs") or []
        if pairs:
            rows = [[p.get("asset", "—"), _fmt_score(p.get("sii_score")),
                      _fmt_score(p.get("psi_score")), _fmt_score(p.get("cqi_score")),
                      _fmt_ts(p.get("computed_at"))]
                     for p in pairs]
            body += _section("CQI Matrix", _table(
                ["Asset", "SII", "PSI", "CQI", "Computed"], rows, [1, 2, 3]))

    # Score history
    hist = data.get("score_history", {})
    if not _error_or_empty(data, "score_history"):
        points = hist.get("points") or []
        if points:
            rows = [[p.get("date", "—"), _fmt_score(p.get("score"))]
                     for p in points[:20]]
            body += _section("PSI Score History (last 20)", _table(
                ["Date", "Score"], rows, [1]))

    # Collateral
    coll = data.get("collateral", {})
    if not _error_or_empty(data, "collateral"):
        col_list = coll.get("collateral") or []
        if col_list:
            rows = [[c.get("token_symbol", "—"), _fmt_usd(c.get("tvl_usd")),
                      '<span class="pill pill-pass">yes</span>' if c.get("is_sii_scored") else '<span class="pill pill-fail">no</span>',
                      _fmt_score(c.get("sii_score")), c.get("pool_type", "—")]
                     for c in col_list[:15]]
            body += _section("Collateral Exposure",
                             f'<p class="meta">Total TVL: {_fmt_usd(coll.get("total_tvl"))}</p>'
                             + _table(["Token", "TVL", "SII Scored", "SII", "Pool Type"], rows, [1, 3]))

        gap = coll.get("unscored_gap") or {}
        if gap.get("count"):
            body += _section("Unscored Exposure Gap",
                             f'<p>{gap.get("count")} unscored stablecoins · '
                             f'{_fmt_usd(gap.get("tvl"))} TVL · '
                             f'{_fmt_pct(gap.get("pct"))} of total</p>'
                             f'<p class="meta">Assets: {", ".join(gap.get("assets") or []) or "—"}</p>')

        treasury_list = coll.get("treasury") or []
        if treasury_list:
            rows = [[t.get("token_symbol", "—"), t.get("token_name", "—"),
                      _fmt_usd(t.get("usd_value")),
                      '<span class="pill pill-pass">yes</span>' if t.get("is_stablecoin") else "no",
                      _fmt_score(t.get("sii_score"))]
                     for t in treasury_list[:15]]
            body += _section("Treasury Holdings", _table(
                ["Symbol", "Name", "Value", "Stablecoin?", "SII"], rows, [2, 4]))

    # Governance
    gov = data.get("governance", {})
    if not _error_or_empty(data, "governance"):
        ev_list = gov.get("events") or []
        if ev_list:
            rows = [[_fmt_ts(e.get("event_timestamp")), e.get("event_type", "—"),
                      e.get("title", "—"),
                      e.get("contributor_tag") or "—", e.get("outcome") or "—"]
                     for e in ev_list[:15]]
            body += _section("Governance Events", _table(
                ["Timestamp", "Type", "Title", "Contributor", "Outcome"], rows))

        prop_list = gov.get("proposals") or []
        if prop_list:
            rows = [[p.get("title", "—")[:60], p.get("source", "—"),
                      p.get("proposal_state", "—"),
                      '<span class="pill pill-pass">risk</span>' if p.get("is_risk_related") else "—",
                      _fmt_pct(p.get("participation_rate"))]
                     for p in prop_list[:10]]
            body += _section("Governance Proposals", _table(
                ["Title", "Source", "State", "Risk-Related?", "Participation"], rows, [4]))

        param_list = gov.get("parameter_changes") or []
        if param_list:
            rows = [[_fmt_ts(p.get("detected_at")), p.get("parameter_type", "—"),
                      f'{p.get("old_value", "—")} → {p.get("new_value", "—")}',
                      p.get("chain", "—")]
                     for p in param_list[:10]]
            body += _section("Parameter Changes", _table(
                ["Detected", "Parameter", "Change", "Chain"], rows))

        inc_list = gov.get("incidents") or []
        if inc_list:
            rows = [[_fmt_ts(i.get("incident_date")), i.get("severity", "—"),
                      i.get("title", "—"), _fmt_usd(i.get("funds_at_risk_usd"))]
                     for i in inc_list[:10]]
            body += _section("Risk Incidents", _table(
                ["Date", "Severity", "Title", "Funds at Risk"], rows, [3]))

    # Evidence
    ev = data.get("evidence", {})
    if not _error_or_empty(data, "evidence"):
        static_list = ev.get("static_evidence") or []
        if static_list:
            rows = [[e.get("component_slug", "—"), e.get("capture_method", "—"),
                      '<span class="pill pill-fail">stale</span>' if e.get("is_stale") else '<span class="pill pill-pass">fresh</span>',
                      _fmt_ts(e.get("captured_at"))]
                     for e in static_list[:10]]
            body += _section("Static Evidence", _table(
                ["Component", "Method", "Status", "Captured"], rows))

    # Signals
    sig = data.get("signals", {})
    if not _error_or_empty(data, "signals"):
        div_list = sig.get("divergence") or []
        if div_list:
            rows = [[d.get("detector_name", "—"), d.get("signal_direction", "—"),
                      _fmt_score(d.get("magnitude")), d.get("severity", "—"),
                      _fmt_ts(d.get("cycle_timestamp"))]
                     for d in div_list[:10]]
            body += _section("Divergence Signals", _table(
                ["Detector", "Direction", "Magnitude", "Severity", "Timestamp"], rows, [2]))

    # Timeline
    tl = data.get("timeline", {})
    if not _error_or_empty(data, "timeline"):
        evts = tl.get("events") or []
        if evts:
            type_colors = {
                "score_change": "pass", "governance": "fail",
                "parameter_change": "fail", "divergence": "fail",
                "incident": "fail",
            }
            rows = [[_fmt_ts(e.get("timestamp")),
                      f'<span class="pill pill-{type_colors.get(e.get("event_type"), "pass")}">'
                      f'{e.get("event_type")}</span>',
                      e.get("detail", "—")]
                     for e in evts[:30]]
            body += _section("Timeline", _table(["Timestamp", "Type", "Detail"], rows))

    return _entity_page(
        f"{name} — Protocol Entity View",
        body,
        form_id="ENTITY · PROTOCOL",
        stats=[f"PSI {_fmt_score(overall)}", slug],
    )


# ---------------------------------------------------------------------------
# Wallet HTML
# ---------------------------------------------------------------------------

def _render_wallet_page(data: dict) -> str:
    addr = data.get("address", "?")
    profile = data.get("profile", {})
    risk_score = profile.get("risk_score")

    body = ""

    from app.templates._html import score_header
    label = ""
    tl = profile.get("treasury_label")
    if tl:
        label = f'{tl.get("entity_name", "")} ({tl.get("entity_type", "")}) · '
    actor = profile.get("actor_type") or "unknown"

    body += score_header(
        f'<code style="font-size:1rem">{addr[:8]}…{addr[-6:]}</code>',
        risk_score,
        subtitle=f'{label}Actor: {actor} · Size: {profile.get("size_tier") or "—"}',
    )

    # Profile summary
    if not _error_or_empty(data, "profile"):
        rows = [
            ["Risk Score", _fmt_score(profile.get("risk_score"))],
            ["Concentration (HHI)", _fmt_score(profile.get("concentration_hhi"))],
            ["Unscored %", _fmt_pct(profile.get("unscored_pct"))],
            ["Coverage Quality", profile.get("coverage_quality") or "—"],
            ["Dominant Asset", f'{profile.get("dominant_asset") or "—"} ({_fmt_pct(profile.get("dominant_asset_pct"))})'],
            ["Total Stablecoin Value", _fmt_usd(profile.get("total_stablecoin_value"))],
            ["Chains Active", str(profile.get("chains_active") or "—")],
            ["Edge Count", str(profile.get("edge_count") or "—")],
            ["Actor Type", f'{profile.get("actor_type") or "—"} (p={_fmt_score(profile.get("agent_probability"))})'],
            ["Computed", _fmt_ts(profile.get("computed_at"))],
        ]
        body += _section("Profile", _table(["Metric", "Value"], rows, [1]))
    else:
        body += _section("Profile", _section_error_msg(data, "profile"))

    # Holdings
    holdings = data.get("holdings", {})
    if not _error_or_empty(data, "holdings"):
        items = holdings.get("items") or []
        if items:
            body += _section("Holdings",
                             f'<p class="meta">Total: {_fmt_usd(holdings.get("total_value"))} · '
                             f'Scored: {holdings.get("scored_count")} · '
                             f'Unscored: {holdings.get("unscored_count")}</p>'
                             + _table(
                                 ["Symbol", "Chain", "Value", "% Wallet", "Scored?", "SII"],
                                 [[h.get("symbol", "—"), h.get("chain", "—"),
                                   _fmt_usd(h.get("value_usd")),
                                   _fmt_pct(h.get("pct_of_wallet")),
                                   '<span class="pill pill-pass">yes</span>' if h.get("is_scored") else '<span class="pill pill-fail">no</span>',
                                   _fmt_score(h.get("sii_score"))]
                                  for h in items[:25]],
                                 [2, 3, 5]))
    else:
        body += _section("Holdings", _section_error_msg(data, "holdings"))

    # Graph — edges
    graph = data.get("graph", {})
    if not _error_or_empty(data, "graph"):
        out = graph.get("outgoing") or []
        if out:
            rows = [[f'<code>{e.get("to_address", "—")[:12]}…</code>',
                      str(e.get("transfer_count", "—")),
                      _fmt_usd(e.get("total_value_usd")),
                      _fmt_ts(e.get("last_transfer_at"))]
                     for e in out[:15]]
            body += _section(f"Outgoing Edges ({len(out)})", _table(
                ["To", "Transfers", "Total Value", "Last Transfer"], rows, [1, 2]))

        inc = graph.get("incoming") or []
        if inc:
            rows = [[f'<code>{e.get("from_address", "—")[:12]}…</code>',
                      str(e.get("transfer_count", "—")),
                      _fmt_usd(e.get("total_value_usd")),
                      _fmt_ts(e.get("last_transfer_at"))]
                     for e in inc[:15]]
            body += _section(f"Incoming Edges ({len(inc)})", _table(
                ["From", "Transfers", "Total Value", "Last Transfer"], rows, [1, 2]))

        body += f'<p class="meta">Total connections: {graph.get("total_connections", 0)}</p>'

    # Activity
    act = data.get("activity", {})
    if not _error_or_empty(data, "activity"):
        payments = act.get("payments") or []
        if payments:
            rows = [[_fmt_ts(p.get("timestamp")), p.get("endpoint", "—"),
                      f'${float(p.get("price_usd") or 0):.4f}',
                      '<span class="pill pill-pass">yes</span>' if p.get("verified") else "no"]
                     for p in payments[:10]]
            body += _section("x402 Payments", _table(
                ["Timestamp", "Endpoint", "Price", "Verified"], rows, [2]))

        assessments = act.get("assessments") or []
        if assessments:
            rows = [[_fmt_ts(a.get("created_at")), a.get("trigger_type", "—"),
                      _fmt_score(a.get("wallet_risk_score")), a.get("severity", "—")]
                     for a in assessments[:10]]
            body += _section("Assessments", _table(
                ["Created", "Trigger", "Risk Score", "Severity"], rows, [2]))

        t_events = act.get("treasury_events") or []
        if t_events:
            rows = [[_fmt_ts(e.get("detected_at")), e.get("event_type", "—"),
                      e.get("severity", "—"), str(e.get("stablecoins_involved") or "—")]
                     for e in t_events[:10]]
            body += _section("Treasury Events", _table(
                ["Detected", "Type", "Severity", "Stablecoins"], rows))

    # Timeline
    tl = data.get("timeline", {})
    if not _error_or_empty(data, "timeline"):
        evts = tl.get("events") or []
        if evts:
            rows = [[_fmt_ts(e.get("timestamp")),
                      f'<span class="pill pill-pass">{e.get("event_type")}</span>',
                      e.get("detail", "—")]
                     for e in evts[:30]]
            body += _section("Timeline", _table(["Timestamp", "Type", "Detail"], rows))

    return _entity_page(
        f"Wallet Entity View",
        body,
        form_id="ENTITY · WALLET",
        stats=[f"Risk {_fmt_score(risk_score)}", f"{addr[:8]}…{addr[-6:]}"],
    )


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------

def register_entity_routes(app):
    """Register entity view routes with the main FastAPI app."""
    app.include_router(router)
    logger.info("Entity view routes registered")
