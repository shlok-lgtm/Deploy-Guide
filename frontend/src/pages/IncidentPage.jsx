import { useState, useEffect } from "react";
import BasisLogo from "../BasisLogo";

// Section 04 — pinned evidence snapshot. Values reflect what production's
// generic_index_scores.raw_values held for each tracked entity on
// 2026-04-20. Not fetched from the API at render time; this is pinned
// record, not live data.
const SECTION_04_VALUES = {
  "kelp-rseth":       { audit_status: 7, admin_key_risk: 95, upgradeability_risk: 70,  withdrawal_queue_impl: 60, slashing_insurance: 40, exploit_history_lst: 10  },
  "lido-steth":       { audit_status: 8, admin_key_risk: 95, upgradeability_risk: 70,  withdrawal_queue_impl: 90, slashing_insurance: 80, exploit_history_lst: 100 },
  "rocket-pool-reth": { audit_status: 6, admin_key_risk: 95, upgradeability_risk: 100, withdrawal_queue_impl: 85, slashing_insurance: 90, exploit_history_lst: 100 },
  "etherfi-eeth":     { audit_status: 7, admin_key_risk: 95, upgradeability_risk: 70,  withdrawal_queue_impl: 85, slashing_insurance: 50, exploit_history_lst: 100 },
};

// Row ordering for the Section 02 "What Basis Knew" table. Labels render
// as plain strings; Admin key risk carries a footnote dagger appended in
// the section renderer. Every measure here is higher-is-better.
const SECTION_04_ROWS = [
  { key: "audit_status",          label: "Audits completed" },
  { key: "upgradeability_risk",   label: "Upgradeability risk" },
  { key: "withdrawal_queue_impl", label: "Withdrawal queue" },
  { key: "slashing_insurance",    label: "Slashing insurance" },
  { key: "exploit_history_lst",   label: "Exploit history" },
  { key: "admin_key_risk",        label: "Admin key risk (0–100, higher = safer)", dagger: true },
];

const SECTION_04_PEERS = ["kelp-rseth", "lido-steth", "rocket-pool-reth", "etherfi-eeth"];

// Bolding rules:
//   - All-equal row → no bold (e.g. admin_key_risk saturates at 95)
//   - rsETH strictly worst (unique minimum) → bold rsETH
//   - Otherwise → bold the best-performing non-rsETH cell
// Matches the visual rhythm the copy describes: worst-on-rsETH rows draw
// the eye to rsETH; on rows where rsETH is at least tied-with-peers, the
// eye lands on the best peer instead.
function section04BoldCell(rowKey, peer) {
  const pairs = SECTION_04_PEERS.map((p) => [p, SECTION_04_VALUES[p][rowKey]]);
  const uniqVals = new Set(pairs.map(([, v]) => v));
  if (uniqVals.size === 1) return false;
  const rsEthVal = SECTION_04_VALUES["kelp-rseth"][rowKey];
  const minVal = Math.min(...pairs.map(([, v]) => v));
  const rsEthStrictlyWorst = rsEthVal === minVal && pairs.every(([p, v]) => p === "kelp-rseth" || v > minVal);
  if (rsEthStrictlyWorst) return peer === "kelp-rseth";
  const maxVal = Math.max(...pairs.map(([, v]) => v));
  const bestPeer = pairs.find(([p, v]) => v === maxVal && p !== "kelp-rseth");
  return bestPeer && peer === bestPeer[0];
}

const T = {
  paper: "#f5f2ec",
  paperWarm: "#f0ece3",
  ink: "#0a0a0a",
  inkMid: "#3a3a3a",
  inkLight: "#6a6a6a",
  inkFaint: "#9a9a9a",
  ruleMid: "#c8c4bc",
  ruleLight: "#e0ddd6",
  accent: "#c0392b",
  mono: "'IBM Plex Mono', monospace",
  sans: "'IBM Plex Sans', system-ui, sans-serif",
};

const API = "";
const _DK = "BF6KF2i34EslzTnvBXAjcLlDZBlQKLSTP9LdrAzxUHI";
const apiFetch = (url, opts) =>
  fetch(`${url}${url.includes("?") ? "&" : "?"}apikey=${_DK}`, opts);

function useIsMobile() {
  const [mobile, setMobile] = useState(
    typeof window !== "undefined" ? window.innerWidth < 820 : false
  );
  useEffect(() => {
    const h = () => setMobile(window.innerWidth < 820);
    window.addEventListener("resize", h);
    return () => window.removeEventListener("resize", h);
  }, []);
  return mobile;
}

function formatValue(componentId, value) {
  if (value === null || value === undefined) return "—";
  const n = typeof value === "number" ? value : parseFloat(value);
  if (Number.isNaN(n)) return String(value);
  switch (componentId) {
    case "market_cap":
      if (n >= 1e9) return `$${(n / 1e9).toFixed(2)}B`;
      if (n >= 1e6) return `$${(n / 1e6).toFixed(1)}M`;
      if (n >= 1e3) return `$${(n / 1e3).toFixed(0)}K`;
      return `$${n.toFixed(0)}`;
    case "eth_price_ratio":
      return n.toFixed(3);
    case "peg_volatility_7d":
      return `${n.toFixed(2)}%`;
    case "exploit_history_lst":
      return String(Math.round(n));
    default:
      return String(n);
  }
}

function designLabel(d) {
  if (d === "rebasing") return "Rebasing";
  if (d === "reward-bearing") return "Reward-bearing";
  return null;
}

export default function IncidentPage({ slug }) {
  const mobile = useIsMobile();
  const [data, setData] = useState(null);
  const [err, setErr] = useState(null);

  useEffect(() => {
    let cancelled = false;
    apiFetch(`${API}/api/incident/${slug}`)
      .then((r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return r.json();
      })
      .then((d) => {
        if (!cancelled) setData(d);
      })
      .catch((e) => {
        if (!cancelled) setErr(e.message);
      });
    return () => {
      cancelled = true;
    };
  }, [slug]);

  if (err) {
    return (
      <Shell mobile={mobile}>
        <div style={{ padding: 40, textAlign: "center", color: T.inkMid, fontFamily: T.mono }}>
          Incident not found.
        </div>
      </Shell>
    );
  }
  if (!data) {
    return (
      <Shell mobile={mobile}>
        <div style={{ padding: 40, textAlign: "center", color: T.inkFaint, fontFamily: T.mono, fontSize: 12 }}>
          Loading incident snapshot…
        </div>
      </Shell>
    );
  }

  return (
    <Shell mobile={mobile}>
      <IncidentBody data={data} mobile={mobile} />
    </Shell>
  );
}

function Shell({ mobile, children }) {
  return (
    <div style={{ minHeight: "100vh", background: T.paper, color: T.ink, fontFamily: T.sans }}>
      <style>{`
        * { box-sizing: border-box; margin: 0; padding: 0; }
        html, body { background: ${T.paper}; }
        body { overflow-x: hidden; }
        a { color: inherit; }
        button { font-family: inherit; }
        button:hover { opacity: 0.88; }
        @keyframes fadeIn { from { opacity: 0; transform: translateY(6px); } to { opacity: 1; transform: translateY(0); } }
      `}</style>
      <div style={{ maxWidth: 1100, margin: "0 auto", padding: mobile ? "12px 14px 64px" : "32px 24px 80px" }}>
        <TopNav mobile={mobile} />
        {children}
      </div>
    </div>
  );
}

function TopNav({ mobile }) {
  return (
    <nav
      style={{
        display: "flex",
        justifyContent: "space-between",
        alignItems: "center",
        paddingBottom: 14,
        marginBottom: mobile ? 18 : 28,
        borderBottom: `1px solid ${T.ruleMid}`,
        fontFamily: T.mono,
        fontSize: 11,
        letterSpacing: 1,
        textTransform: "uppercase",
        color: T.inkLight,
      }}
    >
      <a href="/" style={{ display: "flex", alignItems: "center", gap: 10, color: T.ink, textDecoration: "none", fontWeight: 700, letterSpacing: 0.5 }}>
        <BasisLogo size={mobile ? 28 : 38} />
        <span>Basis Protocol</span>
      </a>
      <span>Incident</span>
    </nav>
  );
}

function IncidentBody({ data, mobile }) {
  // Chronological order: what happened → what Basis knew before →
  // observable measures after → measurement roadmap. Admin-key footnote
  // lives at the page bottom, outside the two-column grid, so it sits
  // below both the main column and the sidebar on desktop.
  return (
    <>
      <IncidentHeader data={data} mobile={mobile} />
      <div style={{ display: "flex", flexDirection: mobile ? "column" : "row", gap: mobile ? 28 : 40, marginTop: mobile ? 24 : 36 }}>
        <div style={{ flex: 1, minWidth: 0 }}>
          <WhatHappenedSection mobile={mobile} />
          <Section02WhatBasisKnew mobile={mobile} />
          <Section03ObservableMeasures mobile={mobile} />
          <Section04MeasurementRoadmap mobile={mobile} />
        </div>
        <aside style={{ width: mobile ? "100%" : 300, flexShrink: 0, order: mobile ? -1 : 0 }}>
          <ActionPanel data={data} mobile={mobile} />
        </aside>
      </div>
      <PageFootnote mobile={mobile} />
    </>
  );
}

function PageFootnote({ mobile }) {
  return (
    <div style={{ marginTop: mobile ? 32 : 48 }}>
      <hr style={{ border: "none", borderTop: `1px solid ${T.ruleMid}`, marginBottom: 14 }} />
      <p style={{
        fontFamily: T.mono, fontSize: 10, fontStyle: "italic",
        color: T.inkFaint, lineHeight: 1.55, maxWidth: 720,
      }}>
        † Admin key risk resolves to 95 across all four tracked LSTs because the live contract-analysis override saturates the value when a standard transparent-proxy + timelock pattern is detected. Per-entity differentiation is a follow-up methodology item.
      </p>
    </div>
  );
}

function IncidentHeader({ data, mobile }) {
  const eventDate = new Date(data.event_date + "T00:00:00Z").toLocaleDateString("en-US", {
    month: "long", day: "numeric", year: "numeric", timeZone: "UTC",
  });
  const capturedAt = data.captured_at
    ? new Date(data.captured_at).toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric", timeZone: "UTC" })
    : "—";
  const slugUpper = (data.slug || "").toUpperCase();
  return (
    <header>
      <div style={{ fontFamily: T.mono, fontSize: 10, letterSpacing: 1.5, color: T.inkLight, textTransform: "uppercase", marginBottom: 10 }}>
        Incident / {slugUpper}
      </div>
      <h1 style={{
        fontFamily: T.sans, fontSize: mobile ? 24 : 32, fontWeight: 700,
        lineHeight: 1.18, color: T.ink, marginBottom: 10, letterSpacing: -0.3,
      }}>
        {data.title}
      </h1>
      <div style={{ fontFamily: T.mono, fontSize: 12, color: T.inkMid, marginBottom: 14 }}>
        {eventDate}
      </div>
      <p style={{ fontFamily: T.sans, fontSize: mobile ? 15 : 16, lineHeight: 1.55, color: T.inkMid, maxWidth: 720, marginBottom: 14 }}>
        Basis's tracked measures showed rsETH lagging peers on withdrawal queue design and slashing insurance before the event, and diverging sharply from peers on peg behavior after.
      </p>
      <div style={{
        fontFamily: T.mono, fontSize: 10, color: T.inkFaint,
        borderTop: `1px solid ${T.ruleMid}`, borderBottom: `1px solid ${T.ruleMid}`,
        padding: "8px 0", letterSpacing: 0.5,
      }}>
        Values on this page are pinned to April 21, 2026.
      </div>
    </header>
  );
}
function SectionTitle({ n, title, mobile }) {
  return (
    <div style={{ marginBottom: 14 }}>
      <div style={{ fontFamily: T.mono, fontSize: 10, letterSpacing: 1.5, color: T.inkLight, textTransform: "uppercase", marginBottom: 6 }}>
        Section {n}
      </div>
      <h2 style={{ fontFamily: T.sans, fontSize: mobile ? 18 : 20, fontWeight: 600, color: T.ink, letterSpacing: -0.2 }}>
        {title}
      </h2>
    </div>
  );
}

function WhatHappenedSection({ mobile }) {
  return (
    <section style={{ marginBottom: 40, paddingTop: 8 }}>
      <SectionTitle n="01" title="What Happened" mobile={mobile} />
      <div style={{ fontFamily: T.sans, fontSize: mobile ? 14 : 15, lineHeight: 1.7, color: T.inkMid, maxWidth: 720 }}>
        <p style={{ marginBottom: 10 }}>
          On April 18, an attacker exploited Kelp DAO's LayerZero bridge to mint 116,500 rsETH (~$292M) without ETH backing, depositing it as Aave V3 collateral to borrow ~$196M of WETH.
        </p>
        <p>
          Kelp paused bridge minting shortly after the exploit was observed.
        </p>
      </div>
    </section>
  );
}
// Section 03 — pinned post-event observable measures. Values hardcoded,
// reflect the April 21, 2026 snapshot. No API call at render time.
const SECTION_03_ROWS = [
  {
    key: "exploit_history",
    label: "Exploit History",
    source: "Basis static config, updated April 20, 2026",
    values: {
      "kelp-rseth":       { display: "10",    bold: true  },
      "lido-steth":       { display: "100",   bold: false },
      "rocket-pool-reth": { display: "100",   bold: false },
      "etherfi-eeth":     { display: "100",   bold: false },
    },
  },
  {
    key: "eth_price_ratio",
    label: "ETH Price Ratio",
    source: "CoinGecko",
    values: {
      "kelp-rseth":       { display: "0.844", design: "Reward-bearing", bold: true  },
      "lido-steth":       { display: "0.993", design: "Rebasing",       bold: false },
      "rocket-pool-reth": { display: "1.155", design: "Reward-bearing", bold: false },
      "etherfi-eeth":     { display: "1.003", design: "Rebasing",       bold: false },
    },
  },
  {
    key: "peg_volatility_7d",
    label: "7d Peg Volatility",
    source: "CoinGecko",
    values: {
      "kelp-rseth":       { display: "14.16%", bold: true  },
      "lido-steth":       { display: "2.26%",  bold: false },
      "rocket-pool-reth": { display: "2.36%",  bold: false },
      "etherfi-eeth":     { display: "3.15%",  bold: false },
    },
  },
  {
    key: "market_cap",
    label: "Market Cap",
    source: "CoinGecko",
    values: {
      "kelp-rseth":       { display: "$1.30B",  bold: false },
      "lido-steth":       { display: "$21.72B", bold: false },
      "rocket-pool-reth": { display: "$903M",   bold: false },
      "etherfi-eeth":     { display: "$602M",   bold: false },
    },
  },
];

function Section03ObservableMeasures({ mobile }) {
  const peerLabelMap = { "kelp-rseth": "rsETH", "lido-steth": "stETH", "rocket-pool-reth": "rETH", "etherfi-eeth": "eETH" };

  return (
    <section style={{ marginBottom: 44 }}>
      <SectionTitle n="03" title="Observable Measures After the Event" mobile={mobile} />
      <p style={{ fontFamily: T.sans, fontSize: mobile ? 14 : 15, lineHeight: 1.65, color: T.inkMid, maxWidth: 720, marginBottom: 16 }}>
        The four measures below, pinned to April 21, 2026, show how rsETH's observable risk profile compared to three peer liquid staking tokens in the days following the exploit.
      </p>

      <div style={{ border: `1px solid ${T.ruleMid}`, overflowX: "auto" }}>
        {!mobile && (
          <div style={{
            display: "grid",
            gridTemplateColumns: "1.4fr 0.9fr 0.9fr 0.9fr 0.9fr 1.4fr",
            padding: "10px 14px",
            borderBottom: `1px solid ${T.ruleMid}`,
            background: T.paperWarm,
            fontFamily: T.mono, fontSize: 10, fontWeight: 600,
            color: T.inkLight, textTransform: "uppercase", letterSpacing: 1,
          }}>
            <span>Measure</span>
            <span>rsETH</span>
            <span>stETH</span>
            <span>rETH</span>
            <span>eETH</span>
            <span>Source</span>
          </div>
        )}

        {SECTION_03_ROWS.map((row, i) => {
          const rowBorder = i < SECTION_03_ROWS.length - 1 ? `1px dotted ${T.ruleMid}` : "none";

          if (mobile) {
            return (
              <div key={row.key} style={{ padding: "12px 14px", borderBottom: rowBorder }}>
                <div style={{ fontFamily: T.sans, fontSize: 13, fontWeight: 600, color: T.ink, marginBottom: 2 }}>
                  {row.label}
                </div>
                <div style={{ fontFamily: T.mono, fontSize: 10, color: T.inkLight, textTransform: "uppercase", letterSpacing: 0.5, marginBottom: 8 }}>
                  {row.source}
                </div>
                {SECTION_04_PEERS.map((p) => {
                  const cell = row.values[p];
                  return (
                    <div key={p} style={{ padding: "3px 0", borderBottom: `1px dotted ${T.ruleLight}` }}>
                      <div style={{ display: "flex", justifyContent: "space-between", fontFamily: T.mono, fontSize: 12, color: T.inkMid }}>
                        <span style={{ color: T.inkLight }}>{peerLabelMap[p]}</span>
                        <span style={{ fontWeight: cell.bold ? 600 : 400, color: cell.bold ? T.ink : T.inkMid }}>
                          {cell.display}
                        </span>
                      </div>
                      {cell.design && (
                        <div style={{ fontFamily: T.mono, fontSize: 10, color: T.inkFaint, textAlign: "right", marginTop: 1, letterSpacing: 0.3 }}>
                          {cell.design}
                        </div>
                      )}
                    </div>
                  );
                })}
              </div>
            );
          }

          return (
            <div key={row.key} style={{
              display: "grid",
              gridTemplateColumns: "1.4fr 0.9fr 0.9fr 0.9fr 0.9fr 1.4fr",
              padding: "12px 14px",
              borderBottom: rowBorder,
              alignItems: "baseline",
            }}>
              <div style={{ fontFamily: T.sans, fontSize: 13, fontWeight: 600, color: T.ink }}>
                {row.label}
              </div>
              {SECTION_04_PEERS.map((p) => {
                const cell = row.values[p];
                return (
                  <div key={p}>
                    <div style={{ fontFamily: T.mono, fontSize: 13, fontWeight: cell.bold ? 600 : 400, color: cell.bold ? T.ink : T.inkMid }}>
                      {cell.display}
                    </div>
                    {cell.design && (
                      <div style={{ fontFamily: T.mono, fontSize: 10, color: T.inkFaint, letterSpacing: 0.3, marginTop: 2 }}>
                        {cell.design}
                      </div>
                    )}
                  </div>
                );
              })}
              <div style={{ fontFamily: T.mono, fontSize: 10, color: T.inkLight, letterSpacing: 0.3 }}>
                {row.source}
              </div>
            </div>
          );
        })}
      </div>
    </section>
  );
}
function Section04MeasurementRoadmap({ mobile }) {
  return (
    <section style={{ marginBottom: 44 }}>
      <SectionTitle n="04" title="Measurement Roadmap" mobile={mobile} />
      <p style={{ fontFamily: T.sans, fontSize: mobile ? 14 : 15, lineHeight: 1.7, color: T.inkMid, maxWidth: 720 }}>
        Basis did not anticipate the bridge exploit itself. The measures that track smart-contract risk (bridge integrity, cross-chain attestation, mint authority control) do not currently cover LayerZero-style message-passing bridges as a failure surface. This is a named follow-up.
      </p>
    </section>
  );
}

function Section02WhatBasisKnew({ mobile }) {
  const peerLabelMap = { "kelp-rseth": "rsETH", "lido-steth": "stETH", "rocket-pool-reth": "rETH", "etherfi-eeth": "eETH" };
  const rowLabel = (row) => (
    <>
      {row.label}
      {row.dagger && <sup style={{ fontSize: "0.65em", marginLeft: 1 }}>†</sup>}
    </>
  );

  return (
    <section style={{ marginBottom: 44 }}>
      <SectionTitle n="02" title="What Basis Knew on April 20, 2026" mobile={mobile} />

      <p style={{ fontFamily: T.sans, fontSize: mobile ? 14 : 15, lineHeight: 1.65, color: T.inkMid, maxWidth: 720, marginBottom: 14 }}>
        Basis measures did not show broad-based weakness in rsETH. They showed a narrower pattern:
      </p>

      <ul style={{ margin: "0 0 20px 22px", padding: 0, fontFamily: T.sans, fontSize: mobile ? 14 : 15, lineHeight: 1.7, color: T.inkMid, maxWidth: 720 }}>
        <li style={{ marginBottom: 6 }}>Relative weakness was concentrated in withdrawal queue design and slashing insurance</li>
        <li>The rest of the tracked smart-contract profile was broadly in line with peers or better</li>
      </ul>

      {/* 5-column table: Measure / rsETH / stETH / rETH / eETH (no SOURCE) */}
      <div style={{ border: `1px solid ${T.ruleMid}`, overflowX: "auto" }}>
        {!mobile && (
          <div style={{
            display: "grid",
            gridTemplateColumns: "2.2fr 0.8fr 0.8fr 0.8fr 0.8fr",
            padding: "10px 14px",
            borderBottom: `1px solid ${T.ruleMid}`,
            background: T.paperWarm,
            fontFamily: T.mono, fontSize: 10, fontWeight: 600,
            color: T.inkLight, textTransform: "uppercase", letterSpacing: 1,
          }}>
            <span>Measure</span>
            <span>rsETH</span>
            <span>stETH</span>
            <span>rETH</span>
            <span>eETH</span>
          </div>
        )}

        {SECTION_04_ROWS.map((row, i) => {
          const rowBorder = i < SECTION_04_ROWS.length - 1 ? `1px dotted ${T.ruleMid}` : "none";

          if (mobile) {
            return (
              <div key={row.key} style={{ padding: "12px 14px", borderBottom: rowBorder }}>
                <div style={{ fontFamily: T.sans, fontSize: 13, fontWeight: 600, color: T.ink, marginBottom: 8 }}>
                  {rowLabel(row)}
                </div>
                {SECTION_04_PEERS.map((p) => {
                  const bold = section04BoldCell(row.key, p);
                  return (
                    <div key={p} style={{ display: "flex", justifyContent: "space-between", fontFamily: T.mono, fontSize: 12, padding: "3px 0", color: T.inkMid }}>
                      <span style={{ color: T.inkLight }}>{peerLabelMap[p]}</span>
                      <span style={{ fontWeight: bold ? 600 : 400, color: bold ? T.ink : T.inkMid }}>
                        {SECTION_04_VALUES[p][row.key]}
                      </span>
                    </div>
                  );
                })}
              </div>
            );
          }

          return (
            <div key={row.key} style={{
              display: "grid",
              gridTemplateColumns: "2.2fr 0.8fr 0.8fr 0.8fr 0.8fr",
              padding: "12px 14px",
              borderBottom: rowBorder,
              alignItems: "baseline",
            }}>
              <div style={{ fontFamily: T.sans, fontSize: 13, fontWeight: 600, color: T.ink }}>
                {rowLabel(row)}
              </div>
              {SECTION_04_PEERS.map((p) => {
                const bold = section04BoldCell(row.key, p);
                return (
                  <div key={p} style={{
                    fontFamily: T.mono, fontSize: 13,
                    fontWeight: bold ? 600 : 400,
                    color: bold ? T.ink : T.inkMid,
                  }}>
                    {SECTION_04_VALUES[p][row.key]}
                  </div>
                );
              })}
            </div>
          );
        })}
      </div>

      <p style={{
        marginTop: 12,
        fontFamily: T.mono, fontSize: 10, color: T.inkFaint,
        lineHeight: 1.5, maxWidth: 720,
      }}>
        This section is evidentiary, values do not update.
      </p>
    </section>
  );
}

function ActionPanel({ data, mobile }) {
  return (
    <div style={{ display: "flex", flexDirection: "column", gap: mobile ? 18 : 22 }}>
      <ShareCard data={data} />
      <EmailCapture slug={data.slug} />
    </div>
  );
}

function ShareCard({ data }) {
  const [copied, setCopied] = useState(false);
  const url = typeof window !== "undefined" ? window.location.href : `https://basisprotocol.xyz/incident/${data.slug}`;
  const img = `/share/incident/${data.slug}.png`;
  const tweet = `Basis published its pre-exploit LSTI measure readings for rsETH: ${url}`;
  const xHref = `https://twitter.com/intent/tweet?text=${encodeURIComponent(tweet)}`;
  const copy = () => {
    navigator.clipboard.writeText(url).then(() => {
      setCopied(true);
      setTimeout(() => setCopied(false), 1500);
    });
  };
  const btn = {
    flex: 1, padding: "8px 10px", fontFamily: T.mono, fontSize: 11,
    letterSpacing: 0.8, textTransform: "uppercase", cursor: "pointer",
    background: "transparent", border: `1px solid ${T.ink}`, color: T.ink,
    textAlign: "center", textDecoration: "none", display: "inline-block",
  };
  return (
    <div style={{ border: `1px solid ${T.ruleMid}`, padding: 12, background: T.paper }}>
      <div style={{ fontFamily: T.mono, fontSize: 10, color: T.inkLight, textTransform: "uppercase", letterSpacing: 1.2, marginBottom: 8 }}>
        Share
      </div>
      <div style={{
        width: "100%", aspectRatio: "1200 / 630",
        background: T.paperWarm, border: `1px solid ${T.ruleMid}`,
        marginBottom: 10, overflow: "hidden",
      }}>
        <img src={img} alt="rsETH pre-exploit scoring share card" style={{ width: "100%", height: "100%", objectFit: "cover", display: "block" }} onError={(e) => { e.currentTarget.style.display = "none"; }} />
      </div>
      <div style={{ display: "flex", gap: 6 }}>
        <button onClick={copy} style={btn}>{copied ? "Copied" : "Copy link"}</button>
        <a href={xHref} target="_blank" rel="noopener noreferrer" style={btn}>Post on X</a>
      </div>
    </div>
  );
}

function EmailCapture({ slug }) {
  const [email, setEmail] = useState("");
  const [status, setStatus] = useState("idle"); // idle | sending | done | error
  const submit = (e) => {
    e.preventDefault();
    if (!email || status === "sending") return;
    setStatus("sending");
    fetch(`${API}/api/incident-notify`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ email, source: `incident-${slug}` }),
    })
      .then((r) => {
        setStatus(r.ok ? "done" : "error");
        if (r.ok) setEmail("");
      })
      .catch(() => setStatus("error"));
  };
  return (
    <form onSubmit={submit} style={{ border: `1px solid ${T.ruleMid}`, padding: 12, background: T.paper }}>
      <div style={{ fontFamily: T.mono, fontSize: 10, color: T.inkLight, textTransform: "uppercase", letterSpacing: 1.2, marginBottom: 8 }}>
        Get notified of future incident analyses
      </div>
      <div style={{ display: "flex", gap: 6 }}>
        <input
          type="email"
          required
          value={email}
          onChange={(e) => setEmail(e.target.value)}
          placeholder="you@work.com"
          style={{
            flex: 1, padding: "8px 10px", fontFamily: T.mono, fontSize: 12,
            border: `1px solid ${T.ruleMid}`, background: T.paper, color: T.ink,
            outline: "none",
          }}
          disabled={status === "sending" || status === "done"}
        />
        <button type="submit" disabled={status === "sending" || status === "done"} style={{
          padding: "8px 12px", fontFamily: T.mono, fontSize: 11,
          letterSpacing: 0.8, textTransform: "uppercase",
          background: T.ink, color: T.paper, border: `1px solid ${T.ink}`,
          cursor: status === "sending" || status === "done" ? "default" : "pointer",
        }}>
          {status === "done" ? "Done" : status === "sending" ? "…" : "Subscribe"}
        </button>
      </div>
      {status === "done" && (
        <div style={{ marginTop: 8, fontFamily: T.mono, fontSize: 10, color: T.inkMid, letterSpacing: 0.5 }}>
          Thanks — we'll only email about incidents.
        </div>
      )}
      {status === "error" && (
        <div style={{ marginTop: 8, fontFamily: T.mono, fontSize: 10, color: T.accent, letterSpacing: 0.5 }}>
          Something went wrong. Try again.
        </div>
      )}
    </form>
  );
}
