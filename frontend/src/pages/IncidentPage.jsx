import { useState, useEffect } from "react";

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

const GOVERNANCE_QUESTIONS = [
  "Should LST-bridge composition be a standing risk consideration for collateral listings?",
  "What threshold of independent risk data should gate collateral proposals?",
  "How should cross-chain distribution be weighted in LST risk assessment?",
  "Is an LST's dependency on a single bridge a fragility or a feature?",
];

const AUDIT_DISCLOSURE =
  "Basis does not publish an overall LSTI score for rsETH or its peers; LSTI v0.1.0 is accruing, not promoted to scored status. The component-level values cited above are drawn from live data sources (CoinGecko, DeFiLlama, Etherscan) at query time and one updated static floor (exploit_history_lst, lowered to 10 on 2026-04-20 in response to the Kelp DAO bridge incident).";

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
    case "dex_pool_depth":
      if (n >= 1e9) return `$${(n / 1e9).toFixed(2)}B`;
      if (n >= 1e6) return `$${(n / 1e6).toFixed(1)}M`;
      if (n >= 1e3) return `$${(n / 1e3).toFixed(0)}K`;
      return `$${n.toFixed(0)}`;
    case "eth_peg_deviation":
    case "peg_volatility_7d":
    case "top_holder_concentration":
      return `${n.toFixed(2)}%`;
    case "exploit_history_lst":
      return String(Math.round(n));
    default:
      return String(n);
  }
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
      <a href="/" style={{ color: T.ink, textDecoration: "none", fontWeight: 700, letterSpacing: 0.5 }}>
        Basis Protocol
      </a>
      <span>Incident</span>
    </nav>
  );
}

function IncidentBody({ data, mobile }) {
  // Assembled from smaller section components declared below this component.
  return (
    <>
      <IncidentHeader data={data} mobile={mobile} />
      <div style={{ display: "flex", flexDirection: mobile ? "column" : "row", gap: mobile ? 28 : 40, marginTop: mobile ? 24 : 36 }}>
        <div style={{ flex: 1, minWidth: 0 }}>
          <WhatHappenedSection mobile={mobile} />
          <ComparisonSection data={data} mobile={mobile} />
          <CapturedMissedSection mobile={mobile} />
          <MethodologySection slug={data.slug} mobile={mobile} />
        </div>
        <aside style={{ width: mobile ? "100%" : 300, flexShrink: 0, order: mobile ? -1 : 0 }}>
          <ActionPanel data={data} mobile={mobile} />
        </aside>
      </div>
    </>
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
        {data.summary}
      </p>
      <div style={{
        fontFamily: T.mono, fontSize: 10, color: T.inkFaint,
        borderTop: `1px solid ${T.ruleMid}`, borderBottom: `1px solid ${T.ruleMid}`,
        padding: "8px 0", letterSpacing: 0.5,
      }}>
        Values on this page are pinned to {capturedAt}. Live data at{" "}
        <a href="/api/lsti/scores/kelp-rseth" style={{ color: T.inkMid, borderBottom: `1px solid ${T.ruleMid}`, textDecoration: "none" }}>
          /api/lsti/scores/kelp-rseth
        </a>.
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
          On April 18, 2026, an attacker exploited Kelp DAO's LayerZero bridge and minted 116,500 rsETH on Ethereum mainnet without corresponding ETH backing on the origin chain.
        </p>
        <p style={{ marginBottom: 10 }}>
          The unbacked rsETH — worth approximately $292M at spot — was deposited as collateral on Aave V3 and used to borrow roughly $196M of WETH.
        </p>
        <p style={{ marginBottom: 10 }}>
          The borrow moved Aave V3 TVL materially within the hour. Kelp paused minting on the bridge shortly after the exploit was observed. rsETH depegged from ETH; peers held their peg.
        </p>
        <p>
          This is the largest DeFi exploit of 2026 to date.
        </p>
      </div>
    </section>
  );
}
function ComparisonSection({ data, mobile }) {
  const order = data.components?.component_order || [];
  const meta = data.components?.component_meta || {};
  const peers = data.components?.peers || {};
  const peerOrder = ["kelp-rseth", "lido-steth", "rocket-pool-reth", "etherfi-eeth"];

  const peerLabel = (p) => {
    const info = peers[p];
    if (!info) return p;
    return info.symbol || info.name || p;
  };

  return (
    <section style={{ marginBottom: 44 }}>
      <SectionTitle n="02" title="Component Comparison" mobile={mobile} />
      <p style={{ fontFamily: T.sans, fontSize: mobile ? 13 : 14, lineHeight: 1.6, color: T.inkLight, marginBottom: 16, maxWidth: 720 }}>
        The six components from the rsETH audit's recommended forum-reply data package, pinned to this incident's capture time. No overall LSTI score is shown — the audit explains why.
      </p>

      <div style={{ border: `1px solid ${T.ruleMid}`, overflowX: "auto" }}>
        {!mobile && (
          <div style={{
            display: "grid",
            gridTemplateColumns: "1.6fr 0.9fr 0.9fr 0.9fr 0.9fr 0.9fr",
            padding: "10px 14px",
            borderBottom: `1px solid ${T.ruleMid}`,
            background: T.paperWarm,
            fontFamily: T.mono, fontSize: 10, fontWeight: 600,
            color: T.inkLight, textTransform: "uppercase", letterSpacing: 1,
          }}>
            <span>Component</span>
            <span>{peerLabel("kelp-rseth")}</span>
            <span>{peerLabel("lido-steth")}</span>
            <span>{peerLabel("rocket-pool-reth")}</span>
            <span>{peerLabel("etherfi-eeth")}</span>
            <span>Source</span>
          </div>
        )}
        {order.map((compId, i) => {
          const m = meta[compId] || {};
          const rowBorder = i < order.length - 1 ? `1px dotted ${T.ruleMid}` : "none";
          if (mobile) {
            return (
              <div key={compId} style={{ padding: "12px 14px", borderBottom: rowBorder }}>
                <div style={{ fontFamily: T.sans, fontSize: 13, fontWeight: 600, color: T.ink, marginBottom: 2 }}>
                  {m.label || compId}
                </div>
                <div style={{ fontFamily: T.mono, fontSize: 10, color: T.inkLight, textTransform: "uppercase", letterSpacing: 0.5, marginBottom: 8 }}>
                  {m.category || ""} · {m.source || ""}
                </div>
                {peerOrder.map((p) => {
                  const val = peers[p]?.values?.[compId];
                  return (
                    <div key={p} style={{ display: "flex", justifyContent: "space-between", fontFamily: T.mono, fontSize: 12, padding: "3px 0", color: T.inkMid }}>
                      <span style={{ color: T.inkLight }}>{peerLabel(p)}</span>
                      <span style={{ fontWeight: p === "kelp-rseth" ? 600 : 400, color: p === "kelp-rseth" ? T.ink : T.inkMid }}>
                        {formatValue(compId, val)}
                      </span>
                    </div>
                  );
                })}
              </div>
            );
          }
          return (
            <div key={compId} style={{
              display: "grid",
              gridTemplateColumns: "1.6fr 0.9fr 0.9fr 0.9fr 0.9fr 0.9fr",
              padding: "12px 14px",
              borderBottom: rowBorder,
              alignItems: "baseline",
            }}>
              <div>
                <div style={{ fontFamily: T.sans, fontSize: 13, fontWeight: 600, color: T.ink }}>
                  {m.label || compId}
                </div>
                <div style={{ fontFamily: T.mono, fontSize: 10, color: T.inkFaint, textTransform: "uppercase", letterSpacing: 0.5, marginTop: 2 }}>
                  {m.category || ""}
                </div>
              </div>
              {peerOrder.map((p) => {
                const val = peers[p]?.values?.[compId];
                const isSubject = p === "kelp-rseth";
                return (
                  <div key={p} style={{ fontFamily: T.mono, fontSize: 13, fontWeight: isSubject ? 600 : 400, color: isSubject ? T.ink : T.inkMid }}>
                    {formatValue(compId, val)}
                  </div>
                );
              })}
              <div style={{ fontFamily: T.mono, fontSize: 10, color: T.inkLight, letterSpacing: 0.3 }}>
                {m.source || ""}
              </div>
            </div>
          );
        })}
      </div>
      <div style={{ marginTop: 10, fontFamily: T.mono, fontSize: 10, color: T.inkFaint, lineHeight: 1.5 }}>
        Bold column = rsETH. Static values noted as "static, updated 2026-04-20" reflect the post-incident audit update; see Section 4.
      </div>
    </section>
  );
}
function CapturedMissedSection({ mobile }) {
  const subLabel = {
    fontFamily: T.mono, fontSize: 10, letterSpacing: 1.5,
    color: T.inkLight, textTransform: "uppercase", marginBottom: 8,
  };
  const body = {
    fontFamily: T.sans, fontSize: mobile ? 14 : 15, lineHeight: 1.65,
    color: T.inkMid, marginBottom: 10,
  };
  return (
    <section style={{ marginBottom: 44 }}>
      <SectionTitle n="03" title="What the Scoring Captured, What It Missed" mobile={mobile} />

      <div style={{
        border: `1px solid ${T.ruleMid}`, borderLeft: `3px solid ${T.ink}`,
        padding: mobile ? "14px 16px" : "16px 20px", marginBottom: 14, maxWidth: 720,
      }}>
        <div style={subLabel}>Captured — pre-exploit</div>
        <p style={body}>
          Before the exploit, rsETH already scored lowest in the tracked LST set on the smart-contract category static values: audit_status = 3 (peers 4–8), admin_key_risk = 55 (peers 65–85), upgradeability_risk = 50 (peers 60–75), withdrawal_queue_impl = 60 (peers 70–90), slashing_insurance = 40 (peers 50–90).
        </p>
        <p style={{ ...body, marginBottom: 0 }}>
          Direction of signal: Basis was scoring rsETH as the weakest LST in the set on the components where the numbers were static and researcher-set, not live-feed-driven.
        </p>
      </div>

      <div style={{
        border: `1px solid ${T.ruleMid}`, borderLeft: `3px solid ${T.accent}`,
        padding: mobile ? "14px 16px" : "16px 20px", maxWidth: 720,
      }}>
        <div style={subLabel}>Missed</div>
        <p style={body}>
          <strong style={{ color: T.ink }}>(a) No bridge-dependency model.</strong> LSTI and BRI scored separately. No component modeled "this LST collapses if this bridge fails." The Kelp/LayerZero dependency that made the exploit a single point of failure was not represented in the LSTI component set.
        </p>
        <p style={body}>
          <strong style={{ color: T.ink }}>(b) cross_chain_liquidity was treated as directionally positive.</strong> More chains mapped to a higher score. The exploit demonstrates that single-bridge-multi-chain is a fragility, not a strength — the same bridge failing produces losses across every chain it reaches.
        </p>
        <p style={{ ...body, marginBottom: 0 }}>
          <strong style={{ color: T.ink }}>(c) exploit_history_lst was 100 pre-exploit (no prior exploits).</strong> This component is historical, not predictive. It correctly labelled rsETH as unexploited up to April 18 and then became obsolete in one transaction. It did not and structurally cannot predict a first exploit.
        </p>
      </div>
    </section>
  );
}
function MethodologySection({ slug, mobile }) {
  const link = {
    color: T.ink, textDecoration: "none",
    borderBottom: `1px solid ${T.ruleMid}`, fontFamily: T.mono, fontSize: 13,
  };
  return (
    <section style={{ marginBottom: 24 }}>
      <SectionTitle n="04" title="Methodology & Data Integrity" mobile={mobile} />
      <p style={{ fontFamily: T.sans, fontSize: mobile ? 14 : 15, lineHeight: 1.65, color: T.inkMid, maxWidth: 720, marginBottom: 14 }}>
        This page displays component-level readings, not an overall LSTI score. See the full data-defensibility audit, the index methodology, and the live source.
      </p>
      <ul style={{ listStyle: "none", padding: 0, marginBottom: 18 }}>
        <li style={{ marginBottom: 6 }}>
          <span style={{ fontFamily: T.mono, fontSize: 10, color: T.inkFaint, textTransform: "uppercase", letterSpacing: 1, marginRight: 10 }}>AUDIT</span>
          <a href={`/audits/lsti_rseth_audit_2026-04-20`} style={link}>/audits/lsti_rseth_audit_2026-04-20</a>
        </li>
        <li style={{ marginBottom: 6 }}>
          <span style={{ fontFamily: T.mono, fontSize: 10, color: T.inkFaint, textTransform: "uppercase", letterSpacing: 1, marginRight: 10 }}>METHODOLOGY</span>
          <a href="/methodology" style={link}>/methodology</a>
        </li>
        <li>
          <span style={{ fontFamily: T.mono, fontSize: 10, color: T.inkFaint, textTransform: "uppercase", letterSpacing: 1, marginRight: 10 }}>LIVE</span>
          <a href="/api/lsti/scores/kelp-rseth" style={link}>/api/lsti/scores/kelp-rseth</a>
        </li>
      </ul>
      <blockquote style={{
        fontFamily: T.sans, fontSize: mobile ? 13 : 14, lineHeight: 1.65,
        color: T.inkMid, borderLeft: `3px solid ${T.ink}`,
        padding: "4px 0 4px 14px", maxWidth: 720,
      }}>
        "{AUDIT_DISCLOSURE}"
      </blockquote>
    </section>
  );
}
function ActionPanel({ data, mobile }) {
  return (
    <div style={{ display: "flex", flexDirection: "column", gap: mobile ? 18 : 22 }}>
      <ShareCard data={data} />
      <GovernanceQuestions />
      <EmailCapture slug={data.slug} />
    </div>
  );
}

function ShareCard({ data }) {
  const [copied, setCopied] = useState(false);
  const url = typeof window !== "undefined" ? window.location.href : `https://basisprotocol.xyz/incident/${data.slug}`;
  const img = `/share/incident/${data.slug}.png`;
  const tweet = `Basis published its pre-exploit LSTI component readings for rsETH: ${url}`;
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

function GovernanceQuestions() {
  const [copiedIdx, setCopiedIdx] = useState(null);
  const copy = (text, i) => {
    navigator.clipboard.writeText(text).then(() => {
      setCopiedIdx(i);
      setTimeout(() => setCopiedIdx(null), 1500);
    });
  };
  return (
    <div style={{ border: `1px solid ${T.ruleMid}`, padding: 12, background: T.paper }}>
      <div style={{ fontFamily: T.mono, fontSize: 10, color: T.inkLight, textTransform: "uppercase", letterSpacing: 1.2, marginBottom: 10 }}>
        Questions for governance
      </div>
      <ul style={{ listStyle: "none", padding: 0, margin: 0 }}>
        {GOVERNANCE_QUESTIONS.map((q, i) => (
          <li key={i} style={{
            fontFamily: T.sans, fontSize: 13, lineHeight: 1.5,
            color: T.inkMid, paddingBottom: 10, marginBottom: 10,
            borderBottom: i < GOVERNANCE_QUESTIONS.length - 1 ? `1px dotted ${T.ruleMid}` : "none",
          }}>
            <div style={{ marginBottom: 4 }}>{q}</div>
            <button onClick={() => copy(q, i)} style={{
              background: "transparent", border: `1px solid ${T.ruleMid}`,
              cursor: "pointer", padding: "2px 8px",
              fontFamily: T.mono, fontSize: 10, letterSpacing: 0.8,
              textTransform: "uppercase", color: T.inkLight,
            }}>
              {copiedIdx === i ? "Copied" : "Copy"}
            </button>
          </li>
        ))}
      </ul>
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
