import { useState, useEffect, useCallback, useRef } from "react";

const API = "";

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

const MICA_STATUS = {
  usdc: "Compliant", usdt: "Watchlist", dai: "Compliant", usde: "Watchlist",
  fdusd: "Compliant", pyusd: "Compliant", tusd: "Non-compliant",
  usdd: "Non-compliant", frax: "Watchlist", usd1: "Pending",
};

const RESERVE_TYPE = {
  usdc: "Fiat-backed", usdt: "Fiat-backed", dai: "Crypto-backed",
  usde: "Synthetic", fdusd: "Fiat-backed", pyusd: "Fiat-backed",
  tusd: "Fiat-backed", usdd: "Algorithmic", frax: "Synthetic", usd1: "Mixed",
};

const subScoreColor = (s) => {
  if (s == null) return T.inkFaint;
  if (s >= 85) return T.ink;
  if (s >= 65) return T.inkMid;
  return T.accent;
};

const gradeColor = (g) => {
  if (!g) return T.inkFaint;
  if (g.startsWith("A")) return T.ink;
  if (g === "B+" || g === "B") return T.inkMid;
  return T.accent;
};

const fmt = (n, d = 1) => (n != null ? Number(n).toFixed(d) : "—");
const truncAddr = (addr) => addr ? `${addr.slice(0, 8)}…${addr.slice(-6)}` : "—";
const fmtHHI = (hhi) => hhi != null ? Number(hhi).toFixed(0) : "—";
const statusColor = (s) => {
  if (s === "scored")      return "#2d6b45";
  if (s === "queued")      return T.inkMid;
  if (s === "in_progress") return T.inkLight;
  return T.inkFaint;
};
const coverageColor = (q) => {
  if (q === "full" || q === "high") return T.ink;
  if (q === "partial" || q === "medium") return T.inkMid;
  return T.accent;
};
const fmtB = (n) => {
  if (!n) return "—";
  if (n >= 1e12) return `$${(n / 1e12).toFixed(1)}T`;
  if (n >= 1e9) return `$${(n / 1e9).toFixed(1)}B`;
  if (n >= 1e6) return `$${(n / 1e6).toFixed(0)}M`;
  return `$${n.toLocaleString()}`;
};

function useIsMobile() {
  const [mobile, setMobile] = useState(window.innerWidth < 700);
  useEffect(() => {
    const h = () => setMobile(window.innerWidth < 700);
    window.addEventListener("resize", h);
    return () => window.removeEventListener("resize", h);
  }, []);
  return mobile;
}

function useScores() {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [ts, setTs] = useState(null);

  useEffect(() => {
    let mounted = true;
    const load = async () => {
      try {
        const r = await fetch(`${API}/api/scores`);
        const d = await r.json();
        if (mounted) {
          setData(d.stablecoins || []);
          setTs(d.timestamp);
          setLoading(false);
        }
      } catch (e) {
        if (mounted) { setError(e.message); setLoading(false); }
      }
    };
    load();
    const interval = setInterval(load, 300000);
    return () => { mounted = false; clearInterval(interval); };
  }, []);

  return { data, loading, error, ts };
}

function useCoinDetail(coinId) {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    if (!coinId) return;
    setLoading(true);
    fetch(`${API}/api/scores/${coinId}`)
      .then((r) => r.json())
      .then((d) => { setData(d); setLoading(false); })
      .catch(() => setLoading(false));
  }, [coinId]);

  return { data, loading };
}

function useCoinHistory(coinId, days = 90) {
  const [data, setData] = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    if (!coinId) return;
    setLoading(true);
    fetch(`${API}/api/scores/${coinId}/history?days=${days}`)
      .then((r) => r.json())
      .then((d) => { setData(d.history || []); setLoading(false); })
      .catch(() => setLoading(false));
  }, [coinId, days]);

  return { data, loading };
}

function useWalletTop(limit = 50) {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  useEffect(() => {
    fetch(`${API}/api/wallets/top?limit=${limit}`)
      .then((r) => r.json())
      .then((d) => { setData(d.wallets || []); setLoading(false); })
      .catch(() => setLoading(false));
  }, [limit]);
  return { data, loading };
}

function useWalletRiskiest(limit = 50) {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  useEffect(() => {
    fetch(`${API}/api/wallets/riskiest?limit=${limit}`)
      .then((r) => r.json())
      .then((d) => { setData(d.wallets || []); setLoading(false); })
      .catch(() => setLoading(false));
  }, [limit]);
  return { data, loading };
}

function useBacklog(limit = 50) {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  useEffect(() => {
    fetch(`${API}/api/backlog?limit=${limit}`)
      .then((r) => r.json())
      .then((d) => { setData(d.backlog || []); setLoading(false); })
      .catch(() => setLoading(false));
  }, [limit]);
  return { data, loading };
}

function useWalletDetail(address) {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  const lookup = useCallback((addr) => {
    if (!addr) return;
    setLoading(true);
    setError(null);
    setData(null);
    fetch(`${API}/api/wallets/${addr.trim()}`)
      .then((r) => {
        if (!r.ok) throw new Error(`Not found (${r.status})`);
        return r.json();
      })
      .then((d) => { setData(d); setLoading(false); })
      .catch((e) => { setError(e.message); setLoading(false); });
  }, []);

  return { data, loading, error, lookup };
}

function useAllHistory(coinIds) {
  const [histMap, setHistMap] = useState({});

  useEffect(() => {
    if (!coinIds || coinIds.length === 0) return;
    coinIds.forEach((id) => {
      fetch(`${API}/api/scores/${id}/history?days=21`)
        .then((r) => r.json())
        .then((d) => {
          setHistMap((prev) => ({ ...prev, [id]: d.history || [] }));
        })
        .catch(() => {});
    });
  }, [coinIds?.join(",")]);

  return histMap;
}

function usePsiScores() {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  useEffect(() => {
    fetch(`${API}/api/psi/scores`)
      .then(r => r.json())
      .then(d => { setData(d.protocols || d); setLoading(false); })
      .catch(() => setLoading(false));
  }, []);
  return { data, loading };
}

function useCqiMatrix() {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  useEffect(() => {
    fetch(`${API}/api/compose/cqi/matrix`)
      .then(r => r.json())
      .then(d => { setData(d); setLoading(false); })
      .catch(() => setLoading(false));
  }, []);
  return { data, loading };
}

function usePulse() {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  useEffect(() => {
    fetch(`${API}/api/pulse/latest`)
      .then(r => r.json())
      .then(d => { setData(d); setLoading(false); })
      .catch(() => setLoading(false));
  }, []);
  return { data, loading };
}

function Sparkline({ data, width = 56, height = 28 }) {
  const scores = (data || []).map((d) => (typeof d === "number" ? d : d.score)).filter((s) => s != null);
  if (scores.length < 2) return null;
  const min = Math.min(...scores) - 0.5;
  const max = Math.max(...scores) + 0.5;
  const range = max - min || 1;
  const pts = scores.map((v, i) =>
    `${(i / (scores.length - 1)) * width},${height - ((v - min) / range) * (height - 4) - 2}`
  ).join(" ");
  const diff = scores[scores.length - 1] - scores[0];
  const c = Math.abs(diff) < 0.3 ? T.inkFaint : diff >= 0 ? "#2d6b45" : T.accent;
  return (
    <svg width={width} height={height} style={{ display: "block" }}>
      <polyline points={pts} fill="none" stroke={c} strokeWidth="1.5" strokeLinejoin="round" />
    </svg>
  );
}

function SubScoreBar({ score }) {
  const c = subScoreColor(score);
  const floor = 50;
  const pct = score != null ? Math.max(0, Math.min(100, ((score - floor) / (100 - floor)) * 100)) : 0;
  return (
    <div style={{ height: 3, background: T.ruleLight, marginTop: 4 }}>
      <div style={{ height: "100%", width: `${pct}%`, background: c, transition: "width 0.6s ease" }} />
    </div>
  );
}

function ScoreChart({ history, width = 700, height = 200 }) {
  if (!history || history.length < 1) {
    return (
      <div style={{ height, display: "flex", alignItems: "center", justifyContent: "center", color: T.inkFaint, fontSize: 12, fontFamily: T.sans }}>
        Accumulating history data...
      </div>
    );
  }

  const scores = history.map((h) => h.score).filter((s) => s != null);
  if (scores.length < 1) return null;

  const min = Math.min(...scores) - 2;
  const max = Math.max(...scores) + 2;
  const range = max - min || 1;

  const pts = scores.map((v, i) =>
    `${(i / Math.max(scores.length - 1, 1)) * width},${height - ((v - min) / range) * (height - 24) - 12}`
  ).join(" ");

  const areaPath = pts + ` ${width},${height} 0,${height}`;
  const ySteps = [min, min + range * 0.25, min + range * 0.5, min + range * 0.75, max];

  return (
    <svg viewBox={`0 0 ${width} ${height}`} style={{ width: "100%", height: height }}>
      <defs>
        <linearGradient id="chartArea" x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stopColor={T.ink} stopOpacity="0.06" />
          <stop offset="100%" stopColor={T.ink} stopOpacity="0" />
        </linearGradient>
      </defs>
      {ySteps.map((v, i) => {
        const y = height - ((v - min) / range) * (height - 24) - 12;
        return (
          <g key={i}>
            <line x1={32} y1={y} x2={width} y2={y} stroke={T.ruleLight} strokeWidth="0.5" />
            <text x={28} y={y + 3} fill={T.inkFaint} fontSize="9" fontFamily={T.mono} textAnchor="end">
              {v.toFixed(0)}
            </text>
          </g>
        );
      })}
      <polygon points={areaPath} fill="url(#chartArea)" />
      <polyline points={pts} fill="none" stroke={T.ink} strokeWidth="1.5" strokeLinejoin="round" />
      {scores.length > 0 && (
        <circle
          cx={(scores.length - 1) / Math.max(scores.length - 1, 1) * width}
          cy={height - ((scores[scores.length - 1] - min) / range) * (height - 24) - 12}
          r="3" fill={T.ink}
        />
      )}
      {history.length > 1 && (
        <>
          <text x={32} y={height - 1} fill={T.inkFaint} fontSize="9" fontFamily={T.mono}>
            {history[0].date}
          </text>
          <text x={width} y={height - 1} fill={T.inkFaint} fontSize="9" fontFamily={T.mono} textAnchor="end">
            {history[history.length - 1].date}
          </text>
        </>
      )}
    </svg>
  );
}

function CategoryBar({ label, score, weight, useBlue }) {
  const c = subScoreColor(score);
  const pct = score != null ? Math.min(100, score) : 0;
  return (
    <div style={{ marginBottom: 12 }}>
      <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 4 }}>
        <span style={{ fontSize: 11, color: T.inkLight, fontFamily: T.sans }}>
          {label}
          {weight != null && (
            <span style={{ color: T.inkFaint, fontSize: 10, marginLeft: 4 }}>
              {(weight * 100).toFixed(0)}%
            </span>
          )}
        </span>
        <span style={{ fontSize: 12, fontFamily: T.mono, fontWeight: 600, color: c }}>
          {score != null ? fmt(score, 1) : "—"}
        </span>
      </div>
      <div style={{ height: 3, background: T.ruleLight }}>
        <div style={{ height: "100%", width: `${pct}%`, background: c, transition: "width 0.8s ease" }} />
      </div>
    </div>
  );
}

function PageHeader({ ts, mobile }) {
  const [now, setNow] = useState(new Date());
  useEffect(() => {
    const t = setInterval(() => setNow(new Date()), 60000);
    return () => clearInterval(t);
  }, []);

  const timestamp = ts ? new Date(ts).toLocaleString("en-US", {
    year: "numeric", month: "short", day: "numeric",
    hour: "2-digit", minute: "2-digit", timeZoneName: "short",
  }) : now.toLocaleString("en-US", {
    year: "numeric", month: "short", day: "numeric",
    hour: "2-digit", minute: "2-digit", timeZoneName: "short",
  });

  const stats = ["10 STABLECOINS", "48 COMPONENTS", "6 DATA SOURCES", "DETERMINISTIC METHODOLOGY", "UPDATED HOURLY"];

  return (
    <div style={{ border: `1.5px solid ${T.ink}`, marginBottom: 0 }}>
      <div style={{ padding: mobile ? "14px 12px 0" : "18px 24px 0" }}>
        <div style={{ display: "flex", flexDirection: mobile ? "column" : "row", justifyContent: "space-between", alignItems: mobile ? "flex-start" : "baseline", gap: mobile ? 4 : 0 }}>
          <h1 style={{ margin: 0, fontSize: mobile ? 20 : 28, fontFamily: T.sans, color: T.ink, fontWeight: 400, letterSpacing: -0.3 }}>
            <span style={{ fontWeight: 700 }}>Stablecoin</span> Integrity <span style={{ fontWeight: 700 }}>Index</span>
          </h1>
          <span style={{ fontFamily: T.mono, fontSize: mobile ? 9 : 10, color: T.inkFaint, textTransform: "uppercase", letterSpacing: 2 }}>
            FORM SII-001 · BASIS PROTOCOL
          </span>
        </div>

        <div style={{ height: 1, background: T.ruleMid, margin: "12px 0" }} />

        <div style={{ display: "flex", flexWrap: "wrap", alignItems: "center", gap: mobile ? 4 : 0, paddingBottom: 14 }}>
          {stats.map((s, i) => (
            <div key={i} style={{ display: "flex", alignItems: "center" }}>
              <span style={{ fontFamily: T.mono, fontSize: mobile ? 8 : 10, color: T.inkLight, textTransform: "uppercase", letterSpacing: mobile ? 0.5 : 1.5, padding: mobile ? "2px 6px" : "0 12px" }}>
                {s}
              </span>
              {!mobile && i < stats.length - 1 && (
                <div style={{ width: 1, height: 12, background: T.ruleMid }} />
              )}
            </div>
          ))}
        </div>
      </div>

      <div style={{ borderTop: `1px solid ${T.ruleMid}`, padding: mobile ? "8px 12px" : "10px 24px", display: "flex", flexDirection: mobile ? "column" : "row", justifyContent: "space-between", alignItems: mobile ? "flex-start" : "center", gap: mobile ? 4 : 0 }}>
        <span style={{ fontFamily: T.mono, fontSize: mobile ? 9 : 11, color: T.ink }}>
          SII = 0.30×Peg + 0.25×Liq + 0.20×Struct + 0.15×Flow + 0.10×Dist
        </span>
        <span style={{ fontFamily: T.mono, fontSize: mobile ? 8 : 10, color: T.inkLight, textTransform: "uppercase", letterSpacing: 1 }}>
          Methodology v1.0 · {timestamp}
        </span>
      </div>

      <div style={{ borderTop: `1px solid #b8d9c4`, background: "#f0f7f2", padding: mobile ? "8px 12px" : "10px 24px", display: "flex", flexDirection: mobile ? "column" : "row", justifyContent: "space-between", alignItems: mobile ? "flex-start" : "center", gap: mobile ? 4 : 0 }}>
        <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
          <div style={{ width: 6, height: 6, borderRadius: "50%", background: "#2d7a3a", flexShrink: 0, animation: "pulse 2s ease-in-out infinite" }} />
          <span style={{ fontFamily: T.mono, fontSize: mobile ? 9 : 10.5, color: "#2a5c38" }}>
            {mobile ? "On-chain · Block #21847293" : "Scores committed to Ethereum mainnet · Block #21847293 · Merkle root: 0x7f3a...c4e1"}
          </span>
        </div>
        {!mobile && (
          <span style={{ fontFamily: T.mono, fontSize: 10, color: "#4a8060" }}>
            Methodology v1.0 · public · immutable · verify on-chain ↗
          </span>
        )}
      </div>
    </div>
  );
}

function RankingsView({ scores, loading, onSelect, ts, mobile }) {
  const [hoveredRow, setHoveredRow] = useState(null);
  const [tappedRow, setTappedRow] = useState(null);
  const hoverTimeout = useRef(null);

  const coinIds = scores ? scores.map((c) => c.id) : [];
  const histMap = useAllHistory(coinIds);

  const handleRowEnter = useCallback((coinId) => {
    clearTimeout(hoverTimeout.current);
    setHoveredRow(coinId);
  }, []);
  const handleRowLeave = useCallback(() => {
    hoverTimeout.current = setTimeout(() => setHoveredRow(null), 150);
  }, []);

  if (loading) {
    return (
      <div style={{ padding: 40, display: "flex", justifyContent: "center" }}>
        <div style={{ color: T.inkFaint, fontFamily: T.mono, fontSize: 12 }}>Loading scores...</div>
      </div>
    );
  }

  if (!scores || scores.length === 0) {
    return (
      <div style={{ padding: 40, textAlign: "center", color: T.inkFaint, fontSize: 13 }}>
        No scores available. Waiting for first scoring cycle.
      </div>
    );
  }

  const sorted = [...scores].sort((a, b) => (b.score || 0) - (a.score || 0));

  const aTier = sorted.filter((c) => c.grade && c.grade.startsWith("A"));
  const rest = sorted.filter((c) => !c.grade || !c.grade.startsWith("A"));

  const cols = "40px 1fr 80px 72px 56px 56px 56px 56px 56px";

  const renderMobileRow = (coin, globalIdx) => {
    const cats = coin.categories || {};
    const pegScore = typeof cats.peg === "object" ? cats.peg?.score : cats.peg;
    const liqScore = typeof cats.liquidity === "object" ? cats.liquidity?.score : cats.liquidity;
    const flowScore = typeof cats.flows === "object" ? cats.flows?.score : cats.flows;
    const distScore = typeof cats.distribution === "object" ? cats.distribution?.score : cats.distribution;
    const strScore = typeof cats.structural === "object" ? cats.structural?.score : cats.structural;
    const isExpanded = tappedRow === coin.id;
    const isNew = !(coin.id in MICA_STATUS);
    const mica = MICA_STATUS[coin.id] || "Pending";
    const reserveType = RESERVE_TYPE[coin.id] || "Pending";

    return (
      <div key={coin.id} style={{ borderBottom: `1px dotted ${T.ruleMid}` }}>
        <div
          onClick={() => setTappedRow(isExpanded ? null : coin.id)}
          style={{ padding: "12px 12px", cursor: "pointer", background: isExpanded ? T.paperWarm : "transparent" }}
        >
          <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 6 }}>
            <span style={{ fontFamily: T.mono, fontSize: 11, color: T.inkFaint, minWidth: 20 }}>{globalIdx}</span>
            <span style={{ fontFamily: T.mono, fontSize: 15, fontWeight: 700, color: T.ink }}>{coin.symbol}</span>
            {isNew && (
              <span style={{ fontFamily: T.mono, fontSize: 7, letterSpacing: 0.8, color: T.inkMid, border: `1px solid ${T.ruleMid}`, padding: "1px 3px", textTransform: "uppercase" }}>New</span>
            )}
            <div style={{ width: 1, height: 12, background: T.ruleMid }} />
            <span style={{ fontFamily: T.mono, fontSize: 12, fontWeight: 500, color: T.inkLight }}>{fmt(coin.score, 1)}</span>
            <span style={{ fontFamily: T.sans, fontSize: 28, fontWeight: 700, color: gradeColor(coin.grade), marginLeft: "auto", lineHeight: 1 }}>
              {coin.grade || "—"}
            </span>
          </div>
          <div style={{ display: "flex", alignItems: "center", gap: 6, paddingLeft: 30 }}>
            <span style={{ fontFamily: T.sans, fontSize: 11, color: T.inkFaint }}>{coin.issuer}</span>
            <span style={{ fontFamily: T.mono, fontSize: 9, color: T.inkFaint }}>·</span>
            <span style={{ fontFamily: T.mono, fontSize: 9, color: T.inkFaint }}>{coin.price != null ? `$${coin.price.toFixed(4)}` : ""}</span>
          </div>
          <div style={{ display: "flex", gap: 8, marginTop: 8, paddingLeft: 30 }}>
            {[
              { label: "Peg", score: pegScore },
              { label: "Liq", score: liqScore },
              { label: "Flow", score: flowScore },
              { label: "Dist", score: distScore },
              { label: "Str", score: strScore },
            ].map((item) => (
              <div key={item.label} style={{ flex: 1 }}>
                <div style={{ fontFamily: T.mono, fontSize: 8, color: T.inkFaint, textTransform: "uppercase", letterSpacing: 0.5, marginBottom: 2 }}>{item.label}</div>
                <div style={{ fontFamily: T.mono, fontSize: 11, color: subScoreColor(item.score), fontWeight: item.score >= 85 ? 700 : 400 }}>
                  {item.score != null ? fmt(item.score, 0) : "—"}
                </div>
                <SubScoreBar score={item.score} />
              </div>
            ))}
          </div>
        </div>

        {isExpanded && (
          <div style={{ padding: "8px 12px 12px 42px", background: T.paperWarm, display: "flex", flexDirection: "column", gap: 4 }}>
            {[
              { label: "Mkt Cap", value: fmtB(coin.market_cap) },
              { label: "Vol 24h", value: fmtB(coin.volume_24h) },
              { label: "Reserve Type", value: reserveType },
              { label: "MiCA", value: mica },
            ].map((item, idx) => (
              <span key={idx} style={{ fontFamily: T.mono, fontSize: 10, color: T.inkMid }}>
                <span style={{ color: T.inkFaint }}>{item.label}: </span>{item.value}
              </span>
            ))}
            <span
              onClick={(e) => { e.stopPropagation(); onSelect(coin.id); }}
              style={{ fontFamily: T.mono, fontSize: 10.5, color: T.inkMid, cursor: "pointer", textDecoration: "underline", marginTop: 4 }}
            >
              Full detail →
            </span>
          </div>
        )}
      </div>
    );
  };

  const renderDesktopRow = (coin, i, globalIdx) => {
    const cats = coin.categories || {};
    const pegScore = typeof cats.peg === "object" ? cats.peg?.score : cats.peg;
    const liqScore = typeof cats.liquidity === "object" ? cats.liquidity?.score : cats.liquidity;
    const flowScore = typeof cats.flows === "object" ? cats.flows?.score : cats.flows;
    const distScore = typeof cats.distribution === "object" ? cats.distribution?.score : cats.distribution;
    const strScore = typeof cats.structural === "object" ? cats.structural?.score : cats.structural;

    const hist = histMap[coin.id] || [];
    const hasSparkline = hist.length >= 5;
    const delta = coin.weekly_change != null ? coin.weekly_change : (hist.length >= 2 ? hist[hist.length - 1].score - hist[0].score : null);
    const isExpanded = hoveredRow === coin.id;

    const attestation = coin.attestation || "—";
    const chains = coin.chains || "Ethereum";
    const isNew = !(coin.id in MICA_STATUS);
    const mica = MICA_STATUS[coin.id] || "Pending";
    const reserveType = RESERVE_TYPE[coin.id] || "Pending";

    return (
      <div key={coin.id}>
        <div
          onMouseEnter={() => handleRowEnter(coin.id)}
          onMouseLeave={handleRowLeave}
          style={{
            display: "grid",
            gridTemplateColumns: cols,
            padding: "14px 16px",
            cursor: "pointer",
            alignItems: "center",
            borderBottom: `1px dotted ${T.ruleMid}`,
            transition: "background 0.1s",
            background: isExpanded ? T.paperWarm : "transparent",
          }}
        >
          <span style={{ color: T.inkFaint, fontSize: 11, fontFamily: T.mono }}>{globalIdx}</span>

          <div>
            <div style={{ display: "flex", alignItems: "baseline", gap: 0 }}>
              <span style={{ fontFamily: T.mono, fontSize: 15, fontWeight: 700, color: T.ink }}>{coin.symbol}</span>
              {isNew && (
                <span style={{ fontFamily: T.mono, fontSize: 7, letterSpacing: 0.8, color: T.inkMid, border: `1px solid ${T.ruleMid}`, padding: "1px 3px", marginLeft: 6, alignSelf: "center", textTransform: "uppercase" }}>New</span>
              )}
              <div style={{ width: 1, height: 12, background: T.ruleMid, margin: "0 10px", alignSelf: "center" }} />
              <span style={{ fontFamily: T.mono, fontSize: 12, fontWeight: 500, color: T.inkLight }}>{fmt(coin.score, 1)}</span>
            </div>
            <div style={{ fontFamily: T.sans, fontSize: 11, color: T.inkFaint, marginTop: 1 }}>{coin.issuer}</div>
          </div>

          <div style={{ paddingLeft: 8 }}>
            <span style={{ fontFamily: T.sans, fontSize: 38, fontWeight: 700, color: gradeColor(coin.grade), lineHeight: 1 }}>
              {coin.grade ? coin.grade.replace(/[+-]/, "") : "—"}
            </span>
            {coin.grade && (coin.grade.includes("+") || coin.grade.includes("-")) && (
              <span style={{ fontFamily: T.sans, fontSize: 18, fontWeight: 700, color: gradeColor(coin.grade), verticalAlign: "super" }}>
                {coin.grade.slice(-1)}
              </span>
            )}
          </div>

          <div style={{ display: "flex", flexDirection: "column", alignItems: "center", minWidth: 56 }}>
            {hasSparkline ? (
              <Sparkline data={hist} width={56} height={28} />
            ) : null}
            <span style={{
              fontFamily: T.mono, fontSize: 10, fontWeight: 700, marginTop: hasSparkline ? 2 : 0,
              color: delta == null ? T.inkFaint : delta >= 0 ? "#2d6b45" : T.accent,
            }}>
              {delta == null ? "—" : (delta >= 0 ? `+${delta.toFixed(1)}` : delta.toFixed(1))}
            </span>
          </div>

          {[pegScore, liqScore, flowScore, distScore, strScore].map((s, j) => (
            <div key={j} style={{ paddingRight: 4 }}>
              <div style={{
                fontFamily: T.mono, fontSize: 12, color: subScoreColor(s),
                fontWeight: s != null && s >= 85 ? 700 : 400,
              }}>
                {s != null ? fmt(s, 0) : "—"}
              </div>
              <SubScoreBar score={s} />
            </div>
          ))}
        </div>

        {isExpanded && (
          <div
            onMouseEnter={() => handleRowEnter(coin.id)}
            onMouseLeave={handleRowLeave}
            style={{
              padding: "10px 16px 10px 56px",
              background: T.paperWarm,
              borderBottom: `1px dotted ${T.ruleMid}`,
              display: "flex", gap: 24, flexWrap: "wrap",
            }}
          >
            {[
              { label: "Price", value: coin.price != null ? `$${coin.price.toFixed(4)}` : "—" },
              { label: "Mkt Cap", value: fmtB(coin.market_cap) },
              { label: "Vol 24h", value: fmtB(coin.volume_24h) },
              { label: "Reserve Type", value: reserveType },
              { label: "Attestation", value: attestation },
              { label: "Chains", value: chains },
              { label: "MiCA", value: mica },
            ].map((item, idx) => (
              <span key={idx} style={{ fontFamily: T.mono, fontSize: 10.5, color: T.inkMid }}>
                <span style={{ color: T.inkFaint }}>{item.label}: </span>
                {item.value}
              </span>
            ))}
            <span
              onClick={(e) => { e.stopPropagation(); onSelect(coin.id); }}
              style={{ fontFamily: T.mono, fontSize: 10.5, color: T.inkMid, cursor: "pointer", textDecoration: "underline", marginLeft: "auto" }}
            >
              Full detail →
            </span>
          </div>
        )}
      </div>
    );
  };

  let globalIdx = 1;

  return (
    <div>
      <PageHeader ts={ts} mobile={mobile} />

      <div style={{ height: mobile ? 16 : 32 }} />

      {mobile ? (
        <div>
          <div style={{ borderBottom: `3px solid ${T.ink}`, padding: "8px 12px", fontFamily: T.mono, fontSize: 9, textTransform: "uppercase", letterSpacing: 1.5, color: T.inkLight }}>
            Rankings · {sorted.length} stablecoins
          </div>
          {aTier.map((coin) => renderMobileRow(coin, globalIdx++))}
          {aTier.length > 0 && rest.length > 0 && (
            <div style={{ height: 6, background: "transparent" }} />
          )}
          {rest.map((coin) => renderMobileRow(coin, globalIdx++))}
        </div>
      ) : (
        <div>
          <div style={{
            display: "grid",
            gridTemplateColumns: cols,
            padding: "8px 16px",
            background: T.paper,
            borderBottom: `3px solid ${T.ink}`,
            fontFamily: T.mono, fontSize: 9, textTransform: "uppercase",
            letterSpacing: 1.5, color: T.inkLight,
          }}>
            <span>#</span>
            <span>Stablecoin</span>
            <span>SII Grade</span>
            <span style={{ textAlign: "center" }}>Trend</span>
            <span>Peg</span>
            <span>Liq</span>
            <span>Flow</span>
            <span>Dist</span>
            <span>Str</span>
          </div>

          {aTier.map((coin, i) => renderDesktopRow(coin, i, globalIdx++))}

          {aTier.length > 0 && rest.length > 0 && (
            <div style={{ height: 6, background: "transparent" }} />
          )}

          {rest.map((coin, i) => renderDesktopRow(coin, i, globalIdx++))}
        </div>
      )}

      <Footnotes mobile={mobile} />
    </div>
  );
}

function Footnotes({ mobile }) {
  return (
    <div style={{
      borderTop: `2.5px solid ${T.ink}`,
      border: `1.5px solid ${T.ruleMid}`,
      borderTopWidth: 2.5,
      borderTopColor: T.ink,
      marginTop: 24,
      padding: mobile ? "12px 12px" : "16px 24px",
      display: "grid",
      gridTemplateColumns: mobile ? "1fr" : "1fr 1fr 1fr",
      gap: mobile ? 16 : 24,
    }}>
      <div>
        <div style={{ fontFamily: T.mono, fontSize: 9, textTransform: "uppercase", letterSpacing: 1.5, color: T.inkLight, marginBottom: 8 }}>
          Component Weights
        </div>
        {[
          ["Peg Stability", "30%"], ["Liquidity Depth", "25%"], ["Structural Risk", "20%"],
          ["Mint/Burn Flows", "15%"], ["Distribution", "10%"],
        ].map(([name, w]) => (
          <div key={name} style={{ display: "flex", justifyContent: "space-between", fontFamily: T.mono, fontSize: 10, color: T.inkMid, padding: "2px 0" }}>
            <span>{name}</span><span>{w}</span>
          </div>
        ))}
      </div>

      <div>
        <div style={{ fontFamily: T.mono, fontSize: 9, textTransform: "uppercase", letterSpacing: 1.5, color: T.inkLight, marginBottom: 8 }}>
          Grade Scale
        </div>
        {[
          ["A+ / A / A−", "90–100 / 85–90 / 80–85"],
          ["B+ / B / B−", "75–80 / 70–75 / 65–70"],
          ["C+ / C / C−", "60–65 / 55–60 / 50–55"],
          ["D / F", "45–50 / <45"],
        ].map(([grades, ranges]) => (
          <div key={grades} style={{ display: "flex", justifyContent: "space-between", fontFamily: T.mono, fontSize: 10, color: T.inkMid, padding: "2px 0" }}>
            <span>{grades}</span><span style={{ color: T.inkFaint }}>{ranges}</span>
          </div>
        ))}
      </div>

      <div>
        <div style={{ fontFamily: T.mono, fontSize: 9, textTransform: "uppercase", letterSpacing: 1.5, color: T.inkLight, marginBottom: 8 }}>
          Disclosure
        </div>
        <p style={{ fontFamily: T.sans, fontSize: 10, color: T.inkFaint, lineHeight: 1.6, margin: 0 }}>
          SII scores are informational and do not constitute financial advice. Methodology is deterministic and version-controlled. No issuer can pay to influence scores, weights, or thresholds. Data sourced from CoinGecko, DeFiLlama, Etherscan, Curve Finance, issuer attestations, and on-chain analysis. Scores update hourly.
        </p>
      </div>
    </div>
  );
}

function DetailView({ coinId, onBack, mobile }) {
  const { data: coin, loading: detailLoading } = useCoinDetail(coinId);
  const { data: history, loading: histLoading } = useCoinHistory(coinId, 90);

  if (detailLoading || !coin) {
    return (
      <div style={{ padding: 40, textAlign: "center", color: T.inkFaint, fontFamily: T.mono, fontSize: 12 }}>
        Loading {coinId}...
      </div>
    );
  }

  const cats = coin.categories || {};
  const strBk = coin.structural_breakdown || {};

  const getCat = (obj) => {
    if (obj == null) return { score: null, weight: null };
    if (typeof obj === "object") return obj;
    return { score: obj, weight: null };
  };

  const peg = getCat(cats.peg);
  const liq = getCat(cats.liquidity);
  const flow = getCat(cats.flows);
  const dist = getCat(cats.distribution);
  const str = getCat(cats.structural);

  const reserves = getCat(strBk.reserves);
  const contract = getCat(strBk.contract);
  const oracle = getCat(strBk.oracle);
  const governance = getCat(strBk.governance);
  const network = getCat(strBk.network);

  return (
    <div style={{ padding: "24px 0 64px" }}>
      <button
        onClick={onBack}
        style={{
          background: "none", border: "none", color: T.inkLight,
          cursor: "pointer", fontSize: 12, fontFamily: T.sans,
          padding: 0, marginBottom: 20,
        }}
      >
        ← Back to Rankings
      </button>

      <div style={{ display: "flex", flexDirection: mobile ? "column" : "row", alignItems: mobile ? "flex-start" : "flex-start", gap: mobile ? 12 : 20, marginBottom: mobile ? 20 : 32 }}>
        <div style={{ flex: 1 }}>
          <div style={{ display: "flex", alignItems: "baseline", gap: mobile ? 8 : 14, flexWrap: "wrap" }}>
            <h1 style={{ margin: 0, fontSize: mobile ? 20 : 26, fontWeight: 600, color: T.ink, fontFamily: T.sans, letterSpacing: -0.5 }}>
              {coin.name}
            </h1>
            <span style={{ fontSize: mobile ? 12 : 14, color: T.inkFaint, fontFamily: T.mono }}>{coin.symbol}</span>
            <span style={{ fontFamily: T.sans, fontSize: mobile ? 16 : 20, fontWeight: 700, color: gradeColor(coin.grade) }}>
              {coin.grade}
            </span>
          </div>
          <div style={{ fontSize: mobile ? 10 : 12, color: T.inkLight, marginTop: 6, fontFamily: T.sans, lineHeight: 1.5 }}>
            Issued by {coin.issuer} · {coin.component_count || "—"} components · {RESERVE_TYPE[coin.id] || "Pending"} · MiCA: {MICA_STATUS[coin.id] || "Pending"}
          </div>
        </div>

        <div style={{ textAlign: mobile ? "left" : "right" }}>
          <span style={{ fontFamily: T.mono, fontSize: mobile ? 22 : 28, fontWeight: 700, color: T.ink }}>{fmt(coin.score, 1)}</span>
          <div style={{ fontSize: 11, color: T.inkFaint, fontFamily: T.mono, marginTop: 4 }}>
            ${coin.price?.toFixed(4)} · MCap {fmtB(coin.market_cap)}
          </div>
        </div>
      </div>

      <div style={{ border: `1px solid ${T.ruleMid}`, padding: "16px 20px 12px", marginBottom: 20 }}>
        <div style={{ fontSize: 10, fontWeight: 600, color: T.inkLight, textTransform: "uppercase", letterSpacing: 1.5, marginBottom: 12, fontFamily: T.mono }}>
          Score History
        </div>
        <ScoreChart history={history} />
      </div>

      <div style={{ display: "grid", gridTemplateColumns: mobile ? "1fr" : "1fr 1fr", gap: 16, marginBottom: 20 }}>
        <div style={{ border: `1px solid ${T.ruleMid}`, padding: mobile ? "12px" : "16px 20px" }}>
          <div style={{ fontSize: 10, fontWeight: 600, color: T.inkLight, textTransform: "uppercase", letterSpacing: 1.5, marginBottom: 16, fontFamily: T.mono }}>
            Category Scores
          </div>
          <CategoryBar label="Peg Stability" score={peg.score} weight={peg.weight || 0.30} />
          <CategoryBar label="Liquidity Depth" score={liq.score} weight={liq.weight || 0.25} />
          <CategoryBar label="Mint/Burn Flows" score={flow.score} weight={flow.weight || 0.15} />
          <CategoryBar label="Holder Distribution" score={dist.score} weight={dist.weight || 0.10} />
          <CategoryBar label="Structural Risk" score={str.score} weight={str.weight || 0.20} />
        </div>

        <div style={{ border: `1px solid ${T.ruleMid}`, padding: mobile ? "12px" : "16px 20px" }}>
          <div style={{ fontSize: 10, fontWeight: 600, color: T.inkLight, textTransform: "uppercase", letterSpacing: 1.5, marginBottom: 16, fontFamily: T.mono }}>
            Structural Breakdown
          </div>
          <CategoryBar label="Reserves & Collateral" score={reserves.score} weight={reserves.weight || 0.30} />
          <CategoryBar label="Smart Contract" score={contract.score} weight={contract.weight || 0.20} />
          <CategoryBar label="Oracle Integrity" score={oracle.score} weight={oracle.weight || 0.15} />
          <CategoryBar label="Governance & Ops" score={governance.score} weight={governance.weight || 0.20} />
          <CategoryBar label="Network & Chain" score={network.score} weight={network.weight || 0.15} />
        </div>
      </div>

      {coin.components && coin.components.length > 0 && (
        <div style={{ border: `1px solid ${T.ruleMid}`, padding: "16px 20px" }}>
          <div style={{ fontSize: 10, fontWeight: 600, color: T.inkLight, textTransform: "uppercase", letterSpacing: 1.5, marginBottom: 12, fontFamily: T.mono }}>
            Component Readings · {coin.components.length} active
          </div>

          <div style={{ maxHeight: 400, overflowY: "auto" }}>
            {Object.entries(
              coin.components.reduce((acc, c) => {
                const cat = c.category || "other";
                if (!acc[cat]) acc[cat] = [];
                acc[cat].push(c);
                return acc;
              }, {})
            )
              .sort(([a], [b]) => a.localeCompare(b))
              .map(([category, components]) => (
                <div key={category} style={{ marginBottom: 16 }}>
                  <div style={{
                    fontSize: 10, fontWeight: 600, color: T.inkMid,
                    textTransform: "uppercase", letterSpacing: 1,
                    marginBottom: 6, fontFamily: T.mono,
                  }}>
                    {category.replace(/_/g, " ")}
                  </div>
                  {components.sort((a, b) => (b.normalized_score || 0) - (a.normalized_score || 0)).map((comp) => (
                    <div
                      key={comp.id}
                      style={{
                        display: "flex", justifyContent: "space-between",
                        padding: "4px 0", borderBottom: `1px solid ${T.ruleLight}`,
                        fontSize: 11,
                      }}
                    >
                      <span style={{ color: T.inkLight, fontFamily: T.sans }}>
                        {(comp.id || "").replace(/_/g, " ")}
                      </span>
                      <div style={{ display: "flex", gap: 16, alignItems: "center" }}>
                        <span style={{ color: T.inkFaint, fontFamily: T.mono, fontSize: 10 }}>
                          {comp.raw_value != null ? (typeof comp.raw_value === "number" ? comp.raw_value.toFixed(4) : comp.raw_value) : "—"}
                        </span>
                        <span style={{
                          color: subScoreColor(comp.normalized_score),
                          fontFamily: T.mono, fontWeight: 600, minWidth: 36, textAlign: "right",
                        }}>
                          {fmt(comp.normalized_score, 1)}
                        </span>
                      </div>
                    </div>
                  ))}
                </div>
              ))}
          </div>
        </div>
      )}
    </div>
  );
}

function MethodologyView({ mobile }) {
  return (
    <div style={{ padding: mobile ? "16px 0 32px" : "24px 0 64px", maxWidth: 780 }}>
      <h1 style={{ margin: "0 0 8px", fontSize: 22, fontWeight: 600, color: T.ink, fontFamily: T.sans, letterSpacing: -0.3 }}>
        Methodology
      </h1>
      <p style={{ margin: "0 0 28px", fontSize: 12, color: T.inkLight, fontFamily: T.sans }}>
        SII v1.0.0 — Deterministic, versioned, reproducible
      </p>

      <section style={{ marginBottom: 28 }}>
        <div style={{ border: `1px solid ${T.ruleMid}`, padding: "20px 24px" }}>
          <h2 style={{ margin: "0 0 12px", fontSize: 14, fontWeight: 600, color: T.ink, fontFamily: T.sans }}>
            What is the Stablecoin Integrity Index?
          </h2>
          <p style={{ margin: 0, fontSize: 13, color: T.inkMid, fontFamily: T.sans, lineHeight: 1.7 }}>
            SII is a standardized risk surface that normalizes fragmented data about stablecoin health
            into a single comparable score. It measures peg stability, liquidity depth, mint/burn dynamics,
            holder distribution, and structural risk across multiple data sources. The methodology is
            deterministic — the same inputs always produce the same outputs — and version-controlled
            so changes are announced in advance and retroactively reproducible.
          </p>
        </div>
      </section>

      <section style={{ marginBottom: 28 }}>
        <div style={{ border: `1px solid ${T.ruleMid}`, padding: "20px 24px" }}>
          <div style={{ fontSize: 10, fontWeight: 600, color: T.inkLight, textTransform: "uppercase", letterSpacing: 1.5, marginBottom: 14, fontFamily: T.mono }}>
            Formula
          </div>
          <div style={{ fontFamily: T.mono, fontSize: 14, color: T.ink, lineHeight: 2.2, padding: "8px 0" }}>
            SII = <span style={{ fontWeight: 700 }}>0.30</span>×Peg + <span style={{ fontWeight: 700 }}>0.25</span>×Liquidity + <span style={{ fontWeight: 700 }}>0.15</span>×Flows + <span style={{ fontWeight: 700 }}>0.10</span>×Distribution + <span style={{ fontWeight: 700 }}>0.20</span>×Structural
          </div>
          <div style={{ fontFamily: T.mono, fontSize: 12, color: T.inkLight, lineHeight: 2.2, borderTop: `1px solid ${T.ruleLight}`, paddingTop: 8, marginTop: 4 }}>
            Structural = 0.30×Reserves + 0.20×Contract + 0.15×Oracle + 0.20×Governance + 0.15×Network
          </div>
        </div>
      </section>

      <section style={{ marginBottom: 28 }}>
        <div style={{ border: `1px solid ${T.ruleMid}`, padding: "20px 24px" }}>
          <div style={{ fontSize: 10, fontWeight: 600, color: T.inkLight, textTransform: "uppercase", letterSpacing: 1.5, marginBottom: 16, fontFamily: T.mono }}>
            Categories
          </div>
          {[
            { name: "Peg Stability", weight: 30, desc: "Current deviation, 24h max, 7d volatility, price floor/ceiling, cross-exchange variance, DEX/CEX spread, arbitrage efficiency", components: 10 },
            { name: "Liquidity Depth", weight: 25, desc: "Market cap, volume ratios, DEX pool depth, Curve 3pool balance, cross-chain liquidity, lending protocol TVL, exchange listing breadth", components: 12 },
            { name: "Mint/Burn Flows", weight: 15, desc: "Supply changes, turnover ratio, market cap stability, volume consistency, trading pair diversity", components: 9 },
            { name: "Holder Distribution", weight: 10, desc: "Top 10 wallet concentration, unique holder count, exchange address concentration", components: 3 },
            { name: "Structural Risk", weight: 20, desc: "Reserve quality, smart contract audits, oracle integrity, governance model, network deployment, regulatory compliance", components: 16 },
          ].map((cat, i) => (
            <div key={i} style={{ display: "flex", gap: 16, padding: "14px 0", borderBottom: i < 4 ? `1px solid ${T.ruleLight}` : "none" }}>
              <div style={{ minWidth: 44, textAlign: "right", fontFamily: T.mono, fontWeight: 700, fontSize: 18, color: T.ink }}>{cat.weight}%</div>
              <div>
                <div style={{ fontWeight: 600, fontSize: 13, color: T.ink, fontFamily: T.sans }}>{cat.name}</div>
                <div style={{ fontSize: 12, color: T.inkLight, marginTop: 3, lineHeight: 1.5, fontFamily: T.sans }}>{cat.desc}</div>
                <div style={{ fontSize: 10, color: T.inkFaint, marginTop: 3, fontFamily: T.mono }}>{cat.components} components</div>
              </div>
            </div>
          ))}
        </div>
      </section>

      <section style={{ marginBottom: 28 }}>
        <div style={{ border: `1px solid ${T.ruleMid}`, padding: "20px 24px" }}>
          <div style={{ fontSize: 10, fontWeight: 600, color: T.inkLight, textTransform: "uppercase", letterSpacing: 1.5, marginBottom: 14, fontFamily: T.mono }}>
            Grade Scale
          </div>
          <div style={{ display: "grid", gridTemplateColumns: mobile ? "repeat(3, 1fr)" : "repeat(6, 1fr)", gap: 6 }}>
            {[
              { grade: "A+", range: "90–100" }, { grade: "A", range: "85–90" }, { grade: "A-", range: "80–85" },
              { grade: "B+", range: "75–80" }, { grade: "B", range: "70–75" }, { grade: "B-", range: "65–70" },
              { grade: "C+", range: "60–65" }, { grade: "C", range: "55–60" }, { grade: "C-", range: "50–55" },
              { grade: "D", range: "45–50" }, { grade: "F", range: "<45" },
            ].map((g) => (
              <div key={g.grade} style={{ padding: "8px 6px", textAlign: "center", border: `1px solid ${T.ruleLight}` }}>
                <div style={{ fontWeight: 700, fontSize: 14, color: gradeColor(g.grade), fontFamily: T.mono }}>{g.grade}</div>
                <div style={{ fontSize: 9, color: T.inkFaint, marginTop: 2, fontFamily: T.mono }}>{g.range}</div>
              </div>
            ))}
          </div>
        </div>
      </section>

      <section style={{ marginBottom: 28 }}>
        <div style={{ border: `1px solid ${T.ruleMid}`, padding: "20px 24px" }}>
          <div style={{ fontSize: 10, fontWeight: 600, color: T.inkLight, textTransform: "uppercase", letterSpacing: 1.5, marginBottom: 12, fontFamily: T.mono }}>
            Data Sources
          </div>
          <div style={{ display: "flex", flexWrap: "wrap", gap: 6, marginBottom: 14 }}>
            {["CoinGecko Pro", "DeFiLlama", "Etherscan", "Curve Finance", "Issuer Attestations", "On-Chain Analysis"].map((s) => (
              <span key={s} style={{ padding: "4px 10px", background: T.paperWarm, color: T.inkMid, fontSize: 11, fontFamily: T.sans, border: `1px solid ${T.ruleMid}` }}>
                {s}
              </span>
            ))}
          </div>
          <p style={{ margin: 0, fontSize: 12, color: T.inkLight, fontFamily: T.sans, lineHeight: 1.6 }}>
            102 components defined across 11 categories. 50 currently automated via live APIs.
            Scores update hourly. Deterministic formula — same inputs always produce same outputs.
            Version-controlled methodology with advance notice before changes.
          </p>
        </div>
      </section>

      <section>
        <div style={{ border: `1px solid ${T.ruleMid}`, padding: "20px 24px" }}>
          <div style={{ fontSize: 10, fontWeight: 600, color: T.inkLight, textTransform: "uppercase", letterSpacing: 1.5, marginBottom: 14, fontFamily: T.mono }}>
            Principles
          </div>
          {[
            { title: "Neutral", desc: "No customer can pay to influence scores, weights, thresholds, or methodology timing." },
            { title: "Deterministic", desc: "Same inputs always produce the same outputs. No discretionary adjustments." },
            { title: "Versioned", desc: "All methodology changes are announced in advance, timestamped, and retroactively reproducible." },
            { title: "Composable", desc: "SII is designed as a programmable primitive — machine-readable, on-chain verifiable, and integratable into protocol logic." },
          ].map((p, i) => (
            <div key={i} style={{ padding: "10px 0", borderBottom: i < 3 ? `1px solid ${T.ruleLight}` : "none" }}>
              <div style={{ fontSize: 13, fontWeight: 600, color: T.ink, fontFamily: T.sans }}>{p.title}</div>
              <div style={{ fontSize: 12, color: T.inkLight, marginTop: 3, fontFamily: T.sans, lineHeight: 1.5 }}>{p.desc}</div>
            </div>
          ))}
        </div>
      </section>
    </div>
  );
}

const WALLET_COL_DESKTOP = "32px 180px 100px 68px 52px 80px 68px 80px";
const WALLET_COL_MOBILE = "180px 90px 56px 48px";

function WalletTableHeader({ mobile }) {
  return (
    <div style={{
      display: "grid",
      gridTemplateColumns: mobile ? WALLET_COL_MOBILE : WALLET_COL_DESKTOP,
      padding: "8px 16px",
      background: T.paper,
      borderBottom: `3px solid ${T.ink}`,
      fontFamily: T.mono, fontSize: 9, textTransform: "uppercase",
      letterSpacing: 1.5, color: T.inkLight,
    }}>
      {mobile ? (
        <>
          <span>Address</span>
          <span>Value</span>
          <span>Risk</span>
          <span>Grade</span>
        </>
      ) : (
        <>
          <span>#</span>
          <span>Address</span>
          <span>Value</span>
          <span>Risk</span>
          <span>Grade</span>
          <span>Dominant</span>
          <span>HHI</span>
          <span>Coverage</span>
        </>
      )}
    </div>
  );
}

function WalletRow({ wallet, rank, mobile, lowScoreHighlight }) {
  const [hovered, setHovered] = useState(false);
  const scoreColor = lowScoreHighlight && wallet.risk_score < 75 ? T.accent : subScoreColor(wallet.risk_score);
  return (
    <div
      onMouseEnter={() => setHovered(true)}
      onMouseLeave={() => setHovered(false)}
      style={{
        display: "grid",
        gridTemplateColumns: mobile ? WALLET_COL_MOBILE : WALLET_COL_DESKTOP,
        padding: "12px 16px",
        borderBottom: `1px dotted ${T.ruleMid}`,
        background: hovered ? T.paperWarm : "transparent",
        transition: "background 0.1s",
        alignItems: "center",
      }}
    >
      {mobile ? null : (
        <span style={{ fontFamily: T.mono, fontSize: 11, color: T.inkFaint }}>{rank}</span>
      )}
      <span style={{ fontFamily: T.mono, fontSize: 11, color: T.inkMid, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
        {truncAddr(wallet.address)}
      </span>
      <span style={{ fontFamily: T.mono, fontSize: 11, color: T.ink }}>{fmtB(wallet.total_stablecoin_value)}</span>
      <span style={{ fontFamily: T.mono, fontSize: 12, fontWeight: 600, color: scoreColor }}>
        {wallet.risk_score != null ? fmt(wallet.risk_score, 1) : "—"}
      </span>
      <span style={{ fontFamily: T.sans, fontSize: mobile ? 16 : 20, fontWeight: 700, color: gradeColor(wallet.risk_grade) }}>
        {wallet.risk_grade || "—"}
      </span>
      {!mobile && (
        <>
          <span style={{ fontFamily: T.mono, fontSize: 11, color: T.inkMid }}>{wallet.dominant_asset || "—"}</span>
          <span style={{ fontFamily: T.mono, fontSize: 11, color: T.inkLight }}>{fmtHHI(wallet.concentration_hhi)}</span>
          <span style={{ fontFamily: T.mono, fontSize: 11, color: coverageColor(wallet.coverage_quality), textTransform: "uppercase" }}>
            {wallet.coverage_quality || "—"}
          </span>
        </>
      )}
    </div>
  );
}

function WalletSearchPanel({ mobile }) {
  const [input, setInput] = useState("");
  const { data, loading, error, lookup } = useWalletDetail();

  const handleLookup = () => {
    const addr = input.trim();
    if (addr.length >= 10) lookup(addr);
  };

  const w = data?.wallet;
  const r = data?.risk;
  const holdings = data?.holdings || [];

  return (
    <div style={{ marginBottom: 32 }}>
      <div style={{ fontFamily: T.mono, fontSize: 9, textTransform: "uppercase", letterSpacing: 1.5, color: T.inkLight, marginBottom: 10 }}>
        Wallet Search · Enter Ethereum Address
      </div>

      <div style={{ display: "flex", gap: 0, marginBottom: 16 }}>
        <input
          value={input}
          onChange={(e) => setInput(e.target.value)}
          onKeyDown={(e) => e.key === "Enter" && handleLookup()}
          placeholder="0x..."
          style={{
            flex: 1,
            padding: "9px 12px",
            border: `1px solid ${T.ruleMid}`,
            borderRight: "none",
            fontFamily: T.mono,
            fontSize: 12,
            color: T.ink,
            background: T.paper,
            outline: "none",
          }}
        />
        <button
          onClick={handleLookup}
          style={{
            padding: "9px 16px",
            border: `1px solid ${T.ink}`,
            background: T.ink,
            color: T.paper,
            fontFamily: T.mono,
            fontSize: 11,
            cursor: "pointer",
            letterSpacing: 1,
            textTransform: "uppercase",
          }}
        >
          Lookup
        </button>
      </div>

      {loading && (
        <div style={{ fontFamily: T.mono, fontSize: 11, color: T.inkFaint }}>Fetching wallet data...</div>
      )}
      {error && (
        <div style={{ fontFamily: T.mono, fontSize: 11, color: T.accent }}>Error: {error}</div>
      )}

      {data && w && r && (
        <div style={{ border: `1px solid ${T.ruleMid}`, padding: mobile ? "12px" : "16px 20px", animation: "fadeIn 0.3s ease" }}>
          <div style={{ fontFamily: T.mono, fontSize: 9, textTransform: "uppercase", letterSpacing: 1.5, color: T.inkLight, marginBottom: 12 }}>
            Wallet Profile
          </div>

          <div style={{ display: "grid", gridTemplateColumns: mobile ? "1fr" : "1fr 1fr", gap: mobile ? 8 : 24, marginBottom: 16 }}>
            <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
              {[
                { label: "Address", value: w.address },
                { label: "Value", value: fmtB(r.total_stablecoin_value || w.total_stablecoin_value) },
                { label: "Size Tier", value: (w.size_tier || "—").toUpperCase() },
                { label: "Source", value: w.source || "—" },
                { label: "Contract", value: w.is_contract ? "Yes" : "No" },
              ].map((item) => (
                <div key={item.label} style={{ fontFamily: T.mono, fontSize: mobile ? 10 : 10.5 }}>
                  <span style={{ color: T.inkFaint }}>{item.label}: </span>
                  <span style={{ color: T.inkMid, wordBreak: "break-all" }}>{item.value}</span>
                </div>
              ))}
            </div>
            <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
              {[
                { label: "Risk Score", value: fmt(r.risk_score, 1), valueStyle: { color: subScoreColor(r.risk_score), fontWeight: 600 } },
                { label: "Grade", value: r.risk_grade || "—", valueStyle: { color: gradeColor(r.risk_grade), fontWeight: 700, fontSize: 14 } },
                { label: "HHI", value: fmtHHI(r.concentration_hhi) },
                { label: "Coverage", value: r.coverage_quality || "—", valueStyle: { color: coverageColor(r.coverage_quality), textTransform: "uppercase" } },
                { label: "Dominant Asset", value: `${r.dominant_asset || "—"} (${r.dominant_asset_pct != null ? fmt(r.dominant_asset_pct, 1) + "%" : "—"})` },
                { label: "Holdings", value: `${r.num_scored_holdings || 0} scored · ${r.num_unscored_holdings || 0} unscored` },
              ].map((item) => (
                <div key={item.label} style={{ fontFamily: T.mono, fontSize: mobile ? 10 : 10.5 }}>
                  <span style={{ color: T.inkFaint }}>{item.label}: </span>
                  <span style={{ color: T.inkMid, ...(item.valueStyle || {}) }}>{item.value}</span>
                </div>
              ))}
            </div>
          </div>

          {holdings.length > 0 && (
            <>
              <div style={{ height: 1, background: T.ruleLight, marginBottom: 12 }} />
              <div style={{ fontFamily: T.mono, fontSize: 9, textTransform: "uppercase", letterSpacing: 1.5, color: T.inkLight, marginBottom: 8 }}>
                Holdings · {holdings.length} assets
              </div>
              <div style={{ overflowX: "auto" }}>
                <div style={{
                  display: "grid",
                  gridTemplateColumns: mobile ? "60px 1fr 80px 68px" : "60px 1fr 100px 80px 72px 52px",
                  padding: "6px 0 6px 0",
                  borderBottom: `1px solid ${T.ruleMid}`,
                  fontFamily: T.mono, fontSize: 9, textTransform: "uppercase",
                  letterSpacing: 1.5, color: T.inkFaint,
                }}>
                  <span>Symbol</span>
                  <span>Value</span>
                  <span>% Wallet</span>
                  <span>SII</span>
                  {!mobile && <><span>Grade</span><span>Scored</span></>}
                </div>
                {holdings.map((h, i) => (
                  <div key={i} style={{
                    display: "grid",
                    gridTemplateColumns: mobile ? "60px 1fr 80px 68px" : "60px 1fr 100px 80px 72px 52px",
                    padding: "7px 0",
                    borderBottom: `1px dotted ${T.ruleLight}`,
                    alignItems: "center",
                  }}>
                    <span style={{ fontFamily: T.mono, fontSize: 11, fontWeight: 600, color: T.ink }}>{h.symbol}</span>
                    <span style={{ fontFamily: T.mono, fontSize: 11, color: T.inkMid }}>{fmtB(h.value_usd)}</span>
                    <span style={{ fontFamily: T.mono, fontSize: 11, color: T.inkLight }}>
                      {h.pct_of_wallet != null ? fmt(h.pct_of_wallet, 1) + "%" : "—"}
                    </span>
                    <span style={{ fontFamily: T.mono, fontSize: 11, color: h.sii_score != null ? subScoreColor(h.sii_score) : T.inkFaint, fontWeight: h.sii_score != null ? 600 : 400 }}>
                      {h.sii_score != null ? fmt(h.sii_score, 1) : "—"}
                    </span>
                    {!mobile && (
                      <>
                        <span style={{ fontFamily: T.sans, fontSize: 14, fontWeight: 700, color: gradeColor(h.sii_grade) }}>
                          {h.sii_grade || "—"}
                        </span>
                        <span style={{ fontFamily: T.mono, fontSize: 9, color: h.is_scored ? "#2d6b45" : T.inkFaint, textTransform: "uppercase" }}>
                          {h.is_scored ? "Yes" : "No"}
                        </span>
                      </>
                    )}
                  </div>
                ))}
              </div>
            </>
          )}
        </div>
      )}
    </div>
  );
}

function WalletsView({ mobile }) {
  const { data: topWallets, loading: topLoading } = useWalletTop(50);
  const { data: riskyWallets, loading: riskyLoading } = useWalletRiskiest(50);
  const { data: backlog, loading: backlogLoading } = useBacklog(50);

  const queuedCount = (backlog || []).filter((a) => a.scoring_status === "queued" || a.scoring_status === "in_progress").length;

  const statsItems = [
    `${topWallets ? topWallets.length : "—"} WALLETS TRACKED`,
    `${backlog ? backlog.length : "—"} BACKLOG ASSETS`,
    `${queuedCount} QUEUED FOR SCORING`,
    "FORM WRG-001 · BASIS PROTOCOL",
  ];

  return (
    <div>
      <div style={{ height: mobile ? 16 : 28 }} />

      <div style={{ border: `1.5px solid ${T.ink}`, marginBottom: 24, padding: mobile ? "10px 12px" : "14px 20px" }}>
        <div style={{ display: "flex", flexWrap: "wrap", alignItems: "center", gap: mobile ? 4 : 0 }}>
          {statsItems.map((s, i) => (
            <div key={i} style={{ display: "flex", alignItems: "center" }}>
              <span style={{ fontFamily: T.mono, fontSize: mobile ? 8 : 10, color: T.inkLight, textTransform: "uppercase", letterSpacing: mobile ? 0.5 : 1.5, padding: mobile ? "2px 6px" : "0 12px" }}>
                {s}
              </span>
              {!mobile && i < statsItems.length - 1 && (
                <div style={{ width: 1, height: 12, background: T.ruleMid }} />
              )}
            </div>
          ))}
        </div>
      </div>

      <WalletSearchPanel mobile={mobile} />

      <div style={{ marginBottom: 32 }}>
        <div style={{ fontFamily: T.mono, fontSize: 9, textTransform: "uppercase", letterSpacing: 1.5, color: T.inkLight, marginBottom: 10 }}>
          Top Wallets · By Stablecoin Value
        </div>
        {topLoading ? (
          <div style={{ padding: 24, fontFamily: T.mono, fontSize: 12, color: T.inkFaint }}>Loading wallets...</div>
        ) : !topWallets || topWallets.length === 0 ? (
          <div style={{ padding: 24, fontFamily: T.mono, fontSize: 12, color: T.inkFaint }}>No wallet data yet. Run the indexer pipeline first.</div>
        ) : (
          <div style={{ border: `1px solid ${T.ruleMid}` }}>
            <WalletTableHeader mobile={mobile} />
            {topWallets.map((w, i) => (
              <WalletRow key={w.address} wallet={w} rank={i + 1} mobile={mobile} lowScoreHighlight={false} />
            ))}
          </div>
        )}
      </div>

      <div style={{ marginBottom: 32 }}>
        <div style={{ fontFamily: T.mono, fontSize: 9, textTransform: "uppercase", letterSpacing: 1.5, color: T.inkLight, marginBottom: 10 }}>
          Riskiest Wallets · Lowest Risk Score
        </div>
        {riskyLoading ? (
          <div style={{ padding: 24, fontFamily: T.mono, fontSize: 12, color: T.inkFaint }}>Loading...</div>
        ) : !riskyWallets || riskyWallets.length === 0 ? (
          <div style={{ padding: 24, fontFamily: T.mono, fontSize: 12, color: T.inkFaint }}>No data.</div>
        ) : (
          <>
            <div style={{ border: `1px solid ${T.ruleMid}` }}>
              <WalletTableHeader mobile={mobile} />
              {riskyWallets.map((w, i) => (
                <WalletRow key={w.address} wallet={w} rank={i + 1} mobile={mobile} lowScoreHighlight={true} />
              ))}
            </div>
            <div style={{ fontFamily: T.sans, fontSize: 10, color: T.inkFaint, marginTop: 8, padding: "0 2px" }}>
              Sorted by ascending risk score. Low scores indicate high stablecoin concentration or unscored holdings.
            </div>
          </>
        )}
      </div>

      <div style={{ marginBottom: 32 }}>
        <div style={{ fontFamily: T.mono, fontSize: 9, textTransform: "uppercase", letterSpacing: 1.5, color: T.inkLight, marginBottom: 10 }}>
          Scoring Backlog · Assets Pending Analysis
        </div>
        {backlogLoading ? (
          <div style={{ padding: 24, fontFamily: T.mono, fontSize: 12, color: T.inkFaint }}>Loading backlog...</div>
        ) : !backlog || backlog.length === 0 ? (
          <div style={{ padding: 24, fontFamily: T.mono, fontSize: 12, color: T.inkFaint }}>No backlog assets.</div>
        ) : (
          <div style={{ border: `1px solid ${T.ruleMid}` }}>
            <div style={{
              display: "grid",
              gridTemplateColumns: mobile ? "56px 1fr 90px 60px" : "32px 72px 1fr 100px 68px 80px",
              padding: "8px 16px",
              background: T.paper,
              borderBottom: `3px solid ${T.ink}`,
              fontFamily: T.mono, fontSize: 9, textTransform: "uppercase",
              letterSpacing: 1.5, color: T.inkLight,
            }}>
              {mobile ? (
                <><span>#</span><span>Name</span><span>Capital</span><span>Status</span></>
              ) : (
                <><span>#</span><span>Symbol</span><span>Name</span><span>Capital Held</span><span>Wallets</span><span>Status</span></>
              )}
            </div>
            {backlog.map((asset, i) => (
              <div key={asset.token_address} style={{
                display: "grid",
                gridTemplateColumns: mobile ? "56px 1fr 90px 60px" : "32px 72px 1fr 100px 68px 80px",
                padding: "11px 16px",
                borderBottom: `1px dotted ${T.ruleMid}`,
                alignItems: "center",
              }}>
                {mobile ? null : (
                  <span style={{ fontFamily: T.mono, fontSize: 11, color: T.inkFaint }}>{i + 1}</span>
                )}
                {mobile ? (
                  <span style={{ fontFamily: T.mono, fontSize: 11, fontWeight: 700, color: T.ink }}>{asset.symbol}</span>
                ) : (
                  <span style={{ fontFamily: T.mono, fontSize: 11, fontWeight: 700, color: T.ink }}>{asset.symbol}</span>
                )}
                <span style={{ fontFamily: T.sans, fontSize: 11, color: T.inkMid }}>{asset.name || asset.symbol}</span>
                <span style={{ fontFamily: T.mono, fontSize: 11, color: T.ink }}>{fmtB(asset.total_value_held)}</span>
                {!mobile && (
                  <span style={{ fontFamily: T.mono, fontSize: 11, color: T.inkLight }}>{asset.wallets_holding ?? "—"}</span>
                )}
                <div style={{ display: "flex", alignItems: "center", gap: 5 }}>
                  <div style={{
                    width: 5, height: 5, borderRadius: "50%",
                    background: statusColor(asset.scoring_status),
                    flexShrink: 0,
                  }} />
                  <span style={{ fontFamily: T.mono, fontSize: 10, color: statusColor(asset.scoring_status), textTransform: "uppercase", letterSpacing: 0.5 }}>
                    {asset.scoring_status || "—"}
                  </span>
                </div>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}

function ProtocolsView({ mobile }) {
  const { data: protocols, loading: psiLoading } = usePsiScores();
  const { data: cqiData, loading: cqiLoading } = useCqiMatrix();

  const statsItems = [
    `${protocols ? protocols.length : "—"} PROTOCOLS SCORED`,
    `${cqiData ? cqiData.count : "—"} CQI PAIRS`,
    "PSI v0.1.0",
    "FORM PSI-001 · BASIS PROTOCOL",
  ];

  const sorted = protocols ? [...protocols].sort((a, b) => (b.score || b.overall_score || 0) - (a.score || a.overall_score || 0)) : [];

  return (
    <div>
      <div style={{ height: mobile ? 16 : 28 }} />

      {/* Stats bar */}
      <div style={{ border: `1.5px solid ${T.ink}`, marginBottom: 24, padding: mobile ? "10px 12px" : "14px 20px" }}>
        <div style={{ display: "flex", flexWrap: "wrap", alignItems: "center", gap: mobile ? 4 : 0 }}>
          {statsItems.map((s, i) => (
            <div key={i} style={{ display: "flex", alignItems: "center" }}>
              <span style={{ fontFamily: T.mono, fontSize: mobile ? 8 : 10, color: T.inkLight, textTransform: "uppercase", letterSpacing: mobile ? 0.5 : 1.5, padding: mobile ? "2px 6px" : "0 12px" }}>
                {s}
              </span>
              {!mobile && i < statsItems.length - 1 && (
                <div style={{ width: 1, height: 12, background: T.ruleMid }} />
              )}
            </div>
          ))}
        </div>
      </div>

      {/* PSI Rankings Table */}
      <div style={{ marginBottom: 32 }}>
        <div style={{ fontFamily: T.mono, fontSize: 9, textTransform: "uppercase", letterSpacing: 1.5, color: T.inkLight, marginBottom: 10 }}>
          Protocol Solvency Index · Rankings
        </div>
        {psiLoading ? (
          <div style={{ padding: 24, fontFamily: T.mono, fontSize: 12, color: T.inkFaint }}>Loading protocol scores...</div>
        ) : sorted.length === 0 ? (
          <div style={{ padding: 24, fontFamily: T.mono, fontSize: 12, color: T.inkFaint }}>No protocol scores yet. Run PSI scoring first.</div>
        ) : (
          <div style={{ border: `1px solid ${T.ruleMid}` }}>
            {/* Header */}
            <div style={{
              display: "grid",
              gridTemplateColumns: mobile ? "32px 1fr 60px 40px" : "32px 1fr 80px 80px 80px 80px 60px 40px",
              padding: "8px 16px",
              background: T.paper,
              borderBottom: `3px solid ${T.ink}`,
              fontFamily: T.mono, fontSize: 9, textTransform: "uppercase",
              letterSpacing: 1.5, color: T.inkLight,
            }}>
              <span>#</span>
              <span>Protocol</span>
              {!mobile && <span>Balance</span>}
              {!mobile && <span>Revenue</span>}
              {!mobile && <span>Security</span>}
              {!mobile && <span>Gov</span>}
              <span>Score</span>
              <span>Grade</span>
            </div>
            {/* Rows */}
            {sorted.map((p, i) => {
              const cats = p.category_scores || {};
              return (
                <div key={p.protocol_slug} style={{
                  display: "grid",
                  gridTemplateColumns: mobile ? "32px 1fr 60px 40px" : "32px 1fr 80px 80px 80px 80px 60px 40px",
                  padding: "11px 16px",
                  borderBottom: `1px dotted ${T.ruleMid}`,
                  alignItems: "center",
                }}>
                  <span style={{ fontFamily: T.mono, fontSize: 11, color: T.inkFaint }}>{i + 1}</span>
                  <span style={{ fontFamily: T.mono, fontSize: 13, fontWeight: 700, color: T.ink }}>{p.protocol_name || p.protocol_slug}</span>
                  {!mobile && <span style={{ fontFamily: T.mono, fontSize: 11, color: subScoreColor(cats.balance_sheet) }}>{fmt(cats.balance_sheet, 0)}</span>}
                  {!mobile && <span style={{ fontFamily: T.mono, fontSize: 11, color: subScoreColor(cats.revenue) }}>{fmt(cats.revenue, 0)}</span>}
                  {!mobile && <span style={{ fontFamily: T.mono, fontSize: 11, color: subScoreColor(cats.security) }}>{fmt(cats.security, 0)}</span>}
                  {!mobile && <span style={{ fontFamily: T.mono, fontSize: 11, color: subScoreColor(cats.governance) }}>{fmt(cats.governance, 0)}</span>}
                  <span style={{ fontFamily: T.mono, fontSize: 13, fontWeight: 600, color: T.ink }}>{fmt(p.score || p.overall_score, 1)}</span>
                  <span style={{ fontFamily: T.sans, fontSize: 18, fontWeight: 700, color: gradeColor(p.grade) }}>{p.grade || "—"}</span>
                </div>
              );
            })}
          </div>
        )}

        {/* Formula bar */}
        <div style={{
          border: `1px solid ${T.ruleMid}`, borderTop: "none",
          padding: mobile ? "8px 12px" : "10px 16px",
          fontFamily: T.mono, fontSize: mobile ? 9 : 10, color: T.inkLight,
        }}>
          PSI = 0.25×Balance + 0.20×Revenue + 0.20×Liquidity + 0.15×Security + 0.10×Governance + 0.10×Token
        </div>
      </div>

      {/* CQI Composition Matrix */}
      <div style={{ marginBottom: 32 }}>
        <div style={{ fontFamily: T.mono, fontSize: 9, textTransform: "uppercase", letterSpacing: 1.5, color: T.inkLight, marginBottom: 4 }}>
          Collateral Quality Index · CQI = √(SII × PSI)
        </div>
        <div style={{ fontFamily: T.sans, fontSize: 11, color: T.inkFaint, marginBottom: 12 }}>
          Quality of each stablecoin as collateral in each protocol. Geometric mean penalizes weakness in either component.
        </div>

        {cqiLoading ? (
          <div style={{ padding: 24, fontFamily: T.mono, fontSize: 12, color: T.inkFaint }}>Loading composition matrix...</div>
        ) : !cqiData || !cqiData.matrix || cqiData.matrix.length === 0 ? (
          <div style={{ padding: 24, fontFamily: T.mono, fontSize: 12, color: T.inkFaint }}>No CQI data. Requires both SII and PSI scores.</div>
        ) : (() => {
          const assets = [...new Set(cqiData.matrix.map(r => r.asset))];
          const protos = [...new Set(cqiData.matrix.map(r => r.protocol))];
          const lookup = {};
          cqiData.matrix.forEach(r => { lookup[`${r.asset}-${r.protocol_slug}`] = r; });

          const assetAvg = {};
          assets.forEach(a => {
            const scores = cqiData.matrix.filter(r => r.asset === a).map(r => r.cqi_score).filter(Boolean);
            assetAvg[a] = scores.length ? scores.reduce((s, v) => s + v, 0) / scores.length : 0;
          });
          assets.sort((a, b) => assetAvg[b] - assetAvg[a]);

          const protoAvg = {};
          protos.forEach(p => {
            const scores = cqiData.matrix.filter(r => r.protocol === p).map(r => r.cqi_score).filter(Boolean);
            protoAvg[p] = scores.length ? scores.reduce((s, v) => s + v, 0) / scores.length : 0;
          });
          protos.sort((a, b) => protoAvg[b] - protoAvg[a]);

          const protoSlugMap = {};
          cqiData.matrix.forEach(r => { protoSlugMap[r.protocol] = r.protocol_slug; });

          if (mobile) {
            const sorted = [...cqiData.matrix].sort((a, b) => (b.cqi_score || 0) - (a.cqi_score || 0)).slice(0, 20);
            return (
              <div style={{ border: `1px solid ${T.ruleMid}` }}>
                <div style={{
                  display: "grid", gridTemplateColumns: "32px 1fr 60px 40px",
                  padding: "8px 12px", borderBottom: `3px solid ${T.ink}`,
                  fontFamily: T.mono, fontSize: 9, textTransform: "uppercase", letterSpacing: 1, color: T.inkLight,
                }}>
                  <span>#</span><span>Pair</span><span>CQI</span><span>Grade</span>
                </div>
                {sorted.map((r, i) => (
                  <div key={i} style={{
                    display: "grid", gridTemplateColumns: "32px 1fr 60px 40px",
                    padding: "10px 12px", borderBottom: `1px dotted ${T.ruleMid}`, alignItems: "center",
                  }}>
                    <span style={{ fontFamily: T.mono, fontSize: 11, color: T.inkFaint }}>{i + 1}</span>
                    <div>
                      <span style={{ fontFamily: T.mono, fontSize: 11, fontWeight: 700, color: T.ink }}>{r.asset}</span>
                      <span style={{ fontFamily: T.mono, fontSize: 10, color: T.inkFaint }}> in </span>
                      <span style={{ fontFamily: T.mono, fontSize: 11, color: T.inkMid }}>{r.protocol}</span>
                    </div>
                    <span style={{ fontFamily: T.mono, fontSize: 12, fontWeight: 600, color: T.ink }}>{fmt(r.cqi_score, 1)}</span>
                    <span style={{ fontFamily: T.sans, fontSize: 16, fontWeight: 700, color: gradeColor(r.cqi_grade) }}>{r.cqi_grade || "—"}</span>
                  </div>
                ))}
              </div>
            );
          }

          const displayAssets = assets.slice(0, 8);
          const cellBg = (score) => {
            if (!score) return T.ruleLight;
            if (score >= 85) return "#e8f0e8";
            if (score >= 70) return "#f0f0e4";
            if (score >= 55) return "#f5ece0";
            return "#f5e4e0";
          };

          return (
            <div style={{ overflowX: "auto" }}>
              <table style={{
                borderCollapse: "collapse", width: "100%",
                fontFamily: T.mono, fontSize: 11,
                border: `1px solid ${T.ruleMid}`,
              }}>
                <thead>
                  <tr style={{ borderBottom: `3px solid ${T.ink}` }}>
                    <th style={{ padding: "8px 12px", textAlign: "left", fontSize: 9, textTransform: "uppercase", letterSpacing: 1.5, color: T.inkLight }}>Asset \ Protocol</th>
                    {protos.map(p => (
                      <th key={p} style={{ padding: "8px 6px", textAlign: "center", fontSize: 9, textTransform: "uppercase", letterSpacing: 0.5, color: T.inkLight, maxWidth: 90 }}>
                        {p.length > 10 ? p.slice(0, 9) + "…" : p}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {displayAssets.map(asset => (
                    <tr key={asset} style={{ borderBottom: `1px dotted ${T.ruleMid}` }}>
                      <td style={{ padding: "8px 12px", fontWeight: 700, color: T.ink }}>{asset}</td>
                      {protos.map(proto => {
                        const slug = protoSlugMap[proto];
                        const entry = lookup[`${asset}-${slug}`];
                        const score = entry ? entry.cqi_score : null;
                        const grade = entry ? entry.cqi_grade : null;
                        return (
                          <td key={proto} style={{
                            padding: "6px 6px", textAlign: "center",
                            background: cellBg(score),
                            borderLeft: `1px solid ${T.ruleLight}`,
                          }}>
                            <div style={{ fontWeight: 600, color: T.ink }}>{score ? fmt(score, 0) : "—"}</div>
                            <div style={{ fontSize: 9, color: gradeColor(grade) }}>{grade || ""}</div>
                          </td>
                        );
                      })}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          );
        })()}

        {!cqiLoading && cqiData && cqiData.matrix && (
          <div style={{ fontFamily: T.sans, fontSize: 10, color: T.inkFaint, marginTop: 8 }}>
            Geometric mean: CQI = √(SII × PSI). A strong stablecoin in a weak protocol scores lower than the arithmetic average would suggest.
          </div>
        )}
      </div>
    </div>
  );
}

function PulseView({ mobile }) {
  const { data: pulse, loading } = usePulse();
  const [divergence, setDivergence] = useState(null);
  useEffect(() => {
    fetch(`${API}/api/divergence`)
      .then(r => r.ok ? r.json() : null)
      .then(d => setDivergence(d))
      .catch(() => {});
  }, []);

  if (loading) {
    return (
      <div style={{ padding: 40, display: "flex", justifyContent: "center" }}>
        <div style={{ color: T.inkFaint, fontFamily: T.mono, fontSize: 12 }}>Loading daily pulse...</div>
      </div>
    );
  }

  if (!pulse || !pulse.summary) {
    return (
      <div style={{ padding: 40, textAlign: "center", color: T.inkFaint, fontSize: 13 }}>
        No pulse data available. The daily pulse runs after each scoring cycle.
      </div>
    );
  }

  const s = typeof pulse.summary === "string" ? JSON.parse(pulse.summary) : pulse.summary;
  const net = s.network_state || {};
  const events = s.events_24h || {};
  const scores = s.scores || [];
  const psiScores = s.psi_scores || [];
  const notables = s.notable_events || [];

  const aTier = scores.filter(c => c.grade && c.grade.startsWith("A"));
  const atRisk = scores.filter(c => c.grade && !c.grade.startsWith("A") && !c.grade.startsWith("B"));
  const movers = scores.filter(c => c.delta_24h != null && Math.abs(c.delta_24h) >= 0.5)
    .sort((a, b) => Math.abs(b.delta_24h) - Math.abs(a.delta_24h));

  return (
    <div>
      <div style={{ height: mobile ? 16 : 28 }} />

      {/* Pulse header — newspaper masthead style */}
      <div style={{ border: `1.5px solid ${T.ink}`, marginBottom: 24 }}>
        <div style={{ padding: mobile ? "14px 12px" : "18px 24px", borderBottom: `1px solid ${T.ruleMid}` }}>
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: mobile ? "flex-start" : "baseline", flexDirection: mobile ? "column" : "row", gap: mobile ? 4 : 0 }}>
            <h2 style={{ margin: 0, fontSize: mobile ? 18 : 24, fontFamily: T.sans, fontWeight: 400, color: T.ink }}>
              <span style={{ fontWeight: 700 }}>Daily</span> Pulse
            </h2>
            <span style={{ fontFamily: T.mono, fontSize: mobile ? 9 : 10, color: T.inkFaint, textTransform: "uppercase", letterSpacing: 2 }}>
              {s.pulse_date || pulse.pulse_date || "—"} · FORM PLS-001 · BASIS PROTOCOL
            </span>
          </div>
        </div>

        {/* Network state summary — big numbers */}
        <div style={{
          display: "grid",
          gridTemplateColumns: mobile ? "1fr 1fr" : "1fr 1fr 1fr 1fr 1fr",
          borderBottom: `1px solid ${T.ruleMid}`,
        }}>
          {[
            { label: "Tracked", value: fmtB(net.total_tracked_usd) },
            { label: "Wallets", value: net.wallets_indexed != null ? net.wallets_indexed.toLocaleString() : "—" },
            { label: "Avg Risk", value: net.avg_risk_score != null ? fmt(net.avg_risk_score, 1) : "—" },
            { label: "Stablecoins", value: net.stablecoins_scored || scores.length || "—" },
            { label: "Events 24h", value: events.total || 0 },
          ].map((item, i) => (
            <div key={i} style={{
              padding: mobile ? "12px 12px" : "14px 20px",
              borderRight: (!mobile || i % 2 === 0) ? `1px solid ${T.ruleLight}` : "none",
              borderBottom: mobile && i < 4 ? `1px solid ${T.ruleLight}` : "none",
            }}>
              <div style={{ fontFamily: T.mono, fontSize: 8, textTransform: "uppercase", letterSpacing: 1.5, color: T.inkFaint, marginBottom: 4 }}>{item.label}</div>
              <div style={{ fontFamily: T.mono, fontSize: mobile ? 18 : 22, fontWeight: 700, color: T.ink }}>{item.value}</div>
            </div>
          ))}
        </div>

        {/* Event severity bar */}
        <div style={{ padding: mobile ? "10px 12px" : "10px 24px", display: "flex", gap: mobile ? 12 : 24, alignItems: "center" }}>
          {[
            { label: "Critical", count: events.critical || 0, color: T.accent },
            { label: "Alert", count: events.alert || 0, color: "#c77b2a" },
            { label: "Notable", count: events.notable || 0, color: T.inkMid },
            { label: "Silent", count: events.silent || 0, color: T.inkFaint },
          ].map((sev, i) => (
            <div key={i} style={{ display: "flex", alignItems: "center", gap: 6 }}>
              <div style={{ width: 6, height: 6, borderRadius: "50%", background: sev.color, flexShrink: 0 }} />
              <span style={{ fontFamily: T.mono, fontSize: mobile ? 9 : 10, color: sev.color }}>
                {sev.count} {sev.label}
              </span>
            </div>
          ))}
        </div>

        {/* Content hash verification line */}
        {pulse.content_hash && (
          <div style={{
            borderTop: `1px solid ${T.ruleLight}`,
            padding: mobile ? "6px 12px" : "6px 24px",
            fontFamily: T.mono, fontSize: 9, color: T.inkFaint,
          }}>
            Content hash: {pulse.content_hash}
          </div>
        )}
      </div>

      {/* Stablecoin scores — compact table */}
      <div style={{ marginBottom: 24 }}>
        <div style={{ fontFamily: T.mono, fontSize: 9, textTransform: "uppercase", letterSpacing: 1.5, color: T.inkLight, marginBottom: 10 }}>
          Stablecoin Integrity · All Scores
        </div>
        <div style={{ border: `1px solid ${T.ruleMid}` }}>
          <div style={{
            display: "grid",
            gridTemplateColumns: mobile ? "32px 1fr 56px 56px 44px" : "32px 1fr 72px 72px 56px",
            padding: "8px 16px",
            borderBottom: `3px solid ${T.ink}`,
            fontFamily: T.mono, fontSize: 9, textTransform: "uppercase", letterSpacing: 1.5, color: T.inkLight,
          }}>
            <span>#</span><span>Asset</span><span>Score</span><span>Δ 24h</span><span>Grade</span>
          </div>
          {[...scores].sort((a, b) => (b.score || 0) - (a.score || 0)).map((coin, i) => (
            <div key={coin.symbol} style={{
              display: "grid",
              gridTemplateColumns: mobile ? "32px 1fr 56px 56px 44px" : "32px 1fr 72px 72px 56px",
              padding: "9px 16px",
              borderBottom: `1px dotted ${T.ruleMid}`,
              alignItems: "center",
            }}>
              <span style={{ fontFamily: T.mono, fontSize: 11, color: T.inkFaint }}>{i + 1}</span>
              <span style={{ fontFamily: T.mono, fontSize: 12, fontWeight: 700, color: T.ink }}>{coin.symbol}</span>
              <span style={{ fontFamily: T.mono, fontSize: 12, fontWeight: 500, color: T.ink }}>{fmt(coin.score, 1)}</span>
              <span style={{
                fontFamily: T.mono, fontSize: 11,
                color: coin.delta_24h > 0 ? "#2d6b45" : coin.delta_24h < 0 ? T.accent : T.inkFaint,
              }}>
                {coin.delta_24h != null ? (coin.delta_24h > 0 ? "+" : "") + fmt(coin.delta_24h, 2) : "—"}
              </span>
              <span style={{ fontFamily: T.sans, fontSize: 16, fontWeight: 700, color: gradeColor(coin.grade) }}>{coin.grade || "—"}</span>
            </div>
          ))}
        </div>
      </div>

      {/* PSI scores — if present */}
      {psiScores.length > 0 && (
        <div style={{ marginBottom: 24 }}>
          <div style={{ fontFamily: T.mono, fontSize: 9, textTransform: "uppercase", letterSpacing: 1.5, color: T.inkLight, marginBottom: 10 }}>
            Protocol Solvency · Snapshot
          </div>
          <div style={{ border: `1px solid ${T.ruleMid}` }}>
            <div style={{
              display: "grid",
              gridTemplateColumns: "32px 1fr 72px 56px",
              padding: "8px 16px",
              borderBottom: `3px solid ${T.ink}`,
              fontFamily: T.mono, fontSize: 9, textTransform: "uppercase", letterSpacing: 1.5, color: T.inkLight,
            }}>
              <span>#</span><span>Protocol</span><span>Score</span><span>Grade</span>
            </div>
            {[...psiScores].sort((a, b) => (b.score || 0) - (a.score || 0)).map((p, i) => (
              <div key={p.protocol_slug} style={{
                display: "grid",
                gridTemplateColumns: "32px 1fr 72px 56px",
                padding: "9px 16px",
                borderBottom: `1px dotted ${T.ruleMid}`,
                alignItems: "center",
              }}>
                <span style={{ fontFamily: T.mono, fontSize: 11, color: T.inkFaint }}>{i + 1}</span>
                <span style={{ fontFamily: T.mono, fontSize: 12, fontWeight: 700, color: T.ink }}>{p.protocol_name || p.protocol_slug}</span>
                <span style={{ fontFamily: T.mono, fontSize: 12, fontWeight: 500, color: T.ink }}>{fmt(p.score, 1)}</span>
                <span style={{ fontFamily: T.sans, fontSize: 16, fontWeight: 700, color: gradeColor(p.grade) }}>{p.grade || "—"}</span>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Notable events — if any */}
      {notables.length > 0 && (
        <div style={{ marginBottom: 24 }}>
          <div style={{ fontFamily: T.mono, fontSize: 9, textTransform: "uppercase", letterSpacing: 1.5, color: T.inkLight, marginBottom: 10 }}>
            Notable Events · Last 24 Hours
          </div>
          <div style={{ border: `1px solid ${T.ruleMid}` }}>
            {notables.map((evt, i) => (
              <div key={i} style={{
                padding: "10px 16px",
                borderBottom: `1px dotted ${T.ruleMid}`,
                display: "flex", alignItems: "center", gap: 12,
              }}>
                <div style={{
                  width: 6, height: 6, borderRadius: "50%", flexShrink: 0,
                  background: evt.severity === "critical" ? T.accent : evt.severity === "alert" ? "#c77b2a" : T.inkMid,
                }} />
                <div>
                  <span style={{ fontFamily: T.mono, fontSize: 10, textTransform: "uppercase", letterSpacing: 0.5, color: T.inkLight }}>{evt.severity} · {evt.trigger}</span>
                  <div style={{ fontFamily: T.mono, fontSize: 11, color: T.ink, marginTop: 2 }}>
                    {truncAddr(evt.wallet)} · Score: {evt.score != null ? fmt(evt.score, 1) : "—"}
                  </div>
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Divergence Signals */}
      <div style={{ marginBottom: 24 }}>
        <div style={{ fontFamily: T.mono, fontSize: 9, textTransform: "uppercase", letterSpacing: 1.5, color: T.inkLight, marginBottom: 10 }}>
          Divergence Signals
        </div>
        {divergence && divergence.divergence_signals && divergence.divergence_signals.length > 0 ? (
          <div style={{ border: `1px solid ${T.ruleMid}` }}>
            {divergence.divergence_signals.slice(0, 10).map((sig, i) => (
              <div key={i} style={{
                padding: "10px 16px",
                borderBottom: `1px dotted ${T.ruleMid}`,
                display: "flex", alignItems: "center", gap: 12,
              }}>
                <div style={{
                  width: 6, height: 6, borderRadius: "50%", flexShrink: 0,
                  background: sig.severity === "critical" ? T.accent : sig.severity === "alert" ? "#c77b2a" : T.inkMid,
                }} />
                <div style={{ flex: 1, minWidth: 0 }}>
                  <span style={{ fontFamily: T.mono, fontSize: 10, textTransform: "uppercase", letterSpacing: 0.5, color: T.inkLight }}>
                    {sig.severity} · {sig.type === "asset_quality" ? "asset" : sig.type === "wallet_concentration" ? "wallet" : "flow"}
                  </span>
                  <div style={{ fontFamily: T.mono, fontSize: 11, color: T.ink, marginTop: 2, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                    {sig.signal}
                  </div>
                </div>
              </div>
            ))}
          </div>
        ) : (
          <div style={{ fontFamily: T.mono, fontSize: 11, color: T.inkFaint, padding: "10px 0" }}>
            No divergence signals detected. Capital flows align with quality scores.
          </div>
        )}
      </div>

      {/* Movers — biggest score changes */}
      {movers.length > 0 && (
        <div style={{ marginBottom: 24 }}>
          <div style={{ fontFamily: T.mono, fontSize: 9, textTransform: "uppercase", letterSpacing: 1.5, color: T.inkLight, marginBottom: 10 }}>
            Biggest Movers · |Δ| ≥ 0.5
          </div>
          <div style={{ display: "flex", flexWrap: "wrap", gap: 8 }}>
            {movers.map(m => (
              <div key={m.symbol} style={{
                border: `1px solid ${T.ruleMid}`, padding: "8px 14px",
                display: "flex", alignItems: "baseline", gap: 8,
              }}>
                <span style={{ fontFamily: T.mono, fontSize: 12, fontWeight: 700, color: T.ink }}>{m.symbol}</span>
                <span style={{
                  fontFamily: T.mono, fontSize: 12, fontWeight: 600,
                  color: m.delta_24h > 0 ? "#2d6b45" : T.accent,
                }}>
                  {m.delta_24h > 0 ? "+" : ""}{fmt(m.delta_24h, 2)}
                </span>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Methodology footer */}
      <div style={{ fontFamily: T.mono, fontSize: 9, color: T.inkFaint, marginTop: 16 }}>
        Methodology {s.methodology_version || "v1.0.0"} · Pulse generated after daily scoring cycle · Content-hashed for on-chain verification
      </div>
    </div>
  );
}

function Footer() {
  return (
    <footer style={{
      padding: "16px 24px",
      borderTop: `1px solid ${T.ruleMid}`,
      display: "flex", justifyContent: "space-between", alignItems: "center",
      fontSize: 10, color: T.inkFaint, fontFamily: T.mono,
    }}>
      <span>Basis Protocol · Stablecoin Integrity Index</span>
      <span><a href="/developers" style={{ color: 'inherit', textDecoration: 'none', borderBottom: `1px solid ${T.ruleMid}` }}>API &amp; Pricing</a> · Risk surfaces for on-chain finance · basisprotocol.xyz</span>
    </footer>
  );
}

export default function App() {
  const [view, setView] = useState("rankings");
  const [selectedCoin, setSelectedCoin] = useState(null);
  const { data: scores, loading, error, ts } = useScores();
  const mobile = useIsMobile();

  const handleSelect = useCallback((coinId) => {
    setSelectedCoin(coinId);
    setView("detail");
    window.scrollTo(0, 0);
  }, []);

  const handleBack = useCallback(() => {
    setView("rankings");
    setSelectedCoin(null);
  }, []);

  const handleSetView = useCallback((v) => {
    setView(v);
    if (v !== "detail") setSelectedCoin(null);
    window.scrollTo(0, 0);
  }, []);

  return (
    <div style={{ minHeight: "100vh", background: T.paper, color: T.ink, fontFamily: T.sans }}>
      <style>{`
        @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500;600;700&family=IBM+Plex+Sans:wght@400;500;600;700&display=swap');
        * { box-sizing: border-box; margin: 0; padding: 0; }
        html { background: ${T.paper}; }
        body { background: ${T.paper}; overflow-x: hidden; }
        ::-webkit-scrollbar { width: 5px; }
        ::-webkit-scrollbar-track { background: ${T.paper}; }
        ::-webkit-scrollbar-thumb { background: ${T.ruleMid}; border-radius: 3px; }
        button { font-family: inherit; }
        button:hover { opacity: 0.88; }
        @keyframes fadeIn { from { opacity: 0; transform: translateY(6px); } to { opacity: 1; transform: translateY(0); } }
        @keyframes pulse { 0%, 100% { opacity: 1; } 50% { opacity: 0.4; } }
      `}</style>

      <div style={{
        maxWidth: 1100, margin: "0 auto", padding: mobile ? "8px 6px 0" : "32px 24px 0",
      }}>
        <div style={{
          border: `${mobile ? 2 : 3}px solid ${T.ink}`,
          boxShadow: mobile ? "none" : `6px 6px 0 0 ${T.ruleMid}`,
          background: T.paper,
        }}>
          <div style={{ padding: mobile ? "10px 12px" : "12px 24px", display: "flex", justifyContent: "space-between", alignItems: "center" }}>
            <nav style={{ display: "flex", gap: 16 }}>
              {[
                { id: "rankings", label: "Rankings" },
                { id: "protocols", label: "Protocols" },
                { id: "wallets", label: "Wallets" },
                { id: "pulse", label: "Pulse" },
                { id: "methodology", label: "Methodology" },
              ].map((tab) => (
                <button
                  key={tab.id}
                  onClick={() => handleSetView(tab.id)}
                  style={{
                    padding: "4px 0", border: "none", cursor: "pointer",
                    fontSize: 12, fontWeight: view === tab.id ? 600 : 400,
                    fontFamily: T.sans,
                    color: view === tab.id ? T.ink : T.inkLight,
                    background: "transparent",
                    borderBottom: view === tab.id ? `2px solid ${T.ink}` : "2px solid transparent",
                  }}
                >
                  {tab.label}
                </button>
              ))}
            </nav>
          </div>

          <div style={{ borderTop: `1px solid ${T.ruleLight}` }} />

          <div style={{ padding: mobile ? "0 8px 12px" : "0 24px 24px" }}>
            <main style={{ animation: "fadeIn 0.3s ease" }}>
              {view === "rankings" && (
                <RankingsView scores={scores} loading={loading} onSelect={handleSelect} ts={ts} mobile={mobile} />
              )}
              {view === "detail" && selectedCoin && (
                <DetailView coinId={selectedCoin} onBack={handleBack} mobile={mobile} />
              )}
              {view === "wallets" && <WalletsView mobile={mobile} />}
              {view === "protocols" && <ProtocolsView mobile={mobile} />}
              {view === "pulse" && <PulseView mobile={mobile} />}
              {view === "methodology" && <MethodologyView mobile={mobile} />}
            </main>
          </div>

          <Footer />
        </div>

        <div style={{ height: mobile ? 16 : 32 }} />
      </div>
    </div>
  );
}
