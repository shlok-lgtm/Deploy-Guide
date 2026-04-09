import { useState, useEffect, useCallback, useRef } from "react";
import OpsDashboard from "./pages/OpsDashboard";

const API = "";
const _DK = "BF6KF2i34EslzTnvBXAjcLlDZBlQKLSTP9LdrAzxUHI";
const apiFetch = (url, opts) => fetch(`${url}${url.includes("?") ? "&" : "?"}apikey=${_DK}`, opts);

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

const TAB_ACCENT = {
  rankings:       "#fc988f",  // coral
  protocols:      "#7BA3A8",  // slate teal
  wallets:        "#8B7B9E",  // muted plum
  witness:        "#A8C4A0",  // sage green
  methodology:    "#B8937A",  // warm clay
  detail:         "#fc988f",
  "witness-detail": "#A8C4A0",
};

function BasisLogo({ accent = "#fc988f", size = 40 }) {
  return (
    <svg viewBox="120 0 880 720" width={size} height={size * 720 / 880} xmlns="http://www.w3.org/2000/svg">
      <g stroke="none">
        <g fill="#20222d">
          <path d="M 941.49 629.18 L 946.51 637.25 L 951.99 646.13 L 954.51 650.28 L 981.98 694.03 L 984.45 698.16 Q 985.37 701.10 987.50 703.29 L 990.00 706.79 A 0.55 0.54 72.2 0 1 989.56 707.65 L 279.25 707.90 L 274.56 707.88 L 264.29 707.88 L 262.52 707.88 L 262.04 707.87 L 260.71 707.88 L 128.66 707.90 A 0.84 0.84 0.0 0 1 127.94 706.63 Q 129.14 704.66 130.44 702.38 Q 132.06 699.53 143.46 680.74 Q 197.91 590.97 201.95 584.46 C 207.70 575.21 212.96 567.68 217.55 560.11 Q 218.04 559.30 227.82 543.37 Q 241.93 520.40 384.32 286.58 Q 422.33 224.16 466.63 151.77 C 469.44 147.19 471.27 143.29 474.15 138.93 Q 478.82 131.84 484.04 122.34 Q 487.51 116.02 489.30 113.40 C 491.39 110.33 494.66 105.41 496.61 101.63 C 500.06 94.92 504.16 88.92 508.67 81.45 Q 527.86 49.63 535.73 36.72 Q 537.88 33.18 545.53 21.75 Q 547.21 19.24 548.10 17.43 C 549.30 14.99 550.17 12.86 551.61 11.60 A 1.30 1.30 0.0 0 1 553.62 11.96 C 556.47 17.30 559.17 22.44 562.35 27.21 C 566.21 33.01 573.18 43.13 578.04 51.96 Q 582.43 59.91 587.61 67.40 Q 592.93 75.06 595.11 79.26 C 597.89 84.60 601.39 89.01 605.14 95.06 C 616.63 113.57 621.25 121.53 629.78 134.60 Q 634.71 142.15 635.89 144.30 Q 638.63 149.29 641.93 154.03 C 647.35 161.80 652.21 171.61 658.01 179.94 C 660.69 183.80 662.70 187.89 665.70 192.66 Q 753.21 331.91 812.05 424.96 Q 852.98 489.67 891.97 552.28 Q 901.27 567.22 902.63 569.24 Q 908.50 577.98 912.82 585.69 Q 918.42 595.70 926.71 607.29 Q 927.72 608.69 929.18 609.97 A 2.19 2.18 -85.6 0 1 929.59 610.45 L 941.49 629.18 Z M 955.26 688.21 C 840.54 505.58 735.55 339.82 600.25 124.50 Q 584.72 99.79 552.71 48.20 A 0.38 0.38 0.0 0 0 552.07 48.20 L 161.88 687.93 A 0.32 0.31 -74.1 0 0 162.14 688.41 L 955.15 688.41 A 0.13 0.13 0.0 0 0 955.26 688.21 Z"/>
        </g>
        <g fill="#f8eee5">
          <path d="M 955.26 688.21 A 0.13 0.13 0.0 0 1 955.15 688.41 L 162.14 688.41 A 0.32 0.31 -74.1 0 1 161.88 687.93 L 552.07 48.20 A 0.38 0.38 0.0 0 1 552.71 48.20 Q 584.72 99.79 600.25 124.50 C 735.55 339.82 840.54 505.58 955.26 688.21 Z M 255.54 654.21 L 893.85 654.22 A 0.31 0.31 0.0 0 0 894.11 653.74 L 886.54 641.61 Q 883.95 637.52 881.25 633.23 Q 673.15 303.22 643.32 255.45 Q 612.22 205.65 574.86 146.13 Q 564.76 130.04 554.21 112.39 A 0.61 0.60 45.3 0 0 553.17 112.38 L 223.00 653.69 A 0.38 0.38 0.0 0 0 223.33 654.27 L 255.54 654.21 Z"/>
          <path d="M 597.88 193.49 L 578.12 225.37 L 554.55 187.51 A 0.44 0.44 0.0 0 0 553.80 187.51 L 294.63 612.43 A 0.46 0.46 0.0 0 0 294.90 613.11 Q 297.14 613.72 299.71 613.23 L 655.21 613.38 Q 654.95 614.72 655.50 615.51 Q 666.67 631.59 677.13 648.56 L 342.21 648.60 L 340.96 648.62 L 238.82 648.62 L 233.10 648.70 A 0.36 0.36 0.0 0 1 232.79 648.16 L 553.29 122.70 A 0.39 0.39 0.0 0 1 553.95 122.70 L 597.88 193.49 Z"/>
          <path d="M 575.21 231.45 L 574.57 231.67 A 1.45 1.43 6.9 0 0 573.83 232.24 Q 570.43 237.24 567.54 242.55 Q 561.79 253.10 554.46 262.26 L 365.83 571.98 A 0.46 0.46 0.0 0 0 366.22 572.67 L 388.97 572.62 L 628.21 572.64 Q 628.16 573.77 628.59 574.43 Q 639.50 591.00 650.46 607.78 L 304.72 608.05 Q 304.30 608.02 304.24 607.56 A 0.31 0.30 60.0 0 0 304.29 607.36 L 553.73 197.96 A 0.43 0.42 -45.4 0 1 554.45 197.95 L 575.21 231.45 Z"/>
          <path d="M 623.37 567.14 L 387.95 567.15 L 375.80 567.16 A 0.26 0.25 -73.9 0 1 375.59 566.77 L 495.94 369.15 Q 546.99 448.28 598.42 527.83 Q 608.74 543.80 619.08 559.75 Q 621.45 563.41 623.37 567.14 Z"/>
        </g>
        <g fill="#27252a">
          <path d="M 886.54 641.61 Q 886.01 645.16 883.46 647.02 L 597.88 193.49 L 553.95 122.70 A 0.39 0.39 0.0 0 0 553.29 122.70 L 232.79 648.16 A 0.36 0.36 0.0 0 0 233.10 648.70 L 238.82 648.62 L 244.74 653.50 A 1.47 1.40 64.2 0 0 245.62 653.83 L 255.54 654.21 L 223.33 654.27 A 0.38 0.38 0.0 0 1 223.00 653.69 L 553.17 112.38 A 0.61 0.60 45.3 0 1 554.21 112.39 Q 564.76 130.04 574.86 146.13 Q 612.22 205.65 643.32 255.45 Q 673.15 303.22 881.25 633.23 Q 883.95 637.52 886.54 641.61 Z"/>
          <path d="M 578.12 225.37 L 821.92 612.61 A 0.47 0.47 0.0 0 1 821.52 613.33 L 802.09 613.37 L 801.56 607.77 L 811.85 608.05 A 0.32 0.32 0.0 0 0 812.13 607.56 L 575.21 231.45 L 554.45 197.95 A 0.43 0.42 -45.4 0 0 553.73 197.96 L 304.29 607.36 A 0.31 0.30 60.0 0 0 304.24 607.56 Q 304.30 608.02 304.72 608.05 L 299.71 613.23 Q 297.14 613.72 294.90 613.11 A 0.46 0.46 0.0 0 1 294.63 612.43 L 553.80 187.51 A 0.44 0.44 0.0 0 1 554.55 187.51 L 578.12 225.37 Z"/>
          <path d="M 554.46 262.26 L 749.53 571.71 A 0.52 0.52 0.0 0 1 749.09 572.50 L 738.95 572.62 Q 739.09 569.76 737.55 567.15 Q 738.75 567.29 739.31 567.08 A 0.63 0.62 64.3 0 0 739.63 566.16 L 554.81 273.32 A 0.48 0.47 44.8 0 0 554.00 273.32 L 495.94 369.15 L 375.59 566.77 A 0.26 0.25 -73.9 0 0 375.80 567.16 L 387.95 567.15 L 388.97 572.62 L 366.22 572.67 A 0.46 0.46 0.0 0 1 365.83 571.98 L 554.46 262.26 Z"/>
          <path d="M 623.37 567.14 L 737.55 567.15 Q 739.09 569.76 738.95 572.62 L 628.21 572.64 L 388.97 572.62 L 387.95 567.15 L 623.37 567.14 Z"/>
          <path d="M 650.46 607.78 L 801.56 607.77 L 802.09 613.37 L 655.21 613.38 L 299.71 613.23 L 304.72 608.05 L 650.46 607.78 Z"/>
          <path d="M 886.54 641.61 L 894.11 653.74 A 0.31 0.31 0.0 0 0 893.85 654.22 L 255.54 654.21 L 245.62 653.83 A 1.47 1.40 64.2 0 0 244.74 653.50 L 238.82 648.62 L 340.96 648.62 L 342.21 648.60 L 677.13 648.56 L 883.69 648.56 A 0.41 0.40 80.3 0 0 884.07 648.02 Q 883.92 647.57 883.46 647.02 Q 886.01 645.16 886.54 641.61 Z"/>
        </g>
        <g fill={accent}>
          <path d="M 883.46 647.02 Q 883.92 647.57 884.07 648.02 A 0.41 0.40 80.3 0 1 883.69 648.56 L 677.13 648.56 Q 666.67 631.59 655.50 615.51 Q 654.95 614.72 655.21 613.38 L 802.09 613.37 L 821.52 613.33 A 0.47 0.47 0.0 0 0 821.92 612.61 L 578.12 225.37 L 597.88 193.49 L 883.46 647.02 Z"/>
          <path d="M 575.21 231.45 L 812.13 607.56 A 0.32 0.32 0.0 0 1 811.85 608.05 L 801.56 607.77 L 650.46 607.78 Q 639.50 591.00 628.59 574.43 Q 628.16 573.77 628.21 572.64 L 738.95 572.62 L 749.09 572.50 A 0.52 0.52 0.0 0 0 749.53 571.71 L 554.46 262.26 Q 561.79 253.10 567.54 242.55 Q 570.43 237.24 573.83 232.24 A 1.45 1.43 6.9 0 1 574.57 231.67 L 575.21 231.45 Z"/>
          <path d="M 737.55 567.15 L 623.37 567.14 Q 621.45 563.41 619.08 559.75 Q 608.74 543.80 598.42 527.83 Q 546.99 448.28 495.94 369.15 L 554.00 273.32 A 0.48 0.47 44.8 0 1 554.81 273.32 L 739.63 566.16 A 0.63 0.62 64.3 0 1 739.31 567.08 Q 738.75 567.29 737.55 567.15 Z"/>
        </g>
      </g>
    </svg>
  );
}

function TabHeader({ title, formId, stats, formulaLine, versionLabel, accent, mobile, showOnChain = true, coinCount }) {
  return (
    <div style={{ border: `1.5px solid ${T.ink}`, marginBottom: 0 }}>
      <div style={{ padding: mobile ? "14px 12px 0" : "18px 24px 0" }}>
        <div style={{ display: "flex", flexDirection: mobile ? "column" : "row", justifyContent: "space-between", alignItems: mobile ? "flex-start" : "center", gap: mobile ? 4 : 0 }}>
          <h1 style={{ margin: 0, fontSize: mobile ? 20 : 28, fontFamily: T.sans, color: T.ink, fontWeight: 400, letterSpacing: -0.3 }}>
            {title}
          </h1>
          <div style={{ display: "flex", alignItems: "center", gap: mobile ? 8 : 14 }}>
            <BasisLogo accent={accent} size={mobile ? 28 : 38} />
            <span style={{ fontFamily: T.mono, fontSize: mobile ? 9 : 10, color: T.inkFaint, textTransform: "uppercase", letterSpacing: 2 }}>
              {formId}
            </span>
          </div>
        </div>

        {stats && stats.length > 0 && (
          <>
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
          </>
        )}
      </div>

      {formulaLine && (
        <div style={{ borderTop: `1px solid ${T.ruleMid}`, padding: mobile ? "8px 12px" : "10px 24px", display: "flex", flexDirection: mobile ? "column" : "row", justifyContent: "space-between", alignItems: mobile ? "flex-start" : "center", gap: mobile ? 4 : 0 }}>
          <span style={{ fontFamily: T.mono, fontSize: mobile ? 9 : 11, color: T.ink }}>
            {formulaLine}
          </span>
          {versionLabel && (
            <span style={{ fontFamily: T.mono, fontSize: mobile ? 8 : 10, color: T.inkLight, textTransform: "uppercase", letterSpacing: 1 }}>
              {versionLabel}
            </span>
          )}
        </div>
      )}

      {showOnChain && (
        <div style={{ borderTop: `1px solid ${T.ruleMid}`, padding: mobile ? "8px 12px" : "8px 24px", display: "flex", flexWrap: "wrap", gap: mobile ? 6 : 10, alignItems: "center" }}>
          <a href="https://basescan.org/address/0x01aaa1d20fe68d55d0c5b6b42399b91024f8cd99" target="_blank" rel="noopener noreferrer" style={{ fontFamily: T.mono, fontSize: mobile ? 8.5 : 10, color: T.inkMid, textDecoration: "none", display: "inline-flex", alignItems: "center", gap: 6, background: T.paperWarm, padding: "4px 12px", borderRadius: 999 }}>
            <span style={{ width: 6, height: 6, borderRadius: "50%", background: "#378ADD", display: "inline-block", flexShrink: 0 }} />
            {`Base \u00b7 0x01aA...cD99 \u00b7 ${coinCount || ""} stablecoins`}
          </a>
          <a href="https://arbiscan.io/address/0x01aaa1d20fe68d55d0c5b6b42399b91024f8cd99" target="_blank" rel="noopener noreferrer" style={{ fontFamily: T.mono, fontSize: mobile ? 8.5 : 10, color: T.inkMid, textDecoration: "none", display: "inline-flex", alignItems: "center", gap: 6, background: T.paperWarm, padding: "4px 12px", borderRadius: 999 }}>
            <span style={{ width: 6, height: 6, borderRadius: "50%", background: "#534AB7", display: "inline-block", flexShrink: 0 }} />
            {`Arbitrum \u00b7 0x01aA...cD99 \u00b7 ${coinCount || ""} stablecoins`}
          </a>
        </div>
      )}
    </div>
  );
}

const MICA_STATUS = {
  usdc: "Compliant", usdt: "Watchlist", dai: "Compliant", usde: "Watchlist",
  fdusd: "Compliant", pyusd: "Compliant", tusd: "Non-compliant",
  usdd: "Non-compliant", frax: "Watchlist", usd1: "Pending",
  rlusd: "Pending", crvusd: "Pending", usdp: "Pending", usds: "Pending",
  gho: "Pending", gusd: "Pending", lusd: "Pending", dola: "Pending",
  mim: "Pending", susd: "Pending", rai: "Pending", eurs: "Pending",
  eurc: "Pending", susde: "Pending", susds: "Pending", euri: "Pending",
  eure: "Pending", steakusdc: "Pending", stkgho: "Pending", sdola: "Pending",
  busd0: "Pending", ousd: "Pending", musd: "Pending", usdtb: "Pending",
  frax_64d0: "Pending", usddbttcbridge: "Pending",
};

const RESERVE_TYPE = {
  usdc: "Fiat-backed", usdt: "Fiat-backed", dai: "Crypto-backed",
  usde: "Synthetic", fdusd: "Fiat-backed", pyusd: "Fiat-backed",
  tusd: "Fiat-backed", usdd: "Algorithmic", frax: "Synthetic", usd1: "Mixed",
  rlusd: "Fiat-backed", crvusd: "Crypto-backed", usdp: "Fiat-backed",
  usds: "Crypto-backed", gho: "Crypto-backed", gusd: "Fiat-backed",
  lusd: "Crypto-backed", dola: "Crypto-backed", mim: "Crypto-backed",
  susd: "Synthetic", rai: "Crypto-backed", eurs: "Fiat-backed",
  eurc: "Fiat-backed", susde: "Synthetic", susds: "Crypto-backed",
  euri: "Fiat-backed", eure: "Fiat-backed", steakusdc: "Crypto-backed",
  stkgho: "Crypto-backed", sdola: "Crypto-backed", busd0: "Fiat-backed",
  ousd: "Crypto-backed", musd: "Crypto-backed", usdtb: "Fiat-backed",
  frax_64d0: "Synthetic", usddbttcbridge: "Algorithmic",
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
const confidenceBadge = (conf, tag, populated, total, missing) => {
  if (!conf || conf === "high") return null;
  const isStandard = conf === "standard";
  const tip = isStandard
    ? `Scored with standard data coverage (${populated || "?"} of ${total || "?"} components)`
    : `Scored with limited data coverage (${populated || "?"} of ${total || "?"} components)${missing && missing.length ? ". " + missing.join(", ") + " categories have incomplete data" : ""}`;
  return (
    <span title={tip} style={{
      fontFamily: T.sans, fontSize: 10, fontWeight: 500,
      color: "#854F0B", background: "#FAEEDA", padding: "2px 8px", borderRadius: 100,
      marginLeft: 6, cursor: "help", whiteSpace: "nowrap",
    }}>partial coverage</span>
  );
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
  const [meta, setMeta] = useState({});

  useEffect(() => {
    let mounted = true;
    const load = async () => {
      try {
        const r = await apiFetch(`${API}/api/scores`);
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        const d = await r.json();
        if (mounted) {
          setData(d.stablecoins || []);
          setTs(d.timestamp);
          setMeta({ dataSourceCount: d.data_source_count, componentCount: d.sii_component_count });
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

  return { data, loading, error, ts, meta };
}

function useCoinDetail(coinId) {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    if (!coinId) return;
    setLoading(true);
    apiFetch(`${API}/api/scores/${coinId}`)
      .then((r) => { if (!r.ok) throw new Error(r.status); return r.json(); })
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
    apiFetch(`${API}/api/scores/${coinId}/history?days=${days}`)
      .then((r) => { if (!r.ok) throw new Error(r.status); return r.json(); })
      .then((d) => { setData(d.history || []); setLoading(false); })
      .catch(() => setLoading(false));
  }, [coinId, days]);

  return { data, loading };
}

function useWalletTop(limit = 50) {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  useEffect(() => {
    apiFetch(`${API}/api/wallets/top?limit=${limit}`)
      .then((r) => { if (!r.ok) throw new Error(r.status); return r.json(); })
      .then((d) => { setData(d.wallets || []); setLoading(false); })
      .catch(() => setLoading(false));
  }, [limit]);
  return { data, loading };
}

function useWalletRiskiest(limit = 50) {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  useEffect(() => {
    apiFetch(`${API}/api/wallets/riskiest?limit=${limit}`)
      .then((r) => { if (!r.ok) throw new Error(r.status); return r.json(); })
      .then((d) => { setData(d.wallets || []); setLoading(false); })
      .catch(() => setLoading(false));
  }, [limit]);
  return { data, loading };
}

function useBacklog(limit = 50) {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  useEffect(() => {
    apiFetch(`${API}/api/backlog?limit=${limit}`)
      .then((r) => { if (!r.ok) throw new Error(r.status); return r.json(); })
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
    apiFetch(`${API}/api/wallets/${addr.trim()}`)
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
      apiFetch(`${API}/api/scores/${id}/history?days=21`)
        .then((r) => { if (!r.ok) throw new Error(r.status); return r.json(); })
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
    apiFetch(`${API}/api/psi/scores`)
      .then(r => { if (!r.ok) throw new Error(r.status); return r.json(); })
      .then(d => { setData(d.protocols || d); setLoading(false); })
      .catch(() => setLoading(false));
  }, []);
  return { data, loading };
}

function useCqiMatrix() {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  useEffect(() => {
    apiFetch(`${API}/api/compose/cqi/matrix`)
      .then(r => { if (!r.ok) throw new Error(r.status); return r.json(); })
      .then(d => { setData(d); setLoading(false); })
      .catch(() => setLoading(false));
  }, []);
  return { data, loading };
}

function usePulse() {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  useEffect(() => {
    apiFetch(`${API}/api/pulse/latest`)
      .then(r => { if (!r.ok) throw new Error(r.status); return r.json(); })
      .then(d => { setData(d); setLoading(false); })
      .catch(() => setLoading(false));
  }, []);
  return { data, loading };
}

function useIntegrity() {
  const [data, setData] = useState(null);
  useEffect(() => {
    apiFetch(`${API}/api/integrity`)
      .then(r => r.ok ? r.json() : null)
      .then(d => setData(d))
      .catch(() => {});
  }, []);
  return data;
}

function useIndices() {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  useEffect(() => {
    apiFetch(`${API}/api/indices`)
      .then(r => { if (!r.ok) throw new Error(r.status); return r.json(); })
      .then(d => { setData(d); setLoading(false); })
      .catch(() => setLoading(false));
  }, []);
  return { data, loading };
}

const DOMAIN_DEPS = {
  rankings: ["sii"],
  protocols: ["psi"],
  wallets: ["wallets"],
  witness: ["cda"],
};

function domainStatus(integrity, tabId) {
  if (!integrity || !integrity.domains) return null;
  const deps = DOMAIN_DEPS[tabId];
  if (!deps) return null;
  let worst = "fresh";
  for (const d of deps) {
    const ds = integrity.domains[d];
    if (!ds) continue;
    if (ds.status === "error" || (ds.warnings && ds.warnings.some(w => w.level === "error"))) return "error";
    if (ds.status === "stale" || (ds.warnings && ds.warnings.length > 0)) worst = "stale";
  }
  return worst;
}

function StatusDot({ status }) {
  if (!status || status === "fresh") return null;
  const color = status === "error" ? "#c0392b" : "#c77b2a";
  return (
    <span style={{ display: "inline-block", width: 6, height: 6, borderRadius: "50%", background: color, marginLeft: 5, verticalAlign: "middle" }} />
  );
}

function useWitnessIssuers() {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  useEffect(() => {
    apiFetch(`${API}/api/cda/issuers`)
      .then(r => { if (!r.ok) throw new Error(r.status); return r.json(); })
      .then(d => { setData(d); setLoading(false); })
      .catch(() => setLoading(false));
  }, []);
  return { data, loading };
}

function useWitnessCoverage() {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  useEffect(() => {
    apiFetch(`${API}/api/cda/coverage`)
      .then(r => { if (!r.ok) throw new Error(r.status); return r.json(); })
      .then(d => { setData(d); setLoading(false); })
      .catch(() => setLoading(false));
  }, []);
  return { data, loading };
}

function useIssuerHistory(symbol) {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  useEffect(() => {
    if (!symbol) return;
    setLoading(true);
    apiFetch(`${API}/api/cda/issuers/${symbol}/history?days=365`)
      .then(r => { if (!r.ok) throw new Error(r.status); return r.json(); })
      .then(d => { setData(d); setLoading(false); })
      .catch(() => setLoading(false));
  }, [symbol]);
  return { data, loading };
}

function useIssuerLatest(symbol) {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  useEffect(() => {
    if (!symbol) return;
    setLoading(true);
    apiFetch(`${API}/api/cda/issuers/${symbol}/latest`)
      .then(r => { if (!r.ok) throw new Error(r.status); return r.json(); })
      .then(d => { setData(d); setLoading(false); })
      .catch(() => setLoading(false));
  }, [symbol]);
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

// =============================================================================
// PSI Detail Expansion — radar chart + category bars
// =============================================================================

const PSI_CATEGORIES = [
  { id: "balance_sheet", label: "Balance", shortLabel: "BAL", weight: 0.25 },
  { id: "revenue", label: "Revenue", shortLabel: "REV", weight: 0.20 },
  { id: "liquidity", label: "Liquidity", shortLabel: "LIQ", weight: 0.20 },
  { id: "security", label: "Security", shortLabel: "SEC", weight: 0.15 },
  { id: "governance", label: "Governance", shortLabel: "GOV", weight: 0.10 },
  { id: "token_health", label: "Token", shortLabel: "TOK", weight: 0.10 },
];

function PsiRadarChart({ categoryScores, size = 220 }) {
  const cx = size / 2, cy = size / 2, r = size * 0.38;
  const n = PSI_CATEGORIES.length;
  const angleStep = (2 * Math.PI) / n;
  const startAngle = -Math.PI / 2;

  const point = (i, pct) => {
    const angle = startAngle + i * angleStep;
    return [cx + r * pct * Math.cos(angle), cy + r * pct * Math.sin(angle)];
  };

  const gridLevels = [0.25, 0.5, 0.75, 1.0];

  const dataPoints = PSI_CATEGORIES.map((cat, i) => {
    const score = categoryScores[cat.id];
    const raw = score != null ? Math.max(0, Math.min(100, score)) / 100 : 0;
    const pct = score != null ? 0.12 + raw * 0.88 : 0;
    return point(i, pct);
  });
  const polygon = dataPoints.map(([x, y]) => `${x},${y}`).join(" ");

  return (
    <svg viewBox={`0 0 ${size} ${size}`} style={{ width: size, height: size }}>
      {/* Grid rings */}
      {gridLevels.map((lvl) => {
        const pts = Array.from({ length: n }, (_, i) => point(i, lvl));
        return (
          <polygon key={lvl}
            points={pts.map(([x, y]) => `${x},${y}`).join(" ")}
            fill="none" stroke={T.ruleLight} strokeWidth="0.5"
          />
        );
      })}
      {/* Axis lines */}
      {PSI_CATEGORIES.map((_, i) => {
        const [x, y] = point(i, 1);
        return <line key={i} x1={cx} y1={cy} x2={x} y2={y} stroke={T.ruleLight} strokeWidth="0.5" />;
      })}
      {/* Data polygon */}
      <polygon points={polygon} fill="#7BA3A8" fillOpacity="0.18" stroke="#7BA3A8" strokeWidth="1.5" />
      {/* Data dots */}
      {dataPoints.map(([x, y], i) => {
        const score = categoryScores[PSI_CATEGORIES[i].id];
        if (score == null) return null;
        return <circle key={i} cx={x} cy={y} r="2.5" fill="#7BA3A8" />;
      })}
      {/* Axis labels */}
      {PSI_CATEGORIES.map((cat, i) => {
        const [x, y] = point(i, 1.18);
        const score = categoryScores[cat.id];
        return (
          <text key={i} x={x} y={y} textAnchor="middle" dominantBaseline="middle"
            fill={score != null ? T.inkLight : T.inkFaint}
            fontSize="9" fontFamily={T.mono} fontWeight="500"
          >{cat.shortLabel}</text>
        );
      })}
    </svg>
  );
}

function PsiCategoryBar({ label, score, weight }) {
  const pct = score != null ? Math.min(100, Math.max(0, score)) : 0;
  let barColor;
  if (score == null) barColor = T.inkFaint;
  else if (score >= 75) barColor = "#2d6b45";
  else if (score >= 50) barColor = "#7BA3A8";
  else if (score >= 35) barColor = "#B8937A";
  else barColor = T.accent;

  return (
    <div style={{ marginBottom: 10 }}>
      <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 3 }}>
        <span style={{ fontSize: 11, color: T.inkLight, fontFamily: T.sans }}>
          {label}
          <span style={{ color: T.inkFaint, fontSize: 10, marginLeft: 4 }}>
            {(weight * 100).toFixed(0)}%
          </span>
        </span>
        <span style={{ fontSize: 12, fontFamily: T.mono, fontWeight: 600, color: score != null ? barColor : T.inkFaint }}>
          {score != null ? fmt(score, 1) : "—"}
        </span>
      </div>
      <div style={{ height: 4, background: T.ruleLight, borderRadius: 1 }}>
        {score != null ? (
          <div style={{ height: "100%", width: `${pct}%`, background: barColor, borderRadius: 1, transition: "width 0.6s ease" }} />
        ) : (
          <div style={{ height: "100%", width: "100%", background: `repeating-linear-gradient(90deg, ${T.ruleLight} 0, ${T.ruleLight} 4px, transparent 4px, transparent 8px)` }} />
        )}
      </div>
    </div>
  );
}

function PsiDetailPanel({ protocol, mobile }) {
  const cats = protocol.category_scores || {};
  const score = protocol.score || protocol.overall_score;

  return (
    <div style={{
      padding: mobile ? "16px 12px" : "20px 24px",
      background: T.paperWarm,
      borderBottom: `1px dotted ${T.ruleMid}`,
      animation: "fadeIn 0.25s ease",
    }}>
      {/* Header */}
      <div style={{ display: "flex", alignItems: "baseline", gap: 10, marginBottom: 16 }}>
        <span style={{ fontFamily: T.mono, fontSize: 15, fontWeight: 700 }}>
          {protocol.protocol_name || protocol.protocol_slug}
        </span>
        <ChainBadge chain={protocol.chain} />
        <span style={{ fontFamily: T.mono, fontSize: 13, fontWeight: 600, color: T.inkMid, marginLeft: "auto" }}>
          PSI {fmt(score, 1)}
        </span>
        <span style={{ fontFamily: T.sans, fontSize: 20, fontWeight: 700, color: gradeColor(protocol.grade) }}>
          {protocol.grade || "—"}
        </span>
      </div>

      {/* Two-column: Radar + Bars */}
      <div style={{
        display: "flex",
        flexDirection: mobile ? "column" : "row",
        gap: mobile ? 20 : 32,
        alignItems: mobile ? "center" : "flex-start",
      }}>
        {/* Left: Radar chart */}
        <div style={{ flexShrink: 0 }}>
          <PsiRadarChart categoryScores={cats} size={mobile ? 200 : 220} />
        </div>

        {/* Right: Category bars */}
        <div style={{ flex: 1, minWidth: 0, width: mobile ? "100%" : undefined }}>
          {PSI_CATEGORIES.map((cat) => (
            <PsiCategoryBar
              key={cat.id}
              label={cat.label}
              score={cats[cat.id] != null ? Math.round(cats[cat.id] * 10) / 10 : null}
              weight={cat.weight}
            />
          ))}
          {/* Formula */}
          <div style={{
            marginTop: 12, padding: "8px 10px",
            border: `1px solid ${T.ruleLight}`,
            fontFamily: T.mono, fontSize: 9, color: T.inkFaint, lineHeight: 1.6,
          }}>
            PSI = 0.25×Balance + 0.20×Revenue + 0.20×Liquidity + 0.15×Security + 0.10×Governance + 0.10×Token
          </div>
        </div>
      </div>
    </div>
  );
}

function PageHeader({ ts, mobile, coinCount, meta = {} }) {
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

  return (
    <TabHeader
      title={<><span style={{ fontWeight: 700 }}>Stablecoin</span> Integrity <span style={{ fontWeight: 700 }}>Index</span></>}
      formId="FORM SII-001 · BASIS PROTOCOL"
      stats={[`${coinCount || "—"} STABLECOINS`, `${meta.componentCount || 37} SCORING COMPONENTS`, `${meta.dataSourceCount || 5} LIVE DATA SOURCES`, "DETERMINISTIC METHODOLOGY", "UPDATED HOURLY"]}
      formulaLine="SII = 0.30×Peg + 0.25×Liq + 0.20×Struct + 0.15×Flow + 0.10×Dist"
      versionLabel={`Methodology v1.0 · ${timestamp}`}
      accent="#fc988f"
      mobile={mobile}
      coinCount={coinCount}
    />
  );
}

function PulseSummarySection({ mobile }) {
  const { data: pulse } = usePulse();
  if (!pulse || !pulse.summary) return null;
  const s = typeof pulse.summary === "string" ? JSON.parse(pulse.summary) : pulse.summary;
  const net = s.network_state || {};
  const pScores = s.scores || [];
  const movers = pScores.filter(c => c.delta_24h != null && Math.abs(c.delta_24h) >= 0.5)
    .sort((a, b) => Math.abs(b.delta_24h) - Math.abs(a.delta_24h));

  return (
    <div style={{ marginTop: 24, border: `1px solid ${T.ruleMid}` }}>
      <div style={{ padding: mobile ? "10px 12px" : "12px 20px", borderBottom: `1px solid ${T.ruleLight}`, display: "flex", justifyContent: "space-between", alignItems: mobile ? "flex-start" : "center", flexDirection: mobile ? "column" : "row", gap: mobile ? 4 : 0 }}>
        <div style={{ fontFamily: T.mono, fontSize: 10, fontWeight: 600, color: T.inkLight, textTransform: "uppercase", letterSpacing: 1.5 }}>
          Daily State Commitment
        </div>
        <div style={{ fontFamily: T.mono, fontSize: 9, color: T.inkFaint }}>
          {s.pulse_date || pulse.pulse_date || "—"}
        </div>
      </div>

      <div style={{
        display: "grid",
        gridTemplateColumns: mobile ? "1fr 1fr" : "1fr 1fr 1fr 1fr",
        borderBottom: `1px solid ${T.ruleLight}`,
      }}>
        {[
          { label: "Tracked", value: fmtB(net.total_tracked_usd) },
          { label: "Wallets", value: net.wallets_active != null ? net.wallets_active.toLocaleString() : "—" },
          { label: "Avg Risk", value: net.avg_risk_score != null ? fmt(net.avg_risk_score, 1) : "—" },
          { label: "Data Points", value: (pScores.length * 51).toLocaleString() },
        ].map((item, i) => (
          <div key={i} style={{
            padding: mobile ? "10px 12px" : "12px 20px",
            borderRight: (!mobile || i % 2 === 0) ? `1px solid ${T.ruleLight}` : "none",
            borderBottom: mobile && i < 2 ? `1px solid ${T.ruleLight}` : "none",
          }}>
            <div style={{ fontFamily: T.mono, fontSize: 8, textTransform: "uppercase", letterSpacing: 1.5, color: T.inkFaint, marginBottom: 2 }}>{item.label}</div>
            <div style={{ fontFamily: T.mono, fontSize: mobile ? 16 : 18, fontWeight: 700, color: T.ink }}>{item.value}</div>
          </div>
        ))}
      </div>

      {movers.length > 0 && (
        <div style={{ padding: mobile ? "8px 12px" : "10px 20px", borderBottom: `1px solid ${T.ruleLight}`, display: "flex", alignItems: "center", gap: 8, flexWrap: "wrap" }}>
          <span style={{ fontFamily: T.mono, fontSize: 9, color: T.inkFaint, textTransform: "uppercase", letterSpacing: 1 }}>Movers</span>
          {movers.slice(0, 5).map(m => (
            <span key={m.symbol} style={{
              fontFamily: T.mono, fontSize: 11, padding: "2px 8px",
              border: `1px solid ${T.ruleLight}`,
              color: m.delta_24h > 0 ? "#2d6b45" : T.accent,
            }}>
              {m.symbol} {m.delta_24h > 0 ? "+" : ""}{fmt(m.delta_24h, 2)}
            </span>
          ))}
        </div>
      )}

      {pulse.content_hash && (
        <div style={{ padding: mobile ? "6px 12px" : "6px 20px", fontFamily: T.mono, fontSize: 9, color: T.inkFaint }}>
          Content hash: {pulse.content_hash}
        </div>
      )}
    </div>
  );
}

function RankingsView({ scores, loading, onSelect, ts, mobile, meta }) {
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
            {confidenceBadge(coin.confidence, coin.confidence_tag, coin.components_populated, coin.components_total, coin.missing_categories)}
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
            {confidenceBadge(coin.confidence, coin.confidence_tag, coin.components_populated, coin.components_total, coin.missing_categories)}
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
      <PageHeader ts={ts} mobile={mobile} coinCount={scores.length} meta={meta} />

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
          SII scores are informational and do not constitute financial advice. Methodology is deterministic and version-controlled. No issuer can pay to influence scores, weights, or thresholds. Scores update hourly.
        </p>
      </div>

    </div>
  );
}

function DetailView({ coinId, onBack, mobile }) {
  const { data: coin, loading: detailLoading } = useCoinDetail(coinId);
  const { data: history, loading: histLoading } = useCoinHistory(coinId, 90);
  const [expandedCats, setExpandedCats] = useState({});

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
            {confidenceBadge(coin.confidence, coin.confidence_tag, coin.components_populated, coin.components_total, coin.missing_categories)}
          </div>
          <div style={{ fontSize: mobile ? 10 : 12, color: T.inkLight, marginTop: 6, fontFamily: T.sans, lineHeight: 1.5 }}>
            Issued by {coin.issuer} · {coin.components_populated || coin.component_count || "—"} of {coin.components_total || "—"} components · {RESERVE_TYPE[coin.id] || "Pending"} · MiCA: {MICA_STATUS[coin.id] || "Pending"}
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

      {coin.components && coin.components.length > 0 && (() => {
        const grouped = coin.components.reduce((acc, c) => {
          const cat = c.category || "other";
          if (!acc[cat]) acc[cat] = [];
          acc[cat].push(c);
          return acc;
        }, {});

        const catOrder = [
          "peg_stability", "liquidity_depth", "mint_burn", "flows",
          "holder_distribution", "market_activity", "network",
          "smart_contract", "transparency", "regulatory", "governance"
        ];
        const sortedCats = Object.keys(grouped).sort((a, b) => {
          const ai = catOrder.indexOf(a), bi = catOrder.indexOf(b);
          if (ai !== -1 && bi !== -1) return ai - bi;
          if (ai !== -1) return -1;
          if (bi !== -1) return 1;
          return a.localeCompare(b);
        });

        const fmtRaw = (v) => {
          if (v == null) return "—";
          const n = Number(v);
          if (isNaN(n)) return String(v);
          if (Math.abs(n) >= 1e9) return (n / 1e9).toFixed(1) + "B";
          if (Math.abs(n) >= 1e6) return (n / 1e6).toFixed(1) + "M";
          if (Math.abs(n) >= 1e3) return (n / 1e3).toFixed(1) + "K";
          if (n === 0) return "0";
          if (Math.abs(n) < 1) return n.toFixed(4);
          if (n >= 0 && n <= 100 && n % 1 !== 0) return n.toFixed(1);
          return n.toFixed(2);
        };

        const fmtName = (id) => (id || "").replace(/_/g, " ").replace(/\b\w/g, c => c.toUpperCase());

        return (
          <div style={{ border: `1px solid ${T.ruleMid}`, padding: "16px 20px" }}>
            <div style={{ fontSize: 10, fontWeight: 600, color: T.inkLight, textTransform: "uppercase", letterSpacing: 1.5, marginBottom: 12, fontFamily: T.mono }}>
              Component Readings · {coin.components.length} unique
            </div>
            <div style={{ maxHeight: 400, overflowY: "auto" }}>
              {sortedCats.map(cat => {
                const comps = grouped[cat].sort((a, b) => (a.normalized_score || 0) - (b.normalized_score || 0));
                const avg = comps.reduce((s, c) => s + (c.normalized_score || 0), 0) / comps.length;
                const isOpen = expandedCats[cat];
                return (
                  <div key={cat} style={{ marginBottom: 8 }}>
                    <div
                      onClick={() => setExpandedCats(prev => ({ ...prev, [cat]: !prev[cat] }))}
                      style={{
                        display: "flex", justifyContent: "space-between", alignItems: "center",
                        padding: "6px 0", cursor: "pointer", userSelect: "none",
                        borderBottom: `1px solid ${T.ruleLight}`,
                      }}
                    >
                      <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                        <span style={{ fontSize: 10, color: T.inkFaint, fontFamily: T.mono }}>{isOpen ? "▾" : "▸"}</span>
                        <span style={{ fontSize: 10, fontWeight: 600, color: T.inkMid, textTransform: "uppercase", letterSpacing: 1, fontFamily: T.mono }}>
                          {cat.replace(/_/g, " ")}
                        </span>
                        <span style={{ fontSize: 9, color: T.inkFaint, fontFamily: T.mono }}>{comps.length}</span>
                      </div>
                      <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                        <div style={{ width: 40, height: 3, background: T.ruleLight, borderRadius: 1 }}>
                          <div style={{ width: `${Math.min(avg, 100)}%`, height: 3, background: subScoreColor(avg), borderRadius: 1 }} />
                        </div>
                        <span style={{ fontSize: 10, fontWeight: 600, color: subScoreColor(avg), fontFamily: T.mono, minWidth: 28, textAlign: "right" }}>
                          {fmt(avg, 1)}
                        </span>
                      </div>
                    </div>
                    {isOpen && comps.map((comp, i) => (
                      <div
                        key={`${comp.id}-${i}`}
                        style={{
                          display: "flex", justifyContent: "space-between", alignItems: "center",
                          padding: "4px 0 4px 20px", borderBottom: `1px solid ${T.ruleLight}`,
                          fontSize: 11,
                        }}
                      >
                        <span style={{ color: T.inkLight, fontFamily: T.sans }}>{fmtName(comp.id)}</span>
                        <div style={{ display: "flex", gap: 12, alignItems: "center" }}>
                          <span style={{ color: T.inkFaint, fontFamily: T.mono, fontSize: 10 }}>{fmtRaw(comp.raw_value)}</span>
                          <div style={{ width: 40, height: 3, background: T.ruleLight, borderRadius: 1 }}>
                            <div style={{ width: `${Math.min(comp.normalized_score || 0, 100)}%`, height: 3, background: subScoreColor(comp.normalized_score), borderRadius: 1 }} />
                          </div>
                          <span style={{ color: subScoreColor(comp.normalized_score), fontFamily: T.mono, fontWeight: 600, minWidth: 36, textAlign: "right" }}>
                            {fmt(comp.normalized_score, 1)}
                          </span>
                        </div>
                      </div>
                    ))}
                  </div>
                );
              })}
            </div>
          </div>
        );
      })()}
    </div>
  );
}

function MethodologyView({ mobile }) {
  const { data: indicesData, loading } = useIndices();

  if (loading) {
    return (
      <div style={{ padding: 40, display: "flex", justifyContent: "center" }}>
        <div style={{ color: T.inkFaint, fontFamily: T.mono, fontSize: 12 }}>Loading methodology...</div>
      </div>
    );
  }

  const indices = indicesData?.indices || [];
  const gradeScale = indicesData?.grade_scale || {};
  const principles = indicesData?.principles || [];
  const dataSources = indicesData?.data_sources || [];

  return (
    <div style={{ padding: mobile ? "16px 0 32px" : "24px 0 64px" }}>

      <TabHeader
        title={<><span style={{ fontWeight: 700 }}>Scoring</span> Methodology</>}
        formId="FORM MTH-001 · BASIS PROTOCOL"
        stats={[
          `${indices.length} INDICES`,
          `${dataSources.length} DATA SOURCES`,
          "DETERMINISTIC",
          "VERSION-CONTROLLED",
        ]}
        accent="#B8937A"
        mobile={mobile}
        showOnChain={false}
      />

      <div style={{ height: 24 }} />

      {/* Intro + Principles */}
      <section style={{ marginBottom: 28 }}>
        <div style={{ border: `1px solid ${T.ruleMid}`, padding: "20px 24px" }}>
          <p style={{ margin: "0 0 20px", fontSize: 13, color: T.inkMid, fontFamily: T.sans, lineHeight: 1.7 }}>
            Basis produces {indices.length} composable risk indices. All use the same deterministic scoring engine — the same inputs always produce the same outputs. Methodologies are version-controlled, changes announced in advance, and all scores retroactively reproducible. New indices are JSON configurations against the generic engine — no code changes required.
          </p>
          <div style={{ borderTop: `1px solid ${T.ruleLight}`, paddingTop: 16 }}>
            {principles.map((p, i) => (
              <div key={i} style={{ padding: "10px 0", borderBottom: i < principles.length - 1 ? `1px solid ${T.ruleLight}` : "none" }}>
                <div style={{ fontSize: 13, fontWeight: 600, color: T.ink, fontFamily: T.sans }}>{p.title}</div>
                <div style={{ fontSize: 12, color: T.inkLight, marginTop: 3, fontFamily: T.sans, lineHeight: 1.5 }}>{p.desc}</div>
              </div>
            ))}
          </div>
        </div>
      </section>

      {/* Each index */}
      {indices.map((idx) => (
        <section key={idx.index_id} style={{ marginBottom: 28 }}>
          <div style={{ border: `1px solid ${T.ruleMid}`, padding: "20px 24px" }}>
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: mobile ? "flex-start" : "baseline", flexDirection: mobile ? "column" : "row", gap: mobile ? 4 : 0, marginBottom: 12 }}>
              <h2 style={{ margin: 0, fontSize: 14, fontWeight: 600, color: T.ink, fontFamily: T.sans }}>
                {idx.name} ({idx.index_id.toUpperCase()})
              </h2>
              <span style={{ fontFamily: T.mono, fontSize: 10, color: T.inkFaint }}>{idx.version}</span>
            </div>
            <p style={{ margin: "0 0 16px", fontSize: 13, color: T.inkMid, fontFamily: T.sans, lineHeight: 1.7 }}>
              {idx.description}
            </p>

            {/* Formula */}
            <div style={{ fontSize: 10, fontWeight: 600, color: T.inkLight, textTransform: "uppercase", letterSpacing: 1.5, marginBottom: 8, fontFamily: T.mono }}>
              Formula
            </div>
            <div style={{ fontFamily: T.mono, fontSize: mobile ? 12 : 14, color: T.ink, lineHeight: 2.2, padding: "8px 0", marginBottom: 14, borderBottom: idx.categories.length > 0 ? `1px solid ${T.ruleLight}` : "none" }}>
              {idx.formula}
            </div>

            {/* Categories (only for scored indices, not compositions) */}
            {idx.categories.length > 0 && (
              <>
                <div style={{ fontSize: 10, fontWeight: 600, color: T.inkLight, textTransform: "uppercase", letterSpacing: 1.5, marginBottom: 14, fontFamily: T.mono }}>
                  Categories
                </div>
                {idx.categories.map((cat, i) => (
                  <div key={cat.id} style={{ display: "flex", gap: 16, padding: "14px 0", borderBottom: i < idx.categories.length - 1 ? `1px solid ${T.ruleLight}` : "none" }}>
                    <div style={{ minWidth: 44, textAlign: "right", fontFamily: T.mono, fontWeight: 700, fontSize: 18, color: T.ink }}>
                      {Math.round(cat.weight * 100)}%
                    </div>
                    <div>
                      <div style={{ fontWeight: 600, fontSize: 13, color: T.ink, fontFamily: T.sans }}>{cat.name}</div>
                      <div style={{ fontSize: 11, color: T.inkLight, marginTop: 3, lineHeight: 1.5, fontFamily: T.mono }}>
                        {cat.components.join(" \u00b7 ")}
                      </div>
                      <div style={{ fontSize: 10, color: T.inkFaint, marginTop: 3, fontFamily: T.mono }}>{cat.component_count} components</div>
                    </div>
                  </div>
                ))}
              </>
            )}

            {idx.total_components > 0 && (
              <div style={{ marginTop: 12, fontFamily: T.mono, fontSize: 10, color: T.inkFaint }}>
                {idx.total_components} total components · {idx.entity_type} scoring
              </div>
            )}
          </div>
        </section>
      ))}

      {/* Grade Scale */}
      <section style={{ marginBottom: 28 }}>
        <div style={{ border: `1px solid ${T.ruleMid}`, padding: "20px 24px" }}>
          <div style={{ fontSize: 10, fontWeight: 600, color: T.inkLight, textTransform: "uppercase", letterSpacing: 1.5, marginBottom: 14, fontFamily: T.mono }}>
            Grade Scale
          </div>
          <div style={{ display: "grid", gridTemplateColumns: mobile ? "repeat(3, 1fr)" : "repeat(6, 1fr)", gap: 6 }}>
            {Object.entries(gradeScale).map(([grade, range]) => (
              <div key={grade} style={{ padding: "8px 6px", textAlign: "center", border: `1px solid ${T.ruleLight}` }}>
                <div style={{ fontWeight: 700, fontSize: 14, color: gradeColor(grade), fontFamily: T.mono }}>{grade}</div>
                <div style={{ fontSize: 9, color: T.inkFaint, marginTop: 2, fontFamily: T.mono }}>{range}</div>
              </div>
            ))}
          </div>
        </div>
      </section>

      {/* Confidence Indicators */}
      <section style={{ marginBottom: 28 }}>
        <div style={{ border: `1px solid ${T.ruleMid}`, padding: "20px 24px" }}>
          <div style={{ fontSize: 10, fontWeight: 600, color: T.inkLight, textTransform: "uppercase", letterSpacing: 1.5, marginBottom: 14, fontFamily: T.mono }}>
            Confidence Indicators
          </div>
          <p style={{ margin: "0 0 14px", fontSize: 13, color: T.inkMid, fontFamily: T.sans, lineHeight: 1.7 }}>
            Every score includes a confidence indicator based on data coverage. A stablecoin or protocol is only scored when every risk dimension has at least one data point (category completeness). The confidence tier reflects how many components within those categories are populated.
          </p>
          <div style={{ display: "grid", gridTemplateColumns: mobile ? "1fr" : "1fr 1fr 1fr", gap: 10 }}>
            {[
              { level: "High", range: "\u226580% component coverage", desc: "Full confidence. All categories well-populated.", color: T.ink, bg: "transparent" },
              { level: "Standard", range: "60\u201379% coverage", desc: "Reliable score with minor data gaps in some categories.", color: "#b8860b", bg: "rgba(234,179,8,0.12)" },
              { level: "Limited", range: "<60% coverage", desc: "Structurally complete but fewer data points per category.", color: "#c0392b", bg: "rgba(239,68,68,0.10)" },
            ].map((t) => (
              <div key={t.level} style={{ padding: "10px 12px", border: `1px solid ${T.ruleLight}` }}>
                <div style={{ display: "flex", alignItems: "center", gap: 6, marginBottom: 6 }}>
                  <span style={{ fontFamily: T.mono, fontSize: 12, fontWeight: 700, color: t.color }}>{t.level}</span>
                  {t.level !== "High" && (
                    <span style={{ fontFamily: T.sans, fontSize: 10, fontWeight: 500, color: "#854F0B", background: "#FAEEDA", padding: "2px 8px", borderRadius: 100 }}>partial coverage</span>
                  )}
                </div>
                <div style={{ fontFamily: T.mono, fontSize: 10, color: T.inkFaint, marginBottom: 4 }}>{t.range}</div>
                <div style={{ fontFamily: T.sans, fontSize: 11, color: T.inkLight, lineHeight: 1.5 }}>{t.desc}</div>
              </div>
            ))}
          </div>
        </div>
      </section>

      {/* Data Sources */}
      <section style={{ marginBottom: 28 }}>
        <div style={{ border: `1px solid ${T.ruleMid}`, padding: "20px 24px" }}>
          <div style={{ fontSize: 10, fontWeight: 600, color: T.inkLight, textTransform: "uppercase", letterSpacing: 1.5, marginBottom: 12, fontFamily: T.mono }}>
            Data Sources
          </div>
          <div style={{ display: "flex", flexWrap: "wrap", gap: 6 }}>
            {dataSources.map((s) => (
              <span key={s} style={{ padding: "4px 10px", background: T.paperWarm, color: T.inkMid, fontSize: 11, fontFamily: T.sans, border: `1px solid ${T.ruleMid}` }}>
                {s}
              </span>
            ))}
          </div>
        </div>
      </section>

    </div>
  );
}

const WALLET_COL_DESKTOP = "32px 170px 90px 60px 52px 140px 80px";
const WALLET_COL_MOBILE = "170px 80px 52px 48px";

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
          <span>Concentration</span>
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
      <a href={`/wallet/${wallet.address}`}
        style={{ fontFamily: T.mono, fontSize: 11, color: T.inkMid, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", textDecoration: "none" }}
        onMouseEnter={(e) => e.target.style.textDecoration = "underline"}
        onMouseLeave={(e) => e.target.style.textDecoration = "none"}
      >
        {truncAddr(wallet.address)}
      </a>
      <span style={{ fontFamily: T.mono, fontSize: 11, color: T.ink }}>{fmtB(wallet.total_stablecoin_value)}</span>
      <span style={{ fontFamily: T.mono, fontSize: 12, fontWeight: 600, color: scoreColor }}>
        {wallet.risk_score != null ? fmt(wallet.risk_score, 1) : "—"}
      </span>
      <span style={{ fontFamily: T.sans, fontSize: mobile ? 16 : 20, fontWeight: 700, color: gradeColor(wallet.risk_grade) }}>
        {wallet.risk_grade || "—"}
      </span>
      {!mobile && (
        <>
          <span style={{ fontFamily: T.mono, fontSize: 11, color: T.inkMid }}>
            {wallet.concentration_hhi != null && wallet.concentration_hhi >= 5000
              ? "Concentrated"
              : wallet.concentration_hhi != null && wallet.concentration_hhi >= 1500
                ? "Mixed"
                : wallet.concentration_hhi != null
                  ? "Diversified"
                  : "—"
            }
          </span>
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
  // Compute actual value from current holdings — authoritative, never uses cached/inflated values
  const holdingsValue = holdings.reduce((sum, h) => sum + (parseFloat(h.value_usd) || 0), 0);

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
                { label: "Address", value: w.address, isLink: true },
                { label: "Value", value: fmtB(holdingsValue || 0) },
                { label: "Size Tier", value: (w.size_tier || "—").toUpperCase() },
                { label: "Source", value: w.source || "—" },
                { label: "Contract", value: w.is_contract ? "Yes" : "No" },
              ].map((item) => (
                <div key={item.label} style={{ fontFamily: T.mono, fontSize: mobile ? 10 : 10.5 }}>
                  <span style={{ color: T.inkFaint }}>{item.label}: </span>
                  {item.isLink ? (
                    <a href={`/wallet/${item.value}`}
                      style={{ color: T.inkMid, wordBreak: "break-all", textDecoration: "none" }}
                      onMouseEnter={(e) => e.target.style.textDecoration = "underline"}
                      onMouseLeave={(e) => e.target.style.textDecoration = "none"}
                    >{item.value}</a>
                  ) : (
                    <span style={{ color: T.inkMid, wordBreak: "break-all" }}>{item.value}</span>
                  )}
                </div>
              ))}
            </div>
            <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
              {[
                { label: "Risk Score", value: fmt(r.risk_score, 1), valueStyle: { color: subScoreColor(r.risk_score), fontWeight: 600 } },
                { label: "Grade", value: r.risk_grade || "—", valueStyle: { color: gradeColor(r.risk_grade), fontWeight: 700, fontSize: 14 } },
                { label: "HHI", value: `${fmtHHI(r.concentration_hhi)} · ${r.concentration_hhi != null && r.concentration_hhi >= 5000 ? "Concentrated" : r.concentration_hhi != null && r.concentration_hhi >= 1500 ? "Mixed" : r.concentration_hhi != null ? "Diversified" : "—"}` },
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
                      {holdingsValue > 0 ? fmt((parseFloat(h.value_usd) || 0) / holdingsValue * 100, 1) + "%" : "—"}
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

function WalletsView({ mobile, integrity }) {
  const { data: topWallets, loading: topLoading } = useWalletTop(50);
  const { data: riskyWallets, loading: riskyLoading } = useWalletRiskiest(50);
  const { data: backlog, loading: backlogLoading } = useBacklog(50);
  const [showDiversifiedOnly, setShowDiversifiedOnly] = useState(false);

  const filteredTopWallets = showDiversifiedOnly
    ? (topWallets || []).filter(w => w.concentration_hhi != null && w.concentration_hhi < 5000)
    : topWallets;

  const walletDomain = integrity?.domains?.wallets || {};
  const edgeDomain = integrity?.domains?.edges || {};
  const walletCount = walletDomain.row_count || (topWallets ? topWallets.length : 0);
  const edgeCount = edgeDomain.row_count || 0;

  return (
    <div>
      <div style={{ height: mobile ? 16 : 28 }} />

      <TabHeader
        title={<><span style={{ fontWeight: 700 }}>Wallet</span> Risk <span style={{ fontWeight: 700 }}>Graph</span></>}
        formId="FORM WRG-001 · BASIS PROTOCOL"
        stats={[
          `${walletCount ? walletCount.toLocaleString() : "—"} WALLETS TRACKED`,
          `${edgeCount ? edgeCount.toLocaleString() : "—"} TRANSFER EDGES`,
          "4 CHAINS",
        ]}
        formulaLine="Risk = f(concentration, coverage quality, behavioral signals)"
        versionLabel="WRG v0.1.0"
        accent="#8B7B9E"
        mobile={mobile}
        showOnChain={true}
      />

      <div style={{ height: 24 }} />

      <WalletSearchPanel mobile={mobile} />

      <div style={{ marginBottom: 32 }}>
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 10 }}>
          <div style={{ fontFamily: T.mono, fontSize: 9, textTransform: "uppercase", letterSpacing: 1.5, color: T.inkLight }}>
            Top Wallets · By Stablecoin Value
          </div>
          <div style={{ display: "flex", gap: 0 }}>
            <button
              onClick={() => setShowDiversifiedOnly(false)}
              style={{
                padding: "3px 10px",
                border: `1px solid ${T.ruleMid}`,
                borderRight: "none",
                background: !showDiversifiedOnly ? T.ink : "transparent",
                color: !showDiversifiedOnly ? T.paper : T.inkLight,
                fontFamily: T.mono,
                fontSize: 9,
                textTransform: "uppercase",
                letterSpacing: 1,
                cursor: "pointer",
              }}
            >
              All
            </button>
            <button
              onClick={() => setShowDiversifiedOnly(true)}
              style={{
                padding: "3px 10px",
                border: `1px solid ${T.ruleMid}`,
                background: showDiversifiedOnly ? T.ink : "transparent",
                color: showDiversifiedOnly ? T.paper : T.inkLight,
                fontFamily: T.mono,
                fontSize: 9,
                textTransform: "uppercase",
                letterSpacing: 1,
                cursor: "pointer",
              }}
            >
              Diversified
            </button>
          </div>
        </div>
        {topLoading ? (
          <div style={{ padding: 24, fontFamily: T.mono, fontSize: 12, color: T.inkFaint }}>Loading wallets...</div>
        ) : !filteredTopWallets || filteredTopWallets.length === 0 ? (
          <div style={{ padding: 24, fontFamily: T.mono, fontSize: 12, color: T.inkFaint }}>
            {showDiversifiedOnly ? "No diversified wallets found (HHI < 5000). Try 'All' view." : "No wallet data yet. Run the indexer pipeline first."}
          </div>
        ) : (
          <div style={{ border: `1px solid ${T.ruleMid}` }}>
            <WalletTableHeader mobile={mobile} />
            {filteredTopWallets.map((w, i) => (
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

function DriftExploitBanner() {
  const [dismissed, setDismissed] = useState(() => sessionStorage.getItem("drift_banner_dismissed") === "1");
  const [driftData, setDriftData] = useState(null);

  useEffect(() => {
    if (dismissed) return;
    // Auto-hide after April 7, 2026
    if (new Date() > new Date("2026-04-08T00:00:00Z")) { setDismissed(true); return; }
    apiFetch(`${API}/api/psi/scores/drift`)
      .then(r => r.ok ? r.json() : null)
      .then(d => { if (d) setDriftData(d); })
      .catch(() => {});
  }, [dismissed]);

  if (dismissed || !driftData) return null;

  const score = driftData.score != null ? driftData.score.toFixed(1) : "—";
  const grade = driftData.grade || "—";

  return (
    <div style={{
      border: `1px solid #c0392b`,
      background: "rgba(192,57,43,0.06)",
      padding: "12px 16px",
      marginBottom: 20,
      fontFamily: "var(--mono, 'JetBrains Mono', monospace)",
      fontSize: 11,
      position: "relative",
    }}>
      <button
        onClick={() => { setDismissed(true); sessionStorage.setItem("drift_banner_dismissed", "1"); }}
        style={{
          position: "absolute", top: 8, right: 12, background: "none", border: "none",
          cursor: "pointer", fontSize: 14, color: "#c0392b", fontWeight: 700,
        }}
        aria-label="Dismiss"
      >&times;</button>
      <div style={{ fontWeight: 700, marginBottom: 4, color: "#c0392b", textTransform: "uppercase", letterSpacing: 1.2, fontSize: 10 }}>
        LIVE: Drift Protocol Exploit — ~$270M Drained
      </div>
      <div style={{ color: "#555", lineHeight: 1.5 }}>
        Basis is scoring Drift in real-time. PSI Score: <strong>{score} ({grade})</strong>.
        {" "}First Solana protocol in the index.
      </div>
    </div>
  );
}

const PROTO_SHORT_NAMES = {
  "jupiter-perpetual-exchange": "Jupiter",
  "compound-finance": "Compound",
  "convex-finance": "Convex",
  "curve-finance": "Curve",
  "eigenlayer": "Eigen",
};

function ChainBadge({ chain }) {
  if (!chain || chain === "ethereum") return null;
  const styles = {
    solana: { bg: "#EEEDFE", color: "#534AB7", text: "SOL" },
  };
  const s = styles[chain] || { bg: "#F1EFE8", color: "#5F5E5A", text: chain.slice(0, 3).toUpperCase() };
  return (
    <span style={{
      fontSize: 8, fontWeight: 700, padding: "1px 5px", borderRadius: 3,
      marginLeft: 6, background: s.bg, color: s.color, letterSpacing: 0.5,
      verticalAlign: "middle",
    }}>{s.text}</span>
  );
}

function ProtocolsView({ mobile }) {
  const { data: protocols, loading: psiLoading } = usePsiScores();
  const { data: cqiData, loading: cqiLoading } = useCqiMatrix();
  const [expandedSlug, setExpandedSlug] = useState(null);

  const sorted = protocols ? [...protocols].sort((a, b) => (b.score || b.overall_score || 0) - (a.score || a.overall_score || 0)) : [];

  return (
    <div>
      <div style={{ height: mobile ? 16 : 28 }} />

      <TabHeader
        title={<><span style={{ fontWeight: 700 }}>Protocol</span> Solvency <span style={{ fontWeight: 700 }}>Index</span></>}
        formId="FORM PSI-001 · BASIS PROTOCOL"
        stats={[
          `${protocols ? protocols.length : "—"} PROTOCOLS SCORED`,
          `${cqiData ? cqiData.count : "—"} CQI PAIRS`,
          "PSI v0.1.0",
        ]}
        formulaLine="PSI = 0.25×Balance Sheet + 0.20×Revenue + 0.20×Liquidity + 0.15×Security + 0.10×Governance + 0.10×Token"
        versionLabel="PSI v0.1.0"
        accent="#7BA3A8"
        mobile={mobile}
        showOnChain={true}
      />

      <div style={{ height: 24 }} />

      <DriftExploitBanner />

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
              gridTemplateColumns: mobile ? "16px 32px 1fr 60px 40px" : "16px 32px 1fr 80px 80px 80px 80px 60px 40px",
              padding: "8px 16px",
              background: T.paper,
              borderBottom: `3px solid ${T.ink}`,
              fontFamily: T.mono, fontSize: 9, textTransform: "uppercase",
              letterSpacing: 1.5, color: T.inkLight,
            }}>
              <span></span>
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
              const isExpanded = expandedSlug === p.protocol_slug;
              return (
                <div key={p.protocol_slug}>
                  <div
                    onClick={() => setExpandedSlug(isExpanded ? null : p.protocol_slug)}
                    style={{
                      display: "grid",
                      gridTemplateColumns: mobile ? "16px 32px 1fr 60px 40px" : "16px 32px 1fr 80px 80px 80px 80px 60px 40px",
                      padding: "11px 16px",
                      borderBottom: isExpanded ? "none" : `1px dotted ${T.ruleMid}`,
                      alignItems: "center",
                      cursor: "pointer",
                      background: isExpanded ? T.paperWarm : "transparent",
                      transition: "background 0.15s ease",
                    }}
                  >
                    <span style={{ fontFamily: T.mono, fontSize: 9, color: T.inkFaint, transition: "transform 0.2s ease", display: "inline-block", transform: isExpanded ? "rotate(90deg)" : "none" }}>▶</span>
                    <span style={{ fontFamily: T.mono, fontSize: 11, color: T.inkFaint }}>{i + 1}</span>
                    <span style={{ fontFamily: T.mono, fontSize: 13, fontWeight: 700, color: T.ink }}>
                      {p.protocol_name || p.protocol_slug}
                      <ChainBadge chain={p.chain} />
                    </span>
                    {!mobile && <span style={{ fontFamily: T.mono, fontSize: 11, color: subScoreColor(cats.balance_sheet) }}>{fmt(cats.balance_sheet, 0)}</span>}
                    {!mobile && <span style={{ fontFamily: T.mono, fontSize: 11, color: subScoreColor(cats.revenue) }}>{fmt(cats.revenue, 0)}</span>}
                    {!mobile && <span style={{ fontFamily: T.mono, fontSize: 11, color: subScoreColor(cats.security) }}>{fmt(cats.security, 0)}</span>}
                    {!mobile && <span style={{ fontFamily: T.mono, fontSize: 11, color: subScoreColor(cats.governance) }}>{fmt(cats.governance, 0)}</span>}
                    <span style={{ fontFamily: T.mono, fontSize: 13, fontWeight: 600, color: T.ink }}>{fmt(p.score || p.overall_score, 1)}</span>
                    <span style={{ fontFamily: T.sans, fontSize: 18, fontWeight: 700, color: gradeColor(p.grade) }}>{p.grade || "—"}</span>
                    {confidenceBadge(p.confidence, p.confidence_tag, p.components_populated, p.components_total, p.missing_categories)}
                  </div>
                  {isExpanded && <PsiDetailPanel protocol={p} mobile={mobile} />}
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
                    {r.confidence && r.confidence !== "high" && confidenceBadge(r.confidence, null, null, null, null)}
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
                    {protos.map(p => {
                      const slug = protoSlugMap[p];
                      const short = PROTO_SHORT_NAMES[slug] || p;
                      const isSolana = slug && ["drift", "jupiter-perpetual-exchange", "raydium"].includes(slug);
                      return (
                        <th key={p} title={p} style={{
                          padding: "8px 6px", textAlign: "center", fontSize: 9,
                          textTransform: "uppercase", letterSpacing: 0.5, color: T.inkLight, maxWidth: 90,
                          borderBottom: isSolana ? "2px solid #534AB7" : undefined,
                        }}>
                          {short}
                        </th>
                      );
                    })}
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

function PulseView({ mobile, integrity }) {
  const { data: pulse, loading } = usePulse();
  const [divergence, setDivergence] = useState(null);
  useEffect(() => {
    apiFetch(`${API}/api/divergence`)
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

      <TabHeader
        title={<><span style={{ fontWeight: 700 }}>Daily</span> Pulse</>}
        formId={`${s.pulse_date || pulse.pulse_date || "—"} · FORM PLS-001 · BASIS PROTOCOL`}
        accent="#C4A882"
        mobile={mobile}
        showOnChain={false}
      />

      {/* Pulse detail — network stats */}
      <div style={{ border: `1.5px solid ${T.ink}`, borderTop: "none", marginBottom: 24 }}>
        {/* Network state summary — big numbers */}
        <div style={{
          display: "grid",
          gridTemplateColumns: mobile ? "1fr 1fr" : "1fr 1fr 1fr 1fr 1fr",
          borderBottom: `1px solid ${T.ruleMid}`,
        }}>
          {[
            { label: "Tracked", value: fmtB(net.total_tracked_usd) },
            { label: "Wallets", value: net.wallets_active != null ? net.wallets_active.toLocaleString() : (net.wallets_scored != null ? net.wallets_scored.toLocaleString() : "—") },
            { label: "Avg Risk", value: net.avg_risk_score != null ? fmt(net.avg_risk_score, 1) : "—" },
            { label: "Stablecoins", value: net.stablecoins_scored || scores.length || "—" },
            { label: "Data Points", value: (scores.length * 51).toLocaleString() },
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

        {/* Data freshness line */}
        {integrity && integrity.domains && (
          <div style={{
            borderTop: `1px solid ${T.ruleLight}`,
            padding: mobile ? "6px 12px" : "6px 24px",
            fontFamily: T.mono, fontSize: 9, color: T.inkFaint,
          }}>
            {["sii", "wallets", "events", "pulse"].map(d => {
              const dm = integrity.domains[d];
              if (!dm) return null;
              const label = d === "sii" ? "SII" : d.charAt(0).toUpperCase() + d.slice(1);
              const age = dm.age_hours;
              const txt = age == null ? "—" : age < 1 ? `${Math.round(age * 60)}m ago` : `${Math.round(age)}h ago`;
              return `${label}: ${txt}`;
            }).filter(Boolean).join(" · ")}
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
                    <a href={`/wallet/${evt.wallet}`} style={{ color: T.ink, textDecoration: "none" }} onMouseEnter={(e) => e.target.style.textDecoration = "underline"} onMouseLeave={(e) => e.target.style.textDecoration = "none"}>{truncAddr(evt.wallet)}</a> · Score: {evt.score != null ? fmt(evt.score, 1) : "—"}
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

function WitnessView({ mobile, onSelectIssuer }) {
  const { data: issuersData, loading: issuersLoading } = useWitnessIssuers();
  const { data: coverageData } = useWitnessCoverage();
  const [latestHashes, setLatestHashes] = useState({});
  const [copiedHash, setCopiedHash] = useState(null);

  const issuers = issuersData?.issuers || [];

  useEffect(() => {
    issuers.forEach(iss => {
      if (!latestHashes[iss.asset_symbol]) {
        apiFetch(`${API}/api/cda/issuers/${iss.asset_symbol}/latest`)
          .then(r => { if (!r.ok) throw new Error(r.status); return r.json(); })
          .then(d => {
            setLatestHashes(prev => ({ ...prev, [iss.asset_symbol]: d.evidence_hash || null }));
          })
          .catch(() => {});
      }
    });
  }, [issuers.length]);

  const copyHash = (hash) => {
    navigator.clipboard.writeText(hash).then(() => {
      setCopiedHash(hash);
      setTimeout(() => setCopiedHash(null), 1500);
    });
  };

  const fmtRelative = (dateStr) => {
    if (!dateStr) return "—";
    const d = new Date(dateStr);
    const now = new Date();
    const diffMs = now - d;
    const mins = Math.floor(diffMs / 60000);
    if (mins < 60) return `${mins}m ago`;
    const hrs = Math.floor(mins / 60);
    if (hrs < 24) return `${hrs}h ago`;
    const days = Math.floor(hrs / 24);
    if (days < 30) return `${days}d ago`;
    return d.toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric" });
  };

  const truncHash = (h) => h ? `${h.slice(0, 10)}…${h.slice(-6)}` : "—";

  const totalAttestations = coverageData?.total_attestations || issuers.length;

  if (issuersLoading) {
    return (
      <div style={{ padding: 40, display: "flex", justifyContent: "center" }}>
        <div style={{ color: T.inkFaint, fontFamily: T.mono, fontSize: 12 }}>Loading witness data...</div>
      </div>
    );
  }

  return (
    <div style={{ padding: mobile ? "16px 0 32px" : "24px 0 64px" }}>
      <TabHeader
        title={<><span style={{ fontWeight: 700 }}>Basis</span> Witness</>}
        formId="FORM CDA-001 · BASIS PROTOCOL"
        stats={[
          `${issuers.length} ISSUERS TRACKED`,
          `${totalAttestations} ATTESTATIONS ARCHIVED`,
          "UPDATED DAILY",
        ]}
        formulaLine="Structured, timestamped, hash-verified archive of stablecoin issuer disclosures"
        versionLabel="CDA v1.0.0"
        accent="#A8C4A0"
        mobile={mobile}
        showOnChain={false}
      />

      <div style={{ height: 24 }} />

      <div style={{ border: `1px solid ${T.ruleMid}` }}>
        {!mobile && (
          <div style={{
            display: "grid", gridTemplateColumns: "1.8fr 1fr 1fr 0.8fr 1.6fr 28px",
            padding: "10px 16px", borderBottom: `1px solid ${T.ruleMid}`,
            fontSize: 10, fontWeight: 600, color: T.inkLight, textTransform: "uppercase",
            letterSpacing: 1, fontFamily: T.mono,
          }}>
            <span>Issuer</span>
            <span>Last Verified</span>
            <span>Source Updated</span>
            <span>Method</span>
            <span>Witness Hash</span>
            <span />
          </div>
        )}
        {issuers.map((iss, i) => {
          const isOnChain = iss.collection_method === "nav_oracle";
          return (
          <div
            key={iss.asset_symbol}
            onClick={() => onSelectIssuer(iss.asset_symbol)}
            style={{
              display: mobile ? "flex" : "grid",
              flexDirection: mobile ? "column" : undefined,
              gridTemplateColumns: mobile ? undefined : "1.8fr 1fr 1fr 0.8fr 1.6fr 28px",
              padding: mobile ? "12px 12px" : "12px 16px",
              borderBottom: i < issuers.length - 1 ? `1px dotted ${T.ruleMid}` : "none",
              cursor: "pointer",
              alignItems: mobile ? "flex-start" : "center",
              gap: mobile ? 6 : 0,
              transition: "background 0.15s",
            }}
            onMouseEnter={e => e.currentTarget.style.background = T.paperWarm}
            onMouseLeave={e => e.currentTarget.style.background = "transparent"}
          >
            <div>
              <span style={{ fontFamily: T.sans, fontSize: 13, fontWeight: 500, color: T.ink }}>{iss.issuer_name}</span>
              <span style={{ fontFamily: T.mono, fontSize: 11, color: T.inkFaint, marginLeft: 8 }}>{iss.asset_symbol}</span>
            </div>
            <div style={{ fontFamily: T.mono, fontSize: 12, color: T.inkMid }}>
              {mobile && <span style={{ fontSize: 10, color: T.inkLight, fontFamily: T.mono, marginRight: 6 }}>VERIFIED:</span>}
              {fmtRelative(iss.last_verified || iss.last_attestation)}
            </div>
            <div style={{ fontFamily: T.mono, fontSize: 12, color: isOnChain ? "#2d6b45" : T.inkMid }}>
              {mobile && <span style={{ fontSize: 10, color: T.inkLight, fontFamily: T.mono, marginRight: 6 }}>SOURCE:</span>}
              {isOnChain ? "Live" : (iss.source_updated || "—")}
            </div>
            <div>
              <span style={{
                fontFamily: T.mono, fontSize: 10, color: T.inkLight,
                border: `1px solid ${T.ruleLight}`, padding: "2px 6px",
                display: "inline-block",
              }}>
                {isOnChain ? "on-chain" : (iss.collection_method || "—")}
              </span>
              {isOnChain && (
                <div style={{ fontFamily: T.sans, fontSize: 11, color: T.inkLight, fontStyle: "italic", marginTop: 4 }}>
                  {iss.transparency_url ? (
                    <a
                      href={iss.transparency_url}
                      target="_blank"
                      rel="noopener noreferrer"
                      onClick={(e) => e.stopPropagation()}
                      style={{ color: T.inkMid, textDecoration: "none", borderBottom: `1px solid ${T.ruleMid}` }}
                    >
                      verify ↗
                    </a>
                  ) : (
                    "on-chain"
                  )}
                </div>
              )}
            </div>
            <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
              {mobile && <span style={{ fontSize: 10, color: T.inkLight, fontFamily: T.mono, marginRight: 2 }}>HASH:</span>}
              <span style={{ fontFamily: T.mono, fontSize: 11, color: T.inkMid }}>
                {truncHash(latestHashes[iss.asset_symbol])}
              </span>
              {latestHashes[iss.asset_symbol] && (
                <button
                  onClick={(e) => { e.stopPropagation(); copyHash(latestHashes[iss.asset_symbol]); }}
                  style={{
                    background: "none", border: `1px solid ${T.ruleLight}`, cursor: "pointer",
                    padding: "1px 5px", fontSize: 10, fontFamily: T.mono, color: T.inkLight,
                  }}
                >
                  {copiedHash === latestHashes[iss.asset_symbol] ? "✓" : "copy"}
                </button>
              )}
            </div>
            <div style={{ fontFamily: T.sans, fontSize: 12, color: T.inkFaint }}>→</div>
          </div>
          );
        })}
      </div>

      <div style={{ marginTop: 28, fontSize: 11, color: T.inkLight, fontFamily: T.mono, lineHeight: 1.7, maxWidth: 640 }}>
        Basis Witness is the disclosure primitive in the Basis Protocol stack. Scores and enforcement surfaces are built on top of this data.
      </div>
      <div style={{ marginTop: 8, fontSize: 11, fontFamily: T.mono }}>
        <a href="/api/cda/" style={{ color: T.inkLight, textDecoration: "none", borderBottom: `1px solid ${T.ruleMid}`, marginRight: 12 }}>API docs</a>
        <a href="/developers" style={{ color: T.inkLight, textDecoration: "none", borderBottom: `1px solid ${T.ruleMid}` }}>Developers</a>
      </div>
    </div>
  );
}

function WitnessDetailView({ symbol, onBack, mobile }) {
  const { data: histData, loading: histLoading } = useIssuerHistory(symbol);
  const { data: latest, loading: latestLoading } = useIssuerLatest(symbol);
  const [expanded, setExpanded] = useState(null);
  const [copiedHash, setCopiedHash] = useState(null);

  const allAttestations = histData?.attestations || [];
  const attestations = allAttestations.filter(a =>
    a.quality === "full" || a.quality === "partial"
  );
  const issuerName = latest?.issuer_name || symbol;
  const transparencyUrl = latest?.source_url || null;

  const copyHash = (hash) => {
    navigator.clipboard.writeText(hash).then(() => {
      setCopiedHash(hash);
      setTimeout(() => setCopiedHash(null), 1500);
    });
  };

  const fmtDate = (dateStr) => {
    if (!dateStr) return "—";
    const d = new Date(dateStr);
    return d.toLocaleDateString("en-US", { year: "numeric", month: "short", day: "numeric" })
      + " " + d.toLocaleTimeString("en-US", { hour: "2-digit", minute: "2-digit" });
  };

  const renderDisplayFields = (fields) => {
    if (!fields || fields.length === 0) return null;
    const meaningful = fields.filter(f => {
      const v = typeof f.value === "object" && f.value !== null ? (f.value.value ?? f.value.amount ?? null) : f.value;
      if (v === null || v === undefined || v === "—" || v === "") return false;
      if ((v === 0 || v === "0" || v === "0%") && f.type !== "ratio") return false;
      return true;
    });
    if (meaningful.length === 0) return null;
    return meaningful.map((f, i) => (
      <div key={i} style={{
        display: "flex", justifyContent: "space-between", flexWrap: "wrap", gap: "2px 8px",
        padding: "6px 0", borderBottom: `1px dotted ${T.ruleLight}`, fontSize: 12,
      }}>
        <span style={{ fontFamily: T.sans, color: T.inkLight }}>{f.label}</span>
        <span style={{ fontFamily: T.mono, color: T.ink }}>
          {(() => { const v = typeof f.value === "object" && f.value !== null ? (f.value.value ?? f.value.amount ?? "—") : f.value;
           return f.type === "currency" ? `$${fmtB(v)}` :
           f.type === "percent" ? `${v}%` :
           f.type === "ratio" ? `${v}×` :
           f.type === "number" ? fmtB(v) :
           String(v); })()}
        </span>
      </div>
    ));
  };

  if (histLoading || latestLoading) {
    return (
      <div style={{ padding: 40, display: "flex", justifyContent: "center" }}>
        <div style={{ color: T.inkFaint, fontFamily: T.mono, fontSize: 12 }}>Loading {symbol} attestations...</div>
      </div>
    );
  }

  return (
    <div style={{ padding: mobile ? "16px 0 32px" : "24px 0 64px" }}>
      <button
        onClick={onBack}
        style={{
          background: "none", border: "none", color: T.inkLight,
          cursor: "pointer", fontSize: 12, fontFamily: T.sans,
          padding: 0, marginBottom: 20,
        }}
      >
        ← Back to Witness
      </button>

      <div style={{ display: "flex", alignItems: mobile ? "flex-start" : "baseline", gap: 12, marginBottom: 8, flexWrap: "wrap" }}>
        <h1 style={{ margin: 0, fontSize: mobile ? 20 : 24, fontWeight: 600, color: T.ink, fontFamily: T.sans, letterSpacing: -0.3 }}>
          {issuerName}
        </h1>
        <span style={{ fontFamily: T.mono, fontSize: 13, color: T.inkFaint }}>{symbol}</span>
        {transparencyUrl && (
          <a
            href={transparencyUrl}
            target="_blank"
            rel="noopener noreferrer"
            style={{ fontSize: 11, fontFamily: T.mono, color: T.inkLight, textDecoration: "none", borderBottom: `1px solid ${T.ruleMid}` }}
          >
            View source ↗
          </a>
        )}
        {latest?.source_urls && latest.source_urls.length > 1 && (
          <span style={{ fontSize: 11, fontFamily: T.mono, color: T.inkFaint }}>
            {latest.source_urls.length} sources monitored
          </span>
        )}
      </div>

      <div style={{
        margin: "20px 0 24px", padding: "12px 16px", border: `1px solid ${T.ruleLight}`,
        fontFamily: T.mono, fontSize: 11, color: T.inkLight, lineHeight: 1.7,
      }}>
        Every attestation is hashed at ingestion. This hash proves the document existed in this exact form on the date recorded. Basis does not modify source documents.
      </div>

      <div style={{ fontSize: 10, fontWeight: 600, color: T.inkLight, textTransform: "uppercase", letterSpacing: 1.5, marginBottom: 12, fontFamily: T.mono }}>
        ATTESTATION HISTORY
      </div>

      <div style={{ border: `1px solid ${T.ruleMid}` }}>
        {!mobile && (
          <div style={{
            display: "grid", gridTemplateColumns: "1.5fr 1fr 1fr",
            padding: "10px 16px", borderBottom: `1px solid ${T.ruleMid}`,
            fontSize: 10, fontWeight: 600, color: T.inkLight, textTransform: "uppercase",
            letterSpacing: 1, fontFamily: T.mono,
          }}>
            <span>Date</span>
            <span>Type</span>
            <span>Quality</span>
          </div>
        )}
        {attestations.length === 0 && (
          <div style={{ padding: 24, textAlign: "center", color: T.inkFaint, fontSize: 12, fontFamily: T.sans, lineHeight: 1.7 }}>
            {latest?.disclosure_type === "overcollateralized" || latest?.disclosure_type === "algorithmic"
              ? "This asset is verified on-chain. No off-chain attestation required."
              : latest?.disclosure_type === "synthetic-derivative"
              ? "No structured custodian data extracted yet."
              : "No structured reserve data extracted yet."}
            {transparencyUrl && (
              <>
                {" "}
                <a
                  href={transparencyUrl}
                  target="_blank"
                  rel="noopener noreferrer"
                  style={{ color: T.inkLight, borderBottom: `1px solid ${T.ruleMid}`, textDecoration: "none" }}
                >
                  View issuer's transparency page directly ↗
                </a>
              </>
            )}
          </div>
        )}
        {attestations.map((att, i) => {
          const isExpanded = expanded === i;
          const sourceLabel = att.source_type === "pdf_attestation" ? "PDF Report" :
            att.source_type === "transparency_page" ? "Web Page" :
            att.source_type === "research" ? "Research" :
            att.source_type || "—";
          return (
            <div key={i}>
              <div
                onClick={() => setExpanded(isExpanded ? null : i)}
                style={{
                  display: mobile ? "flex" : "grid",
                  flexDirection: mobile ? "column" : undefined,
                  gridTemplateColumns: mobile ? undefined : "1.5fr 1fr 1fr",
                  padding: mobile ? "12px 12px" : "12px 16px",
                  borderBottom: (i < attestations.length - 1 || isExpanded) ? `1px dotted ${T.ruleMid}` : "none",
                  cursor: "pointer",
                  alignItems: mobile ? "flex-start" : "center",
                  gap: mobile ? 4 : 0,
                  background: isExpanded ? T.paperWarm : "transparent",
                  transition: "background 0.15s",
                }}
                onMouseEnter={e => { if (!isExpanded) e.currentTarget.style.background = T.paperWarm; }}
                onMouseLeave={e => { if (!isExpanded) e.currentTarget.style.background = "transparent"; }}
              >
                <span style={{ fontFamily: T.mono, fontSize: 12, color: T.ink }}>
                  {mobile && <span style={{ fontSize: 10, color: T.inkLight, marginRight: 6 }}>DATE:</span>}
                  {fmtDate(att.extracted_at)}
                </span>
                <span style={{ fontFamily: T.mono, fontSize: 12, color: T.inkMid }}>
                  {mobile && <span style={{ fontSize: 10, color: T.inkLight, marginRight: 6 }}>TYPE:</span>}
                  {sourceLabel}
                </span>
                <span style={{
                  fontFamily: T.mono, fontSize: 11,
                  color: att.quality === "full" ? "#2d7a3a" :
                         att.quality === "partial" ? T.inkMid :
                         T.inkFaint,
                }}>
                  {mobile && <span style={{ fontSize: 10, color: T.inkLight, marginRight: 6 }}>QUALITY:</span>}
                  {att.quality_label || "—"}
                  {att.disclosure_type && att.disclosure_type !== "fiat-reserve" && (
                    <span style={{
                      fontFamily: T.mono, fontSize: 9, color: T.inkFaint,
                      padding: "1px 4px", border: `1px solid ${T.ruleLight}`,
                      marginLeft: 6,
                    }}>
                      {att.disclosure_type === "synthetic-derivative" ? "SYNTHETIC" :
                       att.disclosure_type === "rwa-tokenized" ? "RWA" :
                       att.disclosure_type === "overcollateralized" ? "VAULT" :
                       att.disclosure_type === "algorithmic" ? "ALGO" :
                       att.disclosure_type.toUpperCase()}
                    </span>
                  )}
                </span>
              </div>
              {isExpanded && (
                <div style={{
                  padding: mobile ? "12px 12px 16px" : "16px 16px 20px",
                  background: T.paperWarm,
                  borderBottom: i < attestations.length - 1 ? `1px dotted ${T.ruleMid}` : "none",
                }}>
                  {att.display_fields && att.display_fields.length > 0 && renderDisplayFields(att.display_fields) && (
                    <div style={{ marginBottom: 16 }}>
                      <div style={{ fontSize: 10, fontWeight: 600, color: T.inkLight, textTransform: "uppercase", letterSpacing: 1.5, marginBottom: 8, fontFamily: T.mono }}>
                        {att.disclosure_type === "synthetic-derivative" ? "BACKING DATA" :
                         att.disclosure_type === "rwa-tokenized" ? "NAV DATA" :
                         att.disclosure_type === "overcollateralized" ? "COLLATERAL DATA" :
                         "RESERVE DATA"}
                      </div>
                      <div style={{ border: `1px solid ${T.ruleLight}`, padding: "8px 12px" }}>
                        {renderDisplayFields(att.display_fields)}
                      </div>
                    </div>
                  )}
                  {att.quality === "metadata" && (
                    <div style={{ fontFamily: T.mono, fontSize: 11, color: T.inkFaint, fontStyle: "italic", marginBottom: 12 }}>
                      Page was scraped but no structured reserve data could be extracted. View the source document directly.
                    </div>
                  )}
                  <div style={{ display: "flex", alignItems: "center", gap: 8, flexWrap: "wrap" }}>
                    <span style={{ fontSize: 10, fontWeight: 600, color: T.inkLight, textTransform: "uppercase", letterSpacing: 1.5, fontFamily: T.mono }}>
                      WITNESS HASH
                    </span>
                    <span style={{ fontFamily: T.mono, fontSize: 11, color: T.inkMid, wordBreak: "break-all" }}>
                      {att.evidence_hash || "—"}
                    </span>
                    {att.evidence_hash && (
                      <button
                        onClick={(e) => { e.stopPropagation(); copyHash(att.evidence_hash); }}
                        style={{
                          background: "none", border: `1px solid ${T.ruleLight}`, cursor: "pointer",
                          padding: "1px 5px", fontSize: 10, fontFamily: T.mono, color: T.inkLight,
                        }}
                      >
                        {copiedHash === att.evidence_hash ? "✓" : "copy"}
                      </button>
                    )}
                  </div>
                </div>
              )}
            </div>
          );
        })}
      </div>
    </div>
  );
}

function Footer({ mobile }) {
  return (
    <footer style={{
      padding: mobile ? "12px 12px" : "16px 24px",
      borderTop: `1px solid ${T.ruleMid}`,
      display: "flex",
      flexDirection: mobile ? "column" : "row",
      justifyContent: "space-between",
      alignItems: mobile ? "flex-start" : "center",
      gap: mobile ? 4 : 0,
      fontSize: 10, color: T.inkFaint, fontFamily: T.mono,
    }}>
      <span>Basis Protocol · Stablecoin Integrity Index</span>
      <span><a href="/developers" style={{ color: 'inherit', textDecoration: 'none', borderBottom: `1px solid ${T.ruleMid}` }}>API &amp; Pricing</a> · Risk surfaces for on-chain finance · basisprotocol.xyz</span>
    </footer>
  );
}

export default function App() {
  // Route /ops to Operations Hub
  if (window.location.pathname.startsWith("/ops")) {
    return <OpsDashboard />;
  }

  const [view, setView] = useState("rankings");
  const [selectedCoin, setSelectedCoin] = useState(null);
  const [witnessSymbol, setWitnessSymbol] = useState(null);
  const { data: scores, loading, error, ts, meta } = useScores();
  const mobile = useIsMobile();
  const integrity = useIntegrity();

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
    if (v !== "witness-detail") setWitnessSymbol(null);
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
            <nav style={{ display: "flex", gap: mobile ? 12 : 16, overflowX: mobile ? "auto" : "visible", WebkitOverflowScrolling: "touch" }}>
              {[
                { id: "rankings", label: "Stablecoins" },
                { id: "protocols", label: "Protocols" },
                { id: "wallets", label: "Wallets" },
                { id: "witness", label: "Witness" },
                { id: "methodology", label: "Methodology" },
              ].map((tab) => (
                <button
                  key={tab.id}
                  onClick={() => handleSetView(tab.id)}
                  style={{
                    padding: "4px 0", border: "none", cursor: "pointer",
                    fontSize: mobile ? 11 : 12, fontWeight: view === tab.id ? 600 : 400,
                    fontFamily: T.sans, whiteSpace: "nowrap",
                    color: view === tab.id ? T.ink : T.inkLight,
                    background: "transparent",
                    borderBottom: view === tab.id ? `2px solid ${T.ink}` : "2px solid transparent",
                  }}
                >
                  {tab.label}
                  <StatusDot status={domainStatus(integrity, tab.id)} />
                </button>
              ))}
            </nav>
          </div>

          <div style={{ borderTop: `1px solid ${T.ruleLight}` }} />

          <div style={{ padding: mobile ? "0 8px 12px" : "0 24px 24px" }}>
            <main style={{ animation: "fadeIn 0.3s ease" }}>
              {view === "rankings" && (
                <RankingsView scores={scores} loading={loading} onSelect={handleSelect} ts={ts} mobile={mobile} meta={meta} />
              )}
              {view === "detail" && selectedCoin && (
                <DetailView coinId={selectedCoin} onBack={handleBack} mobile={mobile} />
              )}
              {view === "wallets" && <WalletsView mobile={mobile} integrity={integrity} />}
              {view === "protocols" && <ProtocolsView mobile={mobile} />}
              {view === "witness" && <WitnessView mobile={mobile} onSelectIssuer={(sym) => { setWitnessSymbol(sym); setView("witness-detail"); window.scrollTo(0, 0); }} />}
              {view === "witness-detail" && witnessSymbol && <WitnessDetailView symbol={witnessSymbol} onBack={() => { setView("witness"); setWitnessSymbol(null); }} mobile={mobile} />}
              {view === "methodology" && <MethodologyView mobile={mobile} />}
            </main>
          </div>

          <Footer mobile={mobile} />
        </div>

        <div style={{ height: mobile ? 16 : 32 }} />
      </div>
    </div>
  );
}
