import { useState, useEffect, useCallback } from "react";

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

const STAGES = [
  "not_started", "recognition", "familiarity", "direct",
  "evaluating", "trying", "binding", "archived",
];

function getAdminKey() {
  const params = new URLSearchParams(window.location.search);
  return params.get("key") || localStorage.getItem("ops_admin_key") || "";
}

function setAdminKey(key) {
  localStorage.setItem("ops_admin_key", key);
}

async function opsFetch(path, opts = {}) {
  const key = getAdminKey();
  const resp = await fetch(path, {
    ...opts,
    headers: { "x-admin-key": key, "Content-Type": "application/json", ...(opts.headers || {}) },
  });
  if (resp.status === 401) throw new Error("unauthorized");
  if (!resp.ok) throw new Error(`${resp.status} ${resp.statusText}`);
  return resp.json();
}

// ─── Shared UI ───────────────────────────────────────────────────────

const btn = (extra = {}) => ({
  fontSize: 10, fontFamily: T.mono, padding: "3px 8px", border: `1px solid ${T.ruleMid}`,
  background: T.paper, cursor: "pointer", whiteSpace: "nowrap", ...extra,
});
const btnActive = (extra = {}) => ({
  ...btn(extra), background: T.ink, color: T.paper, border: `1px solid ${T.ink}`,
});

function StatusDot({ status }) {
  const colors = { healthy: "#27ae60", degraded: "#f39c12", down: "#e74c3c" };
  return (
    <span style={{
      display: "inline-block", width: 8, height: 8, borderRadius: "50%",
      background: colors[status] || "#999", marginRight: 6,
    }} />
  );
}

function StageBadge({ stage }) {
  const colors = {
    not_started: "#999", recognition: "#3498db", familiarity: "#2980b9",
    direct: "#8e44ad", evaluating: "#f39c12", trying: "#e67e22",
    binding: "#27ae60", archived: "#7f8c8d",
  };
  return (
    <span style={{
      fontSize: 10, fontFamily: T.mono, padding: "2px 6px", borderRadius: 3,
      background: (colors[stage] || "#999") + "22", color: colors[stage] || "#999",
      border: `1px solid ${colors[stage] || "#999"}44`,
    }}>
      {(stage || "unknown").replace(/_/g, " ")}
    </span>
  );
}

function TierBadge({ tier }) {
  const labels = { 1: "T1", 2: "T2", 3: "T3" };
  const colors = { 1: "#e74c3c", 2: "#f39c12", 3: "#95a5a6" };
  return (
    <span style={{
      fontSize: 9, fontFamily: T.mono, fontWeight: 600, padding: "1px 4px",
      borderRadius: 2, background: colors[tier] || "#999", color: "#fff", marginRight: 6,
    }}>
      {labels[tier] || `T${tier}`}
    </span>
  );
}

function Lbl({ children }) {
  return <span style={{ fontSize: 10, fontWeight: 600, fontFamily: T.mono, color: T.inkLight, textTransform: "uppercase", letterSpacing: 0.5 }}>{children}</span>;
}

function Flash({ flash }) {
  if (!flash) return null;
  return (
    <div style={{
      padding: "4px 8px", margin: "6px 10px", fontSize: 11, fontFamily: T.mono,
      background: flash.ok ? "#27ae6018" : "#e74c3c18",
      border: `1px solid ${flash.ok ? "#27ae6044" : "#e74c3c44"}`,
      color: flash.ok ? "#27ae60" : T.accent,
    }}>
      {flash.msg}
    </div>
  );
}

function useFlash() {
  const [flash, setFlash] = useState(null);
  const showFlash = (msg, ok = true) => { setFlash({ msg, ok }); setTimeout(() => setFlash(null), 5000); };
  return [flash, showFlash];
}

// ─── Content Item (shared expandable row) ────────────────────────────

const SOURCE_ICONS = {
  tweet: "\uD83D\uDCAC", blog: "\uD83D\uDCDD", forum: "\uD83D\uDCE2", governance: "\u2696\uFE0F",
  article: "\uD83D\uDCF0", podcast: "\uD83C\uDFA7", video: "\uD83C\uDFAC", default: "\uD83D\uDD17",
};

function contentStatusDot(c) {
  if (c.analyzed && c.bridge_found) return { color: "#27ae60", label: "actionable" };
  if (c.analyzed && !c.bridge_found) return { color: "#999", label: "no bridge" };
  return { color: "#f39c12", label: "needs analysis" };
}

function ContentItem({ item, onDecide, onAnalyze, busy, defaultOpen }) {
  const [open, setOpen] = useState(!!defaultOpen);
  const c = item;
  const status = contentStatusDot(c);
  const icon = SOURCE_ICONS[c.source_type] || SOURCE_ICONS.default;
  const decided = !!c.founder_decision;

  return (
    <div style={{ borderBottom: `1px solid ${T.ruleLight}` }}>
      {/* Collapsed row */}
      <div onClick={() => setOpen(!open)} style={{
        padding: "5px 6px", fontSize: 11, display: "flex", alignItems: "center", gap: 6,
        cursor: "pointer", background: open ? T.paperWarm : "transparent", transition: "background 0.1s",
      }}>
        <span style={{ fontSize: 12, width: 16, textAlign: "center", flexShrink: 0 }}>{icon}</span>
        <span style={{ flex: 1, color: T.inkMid, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
          {c.title || c.source_url}
        </span>
        {c.target_name && (
          <span style={{ fontFamily: T.mono, fontSize: 9, color: T.inkFaint, flexShrink: 0 }}>{c.target_name}</span>
        )}
        <span style={{ fontSize: 9, color: T.inkFaint, flexShrink: 0, minWidth: 55, textAlign: "right" }}>
          {c.scraped_at ? new Date(c.scraped_at).toLocaleDateString() : ""}
        </span>
        <span title={status.label} style={{
          display: "inline-block", width: 8, height: 8, borderRadius: "50%",
          background: status.color, flexShrink: 0,
        }} />
        {decided && (
          <span style={{ fontFamily: T.mono, fontSize: 9, color: T.inkFaint, flexShrink: 0 }}>{c.founder_decision}</span>
        )}
      </div>

      {/* Expanded detail */}
      {open && (
        <div style={{ padding: "6px 10px 10px 28px", fontSize: 11, background: T.paperWarm }}>
          {/* Source link */}
          {c.source_url && (
            <div style={{ marginBottom: 4 }}>
              <a href={c.source_url} target="_blank" rel="noopener noreferrer"
                style={{ fontSize: 10, fontFamily: T.mono, color: T.inkLight, textDecoration: "none", borderBottom: `1px solid ${T.ruleLight}` }}>
                {c.source_url.length > 80 ? c.source_url.substring(0, 80) + "..." : c.source_url}
              </a>
            </div>
          )}

          {/* Content text / tweet body */}
          {c.content && c.content !== c.source_url && c.content !== c.title ? (
            <div style={{ marginBottom: 6, color: T.inkMid, lineHeight: 1.4 }}>
              {c.content.length > 300 ? c.content.substring(0, 300) + "..." : c.content}
            </div>
          ) : c.source_type === "tweet" ? (
            <div style={{ marginBottom: 6, color: T.inkFaint, fontSize: 10, fontStyle: "italic" }}>
              Content not extracted — only URL stored.
            </div>
          ) : null}

          {/* Analyzed content */}
          {c.analyzed ? (
            <>
              {c.content_summary && (
                <div style={{ marginBottom: 6 }}>
                  <span style={{ fontSize: 9, fontWeight: 600, color: T.inkLight, textTransform: "uppercase" }}>Summary </span>
                  <span style={{ color: T.inkMid }}>{c.content_summary}</span>
                </div>
              )}
              {c.worldview_extract && (
                <div style={{ marginBottom: 6 }}>
                  <span style={{ fontSize: 9, fontWeight: 600, color: T.inkLight, textTransform: "uppercase" }}>Worldview </span>
                  <span style={{ color: T.inkMid }}>{c.worldview_extract}</span>
                </div>
              )}
              <div style={{ marginBottom: 6, fontSize: 10 }}>
                <span style={{ fontSize: 9, fontWeight: 600, color: T.inkLight, textTransform: "uppercase" }}>Bridge </span>
                {c.bridge_found
                  ? <span style={{ color: "#27ae60" }}>Yes — {c.bridge_text}</span>
                  : <span style={{ color: T.inkFaint }}>No bridge found</span>}
              </div>
              {c.draft_comment && (
                <div style={{ background: T.paper, padding: "6px 8px", border: `1px solid ${T.ruleLight}`, marginBottom: 6, whiteSpace: "pre-wrap" }}>
                  <div style={{ fontSize: 9, color: T.inkFaint, marginBottom: 2, fontFamily: T.mono }}>
                    Draft — {c.comment_type || "comment"}
                  </div>
                  {c.draft_comment}
                </div>
              )}
              {c.relevance_score != null && (
                <div style={{ fontSize: 9, fontFamily: T.mono, color: T.inkFaint, marginBottom: 6 }}>
                  relevance: {c.relevance_score} · action: {c.engagement_action || "pending"}
                </div>
              )}
            </>
          ) : (
            <div style={{ color: T.inkFaint, fontStyle: "italic", marginBottom: 6 }}>
              Not yet analyzed.
            </div>
          )}

          {/* Action buttons */}
          {!decided && (
            <div style={{ display: "flex", gap: 4 }}>
              {c.analyzed && c.draft_comment && (
                <>
                  <button onClick={() => onDecide && onDecide(c.id, "approved")} disabled={!!busy}
                    style={btn({ background: "#27ae6022" })}>{busy === `decide-${c.id}` ? "..." : "Approve"}</button>
                  <button onClick={() => onDecide && onDecide(c.id, "skipped")} disabled={!!busy}
                    style={btn()}>{busy === `decide-${c.id}` ? "..." : "Skip"}</button>
                </>
              )}
              {!c.analyzed && onAnalyze && (
                <button onClick={() => onAnalyze(c.id)} disabled={!!busy}
                  style={btn({ background: "#f39c1222" })}>{busy === `analyze-${c.id}` ? "Analyzing..." : "Analyze"}</button>
              )}
            </div>
          )}
          {decided && (
            <div style={{ fontSize: 10, fontFamily: T.mono, color: T.inkFaint }}>
              Decision: {c.founder_decision}
              {c.posted_at && ` · posted ${new Date(c.posted_at).toLocaleDateString()}`}
            </div>
          )}
        </div>
      )}
    </div>
  );
}

// ─── Auth Gate ────────────────────────────────────────────────────────

function AuthGate({ onAuth }) {
  const [key, setKey] = useState("");
  return (
    <div style={{ padding: 40, textAlign: "center", fontFamily: T.sans }}>
      <h2 style={{ fontFamily: T.mono, marginBottom: 16, fontWeight: 600, fontSize: 16 }}>Basis Operations Hub</h2>
      <p style={{ color: T.inkLight, fontSize: 13, marginBottom: 16 }}>Enter admin key to access</p>
      <input type="password" value={key} onChange={(e) => setKey(e.target.value)}
        onKeyDown={(e) => e.key === "Enter" && onAuth(key)} placeholder="Admin key"
        style={{ fontFamily: T.mono, fontSize: 13, padding: "8px 12px", border: `1px solid ${T.ruleMid}`, background: T.paper, width: 280, marginRight: 8 }} />
      <button onClick={() => onAuth(key)}
        style={{ fontFamily: T.mono, fontSize: 12, padding: "8px 16px", border: `2px solid ${T.ink}`, background: T.ink, color: T.paper, cursor: "pointer" }}>
        Enter
      </button>
    </div>
  );
}

// ─── Pipeline Health ──────────────────────────────────────────────────

function HealthPanel({ health }) {
  if (!health || health.length === 0) return <div style={{ color: T.inkFaint, fontSize: 12 }}>No health data. Run a health check first.</div>;
  return (
    <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(220px, 1fr))", gap: 8 }}>
      {health.map((h) => (
        <div key={h.system} style={{ padding: "8px 10px", border: `1px solid ${T.ruleLight}`, background: T.paperWarm, fontSize: 11, fontFamily: T.mono }}>
          <StatusDot status={h.status} />
          <strong>{h.system.replace(/_/g, " ")}</strong>
          <div style={{ color: T.inkLight, fontSize: 10, marginTop: 4 }}>
            {h.details && typeof h.details === "object"
              ? Object.entries(h.details).slice(0, 3).map(([k, v]) => (
                  <div key={k}>{k}: {typeof v === "object" ? JSON.stringify(v) : String(v)}</div>
                )) : null}
            {h.checked_at && <div style={{ color: T.inkFaint, marginTop: 2 }}>checked: {new Date(h.checked_at).toLocaleTimeString()}</div>}
          </div>
        </div>
      ))}
    </div>
  );
}

// ─── Action Queue ─────────────────────────────────────────────────────

function ActionQueue({ queue, onDecide, decidingId }) {
  if (!queue || queue.length === 0) return <div style={{ color: T.inkFaint, fontSize: 12 }}>No pending actions in queue.</div>;
  return (
    <div>
      {queue.map((item) => (
        <ContentItem key={item.id} item={item} onDecide={onDecide}
          busy={decidingId ? `decide-${decidingId}` : null} />
      ))}
    </div>
  );
}

// ─── Inline Target Row (accordion) ───────────────────────────────────

function TargetRow({ target, onUpdate }) {
  const [open, setOpen] = useState(false);
  const [detail, setDetail] = useState(null);
  const [loadingDetail, setLoadingDetail] = useState(false);
  const [busy, setBusy] = useState(null); // tracks which action is in-flight
  const [engForm, setEngForm] = useState(null); // null or { action_type, channel, content }
  const [stageOpen, setStageOpen] = useState(false);
  const [scrapeUrl, setScrapeUrl] = useState("");
  const [dmTrigger, setDmTrigger] = useState(""); // trigger context for DM draft
  const [dmDraft, setDmDraft] = useState(null); // Claude-generated DM draft
  const [backfillQuery, setBackfillQuery] = useState(""); // search query for backfill
  const [flash, setFlash] = useState(null);

  const t = target;

  const showFlash = (msg, ok = true) => {
    setFlash({ msg, ok });
    setTimeout(() => setFlash(null), 5000);
  };

  const loadDetail = async () => {
    setLoadingDetail(true);
    try {
      const data = await opsFetch(`/api/ops/targets/${t.id}`);
      setDetail(data);
    } catch (e) {
      showFlash(e.message, false);
    }
    setLoadingDetail(false);
  };

  const toggle = () => {
    const next = !open;
    setOpen(next);
    if (next && !detail) loadDetail();
  };

  const handleScrape = async () => {
    if (!scrapeUrl.trim()) return;
    setBusy("scrape");
    try {
      const res = await opsFetch("/api/ops/scrape", {
        method: "POST", body: JSON.stringify({ target_id: t.id, url: scrapeUrl.trim(), source_type: "blog" }),
      });
      const a = res.analysis;
      showFlash(a?.bridge_found ? `Scraped + analyzed — bridge found` : `Scraped + analyzed — no bridge`);
      setScrapeUrl("");
      loadDetail();
    } catch (e) { showFlash(e.message, false); }
    setBusy(null);
  };

  const [exposureReport, setExposureReport] = useState(null);

  const handleExposure = async () => {
    setBusy("exposure");
    try {
      const res = await opsFetch("/api/ops/exposure/generate", {
        method: "POST", body: JSON.stringify({ target_id: t.id }),
      });
      if (res.status === "error") {
        showFlash(res.detail || "Exposure generation failed", false);
      } else {
        setExposureReport(res);
        showFlash(`Exposure report generated — weighted SII ${res.data?.weighted_sii || "N/A"}`);
        loadDetail();
      }
    } catch (e) { showFlash(e.message, false); }
    setBusy(null);
  };

  const handleStageChange = async (newStage) => {
    setBusy("stage");
    try {
      await opsFetch(`/api/ops/targets/${t.id}/stage`, {
        method: "PUT", body: JSON.stringify({ stage: newStage }),
      });
      showFlash(`Stage → ${newStage.replace(/_/g, " ")}`);
      setStageOpen(false);
      if (onUpdate) onUpdate();
      loadDetail();
    } catch (e) { showFlash(e.message, false); }
    setBusy(null);
  };

  const handleEngagement = async () => {
    if (!engForm?.action_type) return;
    setBusy("engagement");
    try {
      await opsFetch(`/api/ops/targets/${t.id}/engagement`, {
        method: "POST", body: JSON.stringify(engForm),
      });
      showFlash("Engagement logged");
      setEngForm(null);
      loadDetail();
    } catch (e) { showFlash(e.message, false); }
    setBusy(null);
  };

  const handleDecideContent = async (contentId, decision) => {
    setBusy(`decide-${contentId}`);
    try {
      await opsFetch(`/api/ops/content/${contentId}/decide`, {
        method: "POST", body: JSON.stringify({ decision }),
      });
      showFlash(`Content ${decision}`);
      loadDetail();
    } catch (e) { showFlash(e.message, false); }
    setBusy(null);
  };

  const handleDraftDm = async () => {
    if (!dmTrigger.trim()) return;
    setBusy("dm");
    try {
      const res = await opsFetch("/api/ops/draft/dm", {
        method: "POST", body: JSON.stringify({ target_id: t.id, trigger: dmTrigger.trim() }),
      });
      setDmDraft(res.draft);
      showFlash("DM draft generated");
    } catch (e) { showFlash(e.message, false); }
    setBusy(null);
  };

  const handleBackfill = async () => {
    if (!backfillQuery.trim()) return;
    setBusy("backfill");
    try {
      const res = await opsFetch("/api/ops/backfill", {
        method: "POST", body: JSON.stringify({ target_id: t.id, query: backfillQuery.trim(), max_results: 15 }),
      });
      const unanalyzed = res.scraped - res.analyzed;
      showFlash(`Backfill: ${res.scraped} scraped, ${res.analyzed} analyzed${unanalyzed > 0 ? ` (${unanalyzed} need analysis)` : ""}`);
      setBackfillQuery("");
      loadDetail();
    } catch (e) { showFlash(e.message, false); }
    setBusy(null);
  };

  const [autoExpandId, setAutoExpandId] = useState(null);

  const handleAnalyzeContent = async (contentId) => {
    setBusy(`analyze-${contentId}`);
    try {
      await opsFetch(`/api/ops/analyze/${contentId}`, { method: "POST" });
      showFlash("Analysis complete — expand to see results");
      setAutoExpandId(contentId);
      await loadDetail();
    } catch (e) { showFlash(e.message, false); }
    setBusy(null);
  };

  const [analyzeProgress, setAnalyzeProgress] = useState(null);

  const handleAnalyzeAll = async () => {
    const unanalyzed = content.filter((c) => !c.analyzed);
    if (unanalyzed.length === 0) return;
    setBusy("analyze-all");
    let done = 0;
    const total = unanalyzed.length;
    setAnalyzeProgress(`0/${total}`);
    for (const c of unanalyzed) {
      try {
        await opsFetch(`/api/ops/analyze/${c.id}`, { method: "POST" });
        done++;
        setAnalyzeProgress(`${done}/${total}`);
      } catch (_) {}
    }
    setAnalyzeProgress(null);
    showFlash(`Analyzed ${done}/${total} items`);
    await loadDetail();
    setBusy(null);
  };

  const contacts = detail?.contacts || [];
  const content = detail?.recent_content || [];
  const engagement = detail?.engagement_log || [];
  const tgt = detail?.target || t;
  const exposure = detail?.latest_exposure;

  return (
    <div style={{ borderBottom: `1px solid ${T.ruleLight}` }}>
      {/* Row header — always visible */}
      <div onClick={toggle} style={{
        padding: "7px 10px", fontSize: 12, display: "flex", alignItems: "center", gap: 8,
        cursor: "pointer", background: open ? T.paperWarm : "transparent",
        transition: "background 0.15s",
      }}>
        <span style={{ fontSize: 10, color: T.inkFaint, width: 12 }}>{open ? "\u25BC" : "\u25B6"}</span>
        <TierBadge tier={t.tier} />
        <div style={{ flex: 1, fontFamily: T.mono, fontWeight: 500 }}>{t.name}</div>
        <StageBadge stage={t.pipeline_stage} />
        {t.track && <span style={{ fontSize: 10, color: T.inkFaint }}>{t.track}</span>}
        {t.last_action_at && (
          <span style={{ fontSize: 9, color: T.inkFaint, fontFamily: T.mono }}>
            {new Date(t.last_action_at).toLocaleDateString()}
          </span>
        )}
        <div style={{ fontSize: 10, color: t.next_action ? T.inkMid : T.inkFaint, minWidth: 80, textAlign: "right" }}>
          {t.next_action || "—"}
        </div>
      </div>

      {/* Expanded detail panel */}
      {open && (
        <div style={{ padding: "0 10px 12px 30px", fontSize: 12, animation: "fadeIn 0.15s ease" }}>
          {loadingDetail && !detail && <div style={{ color: T.inkFaint, padding: "8px 0" }}>Loading...</div>}

          {/* Flash message */}
          {flash && (
            <div style={{
              padding: "4px 8px", margin: "6px 0", fontSize: 11, fontFamily: T.mono,
              background: flash.ok ? "#27ae6018" : "#e74c3c18",
              border: `1px solid ${flash.ok ? "#27ae6044" : "#e74c3c44"}`,
              color: flash.ok ? "#27ae60" : T.accent,
            }}>
              {flash.msg}
            </div>
          )}

          {/* ── Action bar ── */}
          <div style={{ display: "flex", flexWrap: "wrap", gap: 4, padding: "8px 0 6px", borderBottom: `1px solid ${T.ruleLight}`, marginBottom: 8 }}>
            <button onClick={handleExposure} disabled={!!busy} style={btn()}>
              {busy === "exposure" ? "..." : "Generate exposure"}
            </button>
            <button onClick={() => setDmDraft(null) || setDmTrigger(dmTrigger !== null && dmTrigger !== "" ? "" : " ")}
              style={dmTrigger ? btnActive() : btn()}>
              Draft DM
            </button>
            <button onClick={() => setEngForm(engForm ? null : { action_type: "", channel: "", content: "" })}
              style={engForm ? btnActive() : btn()}>
              Log engagement
            </button>
            <button onClick={() => setStageOpen(!stageOpen)} style={stageOpen ? btnActive() : btn()}>
              Update stage
            </button>
            <button onClick={() => setBackfillQuery(backfillQuery ? "" : " ")}
              style={backfillQuery ? btnActive() : btn()}>
              Backfill
            </button>
            <button onClick={() => detail && loadDetail()} style={btn()}>Refresh</button>
          </div>

          {/* ── Stage dropdown ── */}
          {stageOpen && (
            <div style={{ display: "flex", flexWrap: "wrap", gap: 3, padding: "4px 0 8px" }}>
              {STAGES.map((s) => (
                <button key={s} onClick={() => handleStageChange(s)} disabled={s === t.pipeline_stage || !!busy}
                  style={btn({
                    opacity: s === t.pipeline_stage ? 0.4 : 1,
                    background: s === t.pipeline_stage ? T.paperWarm : T.paper,
                  })}>
                  {s.replace(/_/g, " ")}
                </button>
              ))}
            </div>
          )}

          {/* ── Engagement form ── */}
          {engForm && (
            <div style={{ padding: "6px 0 8px", display: "flex", flexWrap: "wrap", gap: 6, alignItems: "flex-end" }}>
              <div>
                <Lbl>Action</Lbl>
                <select value={engForm.action_type} onChange={(e) => setEngForm({ ...engForm, action_type: e.target.value })}
                  style={{ display: "block", fontFamily: T.mono, fontSize: 11, padding: "4px 6px", border: `1px solid ${T.ruleMid}`, background: T.paper, marginTop: 2 }}>
                  <option value="">select...</option>
                  {["comment_posted", "dm_sent", "email_sent", "call_completed", "exposure_sent", "artifact_sent", "forum_posted"].map((a) => (
                    <option key={a} value={a}>{a.replace(/_/g, " ")}</option>
                  ))}
                </select>
              </div>
              <div>
                <Lbl>Channel</Lbl>
                <select value={engForm.channel} onChange={(e) => setEngForm({ ...engForm, channel: e.target.value })}
                  style={{ display: "block", fontFamily: T.mono, fontSize: 11, padding: "4px 6px", border: `1px solid ${T.ruleMid}`, background: T.paper, marginTop: 2 }}>
                  <option value="">select...</option>
                  {["twitter_dm", "twitter_reply", "forum", "email", "call", "linkedin", "discord", "telegram"].map((c) => (
                    <option key={c} value={c}>{c.replace(/_/g, " ")}</option>
                  ))}
                </select>
              </div>
              <div style={{ flex: 1, minWidth: 200 }}>
                <Lbl>Content</Lbl>
                <input value={engForm.content} onChange={(e) => setEngForm({ ...engForm, content: e.target.value })}
                  placeholder="What was said/sent..."
                  style={{ display: "block", width: "100%", fontFamily: T.mono, fontSize: 11, padding: "4px 6px", border: `1px solid ${T.ruleMid}`, background: T.paper, marginTop: 2 }} />
              </div>
              <button onClick={handleEngagement} disabled={!engForm.action_type || !!busy}
                style={btn({ background: "#27ae6022" })}>
                {busy === "engagement" ? "..." : "Save"}
              </button>
            </div>
          )}

          {/* ── DM Draft form ── */}
          {dmTrigger !== "" && dmTrigger !== null && (
            <div style={{ padding: "6px 0 8px" }}>
              <div style={{ display: "flex", gap: 4, alignItems: "center", marginBottom: 6 }}>
                <input value={dmTrigger} onChange={(e) => setDmTrigger(e.target.value)}
                  placeholder="Trigger context (e.g., 'published blog post about stablecoin allocation')..."
                  onKeyDown={(e) => e.key === "Enter" && handleDraftDm()}
                  style={{ flex: 1, fontFamily: T.mono, fontSize: 11, padding: "4px 6px", border: `1px solid ${T.ruleMid}`, background: T.paper }} />
                <button onClick={handleDraftDm} disabled={!dmTrigger.trim() || !!busy} style={btn({ background: "#3498db22" })}>
                  {busy === "dm" ? "Generating..." : "Generate"}
                </button>
                <button onClick={() => { setDmTrigger(""); setDmDraft(null); }} style={btn()}>Cancel</button>
              </div>
              {dmDraft && (
                <div style={{ background: T.paperWarm, border: `1px solid ${T.ruleLight}`, padding: "8px 10px", fontSize: 11 }}>
                  {dmDraft.twitter_dm && (
                    <div style={{ marginBottom: 6 }}>
                      <Lbl>Twitter DM</Lbl>
                      <div style={{ fontFamily: T.mono, marginTop: 2, whiteSpace: "pre-wrap" }}>{dmDraft.twitter_dm}</div>
                    </div>
                  )}
                  {dmDraft.email_subject && (
                    <div style={{ marginBottom: 6 }}>
                      <Lbl>Email</Lbl>
                      <div style={{ fontFamily: T.mono, marginTop: 2 }}>Subject: {dmDraft.email_subject}</div>
                      <div style={{ marginTop: 4, whiteSpace: "pre-wrap" }}>{dmDraft.email_body}</div>
                    </div>
                  )}
                  {dmDraft.rationale && (
                    <div style={{ fontSize: 10, color: T.inkFaint, marginTop: 4, fontStyle: "italic" }}>
                      Rationale: {dmDraft.rationale}
                    </div>
                  )}
                </div>
              )}
            </div>
          )}

          {/* ── Backfill bar ── */}
          {backfillQuery !== "" && backfillQuery !== null && (
            <div style={{ display: "flex", gap: 4, alignItems: "center", padding: "4px 0 8px" }}>
              <input value={backfillQuery} onChange={(e) => setBackfillQuery(e.target.value)}
                placeholder={`Search query (e.g., "all blog posts on ${t.name.toLowerCase().replace(/ /g,'')}.io")...`}
                onKeyDown={(e) => e.key === "Enter" && handleBackfill()}
                style={{ flex: 1, fontFamily: T.mono, fontSize: 11, padding: "4px 6px", border: `1px solid ${T.ruleMid}`, background: T.paper }} />
              <button onClick={handleBackfill} disabled={!backfillQuery.trim() || !!busy} style={btn({ background: "#8e44ad22" })}>
                {busy === "backfill" ? "Searching..." : "Backfill"}
              </button>
              <button onClick={() => setBackfillQuery("")} style={btn()}>Cancel</button>
            </div>
          )}

          {/* ── Scrape bar ── */}
          <div style={{ display: "flex", gap: 4, alignItems: "center", padding: "4px 0 8px" }}>
            <input value={scrapeUrl} onChange={(e) => setScrapeUrl(e.target.value)}
              placeholder="Paste URL to scrape..."
              onKeyDown={(e) => e.key === "Enter" && handleScrape()}
              style={{ flex: 1, fontFamily: T.mono, fontSize: 11, padding: "4px 6px", border: `1px solid ${T.ruleMid}`, background: T.paper }} />
            <button onClick={handleScrape} disabled={!scrapeUrl.trim() || !!busy} style={btn()}>
              {busy === "scrape" ? "Scraping..." : "Scrape"}
            </button>
          </div>

          {/* ── Worldview / Gap / Wedge / Landmine ── */}
          {tgt.worldview_summary && (
            <div style={{ marginBottom: 6, lineHeight: 1.5 }}>
              <Lbl>Worldview</Lbl>
              <div style={{ fontSize: 11, color: T.inkMid, marginTop: 2 }}>{tgt.worldview_summary}</div>
            </div>
          )}
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "4px 16px", marginBottom: 8 }}>
            {tgt.gap && <div><Lbl>Gap</Lbl><div style={{ fontSize: 11, color: T.inkMid, marginTop: 1 }}>{tgt.gap}</div></div>}
            {tgt.first_wedge && <div><Lbl>First wedge</Lbl><div style={{ fontSize: 11, color: T.inkMid, marginTop: 1 }}>{tgt.first_wedge}</div></div>}
            {tgt.landmine && <div><Lbl>Landmine</Lbl><div style={{ fontSize: 11, color: T.accent, marginTop: 1 }}>{tgt.landmine}</div></div>}
            {tgt.positioning && <div><Lbl>Positioning</Lbl><div style={{ fontSize: 11, color: T.inkMid, marginTop: 1 }}>{tgt.positioning}</div></div>}
          </div>

          {/* ── Contacts ── */}
          {contacts.length > 0 && (
            <div style={{ marginBottom: 8 }}>
              <Lbl>Contacts</Lbl>
              <div style={{ marginTop: 3 }}>
                {contacts.map((c) => (
                  <div key={c.id} style={{ fontSize: 11, padding: "2px 0", display: "flex", gap: 8, alignItems: "center" }}>
                    <strong>{c.name}</strong>
                    {c.role && <span style={{ color: T.inkLight }}>{c.role}</span>}
                    {c.twitter_handle && (
                      <a href={`https://twitter.com/${c.twitter_handle.replace(/^@/, "")}`}
                        target="_blank" rel="noopener noreferrer"
                        style={{ color: "#1da1f2", fontSize: 10, fontFamily: T.mono, textDecoration: "none" }}>
                        {c.twitter_handle.startsWith("@") ? c.twitter_handle : `@${c.twitter_handle}`}
                      </a>
                    )}
                    {c.linkedin_url && (
                      <a href={c.linkedin_url} target="_blank" rel="noopener noreferrer"
                        style={{ color: "#0a66c2", fontSize: 10, textDecoration: "none" }}>LinkedIn</a>
                    )}
                    {c.warmth && <span style={{ fontSize: 9, color: T.inkFaint }}>warmth: {c.warmth}/5</span>}
                  </div>
                ))}
              </div>
            </div>
          )}

          {/* ── Scraped content ── */}
          {content.length > 0 && (
            <div style={{ marginBottom: 8 }}>
              <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 3 }}>
                <Lbl>Scraped Content ({content.length})</Lbl>
                {content.filter((c) => !c.analyzed).length > 0 && (
                  <button onClick={handleAnalyzeAll} disabled={!!busy}
                    style={btn({ fontSize: 9, background: "#f39c1222", opacity: busy === "analyze-all" ? 0.5 : 1 })}>
                    {busy === "analyze-all" ? `Analyzing ${analyzeProgress || ""}...` : `Analyze All (${content.filter((c) => !c.analyzed).length})`}
                  </button>
                )}
              </div>
              <div>
                {content.map((c) => (
                  <ContentItem key={c.id} item={c} onDecide={handleDecideContent}
                    onAnalyze={handleAnalyzeContent} busy={busy} defaultOpen={autoExpandId === c.id} />
                ))}
              </div>
            </div>
          )}

          {/* ── Engagement history ── */}
          {engagement.length > 0 && (
            <div style={{ marginBottom: 8 }}>
              <Lbl>Engagement History ({engagement.length})</Lbl>
              <div style={{ marginTop: 3 }}>
                {engagement.map((e) => (
                  <div key={e.id} style={{ fontSize: 11, padding: "3px 0", borderBottom: `1px solid ${T.ruleLight}`, display: "flex", gap: 8 }}>
                    <span style={{ fontFamily: T.mono, fontSize: 10, color: T.inkLight, minWidth: 110 }}>
                      {e.action_type.replace(/_/g, " ")}
                    </span>
                    {e.channel && <span style={{ fontSize: 10, color: T.inkFaint }}>{e.channel}</span>}
                    <span style={{ flex: 1, color: T.inkMid }}>{e.content || "—"}</span>
                    {e.response && <span style={{ color: "#27ae60", fontSize: 10 }}>replied</span>}
                    <span style={{ fontSize: 9, color: T.inkFaint }}>{new Date(e.created_at).toLocaleDateString()}</span>
                  </div>
                ))}
              </div>
            </div>
          )}

          {/* ── Latest exposure report ── */}
          {(exposure || exposureReport) && (
            <div style={{ marginBottom: 8 }}>
              <Lbl>Latest Exposure Report</Lbl>
              {exposureReport && exposureReport.data && (
                <div style={{ fontSize: 11, fontFamily: T.mono, marginTop: 3, padding: 8, background: T.paperWarm, border: `1px solid ${T.ruleLight}` }}>
                  <div style={{ marginBottom: 4 }}><strong>Weighted SII:</strong> {exposureReport.data.weighted_sii}</div>
                  {exposureReport.data.holdings && exposureReport.data.holdings.length > 0 && (
                    <div style={{ fontSize: 10 }}>
                      {exposureReport.data.holdings.map((h, i) => (
                        <div key={i} style={{ display: "flex", gap: 8, padding: "1px 0" }}>
                          <span style={{ minWidth: 60 }}>{h.token_symbol}</span>
                          <span>${Number(h.balance_usd || 0).toLocaleString()}</span>
                          <span style={{ color: T.inkFaint }}>SII: {h.sii_score || "N/A"}</span>
                        </div>
                      ))}
                    </div>
                  )}
                </div>
              )}
              {exposure && exposure.report_markdown && (
                <pre style={{ fontSize: 10, fontFamily: T.mono, whiteSpace: "pre-wrap", background: T.paperWarm, padding: 8, marginTop: 3, border: `1px solid ${T.ruleLight}` }}>
                  {exposure.report_markdown}
                </pre>
              )}
            </div>
          )}

          {/* ── Empty state nudge ── */}
          {detail && content.length === 0 && engagement.length === 0 && (
            <div style={{ color: T.inkFaint, fontSize: 11, fontStyle: "italic", padding: "4px 0" }}>
              No content scraped and no engagement logged yet. Paste a URL above to start.
            </div>
          )}
        </div>
      )}
    </div>
  );
}

// ─── Target Tracker (accordion list) ──────────────────────────────────

function TargetTracker({ targets, onUpdate }) {
  return (
    <div>
      {(targets || []).map((t) => (
        <TargetRow key={t.id} target={t} onUpdate={onUpdate} />
      ))}
    </div>
  );
}

// ─── Content Feed ─────────────────────────────────────────────────────

function ContentFeed({ feed, onDecide, onAnalyze, busy }) {
  if (!feed || feed.length === 0) return <div style={{ color: T.inkFaint, fontSize: 12 }}>No content scraped yet.</div>;
  return (
    <div>
      {feed.slice(0, 30).map((item) => (
        <ContentItem key={item.id} item={item} onDecide={onDecide}
          onAnalyze={onAnalyze} busy={busy} />
      ))}
    </div>
  );
}

// ─── Section wrapper ──────────────────────────────────────────────────

function Section({ title, actions, children }) {
  const [collapsed, setCollapsed] = useState(false);
  return (
    <div style={{ marginBottom: 16 }}>
      <div style={{
        display: "flex", justifyContent: "space-between", alignItems: "center",
        padding: "6px 10px", background: T.ink, color: T.paper, cursor: "pointer",
      }} onClick={() => setCollapsed(!collapsed)}>
        <span style={{ fontFamily: T.mono, fontSize: 12, fontWeight: 600, letterSpacing: 1 }}>
          {collapsed ? "\u25B6" : "\u25BC"} {title}
        </span>
        <div style={{ display: "flex", gap: 4 }} onClick={(e) => e.stopPropagation()}>{actions}</div>
      </div>
      {!collapsed && (
        <div style={{ border: `1px solid ${T.ruleMid}`, borderTop: "none", padding: "8px 0" }}>{children}</div>
      )}
    </div>
  );
}

// ─── State Growth Panel ──────────────────────────────────────────────

function StateGrowthPanel() {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(false);
  const [expanded, setExpanded] = useState({});
  const [flash, showFlash] = useFlash();

  const load = async () => {
    setLoading(true);
    try {
      const res = await opsFetch("/api/ops/state-growth?days=14");
      setData(res);
    } catch (e) {
      showFlash(e.message, false);
    }
    setLoading(false);
  };

  useEffect(() => { load(); }, []);

  const fmtNum = (n) => n != null ? n.toLocaleString() : "—";
  const deltaColor = (d) => d > 0 ? "#27ae60" : d < 0 ? "#e74c3c" : T.inkFaint;
  const deltaPrefix = (d) => d > 0 ? "+" : "";

  const s = data?.summary;

  return (
    <Section title="STATE GROWTH" actions={
      <button onClick={load} disabled={loading} style={{ fontSize: 9, fontFamily: T.mono, padding: "2px 6px", border: `1px solid ${T.paper}44`, background: "transparent", color: T.paper, cursor: "pointer", opacity: loading ? 0.5 : 1 }}>
        {loading ? "Loading..." : "Refresh"}
      </button>
    }>
      <Flash flash={flash} />
      <div style={{ padding: "0 10px" }}>
        {!data && loading && <div style={{ color: T.inkFaint, fontSize: 12 }}>Loading state growth...</div>}
        {!data && !loading && <div style={{ color: T.inkFaint, fontSize: 12 }}>No data</div>}

        {/* Summary bar */}
        {s && (
          <div style={{ display: "flex", gap: 16, flexWrap: "wrap", marginBottom: 12, padding: "8px 0", borderBottom: `1px solid ${T.ruleLight}` }}>
            <div>
              <Lbl>Total Records</Lbl>
              <div style={{ fontSize: 18, fontFamily: T.mono, fontWeight: 700, color: T.ink }}>{fmtNum(s.total_records_now)}</div>
            </div>
            <div>
              <Lbl>7d Growth</Lbl>
              <div style={{ fontSize: 18, fontFamily: T.mono, fontWeight: 700, color: deltaColor(s.total_growth_7d) }}>{deltaPrefix(s.total_growth_7d)}{fmtNum(s.total_growth_7d)}</div>
            </div>
            <div>
              <Lbl>Avg Daily</Lbl>
              <div style={{ fontSize: 14, fontFamily: T.mono, fontWeight: 600, color: T.inkMid }}>{deltaPrefix(s.avg_daily_growth)}{fmtNum(s.avg_daily_growth)}</div>
            </div>
            <div>
              <Lbl>Fastest</Lbl>
              <div style={{ fontSize: 12, fontFamily: T.mono, color: "#27ae60" }}>{s.fastest_growing || "—"}</div>
            </div>
            {s.stalled && s.stalled.length > 0 && (
              <div>
                <Lbl>Stalled</Lbl>
                <div style={{ fontSize: 11, fontFamily: T.mono, color: "#e74c3c" }}>{s.stalled.join(", ")}</div>
              </div>
            )}
          </div>
        )}

        {/* Day-by-day table */}
        {data?.days && data.days.length > 0 && (
          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 11, fontFamily: T.mono }}>
            <thead>
              <tr style={{ borderBottom: `1px solid ${T.ruleMid}` }}>
                <th style={{ textAlign: "left", padding: "4px 6px", fontWeight: 600, color: T.inkLight, fontSize: 10 }}>DATE</th>
                <th style={{ textAlign: "right", padding: "4px 6px", fontWeight: 600, color: T.inkLight, fontSize: 10 }}>TOTAL</th>
                <th style={{ textAlign: "right", padding: "4px 6px", fontWeight: 600, color: T.inkLight, fontSize: 10 }}>DELTA</th>
                <th style={{ width: 24 }}></th>
              </tr>
            </thead>
            <tbody>
              {data.days.map((day) => {
                const isExp = expanded[day.date];
                const bk = day.breakdown || {};
                return (
                  <>
                    <tr key={day.date} style={{ borderBottom: `1px solid ${T.ruleLight}`, cursor: "pointer" }} onClick={() => setExpanded((p) => ({ ...p, [day.date]: !p[day.date] }))}>
                      <td style={{ padding: "4px 6px", color: T.inkMid }}>{day.date}</td>
                      <td style={{ padding: "4px 6px", textAlign: "right" }}>{fmtNum(day.total_records)}</td>
                      <td style={{ padding: "4px 6px", textAlign: "right", color: deltaColor(day.delta) }}>
                        {day.delta != null ? `${deltaPrefix(day.delta)}${fmtNum(day.delta)}` : "—"}
                      </td>
                      <td style={{ textAlign: "center", color: T.inkFaint, fontSize: 10 }}>{isExp ? "\u25BC" : "\u25B6"}</td>
                    </tr>
                    {isExp && Object.keys(bk).length > 0 && (
                      <tr key={day.date + "-detail"}>
                        <td colSpan={4} style={{ padding: "2px 6px 8px 20px", background: T.paperWarm }}>
                          <div style={{ display: "grid", gridTemplateColumns: "1fr auto auto", gap: "1px 12px" }}>
                            {Object.entries(bk).map(([field, info]) => (
                              <div key={field} style={{ display: "contents" }}>
                                <span style={{ color: T.inkMid, padding: "2px 0" }}>{field.replace(/_/g, " ")}</span>
                                <span style={{ textAlign: "right", padding: "2px 0" }}>{fmtNum(info.value)}</span>
                                <span style={{ textAlign: "right", padding: "2px 0", color: deltaColor(info.delta) }}>
                                  {info.delta != null ? `${deltaPrefix(info.delta)}${fmtNum(info.delta)}` : "—"}
                                </span>
                              </div>
                            ))}
                          </div>
                        </td>
                      </tr>
                    )}
                  </>
                );
              })}
            </tbody>
          </table>
        )}
      </div>
    </Section>
  );
}


// ─── Main Dashboard ───────────────────────────────────────────────────

function SeedMetricsPanel() {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(false);
  const [flash, showFlash] = useFlash();

  const load = async () => {
    setLoading(true);
    try {
      const res = await opsFetch("/api/ops/seed-metrics");
      setData(res);
    } catch (e) { showFlash(e.message, false); }
    setLoading(false);
  };

  useEffect(() => { load(); }, []);

  const statCard = (label, value, sub) => (
    <div style={{ flex: 1, padding: "10px 12px", border: `1px solid ${T.ruleMid}`, background: T.paperWarm }}>
      <div style={{ fontFamily: T.mono, fontSize: 9, textTransform: "uppercase", letterSpacing: 1.5, color: T.inkLight, marginBottom: 4 }}>{label}</div>
      <div style={{ fontFamily: T.mono, fontSize: 22, fontWeight: 700, color: T.ink }}>{value}</div>
      {sub && <div style={{ fontFamily: T.mono, fontSize: 10, color: T.inkFaint, marginTop: 2 }}>{sub}</div>}
    </div>
  );

  const channelDot = (status) => {
    if (status === true) return { bg: "#22c55e", label: "live" };
    if (status === false) return { bg: "#9a9a9a", label: "not built" };
    return { bg: "#eab308", label: status };
  };

  return (
    <>
      <Section title="SEED METRICS" actions={
        <button onClick={load} disabled={loading} style={{ fontSize: 9, fontFamily: T.mono, padding: "2px 6px", border: `1px solid ${T.paper}44`, background: "transparent", color: T.paper, cursor: "pointer", opacity: loading ? 0.5 : 1 }}>
          {loading ? "Loading..." : "Refresh"}
        </button>
      }>
        <Flash flash={flash} />
        <div style={{ padding: "0 10px" }}>
          {!data && loading && <div style={{ color: T.inkFaint, fontSize: 12 }}>Loading seed metrics...</div>}
          {!data && !loading && <div style={{ color: T.inkFaint, fontSize: 12 }}>No data yet.</div>}
          {data && !data.error && (
            <>
              {/* Header cards */}
              <div style={{ display: "flex", gap: 8, marginBottom: 16 }}>
                {statCard("External Requests Today", data.realtime?.external_requests_today ?? 0, `${data.realtime?.requests_today ?? 0} total`)}
                {statCard("Active API Keys (7d)", data.active_api_keys_7d ?? 0)}
                {statCard("MCP Tool Calls (30d)", data.month_totals?.mcp_tool_calls ?? 0)}
                {statCard("Channels Live", data.channels_live_count ?? 0, `of ${Object.keys(data.channels || {}).length}`)}
              </div>

              {/* 7-day trend */}
              {data.trend_7d && data.trend_7d.length > 0 && (
                <div style={{ marginBottom: 16 }}>
                  <div style={{ fontFamily: T.mono, fontSize: 10, textTransform: "uppercase", letterSpacing: 1.5, color: T.inkLight, marginBottom: 6 }}>7-Day Trend</div>
                  <div style={{ display: "flex", gap: 4, alignItems: "flex-end", height: 60 }}>
                    {[...data.trend_7d].reverse().map((d, i) => {
                      const maxReq = Math.max(...data.trend_7d.map(r => r.external_api_requests || 1));
                      const h = Math.max(4, ((d.external_api_requests || 0) / maxReq) * 56);
                      return (
                        <div key={i} style={{ flex: 1, display: "flex", flexDirection: "column", alignItems: "center", gap: 2 }}>
                          <div style={{ fontFamily: T.mono, fontSize: 8, color: T.inkFaint }}>{d.external_api_requests || 0}</div>
                          <div style={{ width: "100%", height: h, background: T.ink, borderRadius: 1 }} />
                          <div style={{ fontFamily: T.mono, fontSize: 8, color: T.inkFaint }}>{String(d.date).slice(5)}</div>
                        </div>
                      );
                    })}
                  </div>
                </div>
              )}
            </>
          )}
          {data?.error && <div style={{ color: T.accent, fontSize: 11 }}>Error: {data.error}</div>}
        </div>
      </Section>

      {data && !data.error && (
        <>
          {/* Top External Consumers */}
          <Section title="TOP EXTERNAL CONSUMERS (7d)">
            <div style={{ padding: "0 10px" }}>
              {(data.top_external_consumers || []).length === 0
                ? <div style={{ color: T.inkFaint, fontSize: 11 }}>No external consumers yet.</div>
                : <table style={{ width: "100%", fontSize: 11, fontFamily: T.mono, borderCollapse: "collapse" }}>
                    <thead>
                      <tr style={{ borderBottom: `1px solid ${T.ruleMid}`, fontSize: 10, color: T.inkLight, textAlign: "left" }}>
                        <th style={{ padding: "4px 0" }}>IP</th><th>User Agent</th><th style={{ textAlign: "right" }}>Requests</th>
                      </tr>
                    </thead>
                    <tbody>
                      {data.top_external_consumers.map((c, i) => (
                        <tr key={i} style={{ borderBottom: `1px solid ${T.ruleLight}` }}>
                          <td style={{ padding: "3px 0", fontSize: 10 }}>{c.ip_address}</td>
                          <td style={{ fontSize: 10, color: T.inkMid, maxWidth: 300, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{c.ua}</td>
                          <td style={{ textAlign: "right", fontWeight: 600 }}>{c.requests}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
              }
            </div>
          </Section>

          {/* MCP Tool Breakdown */}
          <Section title="MCP TOOL CALLS (7d)">
            <div style={{ padding: "0 10px" }}>
              {(data.mcp_tool_breakdown || []).length === 0
                ? <div style={{ color: T.inkFaint, fontSize: 11 }}>No MCP tool calls recorded yet.</div>
                : <table style={{ width: "100%", fontSize: 11, fontFamily: T.mono, borderCollapse: "collapse" }}>
                    <thead>
                      <tr style={{ borderBottom: `1px solid ${T.ruleMid}`, fontSize: 10, color: T.inkLight, textAlign: "left" }}>
                        <th style={{ padding: "4px 0" }}>Tool</th><th style={{ textAlign: "right" }}>Calls</th>
                      </tr>
                    </thead>
                    <tbody>
                      {data.mcp_tool_breakdown.map((t, i) => (
                        <tr key={i} style={{ borderBottom: `1px solid ${T.ruleLight}` }}>
                          <td style={{ padding: "3px 0" }}>{t.tool_name}</td>
                          <td style={{ textAlign: "right", fontWeight: 600 }}>{t.calls}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
              }
            </div>
          </Section>

          {/* Top Entities */}
          <Section title="MOST QUERIED ENTITIES (7d)">
            <div style={{ padding: "0 10px" }}>
              {(data.top_entities || []).length === 0
                ? <div style={{ color: T.inkFaint, fontSize: 11 }}>No entity lookups yet.</div>
                : <table style={{ width: "100%", fontSize: 11, fontFamily: T.mono, borderCollapse: "collapse" }}>
                    <thead>
                      <tr style={{ borderBottom: `1px solid ${T.ruleMid}`, fontSize: 10, color: T.inkLight, textAlign: "left" }}>
                        <th style={{ padding: "4px 0" }}>Type</th><th>Entity</th><th style={{ textAlign: "right" }}>Lookups</th>
                      </tr>
                    </thead>
                    <tbody>
                      {data.top_entities.map((e, i) => (
                        <tr key={i} style={{ borderBottom: `1px solid ${T.ruleLight}` }}>
                          <td style={{ padding: "3px 0", fontSize: 10, color: T.inkLight }}>{e.entity_type}</td>
                          <td style={{ fontSize: 10 }}>{e.entity_id}</td>
                          <td style={{ textAlign: "right", fontWeight: 600 }}>{e.lookups}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
              }
            </div>
          </Section>

          {/* Channels Status */}
          <Section title="DISTRIBUTION CHANNELS">
            <div style={{ padding: "0 10px", display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 6 }}>
              {Object.entries(data.channels || {}).map(([name, status]) => {
                const dot = channelDot(status);
                return (
                  <div key={name} style={{ display: "flex", alignItems: "center", gap: 6, fontSize: 11, fontFamily: T.mono, padding: "3px 0" }}>
                    <span style={{ width: 8, height: 8, borderRadius: "50%", background: dot.bg, display: "inline-block", flexShrink: 0 }} />
                    <span>{name.replace(/_/g, " ")}</span>
                    {dot.label !== "live" && dot.label !== "not built" && (
                      <span style={{ fontSize: 9, color: T.inkFaint }}>({dot.label})</span>
                    )}
                  </div>
                );
              })}
            </div>
          </Section>

          {/* Keeper Publishes */}
          {data.keeper_publishes && data.keeper_publishes.length > 0 && (
            <Section title="ORACLE KEEPER (7d)">
              <div style={{ padding: "0 10px" }}>
                {data.keeper_publishes.map((k, i) => (
                  <div key={i} style={{ fontSize: 11, fontFamily: T.mono, padding: "3px 0", borderBottom: `1px solid ${T.ruleLight}` }}>
                    <strong>{k.chain}</strong> — {k.publishes} publishes — last: {k.last_publish ? new Date(k.last_publish).toLocaleString() : "—"}
                  </div>
                ))}
              </div>
            </Section>
          )}
        </>
      )}
    </>
  );
}

function ProtocolDeepDive({ slug }) {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [expandedCat, setExpandedCat] = useState(null);

  useEffect(() => {
    (async () => {
      try {
        const res = await opsFetch(`/api/ops/protocol/${slug}/deep-dive`);
        setData(res);
      } catch (e) { setError(e.message); }
      setLoading(false);
    })();
  }, [slug]);

  if (loading) return <div style={{ padding: 20, fontFamily: T.mono, fontSize: 12, color: T.inkLight }}>Loading protocol deep dive...</div>;
  if (error) return <div style={{ padding: 20, fontFamily: T.mono, fontSize: 12, color: T.accent }}>Error: {error}</div>;
  if (!data) return null;

  const confColor = (c) => c === "high" ? "#22c55e" : c === "standard" ? "#eab308" : "#ef4444";
  const statusDot = (s) => s === "available" ? "#22c55e" : "#9a9a9a";

  return (
    <div style={{ maxWidth: 900, margin: "0 auto", padding: 20, fontFamily: T.sans, color: T.ink, background: T.paper, minHeight: "100vh" }}>
      <div style={{ marginBottom: 16 }}>
        <a href="/ops" style={{ fontFamily: T.mono, fontSize: 10, color: T.inkLight, textDecoration: "none" }}>&larr; Back to Ops</a>
      </div>

      {/* Header */}
      <div style={{ marginBottom: 24, borderBottom: `1px solid ${T.ruleMid}`, paddingBottom: 16 }}>
        <div style={{ fontFamily: T.mono, fontSize: 9, textTransform: "uppercase", letterSpacing: 1.5, color: T.inkLight }}>Protocol Deep Dive</div>
        <div style={{ fontSize: 24, fontWeight: 700, marginTop: 4 }}>{data.protocol_name}</div>
        <div style={{ display: "flex", gap: 16, marginTop: 8, fontFamily: T.mono, fontSize: 13 }}>
          <span>PSI Score: <strong>{data.score}</strong></span>
          <span>Grade: <strong>{data.grade}</strong></span>
          <span style={{ color: confColor(data.confidence) }}>Confidence: {data.confidence}{data.confidence_tag ? ` (${data.confidence_tag})` : ""}</span>
          <span style={{ color: T.inkFaint }}>Coverage: {(data.component_coverage * 100).toFixed(0)}% ({data.components_populated}/{data.components_total})</span>
        </div>
        {data.missing_categories.length > 0 && (
          <div style={{ fontFamily: T.mono, fontSize: 10, color: T.inkFaint, marginTop: 4 }}>
            Missing categories: {data.missing_categories.join(", ")}
          </div>
        )}
      </div>

      {/* Category Breakdown */}
      <Section title="CATEGORY BREAKDOWN">
        <div style={{ padding: "0 10px" }}>
          {Object.entries(data.category_breakdown || {}).map(([catId, cat]) => (
            <div key={catId} style={{ marginBottom: 8 }}>
              <div
                onClick={() => setExpandedCat(expandedCat === catId ? null : catId)}
                style={{ display: "flex", justifyContent: "space-between", cursor: "pointer", padding: "6px 0", borderBottom: `1px solid ${T.ruleLight}` }}
              >
                <span style={{ fontFamily: T.mono, fontSize: 12, fontWeight: 600 }}>
                  {expandedCat === catId ? "\u25BC" : "\u25B6"} {cat.name}
                  <span style={{ fontWeight: 400, color: T.inkLight, marginLeft: 8 }}>weight: {(cat.weight * 100).toFixed(0)}%</span>
                </span>
                <span style={{ fontFamily: T.mono, fontSize: 12 }}>
                  {cat.score != null ? cat.score.toFixed(1) : "—"}
                  <span style={{ fontSize: 10, color: T.inkFaint, marginLeft: 6 }}>{cat.components_populated}/{cat.components_total} components</span>
                </span>
              </div>
              {expandedCat === catId && (
                <table style={{ width: "100%", fontSize: 10, fontFamily: T.mono, borderCollapse: "collapse", marginTop: 4 }}>
                  <thead>
                    <tr style={{ color: T.inkLight, textAlign: "left", borderBottom: `1px solid ${T.ruleMid}` }}>
                      <th style={{ padding: "3px 0" }}>Component</th><th>Raw</th><th>Score</th><th>Source</th><th>Status</th>
                    </tr>
                  </thead>
                  <tbody>
                    {cat.components.map((c) => (
                      <tr key={c.id} style={{ borderBottom: `1px solid ${T.ruleLight}` }}>
                        <td style={{ padding: "3px 0" }}>{c.name}</td>
                        <td>{c.raw_value != null ? (typeof c.raw_value === "number" ? c.raw_value.toLocaleString() : String(c.raw_value)) : "—"}</td>
                        <td>{c.normalized_score != null ? c.normalized_score.toFixed(1) : "—"}</td>
                        <td style={{ color: T.inkFaint }}>{c.data_source}</td>
                        <td><span style={{ display: "inline-block", width: 8, height: 8, borderRadius: "50%", background: statusDot(c.status) }} /></td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
            </div>
          ))}
        </div>
      </Section>

      {/* Score History */}
      {data.score_history && data.score_history.length > 0 && (
        <Section title={`SCORE HISTORY (${data.score_history.length} entries)`}>
          <div style={{ padding: "0 10px" }}>
            <div style={{ display: "flex", gap: 4, alignItems: "flex-end", height: 80, marginBottom: 8 }}>
              {[...data.score_history].reverse().slice(-30).map((h, i) => {
                const barH = Math.max(4, ((h.score || 0) / 100) * 76);
                return (
                  <div key={i} style={{ flex: 1, display: "flex", flexDirection: "column", alignItems: "center", gap: 1 }}>
                    <div style={{ fontFamily: T.mono, fontSize: 7, color: T.inkFaint }}>{h.score ? Math.round(h.score) : ""}</div>
                    <div style={{ width: "100%", height: barH, background: T.ink, borderRadius: 1 }} />
                  </div>
                );
              })}
            </div>
          </div>
        </Section>
      )}

      {/* Stablecoin Exposure */}
      <Section title="STABLECOIN EXPOSURE">
        <div style={{ padding: "0 10px" }}>
          {(data.stablecoin_exposure?.treasury || []).length > 0 && (
            <div style={{ marginBottom: 12 }}>
              <div style={{ fontFamily: T.mono, fontSize: 10, color: T.inkLight, textTransform: "uppercase", letterSpacing: 1, marginBottom: 4 }}>Treasury Holdings</div>
              <table style={{ width: "100%", fontSize: 10, fontFamily: T.mono, borderCollapse: "collapse" }}>
                <thead><tr style={{ borderBottom: `1px solid ${T.ruleMid}`, color: T.inkLight, textAlign: "left" }}>
                  <th style={{ padding: "3px 0" }}>Token</th><th style={{ textAlign: "right" }}>USD Value</th><th>SII Score</th><th>Grade</th>
                </tr></thead>
                <tbody>
                  {data.stablecoin_exposure.treasury.map((t, i) => (
                    <tr key={i} style={{ borderBottom: `1px solid ${T.ruleLight}` }}>
                      <td style={{ padding: "3px 0" }}>{t.token_symbol}</td>
                      <td style={{ textAlign: "right" }}>${(t.usd_value || 0).toLocaleString()}</td>
                      <td>{t.sii_score ?? "—"}</td>
                      <td>{t.sii_grade ?? <span style={{ color: "#ef4444" }}>unscored</span>}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
          {(data.stablecoin_exposure?.collateral || []).length > 0 && (
            <div>
              <div style={{ fontFamily: T.mono, fontSize: 10, color: T.inkLight, textTransform: "uppercase", letterSpacing: 1, marginBottom: 4 }}>Collateral Accepted</div>
              <table style={{ width: "100%", fontSize: 10, fontFamily: T.mono, borderCollapse: "collapse" }}>
                <thead><tr style={{ borderBottom: `1px solid ${T.ruleMid}`, color: T.inkLight, textAlign: "left" }}>
                  <th style={{ padding: "3px 0" }}>Stablecoin</th><th style={{ textAlign: "right" }}>TVL</th><th>Pools</th>
                </tr></thead>
                <tbody>
                  {data.stablecoin_exposure.collateral.map((c, i) => (
                    <tr key={i} style={{ borderBottom: `1px solid ${T.ruleLight}` }}>
                      <td style={{ padding: "3px 0" }}>{c.stablecoin_symbol}</td>
                      <td style={{ textAlign: "right" }}>${(c.tvl_usd || 0).toLocaleString()}</td>
                      <td>{c.pool_count}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
          {(data.stablecoin_exposure?.treasury || []).length === 0 && (data.stablecoin_exposure?.collateral || []).length === 0 && (
            <div style={{ color: T.inkFaint, fontSize: 11 }}>No stablecoin exposure data available.</div>
          )}
        </div>
      </Section>

      {/* CQI Matrix Row */}
      {data.cqi_matrix_row && data.cqi_matrix_row.length > 0 && (
        <Section title="CQI MATRIX">
          <div style={{ padding: "0 10px" }}>
            <table style={{ width: "100%", fontSize: 10, fontFamily: T.mono, borderCollapse: "collapse" }}>
              <thead><tr style={{ borderBottom: `1px solid ${T.ruleMid}`, color: T.inkLight, textAlign: "left" }}>
                <th style={{ padding: "3px 0" }}>Asset</th><th>SII</th><th>PSI</th><th>CQI</th><th>Grade</th><th>Confidence</th>
              </tr></thead>
              <tbody>
                {data.cqi_matrix_row.map((r, i) => (
                  <tr key={i} style={{ borderBottom: `1px solid ${T.ruleLight}` }}>
                    <td style={{ padding: "3px 0", fontWeight: 600 }}>{r.asset}</td>
                    <td>{r.sii_score?.toFixed(1)}</td>
                    <td>{r.psi_score?.toFixed(1)}</td>
                    <td style={{ fontWeight: 600 }}>{r.cqi_score?.toFixed(1)}</td>
                    <td>{r.cqi_grade}</td>
                    <td style={{ color: confColor(r.cqi_confidence) }}>{r.cqi_confidence}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </Section>
      )}

      {/* Risk Summary */}
      <Section title="RISK SUMMARY">
        <div style={{ padding: "0 10px", fontSize: 11, fontFamily: T.mono }}>
          {data.risk_summary?.lowest_category && (
            <div>Weakest category: <strong>{data.risk_summary.lowest_category}</strong> ({data.risk_summary.lowest_category_score?.toFixed(1)})</div>
          )}
          {data.discovery_signals && data.discovery_signals.length > 0 && (
            <div style={{ marginTop: 8 }}>
              <div style={{ fontSize: 10, color: T.inkLight, textTransform: "uppercase", letterSpacing: 1, marginBottom: 4 }}>Discovery Signals</div>
              {data.discovery_signals.map((s, i) => (
                <div key={i} style={{ fontSize: 10, padding: "2px 0", borderBottom: `1px solid ${T.ruleLight}` }}>
                  <span style={{ color: s.severity === "critical" ? "#ef4444" : s.severity === "alert" ? "#f97316" : T.inkMid }}>[{s.severity}]</span>
                  {" "}{s.signal_type} — {typeof s.details === "object" ? JSON.stringify(s.details) : s.details}
                </div>
              ))}
            </div>
          )}
          {(!data.discovery_signals || data.discovery_signals.length === 0) && !data.risk_summary?.lowest_category && (
            <div style={{ color: T.inkFaint }}>No risk signals.</div>
          )}
        </div>
      </Section>

      <div style={{ fontFamily: T.mono, fontSize: 10, color: T.inkFaint, textAlign: "center", marginTop: 24 }}>
        Basis Protocol · Protocol Deep Dive · Internal Use Only
      </div>
    </div>
  );
}

function ReportsPanel() {
  const [entityType, setEntityType] = useState("protocol");
  const [entityId, setEntityId] = useState("");
  const [template, setTemplate] = useState("protocol_risk");
  const [lens, setLens] = useState("");
  const [format, setFormat] = useState("html");
  const [entities, setEntities] = useState([]);
  const [preview, setPreview] = useState(null);
  const [previewFormat, setPreviewFormat] = useState(null);
  const [attestation, setAttestation] = useState(null);
  const [recentReports, setRecentReports] = useState([]);
  const [generating, setGenerating] = useState(false);
  const [batchResults, setBatchResults] = useState([]);

  useEffect(() => {
    loadEntities();
  }, [entityType]);

  useEffect(() => {
    loadRecentReports();
  }, []);

  async function loadEntities() {
    try {
      if (entityType === "stablecoin") {
        const data = await opsFetch("/api/scores");
        setEntities((data.stablecoins || []).map(s => ({ id: s.id, label: `${s.symbol} — ${s.name} (${s.score})` })));
        setEntityId((data.stablecoins || [])[0]?.id || "");
      } else if (entityType === "protocol") {
        const data = await opsFetch("/api/psi/scores");
        setEntities((data.protocols || []).map(p => ({ id: p.slug || p.id, label: `${p.name} (${p.score})` })));
        setEntityId((data.protocols || [])[0]?.slug || (data.protocols || [])[0]?.id || "");
      } else {
        setEntities([]);
        setEntityId("");
      }
    } catch (e) {
      console.error("Failed to load entities:", e);
    }
  }

  async function loadRecentReports() {
    try {
      const data = await opsFetch("/api/ops/reports/recent").catch(() => ({ reports: [] }));
      setRecentReports(data.reports || []);
    } catch (e) {
      setRecentReports([]);
    }
  }

  async function generateReport() {
    if (!entityId) return;
    setGenerating(true);
    setPreview(null);
    setAttestation(null);
    try {
      const params = new URLSearchParams({ template, format });
      if (lens) params.set("lens", lens);

      const key = getAdminKey();
      const resp = await fetch(`/api/reports/${entityType}/${entityId}?${params}`, {
        headers: { "x-admin-key": key },
      });

      if (!resp.ok) throw new Error(`${resp.status} ${resp.statusText}`);

      if (format === "json") {
        const data = await resp.json();
        setPreview(JSON.stringify(data, null, 2));
        setPreviewFormat("json");
        if (data.attestation) setAttestation(data.attestation);
      } else {
        const html = await resp.text();
        setPreview(html);
        setPreviewFormat("html");
      }

      loadRecentReports();
    } catch (e) {
      setPreview(`Error: ${e.message}`);
      setPreviewFormat("error");
    }
    setGenerating(false);
  }

  async function generateBatch(batchType) {
    setGenerating(true);
    setBatchResults([]);
    const results = [];

    let items = [];
    if (batchType === "top5_basel") {
      items = [
        { type: "stablecoin", id: "usdc", template: "compliance", lens: "SCO60" },
        { type: "stablecoin", id: "usdt", template: "compliance", lens: "SCO60" },
        { type: "stablecoin", id: "dai", template: "compliance", lens: "SCO60" },
        { type: "stablecoin", id: "usde", template: "compliance", lens: "SCO60" },
        { type: "stablecoin", id: "fdusd", template: "compliance", lens: "SCO60" },
      ];
    } else if (batchType === "all_protocols") {
      const data = await opsFetch("/api/psi/scores").catch(() => ({ protocols: [] }));
      items = (data.protocols || []).map(p => ({
        type: "protocol", id: p.slug || p.id, template: "protocol_risk", lens: "",
      }));
    }

    for (const item of items) {
      try {
        const params = new URLSearchParams({ template: item.template, format: "json" });
        if (item.lens) params.set("lens", item.lens);
        const key = getAdminKey();
        const resp = await fetch(`/api/reports/${item.type}/${item.id}?${params}`, {
          headers: { "x-admin-key": key },
        });
        if (resp.ok) {
          results.push({ ...item, status: "ok" });
        } else {
          results.push({ ...item, status: `error: ${resp.status}` });
        }
      } catch (e) {
        results.push({ ...item, status: `error: ${e.message}` });
      }
    }

    setBatchResults(results);
    setGenerating(false);
    loadRecentReports();
  }

  const templateOptions = entityType === "wallet"
    ? ["wallet_risk"]
    : ["protocol_risk", "compliance", "underwriting", "sbt_metadata"];

  const lensOptions = ["", "SCO60", "MICA67", "GENIUS"];
  const showLens = template === "compliance";

  return (
    <>
      {/* Generator */}
      <Section title="REPORT GENERATOR">
        <div style={{ display: "flex", gap: 12, flexWrap: "wrap", marginBottom: 16 }}>
          {/* Entity Type */}
          <div>
            <div style={{ fontSize: 9, fontFamily: T.mono, color: T.inkLight, marginBottom: 3, textTransform: "uppercase" }}>Entity Type</div>
            <select value={entityType} onChange={e => { setEntityType(e.target.value); setTemplate(e.target.value === "wallet" ? "wallet_risk" : "protocol_risk"); }}
              style={{ fontFamily: T.mono, fontSize: 11, padding: "4px 8px", border: `1px solid ${T.ruleMid}`, background: T.paper }}>
              <option value="protocol">Protocol</option>
              <option value="stablecoin">Stablecoin</option>
              <option value="wallet">Wallet</option>
            </select>
          </div>

          {/* Entity ID */}
          <div style={{ flex: 1, minWidth: 180 }}>
            <div style={{ fontSize: 9, fontFamily: T.mono, color: T.inkLight, marginBottom: 3, textTransform: "uppercase" }}>Entity</div>
            {entityType === "wallet" ? (
              <input value={entityId} onChange={e => setEntityId(e.target.value)} placeholder="0x..."
                style={{ fontFamily: T.mono, fontSize: 11, padding: "4px 8px", border: `1px solid ${T.ruleMid}`, width: "100%", boxSizing: "border-box" }} />
            ) : (
              <select value={entityId} onChange={e => setEntityId(e.target.value)}
                style={{ fontFamily: T.mono, fontSize: 11, padding: "4px 8px", border: `1px solid ${T.ruleMid}`, background: T.paper, maxWidth: 300 }}>
                {entities.map(e => <option key={e.id} value={e.id}>{e.label}</option>)}
              </select>
            )}
          </div>

          {/* Template */}
          <div>
            <div style={{ fontSize: 9, fontFamily: T.mono, color: T.inkLight, marginBottom: 3, textTransform: "uppercase" }}>Template</div>
            <select value={template} onChange={e => setTemplate(e.target.value)}
              style={{ fontFamily: T.mono, fontSize: 11, padding: "4px 8px", border: `1px solid ${T.ruleMid}`, background: T.paper }}>
              {templateOptions.map(t => <option key={t} value={t}>{t}</option>)}
            </select>
          </div>

          {/* Lens */}
          {showLens && (
            <div>
              <div style={{ fontSize: 9, fontFamily: T.mono, color: T.inkLight, marginBottom: 3, textTransform: "uppercase" }}>Lens</div>
              <select value={lens} onChange={e => setLens(e.target.value)}
                style={{ fontFamily: T.mono, fontSize: 11, padding: "4px 8px", border: `1px solid ${T.ruleMid}`, background: T.paper }}>
                {lensOptions.map(l => <option key={l} value={l}>{l || "None"}</option>)}
              </select>
            </div>
          )}

          {/* Format */}
          <div>
            <div style={{ fontSize: 9, fontFamily: T.mono, color: T.inkLight, marginBottom: 3, textTransform: "uppercase" }}>Format</div>
            <div style={{ display: "flex", gap: 4 }}>
              <button onClick={() => setFormat("html")} style={format === "html" ? btnActive() : btn()}>HTML</button>
              <button onClick={() => setFormat("json")} style={format === "json" ? btnActive() : btn()}>JSON</button>
            </div>
          </div>

          {/* Generate */}
          <div style={{ display: "flex", alignItems: "flex-end" }}>
            <button onClick={generateReport} disabled={generating || !entityId}
              style={btnActive({ opacity: generating ? 0.5 : 1, padding: "4px 16px" })}>
              {generating ? "Generating..." : "Generate"}
            </button>
          </div>
        </div>
      </Section>

      {/* Quick Actions */}
      <Section title="QUICK ACTIONS">
        <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
          <button onClick={() => { setEntityType("protocol"); setEntityId("aave"); setTemplate("protocol_risk"); setLens(""); setFormat("html"); setTimeout(generateReport, 100); }}
            style={btn()}>Aave Protocol Risk</button>
          <button onClick={() => { setEntityType("stablecoin"); setEntityId("usdc"); setTemplate("compliance"); setLens("SCO60"); setFormat("html"); setTimeout(generateReport, 100); }}
            style={btn()}>USDC Basel SCO60</button>
          <button onClick={() => { setEntityType("stablecoin"); setEntityId("usdc"); setTemplate("compliance"); setLens("MICA67"); setFormat("html"); setTimeout(generateReport, 100); }}
            style={btn()}>USDC MiCA</button>
          <button onClick={() => { setEntityType("stablecoin"); setEntityId("usdc"); setTemplate("compliance"); setLens("GENIUS"); setFormat("html"); setTimeout(generateReport, 100); }}
            style={btn()}>USDC GENIUS Act</button>
          <button onClick={() => generateBatch("top5_basel")} disabled={generating}
            style={btn()}>Batch: Top 5 Basel SCO60</button>
          <button onClick={() => generateBatch("all_protocols")} disabled={generating}
            style={btn()}>Batch: All Protocol Risk</button>
        </div>
        {batchResults.length > 0 && (
          <div style={{ marginTop: 12 }}>
            {batchResults.map((r, i) => (
              <div key={i} style={{ fontSize: 10, fontFamily: T.mono, color: r.status === "ok" ? "#27ae60" : T.accent, marginBottom: 2 }}>
                {r.type}/{r.id} ({r.template}{r.lens ? `+${r.lens}` : ""}) — {r.status}
              </div>
            ))}
          </div>
        )}
      </Section>

      {/* Preview */}
      {preview && (
        <Section title="REPORT PREVIEW">
          {previewFormat === "html" ? (
            <iframe
              srcDoc={preview}
              style={{ width: "100%", height: 600, border: `1px solid ${T.ruleMid}`, borderRadius: 3, background: "#fff" }}
              sandbox="allow-same-origin"
            />
          ) : previewFormat === "json" ? (
            <pre style={{ fontFamily: T.mono, fontSize: 10, background: T.paperWarm, padding: 12, border: `1px solid ${T.ruleLight}`, overflow: "auto", maxHeight: 500, whiteSpace: "pre-wrap" }}>
              {preview}
            </pre>
          ) : (
            <div style={{ fontFamily: T.mono, fontSize: 11, color: T.accent, padding: 12 }}>{preview}</div>
          )}
          {attestation && (
            <div style={{ marginTop: 12, padding: 10, background: T.paperWarm, border: `1px solid ${T.ruleLight}`, fontSize: 10, fontFamily: T.mono }}>
              <div><strong>Report Hash:</strong> {attestation.report_hash}</div>
              <div><strong>Generated:</strong> {attestation.generated_at}</div>
              <div><strong>Methodology:</strong> {attestation.methodology_version}</div>
              {attestation.lens && <div><strong>Lens:</strong> {attestation.lens} v{attestation.lens_version}</div>}
              <div style={{ marginTop: 4 }}>
                <a href={`/api/reports/verify/${attestation.report_hash}`} target="_blank" rel="noopener"
                  style={{ color: T.accent, fontSize: 10 }}>Verify attestation →</a>
              </div>
            </div>
          )}
        </Section>
      )}

      {/* Recent Reports */}
      <Section title="RECENT REPORTS" actions={
        <button onClick={loadRecentReports} style={btn()}>Refresh</button>
      }>
        {recentReports.length === 0 ? (
          <div style={{ fontSize: 11, color: T.inkFaint, fontStyle: "italic" }}>No reports generated yet</div>
        ) : (
          <table style={{ width: "100%", fontSize: 10, fontFamily: T.mono, borderCollapse: "collapse" }}>
            <thead>
              <tr style={{ borderBottom: `1px solid ${T.ruleMid}`, textAlign: "left" }}>
                <th style={{ padding: "4px 8px", color: T.inkLight }}>Generated</th>
                <th style={{ padding: "4px 8px", color: T.inkLight }}>Entity</th>
                <th style={{ padding: "4px 8px", color: T.inkLight }}>Template</th>
                <th style={{ padding: "4px 8px", color: T.inkLight }}>Lens</th>
                <th style={{ padding: "4px 8px", color: T.inkLight }}>Hash</th>
                <th style={{ padding: "4px 8px", color: T.inkLight }}></th>
              </tr>
            </thead>
            <tbody>
              {recentReports.map((r, i) => (
                <tr key={i} style={{ borderBottom: `1px solid ${T.ruleLight}` }}>
                  <td style={{ padding: "4px 8px" }}>{new Date(r.generated_at).toLocaleString()}</td>
                  <td style={{ padding: "4px 8px" }}>{r.entity_type}/{r.entity_id}</td>
                  <td style={{ padding: "4px 8px" }}>{r.template}</td>
                  <td style={{ padding: "4px 8px" }}>{r.lens || "—"}</td>
                  <td style={{ padding: "4px 8px", color: T.inkFaint }}>{(r.report_hash || "").slice(0, 16)}…</td>
                  <td style={{ padding: "4px 8px" }}>
                    <a href={`/api/reports/verify/${r.report_hash}`} target="_blank" rel="noopener" style={{ color: T.accent }}>verify</a>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </Section>
    </>
  );
}

function QueryPanel() {
  const [queryText, setQueryText] = useState("");
  const [results, setResults] = useState(null);
  const [templates, setTemplates] = useState([]);
  const [schema, setSchema] = useState(null);
  const [running, setRunning] = useState(false);
  const [error, setError] = useState(null);

  useEffect(() => {
    opsFetch("/api/query/templates").then(d => setTemplates(d.templates || d || [])).catch(() => {});
    opsFetch("/api/query/schema").then(d => setSchema(d)).catch(() => {});
  }, []);

  async function runQuery() {
    setRunning(true);
    setError(null);
    setResults(null);
    try {
      let parsed;
      try { parsed = JSON.parse(queryText); } catch { throw new Error("Invalid JSON"); }
      const data = await opsFetch("/api/query", { method: "POST", body: JSON.stringify(parsed) });
      setResults(data);
    } catch (e) { setError(e.message); }
    setRunning(false);
  }

  function loadTemplate(t) {
    const q = t.query || t;
    setQueryText(JSON.stringify(q, null, 2));
  }

  return (
    <>
      <Section title="QUERY ENGINE">
        {templates.length > 0 && (
          <div style={{ marginBottom: 12 }}>
            <div style={{ fontSize: 9, fontFamily: T.mono, color: T.inkLight, marginBottom: 6, textTransform: "uppercase" }}>Templates</div>
            <div style={{ display: "flex", gap: 6, flexWrap: "wrap" }}>
              {templates.map((t, i) => (
                <button key={i} onClick={() => loadTemplate(t)} style={btn()}>
                  {t.name || t.label || `Template ${i + 1}`}
                </button>
              ))}
            </div>
          </div>
        )}

        <textarea
          value={queryText}
          onChange={e => setQueryText(e.target.value)}
          placeholder='{"filters": {"min_score": 70, "max_hhi": 6000}, "limit": 50}'
          style={{
            width: "100%", height: 150, fontFamily: T.mono, fontSize: 11,
            padding: 12, border: `1px solid ${T.ruleMid}`, background: T.paperWarm,
            boxSizing: "border-box", resize: "vertical",
          }}
        />

        <div style={{ display: "flex", gap: 8, marginTop: 8 }}>
          <button onClick={runQuery} disabled={running || !queryText.trim()} style={btnActive({ opacity: running ? 0.5 : 1 })}>
            {running ? "Running..." : "Run Query"}
          </button>
          <button onClick={() => setQueryText("")} style={btn()}>Clear</button>
        </div>

        {error && <div style={{ marginTop: 8, fontSize: 11, color: T.accent, fontFamily: T.mono }}>{error}</div>}
      </Section>

      {results && (
        <Section title={`RESULTS · ${Array.isArray(results.results || results) ? (results.results || results).length : "?"} rows`}>
          <pre style={{
            fontFamily: T.mono, fontSize: 10, background: T.paperWarm,
            padding: 12, border: `1px solid ${T.ruleLight}`, overflow: "auto",
            maxHeight: 500, whiteSpace: "pre-wrap",
          }}>
            {JSON.stringify(results, null, 2)}
          </pre>
        </Section>
      )}

      {schema && (
        <Section title="SCHEMA">
          <pre style={{
            fontFamily: T.mono, fontSize: 9, color: T.inkLight,
            background: T.paperWarm, padding: 8, border: `1px solid ${T.ruleLight}`,
            overflow: "auto", maxHeight: 200, whiteSpace: "pre-wrap",
          }}>
            {JSON.stringify(schema, null, 2)}
          </pre>
        </Section>
      )}
    </>
  );
}

function GraphPanel() {
  const [address, setAddress] = useState("");
  const [connections, setConnections] = useState(null);
  const [contagion, setContagion] = useState(null);
  const [actor, setActor] = useState(null);
  const [profile, setProfile] = useState(null);
  const [loading, setLoading] = useState(false);
  const [graphStats, setGraphStats] = useState(null);
  const [activeView, setActiveView] = useState("connections");

  useEffect(() => {
    opsFetch("/api/graph/stats").then(setGraphStats).catch(() => {});
  }, []);

  async function loadWallet() {
    if (!address.trim()) return;
    setLoading(true);
    setConnections(null);
    setContagion(null);
    setActor(null);
    setProfile(null);
    const addr = address.trim().toLowerCase();
    try {
      const [conn, cont, act, prof] = await Promise.all([
        opsFetch(`/api/wallets/${addr}/connections`).catch(() => null),
        opsFetch(`/api/wallets/${addr}/contagion`).catch(() => null),
        opsFetch(`/api/wallets/${addr}/actor`).catch(() => null),
        opsFetch(`/api/wallets/${addr}/profile`).catch(() => null),
      ]);
      setConnections(conn);
      setContagion(cont);
      setActor(act);
      setProfile(prof);
    } catch (e) { console.error(e); }
    setLoading(false);
  }

  const [topWallets, setTopWallets] = useState([]);
  useEffect(() => {
    opsFetch("/api/wallets/top?limit=10").then(d => setTopWallets(d.wallets || d || [])).catch(() => {});
  }, []);

  return (
    <>
      {graphStats && (
        <Section title="GRAPH OVERVIEW">
          <div style={{ display: "flex", gap: 24, flexWrap: "wrap", fontSize: 11, fontFamily: T.mono }}>
            {Object.entries(graphStats).map(([k, v]) => (
              <div key={k}>
                <span style={{ color: T.inkLight }}>{k}: </span>
                <span style={{ color: T.ink, fontWeight: 600 }}>{typeof v === "number" ? v.toLocaleString() : JSON.stringify(v)}</span>
              </div>
            ))}
          </div>
        </Section>
      )}

      <Section title="WALLET EXPLORER">
        <div style={{ display: "flex", gap: 8, marginBottom: 12 }}>
          <input
            value={address}
            onChange={e => setAddress(e.target.value)}
            onKeyDown={e => e.key === "Enter" && loadWallet()}
            placeholder="0x... wallet address"
            style={{ flex: 1, fontFamily: T.mono, fontSize: 11, padding: "6px 10px", border: `1px solid ${T.ruleMid}` }}
          />
          <button onClick={loadWallet} disabled={loading} style={btnActive()}>
            {loading ? "Loading..." : "Explore"}
          </button>
        </div>

        {topWallets.length > 0 && (
          <div style={{ display: "flex", gap: 4, flexWrap: "wrap", marginBottom: 12 }}>
            <span style={{ fontSize: 9, color: T.inkFaint, fontFamily: T.mono }}>Top wallets: </span>
            {topWallets.slice(0, 8).map((w, i) => (
              <button key={i} onClick={() => { setAddress(w.address || w); setTimeout(loadWallet, 50); }}
                style={{ ...btn(), fontSize: 9 }}>
                {(w.address || w).slice(0, 6)}...{(w.address || w).slice(-4)}
              </button>
            ))}
          </div>
        )}

        {(connections || contagion || actor) && (
          <div style={{ display: "flex", gap: 6, marginBottom: 12 }}>
            {["connections", "contagion", "actor", "profile"].map(v => (
              <button key={v} onClick={() => setActiveView(v)}
                style={activeView === v ? btnActive() : btn()}>
                {v}
              </button>
            ))}
          </div>
        )}

        {profile && activeView === "profile" && (
          <pre style={{ fontFamily: T.mono, fontSize: 10, background: T.paperWarm, padding: 12, border: `1px solid ${T.ruleLight}`, overflow: "auto", maxHeight: 400, whiteSpace: "pre-wrap" }}>
            {JSON.stringify(profile, null, 2)}
          </pre>
        )}

        {connections && activeView === "connections" && (
          <div>
            <div style={{ fontSize: 10, fontFamily: T.mono, color: T.inkLight, marginBottom: 8 }}>
              {Array.isArray(connections.connections || connections) ? (connections.connections || connections).length : "?"} connections
            </div>
            <pre style={{ fontFamily: T.mono, fontSize: 10, background: T.paperWarm, padding: 12, border: `1px solid ${T.ruleLight}`, overflow: "auto", maxHeight: 400, whiteSpace: "pre-wrap" }}>
              {JSON.stringify(connections, null, 2)}
            </pre>
          </div>
        )}

        {contagion && activeView === "contagion" && (
          <div>
            <div style={{ fontSize: 10, fontFamily: T.mono, color: T.accent, marginBottom: 8 }}>
              Depth-3 contagion blast radius
            </div>
            <pre style={{ fontFamily: T.mono, fontSize: 10, background: T.paperWarm, padding: 12, border: `1px solid ${T.ruleLight}`, overflow: "auto", maxHeight: 400, whiteSpace: "pre-wrap" }}>
              {JSON.stringify(contagion, null, 2)}
            </pre>
          </div>
        )}

        {actor && activeView === "actor" && (
          <pre style={{ fontFamily: T.mono, fontSize: 10, background: T.paperWarm, padding: 12, border: `1px solid ${T.ruleLight}`, overflow: "auto", maxHeight: 400, whiteSpace: "pre-wrap" }}>
            {JSON.stringify(actor, null, 2)}
          </pre>
        )}
      </Section>
    </>
  );
}

function BacktestPanel() {
  const [entityType, setEntityType] = useState("stablecoin");
  const [entityId, setEntityId] = useState("usdc");
  const [event, setEvent] = useState("svb");
  const [results, setResults] = useState(null);
  const [loading, setLoading] = useState(false);
  const [entities, setEntities] = useState([]);
  const [scoreHistory, setScoreHistory] = useState(null);

  const events = [
    { id: "svb", label: "SVB Crisis (Mar 2023)" },
    { id: "terra_luna", label: "Terra/Luna Collapse (May 2022)" },
    { id: "drift_exploit", label: "Drift Exploit (Apr 2026)" },
  ];

  useEffect(() => {
    if (entityType === "stablecoin") {
      opsFetch("/api/scores").then(d => {
        setEntities((d.stablecoins || []).map(s => ({ id: s.id, label: s.symbol })));
        setEntityId((d.stablecoins || [])[0]?.id || "usdc");
      }).catch(() => {});
    } else {
      opsFetch("/api/psi/scores").then(d => {
        setEntities((d.protocols || []).map(p => ({ id: p.slug || p.id, label: p.name })));
        setEntityId((d.protocols || [])[0]?.slug || "aave");
      }).catch(() => {});
    }
  }, [entityType]);

  async function runBacktest() {
    setLoading(true);
    setResults(null);
    setScoreHistory(null);
    try {
      let data;
      if (entityType === "stablecoin") {
        data = await opsFetch(`/api/backtest/${entityId}?event=${event}`);
      } else {
        data = await opsFetch(`/api/psi/scores/${entityId}/backtest/${event}`);
      }
      setResults(data);
    } catch (e) {
      setResults({ error: e.message });
    }
    try {
      if (entityType === "stablecoin") {
        const hist = await opsFetch(`/api/scores/${entityId}/history?days=365`);
        setScoreHistory(hist);
      }
    } catch (e) {}
    setLoading(false);
  }

  async function quickBacktest(type, id, evt) {
    setEntityType(type);
    setEntityId(id);
    setEvent(evt);
    setLoading(true);
    setResults(null);
    try {
      const data = type === "stablecoin"
        ? await opsFetch(`/api/backtest/${id}?event=${evt}`)
        : await opsFetch(`/api/psi/scores/${id}/backtest/${evt}`);
      setResults(data);
    } catch (e) { setResults({ error: e.message }); }
    setLoading(false);
  }

  return (
    <>
      <Section title="CRISIS BACKTESTER">
        <div style={{ display: "flex", gap: 12, flexWrap: "wrap", marginBottom: 12 }}>
          <div>
            <div style={{ fontSize: 9, fontFamily: T.mono, color: T.inkLight, marginBottom: 3, textTransform: "uppercase" }}>Type</div>
            <select value={entityType} onChange={e => setEntityType(e.target.value)}
              style={{ fontFamily: T.mono, fontSize: 11, padding: "4px 8px", border: `1px solid ${T.ruleMid}`, background: T.paper }}>
              <option value="stablecoin">Stablecoin</option>
              <option value="protocol">Protocol</option>
            </select>
          </div>
          <div>
            <div style={{ fontSize: 9, fontFamily: T.mono, color: T.inkLight, marginBottom: 3, textTransform: "uppercase" }}>Entity</div>
            <select value={entityId} onChange={e => setEntityId(e.target.value)}
              style={{ fontFamily: T.mono, fontSize: 11, padding: "4px 8px", border: `1px solid ${T.ruleMid}`, background: T.paper }}>
              {entities.map(e => <option key={e.id} value={e.id}>{e.label}</option>)}
            </select>
          </div>
          <div>
            <div style={{ fontSize: 9, fontFamily: T.mono, color: T.inkLight, marginBottom: 3, textTransform: "uppercase" }}>Crisis Event</div>
            <select value={event} onChange={e => setEvent(e.target.value)}
              style={{ fontFamily: T.mono, fontSize: 11, padding: "4px 8px", border: `1px solid ${T.ruleMid}`, background: T.paper }}>
              {events.map(e => <option key={e.id} value={e.id}>{e.label}</option>)}
            </select>
          </div>
          <div style={{ display: "flex", alignItems: "flex-end" }}>
            <button onClick={runBacktest} disabled={loading} style={btnActive({ opacity: loading ? 0.5 : 1 })}>
              {loading ? "Running..." : "Run Backtest"}
            </button>
          </div>
        </div>

        <div style={{ display: "flex", gap: 6, flexWrap: "wrap" }}>
          <button onClick={() => quickBacktest("stablecoin", "usdc", "svb")} style={btn()}>USDC × SVB</button>
          <button onClick={() => quickBacktest("stablecoin", "usdt", "svb")} style={btn()}>USDT × SVB</button>
          <button onClick={() => quickBacktest("stablecoin", "dai", "terra_luna")} style={btn()}>DAI × Terra</button>
          <button onClick={() => quickBacktest("protocol", "drift", "drift_exploit")} style={btn()}>Drift × Exploit</button>
          <button onClick={() => quickBacktest("stablecoin", "usdt", "terra_luna")} style={btn()}>USDT × Terra</button>
        </div>
      </Section>

      {results && (
        <Section title="BACKTEST RESULTS">
          {results.error ? (
            <div style={{ fontSize: 11, color: T.accent, fontFamily: T.mono }}>{results.error}</div>
          ) : (
            <pre style={{
              fontFamily: T.mono, fontSize: 10, background: T.paperWarm,
              padding: 12, border: `1px solid ${T.ruleLight}`, overflow: "auto",
              maxHeight: 500, whiteSpace: "pre-wrap",
            }}>
              {JSON.stringify(results, null, 2)}
            </pre>
          )}
        </Section>
      )}

      {scoreHistory && (
        <Section title="SCORE HISTORY (365 DAYS)">
          <pre style={{
            fontFamily: T.mono, fontSize: 9, color: T.inkLight,
            background: T.paperWarm, padding: 8, border: `1px solid ${T.ruleLight}`,
            overflow: "auto", maxHeight: 200, whiteSpace: "pre-wrap",
          }}>
            {JSON.stringify(scoreHistory, null, 2)}
          </pre>
        </Section>
      )}
    </>
  );
}

function CqiMatrixPanel() {
  const [matrix, setMatrix] = useState(null);
  const [loading, setLoading] = useState(false);
  const [selectedPair, setSelectedPair] = useState(null);
  const [pairDetail, setPairDetail] = useState(null);

  async function loadMatrix() {
    setLoading(true);
    try {
      const data = await opsFetch("/api/compose/cqi/matrix");
      setMatrix(data);
    } catch (e) {
      console.error(e);
    }
    setLoading(false);
  }

  useEffect(() => { loadMatrix(); }, []);

  async function loadPairDetail(asset, protocol) {
    setSelectedPair(`${asset}×${protocol}`);
    try {
      const data = await opsFetch(`/api/compose/cqi?asset=${asset}&protocol=${protocol}`);
      setPairDetail(data);
    } catch (e) {
      setPairDetail({ error: e.message });
    }
  }

  const protocols = matrix ? [...new Set((matrix.pairs || matrix.matrix || []).map(p => p.protocol || p.protocol_slug))] : [];
  const assets = matrix ? [...new Set((matrix.pairs || matrix.matrix || []).map(p => p.asset || p.stablecoin || p.symbol))] : [];

  const pairMap = {};
  (matrix?.pairs || matrix?.matrix || []).forEach(p => {
    const key = `${p.asset || p.stablecoin || p.symbol}×${p.protocol || p.protocol_slug}`;
    pairMap[key] = p;
  });

  function cqiColor(score) {
    if (!score) return T.inkFaint;
    if (score >= 80) return "#27ae60";
    if (score >= 60) return "#f39c12";
    return "#e74c3c";
  }

  return (
    <>
      <Section title={`CQI COMPOSITION MATRIX · ${assets.length} assets × ${protocols.length} protocols = ${Object.keys(pairMap).length} pairs`} actions={
        <button onClick={loadMatrix} disabled={loading} style={btn()}>{loading ? "Loading..." : "Refresh"}</button>
      }>
        {!matrix ? (
          <div style={{ fontSize: 11, color: T.inkFaint, fontStyle: "italic" }}>Loading matrix...</div>
        ) : (
          <div style={{ overflow: "auto", maxHeight: 500 }}>
            <table style={{ borderCollapse: "collapse", fontSize: 10, fontFamily: T.mono }}>
              <thead>
                <tr>
                  <th style={{ padding: "4px 8px", position: "sticky", left: 0, background: T.paper, borderBottom: `1px solid ${T.ruleMid}`, textAlign: "left", color: T.inkLight }}>
                    Asset ↓ / Protocol →
                  </th>
                  {protocols.map(p => (
                    <th key={p} style={{ padding: "4px 6px", borderBottom: `1px solid ${T.ruleMid}`, color: T.inkLight, whiteSpace: "nowrap" }}>
                      {p}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {assets.map(a => (
                  <tr key={a}>
                    <td style={{ padding: "4px 8px", position: "sticky", left: 0, background: T.paper, borderBottom: `1px solid ${T.ruleLight}`, fontWeight: 600 }}>
                      {a}
                    </td>
                    {protocols.map(p => {
                      const pair = pairMap[`${a}×${p}`];
                      const score = pair?.cqi_score || pair?.cqi || pair?.score;
                      return (
                        <td key={p}
                          onClick={() => loadPairDetail(a, p)}
                          style={{
                            padding: "4px 6px", borderBottom: `1px solid ${T.ruleLight}`,
                            color: cqiColor(score), fontWeight: 600, cursor: "pointer",
                            textAlign: "center",
                            background: selectedPair === `${a}×${p}` ? T.paperWarm : "transparent",
                          }}>
                          {score ? score.toFixed(1) : "—"}
                        </td>
                      );
                    })}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </Section>

      {selectedPair && pairDetail && (
        <Section title={`PAIR DETAIL: ${selectedPair}`}>
          <pre style={{
            fontFamily: T.mono, fontSize: 10, background: T.paperWarm,
            padding: 12, border: `1px solid ${T.ruleLight}`, overflow: "auto",
            maxHeight: 300, whiteSpace: "pre-wrap",
          }}>
            {JSON.stringify(pairDetail, null, 2)}
          </pre>
        </Section>
      )}
    </>
  );
}

export default function OpsDashboard() {
  // Check for protocol deep-dive route
  const pathMatch = window.location.pathname.match(/^\/ops\/protocol\/([^/]+)/);
  if (pathMatch) {
    const slug = pathMatch[1];
    return <ProtocolDeepDive slug={slug} />;
  }

  const [authed, setAuthed] = useState(!!getAdminKey());
  const [health, setHealth] = useState([]);
  const [queue, setQueue] = useState([]);
  const [targets, setTargets] = useState([]);
  const [feed, setFeed] = useState([]);
  const [contentItems, setContentItems] = useState([]);
  const [tab, setTab] = useState("dashboard");
  const [error, setError] = useState(null);
  const [loading, setLoading] = useState(false);
  const [busy, setBusy] = useState(null); // tracks header button in-flight
  const [flash, showFlash] = useFlash();
  const [decidingId, setDecidingId] = useState(null);

  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const [h, q, t, cf, ci] = await Promise.all([
        opsFetch("/api/ops/health").catch(() => ({ health: [] })),
        opsFetch("/api/ops/queue").catch(() => ({ queue: [] })),
        opsFetch("/api/ops/targets").catch(() => ({ targets: [] })),
        opsFetch("/api/ops/content/feed?limit=30").catch(() => ({ feed: [] })),
        opsFetch("/api/ops/content/items").catch(() => ({ items: [] })),
      ]);
      setHealth(h.health || []);
      setQueue(q.queue || []);
      setTargets(t.targets || []);
      setFeed(cf.feed || []);
      setContentItems(ci.items || []);
    } catch (e) {
      if (e.message === "unauthorized") {
        setAuthed(false);
        localStorage.removeItem("ops_admin_key");
      } else {
        setError(e.message);
      }
    }
    setLoading(false);
  }, []);

  useEffect(() => {
    if (authed) load();
  }, [authed, load]);

  const handleAuth = (key) => { setAdminKey(key); setAuthed(true); };

  const handleDecide = async (contentId, decision) => {
    setDecidingId(contentId);
    try {
      await opsFetch(`/api/ops/content/${contentId}/decide`, { method: "POST", body: JSON.stringify({ decision }) });
      setQueue((prev) => prev.filter((q) => q.id !== contentId));
      showFlash(`Content ${decision}`);
      // Refresh feed to reflect decision
      opsFetch("/api/ops/content/feed?limit=30").then((cf) => setFeed(cf.feed || [])).catch(() => {});
    } catch (e) { showFlash(e.message, false); }
    setDecidingId(null);
  };

  const handleFeedAnalyze = async (contentId) => {
    setBusy(`analyze-${contentId}`);
    try {
      await opsFetch(`/api/ops/analyze/${contentId}`, { method: "POST" });
      showFlash("Analysis complete");
      const cf = await opsFetch("/api/ops/content/feed?limit=30");
      setFeed(cf.feed || []);
    } catch (e) { showFlash(e.message, false); }
    setBusy(null);
  };

  const handleRunHealthCheck = async () => {
    setBusy("health");
    try {
      await opsFetch("/api/ops/health/check", { method: "POST" });
      showFlash("Health check started — refreshing in 8s...");
      // Health check runs in background, poll after delay
      setTimeout(async () => {
        try {
          const res = await opsFetch("/api/ops/health");
          const checks = res.health || [];
          setHealth(checks);
          const healthy = checks.filter((c) => c.status === "healthy").length;
          showFlash(`Health check complete — ${healthy}/${checks.length} healthy`);
        } catch (_) {}
        setBusy(null);
      }, 8000);
    } catch (e) { showFlash(e.message, false); setBusy(null); }
  };

  if (!authed) return <AuthGate onAuth={handleAuth} />;

  const healthSummary = health.length > 0
    ? `${health.filter((h) => h.status === "healthy").length}/${health.length} healthy`
    : "no data";
  const warnings = health.filter((h) => h.status !== "healthy");

  return (
    <div style={{ minHeight: "100vh", background: T.paper, fontFamily: T.sans }}>
      <style>{`
        @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500;600;700&family=IBM+Plex+Sans:wght@400;500;600;700&display=swap');
        * { box-sizing: border-box; }
        @keyframes fadeIn { from { opacity: 0; transform: translateY(4px); } to { opacity: 1; transform: translateY(0); } }
      `}</style>

      <div style={{ maxWidth: 1000, margin: "0 auto", padding: "16px 20px" }}>
        {/* Header */}
        <div style={{
          display: "flex", justifyContent: "space-between", alignItems: "center",
          marginBottom: 16, borderBottom: `3px solid ${T.ink}`, paddingBottom: 8,
        }}>
          <div>
            <h1 style={{ fontFamily: T.mono, fontSize: 16, fontWeight: 700, letterSpacing: 1 }}>BASIS OPERATIONS HUB</h1>
            <div style={{ fontFamily: T.mono, fontSize: 10, color: T.inkFaint, marginTop: 2 }}>
              {new Date().toLocaleDateString("en-US", { weekday: "long", year: "numeric", month: "long", day: "numeric" })}
              {loading && " · loading..."}
            </div>
          </div>
          <div style={{ display: "flex", gap: 6 }}>
            <a href="/" style={{ ...btn(), textDecoration: "none", color: T.ink, display: "flex", alignItems: "center" }}>SII Dashboard</a>
          </div>
        </div>

        {error && (
          <div style={{ padding: "8px 12px", background: "#e74c3c22", border: "1px solid #e74c3c44", fontSize: 12, marginBottom: 12, color: T.accent }}>
            {error}
            <button onClick={() => setError(null)} style={{ marginLeft: 8, border: "none", background: "transparent", cursor: "pointer", fontSize: 14 }}>&times;</button>
          </div>
        )}
        <Flash flash={flash} />

        {/* Tabs */}
        <div style={{ display: "flex", gap: 12, marginBottom: 16 }}>
          {["dashboard", "targets", "content", "metrics", "reports", "query", "graph", "backtest", "matrix"].map((tb) => (
            <button key={tb} onClick={() => setTab(tb)} style={{
              fontFamily: T.mono, fontSize: 11, padding: "4px 0", border: "none",
              background: "transparent", cursor: "pointer", fontWeight: tab === tb ? 700 : 400,
              color: tab === tb ? T.ink : T.inkLight,
              borderBottom: tab === tb ? `2px solid ${T.ink}` : "2px solid transparent",
            }}>
              {tb.charAt(0).toUpperCase() + tb.slice(1)}
            </button>
          ))}
        </div>

        {/* Dashboard tab */}
        {tab === "dashboard" && (
          <>
            <Section title="PIPELINE HEALTH" actions={
              <button onClick={handleRunHealthCheck} disabled={busy === "health"} style={{ fontSize: 9, fontFamily: T.mono, padding: "2px 6px", border: `1px solid ${T.paper}44`, background: "transparent", color: T.paper, cursor: "pointer", opacity: busy === "health" ? 0.5 : 1 }}>
                {busy === "health" ? "Checking..." : "Run Check"}
              </button>
            }>
              <div style={{ padding: "0 10px" }}>
                <div style={{ fontSize: 11, fontFamily: T.mono, color: T.inkMid, marginBottom: 8 }}>
                  {healthSummary}
                  {warnings.length > 0 && <span style={{ color: "#f39c12" }}> · {warnings.length} warning(s): {warnings.map((w) => w.system).join(", ")}</span>}
                </div>
                <HealthPanel health={health} />
              </div>
            </Section>

            <Section title={`ACTION QUEUE (${queue.length} items)`}>
              <ActionQueue queue={queue} onDecide={handleDecide} decidingId={decidingId} />
            </Section>

            <StateGrowthPanel />
          </>
        )}

        {/* Targets tab */}
        {tab === "targets" && (
          <>
            <Section title="TIER 1 — ACTIVE PURSUIT">
              <TargetTracker targets={targets.filter((t) => t.tier === 1)} onUpdate={load} />
            </Section>
            <Section title="TIER 2 — MONITORING">
              <TargetTracker targets={targets.filter((t) => t.tier === 2)} onUpdate={load} />
            </Section>
            <Section title="TIER 3 — WATCH LIST">
              <TargetTracker targets={targets.filter((t) => t.tier === 3)} onUpdate={load} />
            </Section>
          </>
        )}



        {/* Content tab */}
        {tab === "content" && (
          <>
            <Section title="CONTENT ITEMS">
              <div style={{ padding: "0 10px" }}>
                {contentItems.length === 0 ? (
                  <div style={{ color: T.inkFaint, fontSize: 12, lineHeight: 1.6 }}>
                    No scheduled content items yet. Content items track planned posts —
                    forum comments, tweets, governance posts. Use the Signals tab to draft content,
                    or create items via the API (<code style={{ fontFamily: T.mono, fontSize: 10 }}>POST /api/ops/content/items</code>).
                  </div>
                ) : (
                  contentItems.map((item) => (
                    <div key={item.id} style={{ fontSize: 11, padding: "4px 0", borderBottom: `1px solid ${T.ruleLight}`, display: "flex", gap: 8 }}>
                      <span style={{ fontFamily: T.mono, fontSize: 10, color: T.inkLight, minWidth: 60 }}>{item.type}</span>
                      <span style={{ flex: 1 }}>{item.title || "(untitled)"}</span>
                      <StageBadge stage={item.status} />
                      {item.scheduled_for && <span style={{ fontSize: 10, color: T.inkFaint }}>{new Date(item.scheduled_for).toLocaleDateString()}</span>}
                    </div>
                  ))
                )}
              </div>
            </Section>
            <Section title="TARGET CONTENT FEED">
              <div style={{ padding: "0 10px" }}><ContentFeed feed={feed} onDecide={handleDecide} onAnalyze={handleFeedAnalyze} busy={busy} /></div>
            </Section>
          </>
        )}





        {/* Metrics tab */}
        {tab === "metrics" && <SeedMetricsPanel />}

        {tab === "reports" && <ReportsPanel />}

        {tab === "query" && <QueryPanel />}
        {tab === "graph" && <GraphPanel />}
        {tab === "backtest" && <BacktestPanel />}
        {tab === "matrix" && <CqiMatrixPanel />}

        <div style={{ fontFamily: T.mono, fontSize: 10, color: T.inkFaint, textAlign: "center", marginTop: 24, paddingBottom: 16 }}>
          Basis Protocol · Operations Hub · Internal Use Only
        </div>
      </div>
    </div>
  );
}
