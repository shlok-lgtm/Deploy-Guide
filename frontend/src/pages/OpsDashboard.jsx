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

function Section({ title, actions, children, defaultOpen = true }) {
  const [open, setOpen] = useState(defaultOpen);
  return (
    <div style={{ marginTop: 24, border: `1px solid ${T.ruleMid}` }}>
      <div style={{ padding: "10px 20px", borderBottom: open ? `1px solid ${T.ruleLight}` : "none",
        display: "flex", justifyContent: "space-between", alignItems: "center", cursor: "pointer",
      }} onClick={() => setOpen(!open)}>
        <span style={{ fontFamily: T.mono, fontSize: 10, fontWeight: 600, color: T.inkLight,
          textTransform: "uppercase", letterSpacing: 1.5 }}>
          {open ? "\u25BC" : "\u25B6"} {title}
        </span>
        <div style={{ display: "flex", gap: 4 }} onClick={(e) => e.stopPropagation()}>{actions}</div>
      </div>
      {open && <div style={{ padding: "12px 20px" }}>{children}</div>}
    </div>
  );
}

// ─── TabHeader (copied from App.jsx) ────────────────────────────────

function TabHeader({ title, formId, stats, accent, mobile, showOnChain = false }) {
  return (
    <div style={{ border: `1.5px solid ${T.ink}`, marginBottom: 0 }}>
      <div style={{ padding: mobile ? "14px 12px 0" : "18px 24px 0" }}>
        <div style={{ display: "flex", flexDirection: mobile ? "column" : "row", justifyContent: "space-between", alignItems: mobile ? "flex-start" : "center", gap: mobile ? 4 : 0 }}>
          <h1 style={{ margin: 0, fontSize: mobile ? 20 : 28, fontFamily: T.sans, color: T.ink, fontWeight: 400, letterSpacing: -0.3 }}>
            {title}
          </h1>
          <span style={{ fontFamily: T.mono, fontSize: mobile ? 9 : 10, color: T.inkFaint, textTransform: "uppercase", letterSpacing: 2 }}>
            {formId}
          </span>
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
      {accent && (
        <div style={{ height: 3, background: accent }} />
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

        {/* ── Universal Data Layer Live Stats ── */}
        {data?.data_layer && !data.data_layer.error && (() => {
          const dl = data.data_layer;
          const dlSummary = dl.storage || {};
          const wg = dl.wallet_graph || {};
          const ec = dl.entity_coverage || {};
          const dq = dl.data_quality || {};
          const prov = dl.provenance?.sources || {};
          const cats = dl.by_category || {};
          const apiUtil = dl.api_utilization || {};

          return (
            <div style={{ marginTop: 16, borderTop: `1px solid ${T.ruleMid}`, paddingTop: 12 }}>
              <div style={{ fontSize: 10, fontWeight: 700, color: T.inkLight, letterSpacing: 1.5, marginBottom: 8 }}>UNIVERSAL DATA LAYER</div>

              {/* Key metrics bar */}
              <div style={{ display: "flex", gap: 16, flexWrap: "wrap", marginBottom: 12, padding: "8px 0", borderBottom: `1px solid ${T.ruleLight}` }}>
                <div>
                  <Lbl>Tables Tracked</Lbl>
                  <div style={{ fontSize: 18, fontFamily: T.mono, fontWeight: 700, color: T.ink }}>{dlSummary.tables_tracked || 0}</div>
                </div>
                <div>
                  <Lbl>Total Rows</Lbl>
                  <div style={{ fontSize: 18, fontFamily: T.mono, fontWeight: 700, color: T.ink }}>{fmtNum(dlSummary.total_rows)}</div>
                </div>
                <div>
                  <Lbl>Rows +24h</Lbl>
                  <div style={{ fontSize: 18, fontFamily: T.mono, fontWeight: 700, color: "#27ae60" }}>+{fmtNum(dlSummary.rows_added_24h)}</div>
                </div>
                <div>
                  <Lbl>DB Size</Lbl>
                  <div style={{ fontSize: 14, fontFamily: T.mono, fontWeight: 600, color: T.inkMid }}>{dlSummary.actual_db_size_mb ? `${dlSummary.actual_db_size_mb} MB` : dlSummary.estimated_total_mb ? `~${dlSummary.estimated_total_mb} MB` : "—"}</div>
                </div>
              </div>

              {/* Wallet graph */}
              <div style={{ display: "flex", gap: 16, flexWrap: "wrap", marginBottom: 10, padding: "6px 0", borderBottom: `1px solid ${T.ruleLight}` }}>
                <div>
                  <Lbl>Wallet Graph</Lbl>
                  <div style={{ fontSize: 16, fontFamily: T.mono, fontWeight: 700, color: T.ink }}>{fmtNum(wg.total_wallets)}</div>
                </div>
                <div>
                  <Lbl>+24h</Lbl>
                  <div style={{ fontSize: 14, fontFamily: T.mono, color: "#27ae60" }}>+{fmtNum(wg.wallets_added_24h)}</div>
                </div>
                <div>
                  <Lbl>With Scores</Lbl>
                  <div style={{ fontSize: 12, fontFamily: T.mono, color: T.inkMid }}>{fmtNum(wg.wallets_with_risk_scores)}</div>
                </div>
                <div>
                  <Lbl>With Edges</Lbl>
                  <div style={{ fontSize: 12, fontFamily: T.mono, color: T.inkMid }}>{fmtNum(wg.wallets_with_edges)}</div>
                </div>
                <div>
                  <Lbl>Enriched</Lbl>
                  <div style={{ fontSize: 12, fontFamily: T.mono, color: T.inkMid }}>{wg.fully_enriched_pct != null ? `${wg.fully_enriched_pct}%` : "—"}</div>
                </div>
                {wg.days_to_target && (
                  <div>
                    <Lbl>500K Target</Lbl>
                    <div style={{ fontSize: 12, fontFamily: T.mono, color: T.inkMid }}>{wg.days_to_target}d</div>
                  </div>
                )}
              </div>

              {/* Entities */}
              <div style={{ display: "flex", gap: 16, flexWrap: "wrap", marginBottom: 10, padding: "6px 0", borderBottom: `1px solid ${T.ruleLight}` }}>
                <div>
                  <Lbl>Scored Entities</Lbl>
                  <div style={{ fontSize: 16, fontFamily: T.mono, fontWeight: 700, color: T.ink }}>{ec.total_scored_entities || 0}</div>
                </div>
                <div>
                  <Lbl>SII</Lbl>
                  <div style={{ fontSize: 12, fontFamily: T.mono, color: T.inkMid }}>{ec.sii?.scored || 0}/{ec.sii?.total_enabled || 0}</div>
                </div>
                <div>
                  <Lbl>PSI</Lbl>
                  <div style={{ fontSize: 12, fontFamily: T.mono, color: T.inkMid }}>{ec.psi?.scored || 0}</div>
                </div>
                {ec.circle7 && Object.entries(ec.circle7).map(([idx, cnt]) => (
                  <div key={idx}>
                    <Lbl>{idx.toUpperCase()}</Lbl>
                    <div style={{ fontSize: 12, fontFamily: T.mono, color: T.inkMid }}>{cnt}</div>
                  </div>
                ))}
              </div>

              {/* API utilization */}
              {Object.keys(apiUtil).length > 0 && (
                <div style={{ marginBottom: 10, padding: "6px 0", borderBottom: `1px solid ${T.ruleLight}` }}>
                  <div style={{ fontSize: 10, fontWeight: 600, color: T.inkLight, marginBottom: 4 }}>API UTILIZATION</div>
                  <div style={{ display: "grid", gridTemplateColumns: "auto 1fr auto auto", gap: "2px 10px", fontSize: 11, fontFamily: T.mono }}>
                    {Object.entries(apiUtil).map(([prov, info]) => {
                      const pct = info.daily_utilization_pct;
                      const barColor = pct > 90 ? "#e74c3c" : pct > 60 ? "#f39c12" : "#27ae60";
                      return (
                        <div key={prov} style={{ display: "contents" }}>
                          <span style={{ color: T.inkMid }}>{prov}</span>
                          <div style={{ display: "flex", alignItems: "center", gap: 4 }}>
                            {pct != null && (
                              <div style={{ height: 6, borderRadius: 3, background: `${T.paper}22`, flex: 1, maxWidth: 100 }}>
                                <div style={{ height: 6, borderRadius: 3, background: barColor, width: `${Math.min(100, pct)}%` }} />
                              </div>
                            )}
                          </div>
                          <span style={{ textAlign: "right", color: T.inkMid }}>{fmtNum(info.calls_today)}</span>
                          <span style={{ textAlign: "right", color: pct > 90 ? "#e74c3c" : T.inkFaint }}>{pct != null ? `${pct}%` : "free"}</span>
                        </div>
                      );
                    })}
                  </div>
                </div>
              )}

              {/* Provenance */}
              {(() => {
                const provLive = dl.provenance?.live || {};
                const provReg = dl.provenance?.sources || prov;
                const hasProv = provLive.total_proofs > 0 || provReg.total > 0;
                return hasProv ? (
                  <div style={{ display: "flex", gap: 16, flexWrap: "wrap", marginBottom: 10, padding: "6px 0", borderBottom: `1px solid ${T.ruleLight}` }}>
                    <div>
                      <Lbl>Provenance</Lbl>
                      <div style={{ fontSize: 14, fontFamily: T.mono, fontWeight: 600, color: (provLive.coverage_pct || 0) >= 80 ? "#27ae60" : "#f39c12" }}>{provLive.coverage_pct || provReg.coverage_pct || 0}%</div>
                    </div>
                    <div>
                      <Lbl>Sources 24h</Lbl>
                      <div style={{ fontSize: 12, fontFamily: T.mono, color: T.inkMid }}>{provLive.sources_proved_24h || 0}/{provLive.registered_sources || provReg.total || 0}</div>
                    </div>
                    <div>
                      <Lbl>Proofs 24h</Lbl>
                      <div style={{ fontSize: 12, fontFamily: T.mono, color: "#27ae60" }}>+{provLive.proofs_24h || 0}</div>
                    </div>
                    <div>
                      <Lbl>Total Proofs</Lbl>
                      <div style={{ fontSize: 12, fontFamily: T.mono, color: T.inkMid }}>{fmtNum(provLive.total_proofs || 0)}</div>
                    </div>
                  </div>
                ) : null;
              })()}

              {/* Data quality */}
              <div style={{ display: "flex", gap: 16, flexWrap: "wrap", marginBottom: 10, padding: "6px 0", borderBottom: `1px solid ${T.ruleLight}` }}>
                <div>
                  <Lbl>Coherence Flags 24h</Lbl>
                  <div style={{ fontSize: 14, fontFamily: T.mono, color: dq.coherence_flags_24h > 0 ? "#f39c12" : "#27ae60" }}>{dq.coherence_flags_24h || 0}</div>
                </div>
                <div>
                  <Lbl>Unreviewed</Lbl>
                  <div style={{ fontSize: 14, fontFamily: T.mono, color: dq.unreviewed_flags > 0 ? "#e74c3c" : T.inkFaint }}>{dq.unreviewed_flags || 0}</div>
                </div>
                <div>
                  <Lbl>Stale Types</Lbl>
                  <div style={{ fontSize: 14, fontFamily: T.mono, color: dq.stale_count > 0 ? "#e74c3c" : "#27ae60" }}>{dq.stale_count || 0}</div>
                </div>
              </div>

              {/* Collector Health */}
              {dl.collector_health && dl.collector_health.length > 0 && (
                <details style={{ marginBottom: 10, padding: "6px 0", borderBottom: `1px solid ${T.ruleLight}` }}>
                  <summary style={{ fontSize: 10, fontWeight: 700, color: T.inkLight, letterSpacing: 1.2, cursor: "pointer", marginBottom: 4 }}>COLLECTOR HEALTH ({dl.collector_health.length})</summary>
                  <div style={{ display: "grid", gridTemplateColumns: "1fr auto auto auto auto", gap: "2px 10px", fontSize: 10, fontFamily: T.mono, marginTop: 4 }}>
                    <span style={{ fontWeight: 600, color: T.inkLight }}>Collector</span>
                    <span style={{ fontWeight: 600, color: T.inkLight, textAlign: "right" }}>OK</span>
                    <span style={{ fontWeight: 600, color: T.inkLight, textAlign: "right" }}>Err</span>
                    <span style={{ fontWeight: 600, color: T.inkLight, textAlign: "right" }}>Latency</span>
                    <span style={{ fontWeight: 600, color: T.inkLight, textAlign: "right" }}>Rate</span>
                    {dl.collector_health.map((c) => (
                      <div key={c.name} style={{ display: "contents" }}>
                        <span style={{ color: T.inkMid }}>{c.name}</span>
                        <span style={{ textAlign: "right", color: "#27ae60" }}>{c.ok}</span>
                        <span style={{ textAlign: "right", color: c.error > 0 ? "#e74c3c" : T.inkFaint }}>{c.error + c.timeout}</span>
                        <span style={{ textAlign: "right", color: T.inkMid }}>{c.avg_latency_ms}ms</span>
                        <span style={{ textAlign: "right", color: c.success_rate >= 90 ? "#27ae60" : c.success_rate >= 50 ? "#f39c12" : "#e74c3c" }}>{c.success_rate}%</span>
                      </div>
                    ))}
                  </div>
                </details>
              )}

              {/* Active Alerts */}
              {dl.active_alerts && (
                <div style={{ display: "flex", gap: 16, flexWrap: "wrap", marginBottom: 10, padding: "6px 0", borderBottom: `1px solid ${T.ruleLight}` }}>
                  <div style={{ width: "100%", fontSize: 10, fontWeight: 700, color: T.inkLight, letterSpacing: 1.2, marginBottom: 2 }}>ACTIVE ALERTS</div>
                  <div>
                    <Lbl>Oracle Stress</Lbl>
                    <div style={{ fontSize: 14, fontFamily: T.mono, color: dl.active_alerts.oracle_stress_open > 0 ? "#e74c3c" : "#27ae60" }}>{dl.active_alerts.oracle_stress_open || 0}</div>
                  </div>
                  <div>
                    <Lbl>Upgrades 7d</Lbl>
                    <div style={{ fontSize: 14, fontFamily: T.mono, color: dl.active_alerts.contract_upgrades_7d > 0 ? "#f39c12" : T.inkFaint }}>{dl.active_alerts.contract_upgrades_7d || 0}</div>
                  </div>
                  <div>
                    <Lbl>Param Changes 7d</Lbl>
                    <div style={{ fontSize: 14, fontFamily: T.mono, color: dl.active_alerts.parameter_changes_7d > 0 ? "#f39c12" : T.inkFaint }}>{dl.active_alerts.parameter_changes_7d || 0}</div>
                  </div>
                  <div>
                    <Lbl>Signals 24h</Lbl>
                    <div style={{ fontSize: 14, fontFamily: T.mono, color: T.inkMid }}>{dl.active_alerts.discovery_signals_24h || 0}</div>
                  </div>
                </div>
              )}

              {/* Keeper Status */}
              {dl.keeper_status && dl.keeper_status.total_cycles > 0 && (
                <div style={{ display: "flex", gap: 16, flexWrap: "wrap", marginBottom: 10, padding: "6px 0", borderBottom: `1px solid ${T.ruleLight}` }}>
                  <div style={{ width: "100%", fontSize: 10, fontWeight: 700, color: T.inkLight, letterSpacing: 1.2, marginBottom: 2 }}>KEEPER STATUS</div>
                  <div>
                    <Lbl>Cycles 24h</Lbl>
                    <div style={{ fontSize: 14, fontFamily: T.mono, color: T.ink }}>{dl.keeper_status.cycles_24h || 0}</div>
                  </div>
                  <div>
                    <Lbl>Total Cycles</Lbl>
                    <div style={{ fontSize: 12, fontFamily: T.mono, color: T.inkMid }}>{dl.keeper_status.total_cycles}</div>
                  </div>
                  <div>
                    <Lbl>SII Base</Lbl>
                    <div style={{ fontSize: 12, fontFamily: T.mono, color: T.inkMid }}>{dl.keeper_status.sii_updates_base || 0}</div>
                  </div>
                  <div>
                    <Lbl>SII Arb</Lbl>
                    <div style={{ fontSize: 12, fontFamily: T.mono, color: T.inkMid }}>{dl.keeper_status.sii_updates_arb || 0}</div>
                  </div>
                  <div>
                    <Lbl>State Root</Lbl>
                    <div style={{ fontSize: 12, fontFamily: T.mono, color: dl.keeper_status.state_root_published ? "#27ae60" : T.inkFaint }}>{dl.keeper_status.state_root_published ? "Yes" : "No"}</div>
                  </div>
                </div>
              )}

              {/* Scoring Performance */}
              {dl.scoring_performance && dl.scoring_performance.daily_trend && dl.scoring_performance.daily_trend.length > 0 && (
                <details style={{ marginBottom: 10, padding: "6px 0", borderBottom: `1px solid ${T.ruleLight}` }}>
                  <summary style={{ fontSize: 10, fontWeight: 700, color: T.inkLight, letterSpacing: 1.2, cursor: "pointer", marginBottom: 4 }}>SCORING PERFORMANCE</summary>
                  <div style={{ display: "flex", gap: 16, flexWrap: "wrap", marginBottom: 6 }}>
                    <div>
                      <Lbl>Avg Components/Coin</Lbl>
                      <div style={{ fontSize: 14, fontFamily: T.mono, color: T.ink }}>{dl.scoring_performance.avg_components_per_coin || 0}</div>
                    </div>
                  </div>
                  <div style={{ display: "grid", gridTemplateColumns: "auto auto auto auto", gap: "2px 10px", fontSize: 10, fontFamily: T.mono }}>
                    <span style={{ fontWeight: 600, color: T.inkLight }}>Day</span>
                    <span style={{ fontWeight: 600, color: T.inkLight, textAlign: "right" }}>Latency</span>
                    <span style={{ fontWeight: 600, color: T.inkLight, textAlign: "right" }}>Components</span>
                    <span style={{ fontWeight: 600, color: T.inkLight, textAlign: "right" }}>Coins</span>
                    {dl.scoring_performance.daily_trend.map((d) => (
                      <div key={d.day} style={{ display: "contents" }}>
                        <span style={{ color: T.inkMid }}>{d.day}</span>
                        <span style={{ textAlign: "right" }}>{d.avg_latency_ms}ms</span>
                        <span style={{ textAlign: "right" }}>{fmtNum(d.total_components)}</span>
                        <span style={{ textAlign: "right" }}>{d.coins_scored}</span>
                      </div>
                    ))}
                  </div>
                </details>
              )}

              {/* CDA Freshness */}
              {dl.cda_freshness && dl.cda_freshness.length > 0 && (
                <details style={{ marginBottom: 10, padding: "6px 0", borderBottom: `1px solid ${T.ruleLight}` }}>
                  <summary style={{ fontSize: 10, fontWeight: 700, color: T.inkLight, letterSpacing: 1.2, cursor: "pointer", marginBottom: 4 }}>
                    CDA FRESHNESS ({dl.cda_freshness.filter(c => c.stale).length} stale / {dl.cda_freshness.length})
                  </summary>
                  <div style={{ display: "grid", gridTemplateColumns: "auto 1fr auto auto", gap: "2px 10px", fontSize: 10, fontFamily: T.mono, marginTop: 4 }}>
                    <span style={{ fontWeight: 600, color: T.inkLight }}>Asset</span>
                    <span style={{ fontWeight: 600, color: T.inkLight }}>Issuer</span>
                    <span style={{ fontWeight: 600, color: T.inkLight, textAlign: "right" }}>Days</span>
                    <span style={{ fontWeight: 600, color: T.inkLight, textAlign: "right" }}>Status</span>
                    {dl.cda_freshness.map((c) => (
                      <div key={c.asset} style={{ display: "contents" }}>
                        <span style={{ color: T.inkMid }}>{c.asset}</span>
                        <span style={{ color: T.inkFaint, overflow: "hidden", textOverflow: "ellipsis" }}>{c.issuer || "—"}</span>
                        <span style={{ textAlign: "right", color: T.inkMid }}>{c.days_since != null ? c.days_since : "—"}</span>
                        <span style={{ textAlign: "right", color: c.stale ? "#e74c3c" : "#27ae60", fontSize: 9 }}>{c.stale ? "STALE" : "OK"}</span>
                      </div>
                    ))}
                  </div>
                </details>
              )}

              {/* Component Coverage */}
              {dl.component_coverage && dl.component_coverage.sii && (
                <div style={{ display: "flex", gap: 16, flexWrap: "wrap", marginBottom: 10, padding: "6px 0", borderBottom: `1px solid ${T.ruleLight}` }}>
                  <div style={{ width: "100%", fontSize: 10, fontWeight: 700, color: T.inkLight, letterSpacing: 1.2, marginBottom: 2 }}>COMPONENT COVERAGE</div>
                  <div>
                    <Lbl>SII Avg Components</Lbl>
                    <div style={{ fontSize: 14, fontFamily: T.mono, color: T.ink }}>{dl.component_coverage.sii.avg_components || 0}</div>
                  </div>
                  <div>
                    <Lbl>SII Populated</Lbl>
                    <div style={{ fontSize: 12, fontFamily: T.mono, color: "#27ae60" }}>{dl.component_coverage.sii.avg_populated || 0}</div>
                  </div>
                  <div>
                    <Lbl>SII Empty</Lbl>
                    <div style={{ fontSize: 12, fontFamily: T.mono, color: dl.component_coverage.sii.avg_empty > 0 ? "#f39c12" : T.inkFaint }}>{dl.component_coverage.sii.avg_empty || 0}</div>
                  </div>
                  {dl.component_coverage.psi && (
                    <div>
                      <Lbl>PSI Components</Lbl>
                      <div style={{ fontSize: 12, fontFamily: T.mono, color: T.inkMid }}>{dl.component_coverage.psi.unique_components || 0}</div>
                    </div>
                  )}
                  {dl.component_coverage.rpi && (
                    <div>
                      <Lbl>RPI Components</Lbl>
                      <div style={{ fontSize: 12, fontFamily: T.mono, color: T.inkMid }}>{dl.component_coverage.rpi.unique_components || 0}</div>
                    </div>
                  )}
                </div>
              )}

              {/* CQI Contagion */}
              {dl.cqi_contagion && dl.cqi_contagion.protocols_total > 0 && (
                <div style={{ display: "flex", gap: 16, flexWrap: "wrap", marginBottom: 10, padding: "6px 0", borderBottom: `1px solid ${T.ruleLight}` }}>
                  <div style={{ width: "100%", fontSize: 10, fontWeight: 700, color: T.inkLight, letterSpacing: 1.2, marginBottom: 2 }}>CQI CONTAGION</div>
                  <div>
                    <Lbl>Pool Coverage</Lbl>
                    <div style={{ fontSize: 14, fontFamily: T.mono, color: T.ink }}>{dl.cqi_contagion.coverage_pct}%</div>
                  </div>
                  <div>
                    <Lbl>With Pool Data</Lbl>
                    <div style={{ fontSize: 12, fontFamily: T.mono, color: T.inkMid }}>{dl.cqi_contagion.protocols_with_pool_data}/{dl.cqi_contagion.protocols_total}</div>
                  </div>
                  <div>
                    <Lbl>Pool Wallets</Lbl>
                    <div style={{ fontSize: 12, fontFamily: T.mono, color: T.inkMid }}>{fmtNum(dl.cqi_contagion.pool_wallets_discovered)}</div>
                  </div>
                </div>
              )}

              {/* x402 Revenue */}
              {dl.x402_revenue && dl.x402_revenue.total_payments > 0 && (
                <details style={{ marginBottom: 10, padding: "6px 0", borderBottom: `1px solid ${T.ruleLight}` }}>
                  <summary style={{ fontSize: 10, fontWeight: 700, color: T.inkLight, letterSpacing: 1.2, cursor: "pointer", marginBottom: 4 }}>x402 REVENUE</summary>
                  <div style={{ display: "flex", gap: 16, flexWrap: "wrap", marginBottom: 6 }}>
                    <div>
                      <Lbl>Total Revenue</Lbl>
                      <div style={{ fontSize: 14, fontFamily: T.mono, fontWeight: 700, color: T.ink }}>${dl.x402_revenue.total_revenue_usd?.toFixed(4) || "0"}</div>
                    </div>
                    <div>
                      <Lbl>Revenue 7d</Lbl>
                      <div style={{ fontSize: 12, fontFamily: T.mono, color: "#27ae60" }}>${dl.x402_revenue.revenue_7d_usd?.toFixed(4) || "0"}</div>
                    </div>
                    <div>
                      <Lbl>Total Payments</Lbl>
                      <div style={{ fontSize: 12, fontFamily: T.mono, color: T.inkMid }}>{dl.x402_revenue.total_payments}</div>
                    </div>
                    <div>
                      <Lbl>Unique Payers</Lbl>
                      <div style={{ fontSize: 12, fontFamily: T.mono, color: T.inkMid }}>{dl.x402_revenue.unique_payers || 0}</div>
                    </div>
                  </div>
                  {dl.x402_revenue.top_endpoints && dl.x402_revenue.top_endpoints.length > 0 && (
                    <div style={{ display: "grid", gridTemplateColumns: "1fr auto auto", gap: "2px 10px", fontSize: 10, fontFamily: T.mono }}>
                      <span style={{ fontWeight: 600, color: T.inkLight }}>Endpoint</span>
                      <span style={{ fontWeight: 600, color: T.inkLight, textAlign: "right" }}>Calls</span>
                      <span style={{ fontWeight: 600, color: T.inkLight, textAlign: "right" }}>Revenue</span>
                      {dl.x402_revenue.top_endpoints.map((ep) => (
                        <div key={ep.endpoint} style={{ display: "contents" }}>
                          <span style={{ color: T.inkMid, overflow: "hidden", textOverflow: "ellipsis" }}>{ep.endpoint}</span>
                          <span style={{ textAlign: "right" }}>{ep.calls}</span>
                          <span style={{ textAlign: "right", color: "#27ae60" }}>${ep.revenue_usd}</span>
                        </div>
                      ))}
                    </div>
                  )}
                </details>
              )}

              {/* Security Scanning */}
              {dl.security_scanning && dl.security_scanning.contracts_monitored > 0 && (
                <details style={{ marginBottom: 10, padding: "6px 0", borderBottom: `1px solid ${T.ruleLight}` }}>
                  <summary style={{ fontSize: 10, fontWeight: 700, color: T.inkLight, letterSpacing: 1.2, cursor: "pointer", marginBottom: 4 }}>
                    SECURITY SCANNING ({dl.security_scanning.contracts_monitored} contracts)
                  </summary>
                  <div style={{ display: "flex", gap: 16, flexWrap: "wrap", marginBottom: 6 }}>
                    <div>
                      <Lbl>Monitored</Lbl>
                      <div style={{ fontSize: 14, fontFamily: T.mono, color: T.ink }}>{dl.security_scanning.contracts_monitored}</div>
                    </div>
                    <div>
                      <Lbl>Scan Coverage</Lbl>
                      <div style={{ fontSize: 14, fontFamily: T.mono, color: (dl.security_scanning.scan_coverage?.coverage_pct || 0) >= 80 ? "#27ae60" : "#f39c12" }}>{dl.security_scanning.scan_coverage?.coverage_pct || 0}%</div>
                    </div>
                    <div>
                      <Lbl>Upgrades 7d</Lbl>
                      <div style={{ fontSize: 12, fontFamily: T.mono, color: dl.security_scanning.upgrade_alerts_7d > 0 ? "#f39c12" : T.inkFaint }}>{dl.security_scanning.upgrade_alerts_7d || 0}</div>
                    </div>
                    <div>
                      <Lbl>Total Diffs</Lbl>
                      <div style={{ fontSize: 12, fontFamily: T.mono, color: T.inkMid }}>{dl.security_scanning.total_diffs_detected || 0}</div>
                    </div>
                  </div>
                  {dl.security_scanning.admin_key_risk && (
                    <div style={{ display: "flex", gap: 12, flexWrap: "wrap", marginBottom: 6 }}>
                      <div style={{ fontSize: 10, color: T.inkFaint }}>
                        Admin Keys: <span style={{ color: dl.security_scanning.admin_key_risk.contracts_with_admin_keys > 0 ? "#f39c12" : "#27ae60" }}>{dl.security_scanning.admin_key_risk.contracts_with_admin_keys}</span>
                      </div>
                      <div style={{ fontSize: 10, color: T.inkFaint }}>
                        Timelock {"<"}24h: <span style={{ color: dl.security_scanning.admin_key_risk.timelock_under_24h > 0 ? "#e74c3c" : "#27ae60" }}>{dl.security_scanning.admin_key_risk.timelock_under_24h}</span>
                      </div>
                      <div style={{ fontSize: 10, color: T.inkFaint }}>
                        No Multisig: <span style={{ color: dl.security_scanning.admin_key_risk.no_multisig > 0 ? "#e74c3c" : "#27ae60" }}>{dl.security_scanning.admin_key_risk.no_multisig}</span>
                      </div>
                      <div style={{ fontSize: 10, color: T.inkFaint }}>
                        Pause w/o Lock: <span style={{ color: dl.security_scanning.admin_key_risk.pausable_without_timelock > 0 ? "#e74c3c" : "#27ae60" }}>{dl.security_scanning.admin_key_risk.pausable_without_timelock}</span>
                      </div>
                    </div>
                  )}
                  {dl.security_scanning.scan_coverage?.unmonitored_entities?.length > 0 && (
                    <div style={{ fontSize: 10, color: T.inkFaint, marginTop: 4 }}>
                      Unmonitored: <span style={{ color: "#e74c3c" }}>{dl.security_scanning.scan_coverage.unmonitored_entities.join(", ")}</span>
                    </div>
                  )}
                </details>
              )}

              {/* Per-category table breakdown */}
              <div style={{ fontSize: 10, fontWeight: 600, color: T.inkLight, marginBottom: 4 }}>BY CATEGORY</div>
              <div style={{ display: "grid", gridTemplateColumns: "auto auto auto auto", gap: "2px 12px", fontSize: 11, fontFamily: T.mono, marginBottom: 8 }}>
                <span style={{ fontWeight: 600, color: T.inkLight, fontSize: 10 }}>Category</span>
                <span style={{ fontWeight: 600, color: T.inkLight, fontSize: 10, textAlign: "right" }}>Tables</span>
                <span style={{ fontWeight: 600, color: T.inkLight, fontSize: 10, textAlign: "right" }}>Rows</span>
                <span style={{ fontWeight: 600, color: T.inkLight, fontSize: 10, textAlign: "right" }}>+24h</span>
                {Object.entries(cats).sort((a,b) => b[1].rows - a[1].rows).map(([cat, info]) => (
                  <div key={cat} style={{ display: "contents" }}>
                    <span style={{ color: T.inkMid }}>{cat.replace(/_/g, " ")}</span>
                    <span style={{ textAlign: "right" }}>{info.tables}</span>
                    <span style={{ textAlign: "right" }}>{fmtNum(info.rows)}</span>
                    <span style={{ textAlign: "right", color: info.rows_24h > 0 ? "#27ae60" : T.inkFaint }}>{info.rows_24h > 0 ? `+${fmtNum(info.rows_24h)}` : "0"}</span>
                  </div>
                ))}
              </div>

              {/* Per-table details (collapsible) */}
              <details style={{ marginTop: 4 }}>
                <summary style={{ fontSize: 10, color: T.inkFaint, cursor: "pointer" }}>All {Object.keys(dl.tables || {}).length} tables</summary>
                <div style={{ display: "grid", gridTemplateColumns: "1fr auto auto auto", gap: "1px 10px", fontSize: 10, fontFamily: T.mono, marginTop: 4 }}>
                  <span style={{ fontWeight: 600, color: T.inkLight }}>Table</span>
                  <span style={{ fontWeight: 600, color: T.inkLight, textAlign: "right" }}>Rows</span>
                  <span style={{ fontWeight: 600, color: T.inkLight, textAlign: "right" }}>+24h</span>
                  <span style={{ fontWeight: 600, color: T.inkLight, textAlign: "right" }}>+7d</span>
                  {Object.entries(dl.tables || {}).sort((a,b) => b[1].row_count - a[1].row_count).map(([tbl, info]) => (
                    <div key={tbl} style={{ display: "contents" }}>
                      <span style={{ color: T.inkMid, overflow: "hidden", textOverflow: "ellipsis" }}>{tbl}</span>
                      <span style={{ textAlign: "right" }}>{fmtNum(info.row_count)}</span>
                      <span style={{ textAlign: "right", color: info.rows_24h > 0 ? "#27ae60" : T.inkFaint }}>{info.rows_24h > 0 ? `+${fmtNum(info.rows_24h)}` : "0"}</span>
                      <span style={{ textAlign: "right", color: info.rows_7d > 0 ? "#27ae60" : T.inkFaint }}>{info.rows_7d > 0 ? `+${fmtNum(info.rows_7d)}` : "0"}</span>
                    </div>
                  ))}
                </div>
              </details>
            </div>
          );
        })()}
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

// ─── Metrics Expansion Panels ────────────────────────────────────────

function MetricsExpansionPanels() {
  const [data, setData] = useState(null);

  useEffect(() => {
    opsFetch("/api/ops/seed-metrics").then(setData).catch(() => {});
  }, []);

  if (!data || data.error) return null;

  const statCard = (label, value, sub) => (
    <div style={{ flex: 1, padding: "10px 12px", border: `1px solid ${T.ruleMid}`, background: T.paperWarm, minWidth: 140 }}>
      <div style={{ fontFamily: T.mono, fontSize: 9, textTransform: "uppercase", letterSpacing: 1.5, color: T.inkLight, marginBottom: 4 }}>{label}</div>
      <div style={{ fontFamily: T.mono, fontSize: 22, fontWeight: 700, color: T.ink }}>{value ?? "—"}</div>
      {sub && <div style={{ fontFamily: T.mono, fontSize: 10, color: T.inkFaint, marginTop: 2 }}>{sub}</div>}
    </div>
  );

  const lensCard = (lens, lensData) => {
    const age = lensData?.last_generated
      ? `${Math.round((Date.now() - new Date(lensData.last_generated).getTime()) / 3600000)}h ago`
      : "—";
    return (
      <div key={lens} style={{ flex: 1, padding: "10px 12px", border: `1px solid ${T.ruleMid}`, background: T.paperWarm, minWidth: 160 }}>
        <div style={{ fontFamily: T.mono, fontSize: 11, fontWeight: 700, color: T.ink, marginBottom: 4 }}>{lens}</div>
        <div style={{ fontFamily: T.mono, fontSize: 10, color: T.inkMid }}>Reports: {lensData?.total ?? 0}</div>
        <div style={{ fontFamily: T.mono, fontSize: 10, color: T.inkMid }}>Entities: {lensData?.unique_entities ?? 0}</div>
        <div style={{ fontFamily: T.mono, fontSize: 10, color: T.inkFaint }}>Last: {age}</div>
      </div>
    );
  };

  const cm = data.compliance_metrics || {};
  const x4 = data.x402_metrics || {};
  const cov = data.coverage || {};
  const att = data.attestation_metrics || {};
  const rm = data.report_metrics || {};
  const lenses = cm.by_lens || [];

  return (
    <>
      <Section title="COMPLIANCE LENSES">
        <div style={{ display: "flex", gap: 8, flexWrap: "wrap", marginBottom: 12 }}>
          {["SCO60", "MICA67", "GENIUS"].map((lid) => {
            const ld = lenses.find((l) => l.lens === lid) || {};
            return lensCard(lid, ld);
          })}
        </div>
        <div style={{ fontFamily: T.mono, fontSize: 10, color: T.inkFaint }}>
          Entities with all 3 lenses: <strong style={{ color: T.ink }}>{cm.entities_with_all_3_lenses ?? 0}</strong>
          {" · "}Total compliance reports: <strong style={{ color: T.ink }}>{cm.total_compliance_reports ?? 0}</strong>
        </div>
      </Section>

      <Section title="x402 PAYMENTS">
        <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
          {statCard("Total Payments", x4.total_payments ?? 0)}
          {statCard("Revenue (30d)", `$${(x4.revenue_30d_usd ?? 0).toFixed(4)}`)}
          {statCard("Unique Payers", x4.unique_payers ?? 0)}
          {statCard("Total Revenue", `$${(x4.total_revenue_usd ?? 0).toFixed(4)}`)}
        </div>
      </Section>

      <Section title="COVERAGE">
        <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
          {statCard("SII Scored", cov.sii_scored ?? 0, "stablecoins")}
          {statCard("PSI Scored", cov.psi_scored ?? 0, "protocols")}
          {statCard("CDA Issuers", cov.cda_issuers ?? 0, "assets")}
          {statCard("State Domains", att.domains_active ?? 0, att.latest_state_root_age_hours != null ? `root: ${att.latest_state_root_age_hours}h ago` : "")}
        </div>
      </Section>

      <Section title="REPORT ACTIVITY">
        <div style={{ display: "flex", gap: 8, flexWrap: "wrap", marginBottom: 12 }}>
          {statCard("Total Reports", rm.total_reports ?? 0)}
          {statCard("Reports Today", rm.reports_today ?? 0)}
          {statCard("State Attestations", att.total_state_attestations ?? 0)}
        </div>

        {(rm.templates_used || []).length > 0 && (
          <div style={{ marginBottom: 12 }}>
            <div style={{ fontFamily: T.mono, fontSize: 9, textTransform: "uppercase", letterSpacing: 1.5, color: T.inkLight, marginBottom: 6 }}>Templates Breakdown</div>
            <table style={{ width: "100%", fontSize: 11, fontFamily: T.mono, borderCollapse: "collapse" }}>
              <thead>
                <tr style={{ borderBottom: `1px solid ${T.ruleMid}`, fontSize: 10, color: T.inkLight, textAlign: "left" }}>
                  <th style={{ padding: "4px 0" }}>Template</th><th style={{ textAlign: "right" }}>Count</th>
                </tr>
              </thead>
              <tbody>
                {rm.templates_used.map((t, i) => (
                  <tr key={i} style={{ borderBottom: `1px solid ${T.ruleLight}` }}>
                    <td style={{ padding: "3px 0" }}>{t.template}</td>
                    <td style={{ textAlign: "right", fontWeight: 600 }}>{t.c}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}

        {(rm.lenses_used || []).length > 0 && (
          <div>
            <div style={{ fontFamily: T.mono, fontSize: 9, textTransform: "uppercase", letterSpacing: 1.5, color: T.inkLight, marginBottom: 6 }}>Lenses Breakdown</div>
            <table style={{ width: "100%", fontSize: 11, fontFamily: T.mono, borderCollapse: "collapse" }}>
              <thead>
                <tr style={{ borderBottom: `1px solid ${T.ruleMid}`, fontSize: 10, color: T.inkLight, textAlign: "left" }}>
                  <th style={{ padding: "4px 0" }}>Lens</th><th style={{ textAlign: "right" }}>Count</th>
                </tr>
              </thead>
              <tbody>
                {rm.lenses_used.map((l, i) => (
                  <tr key={i} style={{ borderBottom: `1px solid ${T.ruleLight}` }}>
                    <td style={{ padding: "3px 0" }}>{l.lens}</td>
                    <td style={{ textAlign: "right", fontWeight: 600 }}>{l.c}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </Section>
    </>
  );
}

// ─── Index Score Table ──────────────────────────────────────────────

const INDEX_CONFIGS = [
  { id: "sii", label: "SII", full: "Stablecoin Integrity Index", endpoint: "/api/scores", color: "#2563eb" },
  { id: "psi", label: "PSI", full: "Protocol Safety Index", endpoint: "/api/psi/scores", color: "#7c3aed" },
  { id: "rpi", label: "RPI", full: "Revenue Protocol Index", endpoint: "/api/rpi/scores", color: "#0891b2" },
  { id: "lsti", label: "LSTI", full: "Liquid Staking Token Index", endpoint: "/api/lsti/scores", color: "#059669" },
  { id: "bri", label: "BRI", full: "Bridge Risk Index", endpoint: "/api/bri/scores", color: "#d97706" },
  { id: "dohi", label: "DOHI", full: "DAO Health Index", endpoint: "/api/dohi/scores", color: "#dc2626" },
  { id: "vsri", label: "VSRI", full: "Vault Strategy Risk Index", endpoint: "/api/vsri/scores", color: "#4f46e5" },
  { id: "cxri", label: "CXRI", full: "CEX Risk Index", endpoint: "/api/cxri/scores", color: "#be185d" },
  { id: "tti", label: "TTI", full: "Tokenized Treasury Index", endpoint: "/api/tti/scores", color: "#65a30d" },
];

function normalizeScores(indexId, data) {
  if (indexId === "sii") {
    return (data.stablecoins || []).map((s) => ({
      name: s.name || s.symbol,
      slug: s.symbol,
      score: s.score,
      confidence_tag: s.confidence_tag,
      components_populated: s.components_populated,
      components_total: s.components_total,
      computed_at: s.computed_at,
    }));
  }
  if (indexId === "psi") {
    return (data.protocols || []).map((p) => ({
      name: p.protocol_name,
      slug: p.protocol_slug,
      score: p.score,
      confidence_tag: p.confidence_tag,
      components_populated: p.components_populated,
      components_total: p.components_total,
      computed_at: p.computed_at,
    }));
  }
  if (indexId === "rpi") {
    return (data.protocols || []).map((p) => ({
      name: p.protocol_name,
      slug: p.protocol_slug,
      score: p.score,
      confidence_tag: p.confidence_tag,
      components_populated: p.components_populated,
      components_total: p.components_total,
      computed_at: p.computed_at,
    }));
  }
  // Circle 7 indices
  return (data.scores || []).map((s) => ({
    name: s.name,
    slug: s.entity,
    score: s.score,
    confidence_tag: s.confidence_tag,
    components_populated: s.components_populated,
    components_total: s.components_total,
    computed_at: s.scored_date,
  }));
}

function gradeFromScore(score) {
  if (score == null) return "—";
  if (score >= 95) return "A+";
  if (score >= 90) return "A";
  if (score >= 85) return "A-";
  if (score >= 80) return "B+";
  if (score >= 75) return "B";
  if (score >= 70) return "B-";
  if (score >= 65) return "C+";
  if (score >= 60) return "C";
  if (score >= 55) return "C-";
  if (score >= 50) return "D+";
  if (score >= 45) return "D";
  if (score >= 40) return "D-";
  return "F";
}

function gradeColor(score) {
  if (score == null) return T.inkFaint;
  if (score >= 80) return "#22c55e";
  if (score >= 60) return "#eab308";
  if (score >= 40) return "#f97316";
  return "#ef4444";
}

function IndexTable({ config, data, loading, error }) {
  const sorted = data ? [...data].sort((a, b) => (b.score || 0) - (a.score || 0)) : [];
  const latestScored = sorted.length > 0
    ? sorted.reduce((latest, s) => {
        if (!s.computed_at) return latest;
        return !latest || s.computed_at > latest ? s.computed_at : latest;
      }, null)
    : null;

  return (
    <div style={{ marginBottom: 24 }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 8 }}>
        <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
          <span style={{
            fontFamily: T.mono, fontSize: 11, fontWeight: 700, color: "#fff",
            background: config.color, padding: "2px 8px", borderRadius: 2, letterSpacing: 0.5,
          }}>{config.label}</span>
          <span style={{ fontFamily: T.sans, fontSize: 13, color: T.inkMid }}>{config.full}</span>
        </div>
        <div style={{ fontFamily: T.mono, fontSize: 10, color: T.inkFaint }}>
          {sorted.length} entities
          {latestScored && ` · scored ${new Date(latestScored).toLocaleDateString()}`}
        </div>
      </div>

      {loading && <div style={{ fontFamily: T.mono, fontSize: 11, color: T.inkFaint, padding: "8px 0" }}>Loading...</div>}
      {error && <div style={{ fontFamily: T.mono, fontSize: 11, color: T.accent, padding: "8px 0" }}>Failed to load: {error}</div>}

      {!loading && !error && sorted.length > 0 && (
        <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 11, fontFamily: T.mono }}>
          <thead>
            <tr style={{ borderBottom: `1px solid ${T.ruleMid}` }}>
              <th style={{ textAlign: "left", padding: "4px 6px", fontWeight: 600, color: T.inkLight, fontSize: 10, width: 30 }}>#</th>
              <th style={{ textAlign: "left", padding: "4px 6px", fontWeight: 600, color: T.inkLight, fontSize: 10 }}>ENTITY</th>
              <th style={{ textAlign: "right", padding: "4px 6px", fontWeight: 600, color: T.inkLight, fontSize: 10, width: 60 }}>SCORE</th>
              <th style={{ textAlign: "center", padding: "4px 6px", fontWeight: 600, color: T.inkLight, fontSize: 10, width: 50 }}>GRADE</th>
              <th style={{ textAlign: "center", padding: "4px 6px", fontWeight: 600, color: T.inkLight, fontSize: 10, width: 80 }}>CONFIDENCE</th>
              <th style={{ textAlign: "right", padding: "4px 6px", fontWeight: 600, color: T.inkLight, fontSize: 10, width: 80 }}>COVERAGE</th>
              <th style={{ textAlign: "right", padding: "4px 6px", fontWeight: 600, color: T.inkLight, fontSize: 10, width: 90 }}>SCORED</th>
            </tr>
          </thead>
          <tbody>
            {sorted.map((s, i) => (
              <tr key={s.slug} style={{ borderBottom: `1px solid ${T.ruleLight}` }}>
                <td style={{ padding: "4px 6px", color: T.inkFaint }}>{i + 1}</td>
                <td style={{ padding: "4px 6px", color: T.ink }}>{s.name}</td>
                <td style={{ padding: "4px 6px", textAlign: "right", fontWeight: 600, color: gradeColor(s.score) }}>
                  {s.score != null ? s.score.toFixed(1) : "—"}
                </td>
                <td style={{ padding: "4px 6px", textAlign: "center", fontWeight: 600, color: gradeColor(s.score) }}>
                  {gradeFromScore(s.score)}
                </td>
                <td style={{ padding: "4px 6px", textAlign: "center" }}>
                  {s.confidence_tag ? (
                    <span style={{
                      fontSize: 9, padding: "1px 5px", borderRadius: 2,
                      background: s.confidence_tag === "high" ? "#22c55e18" : s.confidence_tag === "standard" ? "#eab30818" : "#ef444418",
                      color: s.confidence_tag === "high" ? "#22c55e" : s.confidence_tag === "standard" ? "#eab308" : "#ef4444",
                      border: `1px solid ${s.confidence_tag === "high" ? "#22c55e33" : s.confidence_tag === "standard" ? "#eab30833" : "#ef444433"}`,
                    }}>{s.confidence_tag}</span>
                  ) : <span style={{ color: T.inkFaint }}>—</span>}
                </td>
                <td style={{ padding: "4px 6px", textAlign: "right", color: T.inkMid }}>
                  {s.components_populated != null && s.components_total != null
                    ? `${s.components_populated}/${s.components_total}`
                    : "—"}
                </td>
                <td style={{ padding: "4px 6px", textAlign: "right", color: T.inkFaint, fontSize: 10 }}>
                  {s.computed_at ? new Date(s.computed_at).toLocaleDateString() : "—"}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}

      {!loading && !error && sorted.length === 0 && (
        <div style={{ fontFamily: T.mono, fontSize: 11, color: T.inkFaint, padding: "8px 0" }}>No scores available.</div>
      )}
    </div>
  );
}

function IndicesPanel() {
  const [indices, setIndices] = useState({});
  const [loading, setLoading] = useState({});
  const [errors, setErrors] = useState({});
  const [lastRefresh, setLastRefresh] = useState(null);

  const loadAll = useCallback(async () => {
    const newLoading = {};
    INDEX_CONFIGS.forEach((c) => { newLoading[c.id] = true; });
    setLoading(newLoading);
    setErrors({});

    const results = {};
    const errs = {};

    await Promise.all(INDEX_CONFIGS.map(async (config) => {
      try {
        const data = await opsFetch(config.endpoint);
        results[config.id] = normalizeScores(config.id, data);
      } catch (e) {
        errs[config.id] = e.message;
        results[config.id] = [];
      }
    }));

    setIndices(results);
    setErrors(errs);
    setLoading({});
    setLastRefresh(new Date());
  }, []);

  useEffect(() => { loadAll(); }, [loadAll]);

  const totalEntities = Object.values(indices).reduce((sum, arr) => sum + (arr?.length || 0), 0);
  const indicesLoaded = Object.keys(indices).length;
  const anyLoading = Object.values(loading).some(Boolean);

  return (
    <div style={{ animation: "fadeIn 0.3s ease" }}>
      <TabHeader
        title={<><span style={{ fontWeight: 700 }}>Index</span> Overview</>}
        formId="FORM IDX-001"
        stats={[
          `${indicesLoaded > 0 ? INDEX_CONFIGS.length : "—"} indices`,
          `${totalEntities} entities`,
          "scored hourly",
          lastRefresh ? `refreshed ${lastRefresh.toLocaleTimeString()}` : "loading...",
        ]}
        accent="#2563eb"
        mobile={false}
      />

      <div style={{ marginTop: 16, display: "flex", justifyContent: "flex-end" }}>
        <button onClick={loadAll} disabled={anyLoading} style={btn({ opacity: anyLoading ? 0.5 : 1 })}>
          {anyLoading ? "Loading..." : "Refresh All"}
        </button>
      </div>

      {INDEX_CONFIGS.map((config) => (
        <IndexTable
          key={config.id}
          config={config}
          data={indices[config.id] || []}
          loading={!!loading[config.id]}
          error={errors[config.id]}
        />
      ))}
    </div>
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
          <span>Score: <strong>{data.overall_score ? Number(data.overall_score).toFixed(1) : "—"}</strong></span>
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
                  <th style={{ padding: "3px 0" }}>Token</th><th style={{ textAlign: "right" }}>USD Value</th><th>SII Score</th>
                </tr></thead>
                <tbody>
                  {data.stablecoin_exposure.treasury.map((t, i) => (
                    <tr key={i} style={{ borderBottom: `1px solid ${T.ruleLight}` }}>
                      <td style={{ padding: "3px 0" }}>{t.token_symbol}</td>
                      <td style={{ textAlign: "right" }}>${(t.usd_value || 0).toLocaleString()}</td>
                      <td>{t.sii_score ?? "—"}</td>
                      <td>{t.is_scored ? "" : <span style={{ color: "#ef4444" }}>unscored</span>}</td>
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
                <th style={{ padding: "3px 0" }}>Asset</th><th>SII</th><th>PSI</th><th>CQI</th><th>Confidence</th>
              </tr></thead>
              <tbody>
                {data.cqi_matrix_row.map((r, i) => (
                  <tr key={i} style={{ borderBottom: `1px solid ${T.ruleLight}` }}>
                    <td style={{ padding: "3px 0", fontWeight: 600 }}>{r.asset}</td>
                    <td>{r.sii_score?.toFixed(1)}</td>
                    <td>{r.psi_score?.toFixed(1)}</td>
                    <td style={{ fontWeight: 600 }}>{r.cqi_score?.toFixed(1)}</td>
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
  const [genElapsed, setGenElapsed] = useState(0);
  const [batchResults, setBatchResults] = useState([]);
  const [emailDraft, setEmailDraft] = useState(null);
  const [emailDraftOpen, setEmailDraftOpen] = useState(false);
  const [copyMsg, setCopyMsg] = useState("");

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
    setGenElapsed(0);
    const timer = setInterval(() => setGenElapsed(e => e + 1), 1000);
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 60000);
    try {
      const params = new URLSearchParams({ template, format });
      if (lens) params.set("lens", lens);

      const key = getAdminKey();
      const resp = await fetch(`/api/reports/${entityType}/${entityId}?${params}`, {
        headers: { "x-admin-key": key },
        signal: controller.signal,
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
      const msg = e.name === "AbortError" ? "Report generation timed out after 60s" : e.message;
      setPreview(`Error: ${msg}`);
      setPreviewFormat("error");
    } finally {
      clearInterval(timer);
      clearTimeout(timeout);
      setGenerating(false);
    }
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
    ? ["wallet_risk", "engagement"]
    : ["protocol_risk", "compliance", "underwriting", "sbt_metadata", "engagement"];

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
              {generating ? `Generating... ${genElapsed}s` : "Generate"}
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
          <button onClick={() => { setEntityType("protocol"); setEntityId("aave"); setTemplate("engagement"); setLens(""); setFormat("markdown"); setTimeout(generateReport, 100); }}
            style={btn()}>Engagement: Aave</button>
          <button onClick={() => { setEntityType("protocol"); setEntityId("morpho"); setTemplate("engagement"); setLens(""); setFormat("markdown"); setTimeout(generateReport, 100); }}
            style={btn()}>Engagement: Morpho</button>
          <button onClick={() => { setEntityType("stablecoin"); setEntityId("usdc"); setTemplate("engagement"); setLens(""); setFormat("markdown"); setTimeout(generateReport, 100); }}
            style={btn()}>Engagement: USDC</button>
          <button onClick={() => { setEntityType("stablecoin"); setEntityId("usde"); setTemplate("engagement"); setLens(""); setFormat("markdown"); setTimeout(generateReport, 100); }}
            style={btn()}>Engagement: USDe</button>
          <button onClick={() => { setEntityType("stablecoin"); setEntityId("dai"); setTemplate("engagement"); setLens(""); setFormat("markdown"); setTimeout(generateReport, 100); }}
            style={btn()}>Engagement: DAI</button>
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
          {/* Engagement sharing tools */}
          <div style={{ marginTop: 12, display: "flex", gap: 8, alignItems: "center", flexWrap: "wrap" }}>
            <button onClick={() => {
              const url = `https://basisprotocol.xyz/engagement/${entityType}/${entityId}`;
              navigator.clipboard.writeText(url).then(() => { setCopyMsg("URL copied"); setTimeout(() => setCopyMsg(""), 2000); });
            }} style={btn()}>Copy Shareable URL</button>
            <button onClick={async () => {
              try {
                const key = getAdminKey();
                const params = new URLSearchParams({ template: "engagement", format: "json" });
                const resp = await fetch(`/api/reports/${entityType}/${entityId}?${params}`, {
                  headers: { "x-admin-key": key },
                });
                if (!resp.ok) throw new Error(`${resp.status}`);
                const data = await resp.json();
                const draft = data.email_draft;
                if (draft) {
                  const text = `Subject: ${draft.subject}\n\n${draft.body}`;
                  navigator.clipboard.writeText(text).then(() => { setCopyMsg("Email draft copied"); setTimeout(() => setCopyMsg(""), 2000); });
                  setEmailDraft(draft);
                  setEmailDraftOpen(true);
                } else {
                  setCopyMsg("No email draft in response");
                  setTimeout(() => setCopyMsg(""), 2000);
                }
              } catch (e) {
                setCopyMsg(`Error: ${e.message}`);
                setTimeout(() => setCopyMsg(""), 3000);
              }
            }} style={btn()}>Copy Email Draft</button>
            {copyMsg && <span style={{ fontSize: 10, fontFamily: T.mono, color: "#27ae60" }}>{copyMsg}</span>}
          </div>
          {/* Collapsible email draft */}
          {emailDraft && (
            <div style={{ marginTop: 12 }}>
              <button onClick={() => setEmailDraftOpen(!emailDraftOpen)}
                style={{ ...btn(), fontSize: 10, marginBottom: 8 }}>
                {emailDraftOpen ? "Hide" : "Show"} Email Draft
              </button>
              {emailDraftOpen && (
                <div style={{ padding: 12, background: T.paperWarm, border: `1px solid ${T.ruleLight}`, fontSize: 11, fontFamily: T.mono }}>
                  <div style={{ marginBottom: 8 }}>
                    <strong>Subject:</strong> {emailDraft.subject}
                  </div>
                  <pre style={{ whiteSpace: "pre-wrap", margin: 0, fontSize: 10, lineHeight: 1.5 }}>{emailDraft.body}</pre>
                </div>
              )}
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

// ─── ABM Panel ───────────────────────────────────────────────────────

const ABM_STATE_LABELS = {
  0: "Unaware", 1: "Seen", 2: "Engaged", 3: "Opened Private",
  4: "Asked Question", 5: "Call Booked", 6: "Call Done", 7: "Ask Made", 8: "Committed",
};

const ABM_STATE_COLORS = {
  0: "#999", 1: "#95a5a6", 2: "#3498db", 3: "#2980b9",
  4: "#8e44ad", 5: "#f39c12", 6: "#e67e22", 7: "#e74c3c", 8: "#27ae60",
};

function AbmStateBadge({ state }) {
  const label = ABM_STATE_LABELS[state] || `State ${state}`;
  const color = ABM_STATE_COLORS[state] || "#999";
  return (
    <span style={{
      fontSize: 10, fontFamily: T.mono, padding: "2px 6px", borderRadius: 3,
      background: color + "22", color: color, border: `1px solid ${color}44`,
    }}>
      {label}
    </span>
  );
}

function TrackRecordPanel() {
  const [data, setData] = useState(null);
  const [onChain, setOnChain] = useState(null);
  const [loading, setLoading] = useState(false);
  const [flash, showFlash] = useFlash();

  const load = async () => {
    setLoading(true);
    try {
      const [res, oc] = await Promise.all([
        opsFetch("/api/ops/track-record/summary"),
        opsFetch("/api/ops/track-record/on-chain-status").catch(() => null),
      ]);
      setData(res);
      setOnChain(oc);
    } catch (e) {
      showFlash(e.message, false);
    }
    setLoading(false);
  };

  useEffect(() => { load(); }, []);

  return (
    <Section title="TRACK RECORD" actions={
      <button onClick={load} disabled={loading} style={{ fontSize: 9, fontFamily: T.mono, padding: "2px 6px", border: `1px solid ${T.paper}44`, background: "transparent", color: T.paper, cursor: "pointer", opacity: loading ? 0.5 : 1 }}>
        {loading ? "Loading..." : "Refresh"}
      </button>
    }>
      <Flash flash={flash} />
      <div style={{ padding: "0 10px" }}>
        {!data && !loading && <div style={{ color: T.inkFaint, fontSize: 12 }}>No entries yet. The first entry will appear when the next slow cycle detects a qualifying signal.</div>}

        {data && (
          <>
            {/* Summary bar */}
            <div style={{ display: "flex", gap: 16, flexWrap: "wrap", marginBottom: 12, padding: "8px 0", borderBottom: `1px solid ${T.ruleLight}` }}>
              <div>
                <Lbl>Total Entries</Lbl>
                <div style={{ fontSize: 18, fontFamily: T.mono, fontWeight: 700, color: T.ink }}>{data.total_entries || 0}</div>
              </div>
              <div>
                <Lbl>Featured</Lbl>
                <div style={{ fontSize: 14, fontFamily: T.mono, color: "#f39c12" }}>{(data.featured || []).length}</div>
              </div>
              <div>
                <Lbl>Pending Followups</Lbl>
                <div style={{ fontSize: 14, fontFamily: T.mono, color: (data.pending_followups || []).length > 0 ? "#e74c3c" : T.inkFaint }}>{(data.pending_followups || []).length}</div>
              </div>
            </div>

            {/* On-chain anchoring status */}
            {onChain && (
              <div style={{ display: "flex", gap: 16, flexWrap: "wrap", marginBottom: 12, padding: "8px 0", borderBottom: `1px solid ${T.ruleLight}` }}>
                <div>
                  <Lbl>On-chain (Base)</Lbl>
                  <div style={{ fontSize: 14, fontFamily: T.mono, color: onChain.committed_base > 0 ? "#27ae60" : T.inkFaint }}>{onChain.committed_base || 0} / {onChain.total || 0}</div>
                </div>
                <div>
                  <Lbl>On-chain (Arb)</Lbl>
                  <div style={{ fontSize: 14, fontFamily: T.mono, color: onChain.committed_arbitrum > 0 ? "#27ae60" : T.inkFaint }}>{onChain.committed_arbitrum || 0} / {onChain.total || 0}</div>
                </div>
                <div>
                  <Lbl>Uncommitted</Lbl>
                  <div style={{ fontSize: 14, fontFamily: T.mono, color: (onChain.uncommitted_either || 0) > 0 ? "#e74c3c" : "#27ae60" }}>{onChain.uncommitted_either || 0}</div>
                </div>
              </div>
            )}

            {/* By trigger kind (30 days) */}
            {data.by_trigger_kind_30d && data.by_trigger_kind_30d.length > 0 && (
              <div style={{ marginBottom: 10, padding: "6px 0", borderBottom: `1px solid ${T.ruleLight}` }}>
                <div style={{ fontSize: 10, fontWeight: 600, color: T.inkLight, marginBottom: 4 }}>AUTO ENTRIES (30D)</div>
                <div style={{ display: "flex", gap: 12, flexWrap: "wrap", fontSize: 11, fontFamily: T.mono }}>
                  {data.by_trigger_kind_30d.map(r => (
                    <div key={r.trigger_kind}>
                      <span style={{ color: T.inkMid }}>{r.trigger_kind}: </span>
                      <span style={{ fontWeight: 600 }}>{r.cnt}</span>
                    </div>
                  ))}
                </div>
              </div>
            )}

            {/* Featured entries */}
            {data.featured && data.featured.length > 0 && (
              <div style={{ marginBottom: 10, padding: "6px 0", borderBottom: `1px solid ${T.ruleLight}` }}>
                <div style={{ fontSize: 10, fontWeight: 600, color: T.inkLight, marginBottom: 4 }}>FEATURED CALLS</div>
                {data.featured.map(f => (
                  <div key={f.entry_id} style={{ fontSize: 11, fontFamily: T.mono, padding: "2px 0", color: T.inkMid }}>
                    <span style={{ color: "#f39c12" }}>★</span> {f.entity_slug} ({f.index_name}) — {f.trigger_kind}
                    {f.narrative_markdown && <div style={{ fontSize: 10, color: T.inkFaint, marginLeft: 14 }}>{f.narrative_markdown.substring(0, 100)}</div>}
                  </div>
                ))}
              </div>
            )}

            {/* Calibration */}
            {data.calibration && data.calibration.length > 0 && (
              <div style={{ marginBottom: 10, padding: "6px 0" }}>
                <div style={{ fontSize: 10, fontWeight: 600, color: T.inkLight, marginBottom: 4 }}>CALIBRATION</div>
                <div style={{ display: "grid", gridTemplateColumns: "auto auto auto", gap: "2px 12px", fontSize: 11, fontFamily: T.mono }}>
                  {data.calibration.map((c, i) => (
                    <div key={i} style={{ display: "contents" }}>
                      <span style={{ color: T.inkMid }}>{c.trigger_kind}</span>
                      <span style={{ color: c.outcome_category === "validated" ? "#27ae60" : c.outcome_category === "not_borne_out" ? "#e74c3c" : T.inkFaint }}>{c.outcome_category}</span>
                      <span>{c.cnt}</span>
                    </div>
                  ))}
                </div>
              </div>
            )}
          </>
        )}
      </div>
    </Section>
  );
}


function MethodologyPanel() {
  const [items, setItems] = useState([]);
  const [loading, setLoading] = useState(false);
  const [flash, showFlash] = useFlash();

  const load = async () => {
    setLoading(true);
    try {
      const res = await opsFetch("/api/ops/methodology");
      setItems(res.methodologies || []);
    } catch (e) {
      showFlash(e.message, false);
    }
    setLoading(false);
  };

  useEffect(() => { load(); }, []);

  return (
    <Section title="METHODOLOGY HASHES" actions={
      <button onClick={load} disabled={loading} style={{ fontSize: 9, fontFamily: T.mono, padding: "2px 6px", border: `1px solid ${T.paper}44`, background: "transparent", color: T.paper, cursor: "pointer", opacity: loading ? 0.5 : 1 }}>
        {loading ? "Loading..." : "Refresh"}
      </button>
    }>
      <Flash flash={flash} />
      <div style={{ padding: "0 10px" }}>
        {items.length === 0 && !loading && (
          <div style={{ color: T.inkFaint, fontSize: 12 }}>No methodologies registered yet.</div>
        )}
        {items.length > 0 && (
          <div style={{ display: "grid", gridTemplateColumns: "1fr auto auto auto auto", gap: "4px 12px", fontSize: 11, fontFamily: T.mono, alignItems: "center" }}>
            <div style={{ fontWeight: 600, fontSize: 10, color: T.inkLight, textTransform: "uppercase" }}>ID</div>
            <div style={{ fontWeight: 600, fontSize: 10, color: T.inkLight, textTransform: "uppercase" }}>Hash</div>
            <div style={{ fontWeight: 600, fontSize: 10, color: T.inkLight, textTransform: "uppercase" }}>Base</div>
            <div style={{ fontWeight: 600, fontSize: 10, color: T.inkLight, textTransform: "uppercase" }}>Arb</div>
            <div style={{ fontWeight: 600, fontSize: 10, color: T.inkLight, textTransform: "uppercase" }}>Registered</div>
            {items.map((m) => (
              <div key={m.methodology_id} style={{ display: "contents" }}>
                <span style={{ color: T.inkMid }}>{m.methodology_id}</span>
                <span style={{ color: T.inkFaint, fontSize: 10 }}>{(m.content_hash || "").substring(0, 12)}...</span>
                <span style={{ color: m.committed_on_chain_base ? "#27ae60" : "#e74c3c" }}>{m.committed_on_chain_base ? "✓" : "✗"}</span>
                <span style={{ color: m.committed_on_chain_arbitrum ? "#27ae60" : "#e74c3c" }}>{m.committed_on_chain_arbitrum ? "✓" : "✗"}</span>
                <span style={{ color: T.inkFaint, fontSize: 10 }}>{m.registered_at ? new Date(m.registered_at).toLocaleDateString() : ""}</span>
              </div>
            ))}
          </div>
        )}
      </div>
    </Section>
  );
}


function ABMPanel() {
  const [campaigns, setCampaigns] = useState([]);
  const [config, setConfig] = useState(null);
  const [targets, setTargets] = useState([]);
  const [loading, setLoading] = useState(false);
  const [flash, showFlash] = useFlash();
  const [busy, setBusy] = useState(null);

  // View state
  const [view, setView] = useState("list"); // list | create | detail
  const [selectedId, setSelectedId] = useState(null);
  const [detail, setDetail] = useState(null);

  // Create wizard state
  const [step, setStep] = useState(0);
  const [form, setForm] = useState({
    mode: "icp", icp_type: "", org: "", person: "", title: "",
    stablecoins: [], lenses: [], pain_points: [], named_target_id: null,
  });

  // Score data for detail view
  const [scores, setScores] = useState(null);

  const loadCampaigns = async () => {
    setLoading(true);
    try {
      const res = await opsFetch("/api/ops/abm/campaigns");
      setCampaigns(res.campaigns || []);
    } catch (e) { showFlash(e.message, false); }
    setLoading(false);
  };

  const loadConfig = async () => {
    try {
      const res = await opsFetch("/api/ops/abm/config");
      setConfig(res);
    } catch (e) { /* config load failure is non-fatal */ }
  };

  const loadTargets = async () => {
    try {
      const res = await opsFetch("/api/ops/targets");
      setTargets(res.targets || []);
    } catch (e) { /* targets load failure is non-fatal */ }
  };

  useEffect(() => {
    loadCampaigns();
    loadConfig();
    loadTargets();
  }, []);

  const openDetail = async (id) => {
    setSelectedId(id);
    setView("detail");
    setBusy("detail");
    try {
      const res = await opsFetch(`/api/ops/abm/campaigns/${id}`);
      setDetail(res);
      // Fetch SII scores for campaign stablecoins
      const coins = res.campaign?.stablecoins || [];
      if (coins.length > 0) {
        try {
          const scoresRes = await fetch("/api/scores");
          if (scoresRes.ok) {
            const scoresData = await scoresRes.json();
            setScores(scoresData.scores || scoresData);
          }
        } catch (e) { /* scores are optional */ }
      }
    } catch (e) { showFlash(e.message, false); setView("list"); }
    setBusy(null);
  };

  const handleCreate = async () => {
    setBusy("create");
    try {
      await opsFetch("/api/ops/abm/campaigns", {
        method: "POST",
        body: JSON.stringify(form),
      });
      showFlash("Campaign created");
      setView("list");
      setStep(0);
      setForm({ mode: "icp", icp_type: "", org: "", person: "", title: "", stablecoins: [], lenses: [], pain_points: [], named_target_id: null });
      await loadCampaigns();
    } catch (e) { showFlash(e.message, false); }
    setBusy(null);
  };

  const handleStateChange = async (campaignId, newState) => {
    setBusy("state");
    try {
      await opsFetch(`/api/ops/abm/campaigns/${campaignId}/state`, {
        method: "PUT",
        body: JSON.stringify({ state: newState }),
      });
      showFlash(`State updated to ${ABM_STATE_LABELS[newState]}`);
      await openDetail(campaignId);
      await loadCampaigns();
    } catch (e) { showFlash(e.message, false); }
    setBusy(null);
  };

  const handleAddLog = async (campaignId, note) => {
    setBusy("log");
    try {
      await opsFetch(`/api/ops/abm/campaigns/${campaignId}/log`, {
        method: "POST",
        body: JSON.stringify({ note }),
      });
      showFlash("Log entry added");
      await openDetail(campaignId);
    } catch (e) { showFlash(e.message, false); }
    setBusy(null);
  };

  const handleDripUpdate = async (touchId, status, response_text) => {
    setBusy("drip");
    try {
      const body = { status };
      if (response_text !== undefined) body.response = response_text;
      await opsFetch(`/api/ops/abm/drip/${touchId}`, {
        method: "PUT",
        body: JSON.stringify(body),
      });
      showFlash(`Touch marked ${status}`);
      if (selectedId) await openDetail(selectedId);
    } catch (e) { showFlash(e.message, false); }
    setBusy(null);
  };

  // Drip touch accordion state
  const [openTouchId, setOpenTouchId] = useState(null);
  const [touchDrafts, setTouchDrafts] = useState({}); // { touchId: string }
  const [touchResponses, setTouchResponses] = useState({}); // { touchId: string }
  const [dripDmDraft, setDripDmDraft] = useState(null); // { touchId, draft }
  const [dripDmBusy, setDripDmBusy] = useState(null); // touchId

  const toggleTouch = (touchId) => {
    setOpenTouchId(prev => prev === touchId ? null : touchId);
  };

  const handleDripDraftDm = async (touchId, targetId, trigger) => {
    if (!targetId) { showFlash("No linked target — cannot draft DM", false); return; }
    setDripDmBusy(touchId);
    try {
      const res = await opsFetch("/api/ops/draft/dm", {
        method: "POST", body: JSON.stringify({ target_id: targetId, trigger }),
      });
      setDripDmDraft({ touchId, draft: res.draft });
      showFlash("DM draft generated");
    } catch (e) { showFlash(e.message, false); }
    setDripDmBusy(null);
  };

  const handleDripLogResponse = async (touchId, responseText) => {
    setBusy("drip");
    try {
      await opsFetch(`/api/ops/abm/drip/${touchId}`, {
        method: "PUT",
        body: JSON.stringify({ status: "sent", response: responseText }),
      });
      showFlash("Response logged");
      setTouchResponses(prev => ({ ...prev, [touchId]: "" }));
      if (selectedId) await openDetail(selectedId);
    } catch (e) { showFlash(e.message, false); }
    setBusy(null);
  };

  const [guideText, setGuideText] = useState(null);

  const handleGenerateGuide = async (campaignId) => {
    setBusy("guide");
    try {
      const res = await opsFetch(`/api/ops/abm/campaigns/${campaignId}/generate-guide`, { method: "POST" });
      setGuideText(res.guide_markdown || null);
      showFlash(`Guide generated (hash: ${res.guide_hash})`);
      if (selectedId) await openDetail(selectedId);
    } catch (e) { showFlash(e.message, false); }
    setBusy(null);
  };

  const handleDelete = async (campaignId) => {
    setBusy("delete");
    try {
      await opsFetch(`/api/ops/abm/campaigns/${campaignId}`, { method: "DELETE" });
      showFlash("Campaign deleted");
      setView("list");
      setDetail(null);
      await loadCampaigns();
    } catch (e) { showFlash(e.message, false); }
    setBusy(null);
  };

  // Toggle helpers for stablecoins / lenses / pain_points
  const toggleItem = (field, item) => {
    setForm(prev => {
      const arr = prev[field] || [];
      return { ...prev, [field]: arr.includes(item) ? arr.filter(x => x !== item) : [...arr, item] };
    });
  };

  const icpTypes = config?.icp_types || {};
  const stateLabels = config?.state_labels || ABM_STATE_LABELS;

  // ─── Create Wizard ───────────────────────────────────────────────

  const renderCreateWizard = () => {
    const selectedIcp = icpTypes[form.icp_type] || {};

    return (
      <Section title="NEW ABM CAMPAIGN" actions={
        <button onClick={() => { setView("list"); setStep(0); }} style={{ fontSize: 9, fontFamily: T.mono, padding: "2px 6px", border: `1px solid ${T.paper}44`, background: "transparent", color: T.paper, cursor: "pointer" }}>
          Cancel
        </button>
      }>
        <div style={{ padding: "8px 10px" }}>
          {/* Step indicator */}
          <div style={{ display: "flex", gap: 8, marginBottom: 12 }}>
            {["Mode", "Type", "Details", "Configure", "Launch"].map((s, i) => (
              <span key={i} style={{
                fontSize: 9, fontFamily: T.mono, padding: "2px 6px",
                background: step === i ? T.ink : "transparent",
                color: step === i ? T.paper : T.inkFaint,
                border: `1px solid ${step === i ? T.ink : T.ruleMid}`,
              }}>
                {i + 1}. {s}
              </span>
            ))}
          </div>

          {/* Step 0: Mode */}
          {step === 0 && (
            <div>
              <Lbl>Campaign Mode</Lbl>
              <div style={{ display: "flex", gap: 8, marginTop: 6 }}>
                <button onClick={() => { setForm({ ...form, mode: "named" }); setStep(1); }}
                  style={form.mode === "named" ? btnActive({ padding: "6px 12px" }) : btn({ padding: "6px 12px" })}>
                  Named Target
                </button>
                <button onClick={() => { setForm({ ...form, mode: "icp" }); setStep(1); }}
                  style={form.mode === "icp" ? btnActive({ padding: "6px 12px" }) : btn({ padding: "6px 12px" })}>
                  ICP at Organization
                </button>
              </div>
              <div style={{ fontSize: 10, color: T.inkFaint, marginTop: 6 }}>
                {form.mode === "named"
                  ? "Link to an existing ops target for integrated tracking."
                  : "Create a campaign for a new organization by ICP type."}
              </div>
            </div>
          )}

          {/* Step 1: ICP type or Named target */}
          {step === 1 && (
            <div>
              {form.mode === "named" ? (
                <div>
                  <Lbl>Select Existing Target</Lbl>
                  <div style={{ marginTop: 6, maxHeight: 200, overflowY: "auto" }}>
                    {targets.length === 0 && <div style={{ fontSize: 11, color: T.inkFaint }}>No targets found.</div>}
                    {targets.map(t => (
                      <div key={t.id} onClick={() => {
                        setForm({ ...form, named_target_id: t.id, org: t.name, icp_type: form.icp_type || "exchange_eu" });
                      }} style={{
                        padding: "4px 8px", fontSize: 11, fontFamily: T.mono, cursor: "pointer",
                        borderBottom: `1px dotted ${T.ruleMid}`,
                        background: form.named_target_id === t.id ? T.paperWarm : "transparent",
                      }}>
                        <TierBadge tier={t.tier} /> {t.name} <span style={{ color: T.inkFaint }}>({t.type})</span>
                      </div>
                    ))}
                  </div>
                  {form.named_target_id && (
                    <div style={{ marginTop: 8 }}>
                      <Lbl>ICP Type</Lbl>
                      <div style={{ display: "flex", gap: 4, flexWrap: "wrap", marginTop: 4 }}>
                        {Object.entries(icpTypes).map(([key, val]) => (
                          <button key={key} onClick={() => setForm({ ...form, icp_type: key })}
                            style={form.icp_type === key ? btnActive() : btn()}>
                            {val.label}
                          </button>
                        ))}
                      </div>
                    </div>
                  )}
                  {form.named_target_id && form.icp_type && (
                    <button onClick={() => setStep(3)} style={{ ...btn({ marginTop: 8 }), background: T.paperWarm }}>Next →</button>
                  )}
                </div>
              ) : (
                <div>
                  <Lbl>Select ICP Type</Lbl>
                  <div style={{ display: "flex", gap: 6, flexWrap: "wrap", marginTop: 6 }}>
                    {Object.entries(icpTypes).map(([key, val]) => (
                      <button key={key} onClick={() => {
                        const cfg = icpTypes[key] || {};
                        setForm({
                          ...form, icp_type: key,
                          stablecoins: cfg.default_coins || [],
                          lenses: cfg.lenses || [],
                          pain_points: cfg.pain_points || [],
                        });
                        setStep(2);
                      }} style={form.icp_type === key ? btnActive({ padding: "6px 12px" }) : btn({ padding: "6px 12px" })}>
                        {val.label}
                      </button>
                    ))}
                  </div>
                </div>
              )}
              <button onClick={() => setStep(0)} style={btn({ marginTop: 8 })}>← Back</button>
            </div>
          )}

          {/* Step 2: Org / Person / Title (ICP mode) */}
          {step === 2 && (
            <div>
              <div style={{ display: "flex", gap: 8, flexWrap: "wrap", marginBottom: 8 }}>
                <div style={{ flex: 1, minWidth: 180 }}>
                  <Lbl>Organization</Lbl>
                  <input value={form.org} onChange={e => setForm({ ...form, org: e.target.value })}
                    placeholder="e.g. Kraken, MakerDAO..."
                    style={{ display: "block", width: "100%", fontFamily: T.mono, fontSize: 11, padding: "4px 6px", border: `1px solid ${T.ruleMid}`, background: T.paper, marginTop: 2, boxSizing: "border-box" }} />
                </div>
                <div style={{ flex: 1, minWidth: 140 }}>
                  <Lbl>Contact Person</Lbl>
                  <input value={form.person || ""} onChange={e => setForm({ ...form, person: e.target.value })}
                    placeholder="Optional"
                    style={{ display: "block", width: "100%", fontFamily: T.mono, fontSize: 11, padding: "4px 6px", border: `1px solid ${T.ruleMid}`, background: T.paper, marginTop: 2, boxSizing: "border-box" }} />
                </div>
                <div style={{ flex: 1, minWidth: 140 }}>
                  <Lbl>Title / Role</Lbl>
                  <input value={form.title || ""} onChange={e => setForm({ ...form, title: e.target.value })}
                    placeholder="e.g. Head of Compliance"
                    style={{ display: "block", width: "100%", fontFamily: T.mono, fontSize: 11, padding: "4px 6px", border: `1px solid ${T.ruleMid}`, background: T.paper, marginTop: 2, boxSizing: "border-box" }} />
                </div>
              </div>
              <div style={{ display: "flex", gap: 6 }}>
                <button onClick={() => setStep(1)} style={btn()}>← Back</button>
                <button onClick={() => setStep(3)} disabled={!form.org.trim()}
                  style={btn({ background: form.org.trim() ? T.paperWarm : T.paper, opacity: form.org.trim() ? 1 : 0.5 })}>
                  Next →
                </button>
              </div>
            </div>
          )}

          {/* Step 3: Configure stablecoins, lenses, pain points */}
          {step === 3 && (
            <div>
              <div style={{ marginBottom: 10 }}>
                <Lbl>Stablecoins</Lbl>
                <div style={{ display: "flex", gap: 4, flexWrap: "wrap", marginTop: 4 }}>
                  {["USDT", "USDC", "DAI", "FRAX", "PYUSD", "FDUSD", "TUSD", "USDD", "USDe", "USD1", "GUSD", "LUSD", "XSGD"].map(coin => (
                    <button key={coin} onClick={() => toggleItem("stablecoins", coin)}
                      style={form.stablecoins.includes(coin) ? btnActive() : btn()}>
                      {coin}
                    </button>
                  ))}
                </div>
              </div>
              <div style={{ marginBottom: 10 }}>
                <Lbl>Regulatory Lenses</Lbl>
                <div style={{ display: "flex", gap: 4, flexWrap: "wrap", marginTop: 4 }}>
                  {["MICA67", "SCO60", "GENIUS", "OCC", "MAS10"].map(lens => (
                    <button key={lens} onClick={() => toggleItem("lenses", lens)}
                      style={form.lenses.includes(lens)
                        ? btnActive({ padding: "4px 10px" })
                        : btn({ padding: "4px 10px" })}>
                      {lens}
                    </button>
                  ))}
                </div>
              </div>
              <div style={{ marginBottom: 10 }}>
                <Lbl>Pain Points</Lbl>
                <div style={{ marginTop: 4 }}>
                  {form.pain_points.map((pp, i) => (
                    <div key={i} style={{ display: "flex", alignItems: "center", gap: 4, marginBottom: 3 }}>
                      <span style={{ fontSize: 10, fontFamily: T.mono, color: T.inkMid, flex: 1 }}>{pp}</span>
                      <button onClick={() => setForm(prev => ({ ...prev, pain_points: prev.pain_points.filter((_, j) => j !== i) }))}
                        style={{ fontSize: 9, fontFamily: T.mono, border: "none", background: "transparent", color: T.accent, cursor: "pointer" }}>
                        ×
                      </button>
                    </div>
                  ))}
                </div>
              </div>
              <div style={{ display: "flex", gap: 6 }}>
                <button onClick={() => setStep(form.mode === "named" ? 1 : 2)} style={btn()}>← Back</button>
                <button onClick={() => setStep(4)} style={btn({ background: T.paperWarm })}>Next →</button>
              </div>
            </div>
          )}

          {/* Step 4: Summary + Launch */}
          {step === 4 && (
            <div>
              <div style={{ background: T.paperWarm, border: `1px solid ${T.ruleLight}`, padding: 10, marginBottom: 10 }}>
                <div style={{ display: "flex", gap: 16, flexWrap: "wrap", marginBottom: 8 }}>
                  <div><Lbl>Mode</Lbl><div style={{ fontSize: 11, fontFamily: T.mono }}>{form.mode}</div></div>
                  <div><Lbl>ICP Type</Lbl><div style={{ fontSize: 11, fontFamily: T.mono }}>{icpTypes[form.icp_type]?.label || form.icp_type}</div></div>
                  <div><Lbl>Organization</Lbl><div style={{ fontSize: 11, fontFamily: T.mono, fontWeight: 600 }}>{form.org}</div></div>
                  {form.person && <div><Lbl>Person</Lbl><div style={{ fontSize: 11, fontFamily: T.mono }}>{form.person}</div></div>}
                  {form.title && <div><Lbl>Title</Lbl><div style={{ fontSize: 11, fontFamily: T.mono }}>{form.title}</div></div>}
                </div>
                <div style={{ marginBottom: 6 }}>
                  <Lbl>Stablecoins</Lbl>
                  <div style={{ fontSize: 10, fontFamily: T.mono, color: T.inkMid, marginTop: 2 }}>{form.stablecoins.join(", ") || "—"}</div>
                </div>
                <div style={{ marginBottom: 6 }}>
                  <Lbl>Lenses</Lbl>
                  <div style={{ fontSize: 10, fontFamily: T.mono, color: T.inkMid, marginTop: 2 }}>{form.lenses.join(", ") || "—"}</div>
                </div>
                <div>
                  <Lbl>Pain Points</Lbl>
                  <div style={{ fontSize: 10, fontFamily: T.mono, color: T.inkMid, marginTop: 2 }}>
                    {form.pain_points.length > 0 ? form.pain_points.map((pp, i) => <div key={i}>· {pp}</div>) : "—"}
                  </div>
                </div>
              </div>
              <div style={{ display: "flex", gap: 6 }}>
                <button onClick={() => setStep(3)} style={btn()}>← Back</button>
                <button onClick={handleCreate} disabled={busy === "create" || !form.org.trim() || !form.icp_type}
                  style={btnActive({ padding: "6px 14px", opacity: busy === "create" ? 0.5 : 1 })}>
                  {busy === "create" ? "Creating..." : "Launch Campaign"}
                </button>
              </div>
            </div>
          )}
        </div>
      </Section>
    );
  };

  // ─── Detail View ─────────────────────────────────────────────────

  const renderDetail = () => {
    if (!detail) return <div style={{ padding: 10, color: T.inkFaint, fontSize: 12 }}>Loading...</div>;
    const c = detail.campaign;
    const touches = detail.touches || [];
    const log = detail.log || [];
    const campaignCoins = c.stablecoins || [];
    const campaignLenses = c.lenses || [];

    // Filter SII scores to campaign stablecoins
    const coinScores = scores
      ? (Array.isArray(scores) ? scores : Object.values(scores)).filter(s => campaignCoins.includes(s.symbol || s.stablecoin))
      : [];

    return (
      <>
        <Section title={`CAMPAIGN: ${c.org}`} actions={
          <div style={{ display: "flex", gap: 4 }}>
            <button onClick={() => { setView("list"); setDetail(null); setScores(null); }}
              style={{ fontSize: 9, fontFamily: T.mono, padding: "2px 6px", border: `1px solid ${T.paper}44`, background: "transparent", color: T.paper, cursor: "pointer" }}>
              ← Back
            </button>
            <button onClick={() => handleGenerateGuide(c.id)} disabled={busy === "guide"}
              style={{ fontSize: 9, fontFamily: T.mono, padding: "2px 6px", border: `1px solid ${T.paper}44`, background: "transparent", color: T.paper, cursor: "pointer", opacity: busy === "guide" ? 0.5 : 1 }}>
              {busy === "guide" ? "Generating..." : "Generate Guide"}
            </button>
            <button onClick={() => handleDelete(c.id)} disabled={busy === "delete"}
              style={{ fontSize: 9, fontFamily: T.mono, padding: "2px 6px", border: `1px solid ${T.accent}44`, background: "transparent", color: "#fc988f", cursor: "pointer", opacity: busy === "delete" ? 0.5 : 1 }}>
              {busy === "delete" ? "..." : "Delete"}
            </button>
          </div>
        }>
          <div style={{ padding: "8px 10px" }}>
            {/* Campaign header */}
            <div style={{ display: "flex", gap: 16, flexWrap: "wrap", marginBottom: 10, paddingBottom: 8, borderBottom: `1px solid ${T.ruleLight}` }}>
              <div><Lbl>ICP Type</Lbl><div style={{ fontSize: 11, fontFamily: T.mono }}>{icpTypes[c.icp_type]?.label || c.icp_type}</div></div>
              <div><Lbl>Mode</Lbl><div style={{ fontSize: 11, fontFamily: T.mono }}>{c.mode}</div></div>
              {c.person && <div><Lbl>Person</Lbl><div style={{ fontSize: 11, fontFamily: T.mono }}>{c.person}{c.title ? ` — ${c.title}` : ""}</div></div>}
              <div><Lbl>State</Lbl><div style={{ marginTop: 2 }}><AbmStateBadge state={c.state} /></div></div>
              {c.report_hash && <div><Lbl>Report Hash</Lbl><div style={{ fontSize: 10, fontFamily: T.mono, color: T.inkFaint }}>{c.report_hash}</div></div>}
              <div><Lbl>Created</Lbl><div style={{ fontSize: 10, fontFamily: T.mono, color: T.inkFaint }}>{new Date(c.created_at).toLocaleDateString()}</div></div>
            </div>

            {/* Stablecoins + Lenses */}
            <div style={{ display: "flex", gap: 16, marginBottom: 10 }}>
              <div>
                <Lbl>Stablecoins</Lbl>
                <div style={{ display: "flex", gap: 3, marginTop: 3 }}>
                  {campaignCoins.map(coin => (
                    <span key={coin} style={{ fontSize: 10, fontFamily: T.mono, padding: "2px 5px", background: T.paperWarm, border: `1px solid ${T.ruleLight}` }}>{coin}</span>
                  ))}
                </div>
              </div>
              <div>
                <Lbl>Lenses</Lbl>
                <div style={{ display: "flex", gap: 3, marginTop: 3 }}>
                  {campaignLenses.map(lens => (
                    <span key={lens} style={{ fontSize: 10, fontFamily: T.mono, padding: "2px 5px", background: T.accent + "15", border: `1px solid ${T.accent}33`, color: T.accent }}>{lens}</span>
                  ))}
                </div>
              </div>
            </div>

            {/* Pain Points */}
            {(c.pain_points || []).length > 0 && (
              <div style={{ marginBottom: 10 }}>
                <Lbl>Pain Points</Lbl>
                <div style={{ marginTop: 3 }}>
                  {c.pain_points.map((pp, i) => (
                    <div key={i} style={{ fontSize: 10, fontFamily: T.mono, color: T.inkMid, padding: "2px 0", borderBottom: `1px dotted ${T.ruleMid}` }}>· {pp}</div>
                  ))}
                </div>
              </div>
            )}

            {/* SII Scores table */}
            {coinScores.length > 0 && (
              <div style={{ marginBottom: 10 }}>
                <Lbl>SII Scores</Lbl>
                <table style={{ width: "100%", fontSize: 10, fontFamily: T.mono, borderCollapse: "collapse", marginTop: 4 }}>
                  <thead>
                    <tr style={{ borderBottom: `1px solid ${T.ruleMid}`, textAlign: "left" }}>
                      <th style={{ padding: "4px 8px", color: T.inkLight }}>Coin</th>
                      <th style={{ padding: "4px 8px", color: T.inkLight }}>Score</th>
                      <th style={{ padding: "4px 8px", color: T.inkLight }}>Peg</th>
                      <th style={{ padding: "4px 8px", color: T.inkLight }}>Liquidity</th>
                      <th style={{ padding: "4px 8px", color: T.inkLight }}>Structural</th>
                    </tr>
                  </thead>
                  <tbody>
                    {coinScores.map((s, i) => (
                      <tr key={i} style={{ borderBottom: `1px solid ${T.ruleLight}` }}>
                        <td style={{ padding: "4px 8px", fontWeight: 600 }}>{s.symbol || s.stablecoin}</td>
                        <td style={{ padding: "4px 8px" }}>{s.overall_score != null ? Number(s.overall_score).toFixed(1) : "—"}</td>
                        <td style={{ padding: "4px 8px", color: T.inkMid }}>{s.peg_score != null ? Number(s.peg_score).toFixed(1) : "—"}</td>
                        <td style={{ padding: "4px 8px", color: T.inkMid }}>{s.liquidity_score != null ? Number(s.liquidity_score).toFixed(1) : "—"}</td>
                        <td style={{ padding: "4px 8px", color: T.inkMid }}>{s.structural_score != null ? Number(s.structural_score).toFixed(1) : "—"}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </div>
        </Section>

        {/* State Tracker */}
        <Section title="STATE TRACKER">
          <div style={{ padding: "8px 10px" }}>
            <div style={{ display: "flex", gap: 3, flexWrap: "wrap", marginBottom: 10 }}>
              {[0, 1, 2, 3, 4, 5, 6, 7, 8].map(s => (
                <button key={s} onClick={() => handleStateChange(c.id, s)} disabled={busy === "state"}
                  style={c.state === s
                    ? btnActive({ padding: "4px 8px", fontSize: 9 })
                    : btn({ padding: "4px 8px", fontSize: 9, opacity: c.state > s ? 0.6 : 1 })}>
                  {s}: {ABM_STATE_LABELS[s]}
                </button>
              ))}
            </div>
          </div>
        </Section>

        {/* Drip Sequence */}
        {(() => {
          // Find the "current" touch: first pending after last sent
          let lastSentIdx = -1;
          touches.forEach((t, i) => { if (t.status === "sent") lastSentIdx = i; });
          const currentTouchId = touches.find((t, i) => t.status === "pending" && i > lastSentIdx)?.id || null;

          return (
            <Section title={`DRIP SEQUENCE (${touches.length} touches)`}>
              <div style={{ padding: "4px 0" }}>
                {touches.length === 0 && <div style={{ fontSize: 11, color: T.inkFaint, padding: "8px 10px" }}>No drip touches configured.</div>}
                {touches.map((t) => {
                  const isOpen = openTouchId === t.id;
                  const isCurrent = t.id === currentTouchId;
                  const isSent = t.status === "sent";
                  const isSkipped = t.status === "skipped";
                  const dotColor = isSent ? "#27ae60" : isSkipped ? T.ruleLight : T.inkFaint;
                  const channelBg = t.channel === "email" ? "#3498db15" : t.channel === "linkedin" ? "#0077b515" : t.channel === "twitter" ? "#1da1f215" : "#95a5a615";
                  const channelBorder = t.channel === "email" ? "#3498db33" : t.channel === "linkedin" ? "#0077b533" : t.channel === "twitter" ? "#1da1f233" : "#95a5a633";

                  return (
                    <div key={t.id} style={{ borderBottom: `1px dotted ${T.ruleMid}` }}>
                      {/* Collapsed row — always visible */}
                      <div onClick={() => toggleTouch(t.id)} style={{
                        display: "flex", gap: 8, alignItems: "center", padding: "6px 10px",
                        cursor: "pointer", opacity: isSkipped ? 0.4 : 1,
                        background: isCurrent && !isOpen ? T.paperWarm : isOpen ? T.paperWarm : "transparent",
                        transition: "background 0.15s",
                      }}>
                        {/* Status dot */}
                        <span style={{
                          width: 6, height: 6, borderRadius: "50%", flexShrink: 0,
                          background: dotColor,
                        }} />
                        <span style={{ fontSize: 10, fontFamily: T.mono, color: T.inkFaint, minWidth: 28 }}>D{t.day}</span>
                        <span style={{
                          fontSize: 9, fontFamily: T.mono, padding: "1px 4px", minWidth: 50, textAlign: "center",
                          background: channelBg, border: `1px solid ${channelBorder}`, color: T.inkMid,
                        }}>
                          {t.channel}
                        </span>
                        {t.is_gate && (
                          <span style={{ fontSize: 9, fontFamily: T.mono, padding: "1px 3px", border: `1px solid ${T.accent}`, color: T.accent }}>GATE</span>
                        )}
                        <span style={{
                          flex: 1, fontSize: 10, fontFamily: T.mono, color: T.inkMid,
                          textDecoration: isSent ? "line-through" : "none",
                        }}>
                          {t.subject}
                        </span>
                        {isSent && t.sent_at && (
                          <span style={{ fontSize: 9, fontFamily: T.mono, color: "#27ae60" }}>
                            {new Date(t.sent_at).toLocaleDateString()}
                          </span>
                        )}
                        <span style={{ fontSize: 10, color: T.inkFaint }}>{isOpen ? "\u25BC" : "\u25B6"}</span>
                      </div>

                      {/* Expanded detail panel */}
                      {isOpen && (
                        <div style={{
                          padding: "8px 10px 12px 24px", background: T.paperWarm,
                          animation: "fadeIn 0.15s ease",
                        }}>
                          {/* Description */}
                          {t.description && (
                            <div style={{ fontSize: 11, color: T.inkMid, marginBottom: 8, lineHeight: 1.5 }}>
                              {t.description}
                            </div>
                          )}

                          {/* Gate context — prominent display */}
                          {t.is_gate && (
                            <div style={{
                              padding: "6px 8px", marginBottom: 8,
                              border: `1px solid ${T.accent}`, background: T.accent + "08",
                            }}>
                              <div style={{ fontSize: 9, fontFamily: T.mono, color: T.accent, textTransform: "uppercase", letterSpacing: 0.5, marginBottom: 3 }}>Gate Requirement</div>
                              <div style={{ fontSize: 11, color: T.inkMid }}>
                                {(t.description || "").replace(/^GATE:\s*/i, "") || "Prospect must complete this gate to proceed."}
                              </div>
                            </div>
                          )}

                          {/* Pending touch: compose + actions */}
                          {t.status === "pending" && (
                            <>
                              <div style={{ marginBottom: 6 }}>
                                <Lbl>Compose</Lbl>
                                <textarea
                                  value={touchDrafts[t.id] !== undefined ? touchDrafts[t.id] : t.subject}
                                  onChange={(e) => setTouchDrafts(prev => ({ ...prev, [t.id]: e.target.value }))}
                                  rows={3}
                                  style={{
                                    width: "100%", marginTop: 3, fontFamily: T.mono, fontSize: 11,
                                    padding: "6px 8px", border: `1px solid ${T.ruleMid}`, background: T.paper,
                                    resize: "vertical", lineHeight: 1.5,
                                  }}
                                />
                              </div>
                              <div style={{ display: "flex", gap: 4, flexWrap: "wrap" }}>
                                <button onClick={() => handleDripUpdate(t.id, "sent")} disabled={busy === "drip"}
                                  style={btn({ background: "#27ae6018", color: "#27ae60", fontWeight: 600 })}>
                                  {busy === "drip" ? "..." : "Mark Sent"}
                                </button>
                                <button onClick={() => handleDripUpdate(t.id, "skipped")} disabled={busy === "drip"}
                                  style={btn({ color: T.inkFaint })}>
                                  Skip
                                </button>
                                {c.named_target_id && (
                                  <button onClick={() => handleDripDraftDm(t.id, c.named_target_id, t.subject)}
                                    disabled={dripDmBusy === t.id}
                                    style={btn({ background: "#3498db18", color: "#3498db" })}>
                                    {dripDmBusy === t.id ? "Generating..." : "Draft Email"}
                                  </button>
                                )}
                              </div>
                            </>
                          )}

                          {/* Sent touch: show timestamp + response logging */}
                          {isSent && (
                            <div>
                              <div style={{ fontSize: 10, fontFamily: T.mono, color: "#27ae60", marginBottom: 6 }}>
                                Sent {t.sent_at ? new Date(t.sent_at).toLocaleString() : ""}
                                {t.response && <span style={{ color: T.inkMid, marginLeft: 8 }}>Response: {t.response}</span>}
                              </div>
                              <div style={{ display: "flex", gap: 4, alignItems: "center" }}>
                                <input
                                  value={touchResponses[t.id] || ""}
                                  onChange={(e) => setTouchResponses(prev => ({ ...prev, [t.id]: e.target.value }))}
                                  placeholder="Log response..."
                                  onKeyDown={(e) => e.key === "Enter" && touchResponses[t.id]?.trim() && handleDripLogResponse(t.id, touchResponses[t.id].trim())}
                                  style={{
                                    flex: 1, fontFamily: T.mono, fontSize: 11, padding: "4px 6px",
                                    border: `1px solid ${T.ruleMid}`, background: T.paper,
                                  }}
                                />
                                <button onClick={() => touchResponses[t.id]?.trim() && handleDripLogResponse(t.id, touchResponses[t.id].trim())}
                                  disabled={!touchResponses[t.id]?.trim() || busy === "drip"}
                                  style={btn({ fontSize: 9 })}>
                                  {busy === "drip" ? "..." : "Save"}
                                </button>
                              </div>
                            </div>
                          )}

                          {/* Skipped touch: minimal info */}
                          {isSkipped && (
                            <div style={{ fontSize: 10, color: T.inkFaint, fontStyle: "italic" }}>Skipped</div>
                          )}

                          {/* DM Draft display */}
                          {dripDmDraft && dripDmDraft.touchId === t.id && dripDmDraft.draft && (
                            <div style={{ marginTop: 8, background: T.paper, border: `1px solid ${T.ruleLight}`, padding: "8px 10px", fontSize: 11 }}>
                              {dripDmDraft.draft.twitter_dm && (
                                <div style={{ marginBottom: 6 }}>
                                  <Lbl>Twitter DM</Lbl>
                                  <div style={{ fontFamily: T.mono, marginTop: 2, whiteSpace: "pre-wrap" }}>{dripDmDraft.draft.twitter_dm}</div>
                                </div>
                              )}
                              {dripDmDraft.draft.email_subject && (
                                <div style={{ marginBottom: 6 }}>
                                  <Lbl>Email</Lbl>
                                  <div style={{ fontFamily: T.mono, marginTop: 2 }}>Subject: {dripDmDraft.draft.email_subject}</div>
                                  <div style={{ marginTop: 4, whiteSpace: "pre-wrap" }}>{dripDmDraft.draft.email_body}</div>
                                </div>
                              )}
                              {dripDmDraft.draft.rationale && (
                                <div style={{ fontSize: 10, color: T.inkFaint, marginTop: 4, fontStyle: "italic" }}>
                                  Rationale: {dripDmDraft.draft.rationale}
                                </div>
                              )}
                            </div>
                          )}
                        </div>
                      )}
                    </div>
                  );
                })}
              </div>
            </Section>
          );
        })()}

        {/* Touch Log */}
        {/* Generated Guide Preview */}
        {guideText && (
          <Section title="INTEGRATION GUIDE PREVIEW">
            <pre style={{
              fontFamily: T.mono, fontSize: 10, background: T.paperWarm,
              padding: 12, border: `1px solid ${T.ruleLight}`, overflow: "auto",
              maxHeight: 400, whiteSpace: "pre-wrap", lineHeight: 1.5,
            }}>
              {guideText}
            </pre>
          </Section>
        )}

        <AbmLogSection campaignId={c.id} log={log} onAddLog={handleAddLog} busy={busy} />
      </>
    );
  };

  // ─── Campaigns List ──────────────────────────────────────────────

  return (
    <>
      <Flash flash={flash} />

      {view === "list" && (
        <Section title={`ABM CAMPAIGNS (${campaigns.length})`} actions={
          <div style={{ display: "flex", gap: 4 }}>
            <button onClick={loadCampaigns} disabled={loading}
              style={{ fontSize: 9, fontFamily: T.mono, padding: "2px 6px", border: `1px solid ${T.paper}44`, background: "transparent", color: T.paper, cursor: "pointer", opacity: loading ? 0.5 : 1 }}>
              {loading ? "..." : "Refresh"}
            </button>
            <button onClick={() => setView("create")}
              style={{ fontSize: 9, fontFamily: T.mono, padding: "2px 6px", border: `1px solid ${T.paper}44`, background: "transparent", color: T.paper, cursor: "pointer" }}>
              + New
            </button>
          </div>
        }>
          <div style={{ padding: "0 10px" }}>
            {campaigns.length === 0 && !loading && (
              <div style={{ color: T.inkFaint, fontSize: 12, lineHeight: 1.6, padding: "8px 0" }}>
                No ABM campaigns yet. Create one to start a personalized Comply experience for a target account.
              </div>
            )}
            {loading && campaigns.length === 0 && (
              <div style={{ color: T.inkFaint, fontSize: 12, padding: "8px 0" }}>Loading campaigns...</div>
            )}
            {campaigns.map(c => (
              <div key={c.id} onClick={() => openDetail(c.id)} style={{
                display: "flex", gap: 8, alignItems: "center", padding: "6px 0",
                borderBottom: `1px dotted ${T.ruleMid}`, cursor: "pointer",
              }}>
                <AbmStateBadge state={c.state} />
                <span style={{ fontFamily: T.mono, fontSize: 11, fontWeight: 600, flex: 1 }}>{c.org}</span>
                <span style={{ fontSize: 10, fontFamily: T.mono, color: T.inkLight }}>{icpTypes[c.icp_type]?.label || c.icp_type}</span>
                {c.person && <span style={{ fontSize: 10, color: T.inkFaint }}>{c.person}</span>}
                <span style={{ fontSize: 9, fontFamily: T.mono, color: T.inkFaint }}>
                  {c.updated_at ? new Date(c.updated_at).toLocaleDateString() : ""}
                </span>
              </div>
            ))}
          </div>
        </Section>
      )}

      {view === "create" && renderCreateWizard()}

      {view === "detail" && renderDetail()}
    </>
  );
}

function AbmLogSection({ campaignId, log, onAddLog, busy }) {
  const [note, setNote] = useState("");
  return (
    <Section title={`TOUCH LOG (${log.length})`}>
      <div style={{ padding: "8px 10px" }}>
        <div style={{ display: "flex", gap: 4, marginBottom: 8 }}>
          <input value={note} onChange={e => setNote(e.target.value)}
            onKeyDown={e => { if (e.key === "Enter" && note.trim()) { onAddLog(campaignId, note); setNote(""); } }}
            placeholder="Add log entry..."
            style={{ flex: 1, fontFamily: T.mono, fontSize: 11, padding: "4px 6px", border: `1px solid ${T.ruleMid}`, background: T.paper }} />
          <button onClick={() => { if (note.trim()) { onAddLog(campaignId, note); setNote(""); } }}
            disabled={!note.trim() || busy === "log"}
            style={btn({ opacity: note.trim() ? 1 : 0.5 })}>
            {busy === "log" ? "..." : "Add"}
          </button>
        </div>
        {log.length === 0 && <div style={{ fontSize: 11, color: T.inkFaint }}>No log entries yet.</div>}
        {log.map(l => (
          <div key={l.id} style={{ padding: "3px 0", borderBottom: `1px dotted ${T.ruleLight}`, display: "flex", gap: 8 }}>
            <span style={{ fontSize: 9, fontFamily: T.mono, color: T.inkFaint, minWidth: 80 }}>
              {new Date(l.created_at).toLocaleString()}
            </span>
            <span style={{ fontSize: 10, fontFamily: T.mono, color: T.inkMid }}>{l.note}</span>
          </div>
        ))}
      </div>
    </Section>
  );
}

function PlaygroundPanel() {
  const [portfolio, setPortfolio] = useState('[{"asset_symbol":"USDC","amount":500000},{"asset_symbol":"USDT","amount":300000},{"asset_symbol":"DAI","amount":200000}]');
  const [result, setResult] = useState(null);
  const [loading, setLoading] = useState(false);
  const [email, setEmail] = useState("");
  const [emailSent, setEmailSent] = useState(false);
  const [submissions, setSubmissions] = useState([]);
  const [flash, showFlash] = useFlash();

  const compute = async () => {
    setLoading(true);
    setResult(null);
    setEmailSent(false);
    try {
      let parsed;
      try { parsed = JSON.parse(portfolio); } catch { showFlash("Invalid JSON", false); setLoading(false); return; }
      const res = await opsFetch("/api/ops/playground/compute", {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ portfolio: parsed }),
      });
      setResult(res);
      loadSubmissions();
    } catch (e) { showFlash(e.message, false); }
    setLoading(false);
  };

  const requestReport = async () => {
    if (!result?.submission_id || !email) return;
    try {
      await opsFetch("/api/ops/playground/request-report", {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ submission_id: result.submission_id, email }),
      });
      setEmailSent(true);
      showFlash(`Report link sent to ${email}`, true);
    } catch (e) { showFlash(e.message, false); }
  };

  const loadSubmissions = async () => {
    try {
      const res = await opsFetch("/api/ops/playground/submissions?limit=20");
      setSubmissions(res.submissions || []);
    } catch {}
  };

  useEffect(() => { loadSubmissions(); }, []);

  const cqi = result?.cqi;
  const stress = result?.stress;

  return (
    <>
      <Section title="COMPOSITION PLAYGROUND">
        <Flash flash={flash} />
        <div style={{ padding: "0 10px" }}>
          <Lbl>Portfolio (JSON array)</Lbl>
          <textarea value={portfolio} onChange={e => setPortfolio(e.target.value)}
            style={{ width: "100%", height: 100, fontFamily: T.mono, fontSize: 11, padding: 8,
              border: `1px solid ${T.ruleMid}`, background: T.paper, resize: "vertical" }} />
          <div style={{ margin: "8px 0" }}>
            <button onClick={compute} disabled={loading}
              style={{ fontSize: 11, fontFamily: T.mono, padding: "4px 16px", border: `1px solid ${T.ink}`,
                background: T.ink, color: T.paper, cursor: "pointer", opacity: loading ? 0.5 : 1 }}>
              {loading ? "Computing..." : "Compute CQI + Stress"}
            </button>
          </div>
          <p style={{ fontSize: 9, color: T.inkFaint, fontFamily: T.mono }}>
            We'll send you one email with a link to your report. We won't use your email for anything else.
            Your portfolio data is retained for product analytics; to delete it, email shlok@basisprotocol.xyz.
          </p>

          {cqi && (
            <div style={{ marginTop: 12, borderTop: `1px solid ${T.ruleLight}`, paddingTop: 12 }}>
              <div style={{ display: "flex", gap: 16, flexWrap: "wrap", marginBottom: 8 }}>
                <div><Lbl>Aggregate CQI</Lbl><div style={{ fontSize: 24, fontFamily: T.mono, fontWeight: 700 }}>{cqi.aggregate_cqi?.toFixed(1)}</div></div>
                <div><Lbl>Grade</Lbl><div style={{ fontSize: 18, fontFamily: T.mono }}>{cqi.grade}</div></div>
                <div><Lbl>Positions</Lbl><div style={{ fontSize: 14, fontFamily: T.mono }}>{cqi.position_count}</div></div>
              </div>
              {stress && (
                <div style={{ marginBottom: 8 }}>
                  <Lbl>Stress Scenarios</Lbl>
                  <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
                    {stress.scenarios?.map((s, i) => (
                      <div key={i} style={{ fontSize: 10, fontFamily: T.mono, padding: "2px 8px",
                        background: s.pass ? "rgba(45,107,69,0.1)" : "rgba(192,57,43,0.1)",
                        color: s.pass ? "#2d6b45" : "#c0392b", borderRadius: 2 }}>
                        {s.name}: {s.pass ? "PASS" : "FAIL"} ({s.post_shock_cqi?.toFixed(1)})
                      </div>
                    ))}
                  </div>
                </div>
              )}
              {result?.preview_markdown && (
                <details style={{ marginTop: 8 }}>
                  <summary style={{ fontSize: 10, color: T.inkFaint, cursor: "pointer" }}>Basel SCO60 Preview</summary>
                  <pre style={{ fontSize: 10, fontFamily: T.mono, whiteSpace: "pre-wrap", marginTop: 4,
                    background: T.paperWarm, padding: 8, maxHeight: 300, overflow: "auto" }}>
                    {result.preview_markdown}
                  </pre>
                </details>
              )}
              {!emailSent && (
                <div style={{ marginTop: 12, display: "flex", gap: 8, alignItems: "center" }}>
                  <input type="email" placeholder="Email for full report" value={email}
                    onChange={e => setEmail(e.target.value)}
                    style={{ fontFamily: T.mono, fontSize: 11, padding: "4px 8px", border: `1px solid ${T.ruleMid}`,
                      background: T.paper, width: 250 }} />
                  <button onClick={requestReport} disabled={!email}
                    style={{ fontSize: 11, fontFamily: T.mono, padding: "4px 12px", border: `1px solid ${T.ink}`,
                      background: "transparent", cursor: "pointer" }}>
                    Get Full Report
                  </button>
                </div>
              )}
              {emailSent && <p style={{ fontSize: 11, color: "#2d6b45", fontFamily: T.mono, marginTop: 8 }}>Report link sent to {email}. Check your inbox.</p>}
            </div>
          )}
        </div>
      </Section>

      {submissions.length > 0 && (
        <Section title="RECENT SUBMISSIONS">
          <div style={{ padding: "0 10px" }}>
            <div style={{ display: "grid", gridTemplateColumns: "auto auto auto auto auto", gap: "2px 10px", fontSize: 10, fontFamily: T.mono }}>
              <span style={{ fontWeight: 600, color: T.inkLight }}>Time</span>
              <span style={{ fontWeight: 600, color: T.inkLight, textAlign: "right" }}>Positions</span>
              <span style={{ fontWeight: 600, color: T.inkLight, textAlign: "right" }}>CQI</span>
              <span style={{ fontWeight: 600, color: T.inkLight }}>Report</span>
              <span style={{ fontWeight: 600, color: T.inkLight, textAlign: "right" }}>Views</span>
              {submissions.map(s => (
                <div key={s.id} style={{ display: "contents" }}>
                  <span style={{ color: T.inkMid }}>{(s.submitted_at || "").slice(5, 16)}</span>
                  <span style={{ textAlign: "right" }}>{s.position_count}</span>
                  <span style={{ textAlign: "right" }}>{s.aggregate_cqi?.toFixed(1) || "—"}</span>
                  <span style={{ color: s.report_requested ? "#2d6b45" : T.inkFaint }}>{s.report_requested ? "sent" : "—"}</span>
                  <span style={{ textAlign: "right" }}>{s.access_count || 0}</span>
                </div>
              ))}
            </div>
          </div>
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
  const [tab, setTab] = useState("indices");
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

  const TAB_ITEMS = [
    { id: "indices", label: "Indices" },
    { id: "data", label: "Data Layer" },
    { id: "health", label: "Health" },
    { id: "overview", label: "Overview" },
    { id: "pipeline", label: "Pipeline" },
    { id: "metrics", label: "Metrics" },
    { id: "reports", label: "Reports" },
    { id: "tools", label: "Tools" },
    { id: "playground", label: "Playground" },
  ];

  return (
    <div style={{ minHeight: "100vh", background: T.paper, fontFamily: T.sans }}>
      <style>{`
        @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500;600;700&family=IBM+Plex+Sans:wght@400;500;600;700&display=swap');
        * { box-sizing: border-box; margin: 0; padding: 0; }
        html { background: ${T.paper}; }
        @keyframes fadeIn { from { opacity: 0; transform: translateY(4px); } to { opacity: 1; transform: translateY(0); } }
      `}</style>

      <div style={{ maxWidth: 1100, margin: "0 auto", padding: "32px 24px 0" }}>
        <div style={{
          border: `3px solid ${T.ink}`,
          boxShadow: `6px 6px 0 0 ${T.ruleMid}`,
          background: T.paper,
        }}>
          {/* Nav */}
          <div style={{ padding: "12px 24px", display: "flex", justifyContent: "space-between", alignItems: "center" }}>
            <nav style={{ display: "flex", gap: 16 }}>
              {TAB_ITEMS.map((tb) => (
                <button key={tb.id} onClick={() => setTab(tb.id)} style={{
                  padding: "4px 0", border: "none", cursor: "pointer",
                  fontSize: 12, fontWeight: tab === tb.id ? 600 : 400,
                  fontFamily: T.sans, color: tab === tb.id ? T.ink : T.inkLight,
                  background: "transparent",
                  borderBottom: tab === tb.id ? `2px solid ${T.ink}` : "2px solid transparent",
                }}>
                  {tb.label}
                </button>
              ))}
            </nav>
            <a href="/" style={{ fontFamily: T.mono, fontSize: 10, color: T.inkLight, textDecoration: "none" }}>SII Dashboard</a>
          </div>
          <div style={{ borderTop: `1px solid ${T.ruleLight}` }} />

          {/* Content */}
          <div style={{ padding: "0 24px 24px" }}>
            {error && (
              <div style={{ padding: "8px 12px", background: "#e74c3c22", border: "1px solid #e74c3c44", fontSize: 12, marginTop: 12, color: T.accent }}>
                {error}
                <button onClick={() => setError(null)} style={{ marginLeft: 8, border: "none", background: "transparent", cursor: "pointer", fontSize: 14 }}>&times;</button>
              </div>
            )}
            <Flash flash={flash} />

            {/* ═══ INDICES TAB ═══ */}
            {tab === "indices" && <IndicesPanel />}

            {/* ═══ DATA LAYER TAB ═══ */}
            {tab === "data" && (
              <div style={{ animation: "fadeIn 0.3s ease" }}>
                <TabHeader
                  title={<><span style={{ fontWeight: 700 }}>Data</span> Layer</>}
                  formId="FORM DL-001"
                  stats={["component coverage", "collector status", "data freshness"]}
                  accent="#0891b2"
                  mobile={false}
                />
                <StateGrowthPanel />
              </div>
            )}

            {/* ═══ HEALTH TAB ═══ */}
            {tab === "health" && (
              <div style={{ animation: "fadeIn 0.3s ease" }}>
                <TabHeader
                  title={<><span style={{ fontWeight: 700 }}>System</span> Health</>}
                  formId="FORM HL-001"
                  stats={[healthSummary, loading ? "loading..." : new Date().toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric" })]}
                  accent={warnings.length > 0 ? "#f39c12" : "#27ae60"}
                  mobile={false}
                />

                <Section title="SYSTEM HEALTH" actions={
                  <button onClick={handleRunHealthCheck} disabled={busy === "health"} style={btn({ opacity: busy === "health" ? 0.5 : 1 })}>
                    {busy === "health" ? "Checking..." : "Run Check"}
                  </button>
                }>
                  <div style={{ fontSize: 11, fontFamily: T.mono, color: T.inkMid, marginBottom: 8 }}>
                    {healthSummary}
                    {warnings.length > 0 && <span style={{ color: "#f39c12" }}> · {warnings.length} warning(s): {warnings.map((w) => w.system).join(", ")}</span>}
                  </div>
                  <HealthPanel health={health} />
                </Section>
              </div>
            )}

            {/* ═══ OVERVIEW TAB ═══ */}
            {tab === "overview" && (
              <div style={{ animation: "fadeIn 0.3s ease" }}>
                <TabHeader
                  title={<><span style={{ fontWeight: 700 }}>Operations</span> Hub</>}
                  formId="FORM OPS-001"
                  stats={[healthSummary, loading ? "loading..." : new Date().toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric" })]}
                  accent={warnings.length > 0 ? "#f39c12" : "#27ae60"}
                  mobile={false}
                  showOnChain={false}
                />

                <Section title={`ACTION QUEUE (${queue.length} items)`}>
                  <ActionQueue queue={queue} onDecide={handleDecide} decidingId={decidingId} />
                </Section>
              </div>
            )}

            {/* ═══ PIPELINE TAB ═══ */}
            {tab === "pipeline" && (
              <div style={{ animation: "fadeIn 0.3s ease" }}>
                <Section title="TIER 1 — ACTIVE PURSUIT">
                  <TargetTracker targets={targets.filter((t) => t.tier === 1)} onUpdate={load} />
                </Section>
                <Section title="TIER 2 — MONITORING">
                  <TargetTracker targets={targets.filter((t) => t.tier === 2)} onUpdate={load} />
                </Section>
                <Section title="TIER 3 — WATCH LIST">
                  <TargetTracker targets={targets.filter((t) => t.tier === 3)} onUpdate={load} />
                </Section>

                <Section title="CONTENT ITEMS">
                  {contentItems.length === 0 ? (
                    <div style={{ color: T.inkFaint, fontSize: 12, lineHeight: 1.6 }}>
                      No scheduled content items yet. Content items track planned posts —
                      forum comments, tweets, governance posts.
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
                </Section>
                <Section title="TARGET CONTENT FEED">
                  <ContentFeed feed={feed} onDecide={handleDecide} onAnalyze={handleFeedAnalyze} busy={busy} />
                </Section>
              </div>
            )}

            {/* ═══ METRICS TAB ═══ */}
            {tab === "metrics" && (
              <div style={{ animation: "fadeIn 0.3s ease" }}>
                <SeedMetricsPanel />
                <MetricsExpansionPanels />
              </div>
            )}

            {/* ═══ REPORTS TAB ═══ */}
            {tab === "reports" && (
              <div style={{ animation: "fadeIn 0.3s ease" }}>
                <ReportsPanel />
                <CqiMatrixPanel />
              </div>
            )}

            {/* ═══ TOOLS TAB ═══ */}
            {tab === "tools" && (
              <div style={{ animation: "fadeIn 0.3s ease" }}>
                <QueryPanel />
                <GraphPanel />
                <BacktestPanel />
                <ABMPanel />
                <TrackRecordPanel />
                <MethodologyPanel />
              </div>
            )}
            {tab === "playground" && (
              <div style={{ animation: "fadeIn 0.3s ease" }}>
                <PlaygroundPanel />
              </div>
            )}
          </div>

          {/* Footer */}
          <div style={{
            padding: "10px 24px", borderTop: `1px solid ${T.ruleLight}`,
            display: "flex", justifyContent: "space-between",
            fontFamily: T.mono, fontSize: 10, color: T.inkFaint,
          }}>
            <span>Basis Protocol · Operations Hub · Internal Use Only</span>
            <span>
              <a href="https://plausible.io/docs/excluding" style={{ color: T.inkLight, textDecoration: "none" }} target="_blank" rel="noopener">Exclude analytics</a>
            </span>
          </div>
        </div>
        <div style={{ height: 32 }} />
      </div>
    </div>
  );
}
