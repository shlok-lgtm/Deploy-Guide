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

// ─── Investor Row (accordion) ─────────────────────────────────────────

const INVESTOR_STAGES = [
  "not_started", "researching", "warm_intro_sent", "meeting_scheduled",
  "meeting_completed", "dd_in_progress", "term_sheet", "closed", "passed",
  "advisor_in_place",
];

function InvestorRow({ investor, onUpdate }) {
  const [open, setOpen] = useState(false);
  const [detail, setDetail] = useState(null);
  const [busy, setBusy] = useState(null);
  const [stageOpen, setStageOpen] = useState(false);
  const [intForm, setIntForm] = useState(null);
  const [flash, setFlash] = useState(null);

  const inv = investor;

  const showFlash = (msg, ok = true) => {
    setFlash({ msg, ok });
    setTimeout(() => setFlash(null), 5000);
  };

  const loadDetail = async () => {
    try {
      const data = await opsFetch(`/api/ops/investors/${inv.id}`);
      setDetail(data);
    } catch (e) { showFlash(e.message, false); }
  };

  const toggle = () => {
    const next = !open;
    setOpen(next);
    if (next && !detail) loadDetail();
  };

  const handleStageChange = async (newStage) => {
    setBusy("stage");
    try {
      await opsFetch(`/api/ops/investors/${inv.id}/stage`, {
        method: "PUT", body: JSON.stringify({ stage: newStage }),
      });
      showFlash(`Stage → ${newStage.replace(/_/g, " ")}`);
      setStageOpen(false);
      if (onUpdate) onUpdate();
      loadDetail();
    } catch (e) { showFlash(e.message, false); }
    setBusy(null);
  };

  const handleLogInteraction = async () => {
    if (!intForm?.action_type) return;
    setBusy("interaction");
    try {
      await opsFetch(`/api/ops/investors/${inv.id}/interaction`, {
        method: "POST", body: JSON.stringify(intForm),
      });
      showFlash("Interaction logged");
      setIntForm(null);
      loadDetail();
    } catch (e) { showFlash(e.message, false); }
    setBusy(null);
  };

  const interactions = detail?.interactions || [];
  const invDetail = detail?.investor || inv;

  const stageColors = {
    not_started: "#999", researching: "#3498db", warm_intro_sent: "#2980b9",
    meeting_scheduled: "#8e44ad", meeting_completed: "#9b59b6",
    dd_in_progress: "#f39c12", term_sheet: "#e67e22",
    closed: "#27ae60", passed: "#7f8c8d", advisor_in_place: "#27ae60",
  };

  return (
    <div style={{ borderBottom: `1px solid ${T.ruleLight}` }}>
      <div onClick={toggle} style={{
        padding: "7px 10px", fontSize: 11, fontFamily: T.mono, display: "flex", alignItems: "center", gap: 8,
        cursor: "pointer", background: open ? T.paperWarm : "transparent", transition: "background 0.15s",
      }}>
        <span style={{ fontSize: 10, color: T.inkFaint, width: 12 }}>{open ? "\u25BC" : "\u25B6"}</span>
        <TierBadge tier={inv.tier} />
        <div style={{ flex: 1, fontWeight: 500 }}>{inv.name}</div>
        {inv.firm && inv.firm !== inv.name && <span style={{ color: T.inkFaint, fontSize: 10 }}>{inv.firm}</span>}
        <span style={{
          fontSize: 10, padding: "2px 6px", borderRadius: 3,
          background: (stageColors[inv.stage] || "#999") + "22", color: stageColors[inv.stage] || "#999",
          border: `1px solid ${stageColors[inv.stage] || "#999"}44`,
        }}>
          {(inv.stage || "not started").replace(/_/g, " ")}
        </span>
        <div style={{ fontSize: 10, color: T.inkFaint, minWidth: 80, textAlign: "right" }}>
          {inv.next_action || "—"}
        </div>
      </div>

      {open && (
        <div style={{ padding: "0 10px 12px 30px", fontSize: 12, animation: "fadeIn 0.15s ease" }}>
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

          {/* Action bar */}
          <div style={{ display: "flex", flexWrap: "wrap", gap: 4, padding: "8px 0 6px", borderBottom: `1px solid ${T.ruleLight}`, marginBottom: 8 }}>
            <button onClick={() => setStageOpen(!stageOpen)} style={stageOpen ? btnActive() : btn()}>Update stage</button>
            <button onClick={() => setIntForm(intForm ? null : { action_type: "", content: "", next_step: "" })}
              style={intForm ? btnActive() : btn()}>Log interaction</button>
            <button onClick={loadDetail} style={btn()}>Refresh</button>
          </div>

          {/* Stage dropdown */}
          {stageOpen && (
            <div style={{ display: "flex", flexWrap: "wrap", gap: 3, padding: "4px 0 8px" }}>
              {INVESTOR_STAGES.map((s) => (
                <button key={s} onClick={() => handleStageChange(s)} disabled={s === inv.stage || !!busy}
                  style={btn({ opacity: s === inv.stage ? 0.4 : 1, background: s === inv.stage ? T.paperWarm : T.paper })}>
                  {s.replace(/_/g, " ")}
                </button>
              ))}
            </div>
          )}

          {/* Interaction form */}
          {intForm && (
            <div style={{ padding: "6px 0 8px", display: "flex", flexWrap: "wrap", gap: 6, alignItems: "flex-end" }}>
              <div>
                <Lbl>Action</Lbl>
                <select value={intForm.action_type} onChange={(e) => setIntForm({ ...intForm, action_type: e.target.value })}
                  style={{ display: "block", fontFamily: T.mono, fontSize: 11, padding: "4px 6px", border: `1px solid ${T.ruleMid}`, background: T.paper, marginTop: 2 }}>
                  <option value="">select...</option>
                  {["email_sent", "meeting", "materials_sent", "follow_up", "intro_request", "call", "dm_sent"].map((a) => (
                    <option key={a} value={a}>{a.replace(/_/g, " ")}</option>
                  ))}
                </select>
              </div>
              <div style={{ flex: 1, minWidth: 200 }}>
                <Lbl>Content</Lbl>
                <input value={intForm.content} onChange={(e) => setIntForm({ ...intForm, content: e.target.value })}
                  placeholder="What happened..."
                  style={{ display: "block", width: "100%", fontFamily: T.mono, fontSize: 11, padding: "4px 6px", border: `1px solid ${T.ruleMid}`, background: T.paper, marginTop: 2 }} />
              </div>
              <div style={{ minWidth: 160 }}>
                <Lbl>Next step</Lbl>
                <input value={intForm.next_step} onChange={(e) => setIntForm({ ...intForm, next_step: e.target.value })}
                  placeholder="Follow up with..."
                  style={{ display: "block", width: "100%", fontFamily: T.mono, fontSize: 11, padding: "4px 6px", border: `1px solid ${T.ruleMid}`, background: T.paper, marginTop: 2 }} />
              </div>
              <button onClick={handleLogInteraction} disabled={!intForm.action_type || !!busy}
                style={btn({ background: "#27ae6022" })}>
                {busy === "interaction" ? "..." : "Save"}
              </button>
            </div>
          )}

          {/* Investor details */}
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "4px 16px", marginBottom: 8 }}>
            {invDetail.key_person && <div><Lbl>Key person</Lbl><div style={{ fontSize: 11, color: T.inkMid, marginTop: 1 }}>{invDetail.key_person}</div></div>}
            {invDetail.warm_path && <div><Lbl>Warm path</Lbl><div style={{ fontSize: 11, color: T.inkMid, marginTop: 1 }}>{invDetail.warm_path}</div></div>}
            {invDetail.thesis_alignment && <div><Lbl>Thesis alignment</Lbl><div style={{ fontSize: 11, color: T.inkMid, marginTop: 1 }}>{invDetail.thesis_alignment}</div></div>}
            {invDetail.notes && <div><Lbl>Notes</Lbl><div style={{ fontSize: 11, color: T.inkMid, marginTop: 1 }}>{invDetail.notes}</div></div>}
          </div>

          {invDetail.materials_sent && invDetail.materials_sent.length > 0 && (
            <div style={{ marginBottom: 6 }}>
              <Lbl>Materials sent</Lbl>
              <div style={{ fontSize: 11, color: T.inkMid, marginTop: 1 }}>{invDetail.materials_sent.join(", ")}</div>
            </div>
          )}

          {/* Interaction history */}
          {interactions.length > 0 && (
            <div style={{ marginBottom: 8 }}>
              <Lbl>Interaction History ({interactions.length})</Lbl>
              <div style={{ marginTop: 3 }}>
                {interactions.map((i) => (
                  <div key={i.id} style={{ fontSize: 11, padding: "3px 0", borderBottom: `1px solid ${T.ruleLight}`, display: "flex", gap: 8 }}>
                    <span style={{ fontFamily: T.mono, fontSize: 10, color: T.inkLight, minWidth: 100 }}>
                      {i.action_type.replace(/_/g, " ")}
                    </span>
                    <span style={{ flex: 1, color: T.inkMid }}>{i.content || "—"}</span>
                    {i.response && <span style={{ color: "#27ae60", fontSize: 10 }}>response: {i.response}</span>}
                    {i.next_step && <span style={{ color: T.inkFaint, fontSize: 10 }}>next: {i.next_step}</span>}
                    <span style={{ fontSize: 9, color: T.inkFaint }}>{new Date(i.occurred_at).toLocaleDateString()}</span>
                  </div>
                ))}
              </div>
            </div>
          )}

          {interactions.length === 0 && (
            <div style={{ color: T.inkFaint, fontSize: 11, fontStyle: "italic", padding: "4px 0" }}>
              No interactions logged yet. Use "Log interaction" above to start tracking.
            </div>
          )}
        </div>
      )}
    </div>
  );
}

// ─── Fundraise Panel ──────────────────────────────────────────────────

function FundraisePanel({ data, onUpdate }) {
  if (!data) return null;
  const { investors, milestones: ms, raise: raiseInfo } = data;
  return (
    <div>
      {raiseInfo && (
        <div style={{ fontSize: 11, fontFamily: T.mono, color: T.inkMid, marginBottom: 10 }}>
          {raiseInfo.target} at {raiseInfo.valuation} · Target: {raiseInfo.timing}
        </div>
      )}
      {ms && (
        <div style={{ marginBottom: 12 }}>
          <div style={{ fontSize: 11, fontWeight: 600, marginBottom: 4 }}>
            Seed Triggers: {ms.met}/{ms.total} met (need {ms.threshold})
          </div>
          {(ms.milestones || []).map((m, i) => (
            <div key={i} style={{ fontSize: 11, fontFamily: T.mono, padding: "2px 0" }}>
              <span style={{ marginRight: 6 }}>{m.met ? "\u2705" : "\u274C"}</span>
              {m.name}
              {m.current != null && <span style={{ color: T.inkFaint }}> ({m.current}/{m.target})</span>}
            </div>
          ))}
        </div>
      )}
      <div style={{ fontSize: 11, fontWeight: 600, marginBottom: 4 }}>Investor Pipeline</div>
      {(investors || []).map((inv) => (
        <InvestorRow key={inv.id} investor={inv} onUpdate={onUpdate} />
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

// ─── Chain Expansion Panel ────────────────────────────────────────────

function ChainExpansionPanel() {
  const [candidates, setCandidates] = useState(null);
  const [loading, setLoading] = useState(false);
  const [expanding, setExpanding] = useState(false);
  const [specChain, setSpecChain] = useState(null);
  const [specContent, setSpecContent] = useState(null);
  const [flash, showFlash] = useFlash();

  const scan = async () => {
    setLoading(true);
    try {
      const res = await opsFetch("/api/ops/chain-candidates");
      setCandidates(res.candidates || []);
      showFlash(`Found ${res.count} chain candidate(s)`);
    } catch (e) { showFlash(e.message, false); }
    setLoading(false);
  };

  const expand = async () => {
    setExpanding(true);
    try {
      const res = await opsFetch("/api/ops/chain-expand", { method: "POST" });
      showFlash(`Generated ${res.specs_generated} spec(s) for ${(res.chains || []).join(", ")}`);
      scan();
    } catch (e) { showFlash(e.message, false); }
    setExpanding(false);
  };

  const viewSpec = async (chain) => {
    if (specChain === chain) { setSpecChain(null); setSpecContent(null); return; }
    try {
      const res = await opsFetch(`/api/ops/chain-spec/${chain}`);
      setSpecChain(chain);
      setSpecContent(res.spec);
    } catch (e) { showFlash(e.message, false); }
  };

  return (
    <Section title="CHAIN EXPANSION" actions={
      <div style={{ display: "flex", gap: 4 }}>
        <button onClick={expand} disabled={expanding} style={{ fontSize: 9, fontFamily: T.mono, padding: "2px 6px", border: `1px solid ${T.paper}44`, background: "transparent", color: T.paper, cursor: "pointer", opacity: expanding ? 0.5 : 1 }}>
          {expanding ? "Generating..." : "Generate Specs"}
        </button>
        <button onClick={scan} disabled={loading} style={{ fontSize: 9, fontFamily: T.mono, padding: "2px 6px", border: `1px solid ${T.paper}44`, background: "transparent", color: T.paper, cursor: "pointer", opacity: loading ? 0.5 : 1 }}>
          {loading ? "Scanning..." : "Scan Chains"}
        </button>
      </div>
    }>
      <Flash flash={flash} />
      <div style={{ padding: "0 10px" }}>
        {!candidates && <div style={{ color: T.inkFaint, fontSize: 12 }}>Click "Scan Chains" to discover expansion candidates.</div>}
        {candidates && candidates.length === 0 && <div style={{ color: T.inkFaint, fontSize: 12 }}>No chains above threshold.</div>}
        {candidates && candidates.length > 0 && (
          <div style={{ fontSize: 11 }}>
            {candidates.map((c) => (
              <div key={c.chain} style={{ padding: "6px 0", borderBottom: `1px solid ${T.ruleLight}` }}>
                <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
                  <span style={{ fontFamily: T.mono, fontWeight: 600, minWidth: 90 }}>{c.chain}</span>
                  <span style={{ fontFamily: T.mono, fontSize: 10, color: T.inkMid }}>
                    ${(c.stablecoin_tvl / 1e6).toFixed(0)}M TVL
                  </span>
                  <span style={{ fontSize: 10, color: T.inkLight }}>{c.protocol_count} protocols</span>
                  <span style={{ fontSize: 10, color: T.inkFaint }}>{(c.stablecoins || []).slice(0, 4).join(", ")}</span>
                  <span style={{ marginLeft: "auto" }}>
                    {c.spec_exists ? (
                      <button onClick={() => viewSpec(c.chain)} style={{ fontSize: 9, fontFamily: T.mono, padding: "1px 5px", border: `1px solid ${T.ruleMid}`, background: specChain === c.chain ? T.ink : T.paper, color: specChain === c.chain ? T.paper : T.ink, cursor: "pointer" }}>
                        {specChain === c.chain ? "Hide Spec" : "View Spec"}
                      </button>
                    ) : (
                      <span style={{ fontSize: 9, fontFamily: T.mono, color: T.inkFaint }}>no spec</span>
                    )}
                  </span>
                </div>
                {specChain === c.chain && specContent && (
                  <pre style={{ marginTop: 6, padding: 8, background: T.paperWarm, border: `1px solid ${T.ruleLight}`, fontSize: 10, fontFamily: T.mono, whiteSpace: "pre-wrap", maxHeight: 400, overflow: "auto" }}>
                    {specContent}
                  </pre>
                )}
              </div>
            ))}
          </div>
        )}
      </div>
    </Section>
  );
}

// ─── Discovery Panel (lazy-loaded) ────────────────────────────────────

function DiscoveryPanel() {
  const [signals, setSignals] = useState(null);
  const [loading, setLoading] = useState(false);
  const [flash, showFlash] = useFlash();

  useEffect(() => { scan(); }, []);

  const scan = async () => {
    setLoading(true);
    try {
      const res = await opsFetch("/api/ops/discovery/scan?limit=10");
      setSignals(res);
      showFlash(`Found ${(res.signals || []).length} discovery signals`);
    } catch (e) {
      showFlash(e.message, false);
    }
    setLoading(false);
  };

  return (
    <Section title="DISCOVERY SIGNALS" actions={
      <button onClick={scan} disabled={loading} style={{ fontSize: 9, fontFamily: T.mono, padding: "2px 6px", border: `1px solid ${T.paper}44`, background: "transparent", color: T.paper, cursor: "pointer", opacity: loading ? 0.5 : 1 }}>
        {loading ? "Scanning..." : "Scan"}
      </button>
    }>
      <Flash flash={flash} />
      <div style={{ padding: "0 10px" }}>
        {!signals && <div style={{ color: T.inkFaint, fontSize: 12 }}>Click "Scan" to query discovery signals.</div>}
        {signals && (
          <>
            <div style={{ fontSize: 11, fontFamily: T.mono, color: T.inkMid, marginBottom: 8 }}>{signals.summary}</div>
            {(signals.signals || []).map((sig) => (
              <div key={sig.signal_id} style={{ padding: "5px 0", borderBottom: `1px solid ${T.ruleLight}`, fontSize: 11 }}>
                <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
                  <span style={{ fontFamily: T.mono, fontSize: 10, color: T.inkLight, minWidth: 70 }}>{sig.domain}</span>
                  <span style={{ flex: 1, fontWeight: 500 }}>{sig.title || sig.signal_type}</span>
                  <span style={{ fontFamily: T.mono, fontSize: 10, color: sig.content_score >= 0.5 ? "#27ae60" : T.inkFaint }}>
                    score: {sig.content_score.toFixed(2)}
                  </span>
                  <span style={{ fontSize: 10, fontFamily: T.mono, color: T.inkLight }}>{sig.suggested_action.replace(/_/g, " ")}</span>
                </div>
                {sig.description && <div style={{ fontSize: 10, color: T.inkMid, marginTop: 2 }}>{sig.description}</div>}
                {sig.relevant_targets.length > 0 && (
                  <div style={{ fontSize: 10, color: T.inkFaint, marginTop: 2 }}>
                    Targets: {sig.relevant_targets.map((t) => t.name).join(", ")}
                  </div>
                )}
              </div>
            ))}
          </>
        )}
      </div>
    </Section>
  );
}

// ─── Milestones Panel (lazy-loaded) ───────────────────────────────────

function MilestonesPanel() {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(false);
  const [flash, showFlash] = useFlash();

  const check = async () => {
    setLoading(true);
    try {
      const res = await opsFetch("/api/ops/milestones");
      setData(res);
    } catch (e) {
      showFlash(e.message, false);
    }
    setLoading(false);
  };

  useEffect(() => { check(); }, []);

  const st = data?.seed_triggers;
  const ks = data?.kill_signals;
  const am = data?.adoption_metrics;

  return (
    <Section title="MILESTONES" actions={
      <button onClick={check} disabled={loading} style={{ fontSize: 9, fontFamily: T.mono, padding: "2px 6px", border: `1px solid ${T.paper}44`, background: "transparent", color: T.paper, cursor: "pointer", opacity: loading ? 0.5 : 1 }}>
        {loading ? "Checking..." : "Check"}
      </button>
    }>
      <Flash flash={flash} />
      <div style={{ padding: "0 10px" }}>
        {!data && loading && <div style={{ color: T.inkFaint, fontSize: 12 }}>Loading milestones...</div>}
        {!data && !loading && <div style={{ color: T.inkFaint, fontSize: 12 }}>Click "Refresh" to compute milestones from live data.</div>}
        {st && (
          <div style={{ marginBottom: 10 }}>
            <div style={{ fontSize: 11, fontWeight: 600, marginBottom: 4 }}>
              Seed Triggers: {st.met}/{st.total} met (need {st.threshold})
              {st.activated && <span style={{ color: "#27ae60", marginLeft: 8 }}>ACTIVATED</span>}
            </div>
            {st.milestones.map((m, i) => (
              <div key={i} style={{ fontSize: 11, fontFamily: T.mono, padding: "2px 0" }}>
                <span style={{ marginRight: 6 }}>{m.met ? "\u2705" : "\u274C"}</span>
                {m.name}
                {m.current != null && <span style={{ color: T.inkFaint }}> ({m.current}/{m.target})</span>}
              </div>
            ))}
          </div>
        )}
        {ks && (
          <div style={{ marginBottom: 10 }}>
            <div style={{ fontSize: 11, fontWeight: 600, marginBottom: 4 }}>Kill Signals</div>
            {ks.signals.map((s, i) => (
              <div key={i} style={{ fontSize: 11, fontFamily: T.mono, padding: "2px 0", color: s.status === "at_risk" ? T.accent : T.inkMid }}>
                {s.status === "safe" ? "\u26AA" : s.status === "at_risk" ? "\uD83D\uDD34" : "\u26AA"} {s.name}
                {s.evaluable && <span style={{ color: T.inkFaint, marginLeft: 8 }}>
                  API: {s.conditions.api_calls_daily || 0}/day
                </span>}
              </div>
            ))}
          </div>
        )}
        {am && (
          <div>
            <div style={{ fontSize: 11, fontWeight: 600, marginBottom: 4 }}>Adoption Metrics</div>
            {am.metrics.map((m, i) => (
              <div key={i} style={{ fontSize: 11, fontFamily: T.mono, padding: "2px 0", display: "flex", gap: 8 }}>
                <span style={{ flex: 1 }}>{m.name}</span>
                <span style={{ color: T.inkMid }}>{m.current}</span>
                <span style={{ color: T.inkFaint, fontSize: 10 }}>M6: {m.m6_target}</span>
                <span style={{ color: T.inkFaint, fontSize: 10 }}>M12: {m.m12_target}</span>
              </div>
            ))}
          </div>
        )}
      </div>
    </Section>
  );
}

// ─── Analytics Panel ──────────────────────────────────────────────────

function AnalyticsPanel() {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(false);
  const [flash, showFlash] = useFlash();

  const load = async () => {
    setLoading(true);
    try {
      const res = await opsFetch("/api/ops/analytics");
      setData(res);
    } catch (e) { showFlash(e.message, false); }
    setLoading(false);
  };

  useEffect(() => { load(); }, []);

  const eng = data?.engagement;
  const pipe = data?.pipeline;
  const cont = data?.content;
  const api = data?.api_usage;
  const isEmpty = eng && eng.total_engagements === 0 && pipe && pipe.active_targets === 0 && cont && cont.total_content === 0;

  return (
    <Section title="ANALYTICS" actions={
      <button onClick={load} disabled={loading} style={{ fontSize: 9, fontFamily: T.mono, padding: "2px 6px", border: `1px solid ${T.paper}44`, background: "transparent", color: T.paper, cursor: "pointer", opacity: loading ? 0.5 : 1 }}>
        {loading ? "Computing..." : "Refresh"}
      </button>
    }>
      <Flash flash={flash} />
      <div style={{ padding: "0 10px" }}>
        {!data && loading && <div style={{ color: T.inkFaint, fontSize: 12 }}>Loading analytics...</div>}
        {!data && !loading && <div style={{ color: T.inkFaint, fontSize: 12 }}>Click "Refresh" to generate analytics.</div>}
        {isEmpty && (
          <div style={{ color: T.inkFaint, fontSize: 12, lineHeight: 1.6, padding: "8px 0" }}>
            No engagement data yet. Analytics will populate as you log engagements and post content.
            Use the Targets tab to log engagement, scrape content, and move targets through the pipeline.
          </div>
        )}
        {eng && (
          <div style={{ marginBottom: 12 }}>
            <div style={{ fontSize: 11, fontWeight: 600, marginBottom: 4 }}>Engagement</div>
            <div style={{ fontSize: 11, fontFamily: T.mono, display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 8, marginBottom: 6 }}>
              <div>Total: <strong>{eng.total_engagements}</strong></div>
              <div>Responses: <strong>{eng.total_responses}</strong></div>
              <div>Rate: <strong>{eng.overall_response_rate}%</strong></div>
            </div>
            {eng.by_action_type.length > 0 && (
              <div style={{ fontSize: 10 }}>
                {eng.by_action_type.map((a, i) => (
                  <div key={i} style={{ fontFamily: T.mono, display: "flex", gap: 8, padding: "1px 0" }}>
                    <span style={{ minWidth: 120 }}>{a.action_type.replace(/_/g, " ")}</span>
                    <span>{a.total} sent</span>
                    <span style={{ color: a.response_rate > 0 ? "#27ae60" : T.inkFaint }}>{a.response_rate}% response</span>
                  </div>
                ))}
              </div>
            )}
          </div>
        )}
        {pipe && (
          <div style={{ marginBottom: 12 }}>
            <div style={{ fontSize: 11, fontWeight: 600, marginBottom: 4 }}>Pipeline</div>
            <div style={{ fontSize: 11, fontFamily: T.mono, marginBottom: 4 }}>Active targets: {pipe.active_targets}</div>
            {pipe.avg_days_to_first_engagement && (
              <div style={{ fontSize: 11, fontFamily: T.mono }}>Avg days to first engagement: {pipe.avg_days_to_first_engagement}</div>
            )}
            {pipe.overdue_actions.length > 0 && (
              <div style={{ marginTop: 4 }}>
                <div style={{ fontSize: 10, color: T.accent, fontWeight: 600 }}>Overdue Actions:</div>
                {pipe.overdue_actions.map((o) => (
                  <div key={o.id} style={{ fontSize: 10, fontFamily: T.mono, color: T.accent }}>
                    T{o.tier} {o.name}: {o.next_action} (due {new Date(o.next_action_due).toLocaleDateString()})
                  </div>
                ))}
              </div>
            )}
          </div>
        )}
        {cont && (
          <div style={{ marginBottom: 12 }}>
            <div style={{ fontSize: 11, fontWeight: 600, marginBottom: 4 }}>Content</div>
            <div style={{ fontSize: 10, fontFamily: T.mono, display: "grid", gridTemplateColumns: "1fr 1fr 1fr 1fr", gap: 4 }}>
              <div>Scraped: {cont.total_content}</div>
              <div>Analyzed: {cont.analyzed}</div>
              <div>Bridges: {cont.bridges_found} ({cont.bridge_rate}%)</div>
              <div>Approved: {cont.approved} ({cont.approval_rate}%)</div>
            </div>
          </div>
        )}
        {api && (
          <div>
            <div style={{ fontSize: 11, fontWeight: 600, marginBottom: 4 }}>API Usage</div>
            <div style={{ fontSize: 10, fontFamily: T.mono, display: "flex", gap: 12 }}>
              <span>Today: {api.requests.today}</span>
              <span>Week: {api.requests.week}</span>
              <span>Month: {api.requests.month}</span>
              <span>Active keys (7d): {api.active_api_keys_7d}</span>
            </div>
          </div>
        )}
      </div>
    </Section>
  );
}

// ─── News Panel ──────────────────────────────────────────────────────

function NewsPanel() {
  const [news, setNews] = useState(null);
  const [loading, setLoading] = useState(false);
  const [flash, showFlash] = useFlash();
  const [showAll, setShowAll] = useState(false);

  useEffect(() => { loadFeed(); }, []);

  const scan = async () => {
    setLoading(true);
    try {
      const scanResult = await opsFetch("/api/ops/news/scan", { method: "POST" });
      const feed = await opsFetch(`/api/ops/news/feed?limit=50&relevant_only=${!showAll}`);
      const items = feed.news || [];
      setNews(items);
      const incidents = items.filter((n) => n.incident_detected).length;
      showFlash(`Fetched ${scanResult.fetched || 0}, ${scanResult.stablecoin_relevant || 0} relevant${incidents ? `, ${incidents} incidents` : ""}`);
    } catch (e) { showFlash(e.message, false); }
    setLoading(false);
  };

  const loadFeed = async () => {
    setLoading(true);
    try {
      const feed = await opsFetch(`/api/ops/news/feed?limit=50&relevant_only=${!showAll}`);
      const items = feed.news || [];
      setNews(items);
      showFlash(`Loaded ${items.length} news items`);
    } catch (e) { showFlash(e.message, false); }
    setLoading(false);
  };

  const toggleShowAll = () => {
    setShowAll(!showAll);
    if (news) {
      // Reload with new filter
      setLoading(true);
      opsFetch(`/api/ops/news/feed?limit=50&relevant_only=${showAll}`)
        .then((feed) => { setNews(feed.news || []); setLoading(false); })
        .catch(() => setLoading(false));
    }
  };

  const relevant = (news || []).filter((n) => n.stablecoin_relevant);
  const headerCount = news ? `${relevant.length} relevant / ${news.length} total` : "not loaded";

  return (
    <Section title={`COINGECKO NEWS — ${headerCount}`} actions={
      <div style={{ display: "flex", gap: 4 }}>
        <button onClick={toggleShowAll}
          style={{ fontSize: 9, fontFamily: T.mono, padding: "2px 6px", border: `1px solid ${T.paper}44`,
            background: showAll ? T.paper + "44" : "transparent", color: T.paper, cursor: "pointer" }}>
          {showAll ? "Relevant Only" : "Show All"}
        </button>
        <button onClick={scan} disabled={loading}
          style={{ fontSize: 9, fontFamily: T.mono, padding: "2px 6px", border: `1px solid ${T.paper}44`,
            background: "transparent", color: T.paper, cursor: "pointer", opacity: loading ? 0.5 : 1 }}>
          {loading ? "Scanning..." : "Scan News"}
        </button>
      </div>
    }>
      <Flash flash={flash} />
      <div style={{ padding: "0 10px" }}>
        {!news && <div style={{ color: T.inkFaint, fontSize: 12 }}>Click "Scan News" to fetch stablecoin-related news.</div>}
        {news && news.length === 0 && <div style={{ color: T.inkFaint, fontSize: 12 }}>No news items found.</div>}
        {news && news.map((n) => (
          <div key={n.id} style={{ padding: "4px 0", borderBottom: `1px solid ${T.ruleLight}`, fontSize: 11,
            opacity: n.stablecoin_relevant ? 1 : 0.5 }}>
            <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
              {n.incident_detected && <span style={{ color: T.accent, fontWeight: 700, fontSize: 10 }}>INCIDENT</span>}
              {n.stablecoin_relevant && !n.incident_detected && (
                <span style={{ color: "#27ae60", fontWeight: 600, fontSize: 9 }}>RELEVANT</span>
              )}
              <a href={n.url} target="_blank" rel="noopener noreferrer"
                style={{ flex: 1, color: T.inkMid, textDecoration: "none", borderBottom: `1px solid ${T.ruleLight}` }}>
                {n.title}
              </a>
              <span style={{ fontSize: 9, color: T.inkFaint }}>{n.source}</span>
            </div>
            {n.relevant_symbols && n.relevant_symbols.length > 0 && (
              <div style={{ fontSize: 9, color: T.inkLight, fontFamily: T.mono, marginTop: 1 }}>
                {n.relevant_symbols.join(", ")}
              </div>
            )}
          </div>
        ))}
      </div>
    </Section>
  );
}

// ─── Alerts Panel ────────────────────────────────────────────────────

function AlertsPanel() {
  const [alerts, setAlerts] = useState(null);
  const [loading, setLoading] = useState(false);
  const [flash, showFlash] = useFlash();

  const load = async () => {
    setLoading(true);
    try {
      const res = await opsFetch("/api/ops/alerts?limit=20");
      const items = res.alerts || [];
      setAlerts(items);
      showFlash(`Loaded ${items.length} alerts`);
    } catch (e) { showFlash(e.message, false); }
    setLoading(false);
  };

  return (
    <Section title="ALERT LOG" actions={
      <button onClick={load} disabled={loading} style={{ fontSize: 9, fontFamily: T.mono, padding: "2px 6px", border: `1px solid ${T.paper}44`, background: "transparent", color: T.paper, cursor: "pointer", opacity: loading ? 0.5 : 1 }}>
        {loading ? "Loading..." : "Load"}
      </button>
    }>
      <Flash flash={flash} />
      <div style={{ padding: "0 10px" }}>
        {!alerts && <div style={{ color: T.inkFaint, fontSize: 12 }}>Click "Load" to view recent alerts.</div>}
        {alerts && alerts.length === 0 && <div style={{ color: T.inkFaint, fontSize: 12 }}>No alerts sent yet.</div>}
        {alerts && alerts.map((a) => (
          <div key={a.id} style={{ padding: "3px 0", borderBottom: `1px solid ${T.ruleLight}`, fontSize: 11, display: "flex", gap: 8 }}>
            <span style={{ fontFamily: T.mono, fontSize: 10, color: a.alert_type === "health_failure" ? T.accent : "#3498db", minWidth: 120 }}>
              {a.alert_type.replace(/_/g, " ")}
            </span>
            <span style={{ fontSize: 10, color: T.inkLight }}>{a.channel}</span>
            <span style={{ flex: 1, color: T.inkMid }}>{(a.message || "").replace(/\*/g, "").substring(0, 120)}</span>
            <span style={{ fontSize: 9, color: T.inkFaint }}>{new Date(a.sent_at).toLocaleString()}</span>
          </div>
        ))}
      </div>
    </Section>
  );
}

// ─── Twitter Panel ──────────────────────────────────────────────────

const SIGNAL_RE = /stablecoin|stable.?coin|risk|governance|treasury|curation|peg|depeg|reserve|audit|collateral/i;
function tweetIsSignal(tw) {
  const text = (tw.title || "") + " " + (tw.content || "") + " " + (tw.bridge_text || "");
  return SIGNAL_RE.test(text) || tw.bridge_found;
}

function TwitterPanel() {
  const [tweets, setTweets] = useState(null);
  const [loading, setLoading] = useState(false);
  const [flash, showFlash] = useFlash();
  const [showAll, setShowAll] = useState(false);
  const [draftingId, setDraftingId] = useState(null);
  const [drafts, setDrafts] = useState({});
  const [decidingId, setDecidingId] = useState(null);

  useEffect(() => { loadFeed(); }, []);

  const SEVEN_DAYS = 7 * 24 * 60 * 60 * 1000;
  const allTweets = tweets || [];

  const filtered = allTweets.filter((tw) =>
    showAll || ((tw.tier === 1) && (!tw.scraped_at || Date.now() - new Date(tw.scraped_at).getTime() < SEVEN_DAYS))
  );
  const sorted = [...filtered].sort((a, b) => {
    const diff = (tweetIsSignal(b) ? 1 : 0) - (tweetIsSignal(a) ? 1 : 0);
    if (diff !== 0) return diff;
    return new Date(b.scraped_at || 0) - new Date(a.scraped_at || 0);
  });
  const actionableCount = filtered.filter(tweetIsSignal).length;

  const scan = async () => {
    setLoading(true);
    try {
      await opsFetch("/api/ops/twitter/scan", { method: "POST" });
      const res = await opsFetch("/api/ops/twitter/feed?limit=100");
      const items = res.tweets || [];
      setTweets(items);
      const hl = items.filter(tweetIsSignal).length;
      showFlash(`Found ${items.length} tweets (${hl} actionable)`);
    } catch (e) { showFlash(e.message, false); }
    setLoading(false);
  };

  const loadFeed = async () => {
    setLoading(true);
    try {
      const res = await opsFetch("/api/ops/twitter/feed?limit=100");
      const items = res.tweets || [];
      setTweets(items);
      const hl = items.filter(tweetIsSignal).length;
      showFlash(`Loaded ${items.length} tweets (${hl} actionable)`);
    } catch (e) { showFlash(e.message, false); }
    setLoading(false);
  };

  const handleDraftReply = async (tw) => {
    if (tw.draft_comment && !drafts[tw.id]) {
      setDrafts((prev) => ({ ...prev, [tw.id]: { draft_comment: tw.draft_comment, comment_type: tw.comment_type } }));
      return;
    }
    setDraftingId(tw.id);
    try {
      const res = await opsFetch(`/api/ops/analyze/${tw.id}`, { method: "POST" });
      setDrafts((prev) => ({ ...prev, [tw.id]: res.analysis }));
      showFlash("Draft reply generated");
    } catch (e) { showFlash(e.message, false); }
    setDraftingId(null);
  };

  const handleDecideTweet = async (id, decision) => {
    setDecidingId(id);
    try {
      await opsFetch(`/api/ops/content/${id}/decide`, {
        method: "POST", body: JSON.stringify({ decision }),
      });
      setDrafts((prev) => { const n = { ...prev }; delete n[id]; return n; });
      showFlash(`Reply ${decision}`);
      await loadFeed();
    } catch (e) { showFlash(e.message, false); }
    setDecidingId(null);
  };

  const sectionBtn = (onClick, label, opts = {}) => (
    <button onClick={onClick} disabled={opts.disabled || loading}
      style={{ fontSize: 9, fontFamily: T.mono, padding: "2px 6px", border: `1px solid ${T.paper}44`,
        background: opts.active ? T.paper + "44" : "transparent", color: T.paper, cursor: "pointer",
        opacity: (opts.disabled || loading) ? 0.5 : 1 }}>
      {label}
    </button>
  );

  return (
    <Section title={`TWITTER — ${tweets ? `${actionableCount} actionable / ${filtered.length} shown` : "not loaded"}`} actions={
      <div style={{ display: "flex", gap: 4 }}>
        {sectionBtn(() => setShowAll(!showAll), showAll ? "T1 + 7d" : "Show All", { active: showAll, disabled: false })}
        {sectionBtn(loadFeed, loading ? "Loading..." : "Load")}
        {sectionBtn(scan, loading ? "Scanning..." : "Scan All")}
      </div>
    }>
      <Flash flash={flash} />
      <div style={{ padding: "0 10px" }}>
        {!tweets && <div style={{ color: T.inkFaint, fontSize: 12 }}>Click "Load" to view cached tweets or "Scan All" to fetch new ones.</div>}
        {tweets && sorted.length === 0 && <div style={{ color: T.inkFaint, fontSize: 12 }}>No tweets match current filter.</div>}
        {sorted.map((tw) => {
          const highlighted = tweetIsSignal(tw);
          const draft = drafts[tw.id];
          return (
            <div key={tw.id} style={{ padding: "5px 0", borderBottom: `1px solid ${T.ruleLight}`, fontSize: 11,
              background: highlighted ? "#f39c1208" : "transparent" }}>
              <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
                <TierBadge tier={tw.tier || 0} />
                <span style={{ fontFamily: T.mono, fontSize: 10, color: T.inkLight, minWidth: 90,
                  cursor: "pointer", textDecoration: "underline dotted", textUnderlineOffset: 2 }}
                  title="View in Target Tracker">{tw.target_name}</span>
                <a href={tw.source_url} target="_blank" rel="noopener noreferrer"
                  style={{ color: T.inkMid, textDecoration: "none", borderBottom: `1px solid ${T.ruleLight}`, fontSize: 10, flexShrink: 0 }}>
                  link
                </a>
                <span style={{ flex: 1, color: T.inkMid }}>
                  {tw.content && tw.content !== tw.source_url && tw.content !== tw.title
                    ? (tw.content.length > 140 ? tw.content.substring(0, 140) + "..." : tw.content)
                    : tw.title || tw.source_url}
                </span>
                {highlighted && !draft && !tw.founder_decision && (
                  <button onClick={() => handleDraftReply(tw)} disabled={!!draftingId}
                    style={btn({ background: "#3498db18", fontSize: 9, opacity: draftingId ? 0.5 : 1 })}>
                    {draftingId === tw.id ? "Drafting..." : "Draft Reply"}
                  </button>
                )}
                {tw.founder_decision && (
                  <span style={{ fontSize: 9, fontFamily: T.mono, color: T.inkFaint }}>{tw.founder_decision}</span>
                )}
                <span style={{ fontSize: 9, color: T.inkFaint, minWidth: 65, textAlign: "right" }}>
                  {tw.scraped_at ? new Date(tw.scraped_at).toLocaleDateString() : ""}
                </span>
              </div>
              {tw.content_summary && !draft && (
                <div style={{ fontSize: 10, color: T.inkLight, marginTop: 2, marginLeft: 28 }}>
                  {tw.content_summary}
                </div>
              )}
              {tw.bridge_found && !draft && (
                <div style={{ fontSize: 9, color: "#27ae60", fontFamily: T.mono, marginTop: 2, marginLeft: 28 }}>
                  BRIDGE: {tw.bridge_text?.substring(0, 100)}
                </div>
              )}
              {draft && (
                <div style={{ marginTop: 4, marginLeft: 28 }}>
                  <div style={{ background: T.paperWarm, padding: "6px 8px", fontSize: 11,
                    border: `1px solid ${T.ruleLight}`, whiteSpace: "pre-wrap" }}>
                    <div style={{ fontSize: 9, color: T.inkFaint, marginBottom: 2 }}>
                      Draft reply ({draft.comment_type || "comment"})
                    </div>
                    {draft.draft_comment}
                  </div>
                  <div style={{ display: "flex", gap: 4, marginTop: 4 }}>
                    <button onClick={() => handleDecideTweet(tw.id, "approved")} disabled={!!decidingId}
                      style={btn({ background: "#27ae6022" })}>{decidingId === tw.id ? "..." : "Approve"}</button>
                    <button onClick={() => handleDecideTweet(tw.id, "skipped")} disabled={!!decidingId}
                      style={btn()}>{decidingId === tw.id ? "..." : "Skip"}</button>
                  </div>
                </div>
              )}
            </div>
          );
        })}
      </div>
    </Section>
  );
}

// ─── Governance Panel ───────────────────────────────────────────────

function GovernancePanel({ targets }) {
  const [proposals, setProposals] = useState(null);
  const [loading, setLoading] = useState(false);
  const [flash, showFlash] = useFlash();
  const [showClosed, setShowClosed] = useState(false);
  const [showAllTiers, setShowAllTiers] = useState(false);
  const [draftingId, setDraftingId] = useState(null);
  const [drafts, setDrafts] = useState({});

  // Build tier lookup from targets prop
  const tierByName = {};
  (targets || []).forEach((t) => { tierByName[t.name] = t.tier; });

  useEffect(() => { loadFeed(); }, []);

  const isActive = (p) => {
    const s = (p.state || "").toLowerCase();
    return s === "active" || s === "open" || s === "pending";
  };

  const getRelevance = (p) => {
    const tier = tierByName[p.target_name] || 3;
    if (tier <= 1 && p.stablecoin_relevant) return "high";
    if ((tier <= 2 && p.stablecoin_relevant) || tier <= 1) return "medium";
    return "low";
  };

  const relevanceOrder = { high: 0, medium: 1, low: 2 };
  const relevanceColors = { high: "#27ae60", medium: "#f39c12", low: T.inkFaint };

  const allProposals = proposals || [];
  const filtered = allProposals.filter((p) => {
    if (!showClosed && !isActive(p)) return false;
    if (!showAllTiers) {
      const tier = tierByName[p.target_name];
      if (tier && tier > 2) return false;
    }
    return true;
  });
  const sorted = [...filtered].sort((a, b) => {
    return (relevanceOrder[getRelevance(a)] || 2) - (relevanceOrder[getRelevance(b)] || 2);
  });
  const actionableCount = sorted.filter((p) => p.stablecoin_relevant && isActive(p)).length;

  const stateColor = (state) => {
    if (!state) return T.inkFaint;
    const s = state.toLowerCase();
    if (s === "active" || s === "pending") return "#3498db";
    if (s === "closed" || s === "executed") return "#27ae60";
    return T.inkFaint;
  };

  const scan = async () => {
    setLoading(true);
    try {
      await opsFetch("/api/ops/governance/scan", { method: "POST" });
      await loadFeed(true);
    } catch (e) { showFlash(e.message, false); }
    setLoading(false);
  };

  const loadFeed = async (scanned = false) => {
    try {
      const res = await opsFetch("/api/ops/governance/feed?limit=60");
      const items = res.proposals || [];
      setProposals(items);
      const stableCount = items.filter((p) => p.stablecoin_relevant).length;
      showFlash(scanned
        ? `${items.length} proposals scanned (${stableCount} stablecoin-relevant)`
        : `Loaded ${items.length} proposals`);
    } catch (e) { showFlash(e.message, false); }
  };

  const handleDraftComment = async (p) => {
    setDraftingId(p.id);
    try {
      const res = await opsFetch("/api/ops/draft/forum", {
        method: "POST",
        body: JSON.stringify({ forum: (p.platform || "").toLowerCase(), topic: p.title }),
      });
      setDrafts((prev) => ({ ...prev, [p.id]: res.draft }));
      showFlash("Draft comment generated");
    } catch (e) { showFlash(e.message, false); }
    setDraftingId(null);
  };

  const sectionBtn = (onClick, label, opts = {}) => (
    <button onClick={onClick} disabled={opts.disabled || loading}
      style={{ fontSize: 9, fontFamily: T.mono, padding: "2px 6px", border: `1px solid ${T.paper}44`,
        background: opts.active ? T.paper + "44" : "transparent", color: T.paper, cursor: "pointer",
        opacity: (opts.disabled || loading) ? 0.5 : 1 }}>
      {label}
    </button>
  );

  return (
    <Section title={`GOVERNANCE — ${proposals ? `${actionableCount} actionable / ${sorted.length} shown` : "not loaded"}`} actions={
      <div style={{ display: "flex", gap: 4 }}>
        {sectionBtn(() => setShowClosed(!showClosed), showClosed ? "Hide Closed" : "Show Closed", { active: showClosed, disabled: false })}
        {sectionBtn(() => setShowAllTiers(!showAllTiers), showAllTiers ? "T1+T2 Only" : "All Tiers", { active: showAllTiers, disabled: false })}
        {sectionBtn(() => { if (!proposals) loadFeed(); else scan(); },
          loading ? "Scanning..." : proposals ? "Scan" : "Load")}
      </div>
    }>
      <Flash flash={flash} />
      <div style={{ padding: "0 10px" }}>
        {!proposals && <div style={{ color: T.inkFaint, fontSize: 12 }}>Click "Load" to view governance proposals or "Scan" to fetch new ones.</div>}
        {proposals && sorted.length === 0 && <div style={{ color: T.inkFaint, fontSize: 12 }}>No proposals match current filters.</div>}
        {sorted.map((p) => {
          const rel = getRelevance(p);
          const draft = drafts[p.id];
          return (
            <div key={p.id} style={{ padding: "5px 0", borderBottom: `1px solid ${T.ruleLight}`, fontSize: 11,
              background: rel === "high" ? "#27ae6008" : "transparent" }}>
              <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
                <span style={{ fontFamily: T.mono, fontSize: 8, fontWeight: 700, minWidth: 32, textAlign: "center",
                  color: relevanceColors[rel] }}>{rel.toUpperCase()}</span>
                <span style={{ fontFamily: T.mono, fontSize: 9, color: T.inkFaint, minWidth: 50 }}>{p.platform}</span>
                <span style={{ fontFamily: T.mono, fontSize: 10, color: T.inkLight, minWidth: 90,
                  cursor: "pointer", textDecoration: "underline dotted", textUnderlineOffset: 2 }}
                  title="View in Target Tracker">{p.target_name}</span>
                <span style={{ flex: 1, color: T.inkMid }}>{p.title}</span>
                <span style={{ fontFamily: T.mono, fontSize: 9, color: stateColor(p.state), fontWeight: 600 }}>{p.state}</span>
                {p.stablecoin_relevant && isActive(p) && !draft && (
                  <button onClick={() => handleDraftComment(p)} disabled={!!draftingId}
                    style={btn({ background: "#8e44ad18", fontSize: 9, opacity: draftingId ? 0.5 : 1 })}>
                    {draftingId === p.id ? "Drafting..." : "Draft Comment"}
                  </button>
                )}
                {p.votes_count > 0 && <span style={{ fontSize: 9, color: T.inkFaint }}>{p.votes_count} votes</span>}
              </div>
              {p.stablecoin_relevant && (
                <div style={{ fontSize: 9, marginTop: 1, marginLeft: 40 }}>
                  <span style={{ color: "#27ae60", fontFamily: T.mono, fontWeight: 600 }}>STABLECOIN</span>
                  {p.relevant_coins && p.relevant_coins.length > 0 && (
                    <span style={{ color: T.inkLight, marginLeft: 6 }}>{p.relevant_coins.join(", ")}</span>
                  )}
                </div>
              )}
              {p.space_or_org && !draft && (
                <div style={{ fontSize: 9, color: T.inkFaint, marginLeft: 40 }}>{p.space_or_org}</div>
              )}
              {draft && (
                <div style={{ marginTop: 4, marginLeft: 40 }}>
                  <div style={{ background: T.paperWarm, padding: "6px 8px", fontSize: 11,
                    border: `1px solid ${T.ruleLight}` }}>
                    <div style={{ fontSize: 9, color: T.inkFaint, marginBottom: 2 }}>
                      Draft comment — {draft.title || "forum post"}
                    </div>
                    {draft.tldr && <div style={{ fontWeight: 500, marginBottom: 4 }}>{draft.tldr}</div>}
                    <div style={{ whiteSpace: "pre-wrap", fontSize: 10, maxHeight: 200, overflow: "auto" }}>{draft.body}</div>
                    {draft.tags && draft.tags.length > 0 && (
                      <div style={{ fontSize: 9, color: T.inkFaint, marginTop: 4 }}>tags: {draft.tags.join(", ")}</div>
                    )}
                  </div>
                  <div style={{ display: "flex", gap: 4, marginTop: 4 }}>
                    <button onClick={() => { navigator.clipboard.writeText(draft.body || ""); showFlash("Copied to clipboard"); }}
                      style={btn({ background: "#27ae6022" })}>Copy</button>
                    <button onClick={() => setDrafts((prev) => { const n = { ...prev }; delete n[p.id]; return n; })}
                      style={btn()}>Dismiss</button>
                  </div>
                </div>
              )}
            </div>
          );
        })}
      </div>
    </Section>
  );
}

// ─── Investor Content Panel ─────────────────────────────────────────

function InvestorContentPanel() {
  const [content, setContent] = useState(null);
  const [signals, setSignals] = useState(null);
  const [loading, setLoading] = useState(false);
  const [flash, showFlash] = useFlash();
  const [showAll, setShowAll] = useState(true);
  const [draftingId, setDraftingId] = useState(null);
  const [drafts, setDrafts] = useState({});

  useEffect(() => { loadFeed(); }, []);

  const allContent = content || [];
  const filtered = allContent.filter((c) =>
    showAll || (c.analyzed && c.alignment_score !== null && c.alignment_score > 0.5)
  );
  const sorted = [...filtered].sort((a, b) => {
    // Timing signals first
    const aTiming = a.timing_signal ? 1 : 0;
    const bTiming = b.timing_signal ? 1 : 0;
    if (aTiming !== bTiming) return bTiming - aTiming;
    // Then by alignment score
    return (b.alignment_score || 0) - (a.alignment_score || 0);
  });
  const actionableCount = sorted.filter((c) => (c.alignment_score > 0.5) || c.timing_signal).length;

  const scan = async () => {
    setLoading(true);
    try {
      await opsFetch("/api/ops/investors/content/scan", { method: "POST" });
      await loadFeed(true);
    } catch (e) { showFlash(e.message, false); }
    setLoading(false);
  };

  const loadFeed = async (scanned = false) => {
    try {
      const [feed, sigs] = await Promise.all([
        opsFetch("/api/ops/investors/content/feed?limit=50"),
        opsFetch("/api/ops/investors/content/signals?limit=10"),
      ]);
      const items = feed.content || [];
      const sigItems = sigs.signals || [];
      setContent(items);
      setSignals(sigItems);
      const highAlign = items.filter((c) => c.alignment_score > 0.5).length;
      showFlash(scanned
        ? `Scanned ${items.length} items (${highAlign} high-alignment, ${sigItems.length} timing signals)`
        : `Loaded ${items.length} items (${highAlign} high-alignment)`);
    } catch (e) { showFlash(e.message, false); }
  };

  const handleDraftOutreach = async (c) => {
    // If already analyzed with outreach angle, show it immediately
    if (c.analyzed && c.outreach_angle && !drafts[c.id]) {
      setDrafts((prev) => ({ ...prev, [c.id]: { outreach_angle: c.outreach_angle, timing_notes: c.timing_notes, alignment_notes: c.alignment_notes } }));
      return;
    }
    // Otherwise trigger analysis
    setDraftingId(c.id);
    try {
      const res = await opsFetch(`/api/ops/investors/content/${c.id}/analyze`, { method: "POST" });
      const a = res.analysis || {};
      setDrafts((prev) => ({ ...prev, [c.id]: { outreach_angle: a.outreach_angle, timing_notes: a.timing_notes, alignment_notes: a.alignment_notes } }));
      showFlash("Outreach draft generated");
      await loadFeed();
    } catch (e) { showFlash(e.message, false); }
    setDraftingId(null);
  };

  const alignmentBar = (score) => {
    if (score === null || score === undefined) return null;
    const pct = Math.round(score * 100);
    const color = pct >= 70 ? "#27ae60" : pct >= 50 ? "#f39c12" : T.inkFaint;
    return (
      <div style={{ display: "flex", alignItems: "center", gap: 4, minWidth: 70 }}>
        <div style={{ width: 40, height: 4, background: T.ruleLight, borderRadius: 2, overflow: "hidden" }}>
          <div style={{ width: `${pct}%`, height: "100%", background: color, borderRadius: 2 }} />
        </div>
        <span style={{ fontFamily: T.mono, fontSize: 9, color, fontWeight: 600 }}>{pct}%</span>
      </div>
    );
  };

  const sectionBtn = (onClick, label, opts = {}) => (
    <button onClick={onClick} disabled={opts.disabled || loading}
      style={{ fontSize: 9, fontFamily: T.mono, padding: "2px 6px", border: `1px solid ${T.paper}44`,
        background: opts.active ? T.paper + "44" : "transparent", color: T.paper, cursor: "pointer",
        opacity: (opts.disabled || loading) ? 0.5 : 1 }}>
      {label}
    </button>
  );

  return (
    <>
      {signals && signals.length > 0 && (
        <Section title={`TIMING SIGNALS (${signals.length})`}>
          <div style={{ padding: "0 10px" }}>
            {signals.map((s) => (
              <div key={s.id} style={{ padding: "6px 0", borderBottom: `1px solid ${T.ruleLight}`, fontSize: 11,
                background: "#e74c3c08" }}>
                <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
                  <span style={{ fontFamily: T.mono, fontSize: 10, color: "#e74c3c", fontWeight: 700 }}>TIMING</span>
                  <span style={{ fontWeight: 600 }}>{s.investor_name}</span>
                  <span style={{ color: T.inkLight, flex: 1 }}>{s.title || s.source_type}</span>
                  {s.alignment_score && alignmentBar(s.alignment_score)}
                </div>
                {s.timing_notes && <div style={{ fontSize: 10, color: T.inkMid, marginTop: 2, marginLeft: 8 }}>{s.timing_notes}</div>}
                {s.outreach_angle && <div style={{ fontSize: 10, color: "#3498db", marginTop: 1, marginLeft: 8 }}>Angle: {s.outreach_angle.substring(0, 150)}</div>}
              </div>
            ))}
          </div>
        </Section>
      )}

      <Section title={`INVESTOR CONTENT — ${content ? `${actionableCount} actionable / ${sorted.length} shown` : "not loaded"}`} actions={
        <div style={{ display: "flex", gap: 4 }}>
          {sectionBtn(() => setShowAll(!showAll), showAll ? "High Alignment" : "Show All", { active: showAll, disabled: false })}
          {sectionBtn(() => loadFeed(), loading ? "Loading..." : "Load")}
          {sectionBtn(scan, loading ? "Scanning..." : "Scan All")}
        </div>
      }>
        <Flash flash={flash} />
        <div style={{ padding: "0 10px" }}>
          {!content && <div style={{ color: T.inkFaint, fontSize: 12 }}>Click "Load" to view investor content or "Scan All" to fetch new.</div>}
          {content && sorted.length === 0 && <div style={{ color: T.inkFaint, fontSize: 12 }}>No content matches current filter. Try "Show All".</div>}
          {sorted.map((c) => {
            const draft = drafts[c.id];
            const hasOutreach = c.analyzed && c.alignment_score > 0.5;
            return (
              <div key={c.id} style={{ padding: "5px 0", borderBottom: `1px solid ${T.ruleLight}`, fontSize: 11,
                background: c.timing_signal ? "#e74c3c08" : "transparent" }}>
                <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
                  {c.timing_signal && <span style={{ fontSize: 9, color: "#e74c3c", fontWeight: 700 }}>TIMING</span>}
                  <span style={{ fontWeight: 500, minWidth: 90 }}>{c.investor_name}</span>
                  <span style={{ fontFamily: T.mono, fontSize: 10, color: T.inkLight, minWidth: 60 }}>{c.source_type}</span>
                  <a href={c.source_url} target="_blank" rel="noopener noreferrer"
                    style={{ flex: 1, color: T.inkMid, textDecoration: "none", borderBottom: `1px solid ${T.ruleLight}` }}>
                    {c.title || c.source_url?.substring(0, 80)}
                  </a>
                  {c.analyzed && alignmentBar(c.alignment_score)}
                  {hasOutreach && !draft && (
                    <button onClick={() => handleDraftOutreach(c)} disabled={!!draftingId}
                      style={btn({ background: "#3498db18", fontSize: 9, opacity: draftingId ? 0.5 : 1 })}>
                      {draftingId === c.id ? "Drafting..." : "Draft Outreach"}
                    </button>
                  )}
                  {!c.analyzed && (
                    <button onClick={() => handleDraftOutreach(c)} disabled={!!draftingId}
                      style={{ fontSize: 9, fontFamily: T.mono, padding: "1px 4px", border: `1px solid ${T.inkFaint}`,
                        background: "transparent", color: T.inkLight, cursor: "pointer", opacity: draftingId ? 0.5 : 1 }}>
                      {draftingId === c.id ? "Analyzing..." : "Analyze"}
                    </button>
                  )}
                </div>
                {c.thesis_extract && !draft && (
                  <div style={{ fontSize: 10, color: T.inkLight, marginTop: 1, marginLeft: 8 }}>{c.thesis_extract}</div>
                )}
                {draft && (
                  <div style={{ marginTop: 4, marginLeft: 8 }}>
                    <div style={{ background: T.paperWarm, padding: "6px 8px", fontSize: 11,
                      border: `1px solid ${T.ruleLight}` }}>
                      <div style={{ fontSize: 9, color: T.inkFaint, marginBottom: 2 }}>Outreach angle</div>
                      <div style={{ whiteSpace: "pre-wrap" }}>{draft.outreach_angle || "No outreach angle generated"}</div>
                      {draft.timing_notes && (
                        <div style={{ marginTop: 4, fontSize: 10, color: "#e74c3c" }}>Timing: {draft.timing_notes}</div>
                      )}
                      {draft.alignment_notes && (
                        <div style={{ marginTop: 2, fontSize: 10, color: T.inkLight }}>{draft.alignment_notes}</div>
                      )}
                    </div>
                    <div style={{ display: "flex", gap: 4, marginTop: 4 }}>
                      <button onClick={() => { navigator.clipboard.writeText(draft.outreach_angle || ""); showFlash("Copied to clipboard"); }}
                        style={btn({ background: "#27ae6022" })}>Copy</button>
                      <button onClick={() => setDrafts((prev) => { const n = { ...prev }; delete n[c.id]; return n; })}
                        style={btn()}>Dismiss</button>
                    </div>
                  </div>
                )}
              </div>
            );
          })}
        </div>
      </Section>
    </>
  );
}

// ─── Main Dashboard ───────────────────────────────────────────────────

export default function OpsDashboard() {
  const [authed, setAuthed] = useState(!!getAdminKey());
  const [health, setHealth] = useState([]);
  const [queue, setQueue] = useState([]);
  const [targets, setTargets] = useState([]);
  const [fundraise, setFundraise] = useState(null);
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
      const [h, q, t, f, cf, ci] = await Promise.all([
        opsFetch("/api/ops/health").catch(() => ({ health: [] })),
        opsFetch("/api/ops/queue").catch(() => ({ queue: [] })),
        opsFetch("/api/ops/targets").catch(() => ({ targets: [] })),
        opsFetch("/api/ops/fundraise/dashboard").catch(() => null),
        opsFetch("/api/ops/content/feed?limit=30").catch(() => ({ feed: [] })),
        opsFetch("/api/ops/content/items").catch(() => ({ items: [] })),
      ]);
      setHealth(h.health || []);
      setQueue(q.queue || []);
      setTargets(t.targets || []);
      setFundraise(f);
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

  const handleSeed = async () => {
    setBusy("seed");
    try {
      await opsFetch("/api/ops/seed", { method: "POST" });
      showFlash("Seed complete");
      load();
    } catch (e) { showFlash(e.message, false); }
    setBusy(null);
  };

  const handleMigrate = async () => {
    setBusy("migrate");
    try {
      await opsFetch("/api/ops/migrate", { method: "POST" });
      // Also run migration 033 if available
      try { await opsFetch("/api/ops/migrate/033", { method: "POST" }); } catch (_) {}
      showFlash("Migration complete");
      load();
    } catch (e) { showFlash(e.message, false); }
    setBusy(null);
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
            <button onClick={handleMigrate} disabled={!!busy} style={btn({ opacity: busy === "migrate" ? 0.5 : 1 })}>
              {busy === "migrate" ? "Migrating..." : "Migrate"}
            </button>
            <button onClick={handleSeed} disabled={!!busy} style={btn({ opacity: busy === "seed" ? 0.5 : 1 })}>
              {busy === "seed" ? "Seeding..." : "Seed"}
            </button>
            <button onClick={load} disabled={loading} style={btn({ opacity: loading ? 0.5 : 1 })}>
              {loading ? "Loading..." : "Refresh"}
            </button>
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
          {["dashboard", "targets", "fundraise", "content", "signals", "analytics"].map((tb) => (
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

            <Section title={`TARGET TRACKER (${targets.filter((t) => t.tier <= 2).length} active)`}>
              <TargetTracker targets={targets.filter((t) => t.tier <= 2)} onUpdate={load} />
            </Section>

            <Section title="RECENT TARGET CONTENT">
              <div style={{ padding: "0 10px" }}><ContentFeed feed={feed} onDecide={handleDecide} onAnalyze={handleFeedAnalyze} busy={busy} /></div>
            </Section>

            <DiscoveryPanel />
            <ChainExpansionPanel />
            <MilestonesPanel />
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

        {/* Fundraise tab */}
        {tab === "fundraise" && (
          <Section title="FUNDRAISE PIPELINE">
            <div style={{ padding: "0 10px" }}><FundraisePanel data={fundraise} onUpdate={load} /></div>
          </Section>
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

        {/* Signals tab — Twitter, Governance, Investor Content */}
        {tab === "signals" && (
          <>
            <TwitterPanel />
            <GovernancePanel targets={targets} />
            <InvestorContentPanel />
          </>
        )}

        {/* Analytics tab */}
        {tab === "analytics" && (
          <>
            <AnalyticsPanel />
            <NewsPanel />
            <AlertsPanel />
          </>
        )}

        <div style={{ fontFamily: T.mono, fontSize: 10, color: T.inkFaint, textAlign: "center", marginTop: 24, paddingBottom: 16 }}>
          Basis Protocol · Operations Hub · Internal Use Only
        </div>
      </div>
    </div>
  );
}
