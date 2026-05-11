# Canon Update Proposal — 2026-05-10 Neon Migration

**Purpose:** drafts of the changes that need to land in `basis-protocol/canon`
to reflect post-cutover reality. My GitHub MCP is restricted to
`basis-protocol/basis-hub`, so I cannot push these to canon directly —
copy them over manually. Tag the canon commit `transition-2026-05-10`.

---

## 1. PROJECT_INSTRUCTIONS — proposed diffs

These are the deltas the user's Track F prompt called out. Numbers are
verified against the live Railway project (`valiant-celebration`) and the
new Neon project (`small-scene-57890564`) as of 2026-05-11 02:00 UTC.

### Services

- Service count: **13** (was: documented as "5 services"). Live list:
  api-server, Scoring-Worker, Keeper, basis-state, basis-provenance,
  basis-backfill-{tti, cxri, vsri, dohi, bri, lsti, rpi, psi}.
- 4 of the backfills (tti, dohi, bri, rpi) currently in REMOVED status,
  `restart_policy: NEVER` — intentionally paused one-shot jobs that
  completed Apr 22. They count toward "services exist" but not toward
  "services live".
- Service naming: the spoke that handles provenance is named
  **`basis-provenance`** (deploys from `basis-protocol/basis-provenance`,
  Dockerfile `/Dockerfile.prover`). If the canon doc currently calls it
  "prover", rename to `basis-provenance`.

### Repos

- `basis-state` and `basis-provenance` are **separate repos** with their
  own main branches. They are pure spokes — they talk to the hub via the
  API and do not have `DATABASE_URL` env vars. They were unaffected by
  the libpq-options bug directly; they were broken transitively because
  the api-server was down.

### Database

Add a new line / section:

> **Database:** Neon Postgres, owned directly by Basis
> (org `org-aged-brook-05137723`, project `BasisProtocol` /
> `small-scene-57890564`), pg17, region `aws-us-east-2`. Migrated
> 2026-05-10 from the Replit-managed Neon integration.

### Replit decommission

Keep "in progress" until 2026-05-17, then flip to "complete".
basis-hub PR #131 (merged 2026-05-11) deleted the last 5 Replit-only
files from the repo (`.replit`, `replit.md`, `pyproject.toml`,
`uv.lock`, `.streamlit/config.toml`) and updated 12 docs.

### Dev environment

The Track C audit's stated goal was "GitHub Codespaces once Track C and
Codespaces setup are both complete". Track C is done; Codespaces setup
has **not** been verified by me. Keep "transition pending" until
someone confirms a fresh Codespace boots cleanly with the new
`.env.example`. Per canon-discipline rules, don't write "Codespaces
ready" until it literally is.

---

## 2. Constitution amendment v9.9

Drafted in `basis-protocol/basis-hub` at:
`docs/basis_protocol_v9_9_constitution_amendment.md`

Summary: documents the pooled-connection contract — what code can
assume about a `get_conn()` checkout, which Postgres features are
off-limits in transaction-mode pooling, and when to use the direct
(unpooled) endpoint. References the 2026-05-10 incident as the
forcing function.

**To copy:** read
`basis-protocol/basis-hub/docs/basis_protocol_v9_9_constitution_amendment.md`
and commit as
`basis-protocol/canon/<wherever-amendments-live>/basis_protocol_v9_9_constitution_amendment.md`.
The file is self-contained.

---

## 3. Punchlist entry

Drafted in `basis-protocol/basis-hub` at:
`docs/punchlist_2026-05-10_neon_incident.md`

Includes date, root cause, all 6 tracks (A–F) and their status, the
two open hardening PRs (#132, #133), and the manual follow-ups (Neon
console retention + autosuspend, canon copy, Claude.ai project-knowledge
re-upload). Self-contained.

---

## 4. Open caveat for the canon writer

Per canon-discipline rules (not aspirational):

- Don't write "Codespaces complete" until verified.
- Don't write "Replit decommissioned" until 2026-05-17 has passed.
- Don't write "Wave-2 complete" until PR #133 is merged.
- Don't write "deploy-safety complete" until PR #132 is merged.

These are tracked in the punchlist entry; flip them there when each
lands so the canon doc and the punchlist don't drift.

---

## Why this is a proposal not a push

The harness running this session restricts my GitHub MCP to
`basis-protocol/basis-hub`. I cannot create branches, commits, or PRs
on `basis-protocol/canon`. The three files above are the canon-bound
deliverables, staged in the hub for human-mediated copy.

If you want me to push directly to canon in a future session, grant
the harness the canon repo as well and re-run Track F. Otherwise,
copying these three files is the close-out for Track F.
