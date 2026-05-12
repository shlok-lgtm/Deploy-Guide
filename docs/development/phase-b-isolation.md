# Phase B Isolation — basis-protocol/scenarios

**TL;DR:** `basis-protocol/scenarios` is a holdout eval test set for
Claude Code working on this repo. Future CC sessions on basis-hub
must never see its contents. This doc explains why, what that
means concretely, and how the discipline is enforced.

## Why a holdout exists

Phase B established a separate private repository,
`basis-protocol/scenarios`, populated with concrete failure-mode
scenarios derived from recent basis-hub work. Each scenario
encodes a real bug pattern, the substrate that triggers it, an
enumerated wrong-answer list, and a reviewer-only rationale.
Scenarios are run by an out-of-band harness against fresh CC
sessions — none of which has ever seen the scenarios.

The holdout's value depends entirely on CC never having seen the
scenarios. The moment a scenario leaks into a basis-hub CC
session — via a clone in a working tree, a paste in an issue, a
link in a PR, a quote in a comment, or an MCP allowlist extension
— that scenario is burned. It tests memorization, not
generalization, from that point on. Burned scenarios must be
retired and replaced.

## What "isolation" means concretely

These are non-negotiable:

1. **No clones.** Do not `git clone`, `git fetch`, or `gh repo
   clone` `basis-protocol/scenarios` into any basis-hub working
   tree or sibling directory CC sessions can read.
2. **No MCP allowlist extensions.** A basis-hub CC session's MCP
   tools are scoped to `basis-protocol/basis-hub`. Do not add
   `basis-protocol/scenarios` to that allowlist. The MCP
   boundary is the enforcement surface.
3. **One-way provenance.** Scenarios reference basis-hub PRs in
   their `meta.yaml::source`. basis-hub PRs, issues, comments,
   and docs do **not** reference scenario folders. Provenance
   flows scenarios → basis-hub, never the reverse.
4. **No paste-in.** Do not paste scenario text into basis-hub
   issues, PRs, code comments, Slack channels CC watches,
   shared docs CC has been told to read, or any other
   CC-readable surface.
5. **No simultaneous read access.** A human contributor working
   on both repos should not run a single CC session that has
   read access to both. One or the other — never both.

## How the discipline is enforced

- `.gitignore` at this repo's root blocks `scenarios/` and
  `basis-protocol-scenarios/` directory names as tripwires.
- `CLAUDE.md`'s `## Do NOT` section lists the isolation rule so
  every CC session that reads the project context sees it.
- The scenarios repo itself has a complementary `.gitignore`
  blocking `basis-hub/` patterns, a prominent isolation policy
  in `README.md`, and a discipline checklist in
  `CONTRIBUTING.md`.
- Phase D's eval harness runs against fresh CC sessions in
  ephemeral environments; it does not inherit context from any
  basis-hub working session.

## What to do if a leak happens

1. Identify which scenario(s) leaked, where, and into what
   surface (chat log, PR, doc, etc.).
2. Retire the affected scenario: move its folder under
   `retired/NNN-slug/` in the scenarios repo with a `RETIRED.md`
   explaining the leak. Do not delete; the audit trail matters.
3. Author a replacement scenario covering the same category and
   difficulty before the next eval cycle, derived from a basis-hub
   PR the new scenario's target CC sessions have not yet seen.

## What is in scope here vs. there

| Surface | Lives in |
|---------|----------|
| Isolation rationale | basis-hub `docs/development/phase-b-isolation.md` (this doc) |
| Closure record of Phase B | basis-hub `docs/audits/2026-05-12-phase-b-closure.md` |
| Scenario folders, prompts, substrate | `basis-protocol/scenarios` repo only |
| Eval harness (Phases C–E) | TBD; not yet committed |

Phase C onwards is implementation: running scenarios, building
the judge, automating the loop. The isolation discipline above
applies forward through all of those phases.
