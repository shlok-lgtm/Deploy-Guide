# Phase B Closure — Dark Factory Holdout Bank

**Date:** 2026-05-12
**Outcome:** Phase B shipped with five of six seed scenarios in
`basis-protocol/scenarios`; safeguards landed in basis-hub via
this PR.

## Locked design decisions

Four decisions were settled before scenario authoring began and
remain in force for Phase C onwards:

1. **Grader: two-stage per scenario.** Stage 1 is a SQL check
   against substrate state (fail-fast filter). Stage 2 is an LLM
   judge over the PR diff against an enumerated wrong-answer
   list.
2. **Code state: current-main only.** Scenarios target today's
   basis-hub `main`. The source PR / issue in each
   `meta.yaml::source` is provenance, not test substrate. A
   `harness.setup_revert` field optionally instructs the harness
   to `git revert --no-edit <SHA>` against `main` before handing
   the tree to CC.
3. **Categories:** `timing/coupling`, `argument-shape`,
   `silent-path`, `hidden-dependency`. A free-text `subcategory`
   field captures finer cuts.
4. **Difficulty levels:** 1 = single-file mechanical fix;
   2 = multi-file or one explicit design choice;
   3 = CC must first decide *what kind of fix* applies.

## Folder layout per scenario

Every scenario folder in `basis-protocol/scenarios` contains
seven files:

```
NNN-slug/
  meta.yaml              # scenario metadata (validates against schema)
  prompt.md              # hint-free problem statement for CC
  setup_code.md          # how to recreate the broken code state
  setup_substrate.sql    # idempotent DB substrate setup
  expected_substrate.sql # stage-1 grader query with pass/fail comments
  wrong_answers.md       # enumerated shallow fixes, anti-examples for judge
  notes.md               # reviewer-only context — NEVER FOR CC CONTEXT
```

`meta.yaml` validates against `schema/meta.schema.yaml` in the
scenarios repo. The required fields are `slug`, `title`,
`category`, `subcategory`, `difficulty`, `source`, `grader`,
`harness`, `tags`. `harness.entry_point` is the Python expression
the harness invokes after substrate setup and CC's submitted fix
are applied; `harness.setup_revert` is the optional commit SHA
the harness reverts before handing the tree to CC.

## Six-seed bank

The Phase B brief specified six seed scenarios across the four
categories above. Five shipped; the sixth was deferred (see
below).

| Status | Category | Difficulty |
|--------|----------|------------|
| Shipped | timing/coupling | 2 |
| Shipped | timing/coupling | 2 |
| Shipped | argument-shape | 1 |
| Shipped | argument-shape | 3 |
| Shipped | silent-path | 2 |
| Deferred | hidden-dependency | 3 |

Slugs and provenance for the shipped scenarios live in the
scenarios repo, not here, by isolation discipline (see
"Isolation" below). The shipped bank covers three of the four
seed categories; `hidden-dependency` will be filled by the
Phase B+ follow-up below.

## Sixth scenario deferred to Phase B+ follow-up

The original Phase B brief listed six seed scenarios across four
categories (timing/coupling, argument-shape, silent-path,
hidden-dependency). Five landed; the sixth (hidden-dependency)
halted at the verification step the brief itself prescribed
(lesson 10: "read directories not single files — grep basis-hub
main for consumers, don't trust this prompt's file:line hint").
Slugs and provenance for the shipped scenarios live in the
scenarios repo, not here, by isolation discipline.

**Premise decay.** The module the brief named —
`app/data_layer/volatility_surfaces.py` — does not exist in
current `main`. `volatility_surfaces` is alive as a Postgres
table with many active consumers (`app/server.py:7916`,
`app/data_layer/peg_monitor.py` as the producer at lines
258/281/454, plus catalog, schema-validator, state-growth, and
component-replay references). There is no apparent orphan to
investigate. Reshaping the scenario to target a real
apparent-orphan module is in scope for Phase B+ but requires
picking a candidate and verifying its consumer graph matches
the lesson-10 pattern before writing the prompt. Candidate
selection is itself out of scope for this closure doc to avoid
burning the future scenario.

**Schema gap surfaced by the attempt.** The current
`schema/meta.schema.yaml` requires `source.pr_fix: integer` and
`harness: object`. Proposal-style scenarios — where pass/fail is
on CC's investigation answer, not substrate state — fit neither:
no single fix PR exists, and no runtime invocation advances
state. The Phase B+ follow-up will land three coordinated
changes:
(a) `source.pr_fix` → nullable, with `null` reserved for
non-fix scenarios;
(b) `harness` → optional, omitted for proposal scenarios;
(c) `grader.stage_1` → accept a `noop` sentinel for scenarios
whose pass/fail is judged entirely by stage 2 over CC's answer.
None of these affect the five shipped scenarios; they extend the
schema, they don't break it.

Phase B ships with the five scenarios above. The follow-up to
land the sixth and the schema patches is tracked separately; it
does not gate any downstream phase (C runs scenarios, D builds
the judge harness, E automates), since all three downstream
phases can proceed against the five-scenario seed bank.

## Isolation discipline

> THIS CC session writes the scenarios — it has already seen the
> underlying bugs (they were fixed minutes ago in basis-hub).
> What matters is FUTURE CC sessions never having scenarios in
> context. No links from basis-hub issues to scenario folders.
> No clones of basis-protocol/scenarios in any basis-hub working
> dir. No paste of scenario content into future prompts.

The discipline is encoded in three surfaces in this repo:

1. `.gitignore` — blocks `scenarios/` and
   `basis-protocol-scenarios/` as tripwires.
2. `CLAUDE.md` — `## Do NOT` section names the isolation rule
   so every CC session that reads project context sees it.
3. `docs/development/phase-b-isolation.md` — full rationale,
   leak-response procedure, scope boundary between basis-hub
   docs and the scenarios repo.

The scenarios repo itself enforces the same boundary from its
side: a prominent isolation policy in `README.md`, a discipline
checklist in `CONTRIBUTING.md`, and a defensive `.gitignore`
blocking `basis-hub/` patterns.

## Phase C / D / E forward pointers

This closure doc records what Phase B landed. The subsequent
phases are out of execution scope here:

- **Phase C — Running scenarios.** Spawn fresh CC sessions in
  ephemeral environments against each scenario; collect diffs
  and substrate-post-states for grading. Inherits no context
  from any basis-hub working session.
- **Phase D — LLM judge harness.** Wire stage 2: judge LLM
  scores CC's diff against `wrong_answers.md` per scenario;
  surface the diff-shape patterns CC actually proposes so the
  wrong-answer lists can grow from observation rather than
  guess. Likely lifts stage-1 SQL assertions out of inline
  comments into structured `meta.yaml::grader.stage_1.assertions`
  triples; also lands the `source.pr_fix`/`harness` schema
  patches above.
- **Phase E — Automation.** Cron the loop. Define stop
  conditions for retiring burned scenarios and authoring
  replacements.

Each phase MUST preserve the isolation discipline from Phase B.
Specifically: no Phase C / D / E artifact may import scenario
content into a basis-hub-facing surface, and no eval harness may
share a context window between a basis-hub working session and
a scenario-running session.
