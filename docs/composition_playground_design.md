# Composition Playground — Design Document

## v1 (Current — Ops Only)

The composition playground lets users submit a portfolio of stablecoin
positions, receive aggregate CQI scores, stress scenario results, and a
truncated Basel SCO60 preview. Full reports are delivered via email link.

### What's Shipped

- Portfolio validation (max 50 positions)
- Aggregate weighted CQI computation
- 3 stress scenarios: single-issuer depeg, algorithmic collapse, protocol contagion
- Basel SCO60 preview (executive summary only)
- Full Basel SCO60 report via 7-day email link
- Submission tracking (playground_submissions table)
- Ops dashboard Playground tab
- Rate limiting (10/hr per IP)

### What's Deferred

- Public route (/playground) — ops-only for now
- Sitemap entry
- Anti-abuse beyond IP rate limits (CAPTCHA, account-based)
- PDF export (HTML report via email link instead)
- Portfolio persistence (save/load named portfolios)
- Historical portfolio tracking (score changes over time)
- Real-time portfolio monitoring alerts

## Graduation to v2 (Public)

Criteria before going public:
1. N days of internal ops usage without issues
2. M reports requested and accessed
3. Zero abuse patterns detected
4. Legal review of privacy notice
5. Email sending reliability confirmed (zero bounces)

## Email Dependency

Provider: Resend (via RESEND_API_KEY env var)
Fallback: if RESEND_API_KEY not set, report request succeeds but email
is not sent — logged as warning. User gets the report URL in the API
response regardless.

## Architecture

- Compute path reuses existing CQI, SII, PSI primitives — no new scoring
- Basel SCO60 preview is the first section of the existing compliance template
- Full report uses the existing compliance template with SCO60 lens
- Submission table is the future lead list — treat as production data
