# Playground Privacy

## Data Collected

- **Portfolio positions**: asset symbols, amounts, optional protocol slugs
- **Email address**: only when the user requests a full report
- **IP hash**: SHA-256 of IP address (for rate limiting, not identification)

## Retention

- Submissions retained indefinitely for product analytics
- Emails retained until user requests deletion via shlok@basisprotocol.xyz
- Portfolios are never shared externally

## Cleanup

- Submissions with expired report links (30 days post-expiry) that were never
  accessed are automatically deleted
- Submissions that were accessed at least once are retained for analytics

## Report Links

- 7-day TTL from request time
- Signed token (32-byte URL-safe random)
- noindex/nofollow meta tag prevents search indexing
- Access count tracked for analytics

## Contact

For deletion requests: shlok@basisprotocol.xyz
