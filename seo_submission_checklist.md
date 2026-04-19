# SEO Submission Checklist — Basis Protocol

## Search Console Submissions

| Engine | Console URL | Action |
|--------|------------|--------|
| Google Search Console | https://search.google.com/search-console | Add property: https://basisprotocol.xyz. Submit sitemap: https://basisprotocol.xyz/sitemap.xml |
| Bing Webmaster Tools | https://www.bing.com/webmasters | Add site: https://basisprotocol.xyz. Submit sitemap: https://basisprotocol.xyz/sitemap.xml |

## Sitemap URL

```
https://basisprotocol.xyz/sitemap.xml
```

Dynamically generated. Lists all /entity/{slug} URLs with lastmod from latest score timestamp. Refreshes on each request (cached 1 hour).

## Verification Methods

- Google: DNS TXT record or HTML file upload
- Bing: DNS CNAME or HTML meta tag

## Entity Pages

113 entities across 9 indices. Each has:
- Canonical URL: /entity/{slug}
- JSON-LD (Dataset + Rating)
- Meta description with score + top 2 categories
- Breadcrumb navigation

## Bot Access

robots.txt explicitly allows: GPTBot, ClaudeBot, Claude-SearchBot, PerplexityBot, Google-Extended, CCBot, Googlebot, Bingbot.

## Rich Results

Validate at: https://search.google.com/test/rich-results
Test URLs: /entity/usdc, /entity/aave, /entity/steth
