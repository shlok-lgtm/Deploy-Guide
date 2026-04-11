# Basis State — Analytics Gaps TODO

The basis-state repo is separate and not available in this workspace.
These changes need to be applied directly to the basis-state repository.

## Gap 1: Add Plausible to basisstate.xyz

Find the base Jinja2 template (likely `templates/base.html`) and add inside
the `<head>` tag:

```html
<script defer data-domain="basisstate.xyz" src="https://plausible.io/js/script.js"></script>
```

**Important**: The data-domain must be "basisstate.xyz", not "basisprotocol.xyz".
These are separate sites with separate tracking.

Also check for any direct HTML strings in route handlers (FastAPI HTMLResponse)
and add the script tag there too.

### Manual step required

Add "basisstate.xyz" as a new site in the Plausible dashboard at
https://plausible.io/sites — this must be done manually after deploying.

## Gap 3: Add request logging middleware

If basis-state does not already have request logging, add a simple
FastAPI middleware:

```python
from datetime import datetime, timezone

@app.middleware("http")
async def log_requests(request, call_next):
    start = time.time()
    response = await call_next(request)
    elapsed_ms = int((time.time() - start) * 1000)

    ip = request.headers.get("x-forwarded-for", "").split(",")[0].strip()
    if not ip:
        ip = request.client.host if request.client else "unknown"
    ua = request.headers.get("user-agent", "")[:500]

    INTERNAL_UA = ["python-httpx", "basis-keeper", "basis-worker",
                   "uvicorn", "claudebot", "python-requests", "replit"]
    INTERNAL_IP = ["35.191.", "10.", "127.0.0."]
    is_internal = (
        any(pat in ua.lower() for pat in INTERNAL_UA)
        or any(ip.startswith(p) for p in INTERNAL_IP)
    )

    # Log to shared DB or local table
    try:
        execute(
            \"\"\"INSERT INTO basisstate_access_log
               (endpoint, method, status_code, response_time_ms,
                ip_address, user_agent, is_internal, timestamp)
            VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())\"\"\",
            (str(request.url.path), request.method, response.status_code,
             elapsed_ms, ip, ua, is_internal)
        )
    except Exception:
        pass  # non-fatal

    return response
```

If basis-state shares the hub database (same DATABASE_URL), you could log
to `api_request_log` instead. Otherwise create a `basisstate_access_log` table:

```sql
CREATE TABLE IF NOT EXISTS basisstate_access_log (
    id SERIAL PRIMARY KEY,
    endpoint TEXT,
    method TEXT,
    status_code INTEGER,
    response_time_ms INTEGER,
    ip_address TEXT,
    user_agent TEXT,
    is_internal BOOLEAN DEFAULT FALSE,
    timestamp TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX ON basisstate_access_log(timestamp);
CREATE INDEX ON basisstate_access_log(is_internal, timestamp);
```
