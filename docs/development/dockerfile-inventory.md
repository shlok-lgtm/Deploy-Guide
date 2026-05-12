# Dockerfile Inventory

## Summary Table

| File | Base Image | Exposed Ports | Entrypoint | Multistage |
|------|-----------|---------------|-----------|-----------|
| Dockerfile.api | python:3.11-slim | None | CMD only | No |
| Dockerfile.worker | python:3.11-slim | None | CMD only | No |
| Dockerfile.keeper | node:22-slim | None | CMD only | No |

---

## Dockerfile.api

**Path:** `Dockerfile.api`

### Base Image(s)
- `python:3.11-slim`

### Multi-stage
No

### WORKDIR
- `/app`

### EXPOSE
None

### ENTRYPOINT / CMD
- **CMD:** `["python", "main.py"]`

### Environment Variables (hardcoded)
- `WORKER_ENABLED=false`
- `KEEPER_ENABLED=false`
- `PORT=8000`

### System Dependencies (apt-get/pip)
- **apt-get:** `libpq-dev`, `gcc` (database client dev libs and C compiler)
- **pip:** dependencies from `requirements.txt`, plus explicitly `x402>=2.6.0`

### COPY/ADD
- `requirements.txt` → `/app/`
- `app/` → `/app/app/`
- `frontend/dist/` → `/app/frontend/dist/` (pre-built frontend)
- `migrations/` → `/app/migrations/`
- `public/` → `/app/public/`
- `fonts/` → `/app/fonts/`
- `main.py` → `/app/`

### USER
None (runs as root)

### HEALTHCHECK
None

### Build Args (ARG)
None declared

### .dockerignore
Yes (shared at repo root)

---

## Dockerfile.worker

**Path:** `Dockerfile.worker`

### Base Image(s)
- `python:3.11-slim`

### Multi-stage
No

### WORKDIR
- `/app`

### EXPOSE
None

### ENTRYPOINT / CMD
- **CMD:** `["python", "-m", "app.worker", "--loop"]`

### Environment Variables (hardcoded)
None

### System Dependencies (apt-get/pip)
- **apt-get:** `libpq-dev`, `gcc` (database client dev libs and C compiler)
- **pip:** dependencies from `requirements.txt`

### COPY/ADD
- `requirements.txt` → `/app/`
- `app/` → `/app/app/` (cache bust comment dated 2026-04-14T01:40Z)

### USER
None (runs as root)

### HEALTHCHECK
None

### Build Args (ARG)
None declared

### .dockerignore
Yes (shared at repo root)

---

## Dockerfile.keeper

**Path:** `Dockerfile.keeper`

### Base Image(s)
- `node:22-slim`

### Multi-stage
No

### WORKDIR
- `/app`

### EXPOSE
None

### ENTRYPOINT / CMD
- **CMD:** `["npx", "tsx", "keeper/index.ts"]`

### Environment Variables (hardcoded)
None

### System Dependencies (apt-get/npm)
- **npm:** dependencies from `package.json` and `package-lock.json`

### COPY/ADD
- `package.json`, `package-lock.json*` → `/app/`
- `keeper/` → `/app/keeper/`
- `tsconfig.base.json*`, `tsconfig.json*` → `/app/` (optional files)

### USER
None (runs as root)

### HEALTHCHECK
None

### Build Args (ARG)
None declared

### .dockerignore
Yes (shared at repo root)

---

## Gaps & Notes

1. **No exposed ports:** None of the three containers declare EXPOSE directives. Service port configuration likely happens at orchestration/deployment layer (e.g., docker-compose, Kubernetes manifests, or Railway config).

2. **Port environment variable:** Only `Dockerfile.api` hardcodes `PORT=8000`. The actual HTTP port exposure is not declared in any Dockerfile.

3. **No HEALTHCHECK directives:** None of the Dockerfiles define health checks. Consider adding if these are deployed in a managed orchestration environment.

4. **All run as root:** No USER directive in any Dockerfile—all services run as root UID inside containers. This is a security consideration for production deployments.

5. **Cache bust comment:** `Dockerfile.worker` contains a cache-bust comment dated 2026-04-14, suggesting intentional rebuild forcing.

6. **Optional tsconfig files:** `Dockerfile.keeper` uses wildcard `*` for tsconfig files—they are optional and may not exist.

7. **Shared .dockerignore:** All three Dockerfiles rely on a single `.dockerignore` at repo root (not per-Dockerfile). The .dockerignore excludes markdown files globally (`*.md`) but preserves `README.md`, which may affect build context size.

8. **No ARG declarations:** No build-time arguments in any Dockerfile—all configuration is hardcoded or passed via environment at runtime.

9. **Requirements verification:** `requirements.txt` and `package.json` are confirmed to exist in repo root. All COPY targets (`app/`, `frontend/dist/`, `migrations/`, `public/`, `fonts/`, `keeper/`) are confirmed to exist.

10. **Keeper missing lock file risk:** `Dockerfile.keeper` uses `package-lock.json*` (optional) which could lead to non-deterministic npm installs if the lock file is not present.
