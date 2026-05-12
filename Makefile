.PHONY: audit-async audit-sync-db audit-async-v1 audit-sync-db-advisory help dev-bootstrap dev dev-down dev-logs dev-shell-worker dev-shell-api dev-reset-db dev-backfill dev-build dev-clean

# Compose command detection. Precedence:
#   1. .compose-cmd (written by scripts/dev-bootstrap.sh) — explicit override.
#   2. `docker-compose` (classic v1 binary) if on PATH.
#   3. `docker compose` (v2 plugin) as the modern default.
# This lets plugin-only operators run `make dev` without first running
# bootstrap. := evaluates once at parse time so we don't shell out per recipe.
COMPOSE := $(shell test -s .compose-cmd && cat .compose-cmd || (command -v docker-compose >/dev/null 2>&1 && echo docker-compose) || echo "docker compose")

audit-async:
	python3 scripts/audit_await_in_args.py

audit-sync-db:
	python3 scripts/audit_sync_db_in_async.py

audit-async-v1:
	python3 scripts/audit_sync_db_in_async.py

audit-sync-db-advisory:
	python3 scripts/audit_sync_db_in_async.py || true

## help: List all available targets with their doc comments
help:
	@grep -E '^## [a-zA-Z_-]+:' $(MAKEFILE_LIST) | awk -F'## ' '{print $$2}' | sort

## dev-bootstrap: One-time local dev setup (checks docker, env.dev, builds images)
dev-bootstrap:
	bash scripts/dev-bootstrap.sh

## dev: Start the local dev stack detached, then follow logs
dev:
	$(COMPOSE) -f docker-compose.dev.yml up -d
	$(MAKE) dev-logs

## dev-down: Stop the local dev stack (preserves volumes)
dev-down:
	$(COMPOSE) -f docker-compose.dev.yml down

## dev-logs: Follow logs from all dev services (tail=100)
dev-logs:
	$(COMPOSE) -f docker-compose.dev.yml logs -f --tail=100

## dev-shell-worker: Open a bash shell in the running worker container
dev-shell-worker:
	$(COMPOSE) -f docker-compose.dev.yml exec worker bash

## dev-shell-api: Open a bash shell in the running api-server container
dev-shell-api:
	$(COMPOSE) -f docker-compose.dev.yml exec api-server bash

## dev-reset-db: Verify/heal Neon dev branch schema (idempotent via schema_heal)
dev-reset-db:
	bash scripts/dev-reset-db.sh

## dev-backfill: Run all 8 backfill one-shot containers (exits when complete)
dev-backfill:
	$(COMPOSE) -f docker-compose.dev.yml --profile backfill up

## dev-build: Rebuild all dev images
dev-build:
	$(COMPOSE) -f docker-compose.dev.yml build

## dev-clean: DESTRUCTIVE - remove containers, volumes, and orphan networks
dev-clean:
	@printf "This will REMOVE all dev containers AND volumes (data loss). "
	@read -p "Type 'clean' to confirm: " CONFIRM && [ "$$CONFIRM" = "clean" ] || (echo "Aborted."; exit 1)
	$(COMPOSE) -f docker-compose.dev.yml down -v --remove-orphans
