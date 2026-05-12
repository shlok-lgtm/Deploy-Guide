.PHONY: audit-async audit-sync-db audit-async-v1 audit-sync-db-advisory help dev-bootstrap dev dev-down dev-logs dev-shell-worker dev-shell-api dev-reset-db dev-backfill dev-build dev-clean

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
	docker-compose -f docker-compose.dev.yml up -d
	$(MAKE) dev-logs

## dev-down: Stop the local dev stack (preserves volumes)
dev-down:
	docker-compose -f docker-compose.dev.yml down

## dev-logs: Follow logs from all dev services (tail=100)
dev-logs:
	docker-compose -f docker-compose.dev.yml logs -f --tail=100

## dev-shell-worker: Open a bash shell in the running worker container
dev-shell-worker:
	docker-compose -f docker-compose.dev.yml exec worker bash

## dev-shell-api: Open a bash shell in the running api-server container
dev-shell-api:
	docker-compose -f docker-compose.dev.yml exec api-server bash

## dev-reset-db: Verify/heal Neon dev branch schema (idempotent via schema_heal)
dev-reset-db:
	bash scripts/dev-reset-db.sh

## dev-backfill: Run all 8 backfill one-shot containers (exits when complete)
dev-backfill:
	docker-compose -f docker-compose.dev.yml --profile backfill up

## dev-build: Rebuild all dev images
dev-build:
	docker-compose -f docker-compose.dev.yml build

## dev-clean: DESTRUCTIVE - remove containers, volumes, and orphan networks
dev-clean:
	@printf "This will REMOVE all dev containers AND volumes (data loss). "
	@read -p "Type 'clean' to confirm: " CONFIRM && [ "$$CONFIRM" = "clean" ] || (echo "Aborted."; exit 1)
	docker-compose -f docker-compose.dev.yml down -v --remove-orphans
