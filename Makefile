.PHONY: audit-async audit-sync-db

audit-async:
	python3 scripts/audit_await_in_args.py

audit-sync-db:
	python3 scripts/audit_sync_db_in_async.py
