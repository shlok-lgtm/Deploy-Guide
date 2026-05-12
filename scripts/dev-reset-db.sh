#!/usr/bin/env bash
# dev-reset-db.sh — verify / heal the Neon dev branch schema.
#
# IMPORTANT: This is NOT a destructive drop-and-recreate. It runs
# app.schema_heal.run() inside a one-shot worker container, which checks that
# the dev DB matches the canonical schema and raises SchemaDriftError if a
# table/column is missing. For Phase A, a true reset = create a new Neon branch
# from the prod snapshot via the Neon UI (https://console.neon.tech) and update
# DATABASE_URL in env.dev. Branch-level reset/drop is out of scope here because
# Neon owns the data plane and we don't want this script to delete anything.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

COMPOSE_FILE="docker-compose.dev.yml"
ENV_FILE="env.dev"

# -----------------------------------------------------------------------------
# Step 1: env.dev sourced
# -----------------------------------------------------------------------------
if [ ! -f "$ENV_FILE" ]; then
  echo "ERROR: $ENV_FILE not found. Run 'make dev-bootstrap' first."
  exit 1
fi

set +u
# shellcheck disable=SC1090
. "./$ENV_FILE"
set -u

DB_URL="${DATABASE_URL:-}"
if [ -z "$DB_URL" ] || echo "$DB_URL" | grep -q "REPLACE_ME"; then
  echo "ERROR: DATABASE_URL is unset or contains REPLACE_ME in $ENV_FILE."
  echo "       Run 'make dev-bootstrap' to set it up."
  exit 1
fi

# -----------------------------------------------------------------------------
# Step 2: extract host from DATABASE_URL (do not print credentials)
# -----------------------------------------------------------------------------
DB_HOST="$(python3 - <<'PY'
import os
from urllib.parse import urlparse
url = os.environ.get("DATABASE_URL", "")
parsed = urlparse(url)
host = parsed.hostname or "<unknown-host>"
print(host)
PY
)"

# -----------------------------------------------------------------------------
# Step 3: confirm destructive intent
# -----------------------------------------------------------------------------
cat <<EOF

==============================================================================
  DEV DATABASE SCHEMA VERIFY/HEAL
==============================================================================
  Target host: $DB_HOST
  Action:      Run app.schema_heal.run() inside a fresh worker container.
  Effect:      Validates schema; raises SchemaDriftError if drift detected.
  Note:        This does NOT drop or recreate tables. For a true reset, create
               a new Neon branch from the prod snapshot in the Neon console
               (https://console.neon.tech) and update DATABASE_URL in env.dev.

  If $DB_HOST looks like the PRODUCTION host, ABORT NOW.
==============================================================================

EOF

read -p "Type 'reset' to confirm: " CONFIRM
if [ "$CONFIRM" != "reset" ]; then
  echo "Aborted."
  exit 1
fi

# -----------------------------------------------------------------------------
# Step 4: pick a compose command
# -----------------------------------------------------------------------------
COMPOSE_CMD=""
if command -v docker-compose >/dev/null 2>&1; then
  COMPOSE_CMD="docker-compose"
elif docker compose version >/dev/null 2>&1; then
  COMPOSE_CMD="docker compose"
else
  echo "ERROR: docker-compose / docker compose not available."
  exit 1
fi

# -----------------------------------------------------------------------------
# Step 5: run schema_heal in a one-shot worker container
# -----------------------------------------------------------------------------
echo "==> Running app.schema_heal.run() in a fresh worker container..."
$COMPOSE_CMD -f "$COMPOSE_FILE" run --rm worker \
  python -c 'from app.schema_heal import run; run()'

echo
echo "==> schema_heal completed (exit code $?)."
echo "    If you saw SchemaDriftError above, create a fresh Neon branch from"
echo "    the prod snapshot via https://console.neon.tech and update env.dev."
