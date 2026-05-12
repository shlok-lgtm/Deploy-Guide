#!/usr/bin/env bash
# dev-bootstrap.sh — one-time local dev setup for basis-hub.
#
# Responsibilities:
#   1. Verify docker + docker-compose (or `docker compose`) are installed.
#   2. Copy env.dev.example -> env.dev if missing.
#   3. Verify DATABASE_URL has been filled in (not REPLACE_ME).
#   4. Build the docker-compose.dev.yml images.
#
# This script is idempotent — run it any time. It never destroys data.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

COMPOSE_FILE="docker-compose.dev.yml"
ENV_FILE="env.dev"
ENV_TEMPLATE="env.dev.example"

# -----------------------------------------------------------------------------
# Step 1: docker + compose available?
# -----------------------------------------------------------------------------
echo "==> Checking Docker toolchain..."
if ! command -v docker >/dev/null 2>&1; then
  echo "ERROR: 'docker' binary not found on PATH."
  echo "       Install Docker Desktop or the Docker Engine, then re-run."
  exit 1
fi
docker --version

# Try classic 'docker-compose' first, then 'docker compose' plugin.
COMPOSE_CMD=""
if command -v docker-compose >/dev/null 2>&1; then
  COMPOSE_CMD="docker-compose"
  docker-compose --version
elif docker compose version >/dev/null 2>&1; then
  COMPOSE_CMD="docker compose"
  docker compose version
else
  echo "ERROR: Neither 'docker-compose' nor 'docker compose' is available."
  echo "       Install Docker Compose v2 (bundled with recent Docker Desktop)."
  exit 1
fi
echo "    using compose: $COMPOSE_CMD"

# Confirm Docker daemon is reachable.
if ! docker info >/dev/null 2>&1; then
  echo "ERROR: Docker daemon is not reachable. Start Docker Desktop / dockerd and retry."
  exit 1
fi

# -----------------------------------------------------------------------------
# Step 2: env.dev present?
# -----------------------------------------------------------------------------
echo "==> Checking $ENV_FILE..."
if [ ! -f "$ENV_FILE" ]; then
  if [ ! -f "$ENV_TEMPLATE" ]; then
    echo "ERROR: Neither $ENV_FILE nor $ENV_TEMPLATE exists. Cannot bootstrap."
    exit 1
  fi
  cp "$ENV_TEMPLATE" "$ENV_FILE"
  echo "    Created $ENV_FILE from $ENV_TEMPLATE."
  echo "    Edit it now to fill REPLACE_ME values, then re-run 'make dev-bootstrap'."
  exit 0
fi
echo "    $ENV_FILE exists."

# -----------------------------------------------------------------------------
# Step 3: DATABASE_URL filled in?
# -----------------------------------------------------------------------------
echo "==> Validating DATABASE_URL in $ENV_FILE..."
# Source env.dev in a subshell so we don't pollute this shell.
set +u
# shellcheck disable=SC1090
. "./$ENV_FILE"
set -u

DB_URL="${DATABASE_URL:-}"
if [ -z "$DB_URL" ] || [ "$DB_URL" = "REPLACE_ME_NEON_DEV_BRANCH_URL" ] || echo "$DB_URL" | grep -q "REPLACE_ME"; then
  cat <<EOF
ERROR: DATABASE_URL in $ENV_FILE is unset or still contains a REPLACE_ME placeholder.

Local dev requires its own Neon branch (do NOT point at the production database).

To create one:
  1. Open the Neon console:  https://console.neon.tech
  2. Select the basis-hub project you have access to.
  3. Create a new branch off the production branch (snapshot of schema + data).
  4. Copy the branch's pooled connection string.
  5. Paste it into $ENV_FILE as DATABASE_URL=postgresql://...
  6. Optionally set BASIS_DEV_BRANCH_ID=<branch-id> for reference.

Then re-run: make dev-bootstrap
EOF
  exit 1
fi
echo "    DATABASE_URL is set (host hidden for safety)."

# -----------------------------------------------------------------------------
# Step 4: build images
# -----------------------------------------------------------------------------
echo "==> Building dev images via $COMPOSE_CMD -f $COMPOSE_FILE build ..."
echo "    (first run is slow — pulling base images + installing deps)"
$COMPOSE_CMD -f "$COMPOSE_FILE" build

# -----------------------------------------------------------------------------
# Done.
# -----------------------------------------------------------------------------
cat <<EOF

==> Bootstrap complete.

Next steps:
  make dev          # start the stack (detached) and follow logs
  make dev-logs     # follow logs without restarting
  make dev-down     # stop the stack (preserves volumes)
  make dev-backfill # run the one-shot backfill jobs
  make dev-reset-db # verify Neon dev branch schema

See docs/development/service-runtime-matrix.md for service details.
EOF
