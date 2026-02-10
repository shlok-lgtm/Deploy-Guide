#!/bin/bash
# Basis Protocol - Start API Server
# Usage: ./run.sh

set -e

# Load .env if it exists
if [ -f .env ]; then
    export $(cat .env | grep -v '^#' | xargs)
fi

# Check required env
if [ -z "$DATABASE_URL" ]; then
    echo "ERROR: DATABASE_URL not set. Copy .env.example to .env and fill in your values."
    exit 1
fi

echo "Starting Basis Protocol API..."
echo "  Database: $(echo $DATABASE_URL | sed 's/:.*@/@/g')"
echo "  Port: ${API_PORT:-8000}"

uvicorn app.server:app --host ${API_HOST:-0.0.0.0} --port ${API_PORT:-8000} --reload
