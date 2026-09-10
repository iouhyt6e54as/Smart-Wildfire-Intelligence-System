#!/usr/bin/env bash
# Smart Wildfire Intelligence System — Shutdown Script (Bash)
set -euo pipefail

WIPE=""
if [ "${1:-}" = "--volumes" ] || [ "${1:-}" = "-v" ]; then
    WIPE="--volumes"
    echo "WARNING: Wiping persistent volumes..."
fi

echo "Stopping Smart Wildfire Intelligence System containers..."
docker compose --profile orchestration --profile tools down $WIPE
echo "Services stopped cleanly."
