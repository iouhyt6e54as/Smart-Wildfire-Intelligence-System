#!/usr/bin/env bash
# Smart Wildfire Intelligence System — Logs Viewer (Bash)
if [ -n "${1:-}" ]; then
    docker compose logs -f "$1"
else
    docker compose logs -f --tail 100
fi
