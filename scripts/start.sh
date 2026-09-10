#!/usr/bin/env bash
# Smart Wildfire Intelligence System — Startup Script (Bash)
set -euo pipefail

PROFILES=""

for arg in "$@"; do
    case $arg in
        --orchestration)
            PROFILES="$PROFILES --profile orchestration"
            ;;
        --tools)
            PROFILES="$PROFILES --profile tools"
            ;;
        --all)
            PROFILES="$PROFILES --profile orchestration --profile tools"
            ;;
    esac
done

echo "============================================================"
echo " Starting Smart Wildfire Intelligence System Infrastructure"
echo "============================================================"

if [ -n "$PROFILES" ]; then
    echo "Active Profiles: $PROFILES"
    docker compose $PROFILES up -d
else
    echo "Active Mode: Core Infrastructure (HDFS, Kafka, Spark, PostgreSQL, Kafka UI)"
    docker compose up -d
fi

echo ""
echo "Waiting for services to become healthy..."
docker compose ps
echo ""
echo "Run ./scripts/status.sh to inspect health and service URLs."
