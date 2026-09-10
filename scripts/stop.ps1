# Smart Wildfire Intelligence System — Shutdown Script (PowerShell)
param(
    [switch]$WipeVolumes
)

Write-Host "Stopping Smart Wildfire Intelligence System containers..." -ForegroundColor Yellow

if ($WipeVolumes) {
    Write-Host "WARNING: Wiping persistent volumes (--volumes)..." -ForegroundColor Red
    docker compose --profile orchestration --profile tools down --volumes
} else {
    docker compose --profile orchestration --profile tools down
}

Write-Host "Services stopped cleanly. Volumes preserved." -ForegroundColor Green
