# Smart Wildfire Intelligence System — Startup Script (PowerShell)
param(
    [switch]$Orchestration,
    [switch]$Tools,
    [switch]$All
)

$ErrorActionPreference = "Stop"

$Profiles = @()
if ($All) {
    $Profiles += "--profile", "orchestration", "--profile", "tools"
} else {
    if ($Orchestration) { $Profiles += "--profile", "orchestration" }
    if ($Tools) { $Profiles += "--profile", "tools" }
}

Write-Host '============================================================' -ForegroundColor Cyan
Write-Host ' Starting Smart Wildfire Intelligence System Infrastructure' -ForegroundColor Cyan
Write-Host '============================================================' -ForegroundColor Cyan

if ($Profiles.Count -gt 0) {
    Write-Host "Active Profiles: $($Profiles -join ' ')" -ForegroundColor Yellow
    docker compose @Profiles up -d
} else {
    Write-Host 'Active Mode: Core Infrastructure (HDFS, Kafka, Spark, PostgreSQL, Kafka UI)' -ForegroundColor Green
    docker compose up -d
}

Write-Host ''
Write-Host 'Waiting for services to become healthy...' -ForegroundColor Cyan
docker compose ps
Write-Host ''
Write-Host 'Use .\scripts\status.ps1 to inspect health and service URLs.' -ForegroundColor Green
