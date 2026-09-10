# Smart Wildfire Intelligence System — Logs Viewer (PowerShell)
param(
    [string]$Service = ""
)

if ($Service) {
    docker compose logs -f $Service
} else {
    docker compose logs -f --tail 100
}
