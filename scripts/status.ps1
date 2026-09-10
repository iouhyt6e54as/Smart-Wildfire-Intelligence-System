# Smart Wildfire Intelligence System — Status & UI URLs (PowerShell)
Write-Host '============================================================' -ForegroundColor Cyan
Write-Host ' Smart Wildfire Intelligence System — Service Status' -ForegroundColor Cyan
Write-Host '============================================================' -ForegroundColor Cyan

docker compose ps

Write-Host ''
Write-Host '------------------------------------------------------------' -ForegroundColor Cyan
Write-Host ' Service Web Interfaces' -ForegroundColor Cyan
Write-Host '------------------------------------------------------------' -ForegroundColor Cyan
Write-Host '  HDFS NameNode UI   : http://localhost:9870' -ForegroundColor White
Write-Host '  HDFS DataNode UI   : http://localhost:9864' -ForegroundColor White
Write-Host '  Spark Master UI    : http://localhost:8080' -ForegroundColor White
Write-Host '  Spark Worker UI    : http://localhost:8081' -ForegroundColor White
Write-Host '  Kafka UI           : http://localhost:8090' -ForegroundColor White
Write-Host '  PostgreSQL Port    : localhost:5432 (databases: airflow, wildfire)' -ForegroundColor White
Write-Host '  Airflow UI (opt)   : http://localhost:8082 (admin / admin)' -ForegroundColor White
Write-Host '  Jupyter Lab (opt)  : http://localhost:8888 (token: lab)' -ForegroundColor White
Write-Host '============================================================' -ForegroundColor Cyan
