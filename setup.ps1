# PowerShell setup script for Windows

Write-Host "=== Pub-Sub Log Aggregator Setup ===" -ForegroundColor Green
Write-Host ""

# Build images
Write-Host "Step 1: Building Docker images..." -ForegroundColor Blue
docker compose build

# Start services
Write-Host "Step 2: Starting services..." -ForegroundColor Blue
docker compose up -d

# Wait
Write-Host "Step 3: Waiting for services to be ready..." -ForegroundColor Blue
Start-Sleep -Seconds 10

# Check health
Write-Host "Step 4: Verifying services..." -ForegroundColor Blue
$maxRetries = 30
$retry = 0

while ($retry -lt $maxRetries) {
    try {
        $response = Invoke-WebRequest -Uri "http://localhost:8080/health" -ErrorAction SilentlyContinue
        if ($response.StatusCode -eq 200) {
            Write-Host "✓ Aggregator is healthy" -ForegroundColor Green
            break
        }
    }
    catch {
        Write-Host "Waiting for aggregator... ($retry/$maxRetries)"
        Start-Sleep -Seconds 1
        $retry++
    }
}

if ($retry -eq $maxRetries) {
    Write-Host "❌ Aggregator not ready" -ForegroundColor Red
    docker compose logs aggregator
    exit 1
}

# Check database
Write-Host "Step 5: Getting initial stats..." -ForegroundColor Blue
$stats = Invoke-WebRequest -Uri "http://localhost:8080/stats"
$stats.Content | ConvertFrom-Json | ConvertTo-Json

Write-Host ""
Write-Host "=== Setup Complete ===" -ForegroundColor Green
Write-Host ""
Write-Host "Available endpoints:"
Write-Host "  • API: http://localhost:8080"
Write-Host "  • Health: http://localhost:8080/health"
Write-Host "  • Stats: http://localhost:8080/stats"
Write-Host "  • Events: http://localhost:8080/events"
Write-Host ""
Write-Host "Commands:"
Write-Host "  • View logs: docker compose logs -f"
Write-Host "  • Stop: docker compose down"
Write-Host "  • Clean: docker compose down; docker volume rm uas_pg_data uas_aggregator_data"
Write-Host ""
