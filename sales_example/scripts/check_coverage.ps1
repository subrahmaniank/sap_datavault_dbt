# PowerShell script to check dbt test coverage
# Usage: .\scripts\check_coverage.ps1

Write-Host "=== dbt Test Coverage Report ===" -ForegroundColor Cyan
Write-Host ""

# Count total models
$totalModels = (uv run dbt list --select resource_type:model --output name).Count
Write-Host "Total Models: $totalModels" -ForegroundColor Yellow

# Get all models
$allModels = uv run dbt list --select resource_type:model --output name

# Count models with tests
$modelsWithTests = 0
$modelsWithoutTests = @()

foreach ($model in $allModels) {
    $testCount = (uv run dbt list --select "test_type:data" --select $model --output name 2>$null).Count
    if ($testCount -gt 0) {
        $modelsWithTests++
    } else {
        $modelsWithoutTests += $model
    }
}

Write-Host "Models with Tests: $modelsWithTests" -ForegroundColor Green

# Calculate coverage percentage
if ($totalModels -gt 0) {
    $coverage = [math]::Round(($modelsWithTests / $totalModels) * 100, 2)
    Write-Host "Test Coverage: ${coverage}%" -ForegroundColor $(if ($coverage -ge 80) { "Green" } elseif ($coverage -ge 50) { "Yellow" } else { "Red" })
} else {
    Write-Host "Test Coverage: 0%" -ForegroundColor Red
}

Write-Host ""
if ($modelsWithoutTests.Count -gt 0) {
    Write-Host "=== Models without tests ===" -ForegroundColor Yellow
    foreach ($model in $modelsWithoutTests) {
        Write-Host "  - $model" -ForegroundColor Red
    }
} else {
    Write-Host "=== All models have tests! ===" -ForegroundColor Green
}

Write-Host ""
Write-Host "=== Test Summary ===" -ForegroundColor Cyan
$totalTests = (uv run dbt list --select test_type:data --output name).Count
Write-Host "Total Tests: $totalTests" -ForegroundColor Yellow
Write-Host "Average Tests per Model: $([math]::Round($totalTests / $totalModels, 2))" -ForegroundColor Yellow

