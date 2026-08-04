[CmdletBinding()]
param(
    [switch]$NoBuild
)

$ErrorActionPreference = "Stop"
$projectRoot = Split-Path -Parent $PSScriptRoot
$envFile = Join-Path $projectRoot ".env"
$exampleEnvFile = Join-Path $projectRoot ".env.example"

if (-not (Test-Path -LiteralPath $envFile)) {
    Copy-Item -LiteralPath $exampleEnvFile -Destination $envFile
    Write-Host "[PayFlow] created .env from .env.example (development-only credentials)"
}

$arguments = @("compose", "--env-file", ".env", "up", "-d")
if (-not $NoBuild) {
    $arguments += "--build"
}
$arguments += @("--wait", "--wait-timeout", "240")

Push-Location -LiteralPath $projectRoot
try {
    Write-Host "[PayFlow] project: $projectRoot"
    Write-Host "[PayFlow] command: docker $($arguments -join ' ')"
    & docker @arguments
    if ($LASTEXITCODE -ne 0) {
        throw "docker compose up failed with exit code $LASTEXITCODE"
    }
    $binding = (& docker compose --env-file .env port payflow-api 8081 | Select-Object -First 1).Trim()
    if ($LASTEXITCODE -ne 0 -or $binding -notmatch ":(?<port>\d+)$") {
        throw "could not resolve the published PayFlow HTTP port from: $binding"
    }
    $baseUrl = "http://127.0.0.1:$($Matches.port)"
    $health = Invoke-RestMethod -Method Get -Uri "$baseUrl/healthz"
    Write-Host "[PayFlow] distributed stack is healthy: $baseUrl"
    $health | ConvertTo-Json -Depth 8
}
finally {
    Pop-Location
}
