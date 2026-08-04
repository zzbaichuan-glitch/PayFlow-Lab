[CmdletBinding()]
param(
    [switch]$NoBuild,
    [switch]$StopAfter
)

$ErrorActionPreference = "Stop"
$projectRoot = Split-Path -Parent $PSScriptRoot

Push-Location -LiteralPath $projectRoot
try {
    Write-Host "[PayFlow] 1/4 local Go tests"
    & go test -count=1 ./...
    if ($LASTEXITCODE -ne 0) { throw "go test failed" }

    Write-Host "[PayFlow] 2/4 compose configuration"
    if (-not (Test-Path -LiteralPath ".env")) {
        Copy-Item -LiteralPath ".env.example" -Destination ".env"
    }
    & docker compose --env-file .env config --quiet
    if ($LASTEXITCODE -ne 0) { throw "docker compose config failed" }

    Write-Host "[PayFlow] 3/4 build/start/health"
    & (Join-Path $PSScriptRoot "start-distributed.ps1") -NoBuild:$NoBuild

    Write-Host "[PayFlow] 4/4 real DTM integration assertions"
    & (Join-Path $PSScriptRoot "demo-distributed.ps1")
}
finally {
    Pop-Location
    if ($StopAfter) {
        & (Join-Path $PSScriptRoot "stop-distributed.ps1")
    }
}
