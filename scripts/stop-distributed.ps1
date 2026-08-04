[CmdletBinding()]
param(
    [switch]$RemoveVolumes
)

$ErrorActionPreference = "Stop"
$projectRoot = Split-Path -Parent $PSScriptRoot
$arguments = @("compose", "--env-file", ".env", "down", "--remove-orphans")
if ($RemoveVolumes) {
    $arguments += "--volumes"
    Write-Warning "Removing PayFlow Lab MySQL and Redis named volumes. This cannot be recovered from this project."
}

Push-Location -LiteralPath $projectRoot
try {
    Write-Host "[PayFlow] command: docker $($arguments -join ' ')"
    & docker @arguments
    if ($LASTEXITCODE -ne 0) {
        throw "docker compose down failed with exit code $LASTEXITCODE"
    }
}
finally {
    Pop-Location
}
