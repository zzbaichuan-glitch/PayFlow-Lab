[CmdletBinding()]
param(
    [ValidateRange(1, 65535)]
    [int]$Port = 8081,

    [string]$BindAddress = "127.0.0.1"
)

$ErrorActionPreference = "Stop"
$projectRoot = Split-Path -Parent $PSScriptRoot
$previousAddress = $env:PAYFLOW_ADDR

Push-Location -LiteralPath $projectRoot
try {
    $env:PAYFLOW_ADDR = "${BindAddress}:${Port}"
    Write-Host "[PayFlow] project: $projectRoot"
    Write-Host "[PayFlow] command: go run ./cmd/server"
    Write-Host "[PayFlow] demo:    http://localhost:$Port"
    & go run ./cmd/server
    if ($LASTEXITCODE -ne 0) {
        throw "go run failed with exit code $LASTEXITCODE"
    }
}
finally {
    Pop-Location
    if ($null -eq $previousAddress) {
        Remove-Item Env:PAYFLOW_ADDR -ErrorAction SilentlyContinue
    }
    else {
        $env:PAYFLOW_ADDR = $previousAddress
    }
}
