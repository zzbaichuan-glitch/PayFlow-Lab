[CmdletBinding()]
param(
    [switch]$Race
)

$ErrorActionPreference = "Stop"
$projectRoot = Split-Path -Parent $PSScriptRoot
$arguments = @("test")
if ($Race) {
    $arguments += "-race"
}
if ($PSBoundParameters.ContainsKey("Verbose")) {
    $arguments += "-v"
}
$arguments += "./..."

Push-Location -LiteralPath $projectRoot
try {
    Write-Host "[PayFlow] project: $projectRoot"
    Write-Host "[PayFlow] command: go $($arguments -join ' ')"
    & go @arguments
    if ($LASTEXITCODE -ne 0) {
        throw "go test failed with exit code $LASTEXITCODE"
    }
}
finally {
    Pop-Location
}
