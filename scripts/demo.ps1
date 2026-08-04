[CmdletBinding()]
param(
    [string]$BaseUrl = "http://localhost:8081"
)

$ErrorActionPreference = "Stop"
$idempotencyKey = "demo-$(Get-Date -Format 'yyyyMMddHHmmssfff')"
$orderId = "order-$(Get-Date -Format 'yyyyMMddHHmmss')"
$headers = @{ "Idempotency-Key" = $idempotencyKey }
$createBody = @{
    order_id = $orderId
    account_id = "demo-user"
    amount_cents = 1999
    currency = "CNY"
} | ConvertTo-Json

Write-Host "[1/6] Health"
Invoke-RestMethod -Method Get -Uri "$BaseUrl/healthz" | ConvertTo-Json -Depth 8

Write-Host "[2/6] Create payment (Try freezes 1999 cents)"
$created = Invoke-RestMethod -Method Post -Uri "$BaseUrl/api/v1/payments" -Headers $headers -ContentType "application/json" -Body $createBody
$created | ConvertTo-Json -Depth 8
$paymentId = $created.data.payment.id

Write-Host "[3/6] Replay the same idempotency key"
Invoke-RestMethod -Method Post -Uri "$BaseUrl/api/v1/payments" -Headers $headers -ContentType "application/json" -Body $createBody | ConvertTo-Json -Depth 8

$callbackBody = @{
    event_id = "channel-$paymentId"
    sequence = 1
    outcome = "success"
} | ConvertTo-Json
Write-Host "[4/6] Apply success callback (Confirm creates one ledger entry)"
Invoke-RestMethod -Method Post -Uri "$BaseUrl/api/v1/payments/$paymentId/callbacks" -ContentType "application/json" -Body $callbackBody | ConvertTo-Json -Depth 8

Write-Host "[5/6] Replay the exact callback (must be duplicate)"
Invoke-RestMethod -Method Post -Uri "$BaseUrl/api/v1/payments/$paymentId/callbacks" -ContentType "application/json" -Body $callbackBody | ConvertTo-Json -Depth 8

$lateFailureBody = @{
    event_id = "late-failure-$paymentId"
    sequence = 2
    outcome = "failed"
    reason = "simulated late callback"
} | ConvertTo-Json
Write-Host "[6/6] Send a later failure (terminal SUCCESS must not roll back)"
Invoke-RestMethod -Method Post -Uri "$BaseUrl/api/v1/payments/$paymentId/callbacks" -ContentType "application/json" -Body $lateFailureBody | ConvertTo-Json -Depth 8

Write-Host "Final payment details"
Invoke-RestMethod -Method Get -Uri "$BaseUrl/api/v1/payments/$paymentId" | ConvertTo-Json -Depth 12

Write-Host "Metrics"
Invoke-RestMethod -Method Get -Uri "$BaseUrl/api/v1/metrics" | ConvertTo-Json -Depth 8
