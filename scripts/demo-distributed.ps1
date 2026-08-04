[CmdletBinding()]
param(
    [string]$BaseUrl = "http://localhost:8081"
)

$ErrorActionPreference = "Stop"

function Invoke-PayFlowCreate {
    param(
        [string]$IdempotencyKey,
        [string]$OrderId,
        [int64]$AmountCents,
        [string]$Fault = ""
    )
    $body = @{
        order_id = $OrderId
        account_id = "demo-user"
        amount_cents = $AmountCents
        currency = "CNY"
        fault = $Fault
    } | ConvertTo-Json
    return Invoke-RestMethod -Method Post -Uri "$BaseUrl/api/v1/distributed/payments" `
        -Headers @{ "Idempotency-Key" = $IdempotencyKey } -ContentType "application/json" -Body $body
}

function Wait-PaymentTerminal {
    param([string]$PaymentId)
    for ($attempt = 0; $attempt -lt 40; $attempt++) {
        $details = Invoke-RestMethod -Method Get -Uri "$BaseUrl/api/v1/payments/$PaymentId"
        if ($details.data.payment.status -in @("SUCCESS", "FAILED", "CLOSED")) {
            return $details.data.payment
        }
        Start-Sleep -Milliseconds 250
    }
    throw "payment $PaymentId did not reach a terminal state in 10 seconds"
}

Write-Host "[1/11] Verify distributed mode and dependencies"
$system = Invoke-RestMethod -Method Get -Uri "$BaseUrl/api/v1/system"
$health = Invoke-RestMethod -Method Get -Uri "$BaseUrl/healthz"
if ($system.data.mode -ne "distributed" -or $health.data.status -ne "ok") {
    throw "PayFlow is not healthy in distributed mode"
}
$health | ConvertTo-Json -Depth 8

Write-Host "[2/11] Reset only PayFlow-owned settled demo transactions"
Invoke-RestMethod -Method Post -Uri "$BaseUrl/api/v1/demo/reset" | Out-Null

$runId = Get-Date -Format "yyyyMMddHHmmssfff"
Write-Host "[3/11] Execute a successful real DTM TCC"
$success = Invoke-PayFlowCreate -IdempotencyKey "dtm-success-$runId" -OrderId "order-success-$runId" -AmountCents 1999
$successPayment = Wait-PaymentTerminal -PaymentId $success.data.payment.id
if ($successPayment.status -ne "SUCCESS" -or [string]::IsNullOrWhiteSpace($successPayment.gid)) {
    throw "successful TCC did not reach SUCCESS with a DTM gid"
}
$successPayment | ConvertTo-Json -Depth 8

Write-Host "[4/11] Replay the MySQL/Redis-backed idempotency key"
$replay = Invoke-PayFlowCreate -IdempotencyKey "dtm-success-$runId" -OrderId "order-success-$runId" -AmountCents 1999
if (-not $replay.data.idempotent_replay -or $replay.data.payment.id -ne $successPayment.id) {
    throw "idempotency replay created a different payment"
}
$replay | ConvertTo-Json -Depth 8

$beforeFailure = Invoke-RestMethod -Method Get -Uri "$BaseUrl/api/v1/accounts/demo-user"
Write-Host "[5/11] Make ledger Try fail; DTM must Cancel the account branch"
$failed = Invoke-PayFlowCreate -IdempotencyKey "dtm-failure-$runId" -OrderId "order-failure-$runId" -AmountCents 2500 -Fault "ledger_try"
$failedPayment = Wait-PaymentTerminal -PaymentId $failed.data.payment.id
if ($failedPayment.status -ne "FAILED") {
    throw "faulted TCC did not reach FAILED after compensation"
}

Write-Host "[6/11] Prove balance restoration and no valid ledger entry"
$afterFailure = Invoke-RestMethod -Method Get -Uri "$BaseUrl/api/v1/accounts/demo-user"
$ledger = Invoke-RestMethod -Method Get -Uri "$BaseUrl/api/v1/ledger"
if ($afterFailure.data.account.available_cents -ne $beforeFailure.data.account.available_cents -or
    $afterFailure.data.account.frozen_cents -ne 0 -or $ledger.data.entries.Count -ne 1) {
    throw "Cancel invariant failed: balance changed, funds remain frozen, or an extra POSTED ledger exists"
}

Write-Host "[7/11] Inspect DTM status, participant intents, and BranchBarrier rows"
$failedView = Invoke-RestMethod -Method Get -Uri "$BaseUrl/api/v1/distributed/transactions/$($failedPayment.gid)"
if ($failedView.data.dtm_status -ne "failed" -or
    $failedView.data.account_reservation.status -ne "CANCELLED" -or
    ($null -ne $failedView.data.ledger_intent -and $failedView.data.ledger_intent.status -eq "POSTED")) {
    throw "diagnostic view does not prove DTM Cancel"
}
$failedView | ConvertTo-Json -Depth 12

Write-Host "[8/11] Abort after account Try; DTM must still prove compensation"
$beforeAccountAbort = Invoke-RestMethod -Method Get -Uri "$BaseUrl/api/v1/accounts/demo-user"
$accountAbort = Invoke-PayFlowCreate -IdempotencyKey "dtm-account-abort-$runId" -OrderId "order-account-abort-$runId" -AmountCents 1800 -Fault "after_account_try"
$accountAbortPayment = Wait-PaymentTerminal -PaymentId $accountAbort.data.payment.id
$afterAccountAbort = Invoke-RestMethod -Method Get -Uri "$BaseUrl/api/v1/accounts/demo-user"
$accountAbortView = Invoke-RestMethod -Method Get -Uri "$BaseUrl/api/v1/distributed/transactions/$($accountAbortPayment.gid)"
if ($accountAbortPayment.status -ne "FAILED" -or
    $afterAccountAbort.data.account.available_cents -ne $beforeAccountAbort.data.account.available_cents -or
    $afterAccountAbort.data.account.frozen_cents -ne 0 -or
    $accountAbortView.data.dtm_status -ne "failed" -or
    $accountAbortView.data.account_reservation.status -ne "CANCELLED") {
    throw "after_account_try compensation invariant failed"
}

Write-Host "[9/11] Race eight identical requests against the MySQL unique key"
$concurrentKey = "dtm-concurrent-$runId"
$concurrentOrder = "order-concurrent-$runId"
$concurrentBody = @{
    order_id = $concurrentOrder
    account_id = "demo-user"
    amount_cents = 1700
    currency = "CNY"
} | ConvertTo-Json -Compress
$jobs = @()
try {
    foreach ($index in 1..8) {
        $jobs += Start-Job -ScriptBlock {
            param($Url, $Key, $Body)
            Invoke-RestMethod -Method Post -Uri "$Url/api/v1/distributed/payments" `
                -Headers @{ "Idempotency-Key" = $Key } -ContentType "application/json" -Body $Body
        } -ArgumentList $BaseUrl, $concurrentKey, $concurrentBody
    }
    $completed = @($jobs | Wait-Job -Timeout 60)
    if ($completed.Count -ne 8 -or @($jobs | Where-Object State -ne "Completed").Count -ne 0) {
        throw "not all concurrent requests completed successfully"
    }
    $concurrentResponses = @($jobs | Receive-Job -ErrorAction Stop)
}
finally {
    $jobs | Remove-Job -Force -ErrorAction SilentlyContinue
}
$uniquePaymentIds = @($concurrentResponses | ForEach-Object { $_.data.payment.id } | Sort-Object -Unique)
if ($concurrentResponses.Count -ne 8 -or $uniquePaymentIds.Count -ne 1) {
    throw "concurrent idempotency failed: responses=$($concurrentResponses.Count), unique payments=$($uniquePaymentIds.Count)"
}
$concurrentPayment = Wait-PaymentTerminal -PaymentId $uniquePaymentIds[0]
if ($concurrentPayment.status -ne "SUCCESS") {
    throw "the single concurrent payment did not finish SUCCESS"
}

Write-Host "[10/11] Prove concurrency produced one payment and one extra ledger entry"
$allPayments = Invoke-RestMethod -Method Get -Uri "$BaseUrl/api/v1/payments"
$matchingPayments = @($allPayments.data.payments | Where-Object order_id -eq $concurrentOrder)
$ledgerAfterConcurrency = Invoke-RestMethod -Method Get -Uri "$BaseUrl/api/v1/ledger"
if ($matchingPayments.Count -ne 1 -or $ledgerAfterConcurrency.data.entries.Count -ne 2) {
    throw "concurrency created duplicate payments or ledger entries"
}

Write-Host "[11/11] Final metrics"
$metrics = Invoke-RestMethod -Method Get -Uri "$BaseUrl/api/v1/metrics"
$metrics | ConvertTo-Json -Depth 8
Write-Host "[PayFlow] PASS: Confirm, both Cancel fault points, and concurrent idempotency verified."
