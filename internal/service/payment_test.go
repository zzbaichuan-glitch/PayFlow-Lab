package service_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/zzbaichuan-glitch/PayFlow-Lab/internal/domain"
	"github.com/zzbaichuan-glitch/PayFlow-Lab/internal/service"
	"github.com/zzbaichuan-glitch/PayFlow-Lab/internal/store"
)

func TestPaymentLifecycleIdempotencyAndCallbackGuards(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	repository := store.NewMemoryStore()
	payments := service.NewPaymentService(repository)

	created, err := payments.Create(ctx, validCreateInput("idem-lifecycle"))
	if err != nil {
		t.Fatalf("create payment: %v", err)
	}
	if created.Payment.Status != domain.PaymentStatusProcessing {
		t.Fatalf("status = %s, want PROCESSING", created.Payment.Status)
	}
	account, err := payments.Account(ctx, "demo-user")
	if err != nil {
		t.Fatalf("get account: %v", err)
	}
	if account.AvailableCents != 998_001 || account.FrozenCents != 1_999 {
		t.Fatalf("balance after freeze = available %d/frozen %d", account.AvailableCents, account.FrozenCents)
	}

	replay, err := payments.Create(ctx, validCreateInput("idem-lifecycle"))
	if err != nil {
		t.Fatalf("replay payment: %v", err)
	}
	if !replay.IdempotentReplay || replay.Payment.ID != created.Payment.ID {
		t.Fatalf("unexpected replay result: %#v", replay)
	}

	conflicting := validCreateInput("idem-lifecycle")
	conflicting.AmountCents++
	if _, err := payments.Create(ctx, conflicting); !errors.Is(err, store.ErrIdempotencyConflict) {
		t.Fatalf("conflicting idempotency key error = %v, want ErrIdempotencyConflict", err)
	}

	callback := service.CallbackInput{EventID: "channel-1", Sequence: 1, Outcome: "success"}
	applied, err := payments.Callback(ctx, created.Payment.ID, callback)
	if err != nil {
		t.Fatalf("apply callback: %v", err)
	}
	if applied.Disposition != domain.CallbackApplied || applied.Payment.Status != domain.PaymentStatusSuccess {
		t.Fatalf("unexpected applied callback: %#v", applied)
	}
	account, _ = payments.Account(ctx, "demo-user")
	if account.AvailableCents != 998_001 || account.FrozenCents != 0 {
		t.Fatalf("balance after confirm = available %d/frozen %d", account.AvailableCents, account.FrozenCents)
	}
	ledger, _ := payments.Ledger(ctx)
	if len(ledger) != 1 {
		t.Fatalf("ledger entries = %d, want 1", len(ledger))
	}

	duplicate, err := payments.Callback(ctx, created.Payment.ID, callback)
	if err != nil {
		t.Fatalf("duplicate callback: %v", err)
	}
	if duplicate.Disposition != domain.CallbackDuplicate {
		t.Fatalf("duplicate disposition = %s", duplicate.Disposition)
	}
	outOfOrder, err := payments.Callback(ctx, created.Payment.ID, service.CallbackInput{
		EventID: "channel-old", Sequence: 1, Outcome: "success",
	})
	if err != nil {
		t.Fatalf("out-of-order callback: %v", err)
	}
	if outOfOrder.Disposition != domain.CallbackOutOfOrder {
		t.Fatalf("out-of-order disposition = %s", outOfOrder.Disposition)
	}
	terminal, err := payments.Callback(ctx, created.Payment.ID, service.CallbackInput{
		EventID: "channel-rollback", Sequence: 2, Outcome: "failed",
	})
	if err != nil {
		t.Fatalf("terminal callback: %v", err)
	}
	if terminal.Disposition != domain.CallbackTerminalIgnored || terminal.Payment.Status != domain.PaymentStatusSuccess {
		t.Fatalf("terminal guard failed: %#v", terminal)
	}
	ledger, _ = payments.Ledger(ctx)
	if len(ledger) != 1 {
		t.Fatalf("duplicate/late callbacks changed ledger count to %d", len(ledger))
	}
}

func TestFaultAfterFreezeCompensatesBalance(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	payments := service.NewPaymentService(store.NewMemoryStore())
	input := validCreateInput("idem-fault")
	input.Fault = "after_freeze"

	result, err := payments.Create(ctx, input)
	if err != nil {
		t.Fatalf("create with fault: %v", err)
	}
	if result.Payment.Status != domain.PaymentStatusFailed {
		t.Fatalf("status = %s, want FAILED", result.Payment.Status)
	}
	account, _ := payments.Account(ctx, "demo-user")
	if account.AvailableCents != 1_000_000 || account.FrozenCents != 0 {
		t.Fatalf("Cancel did not restore balance: %#v", account)
	}
	ledger, _ := payments.Ledger(ctx)
	if len(ledger) != 0 {
		t.Fatalf("failed payment created %d ledger entries", len(ledger))
	}
}

func TestRetrySameCallbackAfterInjectedConfirmFault(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	payments := service.NewPaymentService(store.NewMemoryStore())
	created, err := payments.Create(ctx, validCreateInput("idem-confirm-retry"))
	if err != nil {
		t.Fatal(err)
	}
	callback := service.CallbackInput{
		EventID: "retryable-event", Sequence: 1, Outcome: "success", Fault: "before_confirm",
	}
	if _, err := payments.Callback(ctx, created.Payment.ID, callback); !errors.Is(err, service.ErrInjectedFault) {
		t.Fatalf("fault error = %v, want ErrInjectedFault", err)
	}
	details, _ := payments.Details(ctx, created.Payment.ID)
	if details.Payment.Status != domain.PaymentStatusProcessing || details.Payment.LastCallbackSequence != 0 {
		t.Fatalf("fault consumed callback or changed state: %#v", details.Payment)
	}

	callback.Fault = ""
	result, err := payments.Callback(ctx, created.Payment.ID, callback)
	if err != nil {
		t.Fatalf("retry callback: %v", err)
	}
	if result.Disposition != domain.CallbackApplied || result.Payment.Status != domain.PaymentStatusSuccess {
		t.Fatalf("retry did not apply: %#v", result)
	}
}

func TestConcurrentIdempotencyCreatesOnePayment(t *testing.T) {
	ctx := context.Background()
	payments := service.NewPaymentService(store.NewMemoryStore())
	const workers = 32

	ids := make(chan string, workers)
	statuses := make(chan domain.PaymentStatus, workers)
	errorsFound := make(chan error, workers)
	var group sync.WaitGroup
	for i := 0; i < workers; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			result, err := payments.Create(ctx, validCreateInput("idem-concurrent"))
			if err != nil {
				errorsFound <- err
				return
			}
			ids <- result.Payment.ID
			statuses <- result.Payment.Status
		}()
	}
	group.Wait()
	close(ids)
	close(statuses)
	close(errorsFound)
	for err := range errorsFound {
		t.Errorf("concurrent create: %v", err)
	}
	uniqueIDs := make(map[string]struct{})
	for id := range ids {
		uniqueIDs[id] = struct{}{}
	}
	if len(uniqueIDs) != 1 {
		t.Fatalf("payment ids = %v, want exactly one", uniqueIDs)
	}
	for status := range statuses {
		if status != domain.PaymentStatusProcessing {
			t.Fatalf("concurrent replay observed transient status %s", status)
		}
	}
	items, _ := payments.List(ctx)
	if len(items) != 1 {
		t.Fatalf("payment count = %d, want 1", len(items))
	}
	account, _ := payments.Account(ctx, "demo-user")
	if account.AvailableCents != 998_001 || account.FrozenCents != 1_999 {
		t.Fatalf("same request froze funds more than once: %#v", account)
	}
	metrics, _ := payments.Metrics(ctx)
	if metrics.PaymentsCreatedTotal != 1 || metrics.IdempotentReplaysTotal != workers-1 {
		t.Fatalf("idempotency metrics = created %d/replayed %d", metrics.PaymentsCreatedTotal, metrics.IdempotentReplaysTotal)
	}
}

func TestInsufficientFundsProducesFailedPayment(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	payments := service.NewPaymentService(store.NewMemoryStore())
	input := validCreateInput("idem-insufficient")
	input.AmountCents = 1_000_001

	result, err := payments.Create(ctx, input)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if result.Payment.Status != domain.PaymentStatusFailed || result.Payment.FailureReason != "insufficient funds" {
		t.Fatalf("unexpected result: %#v", result.Payment)
	}
	account, _ := payments.Account(ctx, "demo-user")
	if account.AvailableCents != 1_000_000 || account.FrozenCents != 0 {
		t.Fatalf("insufficient payment changed balance: %#v", account)
	}
}

func validCreateInput(idempotencyKey string) service.CreatePaymentInput {
	return service.CreatePaymentInput{
		OrderID: fmt.Sprintf("order-%s", idempotencyKey), AccountID: "demo-user",
		AmountCents: 1_999, Currency: "CNY", IdempotencyKey: idempotencyKey,
	}
}
