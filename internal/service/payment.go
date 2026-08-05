package service

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zzbaichuan-glitch/PayFlow-Lab/internal/domain"
	"github.com/zzbaichuan-glitch/PayFlow-Lab/internal/store"
)

var ErrInjectedFault = errors.New("injected fault")

type ValidationError struct {
	Message string
}

func (e *ValidationError) Error() string { return e.Message }

type CreatePaymentInput struct {
	OrderID        string
	AccountID      string
	AmountCents    int64
	Currency       string
	IdempotencyKey string
	AutoConfirm    bool
	Fault          string
}

type CreatePaymentResult struct {
	Payment          domain.Payment `json:"payment"`
	IdempotentReplay bool           `json:"idempotent_replay"`
}

type CallbackInput struct {
	EventID  string
	Sequence int64
	Outcome  string
	Reason   string
	Fault    string
}

type PaymentDetails struct {
	Payment domain.Payment        `json:"payment"`
	Events  []domain.PaymentEvent `json:"events"`
}

type MetricsSnapshot struct {
	Mode                          string                       `json:"mode"`
	UptimeSeconds                 int64                        `json:"uptime_seconds"`
	PaymentsTotal                 int                          `json:"payments_total"`
	PaymentsByStatus              map[domain.PaymentStatus]int `json:"payments_by_status"`
	AccountsTotal                 int                          `json:"accounts_total"`
	LedgerEntries                 int                          `json:"ledger_entries"`
	PaymentsCreatedTotal          uint64                       `json:"payments_created_total"`
	IdempotentReplaysTotal        uint64                       `json:"idempotent_replays_total"`
	CallbacksTotal                uint64                       `json:"callbacks_total"`
	CallbacksAppliedTotal         uint64                       `json:"callbacks_applied_total"`
	CallbacksDuplicateTotal       uint64                       `json:"callbacks_duplicate_total"`
	CallbacksOutOfOrderTotal      uint64                       `json:"callbacks_out_of_order_total"`
	CallbacksTerminalIgnoredTotal uint64                       `json:"callbacks_terminal_ignored_total"`
	FaultsInjectedTotal           uint64                       `json:"faults_injected_total"`
	RedisIdempotencyHitsTotal     uint64                       `json:"redis_idempotency_hits_total,omitempty"`
	DTMTransactionsTotal          uint64                       `json:"dtm_transactions_total,omitempty"`
}

type counters struct {
	paymentsCreated          atomic.Uint64
	idempotentReplays        atomic.Uint64
	callbacks                atomic.Uint64
	callbacksApplied         atomic.Uint64
	callbacksDuplicate       atomic.Uint64
	callbacksOutOfOrder      atomic.Uint64
	callbacksTerminalIgnored atomic.Uint64
	faultsInjected           atomic.Uint64
}

type idempotencyLock struct {
	mu    sync.Mutex
	users int
}

// PaymentService is the application layer. It coordinates a local Try/Confirm/
// Cancel workflow through Store. This is intentionally named local coordination:
// it does not claim to be DTM or to provide cross-process atomicity.
type PaymentService struct {
	store           store.Store
	startedUnix     atomic.Int64
	metrics         counters
	idempotencyMu   sync.Mutex
	idempotencyKeys map[string]*idempotencyLock
}

func NewPaymentService(repository store.Store) *PaymentService {
	result := &PaymentService{store: repository, idempotencyKeys: make(map[string]*idempotencyLock)}
	result.startedUnix.Store(time.Now().Unix())
	return result
}

func (s *PaymentService) Create(ctx context.Context, input CreatePaymentInput) (CreatePaymentResult, error) {
	input = NormalizeCreateInput(input)
	if err := ValidateCreateInput(input); err != nil {
		return CreatePaymentResult{}, err
	}
	releaseKey := s.lockIdempotencyKey(input.IdempotencyKey)
	defer releaseKey()

	now := time.Now().UTC()
	payment := domain.Payment{
		ID: NewID("pay"), ExecutionMode: "memory", OrderID: input.OrderID, AccountID: input.AccountID,
		AmountCents: input.AmountCents, Currency: input.Currency,
		Status: domain.PaymentStatusInit, CreatedAt: now, UpdatedAt: now,
	}
	createdPayment, created, err := s.store.CreatePayment(ctx, payment, input.IdempotencyKey, Fingerprint(input))
	if err != nil {
		return CreatePaymentResult{}, err
	}
	if !created {
		s.metrics.idempotentReplays.Add(1)
		return CreatePaymentResult{Payment: createdPayment, IdempotentReplay: true}, nil
	}
	s.metrics.paymentsCreated.Add(1)

	payment, err = s.store.StartPayment(ctx, createdPayment.ID)
	if err != nil {
		return CreatePaymentResult{}, err
	}
	if isFault(input.Fault, "before_freeze", "freeze") {
		s.metrics.faultsInjected.Add(1)
		_ = s.store.RecordEvent(ctx, payment.ID, "FAULT_INJECTED", "failure injected before local Try", map[string]string{"point": "before_freeze"})
		payment, err = s.store.FailAndRelease(ctx, payment.ID, "injected failure before funds freeze")
		return CreatePaymentResult{Payment: payment}, err
	}

	payment, err = s.store.Freeze(ctx, payment.ID)
	if err != nil {
		if errors.Is(err, store.ErrInsufficientFunds) || errors.Is(err, store.ErrNotFound) {
			reason := "account not found"
			if errors.Is(err, store.ErrInsufficientFunds) {
				reason = "insufficient funds"
			}
			payment, failErr := s.store.FailAndRelease(ctx, createdPayment.ID, reason)
			return CreatePaymentResult{Payment: payment}, failErr
		}
		return CreatePaymentResult{}, err
	}
	if isFault(input.Fault, "after_freeze") {
		s.metrics.faultsInjected.Add(1)
		_ = s.store.RecordEvent(ctx, payment.ID, "FAULT_INJECTED", "failure injected after local Try", map[string]string{"point": "after_freeze"})
		payment, err = s.store.FailAndRelease(ctx, payment.ID, "injected failure after funds freeze; local Cancel completed")
		return CreatePaymentResult{Payment: payment}, err
	}

	if input.AutoConfirm {
		callback, callbackErr := s.Callback(ctx, payment.ID, CallbackInput{
			EventID: "auto_" + payment.ID, Sequence: 1, Outcome: "success",
		})
		if callbackErr != nil {
			return CreatePaymentResult{}, callbackErr
		}
		payment = callback.Payment
	}
	return CreatePaymentResult{Payment: payment}, nil
}

func (s *PaymentService) Mode() string { return "memory" }

// lockIdempotencyKey serializes the complete local workflow for one key, so a
// concurrent replay cannot observe the short INIT state between repository
// calls. It is process-local coordination, not a distributed correctness
// mechanism; a durable adapter still needs a database unique constraint.
func (s *PaymentService) lockIdempotencyKey(key string) func() {
	s.idempotencyMu.Lock()
	entry := s.idempotencyKeys[key]
	if entry == nil {
		entry = &idempotencyLock{}
		s.idempotencyKeys[key] = entry
	}
	entry.users++
	s.idempotencyMu.Unlock()

	entry.mu.Lock()
	return func() {
		entry.mu.Unlock()
		s.idempotencyMu.Lock()
		entry.users--
		if entry.users == 0 {
			delete(s.idempotencyKeys, key)
		}
		s.idempotencyMu.Unlock()
	}
}

func (s *PaymentService) Callback(ctx context.Context, paymentID string, input CallbackInput) (domain.CallbackResult, error) {
	paymentID = strings.TrimSpace(paymentID)
	input.EventID = strings.TrimSpace(input.EventID)
	input.Outcome = strings.ToLower(strings.TrimSpace(input.Outcome))
	input.Reason = strings.TrimSpace(input.Reason)
	input.Fault = strings.ToLower(strings.TrimSpace(input.Fault))
	if paymentID == "" {
		return domain.CallbackResult{}, &ValidationError{Message: "payment id is required"}
	}
	if input.EventID == "" || len(input.EventID) > 128 {
		return domain.CallbackResult{}, &ValidationError{Message: "event_id is required and must be at most 128 characters"}
	}
	if input.Sequence <= 0 {
		return domain.CallbackResult{}, &ValidationError{Message: "sequence must be greater than zero"}
	}
	if input.Outcome != "success" && input.Outcome != "failed" {
		return domain.CallbackResult{}, &ValidationError{Message: "outcome must be success or failed"}
	}
	if input.Fault != "" && !isFault(input.Fault, "before_confirm", "confirm") {
		return domain.CallbackResult{}, &ValidationError{Message: "callback fault must be before_confirm"}
	}
	s.metrics.callbacks.Add(1)

	if input.Outcome == "success" && isFault(input.Fault, "before_confirm", "confirm") {
		s.metrics.faultsInjected.Add(1)
		if err := s.store.RecordEvent(ctx, paymentID, "FAULT_INJECTED", "retryable failure injected before local Confirm", map[string]string{"point": "before_confirm"}); err != nil {
			return domain.CallbackResult{}, err
		}
		return domain.CallbackResult{}, fmt.Errorf("%w: before_confirm", ErrInjectedFault)
	}

	result, err := s.store.ProcessCallback(ctx, paymentID, domain.Callback{
		EventID: input.EventID, Sequence: input.Sequence, Outcome: input.Outcome, Reason: input.Reason,
	})
	if err != nil {
		return domain.CallbackResult{}, err
	}
	switch result.Disposition {
	case domain.CallbackApplied:
		s.metrics.callbacksApplied.Add(1)
	case domain.CallbackDuplicate:
		s.metrics.callbacksDuplicate.Add(1)
	case domain.CallbackOutOfOrder:
		s.metrics.callbacksOutOfOrder.Add(1)
	case domain.CallbackTerminalIgnored:
		s.metrics.callbacksTerminalIgnored.Add(1)
	}
	return result, nil
}

func (s *PaymentService) Close(ctx context.Context, paymentID, reason string) (domain.Payment, error) {
	if strings.TrimSpace(paymentID) == "" {
		return domain.Payment{}, &ValidationError{Message: "payment id is required"}
	}
	return s.store.ClosePayment(ctx, paymentID, strings.TrimSpace(reason))
}

func (s *PaymentService) Details(ctx context.Context, paymentID string) (PaymentDetails, error) {
	payment, err := s.store.GetPayment(ctx, strings.TrimSpace(paymentID))
	if err != nil {
		return PaymentDetails{}, err
	}
	events, err := s.store.Events(ctx, payment.ID)
	if err != nil {
		return PaymentDetails{}, err
	}
	return PaymentDetails{Payment: payment, Events: events}, nil
}

func (s *PaymentService) Events(ctx context.Context, paymentID string) ([]domain.PaymentEvent, error) {
	return s.store.Events(ctx, strings.TrimSpace(paymentID))
}

func (s *PaymentService) List(ctx context.Context) ([]domain.Payment, error) {
	return s.store.ListPayments(ctx)
}

func (s *PaymentService) Account(ctx context.Context, accountID string) (domain.Account, error) {
	return s.store.Account(ctx, strings.TrimSpace(accountID))
}

func (s *PaymentService) Ledger(ctx context.Context) ([]domain.LedgerEntry, error) {
	return s.store.Ledger(ctx)
}

func (s *PaymentService) Metrics(ctx context.Context) (MetricsSnapshot, error) {
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return MetricsSnapshot{}, err
	}
	return MetricsSnapshot{
		Mode: "memory", UptimeSeconds: time.Now().Unix() - s.startedUnix.Load(),
		PaymentsTotal: snapshot.PaymentCount, PaymentsByStatus: snapshot.StatusCounts,
		AccountsTotal: snapshot.AccountCount, LedgerEntries: snapshot.LedgerCount,
		PaymentsCreatedTotal:          s.metrics.paymentsCreated.Load(),
		IdempotentReplaysTotal:        s.metrics.idempotentReplays.Load(),
		CallbacksTotal:                s.metrics.callbacks.Load(),
		CallbacksAppliedTotal:         s.metrics.callbacksApplied.Load(),
		CallbacksDuplicateTotal:       s.metrics.callbacksDuplicate.Load(),
		CallbacksOutOfOrderTotal:      s.metrics.callbacksOutOfOrder.Load(),
		CallbacksTerminalIgnoredTotal: s.metrics.callbacksTerminalIgnored.Load(),
		FaultsInjectedTotal:           s.metrics.faultsInjected.Load(),
	}, nil
}

func (s *PaymentService) Reset(ctx context.Context) error {
	if err := s.store.Reset(ctx); err != nil {
		return err
	}
	s.metrics.paymentsCreated.Store(0)
	s.metrics.idempotentReplays.Store(0)
	s.metrics.callbacks.Store(0)
	s.metrics.callbacksApplied.Store(0)
	s.metrics.callbacksDuplicate.Store(0)
	s.metrics.callbacksOutOfOrder.Store(0)
	s.metrics.callbacksTerminalIgnored.Store(0)
	s.metrics.faultsInjected.Store(0)
	s.startedUnix.Store(time.Now().Unix())
	return nil
}

func NormalizeCreateInput(input CreatePaymentInput) CreatePaymentInput {
	input.OrderID = strings.TrimSpace(input.OrderID)
	input.AccountID = strings.TrimSpace(input.AccountID)
	input.Currency = strings.ToUpper(strings.TrimSpace(input.Currency))
	input.IdempotencyKey = strings.TrimSpace(input.IdempotencyKey)
	input.Fault = strings.ToLower(strings.TrimSpace(input.Fault))
	if input.Currency == "" {
		input.Currency = "CNY"
	}
	return input
}

func ValidateCreateInput(input CreatePaymentInput) error {
	if input.OrderID == "" || len(input.OrderID) > 128 {
		return &ValidationError{Message: "order_id is required and must be at most 128 characters"}
	}
	if input.AccountID == "" || len(input.AccountID) > 128 {
		return &ValidationError{Message: "account_id is required and must be at most 128 characters"}
	}
	if input.AmountCents <= 0 || input.AmountCents > 100_000_000 {
		return &ValidationError{Message: "amount_cents must be between 1 and 100000000"}
	}
	if input.Currency != "CNY" {
		return &ValidationError{Message: "this learning build supports currency CNY only"}
	}
	if input.IdempotencyKey == "" || len(input.IdempotencyKey) > 128 {
		return &ValidationError{Message: "idempotency key is required and must be at most 128 characters"}
	}
	if input.Fault != "" && !isFault(input.Fault, "before_freeze", "freeze", "after_freeze") {
		return &ValidationError{Message: "create fault must be before_freeze or after_freeze"}
	}
	return nil
}

func Fingerprint(input CreatePaymentInput) string {
	payload := fmt.Sprintf("%s\x00%s\x00%d\x00%s", input.OrderID, input.AccountID, input.AmountCents, input.Currency)
	digest := sha256.Sum256([]byte(payload))
	return hex.EncodeToString(digest[:])
}

func isFault(value string, choices ...string) bool {
	for _, choice := range choices {
		if value == choice {
			return true
		}
	}
	return false
}

func NewID(prefix string) string {
	buffer := make([]byte, 12)
	if _, err := rand.Read(buffer); err == nil {
		return prefix + "_" + hex.EncodeToString(buffer)
	}
	return fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano())
}
