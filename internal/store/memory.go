package store

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/zzbaichuan-glitch/PayFlow-Lab/internal/domain"
)

const demoBalanceCents int64 = 1_000_000

type idempotencyRecord struct {
	PaymentID   string
	Fingerprint string
}

type reservation struct {
	PaymentID string
	AccountID string
	Amount    int64
	State     string
}

// MemoryStore deliberately uses one mutex. It makes the invariants easy to
// inspect while learning and makes every individual repository operation atomic.
// It is not durable and is not presented as a distributed transaction manager.
type MemoryStore struct {
	mu sync.RWMutex

	payments       map[string]domain.Payment
	events         map[string][]domain.PaymentEvent
	eventSequences map[string]int64
	idempotency    map[string]idempotencyRecord
	callbacksSeen  map[string]map[string]struct{}
	accounts       map[string]domain.Account
	reservations   map[string]reservation
	ledger         []domain.LedgerEntry
	ledgerSequence int64
}

func NewMemoryStore() *MemoryStore {
	s := &MemoryStore{}
	s.resetLocked()
	return s
}

func (s *MemoryStore) CreatePayment(_ context.Context, payment domain.Payment, idempotencyKey, fingerprint string) (domain.Payment, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if record, ok := s.idempotency[idempotencyKey]; ok {
		if record.Fingerprint != fingerprint {
			return domain.Payment{}, false, ErrIdempotencyConflict
		}
		return s.payments[record.PaymentID], false, nil
	}
	if _, exists := s.payments[payment.ID]; exists {
		return domain.Payment{}, false, fmt.Errorf("payment id collision: %s", payment.ID)
	}

	s.payments[payment.ID] = payment
	s.idempotency[idempotencyKey] = idempotencyRecord{PaymentID: payment.ID, Fingerprint: fingerprint}
	s.appendEventLocked(payment.ID, domain.PaymentEvent{
		Type:    "PAYMENT_CREATED",
		To:      domain.PaymentStatusInit,
		Message: "payment accepted with an idempotency key",
	})
	return payment, true, nil
}

func (s *MemoryStore) StartPayment(_ context.Context, paymentID string) (domain.Payment, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	payment, ok := s.payments[paymentID]
	if !ok {
		return domain.Payment{}, ErrNotFound
	}
	if payment.Status != domain.PaymentStatusInit {
		return domain.Payment{}, ErrInvalidTransition
	}
	from := payment.Status
	payment.Status = domain.PaymentStatusProcessing
	payment.UpdatedAt = time.Now().UTC()
	s.payments[paymentID] = payment
	s.appendEventLocked(paymentID, domain.PaymentEvent{
		Type: "STATUS_CHANGED", From: from, To: payment.Status,
		Message: "payment entered processing",
	})
	return payment, nil
}

func (s *MemoryStore) Freeze(_ context.Context, paymentID string) (domain.Payment, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	payment, ok := s.payments[paymentID]
	if !ok {
		return domain.Payment{}, ErrNotFound
	}
	if payment.Status != domain.PaymentStatusProcessing {
		return domain.Payment{}, ErrInvalidTransition
	}
	if existing, ok := s.reservations[paymentID]; ok {
		if existing.State == "FROZEN" || existing.State == "CONFIRMED" {
			return payment, nil
		}
		return domain.Payment{}, ErrInvalidTransition
	}
	account, ok := s.accounts[payment.AccountID]
	if !ok {
		return domain.Payment{}, ErrNotFound
	}
	if account.AvailableCents < payment.AmountCents {
		return domain.Payment{}, ErrInsufficientFunds
	}

	now := time.Now().UTC()
	account.AvailableCents -= payment.AmountCents
	account.FrozenCents += payment.AmountCents
	account.UpdatedAt = now
	s.accounts[account.ID] = account
	s.reservations[paymentID] = reservation{
		PaymentID: paymentID, AccountID: account.ID, Amount: payment.AmountCents, State: "FROZEN",
	}
	s.appendEventLocked(paymentID, domain.PaymentEvent{
		Type: "FUNDS_FROZEN", Message: "account funds reserved in local TCC Try",
		Metadata: map[string]string{"amount_cents": fmt.Sprint(payment.AmountCents), "account_id": account.ID},
	})
	return payment, nil
}

func (s *MemoryStore) FailAndRelease(_ context.Context, paymentID, reason string) (domain.Payment, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	payment, ok := s.payments[paymentID]
	if !ok {
		return domain.Payment{}, ErrNotFound
	}
	if payment.Status.Terminal() {
		return domain.Payment{}, ErrInvalidTransition
	}
	s.releaseLocked(paymentID, "CANCELLED")
	from := payment.Status
	payment.Status = domain.PaymentStatusFailed
	payment.FailureReason = reason
	payment.UpdatedAt = time.Now().UTC()
	s.payments[paymentID] = payment
	s.appendEventLocked(paymentID, domain.PaymentEvent{
		Type: "STATUS_CHANGED", From: from, To: payment.Status, Message: reason,
	})
	return payment, nil
}

func (s *MemoryStore) ProcessCallback(_ context.Context, paymentID string, callback domain.Callback) (domain.CallbackResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if callback.Outcome != "success" && callback.Outcome != "failed" {
		return domain.CallbackResult{}, fmt.Errorf("unsupported callback outcome: %s", callback.Outcome)
	}
	payment, ok := s.payments[paymentID]
	if !ok {
		return domain.CallbackResult{}, ErrNotFound
	}
	seen := s.callbacksSeen[paymentID]
	if seen == nil {
		seen = make(map[string]struct{})
		s.callbacksSeen[paymentID] = seen
	}
	if _, duplicate := seen[callback.EventID]; duplicate {
		s.appendIgnoredCallbackLocked(paymentID, "CALLBACK_DUPLICATE", callback, "event id was already processed")
		return domain.CallbackResult{Payment: payment, Disposition: domain.CallbackDuplicate}, nil
	}
	seen[callback.EventID] = struct{}{}

	if callback.Sequence <= payment.LastCallbackSequence {
		s.appendIgnoredCallbackLocked(paymentID, "CALLBACK_OUT_OF_ORDER", callback, "callback sequence did not advance")
		return domain.CallbackResult{Payment: payment, Disposition: domain.CallbackOutOfOrder}, nil
	}
	if payment.Status.Terminal() {
		s.appendIgnoredCallbackLocked(paymentID, "CALLBACK_TERMINAL_IGNORED", callback, "terminal payment state is immutable")
		return domain.CallbackResult{Payment: payment, Disposition: domain.CallbackTerminalIgnored}, nil
	}
	if payment.Status != domain.PaymentStatusProcessing {
		return domain.CallbackResult{}, ErrInvalidTransition
	}

	from := payment.Status
	payment.LastCallbackSequence = callback.Sequence
	payment.UpdatedAt = time.Now().UTC()
	s.appendEventLocked(paymentID, domain.PaymentEvent{
		Type: "CALLBACK_ACCEPTED", Message: "channel callback passed duplicate and ordering checks",
		Metadata: callbackMetadata(callback),
	})

	switch callback.Outcome {
	case "success":
		reservation, exists := s.reservations[paymentID]
		if !exists || reservation.State != "FROZEN" {
			return domain.CallbackResult{}, ErrInvalidTransition
		}
		account := s.accounts[reservation.AccountID]
		account.FrozenCents -= reservation.Amount
		account.UpdatedAt = payment.UpdatedAt
		s.accounts[account.ID] = account
		reservation.State = "CONFIRMED"
		s.reservations[paymentID] = reservation
		s.ledgerSequence++
		s.ledger = append(s.ledger, domain.LedgerEntry{
			ID: fmt.Sprintf("ledger_%06d", s.ledgerSequence), PaymentID: paymentID,
			AccountID: payment.AccountID, AmountCents: payment.AmountCents,
			Direction: "DEBIT", CreatedAt: payment.UpdatedAt,
		})
		payment.Status = domain.PaymentStatusSuccess
		s.appendEventLocked(paymentID, domain.PaymentEvent{
			Type: "FUNDS_CONFIRMED", Message: "frozen funds committed and one ledger entry created",
		})
	case "failed":
		s.releaseLocked(paymentID, "CANCELLED")
		payment.Status = domain.PaymentStatusFailed
		payment.FailureReason = callback.Reason
		if payment.FailureReason == "" {
			payment.FailureReason = "channel reported failure"
		}
	}

	s.payments[paymentID] = payment
	s.appendEventLocked(paymentID, domain.PaymentEvent{
		Type: "STATUS_CHANGED", From: from, To: payment.Status, Message: "callback applied",
	})
	return domain.CallbackResult{Payment: payment, Disposition: domain.CallbackApplied}, nil
}

func (s *MemoryStore) ClosePayment(_ context.Context, paymentID, reason string) (domain.Payment, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	payment, ok := s.payments[paymentID]
	if !ok {
		return domain.Payment{}, ErrNotFound
	}
	if payment.Status != domain.PaymentStatusProcessing {
		return domain.Payment{}, ErrInvalidTransition
	}
	s.releaseLocked(paymentID, "CANCELLED")
	from := payment.Status
	payment.Status = domain.PaymentStatusClosed
	payment.FailureReason = ""
	payment.UpdatedAt = time.Now().UTC()
	s.payments[paymentID] = payment
	if reason == "" {
		reason = "payment closed by operator"
	}
	s.appendEventLocked(paymentID, domain.PaymentEvent{
		Type: "STATUS_CHANGED", From: from, To: payment.Status, Message: reason,
	})
	return payment, nil
}

func (s *MemoryStore) RecordEvent(_ context.Context, paymentID, eventType, message string, metadata map[string]string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.payments[paymentID]; !ok {
		return ErrNotFound
	}
	s.appendEventLocked(paymentID, domain.PaymentEvent{Type: eventType, Message: message, Metadata: metadata})
	return nil
}

func (s *MemoryStore) GetPayment(_ context.Context, paymentID string) (domain.Payment, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	payment, ok := s.payments[paymentID]
	if !ok {
		return domain.Payment{}, ErrNotFound
	}
	return payment, nil
}

func (s *MemoryStore) ListPayments(_ context.Context) ([]domain.Payment, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]domain.Payment, 0, len(s.payments))
	for _, payment := range s.payments {
		result = append(result, payment)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].CreatedAt.After(result[j].CreatedAt) })
	return result, nil
}

func (s *MemoryStore) Events(_ context.Context, paymentID string) ([]domain.PaymentEvent, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if _, ok := s.payments[paymentID]; !ok {
		return nil, ErrNotFound
	}
	items := s.events[paymentID]
	result := make([]domain.PaymentEvent, len(items))
	for i := range items {
		result[i] = cloneEvent(items[i])
	}
	return result, nil
}

func (s *MemoryStore) Account(_ context.Context, accountID string) (domain.Account, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	account, ok := s.accounts[accountID]
	if !ok {
		return domain.Account{}, ErrNotFound
	}
	return account, nil
}

func (s *MemoryStore) Ledger(_ context.Context) ([]domain.LedgerEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]domain.LedgerEntry, len(s.ledger))
	copy(result, s.ledger)
	return result, nil
}

func (s *MemoryStore) Snapshot(_ context.Context) (domain.StoreSnapshot, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	counts := make(map[domain.PaymentStatus]int)
	for _, payment := range s.payments {
		counts[payment.Status]++
	}
	return domain.StoreSnapshot{
		PaymentCount: len(s.payments), StatusCounts: counts,
		AccountCount: len(s.accounts), LedgerCount: len(s.ledger),
	}, nil
}

func (s *MemoryStore) Reset(_ context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.resetLocked()
	return nil
}

func (s *MemoryStore) resetLocked() {
	now := time.Now().UTC()
	s.payments = make(map[string]domain.Payment)
	s.events = make(map[string][]domain.PaymentEvent)
	s.eventSequences = make(map[string]int64)
	s.idempotency = make(map[string]idempotencyRecord)
	s.callbacksSeen = make(map[string]map[string]struct{})
	s.accounts = map[string]domain.Account{
		"demo-user": {
			ID: "demo-user", AvailableCents: demoBalanceCents,
			FrozenCents: 0, Currency: "CNY", UpdatedAt: now,
		},
	}
	s.reservations = make(map[string]reservation)
	s.ledger = nil
	s.ledgerSequence = 0
}

func (s *MemoryStore) releaseLocked(paymentID, state string) {
	reservation, exists := s.reservations[paymentID]
	if !exists || reservation.State != "FROZEN" {
		return
	}
	account := s.accounts[reservation.AccountID]
	account.AvailableCents += reservation.Amount
	account.FrozenCents -= reservation.Amount
	account.UpdatedAt = time.Now().UTC()
	s.accounts[account.ID] = account
	reservation.State = state
	s.reservations[paymentID] = reservation
	s.appendEventLocked(paymentID, domain.PaymentEvent{
		Type: "FUNDS_RELEASED", Message: "frozen funds returned in local TCC Cancel",
	})
}

func (s *MemoryStore) appendIgnoredCallbackLocked(paymentID, eventType string, callback domain.Callback, message string) {
	s.appendEventLocked(paymentID, domain.PaymentEvent{
		Type: eventType, Message: message, Metadata: callbackMetadata(callback),
	})
}

func (s *MemoryStore) appendEventLocked(paymentID string, event domain.PaymentEvent) {
	s.eventSequences[paymentID]++
	event.Sequence = s.eventSequences[paymentID]
	event.At = time.Now().UTC()
	event.Metadata = cloneMetadata(event.Metadata)
	s.events[paymentID] = append(s.events[paymentID], event)
}

func callbackMetadata(callback domain.Callback) map[string]string {
	return map[string]string{
		"event_id":          callback.EventID,
		"callback_sequence": fmt.Sprint(callback.Sequence),
		"outcome":           callback.Outcome,
	}
}

func cloneEvent(event domain.PaymentEvent) domain.PaymentEvent {
	event.Metadata = cloneMetadata(event.Metadata)
	return event
}

func cloneMetadata(metadata map[string]string) map[string]string {
	if metadata == nil {
		return nil
	}
	result := make(map[string]string, len(metadata))
	for key, value := range metadata {
		result[key] = value
	}
	return result
}
